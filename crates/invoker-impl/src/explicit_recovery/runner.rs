// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The loop of one attempt: reads the SDK's messages, proposes actions, serves reads after the
//! barrier, writes results and the cancel signal, closes on silence, and decides between
//! restart and suspend.

use std::convert::Infallible;
use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::HeaderValue;
use http_body::Frame;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, trace};

use restate_service_protocol_explicit_recovery::CONTENT_TYPE_V1;
use restate_service_protocol_explicit_recovery::codec::{Message, encode};
use restate_service_protocol_explicit_recovery::proto::{
    self, BuiltInSignal, ResultNotificationMessage, SignalNotificationMessage,
    result_notification_message::Result as NotificationResult,
};
use restate_types::errors::InvocationError;
use restate_types::invocation::ResponseResult;
use restate_types::journal_events::ResultVariant;
use restate_types::journal_v2::SignalResult;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_worker_api::invoker::explicit_recovery::{
    ErEffectKind, ErRecord, ErStateScope, ErToInvoker,
};

use super::actions::{
    ActionContext, Classified, Read, from_response_result, from_signal_result, state_keys,
    to_invocation_error, value, void,
};
use super::book::AttemptBook;
use super::decision::{Decision, ShutdownFacts, decide};
use super::{EffectSink, ErTaskOutput, InputItem};
use crate::error::{InvokerError, RequestedErrorBehavior, SdkInvocationErrorV2};

/// Reads of the latest committed state, served to the attempt after the barrier.
pub(crate) trait ErReads {
    fn get_state(
        &mut self,
        scope: ErStateScope,
        key: &Bytes,
    ) -> impl Future<Output = Result<Option<Bytes>, InvokerError>> + Send;
    fn get_state_keys(
        &mut self,
        scope: ErStateScope,
    ) -> impl Future<Output = Result<Vec<Bytes>, InvokerError>> + Send;
    fn read_signal(
        &mut self,
        name: &str,
        index: u32,
    ) -> impl Future<Output = Result<Option<SignalResult>, InvokerError>> + Send;
}

/// Where the runner writes the frames of the request body.
pub(crate) type BodySender = mpsc::UnboundedSender<Result<Frame<Bytes>, Infallible>>;

pub(crate) struct Runner<'a, RD, RS> {
    pub ctx: ActionContext<'a>,
    pub resolver: &'a RS,
    pub reads: RD,
    pub book: AttemptBook,
    /// Non-terminal effects go here, in order.
    pub effects: EffectSink,
    pub body: Option<BodySender>,
    pub inactivity_timeout: Duration,
    pub shutdown_timeout: Duration,
    // -- What the decision needs.
    closed_at: Option<Instant>,
    crossed: bool,
    notified_after_close: bool,
    last_activity: Instant,
    blocked: Option<Read>,
}

impl<'a, RD: ErReads, RS: InvocationTargetResolver> Runner<'a, RD, RS> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        ctx: ActionContext<'a>,
        resolver: &'a RS,
        reads: RD,
        book: AttemptBook,
        effects: EffectSink,
        body: BodySender,
        inactivity_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Self {
        Self {
            ctx,
            resolver,
            reads,
            book,
            effects,
            body: Some(body),
            inactivity_timeout,
            shutdown_timeout,
            closed_at: None,
            crossed: false,
            notified_after_close: false,
            last_activity: Instant::now(),
            blocked: None,
        }
    }

    /// Writes a message to the SDK. Returns false when the runtime's side is closed.
    fn write(&mut self, message: Message) -> bool {
        let Some(body) = &self.body else {
            return false;
        };
        if body.send(Ok(Frame::data(encode(&message)))).is_err() {
            // The request body is gone: what the SDK did not get counts as not sent.
            return false;
        }
        self.last_activity = Instant::now();
        true
    }

    /// Writes a notification: counted in `sent`, or, after the close, in `notified_after_close`.
    fn notify(&mut self, message: Message) {
        if self.write(message) {
            self.book.sent += 1;
        } else {
            self.notified_after_close = true;
        }
    }

    fn write_result(&mut self, result_id: u32, result: NotificationResult) {
        self.notify(
            ResultNotificationMessage {
                result_id,
                result: Some(result),
            }
            .into(),
        );
    }

    fn close(&mut self) {
        debug!("Closing the runtime's side of the stream: silent for the inactivity timeout");
        self.body = None;
        self.closed_at = Some(Instant::now());
    }

    fn propose(&mut self, seq: u64, kind: ErEffectKind) -> Result<(), ErTaskOutput> {
        if self.effects.effect(seq, kind) {
            Ok(())
        } else {
            Err(ErTaskOutput::Failed(
                InvokerError::UnexpectedClosedRequestStream,
            ))
        }
    }

    /// Proposes something only the record keeps. If the main loop is gone the attempt is about
    /// to end, and the next proposal of an action reports it.
    fn record(&mut self, record: ErRecord, internal: Vec<(u32, NotificationResult)>) {
        let seq = self.book.propose(internal, []);
        if !self.effects.effect(seq, ErEffectKind::Record(record)) {
            trace!("Dropping a record of the attempt: the invoker is gone");
        }
    }

    /// Writes the result of an action the attempt waited for, and records its delivery when the
    /// action is recorded and the result can still reach the SDK.
    fn deliver(&mut self, result_id: u32, result: NotificationResult) {
        if self.book.delivered(result_id) && self.body.is_some() {
            let variant = match &result {
                NotificationResult::Void(_) => ResultVariant::Void,
                NotificationResult::Failure(_) => ResultVariant::Failure,
                _ => ResultVariant::Value,
            };
            self.record(ErRecord::ResultDelivered { result_id, variant }, vec![]);
        }
        self.write_result(result_id, result);
    }

    async fn serve(&mut self, read: Read) -> Result<(), InvokerError> {
        trace!(?read, "Serving a read");
        match read {
            Read::GetState {
                scope,
                key,
                result_id,
            } => {
                let result = self
                    .reads
                    .get_state(scope, &key)
                    .await?
                    .map(value)
                    .unwrap_or_else(void);
                self.write_result(result_id, result);
            }
            Read::GetStateKeys { scope, result_id } => {
                let keys = self.reads.get_state_keys(scope).await?;
                self.write_result(result_id, state_keys(keys));
            }
            Read::Signal {
                name,
                index,
                result_id,
            } => match self.reads.read_signal(&name, index).await? {
                Some(result) => self.deliver(result_id, from_signal_result(result)),
                None => self.book.wait_signal(name, index, result_id),
            },
            Read::Barrier { result_id } => {
                if result_id != 0 {
                    self.write_result(result_id, void());
                }
            }
        }
        Ok(())
    }

    async fn on_notification(&mut self, notification: ErToInvoker) -> Result<(), ErTaskOutput> {
        match notification {
            ErToInvoker::Applied { seq, .. } => {
                for (result_id, result) in self.book.applied(seq) {
                    // Only a call's failure for a target that cannot be invoked is recorded.
                    self.deliver(result_id, result);
                }
                if self.book.all_applied()
                    && let Some(read) = self.blocked.take()
                {
                    self.serve(read).await.map_err(ErTaskOutput::Failed)?;
                }
            }
            ErToInvoker::Result {
                result_id, result, ..
            } => {
                if self.book.on_result_forward(result_id) {
                    self.deliver(result_id, from_response_result(result));
                } else {
                    trace!("Dropping result {result_id}: the attempt does not wait for it");
                }
            }
            ErToInvoker::Signal {
                name,
                index,
                result,
                ..
            } => {
                for result_id in self.book.on_signal_forward(&name, index) {
                    self.deliver(result_id, from_signal_result(result.clone()));
                }
            }
            ErToInvoker::Cancel { .. } => {
                if self.book.on_cancel_forward() {
                    self.notify(
                        SignalNotificationMessage {
                            signal_id: BuiltInSignal::Cancel.into(),
                        }
                        .into(),
                    );
                }
            }
        }
        Ok(())
    }

    fn check_headers(&mut self, mut parts: http::response::Parts) -> Result<(), InvokerError> {
        if !parts.status.is_success() {
            return Err(InvokerError::UnexpectedResponse(parts.status));
        }
        let expected = HeaderValue::from_static(CONTENT_TYPE_V1);
        match parts.headers.remove(http::header::CONTENT_TYPE) {
            Some(ct) if ct == expected => Ok(()),
            other => Err(InvokerError::UnexpectedContentType(other, expected)),
        }
    }

    fn decided(&self, answered: bool, processed: u32) -> ErTaskOutput {
        let facts = ShutdownFacts {
            closed_for_inactivity: self.closed_at.is_some(),
            answered,
            processed,
            sent: self.book.sent,
            notified_after_close: self.notified_after_close,
            crossed: self.crossed,
            external_outstanding: self.book.has_external(),
            received_forwards: self.book.received_forwards,
        };
        let decision = decide(&facts);
        debug!(?facts, ?decision, "Attempt shut down");
        match decision {
            Decision::Restart(rule) => ErTaskOutput::Restart(rule),
            Decision::Suspend {
                received_notifications,
            } => ErTaskOutput::Suspend {
                received_notifications,
                outstanding: self.book.outstanding(),
            },
            Decision::Violation(v) => ErTaskOutput::Failed(InvokerError::ErProtocolViolation(v)),
        }
    }

    /// Handles one message of the SDK. `Some` ends the attempt.
    async fn on_message(&mut self, message: Message) -> Option<ErTaskOutput> {
        let closed = self.closed_at.is_some();
        match message {
            Message::Shutdown(m) => {
                // The last message: the runtime closes its side as well.
                self.body = None;
                if !closed {
                    // Unasked: a shutdown not for inactivity (D51).
                    return Some(ErTaskOutput::Restart(
                        restate_worker_api::invoker::explicit_recovery::RestartRule::OwnReason,
                    ));
                }
                Some(self.decided(true, m.processed_notifications))
            }
            Message::End(m) => {
                // D35: the runtime closes as soon as it has the last message.
                self.body = None;
                Some(match m.output {
                    Some(proto::end_message::Output::Value(v)) => {
                        ErTaskOutput::End(ResponseResult::Success(v.content))
                    }
                    Some(proto::end_message::Output::Failure(f)) => {
                        ErTaskOutput::End(ResponseResult::Failure(to_invocation_error(f)))
                    }
                    None => ErTaskOutput::Failed(InvokerError::ErProtocolViolation(
                        "EndMessage without an output".to_owned(),
                    )),
                })
            }
            Message::Error(m) => {
                self.body = None;
                let behavior = match proto::ErrorBehavior::try_from(m.behavior)
                    .unwrap_or(proto::ErrorBehavior::Retry)
                {
                    proto::ErrorBehavior::Retry => {
                        RequestedErrorBehavior::retry(m.next_retry_delay.map(Duration::from_millis))
                    }
                    proto::ErrorBehavior::Pause => RequestedErrorBehavior::Pause,
                    proto::ErrorBehavior::Fail => RequestedErrorBehavior::Fail,
                };
                let mut error =
                    InvocationError::new(u16::try_from(m.code).unwrap_or(500), m.message)
                        .with_metadata_vec(
                            m.metadata.into_iter().map(|m| (m.key, m.value)).collect(),
                        );
                if !m.stacktrace.is_empty() {
                    error = error.with_stacktrace(m.stacktrace);
                }
                Some(ErTaskOutput::Failed(InvokerError::SdkV2(
                    SdkInvocationErrorV2 {
                        related_command: None,
                        requested_error_behavior: behavior,
                        error: error.into(),
                    },
                )))
            }
            Message::Heartbeat(_) => {
                if closed {
                    self.crossed = true;
                }
                None
            }
            Message::Start(_) | Message::ResultNotification(_) | Message::SignalNotification(_) => {
                Some(ErTaskOutput::Failed(InvokerError::ErProtocolViolation(
                    format!("message {:?} sent in the wrong direction", message.ty()),
                )))
            }
            action => {
                if closed {
                    // D39: the action crossed the shutdown. It is still applied.
                    self.crossed = true;
                }
                match self.ctx.classify(action, &mut self.book, self.resolver) {
                    Err(v) => Some(ErTaskOutput::Failed(InvokerError::ErProtocolViolation(v.0))),
                    Ok(Classified::Propose {
                        action,
                        highest_result_id,
                        internal,
                        external,
                        record,
                    }) => {
                        let seq = self.book.propose(internal, external);
                        self.propose(
                            seq,
                            ErEffectKind::Action {
                                action,
                                highest_result_id,
                                record,
                            },
                        )
                        .err()
                    }
                    Ok(Classified::Record { record, internal }) => {
                        let seq = self.book.propose(internal, []);
                        self.propose(seq, ErEffectKind::Record(record)).err()
                    }
                    Ok(Classified::Answer(results)) => {
                        for (result_id, result) in results {
                            self.write_result(result_id, result);
                        }
                        None
                    }
                    Ok(Classified::Read { read, record }) => {
                        // A recorded read is proposed for the record; the read itself waits only
                        // for what came before it.
                        let ready = self.book.all_applied();
                        if let Some(record) = record {
                            self.record(ErRecord::Action(record), vec![]);
                        }
                        if ready {
                            self.serve(read).await.err().map(ErTaskOutput::Failed)
                        } else {
                            // Stop reading until every earlier action is applied (G6).
                            self.blocked = Some(read);
                            None
                        }
                    }
                }
            }
        }
    }

    /// Runs the attempt until it ends. `input` is the SDK's side of the stream.
    pub(crate) async fn run<S>(
        mut self,
        input: &mut S,
        mut notifications: mpsc::UnboundedReceiver<ErToInvoker>,
    ) -> ErTaskOutput
    where
        S: Stream<Item = Result<InputItem, InvokerError>> + Unpin,
    {
        // The main loop holds the sender while the attempt runs; once it is gone nothing more
        // arrives, and the task is about to be aborted.
        let mut notifications_open = true;
        loop {
            let silence_deadline =
                (self.closed_at.is_none() && !self.book.owes() && self.blocked.is_none())
                    .then(|| self.last_activity + self.inactivity_timeout);
            let shutdown_deadline = self.closed_at.map(|at| at + self.shutdown_timeout);

            tokio::select! {
                biased;
                notification = notifications.recv(), if notifications_open => {
                    let Some(notification) = notification else {
                        notifications_open = false;
                        continue;
                    };
                    if let Err(output) = self.on_notification(notification).await {
                        return output;
                    }
                }
                item = input.next(), if self.blocked.is_none() => {
                    match item {
                        Some(Ok(InputItem::Headers(parts))) => {
                            if let Err(e) = self.check_headers(parts) {
                                return ErTaskOutput::Failed(e);
                            }
                        }
                        Some(Ok(InputItem::Partial)) => {
                            // The bytes of a message not complete yet end the silence (D56).
                            self.last_activity = Instant::now();
                        }
                        Some(Ok(InputItem::Message(message))) => {
                            self.last_activity = Instant::now();
                            if let Some(output) = self.on_message(message).await {
                                return output;
                            }
                        }
                        Some(Err(e)) => {
                            if self.closed_at.is_some() {
                                // After the close, every end without a last message is rule 2.
                                return self.decided(false, 0);
                            }
                            return ErTaskOutput::Failed(e);
                        }
                        None => {
                            if self.closed_at.is_some() {
                                return self.decided(false, 0);
                            }
                            return ErTaskOutput::Failed(InvokerError::ErProtocolViolation(
                                "the stream ended without a last message".to_owned(),
                            ));
                        }
                    }
                }
                _ = sleep_until(silence_deadline.unwrap_or_else(Instant::now)), if silence_deadline.is_some() => {
                    self.close();
                }
                _ = sleep_until(shutdown_deadline.unwrap_or_else(Instant::now)), if shutdown_deadline.is_some() => {
                    return self.decided(false, 0);
                }
                else => {
                    // Nothing can make progress any more.
                    return ErTaskOutput::Failed(InvokerError::UnexpectedClosedRequestStream);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::future::join;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    use restate_service_protocol_explicit_recovery::codec::Decoder;
    use restate_types::identifiers::InvocationId;
    use restate_types::invocation::{
        InvocationTarget, InvocationTargetType, ServiceInvocationSpanContext, ServiceType,
        VirtualObjectHandlerType,
    };
    use restate_types::schema::invocation_target::InvocationTargetMetadata;
    use restate_types::schema::invocation_target::test_util::MockInvocationTargetResolver;
    use restate_worker_api::invoker::explicit_recovery::RestartRule;

    use crate::explicit_recovery::{ErTaskEvent, ErTaskMessage};

    const TIMEOUT: Duration = Duration::from_secs(10);

    struct FakeReads;

    impl ErReads for FakeReads {
        async fn get_state(
            &mut self,
            _: ErStateScope,
            _: &Bytes,
        ) -> Result<Option<Bytes>, InvokerError> {
            Ok(Some(Bytes::from_static(b"stored")))
        }
        async fn get_state_keys(&mut self, _: ErStateScope) -> Result<Vec<Bytes>, InvokerError> {
            Ok(vec![])
        }
        async fn read_signal(
            &mut self,
            _: &str,
            _: u32,
        ) -> Result<Option<SignalResult>, InvokerError> {
            Ok(None)
        }
    }

    struct Harness {
        input: mpsc::UnboundedSender<Result<InputItem, InvokerError>>,
        notifications: mpsc::UnboundedSender<ErToInvoker>,
        effects: mpsc::UnboundedReceiver<ErTaskMessage>,
        body: mpsc::UnboundedReceiver<Result<Frame<Bytes>, Infallible>>,
        decoder: Decoder,
    }

    impl Harness {
        fn send(&self, message: impl Into<Message>) {
            self.input
                .send(Ok(InputItem::Message(message.into())))
                .unwrap();
        }

        fn notify(&self, notification: ErToInvoker) {
            self.notifications.send(notification).unwrap();
        }

        async fn effect(&mut self) -> (u64, ErEffectKind) {
            match self.effects.recv().await.unwrap().event {
                ErTaskEvent::Effect { seq, kind } => (seq, kind),
                other => panic!("unexpected {other:?}"),
            }
        }

        /// The next message written to the SDK, `None` once the runtime closed its side.
        async fn written(&mut self) -> Option<Message> {
            loop {
                if let Some(message) = self.decoder.consume_next().unwrap() {
                    return Some(message);
                }
                let frame = self.body.recv().await?.unwrap();
                self.decoder.push(frame.into_data().unwrap());
            }
        }
    }

    /// Runs `driver` against a runner of a virtual object's exclusive handler.
    async fn with_runner<F: Future<Output = ()>>(
        driver: impl FnOnce(Harness) -> F,
    ) -> ErTaskOutput {
        let target = InvocationTarget::mock_virtual_object();
        let span_context = ServiceInvocationSpanContext::empty();
        let mut resolver = MockInvocationTargetResolver::default();
        resolver.add(
            "callee",
            ServiceType::VirtualObject,
            [(
                "h",
                InvocationTargetMetadata::mock(InvocationTargetType::VirtualObject(
                    VirtualObjectHandlerType::Exclusive,
                )),
            )],
        );
        let invocation_id = InvocationId::mock_generate(&target);
        let (input_tx, input_rx) = mpsc::unbounded_channel();
        let (notifications_tx, notifications_rx) = mpsc::unbounded_channel();
        let (effects_tx, effects_rx) = mpsc::unbounded_channel();
        let (body_tx, body_rx) = mpsc::unbounded_channel();
        let runner = Runner::new(
            ActionContext {
                invocation_id,
                invocation_target: &target,
                span_context: &span_context,
            },
            &resolver,
            FakeReads,
            AttemptBook::new(1, false, 2),
            EffectSink {
                tx: effects_tx,
                invocation_id,
                fencing_token: 0,
            },
            body_tx,
            TIMEOUT,
            TIMEOUT,
        );
        let harness = Harness {
            input: input_tx,
            notifications: notifications_tx,
            effects: effects_rx,
            body: body_rx,
            decoder: Decoder::new(usize::MAX),
        };
        let (output, ()) = join(
            async move {
                runner
                    .run(
                        &mut UnboundedReceiverStream::new(input_rx),
                        notifications_rx,
                    )
                    .await
            },
            driver(harness),
        )
        .await;
        output
    }

    fn call() -> proto::CallActionMessage {
        proto::CallActionMessage {
            service_name: "callee".into(),
            handler_name: "h".into(),
            key: "k".into(),
            invocation_id_result_id: 1,
            result_id: 2,
            ..Default::default()
        }
    }

    fn set_state(result_id: u32) -> proto::SetStateActionMessage {
        proto::SetStateActionMessage {
            scope: proto::StateScope::Invocation.into(),
            key: Bytes::from_static(b"k"),
            value: Some(proto::Value {
                content: Bytes::from_static(b"v"),
            }),
            result_id,
            ..Default::default()
        }
    }

    /// A call, its id; then silence: the runtime closes, everything was processed, the call's
    /// result is outstanding: suspend, reporting the forwards taken.
    #[tokio::test(start_paused = true)]
    async fn silence_then_suspend() {
        let output = with_runner(|mut h| async move {
            h.send(call());
            assert_eq!(h.effect().await.0, 2);
            h.notify(ErToInvoker::Applied { attempt: 1, seq: 2 });
            assert!(matches!(
                h.written().await,
                Some(Message::ResultNotification(ResultNotificationMessage {
                    result_id: 1,
                    ..
                }))
            ));
            // A result for an id the attempt did not send: counted, not delivered.
            h.notify(ErToInvoker::Result {
                attempt: 1,
                result_id: 99,
                result: ResponseResult::Success(Bytes::new()),
            });
            // Silent for the inactivity timeout: the runtime closes its side.
            assert!(h.written().await.is_none());
            h.send(proto::ShutdownMessage {
                processed_notifications: 1,
            });
        })
        .await;
        assert!(
            matches!(
                output,
                ErTaskOutput::Suspend {
                    received_notifications: 1,
                    ref outstanding,
                } if outstanding.len() == 1 && outstanding[0].result_id == 2
            ),
            "{output:?}"
        );
    }

    /// An action after the close crossed the shutdown: it is still proposed, then restart.
    #[tokio::test(start_paused = true)]
    async fn crossed_shutdown_restarts() {
        let output = with_runner(|mut h| async move {
            h.send(call());
            assert_eq!(h.effect().await.0, 2);
            h.notify(ErToInvoker::Applied { attempt: 1, seq: 2 });
            h.written().await.unwrap();
            assert!(h.written().await.is_none());
            h.send(set_state(0));
            assert!(matches!(h.effect().await, (3, ErEffectKind::Action { .. })));
            h.send(proto::ShutdownMessage {
                processed_notifications: 1,
            });
        })
        .await;
        assert!(
            matches!(output, ErTaskOutput::Restart(RestartRule::Crossed)),
            "{output:?}"
        );
    }

    /// A read after a write waits for the write to be applied, and nothing the SDK sent after
    /// the read is proposed before the read's result.
    #[tokio::test(start_paused = true)]
    async fn read_waits_for_applied() {
        let output = with_runner(|mut h| async move {
            h.send(set_state(0));
            assert_eq!(h.effect().await.0, 2);
            h.send(proto::GetStateActionMessage {
                scope: proto::StateScope::Invocation.into(),
                key: Bytes::from_static(b"k"),
                result_id: 1,
                ..Default::default()
            });
            h.send(set_state(2));
            // Neither the read's result nor the second write while the first is not applied.
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(h.effects.try_recv().is_err());
            assert!(h.decoder.consume_next().unwrap().is_none());
            assert!(h.body.try_recv().is_err());

            h.notify(ErToInvoker::Applied { attempt: 1, seq: 2 });
            assert!(matches!(
                h.written().await,
                Some(Message::ResultNotification(ResultNotificationMessage {
                    result_id: 1,
                    result: Some(NotificationResult::Value(_)),
                }))
            ));
            assert_eq!(h.effect().await.0, 3);
            h.send(proto::EndMessage {
                output: Some(proto::end_message::Output::Value(proto::Value::default())),
            });
        })
        .await;
        // The last message wins: the attempt ends with the output.
        assert!(
            matches!(output, ErTaskOutput::End(ResponseResult::Success(_))),
            "{output:?}"
        );
    }

    /// A recorded signal read is proposed for the record and waits; a span is proposed for the
    /// record only; the signal's arrival is delivered, and its delivery recorded.
    #[tokio::test(start_paused = true)]
    async fn records_reads_spans_and_deliveries() {
        let output = with_runner(|mut h| async move {
            h.send(proto::ReadSignalActionMessage {
                signal_name: "s".into(),
                result_id: 1,
                record: proto::RecordLevel::RecordSummary.into(),
                ..Default::default()
            });
            assert!(matches!(
                h.effect().await,
                (2, ErEffectKind::Record(ErRecord::Action(r))) if r.kind == "ReadSignal" && r.result_id == Some(1)
            ));
            // The read waits for the attempt start (seq 1); then for the signal.
            h.notify(ErToInvoker::Applied { attempt: 1, seq: 2 });
            h.send(proto::SpanStartActionMessage {
                span_id: 1,
                name: "thinking".into(),
                ..Default::default()
            });
            assert!(matches!(
                h.effect().await,
                (3, ErEffectKind::Record(ErRecord::SpanStart { span_id: 1, .. }))
            ));
            h.notify(ErToInvoker::Signal {
                attempt: 1,
                name: "s".into(),
                index: 0,
                result: SignalResult::Success(Bytes::from_static(b"yes")),
            });
            assert!(matches!(
                h.effect().await,
                (
                    4,
                    ErEffectKind::Record(ErRecord::ResultDelivered {
                        result_id: 1,
                        variant: ResultVariant::Value
                    })
                )
            ));
            assert!(matches!(
                h.written().await,
                Some(Message::ResultNotification(ResultNotificationMessage {
                    result_id: 1,
                    ..
                }))
            ));

            // D22: the failure of a call to a target that cannot be invoked comes once the call
            // is applied, and is recorded as delivered when the call is recorded.
            for (record, first_seq) in [(proto::RecordLevel::RecordDefault, 5), (proto::RecordLevel::RecordNone, 7)] {
                let id = first_seq as u32 - 3;
                h.send(proto::CallActionMessage {
                    service_name: "unknown".into(),
                    handler_name: "h".into(),
                    invocation_id_result_id: id,
                    result_id: id + 1,
                    record: record.into(),
                    ..Default::default()
                });
                assert!(matches!(
                    h.effect().await,
                    (seq, ErEffectKind::Action { .. }) if seq == first_seq
                ));
                h.notify(ErToInvoker::Applied { attempt: 1, seq: first_seq });
                assert!(matches!(
                    h.written().await,
                    Some(Message::ResultNotification(ResultNotificationMessage {
                        result: Some(NotificationResult::InvocationId(_)),
                        ..
                    }))
                ));
                assert!(matches!(
                    h.written().await,
                    Some(Message::ResultNotification(ResultNotificationMessage {
                        result_id,
                        result: Some(NotificationResult::Failure(_)),
                    })) if result_id == id + 1
                ));
                if record == proto::RecordLevel::RecordDefault {
                    assert!(matches!(
                        h.effect().await,
                        (
                            6,
                            ErEffectKind::Record(ErRecord::ResultDelivered {
                                result_id: 3,
                                variant: ResultVariant::Failure
                            })
                        )
                    ));
                    h.notify(ErToInvoker::Applied { attempt: 1, seq: 6 });
                }
            }
            // Nothing is recorded of the `RECORD_NONE` call's result.
            assert!(h.effects.try_recv().is_err());
            h.send(proto::EndMessage {
                output: Some(proto::end_message::Output::Value(proto::Value::default())),
            });
        })
        .await;
        assert!(matches!(output, ErTaskOutput::End(_)), "{output:?}");
    }
}
