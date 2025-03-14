// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::{FutureExt, Stream};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use tokio::sync::{mpsc, oneshot};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use restate_types::Versioned;
use restate_types::live::Live;
use restate_types::logs::metadata::Logs;
use restate_types::nodes_config::NodesConfiguration;
use restate_types::partition_table::PartitionTable;
use restate_types::schema::Schema;

use super::EgressSender;
use super::egress_sender::{Sent, UnboundedEgressSender};
use crate::Metadata;
use crate::network::protobuf::network::{
    Header, Message, SpanContext, message, message::Body, message::ConnectionControl,
};

/// A handle to drop the egress stream remotely, or to be notified if the egress stream has been
/// terminated via other means.
// todo: make pub(crate) after its usage in tests is removed
pub struct DropEgressStream(oneshot::Receiver<Infallible>);

impl DropEgressStream {
    /// same effect as dropping, closing the egress stream and dropping any messages in the buffer
    pub fn close(&mut self) {
        self.0.close();
    }
}

impl Future for DropEgressStream {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match ready!(self.0.poll_unpin(cx)) {
            Ok(_) => unreachable!(),
            Err(_) => Poll::Ready(()),
        }
    }
}

#[derive(Debug)]
pub enum DrainReason {
    /// Node will shutdown and never come back
    Shutdown,
    /// Only this connection is terminating
    ConnectionDrain,
    CodecError(String),
}

impl From<DrainReason> for Body {
    fn from(value: DrainReason) -> Self {
        match value {
            DrainReason::Shutdown => ConnectionControl::shutdown().into(),
            DrainReason::ConnectionDrain => ConnectionControl::connection_reset().into(),
            DrainReason::CodecError(e) => ConnectionControl::codec_error(e).into(),
        }
    }
}

// todo: make this pub(crate) when OwnedConnection::new_fake() stops needing it
pub enum EgressMessage {
    #[cfg(any(test, feature = "test-util"))]
    /// An egress message to send to peer. Used only in tests, header must be populated correctly
    /// by sender
    RawMessage(Message),
    /// An egress body to send to peer, header is populated by egress stream
    /// todo: remove Header once msg-id/in-response-to are removed (est. v1.4)
    Message(Header, Body, Option<Span>),
    /// The message that requires an ack that it was sent
    WithNotifer(Body, Option<Span>, Sent),
    /// A signal to close the bounded stream. The inner stream cannot receive further messages but
    /// we'll continue to drain all buffered messages before dropping.
    Close(DrainReason),
}

struct MetadataVersionCache {
    nodes_config: Live<NodesConfiguration>,
    schema: Live<Schema>,
    partition_table: Live<PartitionTable>,
    logs_metadata: Live<Logs>,
}

impl MetadataVersionCache {
    fn new() -> Self {
        let metadata = Metadata::current();
        Self {
            nodes_config: metadata.updateable_nodes_config(),
            schema: metadata.updateable_schema(),
            partition_table: metadata.updateable_partition_table(),
            logs_metadata: metadata.updateable_logs_metadata(),
        }
    }
}

#[derive(Debug, Default)]
enum State {
    #[default]
    Open,
    // close reason has been sent
    Draining,
    // All channels closed
    Closed,
}

enum Decision {
    /// Ready to send the message
    Ready(Message),
    /// Sending a message indicating starting the drain
    StartDrain(Message),
    /// Sending a drained message
    SendDrainedMessage,
    /// message skipped, move to next
    Continue,
    /// No more messages will arrive
    Drained,
    /// Waiting for more messages
    Pending,
}

/// Egress stream is the egress side of the message fabric. The stream is driven externally
/// (currently tonic/hyper) to stream messages to a peer. In the normal case, it populates the
/// appropriate headers by efficiently fetching latest metadata versions. It also provides a
/// mechanism to externally drain or force-terminate.
///
// todo: make pub(crate) after its usage in tests is removed
pub struct EgressStream {
    state: State,
    /// requests or unary messages are written to this channel
    bounded: Option<mpsc::Receiver<EgressMessage>>,
    /// responses to rpcs or handshake and other control signals are written to this channel
    unbounded: Option<mpsc::UnboundedReceiver<EgressMessage>>,
    /// The sole purpose of this channel, is to force drop the egress channel even if the inner stream's
    /// didn't wake us up. For instance, if there is no available space on the socket's sendbuf and
    /// we still want to drop this stream. We'll signal this by dropping the receiver.
    /// This results a wake-up to drop all buffered messages and releasing the inner
    /// stream.
    drop_notification: Option<oneshot::Sender<Infallible>>,
    metadata_cache: MetadataVersionCache,
    context_propagator: TraceContextPropagator,
    /// Did we send out a RequestStreamDrained signal to peer or not?
    sent_request_stream_drained: bool,
    /// Did we send out a ResponseStreamDrained signal to peer or not?
    sent_response_stream_drained: bool,
}

impl EgressStream {
    pub fn create(
        capacity: usize,
    ) -> (EgressSender, UnboundedEgressSender, Self, DropEgressStream) {
        let (unbounded_tx, unbounded) = mpsc::unbounded_channel();
        let (tx, rx) = mpsc::channel(capacity);
        let (drop_tx, drop_rx) = oneshot::channel();
        (
            EgressSender::new(tx),
            UnboundedEgressSender::new(unbounded_tx),
            Self {
                state: State::default(),
                bounded: Some(rx),
                unbounded: Some(unbounded),
                drop_notification: Some(drop_tx),
                metadata_cache: MetadataVersionCache::new(),
                context_propagator: Default::default(),
                sent_request_stream_drained: false,
                sent_response_stream_drained: false,
            },
            DropEgressStream(drop_rx),
        )
    }

    fn terminate_stream(&mut self) {
        self.bounded.take();
        self.unbounded.take();
        self.drop_notification.take();
        self.state = State::Closed;
    }

    fn stop_external_senders(&mut self) {
        self.state = State::Draining;
    }

    fn fill_header(&mut self, header: &mut Header, span: Option<Span>) {
        header.my_nodes_config_version = Some(
            self.metadata_cache
                .nodes_config
                .live_load()
                .version()
                .into(),
        );
        header.my_schema_version = Some(self.metadata_cache.schema.live_load().version().into());
        header.my_partition_table_version = Some(
            self.metadata_cache
                .partition_table
                .live_load()
                .version()
                .into(),
        );
        header.my_logs_version = Some(
            self.metadata_cache
                .logs_metadata
                .live_load()
                .version()
                .into(),
        );
        if let Some(span) = span {
            let context = span.context();
            let mut span_context = SpanContext::default();
            self.context_propagator
                .inject_context(&context, &mut span_context);
            header.span_context = Some(span_context);
        }
    }

    fn make_drained_message(&mut self, signal: message::Signal) -> Message {
        let mut header = Header::default();
        let control = ConnectionControl {
            signal: signal.into(),
            message: String::new(),
        };
        self.fill_header(&mut header, None);
        Message {
            header: Some(header),
            body: Some(control.into()),
        }
    }

    fn handle_message(
        &mut self,
        entry: Poll<Option<EgressMessage>>,
        drain_message_sent: bool,
    ) -> Decision {
        match entry {
            #[cfg(any(test, feature = "test-util"))]
            Poll::Ready(Some(EgressMessage::RawMessage(msg))) => Decision::Ready(msg),
            Poll::Ready(Some(EgressMessage::Message(mut header, body, span))) => {
                self.fill_header(&mut header, span);
                let msg = Message {
                    header: Some(header),
                    body: Some(body),
                };
                Decision::Ready(msg)
            }
            Poll::Ready(Some(EgressMessage::WithNotifer(body, span, notifier))) => {
                let mut header = Header::default();
                self.fill_header(&mut header, span);
                let msg = Message {
                    header: Some(header),
                    body: Some(body),
                };
                notifier.notify();
                Decision::Ready(msg)
            }
            Poll::Ready(Some(EgressMessage::Close(reason)))
                if matches!(self.state, State::Open) =>
            {
                // No new messages can be enqueued, and we'll continue to drain
                // already enqueued messages.
                self.stop_external_senders();
                let mut header = Header::default();
                self.fill_header(&mut header, None);
                let msg = Message {
                    header: Some(header),
                    body: Some(reason.into()),
                };
                Decision::StartDrain(msg)
            }
            // we are already draining, don't send another reason
            Poll::Ready(Some(EgressMessage::Close(_))) => Decision::Continue,
            Poll::Ready(None) if !drain_message_sent => Decision::SendDrainedMessage,
            Poll::Ready(None) => Decision::Drained,
            Poll::Pending => Decision::Pending,
        }
    }
}

impl Stream for EgressStream {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let State::Closed = self.state {
            return Poll::Ready(None);
        }

        match self.drop_notification {
            // This is a terminated stream already
            None => {
                self.terminate_stream();
                return Poll::Ready(None);
            }
            Some(ref mut drop_notification) => {
                match drop_notification.poll_closed(cx) {
                    Poll::Ready(()) => {
                        // drop has been requested, we'll immediately drop the inner sender and
                        // terminate this stream
                        self.terminate_stream();
                        return Poll::Ready(None);
                    }
                    // fall-through to read the inner stream
                    Poll::Pending => {}
                }
            }
        }

        // Unbounded channel is the priority [this might change in the future]
        // this means that rpc responses and control messages will take precedence vs. sending
        // requests or unary messages:
        // - Reduce tail latency for sending responses
        // - Allows the unbounded channel to be drained faster
        // - Allows us to react to drain signals quickly
        // - downside is potential starvation of the bounded stream.
        let unbounded_finished = loop {
            let drain_message_sent = self.sent_response_stream_drained;
            match self.unbounded.take() {
                Some(mut inner) => {
                    match self.handle_message(inner.poll_recv(cx), drain_message_sent) {
                        Decision::Ready(msg) => {
                            self.unbounded = Some(inner);
                            return Poll::Ready(Some(msg));
                        }
                        Decision::Continue => {
                            self.unbounded = Some(inner);
                            continue;
                        }
                        Decision::StartDrain(msg) => {
                            self.unbounded = Some(inner);
                            // drain only closes the request stream (bounded receiver) and doesn't
                            // impact the unbounded stream.
                            if let Some(stream) = self.bounded.as_mut() {
                                stream.close()
                            }
                            return Poll::Ready(Some(msg));
                        }
                        Decision::SendDrainedMessage => {
                            let msg =
                                self.make_drained_message(message::Signal::ResponseStreamDrained);
                            self.sent_response_stream_drained = true;
                            return Poll::Ready(Some(msg));
                        }
                        Decision::Pending => {
                            self.unbounded = Some(inner);
                            break false;
                        }
                        Decision::Drained => break true,
                    }
                }
                // channel was closed and drained
                None => {
                    break true;
                }
            }
        };

        // bounded channel
        let bounded_finished = loop {
            let drain_message_sent = self.sent_request_stream_drained;
            match self.bounded.take() {
                Some(mut inner) => {
                    match self.handle_message(inner.poll_recv(cx), drain_message_sent) {
                        Decision::Ready(msg) => {
                            self.bounded = Some(inner);
                            return Poll::Ready(Some(msg));
                        }
                        Decision::Continue => {
                            self.bounded = Some(inner);
                            continue;
                        }
                        Decision::StartDrain(msg) => {
                            inner.close();
                            self.bounded = Some(inner);
                            return Poll::Ready(Some(msg));
                        }
                        Decision::SendDrainedMessage => {
                            let msg =
                                self.make_drained_message(message::Signal::RequestStreamDrained);
                            self.sent_request_stream_drained = true;
                            return Poll::Ready(Some(msg));
                        }
                        Decision::Pending => {
                            self.bounded = Some(inner);
                            break false;
                        }
                        Decision::Drained => break true,
                    }
                }
                // channel was closed and drained
                None => {
                    break true;
                }
            }
        };

        if bounded_finished && unbounded_finished {
            self.terminate_stream();
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let bounded_len = self.bounded.as_ref().map(|inner| inner.len()).unwrap_or(0);
        let unbounded_len = self
            .unbounded
            .as_ref()
            .map(|inner| inner.len())
            .unwrap_or(0);
        (bounded_len + unbounded_len, None)
    }
}
