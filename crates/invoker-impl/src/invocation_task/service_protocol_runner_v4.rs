// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use futures::{Stream, StreamExt};
use gardal::futures::StreamExt as GardalStreamExt;
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body::Frame;
use opentelemetry::trace::TraceFlags;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use restate_errors::warn_it;
use restate_invoker_api::JournalMetadata;
use restate_invoker_api::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderTransaction, JournalEntry, JournalKind,
};
use restate_memory::{BudgetLease, DirectionalBudget};
use restate_service_client::{Endpoint, Method, Parts, Request};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_service_protocol_v4::message_codec::{
    Decoder, Encoder, Message, MessageHeader, MessageType, StateEntry, proto,
};
use restate_types::errors::InvocationError;
use restate_types::identifiers::InvocationId;
use restate_types::identifiers::ServiceId;
use restate_types::invocation::{
    Header, InvocationTarget, InvocationTargetType, ServiceInvocationSpanContext, ServiceType,
    SpanRelation,
};
use restate_types::journal;
use restate_types::journal_v2::command::{
    CallCommand, CallRequest, InputCommand, OneWayCallCommand,
};
use restate_types::journal_v2::raw::{RawCommand, RawEntry, RawNotification};
use restate_types::journal_v2::{
    CommandIndex, CommandType, Entry, EntryType, NotificationId, RunCompletion, RunResult, SignalId,
};
use restate_types::schema::deployment::{Deployment, DeploymentType, ProtocolType};
use restate_types::schema::invocation_target::{DeploymentStatus, InvocationTargetResolver};
use restate_types::service_protocol::ServiceProtocolVersion;

use crate::Notification;
use crate::error::{
    CommandPreconditionError, InvocationErrorRelatedCommandV2, InvokerError, SdkInvocationErrorV2,
};
use crate::invocation_task::{
    InvocationTask, InvocationTaskOutputInner, InvokerBody, InvokerBodySender, ResponseChunk,
    ResponseStream, TerminalLoopState, X_RESTATE_SERVER, invocation_id_to_header_value,
    service_protocol_version_to_header_value,
};

///  Provides the value of the invocation id
const INVOCATION_ID_HEADER_NAME: HeaderName = HeaderName::from_static("x-restate-invocation-id");

const GATEWAY_ERRORS_CODES: [StatusCode; 3] = [
    StatusCode::BAD_GATEWAY,
    StatusCode::SERVICE_UNAVAILABLE,
    StatusCode::GATEWAY_TIMEOUT,
];

/// Runs the interaction between the server and the service endpoint.
pub struct ServiceProtocolRunner<'a, EE, Schemas> {
    invocation_task: &'a mut InvocationTask<EE, Schemas>,

    service_protocol_version: ServiceProtocolVersion,

    // Encoder/Decoder
    encoder: Encoder,

    // task state
    command_index: CommandIndex,
}

impl<'a, EE, Schemas> ServiceProtocolRunner<'a, EE, Schemas>
where
    Schemas: InvocationTargetResolver,
{
    pub fn new(
        invocation_task: &'a mut InvocationTask<EE, Schemas>,
        service_protocol_version: ServiceProtocolVersion,
    ) -> Self {
        let encoder = Encoder::new(service_protocol_version);

        Self {
            invocation_task,
            service_protocol_version,
            encoder,
            command_index: 0,
        }
    }

    /// How often to release excess outbound budget capacity during the bidi-stream phase.
    const BUDGET_RELEASE_INTERVAL: Duration = Duration::from_secs(5);

    /// Run the service protocol interaction.
    ///
    /// # Arguments
    /// * `keyed_service_id` - If `Some`, eager state loading is enabled and we'll read/send
    ///   state for this service upfront. If `None`, lazy state is used (either because this
    ///   isn't a keyed service, or lazy state is enabled, or eager state is disabled).
    #[allow(clippy::too_many_arguments)]
    pub async fn run<Txn, IR>(
        mut self,
        txn: Txn,
        journal_metadata: JournalMetadata,
        keyed_service_id: Option<ServiceId>,
        deployment: Deployment,
        invocation_reader: IR,
        outbound_budget: &mut DirectionalBudget,
        inbound_budget: &mut DirectionalBudget,
    ) -> TerminalLoopState<()>
    where
        Txn: InvocationReaderTransaction,
        IR: InvocationReader,
    {
        // Figure out the protocol type. Force RequestResponse if inactivity_timeout is zero
        let protocol_type = if self.invocation_task.inactivity_timeout.is_zero() {
            ProtocolType::RequestResponse
        } else {
            deployment.ty.protocol_type()
        };

        // Close the invoker_rx in case it's request response, this avoids further buffering of messages in this channel.
        if protocol_type == ProtocolType::RequestResponse {
            self.invocation_task.invoker_rx.close();
        }

        let path: PathAndQuery = format!(
            "/invoke/{}/{}",
            self.invocation_task.invocation_target.service_name(),
            self.invocation_task.invocation_target.handler_name()
        )
        .try_into()
        .expect("must be able to build a valid invocation path");

        let journal_size = journal_metadata.length;

        debug!(
            restate.invocation.id = %self.invocation_task.invocation_id,
            deployment.address = %deployment.address_display(),
            deployment.service_protocol_version = %self.service_protocol_version.as_repr(),
            path = %path,
            "Executing invocation at deployment"
        );

        // Create an arc of the parent SpanContext.
        // We send this with every journal entry to correctly link new spans generated from journal entries.
        let service_invocation_span_context = journal_metadata.span_context;

        // Prepare the request
        let (mut http_stream_tx, request) = Self::prepare_request(
            path,
            deployment,
            self.service_protocol_version,
            &self.invocation_task.invocation_id,
            &service_invocation_span_context,
        );

        // Initialize the response stream state
        let http_stream_rx = ResponseStream::initialize(&self.invocation_task.client, request);

        let mut decoder_stream = std::pin::pin!(
            DecoderStream::new(
                http_stream_rx,
                self.service_protocol_version,
                self.invocation_task.message_size_warning,
                self.invocation_task.message_size_limit,
            )
            .throttle(self.invocation_task.action_token_bucket.take())
        );

        // === Replay phase (transaction alive) ===
        {
            // Read state if needed (state is collected for the START message).
            // Budget-gated: each state entry acquires a lease from the outbound
            // budget. The per-entry leases are merged into a single lease that
            // accompanies the start message frame.
            let state = if let Some(ref service_id) = keyed_service_id {
                Some(crate::shortcircuit!(
                    txn.read_state_budgeted(service_id, outbound_budget)
                        .map_err(|e| InvokerError::StateReader(e.into()))
                ))
            } else {
                None
            };

            // Send start message with state (leases are merged inside write_start)
            crate::shortcircuit!(
                self.write_start(
                    &mut http_stream_tx,
                    journal_size,
                    state,
                    self.invocation_task.retry_count_since_last_stored_entry,
                    journal_metadata.last_modification_date.elapsed(),
                    journal_metadata.random_seed
                )
                .await
            );

            // Read journal stream from storage and execute the replay.
            // Budget-gated: each entry acquires a lease before it's sent.
            let journal_stream = crate::shortcircuit!(
                txn.read_journal_budgeted(
                    &self.invocation_task.invocation_id,
                    journal_size,
                    journal_metadata.journal_kind,
                    outbound_budget,
                )
                .map_err(|e| InvokerError::JournalReader(e.into()))
            );
            crate::shortcircuit!(
                self.replay_loop(
                    &mut http_stream_tx,
                    &mut decoder_stream,
                    journal_stream,
                    journal_metadata.length
                )
                .await
            );
        }
        // === End replay phase - streams dropped, transaction can be dropped ===

        // Transaction dropped - RocksDB snapshot released!
        drop(txn);

        // Release excess local capacity accumulated during replay back to the
        // global pool before entering the bidi stream phase.
        outbound_budget.release_excess();

        // If we have the invoker_rx and the protocol type is bidi stream,
        // then we can use the bidi_stream loop reading the invoker_rx and the http_stream_rx
        if protocol_type == ProtocolType::BidiStream {
            trace!("Protocol is in bidi stream mode, will now start the send/receive loop");
            crate::shortcircuit!(
                self.bidi_stream_loop(
                    &service_invocation_span_context,
                    http_stream_tx,
                    &mut decoder_stream,
                    invocation_reader,
                    journal_metadata.journal_kind,
                    outbound_budget,
                    inbound_budget,
                )
                .await
            );
        } else {
            trace!("Protocol is in bidi stream mode, will now drop the sender side of the request");
            // Drop the http_stream_tx.
            // This is required in HTTP/1.1 to let the deployment send the headers back
            drop(http_stream_tx)
        }

        // We don't have the invoker_rx, so we simply consume the response
        trace!("Sender side of the request has been dropped, now processing the response");
        let result = self
            .response_stream_loop(
                &service_invocation_span_context,
                &mut decoder_stream,
                inbound_budget,
            )
            .await;

        // Sanity check of the stream decoder
        if decoder_stream.inner().has_remaining() {
            warn_it!(
                InvokerError::WriteAfterEndOfStream,
                "The read buffer is non empty after the stream has been closed."
            );
        }

        result
    }

    fn prepare_request(
        path: PathAndQuery,
        deployment_metadata: Deployment,
        service_protocol_version: ServiceProtocolVersion,
        invocation_id: &InvocationId,
        parent_span_context: &ServiceInvocationSpanContext,
    ) -> (InvokerBodySender, Request<InvokerBody>) {
        // Use an unbounded channel: backpressure is provided by the memory budget
        // (each frame carries an optional BudgetLease) rather than channel capacity.
        let (http_stream_tx, http_stream_rx) = mpsc::unbounded_channel();
        let req_body = InvokerBody::new(http_stream_rx);

        let service_protocol_header_value =
            service_protocol_version_to_header_value(service_protocol_version);

        let invocation_id_header_value = invocation_id_to_header_value(invocation_id);

        let mut headers = HeaderMap::from_iter([
            (
                http::header::CONTENT_TYPE,
                service_protocol_header_value.clone(),
            ),
            (http::header::ACCEPT, service_protocol_header_value),
            (INVOCATION_ID_HEADER_NAME, invocation_id_header_value),
        ]);

        // Inject OpenTelemetry context into the headers
        // The parent span as seen by the SDK will be the service invocation span context
        // which is emitted at INFO level representing the invocation, *not* the DEBUG level
        // `invoker_invocation_task` which wraps this code. This is so that headers will be sent
        // when in INFO level, not just in DEBUG level.
        {
            let span_context = parent_span_context.span_context();
            if span_context.is_valid() {
                const SUPPORTED_VERSION: u8 = 0;
                let header_value = format!(
                    "{:02x}-{}-{}-{:02x}",
                    SUPPORTED_VERSION,
                    span_context.trace_id(),
                    span_context.span_id(),
                    span_context.trace_flags() & TraceFlags::SAMPLED
                );
                if let Ok(header_value) = HeaderValue::try_from(header_value) {
                    headers.insert("traceparent", header_value);
                }
                if let Ok(tracestate) =
                    HeaderValue::from_str(span_context.trace_state().header().as_ref())
                {
                    headers.insert("tracestate", tracestate);
                }
            }
        }

        let address = match deployment_metadata.ty {
            DeploymentType::Lambda {
                arn,
                assume_role_arn,
                compression,
            } => Endpoint::Lambda(arn, assume_role_arn, compression),
            DeploymentType::Http {
                address,
                http_version,
                ..
            } => Endpoint::Http(address, Some(http_version)),
        };

        headers.extend(deployment_metadata.additional_headers);

        (
            http_stream_tx,
            Request::new(Parts::new(Method::POST, address, path, headers), req_body),
        )
    }

    // --- Loops

    /// This loop concurrently pushes journal entries and waits for the response headers and end of replay.
    async fn replay_loop<JournalStream, S, E>(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        http_stream_rx: &mut S,
        journal_stream: JournalStream,
        expected_entries_count: u32,
    ) -> TerminalLoopState<()>
    where
        JournalStream: Stream<Item = Result<(JournalEntry, BudgetLease), E>> + Unpin,
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut journal_stream = journal_stream.fuse();
        let mut got_headers = false;
        let mut sent_entries = 0;

        loop {
            tokio::select! {
                got_headers_res = http_stream_rx.next(), if !got_headers => {
                    // The reason we want to poll headers in this function is
                    // to exit early in case an error is returned during replays.
                    got_headers = true;
                    match crate::shortcircuit!(got_headers_res.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2::unknown()))
                        },
                        Some(DecoderStreamItem::Parts(headers)) => {
                            crate::shortcircuit!(self.handle_response_headers(headers));
                        }
                        Some(DecoderStreamItem::Message(..)) => {
                            panic!("Unexpected poll after the headers have been resolved already")
                        }
                    }
                },
                opt_je = journal_stream.next() => {
                    match opt_je {
                        Some(Ok((JournalEntry::JournalV2(entry), lease))) => {
                            sent_entries += 1;
                            crate::shortcircuit!(self.write_entry_with_lease(http_stream_tx, entry.inner, Some(lease)));
                        }
                        Some(Ok((JournalEntry::JournalV1(old_entry), lease))) => {
                            sent_entries += 1;
                            if let journal::Entry::Input(input_entry) = crate::shortcircuit!(old_entry.deserialize_entry::<ProtobufRawEntryCodec>()) {
                                crate::shortcircuit!(self.write_entry_with_lease(
                                    http_stream_tx,
                                    Entry::Command(InputCommand {
                                        headers: input_entry.headers,
                                        payload: input_entry.value,
                                        name: Default::default()
                                    }.into()).encode::<ServiceProtocolV4Codec>(),
                                    Some(lease),
                                ));
                            } else {
                                panic!("This is unexpected, when an entry is stored with journal v1, only input entry is allowed!")
                            }
                        },
                        Some(Ok((JournalEntry::JournalV1Completion(_), _))) => {
                            panic!("Unexpected JournalV1Completion during replay: completion arrived before entry was stored")
                        }
                        Some(Err(e)) => {
                            return TerminalLoopState::Failed(InvokerError::JournalReader(e.into()));
                        }
                        None => {
                            // Let's verify if we sent all the entries we promised, otherwise the stream will hang in a bad way!
                            if sent_entries < expected_entries_count {
                                return TerminalLoopState::Failed(InvokerError::UnexpectedEntryCount {
                                    actual: sent_entries,
                                    expected: expected_entries_count,
                                })
                            }

                            // No need to wait for the headers to continue
                            trace!("Finished to replay the journal");
                            return TerminalLoopState::Continue(())
                        }
                    }
                }
            }
        }
    }

    /// This loop concurrently reads the http response stream and journal completions from the invoker.
    #[allow(clippy::too_many_arguments)]
    async fn bidi_stream_loop<S, IR>(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        mut http_stream_tx: InvokerBodySender,
        http_stream_rx: &mut S,
        mut invocation_reader: IR,
        journal_kind: JournalKind,
        outbound_budget: &mut DirectionalBudget,
        inbound_budget: &mut DirectionalBudget,
    ) -> TerminalLoopState<()>
    where
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
        IR: InvocationReader,
    {
        let mut release_interval = tokio::time::interval(Self::BUDGET_RELEASE_INTERVAL);
        release_interval.tick().await; // consume initial immediate tick
        loop {
            tokio::select! {
                opt_completion = self.invocation_task.invoker_rx.recv() => {
                    match opt_completion {
                        Some(Notification::Entry(entry_index)) => {
                            trace!(restate.journal.index = entry_index, "Reading entry from storage");
                            let (journal_entry, lease) = crate::shortcircuit!(
                                invocation_reader
                                    .read_journal_entry_budgeted(
                                        &self.invocation_task.invocation_id,
                                        entry_index,
                                        journal_kind,
                                        outbound_budget,
                                    )
                                    .await
                                    .map_err(|e| InvokerError::JournalReader(e.into()))
                                    .and_then(|opt| opt.ok_or_else(|| InvokerError::JournalReader(
                                        anyhow::anyhow!(
                                            "journal entry {entry_index} not found for notification read"
                                        ),
                                    )))
                            );
                            let raw_entry = match journal_entry {
                                JournalEntry::JournalV2(stored) => stored.inner,
                                other => {
                                    panic!("v4+ protocol runner expected JournalV2 entry but got {other:?}")
                                }
                            };
                            trace!("Sending the entry to the wire");
                            crate::shortcircuit!(self.write_entry_with_lease(&mut http_stream_tx, raw_entry, Some(lease)));
                        }
                        Some(Notification::Completion(_)) => {
                            panic!("We don't expect to receive Notification::Completion in v4+, this is an invoker bug.")
                        },
                        Some(Notification::Ack(entry_index)) => {
                            trace!("Sending the ack to the wire");
                            crate::shortcircuit!(self.write(&mut http_stream_tx, Message::new_command_ack(entry_index)));
                        },
                        None => {
                            // Completion channel is closed,
                            // the invoker main loop won't send completions anymore.
                            // Response stream might still be open though.
                            return TerminalLoopState::Continue(())
                        },
                    }
                },
                chunk = http_stream_rx.next() => {
                    match crate::shortcircuit!(chunk.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2::unknown()));
                        }
                        Some(DecoderStreamItem::Parts(parts)) => crate::shortcircuit!(self.handle_response_headers(parts)),
                        Some(DecoderStreamItem::Message(message_header, message, payload_size)) => {
                            // Acquire inbound budget for the decoded message's wire
                            // size. Will be moved into the http_stream_rx in a follow-up
                            // PR.
                            let lease = match inbound_budget.reserve(payload_size).await {
                                Ok(lease) => lease,
                                Err(_err) => return TerminalLoopState::Failed(InvokerError::InboundBudgetExhausted),
                            };
                            crate::shortcircuit!(self.handle_message(parent_span_context, message_header, message, Some(lease)));
                        }
                    }
                },
                _ = release_interval.tick() => {
                    outbound_budget.release_excess();
                    inbound_budget.release_excess();
                },
                _ = tokio::time::sleep(self.invocation_task.inactivity_timeout) => {
                    debug!("Inactivity detected, going to suspend invocation");
                    // Just return. This will drop the invoker_rx and http_stream_tx,
                    // closing the request stream and the invoker input channel.
                    return TerminalLoopState::Continue(())
                },
            }
        }
    }

    async fn response_stream_loop<S>(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        http_stream_rx: &mut S,
        inbound_budget: &mut DirectionalBudget,
    ) -> TerminalLoopState<()>
    where
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
    {
        loop {
            tokio::select! {
                chunk = http_stream_rx.next() => {
                    // don't read again until all buffered messages has been consumed
                    // to force a back pressure on the read stream

                    match crate::shortcircuit!(chunk.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2::unknown()));
                        }
                        Some(DecoderStreamItem::Parts(parts)) => crate::shortcircuit!(self.handle_response_headers(parts)),
                        Some(DecoderStreamItem::Message(message_header, message, payload_size)) => {
                            // Acquire inbound budget for the decoded message's wire
                            // size. Will be moved into the http_stream_rx in a follow-up
                            // PR.
                            let lease = match inbound_budget.reserve(payload_size).await {
                                Ok(lease) => lease,
                                Err(_err) => return TerminalLoopState::Failed(InvokerError::InboundBudgetExhausted),
                            };
                            crate::shortcircuit!(self.handle_message(parent_span_context, message_header, message, Some(lease)));
                        }
                    }
                },
                _ = tokio::time::sleep(self.invocation_task.abort_timeout) => {
                    warn!("Inactivity detected, going to close invocation");
                    return TerminalLoopState::Failed(InvokerError::AbortTimeoutFired(self.invocation_task.abort_timeout.into()))
                },
            }
        }
    }

    // --- Read and write methods

    async fn write_start<S, E>(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        journal_size: u32,
        state: Option<EagerState<S>>,
        retry_count_since_last_stored_entry: u32,
        duration_since_last_stored_entry: Duration,
        random_seed: u64,
    ) -> Result<(), InvokerError>
    where
        S: Stream<Item = Result<((Bytes, Bytes), BudgetLease), E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Collect state if present, mapping to StateEntry while collecting.
        // Per-entry budget leases are merged into a single combined lease that
        // accompanies the start message frame.
        let (partial_state, state_map, state_lease) = if let Some(state) = state {
            let is_partial = state.is_partial();
            let mut merged_lease: Option<BudgetLease> = None;
            let mut entries = Vec::new();
            let mut stream = std::pin::pin!(state.into_inner());
            while let Some(result) = stream.next().await {
                let ((key, value), lease) =
                    result.map_err(|e| InvokerError::StateReader(e.into()))?;
                entries.push(StateEntry { key, value });
                match &mut merged_lease {
                    Some(existing) => existing.merge(lease),
                    None => merged_lease = Some(lease),
                }
            }
            (is_partial, entries, merged_lease)
        } else {
            (true, Vec::new(), None)
        };

        // Send the invoke frame with the merged state lease
        self.write_with_lease(
            http_stream_tx,
            Message::new_start_message(
                Bytes::copy_from_slice(&self.invocation_task.invocation_id.to_bytes()),
                self.invocation_task.invocation_id.to_string(),
                self.invocation_task
                    .invocation_target
                    .key()
                    .map(|bs| bs.as_bytes().clone()),
                journal_size,
                partial_state,
                state_map,
                retry_count_since_last_stored_entry,
                duration_since_last_stored_entry,
                random_seed,
            ),
            state_lease,
        )
    }

    fn write_entry_with_lease(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        entry: RawEntry,
        lease: Option<BudgetLease>,
    ) -> Result<(), InvokerError> {
        // TODO(slinkydeveloper) could this code be improved a tad bit more introducing something to our magic macro in message_codec?
        match entry {
            RawEntry::Command(cmd) => {
                self.write_raw_with_lease(
                    http_stream_tx,
                    cmd.command_type().into(),
                    cmd.into_serialized_content(),
                    lease,
                )?;
                self.command_index += 1;
            }
            RawEntry::Notification(notif) => {
                self.write_raw_with_lease(
                    http_stream_tx,
                    notif.ty().into(),
                    notif.into_serialized_content(),
                    lease,
                )?;
            }
        }
        Ok(())
    }

    fn write(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        msg: Message,
    ) -> Result<(), InvokerError> {
        self.write_with_lease(http_stream_tx, msg, None)
    }

    fn write_with_lease(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        msg: Message,
        lease: Option<BudgetLease>,
    ) -> Result<(), InvokerError> {
        trace!(restate.protocol.message = ?msg, "Sending message");
        let buf = self.encoder.encode(msg);

        if http_stream_tx.send((Frame::data(buf), lease)).is_err() {
            return Err(InvokerError::UnexpectedClosedRequestStream);
        };
        Ok(())
    }

    fn write_raw_with_lease(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        ty: MessageType,
        buf: Bytes,
        lease: Option<BudgetLease>,
    ) -> Result<(), InvokerError> {
        trace!(restate.protocol.message = ?ty, "Sending message");
        let buf = self.encoder.encode_raw(ty, buf);

        if http_stream_tx.send((Frame::data(buf), lease)).is_err() {
            return Err(InvokerError::UnexpectedClosedRequestStream);
        };
        Ok(())
    }

    fn handle_response_headers(
        &mut self,
        mut parts: http::response::Parts,
    ) -> Result<(), InvokerError> {
        // if service is running behind a gateway, the service can be down
        // but we still get a response code from the gateway itself. In that
        // case we still need to return the proper error
        if GATEWAY_ERRORS_CODES.contains(&parts.status) {
            return Err(InvokerError::ServiceUnavailable(parts.status));
        }

        // otherwise we return generic UnexpectedResponse
        if !parts.status.is_success() {
            // Decorate the error in case of UNSUPPORTED_MEDIA_TYPE, as it probably is the incompatible protocol version
            if parts.status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
                return Err(InvokerError::BadNegotiatedServiceProtocolVersion(
                    self.service_protocol_version,
                ));
            }
            if parts.status == StatusCode::PAYLOAD_TOO_LARGE {
                return Err(InvokerError::ContentTooLarge);
            }

            return Err(InvokerError::UnexpectedResponse(parts.status));
        }

        let content_type = parts.headers.remove(http::header::CONTENT_TYPE);
        let expected_content_type =
            service_protocol_version_to_header_value(self.service_protocol_version);
        match content_type {
            Some(ct) =>
            {
                #[allow(clippy::borrow_interior_mutable_const)]
                if ct != expected_content_type {
                    return Err(InvokerError::UnexpectedContentType(
                        Some(ct),
                        expected_content_type,
                    ));
                }
            }
            None => {
                return Err(InvokerError::UnexpectedContentType(
                    None,
                    expected_content_type,
                ));
            }
        }

        if let Some(hv) = parts.headers.remove(X_RESTATE_SERVER) {
            self.invocation_task.send_invoker_tx(
                InvocationTaskOutputInner::ServerHeaderReceived(
                    hv.to_str()
                        .map_err(|e| InvokerError::BadHeader(X_RESTATE_SERVER, e))?
                        .to_owned(),
                ),
                None,
            )
        }

        Ok(())
    }

    fn handle_new_command(
        &mut self,
        mh: MessageHeader,
        command: RawCommand,
        inbound_lease: Option<BudgetLease>,
    ) {
        self.invocation_task.send_invoker_tx(
            InvocationTaskOutputInner::NewCommand {
                command_index: self.command_index,
                requires_ack: mh
                    .requires_ack()
                    .expect("All command messages support requires_ack"),
                command,
            },
            inbound_lease,
        );
        self.command_index += 1;
    }

    fn handle_message(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        mh: MessageHeader,
        message: Message,
        inbound_lease: Option<BudgetLease>,
    ) -> TerminalLoopState<()> {
        trace!(
            restate.protocol.message_header = ?mh,
            restate.protocol.message = ?message.proto_debug(),
            "Received message"
        );
        match message {
            // Control messages — lease is dropped (no data to track)
            Message::Start { .. } => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessageV4(MessageType::Start))
            }
            Message::CommandAck(_) => TerminalLoopState::Failed(InvokerError::UnexpectedMessageV4(
                MessageType::CommandAck,
            )),
            Message::Suspension(suspension) => self.handle_suspension_message(suspension),
            Message::Error(e) => self.handle_error_message(e),
            Message::End(_) => TerminalLoopState::Closed,

            // Run completion proposal
            Message::ProposeRunCompletion(run_completion) => {
                let notification: Entry = RunCompletion {
                    completion_id: run_completion.result_completion_id,
                    result: match crate::shortcircuit!(
                        run_completion
                            .result
                            .ok_or(InvokerError::MalformedProposeRunCompletion)
                    ) {
                        proto::propose_run_completion_message::Result::Value(b) => {
                            RunResult::Success(b)
                        }
                        proto::propose_run_completion_message::Result::Failure(f) => {
                            RunResult::Failure(f.into())
                        }
                    },
                }
                .into();

                let raw_notification: RawNotification = notification
                    .encode::<ServiceProtocolV4Codec>()
                    .try_into()
                    .expect("a raw notification");

                self.invocation_task.send_invoker_tx(
                    InvocationTaskOutputInner::NewNotificationProposal {
                        notification: raw_notification,
                    },
                    inbound_lease,
                );

                TerminalLoopState::Continue(())
            }

            // Commands — inbound_lease is forwarded to track memory through the channel
            Message::OutputCommand(cmd) => {
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::Output, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::InputCommand(cmd) => {
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::Input, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetInvocationOutputCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(
                    RawCommand::new(CommandType::GetInvocationOutput, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>()
                );
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetInvocationOutput, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::AttachInvocationCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(
                    RawCommand::new(CommandType::AttachInvocation, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>()
                );
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::AttachInvocation, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::RunCommand(cmd) => {
                self.handle_new_command(mh, RawCommand::new(CommandType::Run, cmd), inbound_lease);
                TerminalLoopState::Continue(())
            }
            Message::SendSignalCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(
                    RawCommand::new(CommandType::SendSignal, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>()
                );
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::SendSignal, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::OneWayCallCommand(cmd) => {
                let entry: Entry = OneWayCallCommand {
                    request: crate::shortcircuit!(
                        resolve_call_request(
                            self.invocation_task.schemas.live_load(),
                            InvokeRequest {
                                service_name: cmd.service_name.into(),
                                handler_name: cmd.handler_name.into(),
                                parameter: cmd.parameter,
                                headers: cmd.headers.into_iter().map(Into::into).collect(),
                                key: cmd.key.into(),
                                idempotency_key: cmd.idempotency_key.map(|s| s.into()),
                                span_relation: parent_span_context.as_linked()
                            }
                        )
                        .map_err(|e| InvokerError::CommandPrecondition(
                            self.command_index,
                            EntryType::Command(CommandType::OneWayCall),
                            e
                        ))
                    ),
                    invoke_time: cmd.invoke_time.into(),
                    invocation_id_completion_id: cmd.invocation_id_notification_idx,
                    name: cmd.name.into(),
                }
                .into();
                self.handle_new_command(
                    mh,
                    entry
                        .encode::<ServiceProtocolV4Codec>()
                        .try_into()
                        .expect("a raw command"),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::CallCommand(cmd) => {
                let entry: Entry = CallCommand {
                    request: crate::shortcircuit!(
                        resolve_call_request(
                            self.invocation_task.schemas.live_load(),
                            InvokeRequest {
                                service_name: cmd.service_name.into(),
                                handler_name: cmd.handler_name.into(),
                                parameter: cmd.parameter,
                                headers: cmd.headers.into_iter().map(Into::into).collect(),
                                key: cmd.key.into(),
                                idempotency_key: cmd.idempotency_key.map(|s| s.into()),
                                span_relation: parent_span_context.as_parent()
                            }
                        )
                        .map_err(|e| InvokerError::CommandPrecondition(
                            self.command_index,
                            EntryType::Command(CommandType::Call),
                            e
                        ))
                    ),
                    invocation_id_completion_id: cmd.invocation_id_notification_idx,
                    result_completion_id: cmd.result_completion_id,
                    name: cmd.name.into(),
                }
                .into();
                self.handle_new_command(
                    mh,
                    entry
                        .encode::<ServiceProtocolV4Codec>()
                        .try_into()
                        .expect("a raw command"),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::SleepCommand(cmd) => {
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::Sleep, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::CompletePromiseCommand(cmd) => {
                crate::shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::CompletePromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::CompletePromise, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::PeekPromiseCommand(cmd) => {
                crate::shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::PeekPromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::PeekPromise, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetPromiseCommand(cmd) => {
                crate::shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::GetPromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetPromise, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetEagerStateKeysCommand(cmd) => {
                crate::shortcircuit!(can_read_state(
                    self.command_index,
                    &EntryType::Command(CommandType::GetEagerStateKeys),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetEagerStateKeys, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetEagerStateCommand(cmd) => {
                crate::shortcircuit!(can_read_state(
                    self.command_index,
                    &EntryType::Command(CommandType::GetEagerState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetEagerState, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetLazyStateKeysCommand(cmd) => {
                crate::shortcircuit!(can_read_state(
                    self.command_index,
                    &EntryType::Command(CommandType::GetLazyStateKeys),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetLazyStateKeys, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::ClearAllStateCommand(cmd) => {
                crate::shortcircuit!(can_write_state(
                    self.command_index,
                    &EntryType::Command(CommandType::ClearAllState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::ClearAllState, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::ClearStateCommand(cmd) => {
                crate::shortcircuit!(can_write_state(
                    self.command_index,
                    &EntryType::Command(CommandType::ClearState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::ClearState, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::SetStateCommand(cmd) => {
                crate::shortcircuit!(can_write_state(
                    self.command_index,
                    &EntryType::Command(CommandType::SetState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::SetState, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetLazyStateCommand(cmd) => {
                crate::shortcircuit!(can_read_state(
                    self.command_index,
                    &EntryType::Command(CommandType::GetLazyState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetLazyState, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::CompleteAwakeableCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(
                    RawCommand::new(CommandType::CompleteAwakeable, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>()
                );
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::CompleteAwakeable, cmd),
                    inbound_lease,
                );
                TerminalLoopState::Continue(())
            }
            Message::SignalNotification(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::SignalNotification),
            ),
            Message::GetInvocationOutputCompletionNotification(_) => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessageV4(
                    MessageType::GetInvocationOutputCompletionNotification,
                ))
            }
            Message::AttachInvocationCompletionNotification(_) => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessageV4(
                    MessageType::AttachInvocationCompletionNotification,
                ))
            }
            Message::RunCompletionNotification(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::RunCompletionNotification),
            ),
            Message::CallCompletionNotification(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::CallCompletionNotification),
            ),
            Message::CallInvocationIdCompletionNotification(_) => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessageV4(
                    MessageType::CallInvocationIdCompletionNotification,
                ))
            }
            Message::SleepCompletionNotification(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::SleepCompletionNotification),
            ),
            Message::CompletePromiseCompletionNotification(_) => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessageV4(
                    MessageType::CompletePromiseCompletionNotification,
                ))
            }
            Message::PeekPromiseCompletionNotification(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::PeekPromiseCompletionNotification),
            ),
            Message::GetPromiseCompletionNotification(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::GetPromiseCompletionNotification),
            ),
            Message::GetLazyStateKeysCompletionNotification(_) => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessageV4(
                    MessageType::GetLazyStateKeysCompletionNotification,
                ))
            }
            Message::GetLazyStateCompletionNotification(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::GetLazyStateCompletionNotification),
            ),
            Message::Custom(_, _) => {
                unimplemented!()
            }
        }
    }

    fn handle_suspension_message(
        &mut self,
        suspension: proto::SuspensionMessage,
    ) -> TerminalLoopState<()> {
        let suspension_indexes: HashSet<_> = suspension
            .waiting_completions
            .into_iter()
            .map(NotificationId::for_completion)
            .chain(
                suspension
                    .waiting_signals
                    .into_iter()
                    .map(SignalId::for_index)
                    .map(NotificationId::for_signal),
            )
            .chain(
                suspension
                    .waiting_named_signals
                    .into_iter()
                    .map(|s| SignalId::for_name(s.into()))
                    .map(NotificationId::for_signal),
            )
            .collect();
        // We currently don't support empty suspension_indexes set
        if suspension_indexes.is_empty() {
            return TerminalLoopState::Failed(InvokerError::EmptySuspensionMessage);
        }
        TerminalLoopState::SuspendedV2(suspension_indexes)
    }

    fn handle_error_message(&mut self, error: proto::ErrorMessage) -> TerminalLoopState<()> {
        TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2 {
            related_command: Some(InvocationErrorRelatedCommandV2 {
                related_command_index: error.related_command_index,
                related_command_name: error.related_command_name.clone(),
                related_entry_type: error
                    .related_command_type
                    .and_then(|t| u16::try_from(t).ok())
                    .and_then(|idx| MessageType::try_from(idx).ok())
                    .and_then(|mt| mt.entry_type()),
                command_was_committed: error
                    .related_command_index
                    .is_some_and(|entry_idx| entry_idx < self.command_index),
            }),
            next_retry_interval_override: error.next_retry_delay.map(Duration::from_millis),
            error: InvocationError::from(error).into(),
        }))
    }
}

pub struct InvokeRequest {
    service_name: ByteString,
    handler_name: ByteString,
    headers: Vec<Header>,
    /// Empty if service call.
    /// The reason this is not Option<ByteString> is that it cannot be distinguished purely from the message
    /// whether the key is none or empty.
    key: ByteString,
    idempotency_key: Option<ByteString>,
    span_relation: SpanRelation,
    parameter: Bytes,
}

fn resolve_call_request(
    invocation_target_resolver: &impl InvocationTargetResolver,
    request: InvokeRequest,
) -> Result<CallRequest, CommandPreconditionError> {
    let meta = invocation_target_resolver
        .resolve_latest_invocation_target(&request.service_name, &request.handler_name)
        .ok_or_else(|| {
            CommandPreconditionError::ServiceHandlerNotFound(
                request.service_name.to_string(),
                request.handler_name.to_string(),
            )
        })?;

    if let DeploymentStatus::Deprecated(dp_id) = meta.deployment_status {
        return Err(CommandPreconditionError::DeploymentDeprecated(
            request.service_name.to_string(),
            dp_id,
        ));
    }

    if !request.key.is_empty() && !meta.target_ty.is_keyed() {
        return Err(CommandPreconditionError::UnexpectedKey(
            request.service_name.to_string(),
        ));
    }

    let invocation_target = match meta.target_ty {
        InvocationTargetType::Service => {
            InvocationTarget::service(request.service_name, request.handler_name)
        }
        InvocationTargetType::VirtualObject(h_ty) => InvocationTarget::virtual_object(
            request.service_name.clone(),
            ByteString::try_from(request.key.clone().into_bytes())
                .map_err(CommandPreconditionError::BadRequestKey)?,
            request.handler_name,
            h_ty,
        ),
        InvocationTargetType::Workflow(h_ty) => InvocationTarget::workflow(
            request.service_name.clone(),
            ByteString::try_from(request.key.clone().into_bytes())
                .map_err(CommandPreconditionError::BadRequestKey)?,
            request.handler_name,
            h_ty,
        ),
    };

    let idempotency_key = if let Some(idempotency_key) = &request.idempotency_key {
        if idempotency_key.is_empty() {
            return Err(CommandPreconditionError::EmptyIdempotencyKey);
        }
        Some(idempotency_key.deref())
    } else {
        None
    };
    let invocation_retention = meta.compute_retention(idempotency_key.is_some());
    let invocation_id = InvocationId::generate(&invocation_target, idempotency_key);

    // Create the span context
    let span_context = ServiceInvocationSpanContext::start(&invocation_id, request.span_relation);

    Ok(CallRequest {
        invocation_id,
        invocation_target,
        span_context,
        parameter: request.parameter,
        headers: request.headers,
        idempotency_key: request.idempotency_key,
        completion_retention_duration: invocation_retention.completion_retention,
        journal_retention_duration: invocation_retention.journal_retention,
    })
}

#[inline]
fn check_workflow_type(
    command_index: CommandIndex,
    entry_type: &EntryType,
    service_type: &ServiceType,
) -> Result<(), InvokerError> {
    if *service_type != ServiceType::Workflow {
        return Err(InvokerError::CommandPrecondition(
            command_index,
            *entry_type,
            CommandPreconditionError::NoWorkflowOperations,
        ));
    }
    Ok(())
}

#[inline]
fn can_read_state(
    command_index: CommandIndex,
    entry_type: &EntryType,
    invocation_target_type: &InvocationTargetType,
) -> Result<(), InvokerError> {
    if !invocation_target_type.can_read_state() {
        return Err(InvokerError::CommandPrecondition(
            command_index,
            *entry_type,
            CommandPreconditionError::NoStateOperations,
        ));
    }
    Ok(())
}

#[inline]
fn can_write_state(
    command_index: CommandIndex,
    entry_type: &EntryType,
    invocation_target_type: &InvocationTargetType,
) -> Result<(), InvokerError> {
    can_read_state(command_index, entry_type, invocation_target_type)?;
    if !invocation_target_type.can_write_state() {
        return Err(InvokerError::CommandPrecondition(
            command_index,
            *entry_type,
            CommandPreconditionError::NoWriteStateOperations,
        ));
    }
    Ok(())
}

enum DecoderStreamItem {
    Message(MessageHeader, Message, usize),
    Parts(http::response::Parts),
}

pin_project_lite::pin_project! {
    struct DecoderStream<S> {
        #[pin]
        inner: S,
        decoder: Decoder,
    }
}

impl<S> DecoderStream<S> {
    fn new(
        inner: S,
        service_protocol_version: ServiceProtocolVersion,
        message_size_warning: NonZeroUsize,
        message_size_limit: NonZeroUsize,
    ) -> Self {
        Self {
            inner,
            decoder: Decoder::new(
                service_protocol_version,
                message_size_warning,
                message_size_limit,
            ),
        }
    }

    fn has_remaining(&self) -> bool {
        self.decoder.has_remaining()
    }
}

impl<S> Stream for DecoderStream<S>
where
    S: Stream<Item = Result<ResponseChunk, InvokerError>> + Unpin,
{
    type Item = Result<DecoderStreamItem, InvokerError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.decoder.consume_next() {
                Ok(Some((frame_header, frame, payload_size))) => {
                    return Poll::Ready(Some(Ok(DecoderStreamItem::Message(
                        frame_header,
                        frame,
                        payload_size,
                    ))));
                }
                Ok(None) => match ready!(this.inner.as_mut().poll_next(cx)) {
                    Some(Ok(chunk)) => match chunk {
                        ResponseChunk::Parts(parts) => {
                            return Poll::Ready(Some(Ok(DecoderStreamItem::Parts(parts))));
                        }
                        ResponseChunk::Data(buf) => {
                            this.decoder.push(buf);
                        }
                    },
                    Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    None => return Poll::Ready(None),
                },
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            }
        }
    }
}
