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
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body::Frame;
use opentelemetry::trace::TraceFlags;
use prost::Message;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use restate_errors::warn_it;
use restate_invoker_api::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderTransaction, JournalEntry, JournalKind,
};
use restate_invoker_api::{EntryEnricher, JournalMetadata};
use restate_memory::{BudgetLease, DirectionalBudget};
use restate_service_client::{Endpoint, Method, Parts, Request};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol::message::{
    Decoder, Encoder, MessageHeader, MessageType, ProtocolMessage, StateEntry,
};
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_types::errors::InvocationError;
use restate_types::identifiers::ServiceId;
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::invocation::ServiceInvocationSpanContext;
use restate_types::journal::raw::RawEntryCodec;
use restate_types::journal::{Completion, CompletionResult, EntryType};
use restate_types::journal_v2;
use restate_types::journal_v2::EntryMetadata;
use restate_types::schema::deployment::{Deployment, DeploymentType, ProtocolType};
use restate_types::service_protocol::ServiceProtocolVersion;

use crate::Notification;
use crate::error::{InvocationErrorRelatedEntry, InvokerError, SdkInvocationError};
use crate::invocation_task::{
    InvocationTask, InvocationTaskOutputInner, InvokerBody, InvokerBodySender, ResponseChunk,
    ResponseStream, TerminalLoopState, X_RESTATE_SERVER, invocation_id_to_header_value,
    service_protocol_version_to_header_value,
};

///  Provides the value of the invocation id
const INVOCATION_ID_HEADER_NAME: HeaderName = HeaderName::from_static("x-restate-invocation-id");

const GATEWAY_ERRORS_CODES: [http::StatusCode; 3] = [
    http::StatusCode::BAD_GATEWAY,
    http::StatusCode::SERVICE_UNAVAILABLE,
    http::StatusCode::GATEWAY_TIMEOUT,
];

/// Runs the interaction between the server and the service endpoint.
pub struct ServiceProtocolRunner<'a, EE, DMR> {
    invocation_task: &'a mut InvocationTask<EE, DMR>,

    service_protocol_version: ServiceProtocolVersion,

    // Encoder/Decoder
    encoder: Encoder,
    decoder: Decoder,

    // task state
    next_journal_index: EntryIndex,

    /// Cumulative inbound budget lease. Chunks are merged in as raw HTTP data
    /// arrives; wire_size portions are split off per decoded message and sent
    /// through the invoker channel with the corresponding output.
    cumulative_inbound_lease: Option<BudgetLease>,
}

impl<'a, EE, DMR> ServiceProtocolRunner<'a, EE, DMR>
where
    EE: EntryEnricher,
{
    pub fn new(
        invocation_task: &'a mut InvocationTask<EE, DMR>,
        service_protocol_version: ServiceProtocolVersion,
    ) -> Self {
        let encoder = Encoder::new(service_protocol_version);
        let decoder = Decoder::new(
            service_protocol_version,
            invocation_task.message_size_warning,
            invocation_task.message_size_limit,
        );

        Self {
            invocation_task,
            service_protocol_version,
            encoder,
            decoder,
            next_journal_index: 0,
            cumulative_inbound_lease: None,
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
        let mut http_stream_rx = ResponseStream::initialize(&self.invocation_task.client, request);

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
                    journal_metadata.last_modification_date.elapsed()
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
                self.replay_loop(&mut http_stream_tx, &mut http_stream_rx, journal_stream)
                    .await
            );
        }
        // === End replay phase - streams dropped, transaction can be dropped ===

        // Transaction dropped - RocksDB snapshot released!
        drop(txn);

        // Check all the entries have been replayed
        debug_assert_eq!(self.next_journal_index, journal_size);

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
                    &mut http_stream_rx,
                    invocation_reader,
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
                &mut http_stream_rx,
                inbound_budget,
            )
            .await;

        // Sanity check of the stream decoder
        if self.decoder.has_remaining() {
            warn_it!(
                InvokerError::WriteAfterEndOfStream,
                "The read buffer is non empty after the stream has been closed."
            );
        }

        result
    }

    fn prepare_request(
        path: PathAndQuery,
        deployment: Deployment,
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

        let address = match deployment.ty {
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

        headers.extend(deployment.additional_headers);

        (
            http_stream_tx,
            Request::new(Parts::new(Method::POST, address, path, headers), req_body),
        )
    }

    // --- Loops

    /// This loop concurrently pushes journal entries and waits for the response headers and end of replay.
    async fn replay_loop<JournalStream, E>(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        http_stream_rx: &mut ResponseStream,
        journal_stream: JournalStream,
    ) -> TerminalLoopState<()>
    where
        JournalStream: Stream<Item = Result<(JournalEntry, BudgetLease), E>> + Unpin,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut journal_stream = journal_stream.fuse();
        let mut got_headers = false;
        loop {
            tokio::select! {
                got_headers_res = http_stream_rx.next(), if !got_headers => {
                    got_headers = true;
                    // The reason we want to poll headers in this function is
                    // to exit early in case an error is returned during replays.
                    match crate::shortcircuit!(got_headers_res.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::Sdk(SdkInvocationError::unknown()));
                        }
                        Some(ResponseChunk::Parts(headers)) => {
                            crate::shortcircuit!(self.handle_response_headers(headers));
                        }
                        Some(ResponseChunk::Data(_)) => {
                            panic!("Unexpected poll after the headers have been resolved already")
                        }
                    };

                },
                opt_je = journal_stream.next() => {
                    match opt_je {
                        Some(Ok((JournalEntry::JournalV1(je), lease))) => {
                            crate::shortcircuit!(self.write_with_lease(http_stream_tx, ProtocolMessage::UnparsedEntry(je), Some(lease)));
                            self.next_journal_index += 1;
                        },
                        Some(Ok((JournalEntry::JournalV2(re), lease))) => {
                            if re.ty() == journal_v2::EntryType::Command(journal_v2::CommandType::Input) {
                                let input_entry = crate::shortcircuit!(re.decode::<ServiceProtocolV4Codec, journal_v2::command::InputCommand>());
                                  crate::shortcircuit!(self.write_with_lease(http_stream_tx, ProtocolMessage::UnparsedEntry(
                                    ProtobufRawEntryCodec::serialize_as_input_entry(
                                        input_entry.headers,
                                        input_entry.payload
                                    ).erase_enrichment()
                                ), Some(lease)));
                            self.next_journal_index += 1;
                            } else {
                                panic!("This is unexpected, when an entry is stored with journal v2, only input entry is allowed!")
                            }
                        }
                        Some(Ok((JournalEntry::JournalV1Completion(_), _))) => {
                            // During replay, a JournalV1Completion means the completion
                            // arrived before the entry itself. This entry cannot be replayed
                            // to the SDK because we don't have the original entry bytes.
                            // This should not happen in normal operation since entries are
                            // always stored before completions during replay.
                            panic!("Unexpected JournalV1Completion during replay: completion arrived before entry was stored")
                        }
                        Some(Err(e)) => {
                            return TerminalLoopState::Failed(InvokerError::JournalReader(e.into()));
                        }
                        None => {
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
    async fn bidi_stream_loop<IR>(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        mut http_stream_tx: InvokerBodySender,
        http_stream_rx: &mut ResponseStream,
        mut invocation_reader: IR,
        outbound_budget: &mut DirectionalBudget,
        inbound_budget: &mut DirectionalBudget,
    ) -> TerminalLoopState<()>
    where
        IR: InvocationReader,
    {
        let mut release_interval = tokio::time::interval(Self::BUDGET_RELEASE_INTERVAL);
        release_interval.tick().await; // consume initial immediate tick
        loop {
            tokio::select! {
                opt_completion = self.invocation_task.invoker_rx.recv() => {
                    match opt_completion {
                        Some(Notification::Completion(entry_index)) => {
                            trace!(restate.journal.index = entry_index, "Reading completion from storage");
                            let (completion, lease) = crate::shortcircuit!(
                                read_completion_from_storage_budgeted(
                                    &mut invocation_reader,
                                    &self.invocation_task.invocation_id,
                                    entry_index,
                                    outbound_budget,
                                ).await
                            );
                            trace!("Sending the completion to the wire");
                            crate::shortcircuit!(self.write_with_lease(&mut http_stream_tx, completion.into(), Some(lease)));
                        },
                        Some(Notification::Ack(entry_index)) => {
                            trace!("Sending the ack to the wire");
                            crate::shortcircuit!(self.write(&mut http_stream_tx, ProtocolMessage::new_entry_ack(entry_index)));
                        },
                        Some(Notification::Entry { .. }) => {
                            panic!("We don't expect to receive journal_v2 entries, this is an invoker bug.")
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
                            return TerminalLoopState::Failed(InvokerError::Sdk(SdkInvocationError::unknown()));
                        }
                        Some(ResponseChunk::Parts(parts)) => crate::shortcircuit!(self.handle_response_headers(parts)),
                        Some(ResponseChunk::Data(buf)) => {
                            crate::shortcircuit!(self.handle_read(parent_span_context, buf, inbound_budget).await);
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

    async fn response_stream_loop(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        http_stream_rx: &mut ResponseStream,
        inbound_budget: &mut DirectionalBudget,
    ) -> TerminalLoopState<()> {
        loop {
            tokio::select! {
                chunk = http_stream_rx.next() => {
                    match crate::shortcircuit!(chunk.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::Sdk(SdkInvocationError::unknown()));
                        }
                        Some(ResponseChunk::Parts(parts)) => crate::shortcircuit!(self.handle_response_headers(parts)),
                        Some(ResponseChunk::Data(buf)) => {
                            crate::shortcircuit!(self.handle_read(parent_span_context, buf, inbound_budget).await);
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
            ProtocolMessage::new_start_message(
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
            ),
            state_lease,
        )
    }

    fn write(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        msg: ProtocolMessage,
    ) -> Result<(), InvokerError> {
        self.write_with_lease(http_stream_tx, msg, None)
    }

    fn write_with_lease(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        msg: ProtocolMessage,
        lease: Option<BudgetLease>,
    ) -> Result<(), InvokerError> {
        trace!(restate.protocol.message = ?msg, "Sending message");
        let buf = self.encoder.encode(msg);

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

    async fn handle_read(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        buf: Bytes,
        inbound_budget: &mut DirectionalBudget,
    ) -> TerminalLoopState<()> {
        // Acquire inbound budget for the raw chunk before pushing
        // to the decoder. On ShouldYield, abort the invocation.
        let chunk_lease = match inbound_budget.reserve(buf.len()).await {
            Ok(lease) => lease,
            Err(_should_yield) => {
                return TerminalLoopState::Failed(InvokerError::InboundBudgetExhausted);
            }
        };

        match self.cumulative_inbound_lease.as_mut() {
            Some(cumulative) => cumulative.merge(chunk_lease),
            None => self.cumulative_inbound_lease = Some(chunk_lease),
        }

        self.decoder.push(buf);

        while let Some((frame_header, frame, payload_size)) =
            crate::shortcircuit!(self.decoder.consume_next())
        {
            // Split off this message's payload_size from the cumulative inbound
            // lease.
            let msg_lease = self
                .cumulative_inbound_lease
                .as_mut()
                .map(|lease| lease.split(payload_size));
            crate::shortcircuit!(self.handle_message(
                parent_span_context,
                frame_header,
                frame,
                msg_lease,
            ));
        }

        TerminalLoopState::Continue(())
    }

    fn handle_message(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        mh: MessageHeader,
        message: ProtocolMessage,
        inbound_lease: Option<BudgetLease>,
    ) -> TerminalLoopState<()> {
        trace!(restate.protocol.message_header = ?mh, restate.protocol.message = ?message, "Received message");
        match message {
            ProtocolMessage::Start { .. } => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessage(MessageType::Start))
            }
            ProtocolMessage::Completion(_) => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessage(MessageType::Completion))
            }
            ProtocolMessage::EntryAck(_) => {
                TerminalLoopState::Failed(InvokerError::UnexpectedMessage(MessageType::EntryAck))
            }
            ProtocolMessage::Suspension(suspension) => {
                let suspension_indexes = HashSet::from_iter(suspension.entry_indexes);
                // We currently don't support empty suspension_indexes set
                if suspension_indexes.is_empty() {
                    return TerminalLoopState::Failed(InvokerError::EmptySuspensionMessage);
                }
                // Sanity check on the suspension indexes
                if *suspension_indexes.iter().max().unwrap() >= self.next_journal_index {
                    return TerminalLoopState::Failed(InvokerError::BadSuspensionMessage(
                        suspension_indexes,
                        self.next_journal_index,
                    ));
                }
                TerminalLoopState::Suspended(suspension_indexes)
            }
            ProtocolMessage::Error(e) => {
                TerminalLoopState::Failed(InvokerError::Sdk(SdkInvocationError {
                    related_entry: Some(InvocationErrorRelatedEntry {
                        related_entry_index: e.related_entry_index,
                        related_entry_name: e.related_entry_name.clone(),
                        related_entry_type: e
                            .related_entry_type
                            .and_then(|t| u16::try_from(t).ok())
                            .and_then(|idx| MessageType::try_from(idx).ok())
                            .and_then(|mt| EntryType::try_from(mt).ok()),
                        entry_was_committed: e
                            .related_entry_index
                            .is_some_and(|entry_idx| entry_idx < self.next_journal_index),
                    }),
                    next_retry_interval_override: e.next_retry_delay.map(Duration::from_millis),
                    error: InvocationError::from(e).into(),
                }))
            }
            ProtocolMessage::End(_) => TerminalLoopState::Closed,
            ProtocolMessage::UnparsedEntry(entry) => {
                let entry_type = entry.header().as_entry_type();
                let enriched_entry = crate::shortcircuit!(
                    self.invocation_task
                        .entry_enricher
                        .enrich_entry(
                            entry,
                            &self.invocation_task.invocation_target,
                            parent_span_context
                        )
                        .map_err(|e| InvokerError::EntryEnrichment(
                            self.next_journal_index,
                            entry_type,
                            e
                        ))
                );
                self.invocation_task.send_invoker_tx(
                    InvocationTaskOutputInner::NewEntry {
                        entry_index: self.next_journal_index,
                        entry: enriched_entry.into(),
                        requires_ack: mh
                            .requires_ack()
                            .expect("All entry messages support requires_ack"),
                    },
                    inbound_lease,
                );
                self.next_journal_index += 1;
                TerminalLoopState::Continue(())
            }
        }
    }
}

/// Reads a v1 completion from storage with budget tracking.
///
/// Reads the entry and acquires a [`BudgetLease`] for its serialized size from
/// the outbound budget. Only used by the v1-v3 protocol runner.
async fn read_completion_from_storage_budgeted<IR: InvocationReader>(
    invocation_reader: &mut IR,
    invocation_id: &InvocationId,
    entry_index: EntryIndex,
    budget: &mut DirectionalBudget,
) -> Result<(Completion, BudgetLease), InvokerError> {
    let (entry, lease) = invocation_reader
        .read_journal_entry_budgeted(invocation_id, entry_index, JournalKind::V1, budget)
        .await
        .map_err(|e| InvokerError::JournalReader(e.into()))?
        .ok_or_else(|| {
            InvokerError::JournalReader(anyhow::anyhow!(
                "journal entry {entry_index} not found for completion read"
            ))
        })?;
    let completion = extract_completion(entry_index, entry)?;
    Ok((completion, lease))
}

/// Extracts a [`Completion`] from a journal entry read from storage.
fn extract_completion(
    entry_index: EntryIndex,
    journal_entry: JournalEntry,
) -> Result<Completion, InvokerError> {
    use restate_types::service_protocol;

    match journal_entry {
        JournalEntry::JournalV1(plain_raw_entry) => {
            let extractor = service_protocol::CompletionResultExtractor::decode(
                plain_raw_entry.serialized_entry().clone(),
            )
            .map_err(|e| {
                InvokerError::JournalReader(anyhow::anyhow!(
                    "failed to decode completion from entry {entry_index}: {e}"
                ))
            })?;

            let result = match extractor.result {
                Some(service_protocol::completion_result_extractor::Result::Empty(_)) => {
                    CompletionResult::Empty
                }
                Some(service_protocol::completion_result_extractor::Result::Value(b)) => {
                    CompletionResult::Success(b)
                }
                Some(service_protocol::completion_result_extractor::Result::Failure(f)) => {
                    CompletionResult::Failure(f.code.into(), f.message.into())
                }
                None => {
                    return Err(InvokerError::JournalReader(anyhow::anyhow!(
                        "journal entry {entry_index} has no completion result"
                    )));
                }
            };

            Ok(Completion::new(entry_index, result))
        }
        JournalEntry::JournalV1Completion(result) => Ok(Completion::new(entry_index, result)),
        JournalEntry::JournalV2(_) => {
            panic!("v1-v3 protocol runner should not encounter JournalV2 entries")
        }
    }
}
