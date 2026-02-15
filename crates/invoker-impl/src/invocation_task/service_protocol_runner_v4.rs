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
use futures::{Stream, StreamExt, TryStreamExt};
use gardal::futures::StreamExt as GardalStreamExt;
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body::Frame;
use opentelemetry::trace::TraceFlags;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace, warn};

use restate_errors::warn_it;
use restate_invoker_api::JournalMetadata;
use restate_invoker_api::invocation_reader::{
    EagerState, InvocationReaderTransaction, JournalEntry,
};
use restate_memory::MemoryLease;
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
    InvocationTask, InvocationTaskOutputInner, InvokerBodyStream, InvokerRequestStreamSender,
    ResponseChunk, ResponseStream, TerminalLoopState, X_RESTATE_SERVER,
    invocation_id_to_header_value, service_protocol_version_to_header_value,
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

    /// Inbound memory lease set before calling handle_message.
    /// Consumed by handle_new_command or ProposeRunCompletion.
    pending_inbound_lease: Option<MemoryLease>,
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
            pending_inbound_lease: None,
        }
    }

    /// Run the service protocol interaction.
    ///
    /// # Arguments
    /// * `keyed_service_id` - If `Some`, eager state loading is enabled and we'll read/send
    ///   state for this service upfront. If `None`, lazy state is used (either because this
    ///   isn't a keyed service, or lazy state is enabled, or eager state is disabled).
    pub async fn run<Txn>(
        mut self,
        txn: Txn,
        journal_metadata: JournalMetadata,
        keyed_service_id: Option<ServiceId>,
        deployment: Deployment,
    ) -> TerminalLoopState<()>
    where
        Txn: InvocationReaderTransaction,
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
            // Read state if needed (state is collected for the START message)
            let state = if let Some(ref service_id) = keyed_service_id {
                Some(crate::shortcircuit!(
                    txn.read_state(service_id)
                        .map_err(|e| InvokerError::StateReader(e.into()))
                ))
            } else {
                None
            };

            // Send start message with state
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

            // Read journal stream from storage
            let journal_stream = crate::shortcircuit!(
                txn.read_journal(
                    &self.invocation_task.invocation_id,
                    journal_size,
                    journal_metadata.using_journal_table_v2,
                )
                .map_err(|e| InvokerError::JournalReader(e.into()))
            );
            // Execute the replay
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

        // If we have the invoker_rx and the protocol type is bidi stream,
        // then we can use the bidi_stream loop reading the invoker_rx and the http_stream_rx
        if protocol_type == ProtocolType::BidiStream {
            trace!("Protocol is in bidi stream mode, will now start the send/receive loop");
            crate::shortcircuit!(
                self.bidi_stream_loop(
                    &service_invocation_span_context,
                    http_stream_tx,
                    &mut decoder_stream
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
            .response_stream_loop(&service_invocation_span_context, &mut decoder_stream)
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
    ) -> (InvokerRequestStreamSender, Request<InvokerBodyStream>) {
        // Make this channel a rendezvous channel to avoid unnecessary buffering between the service
        // protocol runner and the underlying hyper HTTP client. This helps with keeping the overall
        // memory consumption per invocation in check.
        let (http_stream_tx, http_stream_rx) = mpsc::channel(1);
        let req_body = InvokerBodyStream::new(ReceiverStream::new(http_stream_rx));

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
        http_stream_tx: &mut InvokerRequestStreamSender,
        http_stream_rx: &mut S,
        journal_stream: JournalStream,
        expected_entries_count: u32,
    ) -> TerminalLoopState<()>
    where
        JournalStream: Stream<Item = Result<JournalEntry, E>> + Unpin,
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
                        Some(DecoderStreamItem::Message(_, _)) => {
                            panic!("Unexpected poll after the headers have been resolved already")
                        }
                    }
                },
                opt_je = journal_stream.next() => {
                    match opt_je {
                        Some(Ok(JournalEntry::JournalV2(entry))) => {
                            sent_entries += 1;
                            crate::shortcircuit!(self.write_entry(http_stream_tx, entry.inner).await);
                        }
                        Some(Ok(JournalEntry::JournalV1(old_entry))) => {
                            sent_entries += 1;
                            if let journal::Entry::Input(input_entry) = crate::shortcircuit!(old_entry.deserialize_entry::<ProtobufRawEntryCodec>()) {
                                crate::shortcircuit!(self.write_entry(
                                    http_stream_tx,
                                    Entry::Command(InputCommand {
                                        headers: input_entry.headers,
                                        payload: input_entry.value,
                                        name: Default::default()
                                    }.into()).encode::<ServiceProtocolV4Codec>()
                                ).await);
                            } else {
                                panic!("This is unexpected, when an entry is stored with journal v1, only input entry is allowed!")
                            }
                        },
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
    async fn bidi_stream_loop<S>(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        mut http_stream_tx: InvokerRequestStreamSender,
        http_stream_rx: &mut S,
    ) -> TerminalLoopState<()>
    where
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
    {
        // Pending inbound message waiting for a memory lease from the inbound pool
        let mut pending_inbound: Option<(MessageHeader, Message, usize)> = None;

        loop {
            tokio::select! {
                opt_completion = self.invocation_task.invoker_rx.recv() => {
                    match opt_completion {
                        Some(Notification::Entry(entry)) => {
                            trace!("Sending the entry to the wire");
                            crate::shortcircuit!(self.write_entry(&mut http_stream_tx, entry).await);
                        }
                        Some(Notification::Completion(_)) => {
                            panic!("We don't expect to receive Notification::Completion, this is an invoker bug.")
                        },
                        Some(Notification::Ack(entry_index)) => {
                            trace!("Sending the ack to the wire");
                            crate::shortcircuit!(self.write(&mut http_stream_tx, Message::new_command_ack(entry_index)).await);
                        },
                        None => {
                            // Completion channel is closed,
                            // the invoker main loop won't send completions anymore.
                            // Response stream might still be open though.
                            return TerminalLoopState::Continue(())
                        },
                    }
                },
                // Read from HTTP/decoder only when no pending inbound message
                chunk = http_stream_rx.next(), if pending_inbound.is_none() => {
                    match crate::shortcircuit!(chunk.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2::unknown()));
                        }
                        Some(DecoderStreamItem::Parts(parts)) => crate::shortcircuit!(self.handle_response_headers(parts)),
                        Some(DecoderStreamItem::Message(message_header, message)) => {
                            let body_size = message_header.frame_length() as usize;
                            match self.invocation_task.try_acquire_inbound(body_size) {
                                Ok(lease) => {
                                    self.pending_inbound_lease = Some(lease);
                                    crate::shortcircuit!(self.handle_message(parent_span_context, message_header, message));
                                }
                                Err(need_size) => {
                                    pending_inbound = Some((message_header, message, need_size));
                                }
                            }
                        }
                    }
                },
                // Acquire inbound lease for stashed message
                lease = self.invocation_task.inbound_pool.reserve(
                    pending_inbound.as_ref().map_or(0, |p| p.2)
                ), if pending_inbound.is_some() => {
                    let (mh, msg, _) = pending_inbound.take().unwrap();
                    self.pending_inbound_lease = Some(lease);
                    crate::shortcircuit!(self.handle_message(parent_span_context, mh, msg));
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
    ) -> TerminalLoopState<()>
    where
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
    {
        let mut pending_inbound: Option<(MessageHeader, Message, usize)> = None;

        loop {
            tokio::select! {
                chunk = http_stream_rx.next(), if pending_inbound.is_none() => {
                    match crate::shortcircuit!(chunk.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2::unknown()));
                        }
                        Some(DecoderStreamItem::Parts(parts)) => crate::shortcircuit!(self.handle_response_headers(parts)),
                        Some(DecoderStreamItem::Message(message_header, message)) => {
                            let body_size = message_header.frame_length() as usize;
                            match self.invocation_task.try_acquire_inbound(body_size) {
                                Ok(lease) => {
                                    self.pending_inbound_lease = Some(lease);
                                    crate::shortcircuit!(self.handle_message(parent_span_context, message_header, message));
                                }
                                Err(need_size) => {
                                    pending_inbound = Some((message_header, message, need_size));
                                }
                            }
                        }
                    }
                },
                lease = self.invocation_task.inbound_pool.reserve(
                    pending_inbound.as_ref().map_or(0, |p| p.2)
                ), if pending_inbound.is_some() => {
                    let (mh, msg, _) = pending_inbound.take().unwrap();
                    self.pending_inbound_lease = Some(lease);
                    crate::shortcircuit!(self.handle_message(parent_span_context, mh, msg));
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
        http_stream_tx: &mut InvokerRequestStreamSender,
        journal_size: u32,
        state: Option<EagerState<S>>,
        retry_count_since_last_stored_entry: u32,
        duration_since_last_stored_entry: Duration,
        random_seed: u64,
    ) -> Result<(), InvokerError>
    where
        S: Stream<Item = Result<(Bytes, Bytes), E>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Collect state if present, mapping to StateEntry while collecting
        let (partial_state, state_map) = if let Some(state) = state {
            let is_partial = state.is_partial();
            let entries: Vec<StateEntry> = state
                .into_inner()
                .map_ok(|(key, value)| StateEntry { key, value })
                .try_collect()
                .await
                .map_err(|e| InvokerError::StateReader(e.into()))?;
            (is_partial, entries)
        } else {
            (true, Vec::new())
        };

        // Send the invoke frame
        self.write(
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
        )
        .await
    }

    async fn write_entry(
        &mut self,
        http_stream_tx: &mut InvokerRequestStreamSender,
        entry: RawEntry,
    ) -> Result<(), InvokerError> {
        // TODO(slinkydeveloper) could this code be improved a tad bit more introducing something to our magic macro in message_codec?
        match entry {
            RawEntry::Command(cmd) => {
                self.write_raw(
                    http_stream_tx,
                    cmd.command_type().into(),
                    cmd.into_serialized_content(),
                )
                .await?;
                self.command_index += 1;
            }
            RawEntry::Notification(notif) => {
                self.write_raw(
                    http_stream_tx,
                    notif.ty().into(),
                    notif.into_serialized_content(),
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn write(
        &mut self,
        http_stream_tx: &mut InvokerRequestStreamSender,
        msg: Message,
    ) -> Result<(), InvokerError> {
        trace!(restate.protocol.message = ?msg, "Sending message");
        let buf = self.encoder.encode(msg);

        if http_stream_tx.send(Ok(Frame::data(buf))).await.is_err() {
            return Err(InvokerError::UnexpectedClosedRequestStream);
        };
        Ok(())
    }

    async fn write_raw(
        &mut self,
        http_stream_tx: &mut InvokerRequestStreamSender,
        ty: MessageType,
        buf: Bytes,
    ) -> Result<(), InvokerError> {
        trace!(restate.protocol.message = ?ty, "Sending message");
        let buf = self.encoder.encode_raw(ty, buf);

        if http_stream_tx.send(Ok(Frame::data(buf))).await.is_err() {
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
            self.invocation_task
                .send_invoker_tx(InvocationTaskOutputInner::ServerHeaderReceived(
                    hv.to_str()
                        .map_err(|e| InvokerError::BadHeader(X_RESTATE_SERVER, e))?
                        .to_owned(),
                ))
        }

        Ok(())
    }

    fn handle_new_command(&mut self, mh: MessageHeader, command: RawCommand) {
        let lease = self
            .pending_inbound_lease
            .take()
            .unwrap_or_else(MemoryLease::unlinked);
        self.invocation_task.send_invoker_tx_with_lease(
            InvocationTaskOutputInner::NewCommand {
                command_index: self.command_index,
                requires_ack: mh
                    .requires_ack()
                    .expect("All command messages support requires_ack"),
                command,
            },
            lease,
        );
        self.command_index += 1;
    }

    fn handle_message(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        mh: MessageHeader,
        message: Message,
    ) -> TerminalLoopState<()> {
        trace!(
            restate.protocol.message_header = ?mh,
            restate.protocol.message = ?message.proto_debug(),
            "Received message"
        );
        match message {
            // Control messages
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

                let lease = self
                    .pending_inbound_lease
                    .take()
                    .unwrap_or_else(MemoryLease::unlinked);
                self.invocation_task.send_invoker_tx_with_lease(
                    InvocationTaskOutputInner::NewNotificationProposal {
                        notification: raw_notification,
                    },
                    lease,
                );

                TerminalLoopState::Continue(())
            }

            // Commands
            Message::OutputCommand(cmd) => {
                self.handle_new_command(mh, RawCommand::new(CommandType::Output, cmd));
                TerminalLoopState::Continue(())
            }
            Message::InputCommand(cmd) => {
                self.handle_new_command(mh, RawCommand::new(CommandType::Input, cmd));
                TerminalLoopState::Continue(())
            }
            Message::GetInvocationOutputCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(
                    RawCommand::new(CommandType::GetInvocationOutput, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>()
                );
                self.handle_new_command(mh, RawCommand::new(CommandType::GetInvocationOutput, cmd));
                TerminalLoopState::Continue(())
            }
            Message::AttachInvocationCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(
                    RawCommand::new(CommandType::AttachInvocation, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>()
                );
                self.handle_new_command(mh, RawCommand::new(CommandType::AttachInvocation, cmd));
                TerminalLoopState::Continue(())
            }
            Message::RunCommand(cmd) => {
                self.handle_new_command(mh, RawCommand::new(CommandType::Run, cmd));
                TerminalLoopState::Continue(())
            }
            Message::SendSignalCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(
                    RawCommand::new(CommandType::SendSignal, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>()
                );
                self.handle_new_command(mh, RawCommand::new(CommandType::SendSignal, cmd));
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
                );
                TerminalLoopState::Continue(())
            }
            Message::SleepCommand(cmd) => {
                self.handle_new_command(mh, RawCommand::new(CommandType::Sleep, cmd));
                TerminalLoopState::Continue(())
            }
            Message::CompletePromiseCommand(cmd) => {
                crate::shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::CompletePromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(mh, RawCommand::new(CommandType::CompletePromise, cmd));
                TerminalLoopState::Continue(())
            }
            Message::PeekPromiseCommand(cmd) => {
                crate::shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::PeekPromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(mh, RawCommand::new(CommandType::PeekPromise, cmd));
                TerminalLoopState::Continue(())
            }
            Message::GetPromiseCommand(cmd) => {
                crate::shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::GetPromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(mh, RawCommand::new(CommandType::GetPromise, cmd));
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
                self.handle_new_command(mh, RawCommand::new(CommandType::GetEagerStateKeys, cmd));
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
                self.handle_new_command(mh, RawCommand::new(CommandType::GetEagerState, cmd));
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
                self.handle_new_command(mh, RawCommand::new(CommandType::GetLazyStateKeys, cmd));
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
                self.handle_new_command(mh, RawCommand::new(CommandType::ClearAllState, cmd));
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
                self.handle_new_command(mh, RawCommand::new(CommandType::ClearState, cmd));
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
                self.handle_new_command(mh, RawCommand::new(CommandType::SetState, cmd));
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
                self.handle_new_command(mh, RawCommand::new(CommandType::GetLazyState, cmd));
                TerminalLoopState::Continue(())
            }
            Message::CompleteAwakeableCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(
                    RawCommand::new(CommandType::CompleteAwakeable, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>()
                );
                self.handle_new_command(mh, RawCommand::new(CommandType::CompleteAwakeable, cmd));
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
    Message(MessageHeader, Message),
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
                Ok(Some((frame_header, frame))) => {
                    return Poll::Ready(Some(Ok(DecoderStreamItem::Message(frame_header, frame))));
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
