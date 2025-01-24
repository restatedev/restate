// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::invocation_task::{
    invocation_id_to_header_value, service_protocol_version_to_header_value,
    InvocationErrorRelatedEntry, InvocationTask, InvocationTaskError, InvocationTaskOutputInner,
    InvokerBodyStream, InvokerRequestStreamSender, ResponseChunk, ResponseStreamState,
    TerminalLoopState, X_RESTATE_SERVER,
};
use crate::Notification;
use anyhow::anyhow;
use bytes::Bytes;
use bytestring::ByteString;
use futures::future::FusedFuture;
use futures::{FutureExt, Stream, StreamExt};
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use http_body::Frame;
use opentelemetry::trace::TraceFlags;
use restate_errors::warn_it;
use restate_invoker_api::journal_reader::JournalEntry;
use restate_invoker_api::{EagerState, JournalMetadata};
use restate_service_client::{Endpoint, Method, Parts, Request};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_service_protocol_v4::message_codec::{
    proto, Decoder, Encoder, Message, MessageHeader, MessageType,
};
use restate_types::errors::{codes, InvocationError};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{
    Header, InvocationTarget, InvocationTargetType, ServiceInvocationSpanContext, ServiceType,
    SpanRelation,
};
use restate_types::journal;
use restate_types::journal_v2::command::{
    CallCommand, CallRequest, InputCommand, OneWayCallCommand,
};
use restate_types::journal_v2::raw::{RawCommand, RawEntry, RawEntryInner, RawNotification};
use restate_types::journal_v2::{
    CommandIndex, CommandType, Entry, EntryType, NotificationId, RunCompletion, RunResult, SignalId,
};
use restate_types::schema::deployment::{
    Deployment, DeploymentMetadata, DeploymentType, ProtocolType,
};
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::service_protocol::ServiceProtocolVersion;
use std::collections::HashSet;
use std::future::poll_fn;
use std::ops::Deref;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, trace, warn};

///  Provides the value of the invocation id
const INVOCATION_ID_HEADER_NAME: HeaderName = HeaderName::from_static("x-restate-invocation-id");

const GATEWAY_ERRORS_CODES: [StatusCode; 3] = [
    StatusCode::BAD_GATEWAY,
    StatusCode::SERVICE_UNAVAILABLE,
    StatusCode::GATEWAY_TIMEOUT,
];

/// Runs the interaction between the server and the service endpoint.
pub struct ServiceProtocolRunner<'a, SR, JR, EE, Schemas> {
    invocation_task: &'a mut InvocationTask<SR, JR, EE, Schemas>,

    service_protocol_version: ServiceProtocolVersion,

    // Encoder/Decoder
    encoder: Encoder,
    decoder: Decoder,

    // task state
    command_index: CommandIndex,
}

impl<'a, SR, JR, EE, Schemas> ServiceProtocolRunner<'a, SR, JR, EE, Schemas>
where
    Schemas: InvocationTargetResolver,
{
    pub fn new(
        invocation_task: &'a mut InvocationTask<SR, JR, EE, Schemas>,
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
            command_index: 0,
        }
    }

    pub async fn run<JournalStream, StateIter>(
        mut self,
        journal_metadata: JournalMetadata,
        deployment: Deployment,
        journal_stream: JournalStream,
        state_iter: EagerState<StateIter>,
    ) -> TerminalLoopState<()>
    where
        JournalStream: Stream<Item = JournalEntry> + Unpin,
        StateIter: Iterator<Item = (Bytes, Bytes)>,
    {
        // Figure out the protocol type. Force RequestResponse if inactivity_timeout is zero
        let protocol_type = if self.invocation_task.inactivity_timeout.is_zero() {
            ProtocolType::RequestResponse
        } else {
            deployment.metadata.ty.protocol_type()
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

        info!(
            invocation.id = %self.invocation_task.invocation_id,
            deployment.address = %deployment.metadata.address_display(),
            deployment.service_protocol_version = %self.service_protocol_version.as_repr(),
            path = %path,
            "Executing invocation at deployment"
        );

        // Create an arc of the parent SpanContext.
        // We send this with every journal entry to correctly link new spans generated from journal entries.
        let service_invocation_span_context = journal_metadata.span_context;

        // Prepare the request and send start message
        let (mut http_stream_tx, request) = Self::prepare_request(
            path,
            deployment.metadata,
            self.service_protocol_version,
            &self.invocation_task.invocation_id,
            &service_invocation_span_context,
        );

        crate::shortcircuit!(
            self.write_start(
                &mut http_stream_tx,
                journal_size,
                state_iter,
                self.invocation_task.retry_count_since_last_stored_entry,
                journal_metadata.last_modification_date.elapsed()
            )
            .await
        );

        // Initialize the response stream state
        let mut http_stream_rx =
            ResponseStreamState::initialize(&self.invocation_task.client, request);

        // Execute the replay
        crate::shortcircuit!(
            self.replay_loop(&mut http_stream_tx, &mut http_stream_rx, journal_stream)
                .await
        );

        // If we have the invoker_rx and the protocol type is bidi stream,
        // then we can use the bidi_stream loop reading the invoker_rx and the http_stream_rx
        if protocol_type == ProtocolType::BidiStream {
            trace!("Protocol is in bidi stream mode, will now start the send/receive loop");
            crate::shortcircuit!(
                self.bidi_stream_loop(
                    &service_invocation_span_context,
                    http_stream_tx,
                    &mut http_stream_rx,
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
            .response_stream_loop(&service_invocation_span_context, &mut http_stream_rx)
            .await;

        // Sanity check of the stream decoder
        if self.decoder.has_remaining() {
            warn_it!(
                InvocationTaskError::WriteAfterEndOfStream,
                "The read buffer is non empty after the stream has been closed."
            );
        }

        result
    }

    fn prepare_request(
        path: PathAndQuery,
        deployment_metadata: DeploymentMetadata,
        service_protocol_version: ServiceProtocolVersion,
        invocation_id: &InvocationId,
        parent_span_context: &ServiceInvocationSpanContext,
    ) -> (InvokerRequestStreamSender, Request<InvokerBodyStream>) {
        // Just an arbitrary buffering size
        let (http_stream_tx, http_stream_rx) = mpsc::channel(10);
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
                if let Ok(tracestate) = HeaderValue::try_from(span_context.trace_state().header()) {
                    headers.insert("tracestate", tracestate);
                }
            }
        }

        let address = match deployment_metadata.ty {
            DeploymentType::Lambda {
                arn,
                assume_role_arn,
            } => Endpoint::Lambda(arn, assume_role_arn),
            DeploymentType::Http {
                address,
                http_version,
                ..
            } => Endpoint::Http(address, Some(http_version)),
        };

        headers.extend(deployment_metadata.delivery_options.additional_headers);

        (
            http_stream_tx,
            Request::new(Parts::new(Method::POST, address, path, headers), req_body),
        )
    }

    // --- Loops

    /// This loop concurrently pushes journal entries and waits for the response headers and end of replay.
    async fn replay_loop<JournalStream>(
        &mut self,
        http_stream_tx: &mut InvokerRequestStreamSender,
        http_stream_rx: &mut ResponseStreamState,
        journal_stream: JournalStream,
    ) -> TerminalLoopState<()>
    where
        JournalStream: Stream<Item = JournalEntry> + Unpin,
    {
        let mut journal_stream = journal_stream.fuse();
        let got_headers_future = poll_fn(|cx| http_stream_rx.poll_only_headers(cx)).fuse();
        tokio::pin!(got_headers_future);

        loop {
            tokio::select! {
                got_headers_res = got_headers_future.as_mut(), if !got_headers_future.is_terminated() => {
                    // The reason we want to poll headers in this function is
                    // to exit early in case an error is returned during replays.
                    let headers = crate::shortcircuit!(got_headers_res);
                    crate::shortcircuit!(self.handle_response_headers(headers));
                },
                opt_je = journal_stream.next() => {
                    match opt_je {
                        Some(JournalEntry::JournalV2(entry)) => {
                            crate::shortcircuit!(self.write_entry(http_stream_tx, entry).await);

                        }
                        Some(JournalEntry::JournalV1(old_entry)) => {
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
    async fn bidi_stream_loop(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        mut http_stream_tx: InvokerRequestStreamSender,
        http_stream_rx: &mut ResponseStreamState,
    ) -> TerminalLoopState<()> {
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
                chunk = poll_fn(|cx| http_stream_rx.poll_next_chunk(cx)) => {
                    match crate::shortcircuit!(chunk) {
                        ResponseChunk::Parts(parts) => crate::shortcircuit!(self.handle_response_headers(parts)),
                        ResponseChunk::Data(buf) => crate::shortcircuit!(self.handle_read(parent_span_context, buf)),
                        ResponseChunk::End => {
                            // Response stream was closed without SuspensionMessage, EndMessage or ErrorMessage
                            return TerminalLoopState::Failed(InvocationTaskError::ErrorMessageReceived {
                                related_entry: None,
                                next_retry_interval_override: None,
                                error: InvocationError::default()
                            })
                        }
                    }
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
        http_stream_rx: &mut ResponseStreamState,
    ) -> TerminalLoopState<()> {
        loop {
            tokio::select! {
                chunk = poll_fn(|cx| http_stream_rx.poll_next_chunk(cx)) => {
                    match crate::shortcircuit!(chunk) {
                        ResponseChunk::Parts(parts) => crate::shortcircuit!(self.handle_response_headers(parts)),
                        ResponseChunk::Data(buf) => crate::shortcircuit!(self.handle_read(parent_span_context, buf)),
                        ResponseChunk::End => {
                            // Response stream was closed without SuspensionMessage, EndMessage or ErrorMessage
                            return TerminalLoopState::Failed(InvocationTaskError::ErrorMessageReceived {
                                related_entry: None,
                                next_retry_interval_override: None,
                                error: InvocationError::default(),
                            })
                        }
                    }
                },
                _ = tokio::time::sleep(self.invocation_task.abort_timeout) => {
                    warn!("Inactivity detected, going to close invocation");
                    return TerminalLoopState::Failed(InvocationTaskError::ResponseTimeout)
                },
            }
        }
    }

    // --- Read and write methods

    async fn write_start<I: Iterator<Item = (Bytes, Bytes)>>(
        &mut self,
        http_stream_tx: &mut InvokerRequestStreamSender,
        journal_size: u32,
        state_entries: EagerState<I>,
        retry_count_since_last_stored_entry: u32,
        duration_since_last_stored_entry: Duration,
    ) -> Result<(), InvocationTaskError> {
        let is_partial = state_entries.is_partial();

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
                is_partial,
                state_entries,
                retry_count_since_last_stored_entry,
                duration_since_last_stored_entry,
            ),
        )
        .await
    }

    async fn write_entry(
        &mut self,
        http_stream_tx: &mut InvokerRequestStreamSender,
        entry: RawEntry,
    ) -> Result<(), InvocationTaskError> {
        // TODO(slinkydeveloper) could this code be improved a tad bit more introducing something to our magic macro in message_codec?
        match entry.inner {
            RawEntryInner::Command(cmd) => {
                self.write_raw(
                    http_stream_tx,
                    cmd.command_type().into(),
                    cmd.serialized_content(),
                )
                .await?;
                self.command_index += 1;
            }
            RawEntryInner::Notification(notif) => {
                self.write_raw(
                    http_stream_tx,
                    notif.ty().into(),
                    notif.serialized_content(),
                )
                .await?;
            }
            RawEntryInner::Event(_) => {
                // We don't send these
            }
        }
        Ok(())
    }

    async fn write(
        &mut self,
        http_stream_tx: &mut InvokerRequestStreamSender,
        msg: Message,
    ) -> Result<(), InvocationTaskError> {
        trace!(restate.protocol.message = ?msg, "Sending message");
        let buf = self.encoder.encode(msg);

        if http_stream_tx.send(Ok(Frame::data(buf))).await.is_err() {
            return Err(InvocationTaskError::UnexpectedClosedRequestStream);
        };
        Ok(())
    }

    async fn write_raw(
        &mut self,
        http_stream_tx: &mut InvokerRequestStreamSender,
        ty: MessageType,
        buf: Bytes,
    ) -> Result<(), InvocationTaskError> {
        trace!(restate.protocol.message = ?ty, "Sending message");
        let buf = self.encoder.encode_raw(ty, buf);

        if http_stream_tx.send(Ok(Frame::data(buf))).await.is_err() {
            return Err(InvocationTaskError::UnexpectedClosedRequestStream);
        };
        Ok(())
    }

    fn handle_response_headers(
        &mut self,
        mut parts: http::response::Parts,
    ) -> Result<(), InvocationTaskError> {
        // if service is running behind a gateway, the service can be down
        // but we still get a response code from the gateway itself. In that
        // case we still need to return the proper error
        if GATEWAY_ERRORS_CODES.contains(&parts.status) {
            return Err(InvocationTaskError::ServiceUnavailable(parts.status));
        }

        // otherwise we return generic UnexpectedResponse
        if !parts.status.is_success() {
            // Decorate the error in case of UNSUPPORTED_MEDIA_TYPE, as it probably is the incompatible protocol version
            if parts.status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
                return Err(InvocationTaskError::BadNegotiatedServiceProtocolVersion(
                    self.service_protocol_version,
                ));
            }

            return Err(InvocationTaskError::UnexpectedResponse(parts.status));
        }

        let content_type = parts.headers.remove(http::header::CONTENT_TYPE);
        let expected_content_type =
            service_protocol_version_to_header_value(self.service_protocol_version);
        match content_type {
            Some(ct) =>
            {
                #[allow(clippy::borrow_interior_mutable_const)]
                if ct != expected_content_type {
                    return Err(InvocationTaskError::UnexpectedContentType(
                        Some(ct),
                        expected_content_type,
                    ));
                }
            }
            None => {
                return Err(InvocationTaskError::UnexpectedContentType(
                    None,
                    expected_content_type,
                ))
            }
        }

        if let Some(hv) = parts.headers.remove(X_RESTATE_SERVER) {
            self.invocation_task
                .send_invoker_tx(InvocationTaskOutputInner::ServerHeaderReceived(
                    hv.to_str()
                        .map_err(|e| InvocationTaskError::BadHeader(X_RESTATE_SERVER, e))?
                        .to_owned(),
                ))
        }

        Ok(())
    }

    fn handle_read(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        buf: Bytes,
    ) -> TerminalLoopState<()> {
        self.decoder.push(buf);

        while let Some((frame_header, frame)) = crate::shortcircuit!(self.decoder.consume_next()) {
            crate::shortcircuit!(self.handle_message(parent_span_context, frame_header, frame));
        }

        TerminalLoopState::Continue(())
    }

    fn handle_new_command(&mut self, mh: MessageHeader, command: RawCommand) {
        self.invocation_task
            .send_invoker_tx(InvocationTaskOutputInner::NewCommand {
                command_index: self.command_index,
                requires_ack: mh
                    .requires_ack()
                    .expect("All command messages support requires_ack"),
                command,
            });
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
            Message::Start { .. } => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessageV4(MessageType::Start),
            ),
            Message::CommandAck(_) => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessageV4(MessageType::CommandAck),
            ),
            Message::Suspension(suspension) => self.handle_suspension_message(suspension),
            Message::Error(e) => self.handle_error_message(e),
            Message::End(_) => TerminalLoopState::Closed,

            // Run completion proposal
            Message::ProposeRunCompletion(run_completion) => {
                let notification: Entry = RunCompletion {
                    completion_id: run_completion.result_completion_id,
                    result: match crate::shortcircuit!(run_completion
                        .result
                        .ok_or(InvocationTaskError::MalformedProposeRunCompletion))
                    {
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
                    .inner
                    .try_into()
                    .expect("a raw notification");

                self.invocation_task.send_invoker_tx(
                    InvocationTaskOutputInner::NewNotificationProposal {
                        notification: raw_notification,
                    },
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
                let _: Entry = crate::shortcircuit!(RawCommand::new(
                    CommandType::GetInvocationOutput,
                    cmd.clone()
                )
                .decode::<ServiceProtocolV4Codec, _>());
                self.handle_new_command(mh, RawCommand::new(CommandType::GetInvocationOutput, cmd));
                TerminalLoopState::Continue(())
            }
            Message::AttachInvocationCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry = crate::shortcircuit!(RawCommand::new(
                    CommandType::AttachInvocation,
                    cmd.clone()
                )
                .decode::<ServiceProtocolV4Codec, _>());
                self.handle_new_command(mh, RawCommand::new(CommandType::AttachInvocation, cmd));
                TerminalLoopState::Continue(())
            }
            Message::RunCommand(cmd) => {
                self.handle_new_command(mh, RawCommand::new(CommandType::Run, cmd));
                TerminalLoopState::Continue(())
            }
            Message::SendSignalCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let _: Entry =
                    crate::shortcircuit!(RawCommand::new(CommandType::SendSignal, cmd.clone())
                        .decode::<ServiceProtocolV4Codec, _>());
                self.handle_new_command(mh, RawCommand::new(CommandType::SendSignal, cmd));
                TerminalLoopState::Continue(())
            }
            Message::OneWayCallCommand(cmd) => {
                let entry: Entry = OneWayCallCommand {
                    request: crate::shortcircuit!(resolve_call_request(
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
                    .map_err(|e| InvocationTaskError::CommandPrecondition(
                        self.command_index,
                        EntryType::Command(CommandType::OneWayCall),
                        e
                    ))),
                    invoke_time: cmd.invoke_time.into(),
                    invocation_id_completion_id: cmd.invocation_id_notification_idx,
                    name: cmd.name.into(),
                }
                .into();
                self.handle_new_command(
                    mh,
                    entry
                        .encode::<ServiceProtocolV4Codec>()
                        .inner
                        .try_into()
                        .expect("a raw command"),
                );
                TerminalLoopState::Continue(())
            }
            Message::CallCommand(cmd) => {
                let entry: Entry = CallCommand {
                    request: crate::shortcircuit!(resolve_call_request(
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
                    .map_err(|e| InvocationTaskError::CommandPrecondition(
                        self.command_index,
                        EntryType::Command(CommandType::Call),
                        e
                    ))),
                    invocation_id_completion_id: cmd.invocation_id_notification_idx,
                    result_completion_id: cmd.result_completion_id,
                    name: cmd.name.into(),
                }
                .into();
                self.handle_new_command(
                    mh,
                    entry
                        .encode::<ServiceProtocolV4Codec>()
                        .inner
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
                let _: Entry = crate::shortcircuit!(RawCommand::new(
                    CommandType::CompleteAwakeable,
                    cmd.clone()
                )
                .decode::<ServiceProtocolV4Codec, _>());
                self.handle_new_command(mh, RawCommand::new(CommandType::CompleteAwakeable, cmd));
                TerminalLoopState::Continue(())
            }
            Message::SignalNotification(_) => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessageV4(MessageType::SignalNotification),
            ),
            Message::GetInvocationOutputCompletionNotification(_) => {
                TerminalLoopState::Failed(InvocationTaskError::UnexpectedMessageV4(
                    MessageType::GetInvocationOutputCompletionNotification,
                ))
            }
            Message::AttachInvocationCompletionNotification(_) => {
                TerminalLoopState::Failed(InvocationTaskError::UnexpectedMessageV4(
                    MessageType::AttachInvocationCompletionNotification,
                ))
            }
            Message::RunCompletionNotification(_) => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessageV4(MessageType::RunCompletionNotification),
            ),
            Message::CallCompletionNotification(_) => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessageV4(MessageType::CallCompletionNotification),
            ),
            Message::CallInvocationIdCompletionNotification(_) => {
                TerminalLoopState::Failed(InvocationTaskError::UnexpectedMessageV4(
                    MessageType::CallInvocationIdCompletionNotification,
                ))
            }
            Message::SleepCompletionNotification(_) => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessageV4(MessageType::SleepCompletionNotification),
            ),
            Message::CompletePromiseCompletionNotification(_) => {
                TerminalLoopState::Failed(InvocationTaskError::UnexpectedMessageV4(
                    MessageType::CompletePromiseCompletionNotification,
                ))
            }
            Message::PeekPromiseCompletionNotification(_) => {
                TerminalLoopState::Failed(InvocationTaskError::UnexpectedMessageV4(
                    MessageType::PeekPromiseCompletionNotification,
                ))
            }
            Message::GetPromiseCompletionNotification(_) => {
                TerminalLoopState::Failed(InvocationTaskError::UnexpectedMessageV4(
                    MessageType::GetPromiseCompletionNotification,
                ))
            }
            Message::GetLazyStateKeysCompletionNotification(_) => {
                TerminalLoopState::Failed(InvocationTaskError::UnexpectedMessageV4(
                    MessageType::GetLazyStateKeysCompletionNotification,
                ))
            }
            Message::GetLazyStateCompletionNotification(_) => {
                TerminalLoopState::Failed(InvocationTaskError::UnexpectedMessageV4(
                    MessageType::GetLazyStateCompletionNotification,
                ))
            }
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
            return TerminalLoopState::Failed(InvocationTaskError::EmptySuspensionMessage);
        }
        TerminalLoopState::SuspendedV2(suspension_indexes)
    }

    fn handle_error_message(&mut self, error: proto::ErrorMessage) -> TerminalLoopState<()> {
        TerminalLoopState::Failed(InvocationTaskError::ErrorMessageReceived {
            related_entry: Some(InvocationErrorRelatedEntry {
                related_entry_index: error.related_command_index,
                related_entry_name: error.related_command_name.clone(),
                // TODO(slinkydeveloper) fill entry type!
                related_entry_type: None,
            }),
            next_retry_interval_override: error.next_retry_delay.map(Duration::from_millis),
            error: InvocationError::from(error),
        })
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
) -> Result<CallRequest, InvocationError> {
    let meta = invocation_target_resolver
        .resolve_latest_invocation_target(&request.service_name, &request.handler_name)
        .ok_or_else(|| {
            InvocationError::service_handler_not_found(&request.service_name, &request.handler_name)
        })?;

    let invocation_target = match meta.target_ty {
        InvocationTargetType::Service => {
            InvocationTarget::service(request.service_name, request.handler_name)
        }
        InvocationTargetType::VirtualObject(h_ty) => InvocationTarget::virtual_object(
            request.service_name.clone(),
            ByteString::try_from(request.key.clone().into_bytes()).map_err(|e| {
                InvocationError::from(anyhow!("The request key is not a valid UTF-8 string: {e}"))
            })?,
            request.handler_name,
            h_ty,
        ),
        InvocationTargetType::Workflow(h_ty) => InvocationTarget::workflow(
            request.service_name.clone(),
            ByteString::try_from(request.key.clone().into_bytes()).map_err(|e| {
                InvocationError::from(anyhow!("The request key is not a valid UTF-8 string: {e}"))
            })?,
            request.handler_name,
            h_ty,
        ),
    };

    let idempotency_key = if let Some(idempotency_key) = &request.idempotency_key {
        if idempotency_key.is_empty() {
            return Err(InvocationError::from(anyhow!(
                "The provided idempotency key is empty"
            )));
        }
        Some(idempotency_key.deref())
    } else {
        None
    };
    let completion_retention_duration = meta
        .compute_retention(idempotency_key.is_some())
        .unwrap_or_default();
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
        completion_retention_duration,
    })
}

#[inline]
fn check_workflow_type(
    command_index: CommandIndex,
    entry_type: &EntryType,
    service_type: &ServiceType,
) -> Result<(), InvocationTaskError> {
    if *service_type != ServiceType::Workflow {
        return Err(
            InvocationTaskError::CommandPrecondition(
                command_index,
                entry_type.clone(),
                InvocationError::new(
                    codes::BAD_REQUEST,
                    format!(
                        "The service type {service_type} does not support the entry type {entry_type}, only Workflow supports it"
                    ),
            )
        ));
    }
    Ok(())
}

#[inline]
fn can_read_state(
    command_index: CommandIndex,
    entry_type: &EntryType,
    invocation_target_type: &InvocationTargetType,
) -> Result<(), InvocationTaskError> {
    if !invocation_target_type.can_read_state() {
        return Err(
            InvocationTaskError::CommandPrecondition(
                command_index,
                entry_type.clone(),
                InvocationError::new(
            codes::BAD_REQUEST,
            format!(
                "The service/handler type {invocation_target_type} does not have state and, therefore, does not support the entry type {entry_type}"
            ),
        )));
    }
    Ok(())
}

#[inline]
fn can_write_state(
    command_index: CommandIndex,
    entry_type: &EntryType,
    invocation_target_type: &InvocationTargetType,
) -> Result<(), InvocationTaskError> {
    can_read_state(command_index, entry_type, invocation_target_type)?;
    if !invocation_target_type.can_write_state() {
        return Err(
            InvocationTaskError::CommandPrecondition(
                command_index,
                entry_type.clone(),
                InvocationError::new(
            codes::BAD_REQUEST,
            format!(
                "The service/handler type {invocation_target_type} has no exclusive state access and, therefore, does not support the entry type {entry_type}"
            ),
        )));
    }
    Ok(())
}
