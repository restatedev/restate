// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::invocation_task::{
    service_protocol_version_to_header_value, InvocationErrorRelatedEntry, InvocationTask,
    InvocationTaskError, InvocationTaskOutputInner, ResponseChunk, ResponseStreamState,
    TerminalLoopState, X_RESTATE_SERVER,
};
use crate::Notification;
use bytes::Bytes;
use futures::future::FusedFuture;
use futures::{FutureExt, Stream, StreamExt};
use hyper::body::Sender;
use hyper::http::uri::PathAndQuery;
use hyper::{http, Body, HeaderMap};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_http::HeaderInjector;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use restate_errors::warn_it;
use restate_invoker_api::{EagerState, EntryEnricher, JournalMetadata};
use restate_schema_api::deployment::{
    Deployment, DeploymentMetadata, DeploymentType, ProtocolType,
};
use restate_service_client::{Endpoint, Method, Parts, Request, ServiceClientError};
use restate_service_protocol::message::{
    Decoder, Encoder, MessageHeader, MessageType, ProtocolMessage,
};
use restate_types::errors::InvocationError;
use restate_types::identifiers::EntryIndex;
use restate_types::invocation::ServiceInvocationSpanContext;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::EntryType;
use restate_types::service_protocol::ServiceProtocolVersion;
use std::collections::HashSet;
use std::future::poll_fn;
use tracing::log::warn;
use tracing::{debug, info, trace, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Runs the interaction between the server and the service endpoint.
pub struct ServiceProtocolRunner<'a, SR, JR, EE, DMR> {
    invocation_task: &'a mut InvocationTask<SR, JR, EE, DMR>,

    service_protocol_version: ServiceProtocolVersion,

    // Encoder/Decoder
    encoder: Encoder,
    decoder: Decoder,

    // task state
    next_journal_index: EntryIndex,
}

impl<'a, SR, JR, EE, DMR> ServiceProtocolRunner<'a, SR, JR, EE, DMR>
where
    EE: EntryEnricher,
{
    pub fn new(
        invocation_task: &'a mut InvocationTask<SR, JR, EE, DMR>,
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
        JournalStream: Stream<Item = PlainRawEntry> + Unpin,
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

        // Attach parent and uri to the current span
        let invocation_task_span = Span::current();
        journal_metadata
            .span_context
            .as_parent()
            .attach_to_span(&invocation_task_span);

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
        let (mut http_stream_tx, request) =
            Self::prepare_request(path, deployment.metadata, self.service_protocol_version);

        crate::shortcircuit!(
            self.write_start(&mut http_stream_tx, journal_size, state_iter)
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

        // Check all the entries have been replayed
        debug_assert_eq!(self.next_journal_index, journal_size);

        // If we have the invoker_rx and the protocol type is bidi stream,
        // then we can use the bidi_stream loop reading the invoker_rx and the http_stream_rx
        if protocol_type == ProtocolType::BidiStream {
            crate::shortcircuit!(
                self.bidi_stream_loop(
                    &service_invocation_span_context,
                    http_stream_tx,
                    &mut http_stream_rx,
                )
                .await
            );
        } else {
            // Drop the http_stream_tx.
            // This is required in HTTP/1.1 to let the deployment send the headers back
            drop(http_stream_tx)
        }

        // We don't have the invoker_rx, so we simply consume the response
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
    ) -> (Sender, Request<Body>) {
        let (http_stream_tx, req_body) = Body::channel();

        let service_protocol_header_value =
            service_protocol_version_to_header_value(service_protocol_version);

        let mut headers = HeaderMap::from_iter([
            (
                http::header::CONTENT_TYPE,
                service_protocol_header_value.clone(),
            ),
            (http::header::ACCEPT, service_protocol_header_value),
        ]);

        // Inject OpenTelemetry context
        TraceContextPropagator::new().inject_context(
            &Span::current().context(),
            &mut HeaderInjector(&mut headers),
        );

        let address = match deployment_metadata.ty {
            DeploymentType::Lambda {
                arn,
                assume_role_arn,
            } => Endpoint::Lambda(arn, assume_role_arn),
            DeploymentType::Http {
                address,
                protocol_type,
            } => Endpoint::Http(
                address,
                match protocol_type {
                    ProtocolType::RequestResponse => http::Version::default(),
                    ProtocolType::BidiStream => http::Version::HTTP_2,
                },
            ),
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
        http_stream_tx: &mut Sender,
        http_stream_rx: &mut ResponseStreamState,
        journal_stream: JournalStream,
    ) -> TerminalLoopState<()>
    where
        JournalStream: Stream<Item = PlainRawEntry> + Unpin,
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
                        Some(je) => {
                            crate::shortcircuit!(self.write(http_stream_tx, ProtocolMessage::UnparsedEntry(je)).await);
                            self.next_journal_index += 1;
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
        mut http_stream_tx: Sender,
        http_stream_rx: &mut ResponseStreamState,
    ) -> TerminalLoopState<()> {
        loop {
            tokio::select! {
                opt_completion = self.invocation_task.invoker_rx.recv() => {
                    match opt_completion {
                        Some(Notification::Completion(completion)) => {
                            trace!("Sending the completion to the wire");
                            crate::shortcircuit!(self.write(&mut http_stream_tx, completion.into()).await);
                        },
                        Some(Notification::Ack(entry_index)) => {
                            trace!("Sending the ack to the wire");
                            crate::shortcircuit!(self.write(&mut http_stream_tx, ProtocolMessage::new_entry_ack(entry_index)).await);
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
                            return TerminalLoopState::Failed(InvocationTaskError::ErrorMessageReceived(
                                None,
                                InvocationError::default()
                            ))
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
                            return TerminalLoopState::Failed(InvocationTaskError::ErrorMessageReceived(
                                None,
                                InvocationError::default()
                            ))
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
        http_stream_tx: &mut Sender,
        journal_size: u32,
        state_entries: EagerState<I>,
    ) -> Result<(), InvocationTaskError> {
        let is_partial = state_entries.is_partial();

        // Send the invoke frame
        self.write(
            http_stream_tx,
            ProtocolMessage::new_start_message(
                Bytes::copy_from_slice(&self.invocation_task.invocation_id.to_bytes()),
                self.invocation_task.invocation_id.to_string(),
                self.invocation_task
                    .invocation_target
                    .key()
                    .map(|bs| bs.as_bytes().clone()),
                journal_size,
                is_partial,
                state_entries,
            ),
        )
        .await
    }

    async fn write(
        &mut self,
        http_stream_tx: &mut Sender,
        msg: ProtocolMessage,
    ) -> Result<(), InvocationTaskError> {
        trace!(restate.protocol.message = ?msg, "Sending message");
        let buf = self.encoder.encode(msg);

        if let Err(hyper_err) = http_stream_tx.send_data(buf).await {
            // is_closed() is try only if the request channel (Sender) has been closed.
            // This can happen if the deployment is suspending.
            if !hyper_err.is_closed() {
                return Err(InvocationTaskError::Client(ServiceClientError::Http(
                    hyper_err.into(),
                )));
            }
        };
        Ok(())
    }

    fn handle_response_headers(
        &mut self,
        mut parts: http::response::Parts,
    ) -> Result<(), InvocationTaskError> {
        if !parts.status.is_success() {
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

    fn handle_message(
        &mut self,
        parent_span_context: &ServiceInvocationSpanContext,
        mh: MessageHeader,
        message: ProtocolMessage,
    ) -> TerminalLoopState<()> {
        trace!(restate.protocol.message_header = ?mh, restate.protocol.message = ?message, "Received message");
        match message {
            ProtocolMessage::Start { .. } => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessage(MessageType::Start),
            ),
            ProtocolMessage::Completion(_) => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessage(MessageType::Completion),
            ),
            ProtocolMessage::EntryAck(_) => TerminalLoopState::Failed(
                InvocationTaskError::UnexpectedMessage(MessageType::EntryAck),
            ),
            ProtocolMessage::Suspension(suspension) => {
                let suspension_indexes = HashSet::from_iter(suspension.entry_indexes);
                // We currently don't support empty suspension_indexes set
                if suspension_indexes.is_empty() {
                    return TerminalLoopState::Failed(InvocationTaskError::EmptySuspensionMessage);
                }
                // Sanity check on the suspension indexes
                if *suspension_indexes.iter().max().unwrap() >= self.next_journal_index {
                    return TerminalLoopState::Failed(InvocationTaskError::BadSuspensionMessage(
                        suspension_indexes,
                        self.next_journal_index,
                    ));
                }
                TerminalLoopState::Suspended(suspension_indexes)
            }
            ProtocolMessage::Error(e) => {
                TerminalLoopState::Failed(InvocationTaskError::ErrorMessageReceived(
                    Some(InvocationErrorRelatedEntry {
                        related_entry_index: e.related_entry_index,
                        related_entry_name: e.related_entry_name.clone(),
                        related_entry_type: e
                            .related_entry_type
                            .and_then(|t| u16::try_from(t).ok())
                            .and_then(|idx| MessageType::try_from(idx).ok())
                            .and_then(|mt| EntryType::try_from(mt).ok()),
                    }),
                    InvocationError::from(e),
                ))
            }
            ProtocolMessage::End(_) => TerminalLoopState::Closed,
            ProtocolMessage::UnparsedEntry(entry) => {
                let entry_type = entry.header().as_entry_type();
                let enriched_entry = crate::shortcircuit!(self
                    .invocation_task
                    .entry_enricher
                    .enrich_entry(
                        entry,
                        &self.invocation_task.invocation_target,
                        parent_span_context
                    )
                    .map_err(|e| InvocationTaskError::EntryEnrichment(
                        self.next_journal_index,
                        entry_type,
                        e
                    )));
                self.invocation_task
                    .send_invoker_tx(InvocationTaskOutputInner::NewEntry {
                        entry_index: self.next_journal_index,
                        entry: enriched_entry,
                        requires_ack: mh
                            .requires_ack()
                            .expect("All entry messages support requires_ack"),
                    });
                self.next_journal_index += 1;
                TerminalLoopState::Continue(())
            }
        }
    }
}
