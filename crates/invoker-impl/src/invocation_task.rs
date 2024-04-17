// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{InvokerError, Notification};

use bytes::Bytes;
use futures::future::FusedFuture;
use futures::{future, stream, FutureExt, Stream, StreamExt};
use hyper::body::Sender;
use hyper::http::response::Parts as ResponseParts;
use hyper::http::{HeaderName, HeaderValue};
use hyper::{http, Body, HeaderMap, Response};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_http::HeaderInjector;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use restate_errors::warn_it;
use restate_invoker_api::{
    EagerState, EntryEnricher, InvokeInputJournal, JournalReader, StateReader,
};
use restate_schema_api::deployment::{
    DeploymentMetadata, DeploymentResolver, DeploymentType, ProtocolType,
};
use restate_service_client::{Endpoint, Parts, Request, ServiceClient, ServiceClientError};
use restate_service_protocol::message::{
    Decoder, Encoder, EncodingError, MessageHeader, MessageType, ProtocolMessage,
};
use restate_types::errors::InvocationError;
use restate_types::identifiers::{DeploymentId, EntryIndex, InvocationId, PartitionLeaderEpoch};
use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::EntryType;
use std::collections::HashSet;
use std::error::Error;

use hyper::http::uri::PathAndQuery;
use std::future::{poll_fn, Future};
use std::iter;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tracing::{debug, info, instrument, trace, warn, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

// Clippy false positive, might be caused by Bytes contained within HeaderValue.
// https://github.com/rust-lang/rust/issues/40543#issuecomment-1212981256
#[allow(clippy::declare_interior_mutable_const)]
const APPLICATION_RESTATE: HeaderValue = HeaderValue::from_static("application/restate");

#[allow(clippy::declare_interior_mutable_const)]
const X_RESTATE_SERVER: HeaderName = HeaderName::from_static("x-restate-server");

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::RT0006)]
pub(crate) enum InvocationTaskError {
    #[error("no deployment was found to process the invocation")]
    NoDeploymentForComponent,
    #[error("the invocation has a deployment id associated, but it was not found in the registry. This might indicate that a deployment was forcefully removed from the registry, but there are still in-flight invocations pinned to it")]
    UnknownDeployment(DeploymentId),
    #[error("unexpected http status code: {0}")]
    UnexpectedResponse(http::StatusCode),
    #[error("unexpected content type: {0:?}")]
    UnexpectedContentType(Option<HeaderValue>),
    #[error("received unexpected message: {0:?}")]
    UnexpectedMessage(MessageType),
    #[error("encoding/decoding error: {0}")]
    Encoding(
        #[from]
        #[code]
        EncodingError,
    ),
    #[error("error when trying to read the journal: {0}")]
    JournalReader(anyhow::Error),
    #[error("error when trying to read the service instance state: {0}")]
    StateReader(anyhow::Error),
    #[error("other client error: {0}")]
    Client(ServiceClientError),
    #[error("unexpected join error, looks like hyper panicked: {0}")]
    UnexpectedJoinError(#[from] JoinError),
    #[error("got bad SuspensionMessage without journal indexes")]
    EmptySuspensionMessage,
    #[error(
        "got bad SuspensionMessage, suspending on journal indexes {0:?}, but journal length is {1}"
    )]
    BadSuspensionMessage(HashSet<EntryIndex>, EntryIndex),
    #[error("response timeout")]
    #[code(restate_errors::RT0001)]
    ResponseTimeout,
    #[error("cannot process received entry at index {0} of type {1}: {2}")]
    EntryEnrichment(EntryIndex, EntryType, #[source] InvocationError),
    #[error(transparent)]
    #[code(restate_errors::RT0007)]
    ErrorMessageReceived(#[from] InvocationError),
    #[error("Unexpected end of invocation stream, received a data frame after a SuspensionMessage or OutputStreamEntry. This is probably an SDK bug")]
    WriteAfterEndOfStream,
    #[error("Bad header {0}: {1}")]
    BadHeader(HeaderName, #[source] hyper::header::ToStrError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl InvokerError for InvocationTaskError {
    fn is_transient(&self) -> bool {
        true
    }

    fn to_invocation_error(&self) -> InvocationError {
        match self {
            InvocationTaskError::ErrorMessageReceived(e) => e.clone(),
            InvocationTaskError::EntryEnrichment(entry_index, entry_type, e) => {
                let msg = format!(
                    "Error when processing entry {} of type {}: {}",
                    entry_index,
                    entry_type,
                    e.message()
                );
                let mut err = InvocationError::new(e.code(), msg);
                if let Some(desc) = e.description() {
                    err = err.with_description(desc);
                }
                err
            }
            e => InvocationError::internal(e),
        }
    }
}

// Copy pasted from hyper::Error
// https://github.com/hyperium/hyper/blob/40c01dfb4f87342a6f86f07564ddc482194c6240/src/error.rs#L229
// TODO hopefully this code is not needed anymore with hyper 1.0,
//  as we'll have more control on the h2 frames themselves.
//  Revisit when upgrading to hyper 1.0.
fn find_source<E: Error + 'static>(err: &hyper::Error) -> Option<&E> {
    let mut cause = err.source();
    while let Some(err) = cause {
        if let Some(typed) = err.downcast_ref() {
            return Some(typed);
        }
        cause = err.source();
    }

    // else
    None
}

fn h2_reason(err: &hyper::Error) -> h2::Reason {
    // Find an h2::Reason somewhere in the cause stack, if it exists,
    // otherwise assume an INTERNAL_ERROR.
    find_source::<h2::Error>(err)
        .and_then(|h2_err| h2_err.reason())
        .unwrap_or(h2::Reason::INTERNAL_ERROR)
}

pub(super) struct InvocationTaskOutput {
    pub(super) partition: PartitionLeaderEpoch,
    pub(super) invocation_id: InvocationId,
    pub(super) inner: InvocationTaskOutputInner,
}

pub(super) enum InvocationTaskOutputInner {
    // `has_changed` indicates if we believe this is a freshly selected endpoint or not.
    SelectedDeployment(DeploymentId, /* has_changed: */ bool),
    ServerHeaderReceived(String),
    NewEntry {
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
        /// If true, the SDK requested to be notified when the entry is correctly stored.
        ///
        /// When reading the entry from the storage this flag will always be false, as we never need to send acks for entries sent during a journal replay.
        ///
        /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#acknowledgment-of-stored-entries
        requires_ack: bool,
    },
    Closed,
    Suspended(HashSet<EntryIndex>),
    Failed(InvocationTaskError),
}

impl From<InvocationTaskError> for InvocationTaskOutputInner {
    fn from(value: InvocationTaskError) -> Self {
        InvocationTaskOutputInner::Failed(value)
    }
}

/// Represents an open invocation stream
pub(super) struct InvocationTask<JR, SR, EE, DMR> {
    // Shared client
    client: ServiceClient,

    // Connection params
    partition: PartitionLeaderEpoch,
    invocation_id: InvocationId,
    invocation_target: InvocationTarget,
    inactivity_timeout: Duration,
    abort_timeout: Duration,
    disable_eager_state: bool,

    // Invoker tx/rx
    journal_reader: JR,
    state_reader: SR,
    entry_enricher: EE,
    deployment_metadata_resolver: DMR,
    invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invoker_rx: mpsc::UnboundedReceiver<Notification>,

    // Encoder/Decoder
    encoder: Encoder,
    decoder: Decoder,

    // Task state
    next_journal_index: EntryIndex,
}

/// This is needed to split the run_internal in multiple loop functions and have shortcircuiting.
enum TerminalLoopState<T> {
    Continue(T),
    Closed,
    Suspended(HashSet<EntryIndex>),
    Failed(InvocationTaskError),
}

impl<T, E: Into<InvocationTaskError>> From<Result<T, E>> for TerminalLoopState<T> {
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(v) => TerminalLoopState::Continue(v),
            Err(e) => TerminalLoopState::Failed(e.into()),
        }
    }
}

/// Could be replaced by ? operator if we had Try stable. See [`InvocationTask::run_internal`]
macro_rules! shortcircuit {
    ($value:expr) => {
        match TerminalLoopState::from($value) {
            TerminalLoopState::Continue(v) => v,
            TerminalLoopState::Closed => return TerminalLoopState::Closed,
            TerminalLoopState::Suspended(v) => return TerminalLoopState::Suspended(v),
            TerminalLoopState::Failed(e) => return TerminalLoopState::Failed(e),
        }
    };
}

impl<JR, SR, EE, DMR> InvocationTask<JR, SR, EE, DMR>
where
    JR: JournalReader + Clone + Send + Sync + 'static,
    <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
    SR: StateReader + Clone + Send + Sync + 'static,
    <SR as StateReader>::StateIter: Send,
    EE: EntryEnricher,
    DMR: DeploymentResolver,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: ServiceClient,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        protocol_version: u16,
        inactivity_timeout: Duration,
        abort_timeout: Duration,
        disable_eager_state: bool,
        message_size_warning: usize,
        message_size_limit: Option<usize>,
        journal_reader: JR,
        state_reader: SR,
        entry_enricher: EE,
        deployment_metadata_resolver: DMR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
    ) -> Self {
        Self {
            client,
            partition,
            invocation_id,
            invocation_target,
            inactivity_timeout,
            abort_timeout,
            disable_eager_state,
            next_journal_index: 0,
            journal_reader,
            state_reader,
            entry_enricher,
            deployment_metadata_resolver,
            invoker_tx,
            invoker_rx,
            encoder: Encoder::new(protocol_version),
            decoder: Decoder::new(message_size_warning, message_size_limit),
        }
    }

    /// Loop opening the request to deployment and consuming the stream
    #[instrument(level = "debug", name = "invoker_invocation_task", fields(rpc.system = "restate", rpc.service = %self.invocation_target.service_name(), restate.invocation.id = %self.invocation_id, restate.invocation.target = %self.invocation_target), skip_all)]
    pub async fn run(mut self, input_journal: InvokeInputJournal) {
        // Execute the task
        let terminal_state = self.run_internal(input_journal).await;

        // Sanity check of the stream decoder
        if self.decoder.has_remaining() {
            warn_it!(
                InvocationTaskError::WriteAfterEndOfStream,
                "The read buffer is non empty after the stream has been closed."
            );
        }

        // Sanity check of the final state
        let inner = match terminal_state {
            TerminalLoopState::Continue(_) => {
                unreachable!("This is not supposed to happen. This is a runtime bug")
            }
            TerminalLoopState::Closed => InvocationTaskOutputInner::Closed,
            TerminalLoopState::Suspended(v) => InvocationTaskOutputInner::Suspended(v),
            TerminalLoopState::Failed(e) => InvocationTaskOutputInner::Failed(e),
        };

        self.send_invoker_tx(inner);
    }

    async fn run_internal(&mut self, input_journal: InvokeInputJournal) -> TerminalLoopState<()> {
        // Resolve journal and its metadata
        let read_journal_future = async {
            Ok(match input_journal {
                InvokeInputJournal::NoCachedJournal => {
                    let (journal_meta, journal_stream) = self
                        .journal_reader
                        .read_journal(&self.invocation_id)
                        .await
                        .map_err(|e| InvocationTaskError::JournalReader(e.into()))?;
                    (journal_meta, future::Either::Left(journal_stream))
                }
                InvokeInputJournal::CachedJournal(journal_meta, journal_items) => (
                    journal_meta,
                    future::Either::Right(stream::iter(journal_items)),
                ),
            })
        };
        // Read eager state
        let read_state_future = async {
            let keyed_service_id = self.invocation_target.as_keyed_service_id();
            if self.disable_eager_state || keyed_service_id.is_none() {
                Ok(EagerState::<iter::Empty<_>>::default().map(itertools::Either::Right))
            } else {
                self.state_reader
                    .read_state(&keyed_service_id.unwrap())
                    .await
                    .map_err(|e| InvocationTaskError::StateReader(e.into()))
                    .map(|r| r.map(itertools::Either::Left))
            }
        };

        // We execute those concurrently
        let ((journal_metadata, journal_stream), state_iter) =
            shortcircuit!(tokio::try_join!(read_journal_future, read_state_future));

        // Resolve the deployment metadata
        let (deployment, deployment_changed) =
            if let Some(deployment_id) = journal_metadata.deployment_id {
                // We have a pinned deployment that we can't change even if newer
                // deployments have been registered for the same service.
                let deployment_metadata = shortcircuit!(self
                    .deployment_metadata_resolver
                    .get_deployment(&deployment_id)
                    .ok_or_else(|| InvocationTaskError::UnknownDeployment(deployment_id)));
                (deployment_metadata, /* has_changed= */ false)
            } else {
                // We can choose the freshest deployment for the latest revision
                // of the registered service.
                let deployment = shortcircuit!(self
                    .deployment_metadata_resolver
                    .resolve_latest_deployment_for_component(self.invocation_target.service_name())
                    .ok_or(InvocationTaskError::NoDeploymentForComponent));
                (deployment, /* has_changed= */ true)
            };

        self.send_invoker_tx(InvocationTaskOutputInner::SelectedDeployment(
            deployment.id,
            deployment_changed,
        ));

        // Figure out the protocol type. Force RequestResponse if inactivity_timeout is zero
        let protocol_type = if self.inactivity_timeout.is_zero() {
            ProtocolType::RequestResponse
        } else {
            deployment.metadata.ty.protocol_type()
        };

        // Close the invoker_rx in case it's request response, this avoids further buffering of messages in this channel.
        if protocol_type == ProtocolType::RequestResponse {
            self.invoker_rx.close();
        }

        let path: PathAndQuery = format!(
            "/invoke/{}/{}",
            self.invocation_target.service_name(),
            self.invocation_target.handler_name()
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
            deployment.address = %deployment.metadata.address_display(),
            path = %path,
            "Executing invocation at deployment"
        );

        // Create an arc of the parent SpanContext.
        // We send this with every journal entry to correctly link new spans generated from journal entries.
        let service_invocation_span_context = journal_metadata.span_context;

        // Prepare the request and send start message
        let (mut http_stream_tx, request) = self.prepare_request(path, deployment.metadata);
        shortcircuit!(
            self.write_start(&mut http_stream_tx, journal_size, state_iter)
                .await
        );

        // Initialize the response stream state
        let mut http_stream_rx = ResponseStreamState::initialize(&self.client, request);

        // Execute the replay
        shortcircuit!(
            self.replay_loop(&mut http_stream_tx, &mut http_stream_rx, journal_stream)
                .await
        );

        // Check all the entries have been replayed
        debug_assert_eq!(self.next_journal_index, journal_size);

        // If we have the invoker_rx and the protocol type is bidi stream,
        // then we can use the bidi_stream loop reading the invoker_rx and the http_stream_rx
        if protocol_type == ProtocolType::BidiStream {
            shortcircuit!(
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
        self.response_stream_loop(&service_invocation_span_context, &mut http_stream_rx)
            .await
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
                    let headers = shortcircuit!(got_headers_res);
                    shortcircuit!(self.handle_response_headers(headers));
                },
                opt_je = journal_stream.next() => {
                    match opt_je {
                        Some(je) => {
                            shortcircuit!(self.write(http_stream_tx, ProtocolMessage::UnparsedEntry(je)).await);
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
                opt_completion = self.invoker_rx.recv() => {
                    match opt_completion {
                        Some(Notification::Completion(completion)) => {
                            trace!("Sending the completion to the wire");
                            shortcircuit!(self.write(&mut http_stream_tx, completion.into()).await);
                        },
                        Some(Notification::Ack(entry_index)) => {
                            trace!("Sending the ack to the wire");
                            shortcircuit!(self.write(&mut http_stream_tx, ProtocolMessage::new_entry_ack(entry_index)).await);
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
                    match shortcircuit!(chunk) {
                        ResponseChunk::Parts(parts) => shortcircuit!(self.handle_response_headers(parts)),
                        ResponseChunk::Data(buf) => shortcircuit!(self.handle_read(parent_span_context, buf)),
                        ResponseChunk::End => {
                            // Response stream was closed without SuspensionMessage, EndMessage or ErrorMessage
                            return TerminalLoopState::Failed(InvocationTaskError::ErrorMessageReceived(
                                InvocationError::default()
                            ))
                        }
                    }
                },
                _ = tokio::time::sleep(self.inactivity_timeout) => {
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
                    match shortcircuit!(chunk) {
                        ResponseChunk::Parts(parts) => shortcircuit!(self.handle_response_headers(parts)),
                        ResponseChunk::Data(buf) => shortcircuit!(self.handle_read(parent_span_context, buf)),
                        ResponseChunk::End => {
                            // Response stream was closed without SuspensionMessage, EndMessage or ErrorMessage
                            return TerminalLoopState::Failed(InvocationTaskError::ErrorMessageReceived(
                                InvocationError::default()
                            ))
                        }
                    }
                },
                _ = tokio::time::sleep(self.abort_timeout) => {
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
                Bytes::copy_from_slice(&self.invocation_id.to_bytes()),
                self.invocation_id.to_string(),
                self.invocation_target.key().map(|bs| bs.as_bytes().clone()),
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
        mut parts: ResponseParts,
    ) -> Result<(), InvocationTaskError> {
        if !parts.status.is_success() {
            return Err(InvocationTaskError::UnexpectedResponse(parts.status));
        }

        let content_type = parts.headers.remove(http::header::CONTENT_TYPE);
        match content_type {
            // Check content type is application/restate
            Some(ct) =>
            {
                #[allow(clippy::borrow_interior_mutable_const)]
                if ct != APPLICATION_RESTATE {
                    return Err(InvocationTaskError::UnexpectedContentType(Some(ct)));
                }
            }
            None => return Err(InvocationTaskError::UnexpectedContentType(None)),
        }

        if let Some(hv) = parts.headers.remove(X_RESTATE_SERVER) {
            self.send_invoker_tx(InvocationTaskOutputInner::ServerHeaderReceived(
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

        while let Some((frame_header, frame)) = shortcircuit!(self.decoder.consume_next()) {
            shortcircuit!(self.handle_message(parent_span_context, frame_header, frame));
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
            ProtocolMessage::Error(e) => TerminalLoopState::Failed(
                InvocationTaskError::ErrorMessageReceived(InvocationError::from(e)),
            ),
            ProtocolMessage::End(_) => TerminalLoopState::Closed,
            ProtocolMessage::UnparsedEntry(entry) => {
                let entry_type = entry.header().as_entry_type();
                let enriched_entry = shortcircuit!(self
                    .entry_enricher
                    .enrich_entry(entry, parent_span_context)
                    .map_err(|e| InvocationTaskError::EntryEnrichment(
                        self.next_journal_index,
                        entry_type,
                        e
                    )));
                self.send_invoker_tx(InvocationTaskOutputInner::NewEntry {
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

    fn prepare_request(
        &mut self,
        path: PathAndQuery,
        deployment_metadata: DeploymentMetadata,
    ) -> (Sender, Request<Body>) {
        let (http_stream_tx, req_body) = Body::channel();

        let mut headers = HeaderMap::from_iter([
            (http::header::CONTENT_TYPE, APPLICATION_RESTATE),
            (http::header::ACCEPT, APPLICATION_RESTATE),
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
            Request::new(Parts::new(address, path, headers), req_body),
        )
    }

    fn send_invoker_tx(&mut self, invocation_task_output_inner: InvocationTaskOutputInner) {
        let _ = self.invoker_tx.send(InvocationTaskOutput {
            partition: self.partition,
            invocation_id: self.invocation_id,
            inner: invocation_task_output_inner,
        });
    }
}

enum ResponseChunk {
    Parts(ResponseParts),
    Data(Bytes),
    End,
}

enum ResponseStreamState {
    WaitingHeaders(AbortOnDrop<Result<Response<Body>, ServiceClientError>>),
    ReadingBody(Option<ResponseParts>, Body),
}

impl ResponseStreamState {
    fn initialize(client: &ServiceClient, req: Request<Body>) -> Self {
        // Because the body sender blocks on waiting for the request body buffer to be available,
        // we need to spawn the request initiation separately, otherwise the loop below
        // will deadlock on the journal entry write.
        // This task::spawn won't be required by hyper 1.0, as the connection will be driven by a task
        // spawned somewhere else (perhaps in the connection pool).
        // See: https://github.com/restatedev/restate/issues/96 and https://github.com/restatedev/restate/issues/76
        Self::WaitingHeaders(AbortOnDrop(tokio::task::spawn(client.call(req))))
    }

    // Could be replaced by a Future implementation
    fn poll_only_headers(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ResponseParts, InvocationTaskError>> {
        match self {
            ResponseStreamState::WaitingHeaders(join_handle) => {
                let http_response = match ready!(join_handle.poll_unpin(cx)) {
                    Ok(Ok(res)) => res,
                    Ok(Err(hyper_err)) => {
                        return Poll::Ready(Err(InvocationTaskError::Client(hyper_err)))
                    }
                    Err(join_err) => {
                        return Poll::Ready(Err(InvocationTaskError::UnexpectedJoinError(join_err)))
                    }
                };

                // Convert to response parts
                let (http_response_header, body) = http_response.into_parts();

                // Transition to reading body
                *self = ResponseStreamState::ReadingBody(None, body);

                Poll::Ready(Ok(http_response_header))
            }
            ResponseStreamState::ReadingBody { .. } => {
                panic!("Unexpected poll after the headers have been resolved already")
            }
        }
    }

    // Could be replaced by a Stream implementation
    fn poll_next_chunk(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<ResponseChunk, InvocationTaskError>> {
        // Could be replaced by a Stream implementation
        loop {
            match self {
                ResponseStreamState::WaitingHeaders(join_handle) => {
                    let http_response = match ready!(join_handle.poll_unpin(cx)) {
                        Ok(Ok(res)) => res,
                        Ok(Err(hyper_err)) => {
                            return Poll::Ready(Err(InvocationTaskError::Client(hyper_err)))
                        }
                        Err(join_err) => {
                            return Poll::Ready(Err(InvocationTaskError::UnexpectedJoinError(
                                join_err,
                            )))
                        }
                    };

                    // Convert to response parts
                    let (http_response_header, body) = http_response.into_parts();

                    // Transition to reading body
                    *self = ResponseStreamState::ReadingBody(Some(http_response_header), body);
                }
                ResponseStreamState::ReadingBody(headers, b) => {
                    // If headers are present, take them
                    if let Some(parts) = headers.take() {
                        return Poll::Ready(Ok(ResponseChunk::Parts(parts)));
                    };

                    let next_element = ready!(b.poll_next_unpin(cx));
                    return Poll::Ready(match next_element.transpose() {
                        Ok(Some(val)) => Ok(ResponseChunk::Data(val)),
                        Ok(None) => Ok(ResponseChunk::End),
                        Err(err) => {
                            if h2_reason(&err) == h2::Reason::NO_ERROR {
                                Ok(ResponseChunk::End)
                            } else {
                                Err(InvocationTaskError::Client(ServiceClientError::Http(
                                    err.into(),
                                )))
                            }
                        }
                    });
                }
            }
        }
    }
}

/// This wrapper makes sure we abort the task when the JoinHandle is dropped,
/// but it doesn't wait for the task to complete, because we simply don't have async drops!
/// For more: https://github.com/tokio-rs/tokio/issues/2596
/// Inspired by: https://github.com/cyb0124/abort-on-drop
#[derive(Debug)]
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Future for AbortOnDrop<T> {
    type Output = <JoinHandle<T> as Future>::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort()
    }
}
