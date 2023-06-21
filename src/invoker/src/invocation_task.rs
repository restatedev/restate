use std::collections::HashSet;
use std::error::Error;
use std::future::{poll_fn, Future};
use std::iter;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use crate::EagerState;
use bytes::Bytes;
use futures::future::FusedFuture;
use futures::{future, stream, FutureExt, Stream, StreamExt};
use hyper::body::Sender;
use hyper::http::response::Parts;
use hyper::http::HeaderValue;
use hyper::{http, Body, Request, Response, Uri};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry_http::HeaderInjector;
use restate_common::errors::{InvocationError, InvocationErrorCode, UserErrorCode};
use restate_common::types::{
    EnrichedRawEntry, EntryIndex, PartitionLeaderEpoch, ServiceInvocationId,
    ServiceInvocationSpanContext,
};
use restate_common::utils::GenericError;
use restate_errors::warn_it;
use restate_journal::raw::{Header, PlainRawEntry, RawEntryHeader};
use restate_journal::Completion;
use restate_journal::{EntryEnricher, EntryType};
use restate_service_metadata::{EndpointMetadata, ProtocolType};
use restate_service_protocol::message::{
    Decoder, Encoder, EncodingError, MessageHeader, MessageType, ProtocolMessage,
};
use tokio::sync::mpsc;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tracing::{debug, info, instrument, trace, warn, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use super::{
    HttpsClient, InvokeInputJournal, InvokerError, JournalMetadata, JournalReader, StateReader,
};

// Clippy false positive, might be caused by Bytes contained within HeaderValue.
// https://github.com/rust-lang/rust/issues/40543#issuecomment-1212981256
#[allow(clippy::declare_interior_mutable_const)]
const APPLICATION_RESTATE: HeaderValue = HeaderValue::from_static("application/restate");

#[derive(Debug, thiserror::Error, codederror::CodedError)]
#[code(restate_errors::RT0006)]
pub(crate) enum InvocationTaskError {
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
    JournalReader(GenericError),
    #[error("error when trying to read the service instance state: {0}")]
    StateReader(GenericError),
    #[error("other hyper error: {0}")]
    Network(hyper::Error),
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
    EntryEnrichment(EntryIndex, EntryType, #[source] GenericError),
    #[error(transparent)]
    Invocation(#[from] InvocationError),
    #[error("Unexpected end of invocation stream, received a data frame after a SuspensionMessage or OutputStreamEntry. This is probably an SDK bug")]
    WriteAfterEndOfStream,
    #[error("Unexpected end of invocation stream, as it was closed with both a SuspensionMessage and an OutputStreamEntry. This is probably an SDK bug")]
    TooManyTerminalMessages,
    #[error("Unexpected end of invocation stream, as it was closed with too many OutputStreamEntry. Only one is allowed. This is probably an SDK bug")]
    TooManyOutputStreamEntry,
    #[error(transparent)]
    Other(#[from] GenericError),
}

impl InvokerError for InvocationTaskError {
    fn is_transient(&self) -> bool {
        true
    }

    fn as_invocation_error(&self) -> InvocationError {
        match self {
            InvocationTaskError::Invocation(e) => e.clone(),
            e => InvocationError::new(UserErrorCode::Internal, e.to_string()),
        }
    }

    fn invocation_error_code(&self) -> InvocationErrorCode {
        match self {
            InvocationTaskError::Invocation(e) => e.code(),
            _ => UserErrorCode::Internal.into(),
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

pub(crate) struct InvocationTaskOutput {
    pub(crate) partition: PartitionLeaderEpoch,
    pub(crate) service_invocation_id: ServiceInvocationId,
    pub(crate) inner: InvocationTaskOutputInner,
}

pub(crate) enum InvocationTaskOutputInner {
    NewEntry {
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
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
pub(crate) struct InvocationTask<JR, SR, EE> {
    // Shared client
    client: HttpsClient,

    // Connection params
    partition: PartitionLeaderEpoch,
    service_invocation_id: ServiceInvocationId,
    endpoint_metadata: EndpointMetadata,
    suspension_timeout: Duration,
    response_abort_timeout: Duration,
    disable_eager_state: bool,

    // Invoker tx/rx
    journal_reader: JR,
    state_reader: SR,
    entry_enricher: EE,
    invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invoker_rx: Option<mpsc::UnboundedReceiver<Completion>>,

    // Encoder/Decoder
    encoder: Encoder,
    decoder: Decoder,

    // Task state
    next_journal_index: EntryIndex,
    saw_output_stream_entry: bool,
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

impl<T> From<hyper::Error> for TerminalLoopState<T> {
    fn from(err: hyper::Error) -> Self {
        if h2_reason(&err) == h2::Reason::NO_ERROR {
            TerminalLoopState::Closed
        } else {
            TerminalLoopState::Failed(InvocationTaskError::Network(err))
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

impl<JR, SR, EE> InvocationTask<JR, SR, EE>
where
    JR: JournalReader + Clone + Send + Sync + 'static,
    <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
    SR: StateReader + Clone + Send + Sync + 'static,
    <SR as StateReader>::StateIter: Send,
    EE: EntryEnricher,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: HttpsClient,
        partition: PartitionLeaderEpoch,
        sid: ServiceInvocationId,
        protocol_version: u16,
        endpoint_metadata: EndpointMetadata,
        suspension_timeout: Duration,
        response_abort_timeout: Duration,
        disable_eager_state: bool,
        message_size_warning: usize,
        message_size_limit: Option<usize>,
        journal_reader: JR,
        state_reader: SR,
        entry_enricher: EE,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: Option<mpsc::UnboundedReceiver<Completion>>,
    ) -> Self {
        Self {
            client,
            partition,
            service_invocation_id: sid,
            endpoint_metadata,
            suspension_timeout,
            response_abort_timeout,
            disable_eager_state,
            next_journal_index: 0,
            journal_reader,
            state_reader,
            entry_enricher,
            invoker_tx,
            invoker_rx,
            encoder: Encoder::new(protocol_version),
            decoder: Decoder::new(message_size_warning, message_size_limit),
            saw_output_stream_entry: false,
        }
    }

    /// Loop opening the request to service endpoint and consuming the stream
    #[instrument(level = "info", name = "invoker_invocation_task", fields(rpc.system = "restate", rpc.service = %self.service_invocation_id.service_id.service_name, restate.invocation.sid = %self.service_invocation_id), skip_all)]
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
        let inner = match (terminal_state, self.saw_output_stream_entry) {
            (TerminalLoopState::Continue(_), _) => {
                unreachable!("This is not supposed to happen. This is a runtime bug")
            }
            (TerminalLoopState::Closed, true) => {
                // For the time being, seeing a OutputStreamEntry marks a correct closing of the invocation stream.
                InvocationTaskOutputInner::Closed
            }
            (TerminalLoopState::Closed, false) => {
                InvocationTaskOutputInner::Failed(InvocationError::default().into())
            }
            (TerminalLoopState::Suspended(_), true) => {
                InvocationTaskOutputInner::Failed(InvocationTaskError::TooManyTerminalMessages)
            }
            (TerminalLoopState::Suspended(v), false) => InvocationTaskOutputInner::Suspended(v),
            (TerminalLoopState::Failed(e), _) => InvocationTaskOutputInner::Failed(e),
        };

        let _ = self.invoker_tx.send(InvocationTaskOutput {
            partition: self.partition,
            service_invocation_id: self.service_invocation_id,
            inner,
        });
    }

    async fn run_internal(&mut self, input_journal: InvokeInputJournal) -> TerminalLoopState<()> {
        // Resolve journal and its metadata
        let read_journal_future = async {
            Ok(match input_journal {
                InvokeInputJournal::NoCachedJournal => {
                    let (journal_meta, journal_stream) = self
                        .journal_reader
                        .read_journal(&self.service_invocation_id)
                        .await
                        .map_err(|e| InvocationTaskError::JournalReader(Box::new(e)))?;
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
            if self.disable_eager_state {
                Ok(EagerState::<iter::Empty<_>>::default().map(itertools::Either::Right))
            } else {
                self.state_reader
                    .read_state(&self.service_invocation_id.service_id)
                    .await
                    .map_err(|e| InvocationTaskError::StateReader(Box::new(e)))
                    .map(|r| r.map(itertools::Either::Left))
            }
        };

        // We execute those concurrently
        let ((journal_metadata, journal_stream), state_iter) =
            shortcircuit!(tokio::try_join!(read_journal_future, read_state_future));

        // Resolve the uri to use for the request
        let uri = self.prepare_uri(&journal_metadata);
        let journal_size = journal_metadata.length;

        // Attach parent and uri to the current span
        let invocation_task_span = Span::current();
        journal_metadata
            .span_context
            .as_parent()
            .attach_to_span(&invocation_task_span);
        info!(http.url = %uri, "Executing invocation at service endpoint");

        // Create an arc of the parent SpanContext.
        // We send this with every journal entry to correctly link new spans generated from journal entries.
        let service_invocation_span_context = journal_metadata.span_context;

        // Prepare the request and send start message
        let (mut http_stream_tx, http_request) = self.prepare_request(uri);
        shortcircuit!(
            self.write_start(&mut http_stream_tx, journal_size, state_iter)
                .await
        );

        // Initialize the response stream state
        let mut http_stream_rx = ResponseStreamState::initialize(&self.client, http_request);

        // Execute the replay
        shortcircuit!(
            self.replay_loop(&mut http_stream_tx, &mut http_stream_rx, journal_stream)
                .await
        );

        // Check all the entries have been replayed
        debug_assert_eq!(self.next_journal_index, journal_size);

        // Force protocol type to RequestResponse if suspension_timeout is zero
        let protocol_type = if self.suspension_timeout.is_zero() {
            ProtocolType::RequestResponse
        } else {
            self.endpoint_metadata.protocol_type()
        };

        // If we have the invoker_rx and the protocol type is bidi stream,
        // then we can use the bidi_stream loop reading the invoker_rx and the http_stream_rx
        if let (Some(invoker_rx), ProtocolType::BidiStream) =
            (self.invoker_rx.take(), protocol_type)
        {
            shortcircuit!(
                self.bidi_stream_loop(
                    &service_invocation_span_context,
                    http_stream_tx,
                    invoker_rx,
                    &mut http_stream_rx
                )
                .await
            );
        } else {
            // Drop the http_stream_tx.
            // This is required in HTTP/1.1 to let the service endpoint send the headers back
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
        let got_headers_future = poll_fn(|cx| http_stream_rx.poll_wait_headers(cx)).fuse();
        tokio::pin!(got_headers_future);

        loop {
            tokio::select! {
                got_headers_res = got_headers_future.as_mut(), if !got_headers_future.is_terminated() => {
                    // The reason we want to poll headers in this function is
                    // to exit early in case an error is returned during replays.
                    shortcircuit!(got_headers_res);
                },
                opt_je = journal_stream.next() => {
                    match opt_je {
                        Some(je) => {
                            if je.header == RawEntryHeader::OutputStream {
                                shortcircuit!(self.notify_saw_output_stream_entry());
                            }
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
        mut invoker_rx: mpsc::UnboundedReceiver<Completion>,
        http_stream_rx: &mut ResponseStreamState,
    ) -> TerminalLoopState<()> {
        loop {
            tokio::select! {
                opt_completion = invoker_rx.recv() => {
                    match opt_completion {
                        Some(completion) => {
                            trace!("Sending the completion to the wire");
                            shortcircuit!(self.write(&mut http_stream_tx, completion.into()).await);
                        },
                        None => {
                            // Completion channel is closed,
                            // the invoker main loop won't send completions anymore.
                            // Response stream might still be open though.
                            return TerminalLoopState::Continue(())
                        },
                    }
                },
                opt_buf = poll_fn(|cx| http_stream_rx.poll_next_chunk(cx)) => {
                    match shortcircuit!(opt_buf) {
                        Some(buf) => shortcircuit!(self.handle_read(parent_span_context, buf)),
                        None => {
                            // Response stream is closed. No further processing is needed.
                            return TerminalLoopState::Closed
                        }
                    }
                },
                _ = tokio::time::sleep(self.suspension_timeout) => {
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
                opt_buf = poll_fn(|cx| http_stream_rx.poll_next_chunk(cx)) => {
                    match shortcircuit!(opt_buf) {
                        Some(buf) => shortcircuit!(self.handle_read(parent_span_context, buf)),
                        None => {
                            // Response stream is closed. No further processing is needed.
                            return TerminalLoopState::Closed
                        }
                    }
                },
                _ = tokio::time::sleep(self.response_abort_timeout) => {
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
                self.service_invocation_id
                    .invocation_id
                    .as_bytes()
                    .to_vec()
                    .into(),
                self.service_invocation_id.service_id.key.clone(),
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
            // This can happen if the service endpoint is suspending.
            if !hyper_err.is_closed() {
                return Err(InvocationTaskError::Network(hyper_err));
            }
        };
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
            ProtocolMessage::Suspension(suspension) => {
                let suspension_indexes = HashSet::from_iter(suspension.entry_indexes.into_iter());
                if suspension_indexes.is_empty() {
                    return TerminalLoopState::Failed(InvocationTaskError::EmptySuspensionMessage);
                }
                if *suspension_indexes.iter().max().unwrap() >= self.next_journal_index {
                    return TerminalLoopState::Failed(InvocationTaskError::BadSuspensionMessage(
                        suspension_indexes,
                        self.next_journal_index,
                    ));
                }
                TerminalLoopState::Suspended(suspension_indexes)
            }
            ProtocolMessage::Error(e) => {
                TerminalLoopState::Failed(InvocationTaskError::Invocation(e.into()))
            }
            ProtocolMessage::UnparsedEntry(entry) => {
                if entry.header == RawEntryHeader::OutputStream {
                    shortcircuit!(self.notify_saw_output_stream_entry());
                }
                let entry_type = entry.header.to_entry_type();
                let enriched_entry = shortcircuit!(self
                    .entry_enricher
                    .enrich_entry(entry, parent_span_context)
                    .map_err(|e| InvocationTaskError::EntryEnrichment(
                        self.next_journal_index,
                        entry_type,
                        e
                    )));
                let _ = self.invoker_tx.send(InvocationTaskOutput {
                    partition: self.partition,
                    service_invocation_id: self.service_invocation_id.clone(),
                    inner: InvocationTaskOutputInner::NewEntry {
                        entry_index: self.next_journal_index,
                        entry: enriched_entry,
                    },
                });
                self.next_journal_index += 1;
                TerminalLoopState::Continue(())
            }
        }
    }

    fn notify_saw_output_stream_entry(&mut self) -> Result<(), InvocationTaskError> {
        if self.saw_output_stream_entry {
            return Err(InvocationTaskError::TooManyOutputStreamEntry);
        }
        self.saw_output_stream_entry = true;
        Ok(())
    }

    // --- HTTP related methods

    fn prepare_uri(&self, journal_metadata: &JournalMetadata) -> Uri {
        Self::append_path(
            self.endpoint_metadata.address(),
            &[
                "invoke",
                self.service_invocation_id
                    .service_id
                    .service_name
                    .chars()
                    .as_str(),
                &journal_metadata.method,
            ],
        )
    }

    fn prepare_request(&mut self, uri: Uri) -> (Sender, Request<Body>) {
        let (http_stream_tx, req_body) = Body::channel();
        let mut http_request_builder = Request::builder()
            .method(http::Method::POST)
            .header(http::header::CONTENT_TYPE, APPLICATION_RESTATE)
            .header(http::header::ACCEPT, APPLICATION_RESTATE)
            .uri(uri);

        // In case it's bidi stream, force HTTP/2
        if self.endpoint_metadata.protocol_type() == ProtocolType::BidiStream {
            http_request_builder = http_request_builder.version(http::Version::HTTP_2);
        }

        // Inject OpenTelemetry context
        TraceContextPropagator::new().inject_context(
            &Span::current().context(),
            &mut HeaderInjector(
                http_request_builder
                    .headers_mut()
                    .expect("The request builder shouldn't fail"),
            ),
        );

        // Inject additional headers
        for (header_name, header_value) in self.endpoint_metadata.additional_headers() {
            http_request_builder = http_request_builder.header(header_name, header_value);
        }

        let http_request = http_request_builder
            .body(req_body)
            // This fails only in case the URI is malformed, which should never happen
            .expect("The request builder shouldn't fail");

        (http_stream_tx, http_request)
    }

    fn append_path(uri: &Uri, fragments: &[&str]) -> Uri {
        let p = format!(
            "{}/{}",
            match uri.path().strip_suffix('/') {
                None => uri.path(),
                Some(s) => s,
            },
            fragments.join("/")
        );

        Uri::builder()
            .authority(
                uri.authority()
                    .expect("The function endpoint URI must have the authority")
                    .clone(),
            )
            .scheme(
                uri.scheme()
                    .expect("The function endpoint URI must have the scheme")
                    .clone(),
            )
            .path_and_query(p)
            .build()
            .unwrap()
    }
}

enum ResponseStreamState {
    WaitingHeaders(AbortOnDrop<Result<Response<Body>, hyper::Error>>),
    ReadingBody(Body),
}

impl ResponseStreamState {
    fn initialize(client: &HttpsClient, req: Request<Body>) -> Self {
        // Because the body sender blocks on waiting for the request body buffer to be available,
        // we need to spawn the request initiation separately, otherwise the loop below
        // will deadlock on the journal entry write.
        // This task::spawn won't be required by hyper 1.0, as the connection will be driven by a task
        // spawned somewhere else (perhaps in the connection pool).
        // See: https://github.com/restatedev/restate/issues/96 and https://github.com/restatedev/restate/issues/76
        Self::WaitingHeaders(AbortOnDrop(tokio::task::spawn(client.request(req))))
    }

    // Could be replaced by a Future implementation
    fn poll_wait_headers(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), InvocationTaskError>> {
        match self {
            ResponseStreamState::WaitingHeaders(join_handle) => {
                let http_response = match ready!(join_handle.poll_unpin(cx)) {
                    Ok(Ok(res)) => res,
                    Ok(Err(hyper_err)) => {
                        return Poll::Ready(Err(InvocationTaskError::Network(hyper_err)))
                    }
                    Err(join_err) => {
                        return Poll::Ready(Err(InvocationTaskError::UnexpectedJoinError(join_err)))
                    }
                };

                // Check the response is valid
                let (http_response_header, body) = http_response.into_parts();
                Self::validate_response(http_response_header)?;

                // Transition to reading body
                *self = ResponseStreamState::ReadingBody(body);

                Poll::Ready(Ok(()))
            }
            ResponseStreamState::ReadingBody(_) => Poll::Ready(Ok(())),
        }
    }

    // Could be replaced by a Stream implementation
    fn poll_next_chunk(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Bytes>, InvocationTaskError>> {
        // Could be replaced by a Stream implementation
        loop {
            match self {
                ResponseStreamState::WaitingHeaders(_) => {
                    ready!(self.poll_wait_headers(cx))?;
                }
                ResponseStreamState::ReadingBody(b) => {
                    let next_element = ready!(b.poll_next_unpin(cx));
                    return Poll::Ready(match next_element.transpose() {
                        Ok(opt) => Ok(opt),
                        Err(err) => {
                            if h2_reason(&err) == h2::Reason::NO_ERROR {
                                Ok(None)
                            } else {
                                Err(InvocationTaskError::Network(err))
                            }
                        }
                    });
                }
            }
        }
    }

    fn validate_response(mut parts: Parts) -> Result<(), InvocationTaskError> {
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

        Ok(())
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
