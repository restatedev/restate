use std::cmp;
use std::error::Error;

use bytes::Bytes;
use common::types::{PartitionLeaderEpoch, ServiceInvocationId};
use futures::{future, select_biased, stream, FutureExt, Stream, StreamExt};
use hyper::body::Sender;
use hyper::client::HttpConnector;
use hyper::http::response::Parts;
use hyper::http::HeaderValue;
use hyper::{http, Body, Request, Response, Uri};
use hyper_tls::HttpsConnector;
use journal::raw::RawEntry;
use journal::{Completion, EntryIndex, JournalRevision};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry_http::HeaderInjector;
use tokio::select;
use tokio::sync::mpsc;
use tracing::trace;

use super::message::{
    Decoder, Encoder, EncodingError, MessageHeader, MessageType, ProtocolMessage,
};
use super::{EndpointMetadata, InvokeInputJournal, JournalMetadata, JournalReader, ProtocolType};

#[allow(clippy::declare_interior_mutable_const)]
const APPLICATION_RESTATE: HeaderValue = HeaderValue::from_static("application/restate");

#[derive(Debug, thiserror::Error)]
pub(crate) enum InvocationTaskResultKind {
    #[error("no error")]
    Ok,

    #[error("unexpected http status code: {0}")]
    UnexpectedResponse(http::StatusCode),
    #[error("unexpected http status code: {0:?}")]
    UnexpectedContentType(Option<HeaderValue>),
    #[error("received unexpected message: {0:?}")]
    UnexpectedMessage(MessageType),
    #[error("encoding/decoding error: {0}")]
    Encoding(#[from] EncodingError),
    #[error("error when trying to read the journal: {0}")]
    JournalReader(Box<dyn Error + Send + Sync + 'static>),
    #[error("other hyper error: {0}")]
    Network(hyper::Error),
    #[error(transparent)]
    Other(#[from] Box<dyn Error + Send + Sync + 'static>),
}

impl From<hyper::Error> for InvocationTaskResultKind {
    fn from(err: hyper::Error) -> Self {
        if h2_reason(&err) == h2::Reason::NO_ERROR {
            InvocationTaskResultKind::Ok
        } else {
            InvocationTaskResultKind::Network(err)
        }
    }
}

// Copy pasted from hyper::Error
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
    Result {
        last_journal_index: EntryIndex,
        last_journal_revision: JournalRevision,

        kind: InvocationTaskResultKind,
    },
    NewEntry {
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    },
}

/// Represents an open invocation stream
pub(crate) struct InvocationTask<JR> {
    // Connection params
    partition: PartitionLeaderEpoch,
    service_invocation_id: ServiceInvocationId,
    endpoint_metadata: EndpointMetadata,

    next_journal_index: EntryIndex,
    // Last revision received from the partition processor.
    // It is updated every time a completion is sent on the wire
    last_journal_revision: JournalRevision,

    // Invoker tx/rx
    journal_reader: JR,
    invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invoker_rx: Option<mpsc::UnboundedReceiver<(JournalRevision, Completion)>>,

    // Encoder/Decoder
    encoder: Encoder,
    decoder: Decoder,
}

impl<JR, JS> InvocationTask<JR>
where
    JR: JournalReader<JournalStream = JS>,
    JS: Stream<Item = RawEntry> + Unpin,
{
    pub fn new(
        partition: PartitionLeaderEpoch,
        sid: ServiceInvocationId,
        protocol_version: u16,
        endpoint_metadata: EndpointMetadata,
        journal_reader: JR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: Option<mpsc::UnboundedReceiver<(JournalRevision, Completion)>>,
    ) -> Self {
        if endpoint_metadata.protocol_type == ProtocolType::RequestResponse {
            // TODO https://github.com/restatedev/restate/issues/83
            unimplemented!("Request response is not implemented yet");
        }
        Self {
            partition,
            service_invocation_id: sid,
            endpoint_metadata,
            next_journal_index: 0,
            last_journal_revision: 0,
            journal_reader,
            invoker_tx,
            invoker_rx,
            encoder: Encoder::new(protocol_version),
            decoder: Default::default(),
        }
    }

    /// Loop opening the request to service endpoint and consuming the stream
    #[tracing::instrument(name = "run", level = "trace", skip_all, fields(restate.sid = %self.service_invocation_id))]
    pub async fn run(mut self, input_journal: InvokeInputJournal) {
        let kind = self
            .run_internal(input_journal)
            .await
            .expect_err("run_internal never returns Ok");
        let _ = self.invoker_tx.send(InvocationTaskOutput {
            partition: self.partition,
            service_invocation_id: self.service_invocation_id,
            inner: InvocationTaskOutputInner::Result {
                last_journal_index: self.next_journal_index - 1,
                last_journal_revision: self.last_journal_revision,
                kind,
            },
        });
    }

    // This method never returns Ok(()). The Result type is used only for shortcircuiting.
    // Unfortunately we cannot implement the try operator on InvocationTaskResultKind yet https://github.com/rust-lang/rust/issues/84277.
    async fn run_internal(
        &mut self,
        input_journal: InvokeInputJournal,
    ) -> Result<(), InvocationTaskResultKind> {
        // Resolve journal and its metadata
        let (journal_metadata, journal_stream) = match input_journal {
            InvokeInputJournal::NoCachedJournal => {
                let (journal_meta, journal_stream) = self
                    .journal_reader
                    .read_journal(&self.service_invocation_id)
                    .await
                    .map_err(|e| InvocationTaskResultKind::JournalReader(Box::new(e)))?;
                (journal_meta, future::Either::Left(journal_stream.fuse()))
            }
            InvokeInputJournal::CachedJournal(journal_meta, journal_items) => (
                journal_meta,
                future::Either::Right(stream::iter(journal_items).fuse()),
            ),
        };

        // Update internal state with journal metadata
        self.last_journal_revision = journal_metadata.journal_revision;

        // Acquire an HTTP client
        let client = Self::get_client();

        // Prepare the request and send start message
        let (mut http_stream_tx, http_request) = self.prepare_request(&journal_metadata);
        self.write_start(&mut http_stream_tx, &journal_metadata)
            .await?;

        // Start the request
        let http_response = self
            .wait_response_and_replay_end_loop(
                &mut http_stream_tx,
                client,
                http_request,
                journal_stream,
            )
            .await?;

        // Check all the entries have been replayed
        debug_assert_eq!(self.next_journal_index, journal_metadata.journal_size);

        // Check the response is valid
        let (http_response_header, mut http_stream_rx) = http_response.into_parts();
        Self::validate_response(http_response_header)?;

        // If we have the invoker_rx, we can use the bidi stream loop, which both reads the invoker_rx and the http_stream_rx
        if let Some(invoker_rx) = self.invoker_rx.take() {
            self.bidi_stream_loop(&mut http_stream_tx, invoker_rx, &mut http_stream_rx)
                .await?;
        }

        // We don't have the invoker_rx, so we simply consume the response
        Err(self.response_stream_loop(&mut http_stream_rx).await)
    }

    /// This loop concurrently pushes journal entries and waits for the response headers and end of replay.
    async fn wait_response_and_replay_end_loop<JournalStream>(
        &mut self,
        http_stream_tx: &mut Sender,
        client: hyper::Client<HttpsConnector<HttpConnector>>,
        req: Request<Body>,
        journal_stream: JournalStream,
    ) -> Result<Response<Body>, InvocationTaskResultKind>
    where
        JournalStream: Stream<Item = RawEntry> + stream::FusedStream,
    {
        tokio::pin! {
            let req_fut = client.request(req).fuse();
            let js = journal_stream;
        }

        let mut response = None;

        loop {
            select_biased! {
                response_res = &mut req_fut => {
                    response = Some(
                        response_res?
                    );
                },
                opt_je = js.next() => {
                    match opt_je {
                        Some(je) => {
                            self.write(http_stream_tx, ProtocolMessage::UnparsedEntry(je)).await?;
                            self.next_journal_index += 1;
                        },
                        None => {
                            trace!("Finished to replay the journal");
                        }
                    }
                },
                complete => break,
            }
        }

        Ok(response.unwrap())
    }

    /// This loop concurrently reads the http response stream and journal completions from the invoker.
    async fn bidi_stream_loop(
        &mut self,
        http_stream_tx: &mut Sender,
        mut invoker_rx: mpsc::UnboundedReceiver<(JournalRevision, Completion)>,
        http_stream_rx: &mut Body,
    ) -> Result<(), InvocationTaskResultKind> {
        loop {
            select! {
                opt_completion = invoker_rx.recv() => {
                    match opt_completion {
                        Some((journal_revision, completion)) => {
                            trace!("Sending the completion to the wire");
                            self.write(http_stream_tx, completion.into()).await?;
                            self.last_journal_revision = cmp::max(self.last_journal_revision, journal_revision);
                        },
                        None => {
                            // Completion channel is closed,
                            // the invoker main loop won't send completions anymore.
                            // Response stream might still be open though.
                            return Ok(())
                        },
                    }
                },
                opt_buf = http_stream_rx.next() => {
                    match opt_buf {
                        Some(Ok(buf)) => self.handle_read(buf)?,
                        Some(Err(hyper_err)) => {
                            return Err(hyper_err.into())
                        },
                        None => {
                            // Response stream is closed. No further processing is needed.
                            return Err(InvocationTaskResultKind::Ok)
                        }
                    }
                },
            }
        }
    }

    async fn response_stream_loop(
        &mut self,
        http_stream_rx: &mut Body,
    ) -> InvocationTaskResultKind {
        while let Some(buf_res) = http_stream_rx.next().await {
            match buf_res {
                Ok(buf) => {
                    if let Err(e) = self.handle_read(buf) {
                        return e;
                    }
                }
                Err(hyper_err) => return hyper_err.into(),
            }
        }

        InvocationTaskResultKind::Ok
    }

    // --- Read and write methods

    async fn write_start(
        &mut self,
        http_stream_tx: &mut Sender,
        journal_metadata: &JournalMetadata,
    ) -> Result<(), InvocationTaskResultKind> {
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
                journal_metadata.journal_size,
            ),
        )
        .await
    }

    async fn write(
        &mut self,
        http_stream_tx: &mut Sender,
        msg: ProtocolMessage,
    ) -> Result<(), InvocationTaskResultKind> {
        let buf = self.encoder.encode(msg);

        if let Err(hyper_err) = http_stream_tx.send_data(buf).await {
            // is_closed() can happen only with sender's channel
            if !hyper_err.is_closed() {
                return Err(InvocationTaskResultKind::Network(hyper_err));
            }
        };
        Ok(())
    }

    fn handle_read(&mut self, buf: Bytes) -> Result<(), InvocationTaskResultKind> {
        self.decoder.push(buf);

        while let Some((frame_header, frame)) = self.decoder.consume_next()? {
            self.handle_message(frame_header, frame)?;
        }

        Ok(())
    }

    fn handle_message(
        &mut self,
        mh: MessageHeader,
        message: ProtocolMessage,
    ) -> Result<(), InvocationTaskResultKind> {
        trace!(restate.protocol.message_header = ?mh, restate.protocol.message = ?message, "Received message");
        match message {
            ProtocolMessage::Start(_) => Err(InvocationTaskResultKind::UnexpectedMessage(
                MessageType::Start,
            )),
            ProtocolMessage::Completion(_) => Err(InvocationTaskResultKind::UnexpectedMessage(
                MessageType::Completion,
            )),
            ProtocolMessage::UnparsedEntry(raw_entry) => {
                let _ = self.invoker_tx.send(InvocationTaskOutput {
                    partition: self.partition,
                    service_invocation_id: self.service_invocation_id.clone(),
                    inner: InvocationTaskOutputInner::NewEntry {
                        entry_index: self.next_journal_index,
                        raw_entry,
                    },
                });
                self.next_journal_index += 1;
                Ok(())
            }
        }
    }

    // --- HTTP related methods

    fn prepare_request(&mut self, journal_metadata: &JournalMetadata) -> (Sender, Request<Body>) {
        let (http_stream_tx, req_body) = Body::channel();
        let mut http_request_builder = Request::builder()
            .method(http::Method::POST)
            .header(http::header::CONTENT_TYPE, APPLICATION_RESTATE)
            .uri(Self::append_path(
                &self.endpoint_metadata.address,
                &[
                    self.service_invocation_id
                        .service_id
                        .service_name
                        .chars()
                        .as_str(),
                    &journal_metadata.method,
                ],
            ))
            .version(http::Version::HTTP_2);

        // Inject OpenTelemetry context
        TraceContextPropagator::new().inject_context(
            &journal_metadata.tracing_context,
            &mut HeaderInjector(
                http_request_builder
                    .headers_mut()
                    .expect("The request builder shouldn't fail"),
            ),
        );

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

    // TODO pooling https://github.com/restatedev/restate/issues/76
    fn get_client() -> hyper::Client<HttpsConnector<HttpConnector>, Body> {
        hyper::Client::builder()
            .http2_only(true)
            .build::<_, Body>(HttpsConnector::new())
    }

    fn validate_response(mut parts: Parts) -> Result<(), InvocationTaskResultKind> {
        if !parts.status.is_success() {
            return Err(InvocationTaskResultKind::UnexpectedResponse(parts.status));
        }

        let content_type = parts.headers.remove(http::header::CONTENT_TYPE);
        match content_type {
            // Check content type is application/restate
            Some(ct) =>
            {
                #[allow(clippy::borrow_interior_mutable_const)]
                if ct != APPLICATION_RESTATE {
                    return Err(InvocationTaskResultKind::UnexpectedContentType(Some(ct)));
                }
            }
            None => return Err(InvocationTaskResultKind::UnexpectedContentType(None)),
        }

        Ok(())
    }
}
