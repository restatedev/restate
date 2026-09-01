// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the `Ingest` stream state machine.
//!
//! Both ends of the stream are driven by the test: the request stream is an mpsc
//! channel the test pushes frames into, and the response stream is unfolded by a
//! background task into a second channel (mirroring what tonic does, so the state
//! machine keeps stepping on its own). The ingestion side is a
//! [`MockIngestionClient`], so no cluster environment is needed: either records
//! commit as soon as they are ingested (see [`auto_commit`]) or the test takes
//! control of *when* every single record commits via [`IngestHandler`] and its
//! [`IngestHandlerResolver`].
//!
//! Tests run with a paused clock, so [`TestStream::settle`] can wait until the
//! state machine has consumed everything the test sent before commits are
//! released.

use std::collections::HashMap;
use std::future::Ready;
use std::hash::Hash;
use std::num::NonZeroU32;
use std::time::Duration;

use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use prost::Message;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Status;

use restate_ingestion_client::test::{MockIngestHandler, MockIngestionClient};
use restate_ingestion_client::{CancelledError, IngestionError, RecordCommit};
use restate_test_util::{assert, assert_eq, let_assert};
use restate_types::identifiers::DeploymentId;
use restate_types::invocation::{InvocationTargetType, ServiceInvocation, WorkflowHandlerType};
use restate_types::live::Live;
use restate_types::logs::Keys;
use restate_types::schema::invocation_target::{DeploymentStatus, InvocationTargetMetadata};
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::{Command, DedupInformation, Destination, Envelope};

use crate::ingestion::ingestion_svc::proto::ingestion_request::Kind;
use crate::ingestion::ingestion_svc::proto::{
    self, DeduplicationMode, ErrorKind, IngestionDefaults, IngestionInvocation, IngestionRequest,
    IngestionResponse, IngestionStart, ingestion_response,
};
use crate::ingestion::ingestion_svc::{IngestionStream, UPDATE_WINDOW_THRESHOLD};
use crate::mocks::{MockSchemas, mock_schemas};

const PRODUCER: &str = "test-producer";
const INTEGRATION: &str = "test-integration/1.0";
/// A `Service` target, from [`mock_schemas`].
const SERVICE: &str = "greeter.Greeter";
/// An exclusive `VirtualObject` target, from [`mock_schemas`].
const OBJECT: &str = "greeter.GreeterObject";
const WORKFLOW: &str = "greeter.GreeterWorkflow";
const PRIVATE: &str = "greeter.PrivateGreeter";
const DEPRECATED: &str = "greeter.DeprecatedGreeter";
const HANDLER: &str = "greet";

/// Opens a new `Ingest` stream with the given maximum send window, driving its
/// responses in the background.
fn stream<F>(max_window_size: u32, handler: F) -> TestStream
where
    F: MockIngestHandler<Envelope> + Send + 'static,
{
    let (request_tx, request_rx) = mpsc::unbounded_channel();

    let stream = IngestionStream::new(
        UnboundedReceiverStream::new(request_rx),
        MockIngestionClient::new(handler),
        Live::from_value(test_schemas()),
        NonZeroU32::new(max_window_size).unwrap(),
    );

    let (response_tx, response_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut stream = std::pin::pin!(futures::stream::unfold(stream, IngestionStream::step));
        while let Some(v) = stream.next().await {
            response_tx.send(v).unwrap();
        }
    });

    TestStream {
        requests: Some(request_tx),
        responses: response_rx,
    }
}

/// The client side of a single `Ingest` stream.
struct TestStream {
    /// `None` once the client half-closed the request stream.
    requests: Option<mpsc::UnboundedSender<Result<IngestionRequest, Status>>>,
    responses: UnboundedReceiver<Result<IngestionResponse, Status>>,
}

impl TestStream {
    fn send(&self, kind: Kind) {
        self.send_request(IngestionRequest { kind: Some(kind) });
    }

    fn send_request(&self, request: IngestionRequest) {
        self.requests
            .as_ref()
            .expect("stream is not half-closed")
            .send(Ok(request))
            .expect("server stream is alive");
    }

    /// Half-closes the request stream, as a client that is done sending does.
    fn half_close(&mut self) {
        self.requests = None;
    }

    async fn next_response(&mut self) -> IngestionResponse {
        self.responses
            .recv()
            .await
            .expect("stream produced a response")
            .expect("responses are never transport errors")
    }

    /// Asserts the next response is a window update, returning
    /// `(increment_bytes, last_committed)`.
    async fn next_window_update(&mut self) -> (u32, Option<u64>) {
        let response = self.next_response().await;
        let_assert!(
            Some(ingestion_response::Response::WindowUpdate(update)) = response.response,
            "expected a window update"
        );
        (update.increment_bytes, response.last_committed)
    }

    /// Asserts the next response is an error, returning it together with
    /// `last_committed`.
    async fn next_error(&mut self) -> (proto::Error, Option<u64>) {
        let response = self.next_response().await;
        let_assert!(
            Some(ingestion_response::Response::Error(error)) = response.response,
            "expected an error"
        );
        (error, response.last_committed)
    }

    /// Lets the state machine (and the ingestion sessions) run until they are all
    /// waiting again, so that everything the test has sent so far has been applied.
    async fn settle(&self) {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    fn assert_no_response(&mut self) {
        assert!(self.responses.recv().now_or_never().is_none());
    }

    /// Asserts the server tore the stream down (every error frame is terminal).
    async fn assert_terminated(&mut self) {
        assert!(self.responses.recv().await.is_none());
    }
}

// -- Helpers ------------------------------------------------------------------

/// [`mock_schemas`] plus the targets needed to exercise the rejection paths.
fn test_schemas() -> MockSchemas {
    let mut private = InvocationTargetMetadata::mock(InvocationTargetType::Service);
    private.public = false;

    let mut deprecated = InvocationTargetMetadata::mock(InvocationTargetType::Service);
    deprecated.deployment_status = DeploymentStatus::Deprecated(DeploymentId::default());

    mock_schemas()
        .with_service_and_target(
            WORKFLOW,
            HANDLER,
            InvocationTargetMetadata::mock(InvocationTargetType::Workflow(
                WorkflowHandlerType::Workflow,
            )),
        )
        .with_service_and_target(PRIVATE, HANDLER, private)
        .with_service_and_target(DEPRECATED, HANDLER, deprecated)
}

fn defaults(service: Option<&str>, handler: Option<&str>) -> IngestionDefaults {
    IngestionDefaults {
        service: service.map(str::to_owned),
        handler: handler.map(str::to_owned),
        ..Default::default()
    }
}

fn start(defaults: Option<IngestionDefaults>) -> IngestionStart {
    IngestionStart {
        producer_id: PRODUCER.to_owned(),
        integration: INTEGRATION.to_owned(),
        deduplication_mode: DeduplicationMode::OffsetBased.into(),
        defaults,
    }
}

fn invocation(offset: u64) -> IngestionInvocation {
    IngestionInvocation {
        offset,
        payload: Bytes::from_static(b"payload"),
        ..Default::default()
    }
}

fn invocation_with_payload(offset: u64, payload_len: usize) -> IngestionInvocation {
    IngestionInvocation {
        offset,
        payload: Bytes::from(vec![b'x'; payload_len]),
        ..Default::default()
    }
}

/// Opens a stream, sends `start` and consumes the initial window advertisement.
async fn started_stream<F>(max_window_size: u32, start: IngestionStart, handler: F) -> TestStream
where
    F: MockIngestHandler<Envelope> + Send + 'static,
{
    let mut stream = stream(max_window_size, handler);

    stream.send(Kind::Start(start));

    let (increment, last_committed) = stream.next_window_update().await;
    assert_eq!(increment, max_window_size);
    assert_eq!(last_committed, None);

    stream
}

fn single_invoke(envelope: Envelope) -> (Box<ServiceInvocation>, Option<DedupInformation>) {
    let_assert!(Destination::Processor { dedup, .. } = envelope.header.dest);
    let_assert!(Command::Invoke(invocation) = envelope.command);
    (invocation, dedup)
}

/// The producer id the server derives for [`PRODUCER`]; pins the hashing that
/// deduplication across streams depends on.
fn expected_producer_id() -> u128 {
    let mut hasher = xxhash_rust::xxh3::Xxh3::default();
    PRODUCER.hash(&mut hasher);
    hasher.digest128()
}

/// Ingestion handler that commits every record right away; for tests that either
/// never get as far as ingesting a record, or that do not care about *when*
/// records commit.
fn auto_commit(_keys: Keys, _record: Envelope) -> Ready<Result<RecordCommit, IngestionError>> {
    std::future::ready(Ok(RecordCommit::resolved()))
}

/// One ingested record, together with the resolver of its pending commit.
type IngestedRecord = (oneshot::Sender<Result<(), CancelledError>>, Keys, Envelope);

/// The test side of an [`IngestHandler`]: every ingested record shows up here and
/// stays uncommitted until the test resolves it.
struct IngestHandlerResolver {
    rx: mpsc::UnboundedReceiver<IngestedRecord>,
}

impl IngestHandlerResolver {
    /// Waits for the next ingested record and resolves its commit with the result
    /// of `f`.
    async fn handle_next<F>(&mut self, f: F)
    where
        F: FnOnce(Keys, Envelope) -> Result<(), CancelledError>,
    {
        let (o, keys, envelope) = self.rx.recv().await.unwrap();
        o.send(f(keys, envelope)).unwrap();
    }

    /// Waits for the next ingested record, commits it and returns its envelope.
    async fn commit_next(&mut self) -> Envelope {
        let (o, _keys, envelope) = self.rx.recv().await.unwrap();
        o.send(Ok(())).unwrap();
        envelope
    }
}

/// Ingestion handler that hands every record to the test through
/// [`IngestHandlerResolver`] instead of committing it.
struct IngestHandler {
    tx: mpsc::UnboundedSender<IngestedRecord>,
}

impl IngestHandler {
    fn new() -> (IngestHandler, IngestHandlerResolver) {
        let (tx, rx) = mpsc::unbounded_channel();
        (IngestHandler { tx }, IngestHandlerResolver { rx })
    }
}

impl MockIngestHandler<Envelope> for IngestHandler {
    fn handle(
        &mut self,
        keys: Keys,
        record: Envelope,
    ) -> impl Future<Output = Result<RecordCommit, IngestionError>> + Send + Sync + 'static {
        let (one_tx, one_rx) = oneshot::channel::<Result<(), CancelledError>>();
        self.tx.send((one_tx, keys, record)).unwrap();

        async { Ok(RecordCommit::with_resolver(one_rx)) }
    }
}

// -- Tests --------------------------------------------------------------------

/// Happy path: the stream advertises its window up front, builds one envelope per
/// record, and replenishes the window while advancing `last_committed`. It also
/// pins the ack batching: while records are still in flight and the window is
/// healthy, commits do not each get their own response.
#[tokio::test(start_paused = true)]
async fn ingests_records_and_replenishes_window() {
    let (handler, mut resolver) = IngestHandler::new();

    let mut stream = started_stream(
        4096,
        start(Some(defaults(Some(SERVICE), Some(HANDLER)))),
        handler,
    )
    .await;

    let first = invocation(0);
    let first_size = first.encoded_len() as u32;
    stream.send(Kind::Invocation(first));

    resolver
        .handle_next(|_keys, envelope| {
            let (invoke, dedup) = single_invoke(envelope);
            assert_eq!(invoke.invocation_target.service_name(), SERVICE);
            assert_eq!(invoke.invocation_target.handler_name(), HANDLER);
            assert_eq!(invoke.argument, Bytes::from_static(b"payload"));
            assert_eq!(
                dedup,
                Some(DedupInformation::producer(expected_producer_id(), 0))
            );
            Ok(())
        })
        .await;

    // and received an ack from the ingestion stream and proper window update
    assert_eq!(
        stream.next_window_update().await,
        (first_size, Some(0)),
        "the committed record replenishes exactly its own size"
    );

    // Two more records, committed one after the other. The window stays well above
    // the yield threshold, so the two commits are acked with a single response.
    let second = invocation(1);
    let third = invocation(2);
    let both_sizes = (second.encoded_len() + third.encoded_len()) as u32;
    stream.send(Kind::Invocation(second));
    stream.send(Kind::Invocation(third));

    for i in 1..=2 {
        resolver
            .handle_next(|_keys, envelope| {
                let (invoke, dedup) = single_invoke(envelope);
                assert_eq!(invoke.invocation_target.service_name(), SERVICE);
                assert_eq!(invoke.invocation_target.handler_name(), HANDLER);
                assert_eq!(invoke.argument, Bytes::from_static(b"payload"));
                assert_eq!(
                    dedup,
                    Some(DedupInformation::producer(expected_producer_id(), i))
                );
                Ok(())
            })
            .await;
    }

    assert_eq!(stream.next_window_update().await, (both_sizes, Some(2)));
}

/// Once the window drops below [`UPDATE_WINDOW_THRESHOLD`] the server must not wait
/// for the in-flight queue to drain before returning credit, otherwise a client
/// that keeps the pipeline full would stall.
#[tokio::test(start_paused = true)]
async fn window_update_is_returned_early_when_window_is_low() {
    // Offsets 1..=4 so that every record encodes to the same size (offset 0 is the
    // proto3 default and is not encoded at all, which would make the first record
    // shorter than the rest).
    let records: Vec<_> = (1..=4).map(|o| invocation_with_payload(o, 90)).collect();
    let record_size = records[0].encoded_len() as u32;
    assert!(
        records
            .iter()
            .all(|r| r.encoded_len() as u32 == record_size)
    );

    // Four records fit in the window with 32 bytes to spare, so the window is below
    // the threshold (half of the maximum) once all four are in flight.
    let max_window_size = 4 * record_size + 32;
    let threshold = max_window_size * UPDATE_WINDOW_THRESHOLD / 100;

    let (handler, mut resolver) = IngestHandler::new();
    let mut stream = started_stream(
        max_window_size,
        start(Some(defaults(Some(SERVICE), Some(HANDLER)))),
        handler,
    )
    .await;

    for record in records {
        stream.send(Kind::Invocation(record));
    }
    stream.settle().await;
    stream.assert_no_response();

    resolver.commit_next().await;
    assert_eq!(stream.next_window_update().await, (record_size, Some(1)));

    // window: 32 + one record, still below the threshold.
    assert!(32 + record_size < threshold);
    resolver.commit_next().await;
    assert_eq!(stream.next_window_update().await, (record_size, Some(2)));

    // window: 32 + two records, back above the threshold, so the last two commits
    // are batched into a single response.
    assert!(32 + 2 * record_size >= threshold);
    resolver.commit_next().await;
    stream.settle().await;
    stream.assert_no_response();

    resolver.commit_next().await;
    assert_eq!(
        stream.next_window_update().await,
        (2 * record_size, Some(4))
    );
}

/// A single oversized invocation may take the window negative, but sending anything
/// else before the window is replenished is a protocol violation.
#[tokio::test(start_paused = true)]
async fn exceeding_the_send_window_terminates_the_stream() {
    // The resolver is kept alive but never used: nothing commits, so the window is
    // never replenished.
    let (handler, _resolver) = IngestHandler::new();
    let mut stream = started_stream(
        64,
        start(Some(defaults(Some(SERVICE), Some(HANDLER)))),
        handler,
    )
    .await;

    // Legal: a single invocation larger than the whole window.
    stream.send(Kind::Invocation(invocation_with_payload(0, 256)));
    stream.settle().await;
    stream.assert_no_response();

    // Illegal: the window is depleted and nothing has committed yet.
    stream.send(Kind::Invocation(invocation(1)));

    let (error, last_committed) = stream.next_error().await;
    assert_eq!(error.kind(), ErrorKind::GoAway);
    assert_eq!(error.invocation_offset, None);
    assert!(error.message.contains("window size violation"));
    assert_eq!(last_committed, None);
    stream.assert_terminated().await;
}

/// Offsets may skip, but must strictly increase within a stream. The error frame
/// must report the last committed offset so the client knows where to resume.
#[tokio::test(start_paused = true)]
async fn offsets_must_strictly_increase() {
    let (handler, mut resolver) = IngestHandler::new();
    let mut stream = started_stream(
        4096,
        start(Some(defaults(Some(SERVICE), Some(HANDLER)))),
        handler,
    )
    .await;

    // A gap between 0 and 5 is fine.
    stream.send(Kind::Invocation(invocation(0)));
    resolver.commit_next().await;
    assert_eq!(stream.next_window_update().await.1, Some(0));

    stream.send(Kind::Invocation(invocation(5)));
    resolver.commit_next().await;
    assert_eq!(stream.next_window_update().await.1, Some(5));

    // Replaying an offset that was already submitted is not.
    stream.send(Kind::Invocation(invocation(5)));
    let (error, last_committed) = stream.next_error().await;
    assert_eq!(error.kind(), ErrorKind::GoAway);
    assert_eq!(error.invocation_offset, None);
    assert!(error.message.contains("offset value violation"));
    assert_eq!(
        last_committed,
        Some(5),
        "the error frame must carry the resume point"
    );
    stream.assert_terminated().await;
}

/// The `Start` frame is mandatory, must be first, must be sent only once, and is
/// validated eagerly.
#[tokio::test(start_paused = true)]
async fn start_frame_contract() {
    let cases: Vec<(&str, IngestionRequest, ErrorKind, &str)> = vec![
        (
            "first frame is not a Start",
            IngestionRequest {
                kind: Some(Kind::Defaults(defaults(Some(SERVICE), Some(HANDLER)))),
            },
            ErrorKind::GoAway,
            "Expecting Start message",
        ),
        (
            "first frame has no kind",
            IngestionRequest { kind: None },
            ErrorKind::GoAway,
            "Missing request payload",
        ),
        (
            "offset based deduplication without a producer id",
            IngestionRequest {
                kind: Some(Kind::Start(IngestionStart {
                    producer_id: String::new(),
                    ..start(None)
                })),
            },
            ErrorKind::GoAway,
            "Required producer id",
        ),
        (
            "unknown deduplication mode",
            IngestionRequest {
                kind: Some(Kind::Start(IngestionStart {
                    deduplication_mode: 42,
                    ..start(None)
                })),
            },
            ErrorKind::BadRequest,
            "Unknown deduplication mode",
        ),
        (
            "defaults referencing an unknown service",
            IngestionRequest {
                kind: Some(Kind::Start(start(Some(defaults(
                    Some("unknown.Service"),
                    None,
                ))))),
            },
            ErrorKind::NotFound,
            "Unknown service unknown.Service",
        ),
        (
            "defaults referencing an unknown handler",
            IngestionRequest {
                kind: Some(Kind::Start(start(Some(defaults(
                    Some(SERVICE),
                    Some("unknown"),
                ))))),
            },
            ErrorKind::NotFound,
            "Unknown service handler",
        ),
    ];

    for (case, request, kind, message) in cases {
        let mut stream = stream(4096, auto_commit);
        stream.send_request(request);

        let (error, last_committed) = stream.next_error().await;
        assert_eq!(error.kind(), kind, "case: {case}");
        assert_eq!(error.invocation_offset, None, "case: {case}");
        assert!(error.message.contains(message), "case: {case}");
        assert_eq!(last_committed, None, "case: {case}");
        stream.assert_terminated().await;
    }
}

/// A client that opens a stream and never sends `Start` must not hold the stream
/// open indefinitely.
#[tokio::test(start_paused = true)]
async fn missing_start_frame_times_out() {
    let mut stream = stream(4096, auto_commit);

    let (error, _) = stream.next_error().await;
    assert_eq!(error.kind(), ErrorKind::GoAway);
    assert!(error.message.contains("Timeout"));
    stream.assert_terminated().await;
}

/// Frames that are malformed for an already-started stream.
#[tokio::test(start_paused = true)]
async fn frames_rejected_while_processing() {
    let cases: Vec<(&str, IngestionRequest, &str)> = vec![
        (
            "a second Start frame",
            IngestionRequest {
                kind: Some(Kind::Start(start(None))),
            },
            "Unexpected Start message",
        ),
        (
            "a frame without payload",
            IngestionRequest { kind: None },
            "Missing request payload",
        ),
        (
            "defaults referencing an unknown service",
            IngestionRequest {
                kind: Some(Kind::Defaults(defaults(Some("unknown.Service"), None))),
            },
            "Unknown service unknown.Service",
        ),
    ];

    for (case, request, message) in cases {
        let mut stream = started_stream(
            4096,
            start(Some(defaults(Some(SERVICE), Some(HANDLER)))),
            auto_commit,
        )
        .await;

        stream.send_request(request);
        let (error, _) = stream.next_error().await;
        assert!(error.message.contains(message), "case: {case}");
        assert_eq!(error.invocation_offset, None, "case: {case}");
        stream.assert_terminated().await;
    }
}

/// Records the server refuses to build an envelope for. All of them are
/// record-scoped: the error must carry the offset of the offending record so the
/// client can skip or fix it, and the stream is still torn down.
#[tokio::test(start_paused = true)]
async fn record_rejections_carry_the_offset() {
    let with_service = |service: &str, mutate: fn(&mut IngestionInvocation)| {
        let mut record = invocation(7);
        record.service = Some(service.to_owned());
        record.handler = Some(HANDLER.to_owned());
        mutate(&mut record);
        record
    };

    let cases: Vec<(&str, IngestionInvocation, ErrorKind, &str)> = vec![
        (
            "unknown service",
            with_service("unknown.Service", |_| {}),
            ErrorKind::NotFound,
            "Unknown service unknown.Service",
        ),
        (
            "unknown handler",
            with_service(SERVICE, |record| {
                record.handler = Some("unknown".to_owned())
            }),
            ErrorKind::NotFound,
            "Unknown service handler",
        ),
        (
            "missing service",
            IngestionInvocation {
                handler: Some(HANDLER.to_owned()),
                ..invocation(7)
            },
            ErrorKind::BadRequest,
            "Missing service key",
        ),
        (
            "missing handler",
            IngestionInvocation {
                service: Some(SERVICE.to_owned()),
                ..invocation(7)
            },
            ErrorKind::BadRequest,
            "Missing service handler",
        ),
        (
            "private service",
            with_service(PRIVATE, |_| {}),
            ErrorKind::BadRequest,
            "Service is private",
        ),
        (
            "deprecated deployment",
            with_service(DEPRECATED, |_| {}),
            ErrorKind::BadRequest,
            "is deprecated",
        ),
        (
            "key on a service target",
            with_service(SERVICE, |record| record.key = Some("key".to_owned())),
            ErrorKind::BadRequest,
            "Unexpected service key",
        ),
        (
            "no key on a virtual object target",
            with_service(OBJECT, |_| {}),
            ErrorKind::BadRequest,
            "Missing required service key",
        ),
        (
            "idempotency key on a workflow handler",
            with_service(WORKFLOW, |record| {
                record.key = Some("key".to_owned());
                record.idempotency_key = Some("idempotency".to_owned());
            }),
            ErrorKind::BadRequest,
            "Unexpected idempotency key",
        ),
        (
            "scope without vqueues enabled",
            with_service(SERVICE, |record| record.scope = Some("scope".to_owned())),
            ErrorKind::BadRequest,
            "Scopes requires VQueues",
        ),
        (
            "limit key without a scope",
            with_service(SERVICE, |record| {
                record.limit_key = Some("limit".to_owned())
            }),
            ErrorKind::BadRequest,
            "Unexpected limit key without scope",
        ),
    ];

    for (case, record, kind, message) in cases {
        let mut stream = started_stream(4096, start(None), auto_commit).await;
        stream.send(Kind::Invocation(record));

        let (error, last_committed) = stream.next_error().await;
        assert_eq!(error.kind(), kind, "case: {case}");
        assert_eq!(error.invocation_offset, Some(7), "case: {case}");
        assert!(error.message.contains(message), "case: {case}");
        assert_eq!(last_committed, None, "case: {case}");
        stream.assert_terminated().await;
    }
}

/// A record the ingestion client refuses to accept (here: its envelope exceeds the
/// client's record size limit) surfaces as a record-scoped bad request.
#[tokio::test(start_paused = true)]
async fn oversized_record_is_rejected_by_the_ingestion_client() {
    let mut stream = started_stream(
        64 * 1024,
        start(Some(defaults(Some(SERVICE), Some(HANDLER)))),
        |_keys, _record: Envelope| {
            std::future::ready::<Result<RecordCommit, IngestionError>>(Err(
                IngestionError::RecordMaxSizeExceeded {
                    size: 1024,
                    limit: 128,
                },
            ))
        },
    )
    .await;

    stream.send(Kind::Invocation(invocation_with_payload(3, 1024)));

    let (error, _) = stream.next_error().await;
    assert_eq!(error.kind(), ErrorKind::BadRequest);
    assert_eq!(error.invocation_offset, Some(3));
    assert!(error.message.contains("exceeds maximum allowed size"));
    stream.assert_terminated().await;
}

/// Defaults are replaced wholesale by a later `IngestionDefaults` frame, and any
/// field set on a record wins over the current default. Headers are the exception:
/// they are merged, with the record's headers overriding.
#[tokio::test(start_paused = true)]
async fn defaults_are_replaced_and_overridden_per_record() {
    let (handler, mut resolver) = IngestHandler::new();
    let mut stream = started_stream(
        64 * 1024,
        start(Some(IngestionDefaults {
            headers: HashMap::from([
                ("a".to_owned(), "1".to_owned()),
                ("b".to_owned(), "2".to_owned()),
            ]),
            idempotency_key: Some("from-defaults".to_owned()),
            ..defaults(Some(SERVICE), Some(HANDLER))
        })),
        handler,
    )
    .await;

    // Purely default-driven.
    stream.send(Kind::Invocation(invocation(0)));
    let (invoke, _) = single_invoke(resolver.commit_next().await);
    assert_eq!(invoke.invocation_target.service_name(), SERVICE);
    assert_eq!(invoke.invocation_target.handler_name(), HANDLER);
    assert_eq!(invoke.idempotency_key.as_deref(), Some("from-defaults"));
    let mut headers: Vec<_> = invoke
        .headers
        .iter()
        .map(|header| (header.name.to_string(), header.value.to_string()))
        .collect();
    headers.sort();
    assert_eq!(
        headers,
        vec![
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "2".to_owned())
        ]
    );
    assert_eq!(stream.next_window_update().await.1, Some(0));

    // Record-level overrides win, and its headers extend/override the defaults.
    stream.send(Kind::Invocation(IngestionInvocation {
        service: Some(OBJECT.to_owned()),
        key: Some("object-key".to_owned()),
        idempotency_key: Some("from-record".to_owned()),
        invoke_time_ts_ms: Some(1_700_000_000_000),
        additional_headers: HashMap::from([
            ("b".to_owned(), "overridden".to_owned()),
            ("c".to_owned(), "3".to_owned()),
        ]),
        ..invocation(1)
    }));
    let (invoke, _) = single_invoke(resolver.commit_next().await);
    assert_eq!(invoke.invocation_target.service_name(), OBJECT);
    assert_eq!(
        invoke.invocation_target.key().map(|key| key.to_string()),
        Some("object-key".to_owned())
    );
    assert_eq!(invoke.idempotency_key.as_deref(), Some("from-record"));
    assert_eq!(
        invoke.execution_time,
        Some(MillisSinceEpoch::from(1_700_000_000_000))
    );
    let mut headers: Vec<_> = invoke
        .headers
        .iter()
        .map(|header| (header.name.to_string(), header.value.to_string()))
        .collect();
    headers.sort();
    assert_eq!(
        headers,
        vec![
            ("a".to_owned(), "1".to_owned()),
            ("b".to_owned(), "overridden".to_owned()),
            ("c".to_owned(), "3".to_owned()),
        ]
    );
    assert_eq!(stream.next_window_update().await.1, Some(1));

    // A new defaults frame replaces the previous one instead of merging with it, so
    // the idempotency key and headers established above are gone.
    stream.send(Kind::Defaults(defaults(Some(OBJECT), Some(HANDLER))));
    stream.send(Kind::Invocation(IngestionInvocation {
        key: Some("other-key".to_owned()),
        ..invocation(2)
    }));
    let (invoke, _) = single_invoke(resolver.commit_next().await);
    assert_eq!(invoke.invocation_target.service_name(), OBJECT);
    assert_eq!(invoke.idempotency_key, None);
    assert!(invoke.headers.is_empty());
    assert_eq!(stream.next_window_update().await.1, Some(2));
}

/// With deduplication disabled the envelope carries no dedup information, and an
/// empty producer id is accepted.
#[tokio::test(start_paused = true)]
async fn deduplication_can_be_disabled() {
    let (handler, mut resolver) = IngestHandler::new();
    let mut stream = started_stream(
        4096,
        IngestionStart {
            producer_id: String::new(),
            deduplication_mode: DeduplicationMode::Disabled.into(),
            ..start(Some(defaults(Some(SERVICE), Some(HANDLER))))
        },
        handler,
    )
    .await;

    stream.send(Kind::Invocation(invocation(0)));
    let (invoke, dedup) = single_invoke(resolver.commit_next().await);
    assert_eq!(dedup, None);
    assert_eq!(invoke.invocation_target.service_name(), SERVICE);

    // Offsets are still tracked and reported even without deduplication.
    assert_eq!(stream.next_window_update().await.1, Some(0));
}

/// When the client half-closes, the server drains everything still in flight and
/// reports the final `last_committed` before ending the stream.
#[restate_core::test(start_paused = true)]
async fn half_close_drains_inflight_records() {
    let (handler, mut resolver) = IngestHandler::new();
    let mut stream = started_stream(
        4096,
        start(Some(defaults(Some(SERVICE), Some(HANDLER)))),
        handler,
    )
    .await;

    stream.send(Kind::Invocation(invocation(0)));
    stream.send(Kind::Invocation(invocation(1)));
    stream.settle().await;

    stream.half_close();
    stream.settle().await;
    stream.assert_no_response();

    resolver.commit_next().await;
    resolver.commit_next().await;

    assert_eq!(
        stream.next_window_update().await,
        (0, Some(1)),
        "the final frame acks the drained records without granting credit"
    );
    stream.assert_terminated().await;
}
