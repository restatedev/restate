// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PoC of the `restate.ingestion.IngestionSvc` gRPC API.
//!
//! An external producer opens a bidirectional `Ingest` stream and pushes records that each become
//! an ingress invocation. Flow control is pull-based (the server grants a window; the client only
//! sends within it) and delivery is exactly-once, keyed on `(producer_id, offset)` via the WAL
//! [`DedupInformation`] mechanism — the same path the built-in Kafka ingress uses.
//!
//! This is a proof of concept: it favors a clear, correct contract over production polish.

use std::collections::BTreeSet;
use std::convert::Infallible;

use futures::StreamExt;
use futures::stream::{BoxStream, FuturesUnordered};
use http::{Request, Response};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};
use tower::util::BoxCloneService;
use tracing::{debug, warn};

use restate_core::TaskCenterFutureExt;
use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_storage_api::deduplication_table::DedupInformation;
use restate_types::identifiers::{
    InvocationId, PartitionProcessorRpcRequestId, WithPartitionKey, partitioner,
};
use restate_types::invocation::{
    Header, InvocationTarget, InvocationTargetType, ServiceInvocation,
};
use restate_types::live::Live;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_wal_protocol::{Command, Destination, Envelope};

pub mod pb {
    // Generated code: the proto carries a not-yet-used `Overrides` placeholder message.
    #![allow(dead_code)]
    tonic::include_proto!("restate.ingestion");
}

use pb::ingestion_svc_server::{IngestionSvc, IngestionSvcServer};

/// How many records the client may have in flight at once. The server grants this up front and
/// replenishes one credit per durable commit, keeping the steady-state in-flight count around it.
const INITIAL_WINDOW: u64 = 256;

/// A type-erased gRPC service, ready to be branched to from the ingress connection service. Erasing
/// the transport generic here keeps [`crate::server::HyperServerIngress`] transport-agnostic.
pub(crate) type GrpcService =
    BoxCloneService<Request<tonic::body::Body>, Response<tonic::body::Body>, Infallible>;

/// Build the type-erased `IngestionSvc` gRPC service from the shared ingestion client and schemas.
pub(crate) fn build_service<T, Schemas>(
    ingestion_client: IngestionClient<T, Envelope>,
    schemas: Live<Schemas>,
) -> GrpcService
where
    T: TransportConnect,
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    BoxCloneService::new(IngestionSvcServer::new(IngestionService {
        ingestion_client,
        schemas,
    }))
}

#[derive(Clone)]
pub(crate) struct IngestionService<T, Schemas> {
    ingestion_client: IngestionClient<T, Envelope>,
    schemas: Live<Schemas>,
}

#[tonic::async_trait]
impl<T, Schemas> IngestionSvc for IngestionService<T, Schemas>
where
    T: TransportConnect,
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    type IngestStream = BoxStream<'static, Result<pb::Response, Status>>;

    async fn ingest(
        &self,
        request: tonic::Request<Streaming<pb::Request>>,
    ) -> Result<tonic::Response<Self::IngestStream>, Status> {
        let inbound = request.into_inner();
        // Small buffer: the client is pull-based, so the outbound queue never runs far ahead.
        let (tx, rx) = mpsc::channel::<Result<pb::Response, Status>>(64);

        let ingestion_client = self.ingestion_client.clone();
        let schemas = self.schemas.clone();

        // Drive the stream on a task that inherits the current TaskCenter context (the ingestion
        // client resolves partitions and spawns per-partition sessions through it).
        tokio::spawn(
            async move {
                let mut driver = StreamDriver {
                    ingestion_client,
                    schemas,
                    inbound,
                    tx,
                };
                if let Err(err) = driver.run().await {
                    // The stream is already being torn down; nothing else to do but note it.
                    debug!("Ingestion stream ended: {err}");
                }
            }
            .in_current_tc(),
        );

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

struct StreamDriver<T, Schemas> {
    ingestion_client: IngestionClient<T, Envelope>,
    schemas: Live<Schemas>,
    inbound: Streaming<pb::Request>,
    tx: mpsc::Sender<Result<pb::Response, Status>>,
}

impl<T, Schemas> StreamDriver<T, Schemas>
where
    T: TransportConnect,
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    async fn run(&mut self) -> Result<(), Status> {
        // 1. The client sends Settings once, before any record.
        let settings = match self.inbound.message().await? {
            Some(pb::Request {
                payload: Some(pb::request::Payload::Settings(settings)),
            }) => settings,
            Some(_) => {
                return Err(Status::invalid_argument(
                    "the first message on the stream must be Settings",
                ));
            }
            None => return Ok(()),
        };
        let producer_id = if settings.producer_id.is_empty() {
            // An explicitly empty producer id disables deduplication for the whole stream.
            None
        } else {
            Some(hash_producer_id(&settings.producer_id))
        };
        debug!(
            producer_id = %settings.producer_id,
            "Opened ingestion stream"
        );

        // 2. Bootstrap the flow: the client stays paused until it sees the first window.
        self.send_ack(None, INITIAL_WINDOW).await?;

        // 3. Interleave reading records with awaiting their durable commits.
        let mut inflight = FuturesUnordered::new();
        let mut ack = AckTracker::default();

        loop {
            tokio::select! {
                inbound = self.inbound.message() => {
                    match inbound? {
                        // Client half-closed: stop reading, drain the rest below.
                        None => break,
                        Some(pb::Request { payload: Some(pb::request::Payload::Record(record)) }) => {
                            let offset = record.offset;
                            let envelope = match build_envelope(&self.schemas, &settings, producer_id, record) {
                                Ok(envelope) => envelope,
                                Err(err) => {
                                    warn!("Rejecting record at offset {offset}: {err}");
                                    self.send_error(err.kind(), err.to_string()).await?;
                                    return Ok(());
                                }
                            };
                            match self.ingestion_client.ingest(envelope.partition_key(), envelope).await {
                                Ok(commit) => {
                                    ack.on_submitted(offset);
                                    inflight.push(async move { (offset, commit.await) });
                                }
                                Err(err) => {
                                    warn!("Failed to enqueue record at offset {offset}: {err}");
                                    self.send_error(pb::ErrorKind::Unknown, format!("ingestion failed: {err}")).await?;
                                    return Ok(());
                                }
                            }
                        }
                        Some(pb::Request { payload: Some(pb::request::Payload::Settings(_)) }) => {
                            return Err(Status::invalid_argument("Settings may only be sent once, at the start of the stream"));
                        }
                        Some(pb::Request { payload: None }) => {}
                    }
                }
                Some((offset, commit)) = inflight.next(), if !inflight.is_empty() => {
                    self.on_commit(offset, commit, &mut ack).await?;
                }
            }
        }

        // Drain the records still in flight after the client half-closed.
        while let Some((offset, commit)) = inflight.next().await {
            self.on_commit(offset, commit, &mut ack).await?;
        }

        Ok(())
    }

    /// Handle a single record's durable commit: advance the watermark and replenish one credit.
    async fn on_commit(
        &self,
        offset: u64,
        commit: Result<(), restate_ingestion_client::CancelledError>,
        ack: &mut AckTracker,
    ) -> Result<(), Status> {
        match commit {
            Ok(()) => {
                let last_committed = ack.on_committed(offset);
                self.send_ack(last_committed, 1).await
            }
            Err(_cancelled) => {
                // The partition session was closed before the record committed (e.g. shutdown).
                // Report a retryable error so the client reconnects and re-sends from its watermark.
                warn!("Record at offset {offset} was cancelled before commit");
                self.send_error(pb::ErrorKind::Unknown, "ingestion cancelled")
                    .await
            }
        }
    }

    /// Send a window update, optionally carrying an advanced `last_committed` watermark.
    async fn send_ack(&self, last_committed: Option<u64>, increment: u64) -> Result<(), Status> {
        self.send(pb::Response {
            last_committed,
            response: Some(pb::response::Response::Ack(pb::WindowUpdate { increment })),
        })
        .await
    }

    async fn send_error(
        &self,
        kind: pb::ErrorKind,
        message: impl Into<String>,
    ) -> Result<(), Status> {
        self.send(pb::Response {
            last_committed: None,
            response: Some(pb::response::Response::Error(pb::Error {
                kind: kind as i32,
                message: message.into(),
            })),
        })
        .await
    }

    async fn send(&self, response: pb::Response) -> Result<(), Status> {
        self.tx
            .send(Ok(response))
            .await
            .map_err(|_| Status::cancelled("client closed the stream"))
    }
}

/// Tracks the highest gap-free committed offset (the `last_committed` watermark).
///
/// Records from one stream fan out across Restate partitions, so their commits complete out of
/// order. We may only report a watermark once every offset up to it has committed, so that the
/// client — which re-seeks to `last_committed + 1` on reconnect — never skips an uncommitted record.
#[derive(Default)]
struct AckTracker {
    /// Committed offsets not yet folded into the contiguous prefix.
    committed: BTreeSet<u64>,
    /// The next offset needed to advance the watermark (the first submitted offset, then +1 each
    /// time the prefix grows). `None` until the first record is submitted.
    next_needed: Option<u64>,
    /// The last watermark we reported.
    watermark: Option<u64>,
}

impl AckTracker {
    fn on_submitted(&mut self, offset: u64) {
        // Records arrive in increasing offset order, so the first one anchors the prefix.
        self.next_needed.get_or_insert(offset);
    }

    /// Record `offset` as committed and return the watermark if it advanced.
    ///
    /// Note: this assumes gap-free offsets per producer (as a Kafka partition provides). A genuine
    /// gap (e.g. a compacted topic) would stall the watermark — acceptable for the PoC.
    fn on_committed(&mut self, offset: u64) -> Option<u64> {
        self.committed.insert(offset);
        let mut advanced = false;
        while let Some(next) = self.next_needed {
            if self.committed.remove(&next) {
                self.watermark = Some(next);
                self.next_needed = Some(next + 1);
                advanced = true;
            } else {
                break;
            }
        }
        advanced.then_some(self.watermark).flatten()
    }
}

#[derive(Debug, thiserror::Error)]
enum BuildError {
    #[error("no target service configured in Settings or Record")]
    MissingService,
    #[error("no target handler configured in Settings or Record")]
    MissingHandler,
    #[error("unknown service '{0}'")]
    UnknownService(String),
    #[error("unknown handler '{service}/{handler}'")]
    UnknownHandler { service: String, handler: String },
    #[error("target '{0}' is a virtual object / workflow but the record has no key")]
    MissingKey(String),
}

impl BuildError {
    fn kind(&self) -> pb::ErrorKind {
        match self {
            // Permanent misconfiguration: the client treats these as fatal and exits.
            BuildError::MissingService | BuildError::UnknownService(_) => {
                pb::ErrorKind::UnknownService
            }
            BuildError::MissingHandler | BuildError::UnknownHandler { .. } => {
                pb::ErrorKind::UnknownHandler
            }
            // A per-record data problem; retryable from the client's perspective.
            BuildError::MissingKey(_) => pb::ErrorKind::Unknown,
        }
    }
}

/// Build the WAL envelope for one record, mirroring `restate-ingress-kafka`'s `EnvelopeBuilder`.
fn build_envelope<Schemas>(
    schemas: &Live<Schemas>,
    settings: &pb::Settings,
    producer_id: Option<u128>,
    record: pb::Record,
) -> Result<Envelope, BuildError>
where
    Schemas: InvocationTargetResolver,
{
    let service = record
        .service
        .as_deref()
        .or(settings.service.as_deref())
        .ok_or(BuildError::MissingService)?;
    let handler = record
        .handler
        .as_deref()
        .or(settings.handler.as_deref())
        .ok_or(BuildError::MissingHandler)?;

    let schemas = schemas.pinned();
    let target_meta = schemas
        .resolve_latest_invocation_target(service, handler)
        .ok_or_else(|| {
            if schemas.resolve_latest_service_type(service).is_none() {
                BuildError::UnknownService(service.to_owned())
            } else {
                BuildError::UnknownHandler {
                    service: service.to_owned(),
                    handler: handler.to_owned(),
                }
            }
        })?;

    let invocation_target = match target_meta.target_ty {
        InvocationTargetType::Service => InvocationTarget::service(service, handler),
        InvocationTargetType::VirtualObject(handler_ty) => {
            let key = record
                .key
                .ok_or_else(|| BuildError::MissingKey(service.to_owned()))?;
            InvocationTarget::virtual_object(service, key, handler, handler_ty)
        }
        InvocationTargetType::Workflow(handler_ty) => {
            let key = record
                .key
                .ok_or_else(|| BuildError::MissingKey(service.to_owned()))?;
            InvocationTarget::workflow(service, key, handler, handler_ty)
        }
    };

    let invocation_retention = target_meta.compute_retention(false);

    // The default Settings headers plus this record's own headers plus W3C trace context.
    let mut headers = Vec::with_capacity(settings.headers.len() + record.additional_headers.len());
    for (name, value) in &settings.headers {
        headers.push(Header::new(name.as_str(), value.as_str()));
    }
    for (name, value) in &record.additional_headers {
        headers.push(Header::new(name.as_str(), value.as_str()));
    }
    if let Some(traceparent) = &record.traceparent {
        headers.push(Header::new("traceparent", traceparent.as_str()));
    }
    if let Some(tracestate) = &record.tracestate {
        headers.push(Header::new("tracestate", tracestate.as_str()));
    }

    // Derive a deterministic partition key from (producer, offset) so re-sends of the same record
    // land on the same partition, where the high-water-mark dedup will drop the duplicate.
    let seed = PartitionKeySeed {
        producer: producer_id.unwrap_or_default(),
        offset: record.offset,
    };
    let invocation_id = InvocationId::generate_or_else(&invocation_target, None, || {
        partitioner::HashPartitioner::compute_partition_key(&seed)
    });

    let mut invocation = Box::new(ServiceInvocation::initialize(
        invocation_id,
        invocation_target,
        restate_types::invocation::Source::Ingress(PartitionProcessorRpcRequestId::default()),
    ));
    invocation.argument = record.payload;
    invocation.headers = headers;
    invocation.with_retention(invocation_retention);

    let dedup = producer_id.map(|producer| DedupInformation::producer(producer, record.offset));
    let header = restate_wal_protocol::Header {
        source: restate_wal_protocol::Source::Ingress {},
        dest: Destination::Processor {
            partition_key: invocation.partition_key(),
            dedup,
        },
    };

    Ok(Envelope::new(header, Command::Invoke(invocation)))
}

#[derive(Hash)]
struct PartitionKeySeed {
    producer: u128,
    offset: u64,
}

/// Hash a producer-id string into the numeric producer id used by the WAL dedup table.
fn hash_producer_id(producer_id: &str) -> u128 {
    xxhash_rust::xxh3::xxh3_128(producer_id.as_bytes())
}

/// The ingress connection service branches to gRPC on this content-type.
pub(crate) fn is_grpc_request<B>(req: &Request<B>) -> bool {
    req.headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/grpc"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_advances_only_over_the_contiguous_committed_prefix() {
        let mut ack = AckTracker::default();
        for offset in 0..3 {
            ack.on_submitted(offset);
        }

        // Committing out of order does not advance past a gap...
        assert_eq!(ack.on_committed(2), None);
        // ...until the gap is filled, at which point it jumps to the far end.
        assert_eq!(ack.on_committed(0), Some(0));
        assert_eq!(ack.on_committed(1), Some(2));
    }

    #[test]
    fn watermark_anchors_at_the_first_submitted_offset_after_a_reseek() {
        // A reconnecting client re-seeks to last_committed + 1, so the stream's first offset is
        // not necessarily 0; the very first commit must still report that offset as committed.
        let mut ack = AckTracker::default();
        ack.on_submitted(5);

        assert_eq!(ack.on_committed(5), Some(5));
    }

    #[test]
    fn grpc_requests_are_detected_by_content_type() {
        let grpc = Request::builder()
            .header(http::header::CONTENT_TYPE, "application/grpc")
            .body(())
            .unwrap();
        let rest = Request::builder()
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(())
            .unwrap();

        assert!(is_grpc_request(&grpc));
        assert!(!is_grpc_request(&rest));
    }
}
