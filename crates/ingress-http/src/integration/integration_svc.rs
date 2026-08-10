// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `IntegrationSvc` gRPC service and its streaming ingestion logic.
//!
//! ## The `Ingest` stream
//!
//! `Ingest` is a bidirectional gRPC stream. The client sends a sequence of
//! [`IngestionRequest`] frames and the server replies with [`IngestionResponse`]
//! frames. The request stream must begin with a `Start` frame; afterwards the
//! client sends `Settings` frames (updating the per-record defaults) and
//! `Invocation` frames (the actual records to ingest). The wire contract is
//! defined in `protobuf/integration_svc.proto`.
//!
//! Each request stream is driven by an [`IngestionStream`] state machine, unfolded
//! into the response stream via [`IngestionStream::step`]. The machine moves
//! through [`State::WaitingStart`] → [`State::Processing`] → [`State::Terminated`].
//!
//! ## Flow control (send window)
//!
//! Ingestion is flow-controlled with a byte-based send window, mirroring the
//! `WindowUpdate` frame in the proto. The server tracks
//! [`ProcessorState::current_window_size`]: every `Invocation` received subtracts
//! its encoded size, and every committed record replenishes the window by the same
//! amount. A [`WindowUpdate`] response returns credit to the client and doubles as
//! an ack of `Response.last_committed`.
//!
//! To avoid a response per commit, the server only yields a `WindowUpdate` when
//! there is nothing left in flight or when the window has dropped to
//! [`UPDATE_WINDOW_THRESHOLD`] of its maximum (see
//! [`IngestionStream::should_yield`]). The client may let the window go negative
//! for a single oversized invocation, but sending more once it is depleted is a
//! protocol violation and results in a `GO_AWAY` error.
//!
//! ## Committing records
//!
//! For each `Invocation` the server resolves the invocation target against the
//! schema, builds a WAL [`Envelope`] (see [`IngestionStream::build_envelope`]) and
//! hands it to the [`IngestionClient`]. The returned [`RecordCommit`] future is
//! pushed onto [`ProcessorState::inflight`] and awaited in order, so
//! `last_committed` advances monotonically and back-pressure from the log flows
//! back to the client through the window.

use std::collections::VecDeque;
use std::hash::Hash;
use std::num::NonZeroU64;
use std::time::Duration;

use futures::future::OptionFuture;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use opentelemetry::global::ObjectSafeSpan;
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{SpanContext, TraceContextExt};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use prost::Message;
use tokio_util::time::FutureExt;
use tonic::{Request, Response, Status, Streaming};

use restate_core::network::TransportConnect;
use restate_ingestion_client::{IngestionClient, IngestionError, RecordCommit};
use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::identifiers::{
    DeploymentId, InvocationId, PartitionProcessorRpcRequestId, partitioner,
};
use restate_types::invocation::{
    Header, InvocationTarget, InvocationTargetType, ServiceInvocation, SpanRelation,
    WorkflowHandlerType,
};
use restate_types::limit_key::parse_limit_key;
use restate_types::live::Live;
use restate_types::schema::invocation_target::{DeploymentStatus, InvocationTargetResolver};
use restate_types::sharding::WithPartitionKey;
use restate_types::time::MillisSinceEpoch;
use restate_types::{Scope, limit_key};
use restate_util_string::{ReString, RestrictedValue, RestrictedValueError};
use restate_wal_protocol::{Command, DedupInformation, Destination, Envelope};

/// Generated protobuf bindings for `dev.restate.ingress.integration`
/// (see `protobuf/integration_svc.proto`).
pub mod proto {
    tonic::include_proto!("dev.restate.ingress.integration.v1");
}

use proto::integration_svc_server::{IntegrationSvc, IntegrationSvcServer};
use proto::{
    Invocation as IngestionInvocation, Request as IngestionRequest, Response as IngestionResponse,
    Settings, WindowUpdate, request, response,
};
use tracing::debug;

use crate::integration::integration_svc::Error::BadRequestWithOffset;
use crate::metric_definitions::INTEGRATION_INGESTED;

/// Builds the tonic server that serves [`IntegrationSvc`], wrapping a fresh
/// [`IntegrationService`] configured with the given ingestion client, schema
/// resolver and maximum send-window size.
pub(crate) fn integration_server<T, Schemas>(
    ingestion_client: IngestionClient<T, Envelope>,
    schemas: Live<Schemas>,
    max_window_size: NonZeroU64,
) -> IntegrationSvcServer<IntegrationService<T, Schemas>>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    IntegrationSvcServer::new(IntegrationService::new(
        ingestion_client,
        schemas,
        max_window_size,
    ))
}

/// Window-replenishment threshold, as a percentage of the maximum window size.
///
/// While records are still in flight the server withholds `WindowUpdate`
/// responses until the remaining window drops to this percentage of its maximum,
/// batching acks instead of replying after every commit. See
/// [`IngestionStream::should_yield`].
const UPDATE_WINDOW_THRESHOLD: u64 = 20; // 20% of max window size

/// The [`IntegrationSvc`] implementation.
///
/// Cheaply cloneable: tonic clones the service per connection, so every field is
/// shared/handle-like. It holds the [`IngestionClient`] used to append envelopes to
/// the WAL, a [`Live`] handle to the schema (re-pinned per use to pick up updates)
/// and the configured maximum send-window size handed to each stream.
#[derive(Clone)]
pub(crate) struct IntegrationService<T, Schemas> {
    ingestion_client: IngestionClient<T, Envelope>,
    schemas: Live<Schemas>,
    max_window_size: NonZeroU64,
}

impl<T, Schemas> IntegrationService<T, Schemas> {
    fn new(
        ingestion_client: IngestionClient<T, Envelope>,
        schemas: Live<Schemas>,
        max_window_size: NonZeroU64,
    ) -> Self {
        Self {
            ingestion_client,
            schemas,
            max_window_size,
        }
    }
}

#[tonic::async_trait]
impl<T, Schemas> IntegrationSvc for IntegrationService<T, Schemas>
where
    T: TransportConnect,
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    type IngestStream = BoxStream<'static, Result<IngestionResponse, Status>>;

    async fn ingest(
        &self,
        request: Request<Streaming<IngestionRequest>>,
    ) -> Result<Response<Self::IngestStream>, Status> {
        // Snapshot the schema once so the stream owns a `'static` resolver rather
        // than borrowing `&self` across `.await` points.

        let stream = IngestionStream::new(
            request.into_inner(),
            self.ingestion_client.clone(),
            self.schemas.clone(),
            self.max_window_size,
        );
        let response = futures::stream::unfold(stream, IngestionStream::step);

        Ok(Response::new(response.boxed()))
    }
}

struct IngestionStream<T, S, Schemas> {
    inbound: S,
    ingestion_client: IngestionClient<T, Envelope>,
    schemas: Live<Schemas>,
    state: State,
    max_window_size: NonZeroU64,
}

/// The lifecycle state of an [`IngestionStream`].
enum State {
    /// Initial state: no `Start` frame has been received yet. The next step
    /// reads and validates the mandatory `Start` frame.
    WaitingStart,
    /// A `Start` frame was accepted; the boxed [`ProcessorState`] holds the
    /// per-stream context (settings, window size, inflight commits) while
    /// records are being ingested. Boxed to keep the enum (and the future that
    /// holds it) small, since this is the large variant.
    Processing { state: Box<ProcessorState> },
    /// Terminal state: the stream has ended (client drained, fatal error, or a
    /// duplicate `Start`). No further frames are produced.
    Terminated,
}

impl<T, S, Schemas> IngestionStream<T, S, Schemas>
where
    T: TransportConnect,
    S: Stream<Item = Result<IngestionRequest, Status>> + Unpin,
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    fn new(
        inbound: S,
        ingestion_client: IngestionClient<T, Envelope>,
        schemas: Live<Schemas>,
        max_window_size: NonZeroU64,
    ) -> Self {
        Self {
            inbound,
            ingestion_client,
            schemas,
            state: State::WaitingStart,
            max_window_size,
        }
    }

    /// Advances the state machine by one step, returning the next response frame
    async fn step(mut self) -> Option<(Result<IngestionResponse, Status>, Self)> {
        loop {
            let state = std::mem::replace(&mut self.state, State::Terminated);

            match state {
                State::Terminated => return None,
                State::WaitingStart => {
                    let state = match self.wait_start().await {
                        Ok(settings) => settings,
                        Err(err) => {
                            let response = IngestionResponse {
                                last_committed: None,
                                response: Some(response::Response::Error(err.into())),
                            };

                            self.state = State::Terminated;
                            return Some((Ok(response), self));
                        }
                    };

                    self.state = State::Processing {
                        state: Box::new(state),
                    };
                }
                State::Processing { mut state } => {
                    let result = match self.process(&mut state).await {
                        Ok(result) => {
                            let last_committed = state.last_committed;
                            let (increment_bytes, state) = match result {
                                ProcessorResult::Continue(increment_bytes) => {
                                    // Credit granted here is exactly the credit the client
                                    // is told about, keeping both sides in sync. It cannot
                                    // push the window past `max_window_size`: the bytes
                                    // consumed by an invocation are only ever returned once
                                    // its record commits, and the one grant that is not a
                                    // replenishment (the initial window in `process`) is
                                    // reachable only before any credit has been handed out.
                                    state.current_window_size += increment_bytes as i64;
                                    (increment_bytes, State::Processing { state })
                                }
                                ProcessorResult::Terminate => (0, State::Terminated),
                            };

                            self.state = state;

                            IngestionResponse {
                                last_committed,
                                response: Some(response::Response::WindowUpdate(WindowUpdate {
                                    increment_bytes,
                                })),
                            }
                        }
                        Err(err) => IngestionResponse {
                            last_committed: state.last_committed,
                            response: Some(response::Response::Error(err.into())),
                        },
                    };

                    return Some((Ok(result), self));
                }
            }
        }
    }

    fn validate_settings(&self, settings: &Settings) -> Result<(), Error> {
        let schemas = self.schemas.pinned();

        let service = settings.service.as_deref();
        let handler = settings.handler.as_deref();

        match (service, handler) {
            (None, None) => Ok(()),
            (Some(service), None) => schemas
                .resolve_latest_service_type(service)
                .map(|_| ())
                .ok_or_else(|| NotFoundError::UnknownService {
                    service: service.to_owned(),
                }),
            (None, Some(_)) => {
                // nothing we can do here, this can fail
                // later when ingesting an invocation if
                // the handler doesn't exist
                Ok(())
            }
            (Some(service), Some(handler)) => schemas
                .resolve_latest_invocation_target(service, handler)
                .map(|_| ())
                .ok_or_else(|| NotFoundError::UnknownHandler {
                    service: service.to_owned(),
                    handler: handler.to_owned(),
                }),
        }
        .map_err(Error::NotFound)?;

        // scope extraction
        // todo: Store the parsed scope and limit key on the setting objects
        // and save few cycles in case invocations don't have their own scope/limit-key
        let scope = settings
            .scope
            .as_deref()
            .map(|scope| RestrictedValue::new(ReString::from(scope)).map(Scope::new))
            .transpose()
            .map_err(|err| Error::BadRequest(BadRequestError::InvalidScope(err)))?;

        if scope.is_some()
            && !Configuration::pinned()
                .common
                .experimental
                .is_vqueues_enabled()
        {
            return Err(Error::BadRequest(BadRequestError::UnexpectedScope));
        }

        if scope.is_none() && settings.limit_key.is_some() {
            return Err(Error::BadRequest(BadRequestError::UnexpectedLimitKey));
        }

        settings
            .limit_key
            .as_deref()
            .map(parse_limit_key::<ReString>)
            .transpose()
            .map_err(|err| Error::BadRequest(BadRequestError::InvalidLimitKey(err)))?
            .unwrap_or_default();

        Ok(())
    }

    /// Reads the first inbound frame and requires it to be a valid `Start`.
    async fn wait_start(&mut self) -> Result<ProcessorState, Error> {
        let first = self
            .inbound
            .next()
            .timeout(Duration::from_secs(2))
            .await
            .map_err(|_| Error::GoAway(GoAwayError::Timeout))?
            .ok_or_else(|| Error::GoAway(GoAwayError::ExpectingStartMessage))?
            .map_err(|status| {
                Error::GoAway(GoAwayError::Unknown(
                    format!("Received status: {status}").into(),
                ))
            })?;

        let payload = first
            .payload
            .ok_or_else(|| Error::GoAway(GoAwayError::MissingRequestPayload))?;

        let request::Payload::Start(start) = payload else {
            return Err(Error::GoAway(GoAwayError::ExpectingStartMessage));
        };

        let settings = start.settings;
        if let Some(ref settings) = settings {
            self.validate_settings(settings)?;
        }

        debug!(
            producer = start.producer_id,
            integration = start.integration,
            "Start processing ingestion stream"
        );

        Ok(ProcessorState::new(
            start.producer_id,
            start.integration,
            settings.unwrap_or_default(),
        ))
    }

    /// Runs one processing burst and returns when it is time to emit a response.
    ///
    /// Before doing any work, if there is nothing in flight and no window credit,
    /// it returns `Continue(max_window_size)` so the very first response advertises
    /// the initial window to the client.
    ///
    /// Otherwise it drives a `select!` loop that concurrently:
    /// * pulls the next inbound frame and applies it via [`Self::handle_incoming`]
    ///   (buffering the resulting commit future in `inflight`), and
    /// * awaits the oldest inflight commit; on completion it pops it, advances
    ///   `last_committed`, bumps the ingested counter and accumulates the bytes to
    ///   `replenish_size`.
    ///
    /// It returns `Continue(replenish_size)` (asking the server to send a
    /// `WindowUpdate`) once nothing is left in flight or [`Self::should_yield`]
    /// signals the window is low. When the inbound stream ends it drains the
    /// remaining inflight commits in order and returns `Terminate`.
    async fn process(&mut self, state: &mut ProcessorState) -> Result<ProcessorResult, Error> {
        if state.inflight.is_empty() && state.current_window_size <= 0 {
            // yielding now will send a window update message
            // to communicate the initial server window sizes.
            return Ok(ProcessorResult::Continue(self.max_window_size.get()));
        }

        let mut replenish_size: u64 = 0;

        loop {
            let head = OptionFuture::from(state.inflight.front_mut());
            tokio::select! {
                incoming = self.inbound.next() => {
                    let Some(incoming) = incoming else {
                        // drain.
                        break;
                    };

                    self.handle_incoming(state, incoming).await?;
                }
                Some(committed) = head => {
                    // a record has been committed. It's now safe to
                    state.inflight.pop_front();
                    let (offset, invocation_size) = committed.map_err(|_| Error::Shutdown)?;

                    state.last_committed = Some(offset);
                    state.ingested_counter.increment(1);
                    replenish_size += invocation_size;

                    if state.inflight.is_empty() || self.should_yield(state){
                        // yielding now will force the stream to send a
                        // window update message to restore the window size
                        // and also update the last committed offset.
                        return Ok(ProcessorResult::Continue(replenish_size));
                    }
                }
            }
        }

        debug!(
            integration = %state.integration,
            producer = %state.producer,
            "Draining inflight records"
        );

        // draining of inflight commits
        for commit in state.inflight.drain(..) {
            let (offset, _) = commit.await.map_err(|_| Error::Shutdown)?;
            state.last_committed = Some(offset);
            state.ingested_counter.increment(1);
        }

        Ok(ProcessorResult::Terminate)
    }

    /// Returns `true` when the remaining window has fallen to
    /// [`UPDATE_WINDOW_THRESHOLD`] percent (or less) of its maximum, i.e. it is
    /// time to replenish the client's credit with a `WindowUpdate`. A negative
    /// window is clamped to zero for the comparison.
    fn should_yield(&self, state: &ProcessorState) -> bool {
        let percent = (state.current_window_size.max(0) as u64 * 100) / self.max_window_size.get();
        percent <= UPDATE_WINDOW_THRESHOLD
    }

    /// Applies a single inbound frame to `state`.
    async fn handle_incoming(
        &mut self,
        state: &mut ProcessorState,
        request: Result<IngestionRequest, Status>,
    ) -> Result<(), Error> {
        let request = request.map_err(|status| Error::GoAway(GoAwayError::unknown(status)))?;

        let Some(payload) = request.payload else {
            return Err(Error::GoAway(GoAwayError::MissingRequestPayload));
        };

        match payload {
            request::Payload::Start(_) => {
                return Err(Error::GoAway(GoAwayError::UnexpectedStartMessage));
            }
            request::Payload::Settings(settings) => {
                self.validate_settings(&settings)?;
                state.settings = settings;
            }
            request::Payload::Invocation(invocation) => {
                if state.current_window_size < 0 {
                    return Err(Error::GoAway(GoAwayError::WindowSizeViolation));
                }

                // it's okay if window size goes below zero for a single message
                // but the client should not send more unless it receives a
                // window update.
                let invocation_size = invocation.encoded_len();
                state.current_window_size -= invocation_size as i64;
                let offset = invocation.offset;
                let envelope = self.build_envelope(state, invocation)?;

                let commit = self
                    .ingestion_client
                    .ingest(envelope.partition_key(), envelope)
                    .await?
                    .map(|_| (offset, invocation_size as u64));

                state.inflight.push_back(commit);
            }
        }

        Ok(())
    }

    /// Builds the WAL [`Envelope`] for one record, mirroring
    /// `restate-ingress-kafka`'s `EnvelopeBuilder`.
    fn build_envelope(
        &self,
        state: &ProcessorState,
        record: IngestionInvocation,
    ) -> Result<Envelope, Error>
    where
        Schemas: InvocationTargetResolver,
    {
        let execution_time = match (record.delay_ms, record.invoke_time_ts_ms) {
            (None, None) => None,
            (Some(_), Some(_)) => {
                return Err(Error::BadRequestWithOffset(
                    record.offset,
                    BadRequestError::ConflictingExecutionTime,
                ));
            }
            (Some(delay_ms), None) => {
                Some(MillisSinceEpoch::after(Duration::from_millis(delay_ms)))
            }
            (None, Some(ts)) => Some(MillisSinceEpoch::from(ts)),
        };

        let service = record
            .service
            .as_deref()
            .or(state.settings.service.as_deref())
            .ok_or(Error::BadRequestWithOffset(
                record.offset,
                BadRequestError::MissingService,
            ))?;
        let handler = record
            .handler
            .as_deref()
            .or(state.settings.handler.as_deref())
            .ok_or(Error::BadRequestWithOffset(
                record.offset,
                BadRequestError::MissingHandler,
            ))?;

        let schemas = self.schemas.pinned();
        let target_meta = schemas
            .resolve_latest_invocation_target(service, handler)
            .ok_or_else(|| {
                if schemas.resolve_latest_service_type(service).is_none() {
                    Error::NotFoundWithOffset(
                        record.offset,
                        NotFoundError::UnknownService {
                            service: service.to_owned(),
                        },
                    )
                } else {
                    Error::NotFoundWithOffset(
                        record.offset,
                        NotFoundError::UnknownHandler {
                            service: service.to_owned(),
                            handler: handler.to_owned(),
                        },
                    )
                }
            })?;

        drop(schemas);

        if let DeploymentStatus::Deprecated(id) = target_meta.deployment_status {
            return Err(BadRequestWithOffset(
                record.offset,
                BadRequestError::DeprecatedDeployment(id),
            ));
        }

        // Scope extraction
        // todo: If we parse and store scope and limit-keys
        // from settings, we can save few cpu cycles here
        let scope = record
            .scope
            .as_deref()
            .or(state.settings.scope.as_deref())
            .map(|scope| RestrictedValue::new(ReString::from(scope)).map(Scope::new))
            .transpose()
            .map_err(|err| {
                Error::BadRequestWithOffset(record.offset, BadRequestError::InvalidScope(err))
            })?;

        if scope.is_some()
            && !Configuration::pinned()
                .common
                .experimental
                .is_vqueues_enabled()
        {
            return Err(Error::BadRequestWithOffset(
                record.offset,
                BadRequestError::UnexpectedScope,
            ));
        }

        if scope.is_none() && record.limit_key.is_some() {
            return Err(Error::BadRequestWithOffset(
                record.offset,
                BadRequestError::UnexpectedLimitKey,
            ));
        }

        let limit_key = record
            .limit_key
            .as_deref()
            .or(state.settings.limit_key.as_deref())
            .map(parse_limit_key::<ReString>)
            .transpose()
            .map_err(|err| {
                Error::BadRequestWithOffset(record.offset, BadRequestError::InvalidLimitKey(err))
            })?
            .unwrap_or_default();

        let invocation_target = match target_meta.target_ty {
            InvocationTargetType::Service => {
                if record
                    .key
                    .as_deref()
                    .or(state.settings.key.as_deref())
                    .is_some()
                {
                    return Err(Error::BadRequestWithOffset(
                        record.offset,
                        BadRequestError::UnexpectedKey,
                    ));
                }
                InvocationTarget::service(service, handler)
            }
            InvocationTargetType::VirtualObject(handler_ty) => {
                let key = record
                    .key
                    .as_deref()
                    .or(state.settings.key.as_deref())
                    .ok_or_else(|| {
                        Error::BadRequestWithOffset(record.offset, BadRequestError::MissingKey)
                    })?;

                match scope {
                    Some(scope) => InvocationTarget::scoped_virtual_object(
                        service, key, handler, handler_ty, scope,
                    ),
                    None => InvocationTarget::virtual_object(service, key, handler, handler_ty),
                }
            }
            InvocationTargetType::Workflow(handler_ty) => {
                let key = record
                    .key
                    .as_deref()
                    .or(state.settings.key.as_deref())
                    .ok_or_else(|| {
                        Error::BadRequestWithOffset(record.offset, BadRequestError::MissingKey)
                    })?;

                match scope {
                    Some(scope) => {
                        InvocationTarget::scoped_workflow(service, key, handler, handler_ty, scope)
                    }
                    None => InvocationTarget::workflow(service, key, handler, handler_ty),
                }
            }
        };

        let idempotency_key = record
            .idempotency_key
            .as_deref()
            .or(state.settings.idempotency_key.as_deref());

        if idempotency_key.is_some()
            && target_meta.target_ty
                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            return Err(Error::BadRequestWithOffset(
                record.offset,
                BadRequestError::UnexpectedIdempotencyKey,
            ));
        }

        let invocation_retention = target_meta.compute_retention(idempotency_key.is_some());

        // The default Settings headers plus this record's own headers plus W3C trace context.
        let mut headers =
            Vec::with_capacity(state.settings.headers.len() + record.additional_headers.len());
        for (name, value) in &state.settings.headers {
            headers.push(Header::new(name.as_str(), value.as_str()));
        }
        for (name, value) in &record.additional_headers {
            headers.push(Header::new(name.as_str(), value.as_str()));
        }

        let seed = PartitionKeySeed {
            producer: state.producer_id,
            offset: record.offset,
        };

        let invocation_id =
            InvocationId::generate_or_else(&invocation_target, idempotency_key, || {
                partitioner::HashPartitioner::compute_partition_key(seed)
            });

        let span_context = prepare_tracing_span(
            &invocation_id,
            &invocation_target,
            &state.integration,
            &state.producer,
            TraceContextExtractor {
                trace_parent: record.traceparent.as_deref(),
                trace_state: record.tracestate.as_deref(),
            },
            record.offset,
        );

        let mut invocation = Box::new(ServiceInvocation::initialize(
            invocation_id,
            invocation_target,
            restate_types::invocation::Source::Ingress(PartitionProcessorRpcRequestId::default()),
        ));

        invocation.with_related_span(SpanRelation::parent(span_context));
        invocation.argument = record.payload;
        invocation.headers = headers;
        invocation.idempotency_key = idempotency_key.map(Into::into);
        invocation.with_retention(invocation_retention);
        invocation.execution_time = execution_time;
        invocation.limit_key = limit_key;

        let dedup = state
            .producer_id
            .map(|producer| DedupInformation::producer(producer, record.offset));

        let header = restate_wal_protocol::Header {
            source: restate_wal_protocol::Source::Ingress {},
            dest: Destination::Processor {
                partition_key: invocation.partition_key(),
                dedup,
            },
        };

        Ok(Envelope::new(header, Command::Invoke(invocation)))
    }
}

#[allow(clippy::too_many_arguments)]
fn prepare_tracing_span(
    invocation_id: &InvocationId,
    invocation_target: &InvocationTarget,
    integration: &str,
    producer: &str,
    extractor: TraceContextExtractor<'_>,
    offset: u64,
) -> SpanContext {
    let tracing_context = TraceContextPropagator::new().extract(&extractor);
    let inbound_span = tracing_context.span();

    let relation = if inbound_span.span_context().is_valid() {
        SpanRelation::parent(inbound_span.span_context())
    } else {
        SpanRelation::None
    };

    let span = restate_tracing_instrumentation::info_invocation_span!(
        relation = relation,
        prefix = "integration",
        id = invocation_id,
        target = invocation_target,
        tags = (
            messaging.system = "integration",
            messaging.operation.type = "process",
            messaging.integration = integration.to_owned(),
            messaging.integration.offset = offset as i64,
            messaging.integration.producer = producer.to_owned()
        )
    );

    span.span_context().clone()
}

struct TraceContextExtractor<'a> {
    trace_parent: Option<&'a str>,
    trace_state: Option<&'a str>,
}

impl<'a> Extractor for TraceContextExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.trace_parent,
            "tracestate" => self.trace_state,
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        vec!["traceparent", "tracestate"]
    }
}
/// Seed hashed into a partition key for records that have no idempotency key.
///
/// Combining the `producer` id with the record `offset` keeps the derived
/// [`InvocationId`] deterministic and stable per producer, so retrying the same
/// record yields the same key.
#[derive(Hash)]
struct PartitionKeySeed {
    producer: Option<u128>,
    offset: u64,
}

/// Outcome of one [`IngestionStream::process`] burst.
enum ProcessorResult {
    /// Keep processing; the payload is the number of bytes to add back to the
    /// window in the emitted `WindowUpdate`.
    Continue(u64),
    /// The inbound stream ended and all inflight records were drained; the
    /// stream should transition to [`State::Terminated`].
    Terminate,
}

/// A committed log offset, as reported back to the client via
/// `Response.last_committed`.
type CommittedOffset = u64;

/// Errors raised while processing a stream. Each maps to a [`proto::Error`]
/// (with an [`proto::ErrorKind`]) via the [`From`] impl below. Variants suffixed
/// with `WithOffset` carry the offset of the record that caused them so the
/// client can associate the error with a specific invocation.
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Ingress is shutting down")]
    Shutdown,
    #[error("Protocol violation: {0}")]
    GoAway(GoAwayError),
    #[error("Invocation target not found: {0}")]
    NotFound(NotFoundError),
    #[error("Invocation target not found for invocation at offset {0}: {1}")]
    NotFoundWithOffset(u64, NotFoundError),
    #[error("Bad request: {0}")]
    BadRequest(BadRequestError),
    #[error("Bad request for invocation at offset {0}: {1}")]
    BadRequestWithOffset(u64, BadRequestError),
    #[error("Internal ingestion error: {0}")]
    IngestionError(#[from] IngestionError),
}

impl From<Error> for proto::Error {
    fn from(value: Error) -> Self {
        let (invocation_offset, kind) = match value {
            Error::Shutdown => (None, proto::ErrorKind::ShuttingDown),
            Error::GoAway(_) => (None, proto::ErrorKind::GoAway),
            Error::IngestionError(_) => (None, proto::ErrorKind::Unknown),
            Error::NotFound(_) => (None, proto::ErrorKind::NotFound),
            Error::NotFoundWithOffset(offset, _) => (Some(offset), proto::ErrorKind::NotFound),
            Error::BadRequest(_) => (None, proto::ErrorKind::BadRequest),
            Error::BadRequestWithOffset(offset, _) => (Some(offset), proto::ErrorKind::BadRequest),
        };

        Self {
            invocation_offset,
            kind: kind.into(),
            message: value.to_string(),
        }
    }
}

/// A protocol violation by the client. Any of these maps to
/// [`proto::ErrorKind::GoAway`] and tears the stream down: the client is
/// expected to reconnect rather than continue.
#[derive(Debug, thiserror::Error)]
enum GoAwayError {
    #[error("Expecting Start message")]
    ExpectingStartMessage,
    #[error("Unexpected Start message")]
    UnexpectedStartMessage,
    #[error("window size violation")]
    WindowSizeViolation,
    #[error("Missing request payload")]
    MissingRequestPayload,
    #[error("Timeout")]
    Timeout,
    #[error(transparent)]
    Unknown(#[from] GenericError),
}

impl GoAwayError {
    fn unknown(err: impl Into<GenericError>) -> Self {
        Self::Unknown(err.into())
    }
}

/// A malformed record: a mandatory field could not be resolved from either the
/// record or the stream settings. Maps to [`proto::ErrorKind::BadRequest`].
#[allow(clippy::enum_variant_names)]
#[derive(Debug, thiserror::Error)]
enum BadRequestError {
    #[error("Missing service key")]
    MissingService,
    #[error("Missing service handler")]
    MissingHandler,
    #[error("Missing required service key")]
    MissingKey,
    #[error("Unexpected service key")]
    UnexpectedKey,
    #[error("`delay_ms` and `invocation_time_ts_ms` are mutually exclusive")]
    ConflictingExecutionTime,
    #[error("Scopes requires VQueues")]
    UnexpectedScope,
    #[error("Invalid scope: {0}")]
    InvalidScope(RestrictedValueError),
    #[error("Unexpected limit key without scope")]
    UnexpectedLimitKey,
    #[error("Invalid limit key: {0}")]
    InvalidLimitKey(limit_key::ParseError),
    #[error("Unexpected idempotency key for the handler type")]
    UnexpectedIdempotencyKey,
    #[error("Deployment '{0}' is deprecated")]
    DeprecatedDeployment(DeploymentId),
}

/// The referenced invocation target does not exist in the current schema. Maps
/// to [`proto::ErrorKind::NotFound`].
#[derive(Debug, thiserror::Error)]
enum NotFoundError {
    #[error("Unknown service {service}")]
    UnknownService { service: String },
    #[error("Unknown service handler {service}/{handler}")]
    UnknownHandler { service: String, handler: String },
}

/// Per-stream context held while in [`State::Processing`].
///
/// Created once the `Start` frame is accepted and carried for the lifetime of the
/// stream. It tracks the current defaults (`settings`), the flow-control window and
/// the queue of records awaiting commit.
struct ProcessorState {
    /// Hash of the producer id used for deduplication, or `None` when the
    /// producer id was empty (deduplication disabled for the stream).
    producer_id: Option<u128>,
    /// The raw producer id from the `Start` frame, used for metric labels and
    /// logging.
    producer: ReString,
    /// The `name/version` integration identifier from the `Start` frame.
    integration: ReString,
    /// Counter incremented once per committed record.
    ingested_counter: metrics::Counter,
    /// Current per-record defaults; replaced whenever a `Settings` frame arrives.
    settings: Settings,
    /// Highest committed offset so far, or `None` if nothing has committed yet.
    /// Offsets are 0-based, hence the `Option`.
    last_committed: Option<CommittedOffset>,
    /// Remaining send-window credit in bytes. Debited when an invocation is
    /// received and replenished when it commits; may briefly go negative for a
    /// single oversized invocation.
    current_window_size: i64,
    /// Commit futures for submitted-but-not-yet-committed records, in submission
    /// order. Each resolves to `(offset, encoded_size)`.
    inflight: VecDeque<RecordCommit<(u64, u64)>>,
}

impl ProcessorState {
    fn new(
        producer_id: impl Into<ReString>,
        integration: impl Into<ReString>,
        settings: Settings,
    ) -> Self {
        let producer = producer_id.into();
        let integration = integration.into();
        Self {
            producer_id: if producer.is_empty() {
                None
            } else {
                let mut hasher = xxhash_rust::xxh3::Xxh3::default();
                producer.hash(&mut hasher);
                Some(hasher.digest128())
            },
            producer: producer.clone(),
            ingested_counter: metrics::counter!(
                INTEGRATION_INGESTED,
                "integration" => integration.clone(),
                "producer" => producer,
            ),
            integration,
            settings,
            last_committed: None,
            current_window_size: 0,
            inflight: VecDeque::default(),
        }
    }
}
