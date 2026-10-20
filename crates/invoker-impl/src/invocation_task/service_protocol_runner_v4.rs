// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::Bytes;
use bytestring::ByteString;
use futures::{Stream, StreamExt};
use gardal::futures::StreamExt as GardalStreamExt;
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use metrics::{Counter, counter};
use opentelemetry::KeyValue;
use opentelemetry::trace::{Span, SpanContext, Status, TraceFlags};
use prost::Message as ProstMessage;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

use restate_errors::warn_it;
use restate_memory::{LocalMemoryLease, LocalMemoryPool, PinnableMemoryStream};
use restate_service_client::{Endpoint, Method, Parts, Request};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_service_protocol_v4::message_codec::{
    Decoder, Encoder, EncodingError, Message, MessageHeader, MessageType, ServiceWireDecoder,
    StateEntry, proto,
};
use restate_service_protocol_v4::{preflight, proto_lite};
use restate_tracing_instrumentation::ServiceSpan;
use restate_types::Scope;
use restate_types::config::Configuration;
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::{GenericError, InvocationError};
use restate_types::identifiers::{DeploymentId, EntryIndex, InvocationId};
use restate_types::invocation::{
    Header, InvocationTarget, InvocationTargetType, ServiceInvocationSpanContext, ServiceType,
    SpanRelation, VirtualObjectHandlerType, WorkflowHandlerType, X_RESTATE_HANDLER_KIND_HEADER,
};
use restate_types::journal;
use restate_types::journal_v2::command::{CallRequest, InputCommand};
use restate_types::journal_v2::raw::{
    CallOrSendMetadata, RawCommand, RawCommandSpecificMetadata, RawEntry, RawNotification,
};
use restate_types::journal_v2::{
    CommandIndex, CommandType, Entry, EntryMetadata, EntryType, RunCommand, RunCompletion,
    RunResult, SleepCommand, UnresolvedFuture,
};
use restate_types::limit_key::LimitKey;
use restate_types::schema::deployment::{DeploymentType, ProtocolType};
use restate_types::schema::invocation_target::{
    DeploymentStatus, InvocationTargetMetadata, InvocationTargetResolver, RequestedRetentions,
    StatePreloadPolicy,
};
use restate_types::service_protocol::{
    MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION, MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION,
    ServiceProtocolVersion,
};
use restate_util_string::{ReString, RestateString, RestrictedValue, StringLike, ToReString};
use restate_worker_api::invoker::JournalMetadata;
use restate_worker_api::invoker::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderError, InvocationReaderTransaction, JournalEntry,
    JournalKind,
};

use crate::error::{
    CommandPreconditionError, InvocationErrorRelatedCommandV2, InvokerError,
    RequestedErrorBehavior, SdkInvocationErrorV2,
};
use crate::invocation_task::{
    AttemptTarget, InvocationTask, InvocationTaskOutputInner, InvokerBodySender, InvokerBodyType,
    PREFLIGHT_CONTENT_TYPE, PreflightTarget, ResponseChunk, ResponseStream, TerminalLoopState,
    X_RESTATE_SERVER, collect_eager_state, invocation_id_to_header_value, leased_frame,
    new_invoker_body, retry_after, service_protocol_version_to_header_value,
};
use crate::metric_definitions::{
    INVOKER_CLIENT_REQUESTS, INVOKER_RECEIVED_BYTES, INVOKER_SENT_BYTES,
};
use crate::{Notification, shortcircuit};

///  Provides the value of the invocation id
const INVOCATION_ID_HEADER_NAME: HeaderName = HeaderName::from_static("x-restate-invocation-id");
const SERVICE_KEY_HEADER_NAME: HeaderName = HeaderName::from_static("x-restate-service-key");

const GATEWAY_ERRORS_CODES: [StatusCode; 2] =
    [StatusCode::BAD_GATEWAY, StatusCode::GATEWAY_TIMEOUT];

const RATE_LIMITED_CODES: [StatusCode; 2] = [
    StatusCode::SERVICE_UNAVAILABLE,
    StatusCode::TOO_MANY_REQUESTS,
];

/// Runs the interaction between the server and the service endpoint.
pub struct ServiceProtocolRunner<'a, Schemas> {
    invocation_task: &'a mut InvocationTask<Schemas>,

    service_protocol_version: ServiceProtocolVersion,

    // Encoder/Decoder
    encoder: Encoder,

    // task state
    command_index: CommandIndex,

    deployment_type_str: &'static str,

    max_awaited_future_depth: usize,

    /// Whether this stream was opened in preflight mode: only then the relay-only fields of the
    /// commands are honored.
    preflight: bool,
}

impl<'a, Schemas> ServiceProtocolRunner<'a, Schemas>
where
    Schemas: InvocationTargetResolver,
{
    pub fn new(
        invocation_task: &'a mut InvocationTask<Schemas>,
        service_protocol_version: ServiceProtocolVersion,
        deployment_type_str: &'static str,
        max_awaited_future_depth: usize,
    ) -> Self {
        let encoder = Encoder::new(service_protocol_version);

        Self {
            invocation_task,
            service_protocol_version,
            encoder,
            command_index: 0,
            deployment_type_str,
            max_awaited_future_depth,
            preflight: false,
        }
    }

    /// How often to release excess outbound budget capacity during the bidi-stream phase.
    const BUDGET_RELEASE_INTERVAL: Duration = Duration::from_secs(5);

    /// Run the service protocol interaction. `state_read` is `Some` to preload state upfront per
    /// its config, or `None` for fully lazy state.
    pub async fn run<Txn, IR>(
        mut self,
        txn: Txn,
        journal_metadata: JournalMetadata,
        state_read: Option<StatePreloadPolicy>,
        target: AttemptTarget,
        invocation_reader: IR,
        outbound_budget: &mut LocalMemoryPool,
    ) -> TerminalLoopState<()>
    where
        Txn: InvocationReaderTransaction,
        IR: InvocationReader,
    {
        let (deployment_id, address, protocol_type, endpoint, additional_headers, preflight_pin) =
            match target {
                AttemptTarget::Deployment(deployment) => {
                    let address = deployment.address_display().to_string();
                    let protocol_type = deployment.ty.protocol_type();
                    let endpoint = match deployment.ty {
                        DeploymentType::Lambda {
                            arn,
                            assume_role_arn,
                            compression,
                        } => Endpoint::Lambda(arn, assume_role_arn, compression),
                        DeploymentType::Http {
                            address,
                            http_version,
                            auth,
                            ..
                        } => Endpoint::Http(address, Some(http_version), auth),
                    };
                    (
                        Some(deployment.id),
                        address,
                        protocol_type,
                        endpoint,
                        deployment.additional_headers,
                        None,
                    )
                }
                AttemptTarget::Preflight(PreflightTarget {
                    address,
                    additional_headers,
                    pin,
                }) => (
                    pin.as_ref().map(|pin| pin.deployment_id),
                    address.to_string(),
                    // The invoker <-> relay leg is always full duplex, the deployment's protocol
                    // type comes with the preflight response.
                    ProtocolType::BidiStream,
                    Endpoint::Http(address, Some(http::Version::HTTP_2), None),
                    additional_headers,
                    Some(pin),
                ),
            };
        self.preflight = preflight_pin.is_some();

        let mut attempt_span = restate_tracing_instrumentation::create_invocation_attempt_span(
            &self.invocation_task.invocation_id,
            &self.invocation_task.invocation_target,
            deployment_id,
            &address,
            // Not negotiated yet in preflight mode.
            (!self.preflight).then_some(self.service_protocol_version),
            // The attempt span has as parent the invocation start span created by the PP.
            &journal_metadata.span_context,
        );

        let path: PathAndQuery = format!(
            "/invoke/{}/{}",
            self.invocation_task.invocation_target.service_name(),
            self.invocation_task.invocation_target.handler_name()
        )
        .try_into()
        .expect("must be able to build a valid invocation path");

        debug!(
            restate.invocation.id = %self.invocation_task.invocation_id,
            deployment.address = %address,
            deployment.service_protocol_version = %self.service_protocol_version.as_repr(),
            preflight = self.preflight,
            path = %path,
            "Executing invocation at deployment"
        );

        // Prepare the request
        let (mut http_stream_tx, request) = Self::prepare_request(
            path,
            endpoint,
            additional_headers,
            if self.preflight {
                PREFLIGHT_CONTENT_TYPE
            } else {
                service_protocol_version_to_header_value(self.service_protocol_version)
            },
            &self.invocation_task.invocation_id,
            attempt_span.span_context(),
            self.invocation_task.invocation_target.key(),
        );

        // Initialize the response stream state
        let http_stream_rx = ResponseStream::new(self.invocation_task.client.call(request));

        let mut decoder_stream = std::pin::pin!(
            DecoderStream::new(
                http_stream_rx,
                self.service_protocol_version,
                self.invocation_task.message_size_warning,
                self.invocation_task.message_size_limit,
                self.deployment_type_str,
            )
            .throttle(self.invocation_task.action_token_bucket.take())
        );

        let result = async {
            let protocol_type = if let Some(pin) = preflight_pin {
                let negotiated = shortcircuit!(
                    self.preflight(&mut http_stream_tx, &mut decoder_stream, pin.as_ref())
                        .await
                );
                shortcircuit!(self.apply_preflight_response(
                    &negotiated,
                    pin.is_none(),
                    decoder_stream.as_mut().inner_pin_mut().project().decoder,
                    &mut attempt_span,
                ));
                negotiated.protocol_type
            } else {
                protocol_type
            };

            // Figure out the protocol type. Force RequestResponse if inactivity_timeout is zero
            let protocol_type = if self.invocation_task.inactivity_timeout.is_zero() {
                ProtocolType::RequestResponse
            } else {
                protocol_type
            };

            // Close the invoker_rx in case it's request response, this avoids further buffering of messages in this channel.
            if protocol_type == ProtocolType::RequestResponse {
                self.invocation_task.invoker_rx.close();
            }

            self.run_inner(
                txn,
                protocol_type,
                journal_metadata,
                state_read,
                http_stream_tx,
                &mut decoder_stream,
                invocation_reader,
                outbound_budget,
                &mut attempt_span,
            )
            .await
        }
        .await;

        // After successful termination decoder should
        // not have any remaining buffer
        if result.is_closed() || result.is_suspend() {
            // Sanity check of the stream decoder
            if decoder_stream.inner().has_remaining() {
                warn_it!(
                    InvokerError::WriteAfterEndOfStream,
                    restate.invocation.id = %self.invocation_task.invocation_id,
                    deployment.address = %address,
                    deployment.service_protocol_version = %self.service_protocol_version.as_repr(),
                    "The read buffer is non empty after the stream has been closed."
                );
            }
        }

        let mut inner_stream = decoder_stream.inner_pin_mut().project().inner;

        if tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match inner_stream.next().await {
                    None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        debug!(%err, "Error while draining invocation response stream");
                        break;
                    }
                }
            }
        })
        .await
        .is_err()
        {
            warn!(
                restate.invocation.id = %self.invocation_task.invocation_id,
                deployment.address = %address,
                deployment.service_protocol_version = %self.service_protocol_version.as_repr(),
                "Response stream draining timeout!"
            );
        }

        // End attempt span
        match &result {
            TerminalLoopState::Closed => {
                attempt_span.set_status(Status::Ok);
            }

            TerminalLoopState::SuspendedV2(_) | TerminalLoopState::SuspendedV3(_) => {
                attempt_span.add_event(
                    restate_tracing_instrumentation::semconv::event::RESTATE_INVOCATION_LIFECYCLE_SUSPENDED,
                    vec![],
                );
                attempt_span.set_status(Status::Unset);
            }
            TerminalLoopState::Failed(_) => {
                attempt_span.set_status(Status::Error {
                    description: "transient failure".into(),
                });
            }
            TerminalLoopState::ShouldYield(_) => {
                attempt_span.add_event(
                    restate_tracing_instrumentation::semconv::event::RESTATE_INVOCATION_LIFECYCLE_YIELDED,
                    vec![],
                );
                attempt_span.set_status(Status::Unset);
            }
            _ => {}
        }

        result
    }

    /// Preflight exchange with the relay: write the [`proto::PreflightRequestMessage`], then wait
    /// for the response head and the first frame, either a [`proto::PreflightResponseMessage`] or
    /// an `ErrorMessage` the relay sends instead.
    async fn preflight<S>(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        http_stream_rx: &mut S,
        pin: Option<&PinnedDeployment>,
    ) -> TerminalLoopState<NegotiatedPreflight>
    where
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
    {
        let target = &self.invocation_task.invocation_target;
        let preflight_request = proto::PreflightRequestMessage {
            service_name: target.service_name().to_string(),
            handler_name: target.handler_name().to_string(),
            key: target.key().map(ToString::to_string).unwrap_or_default(),
            scope: target.scope().map(ToString::to_string),
            invocation_id: self.invocation_task.invocation_id.to_string(),
            min_protocol_version: MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION.as_repr() as u32,
            max_protocol_version: MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION.as_repr() as u32,
            pinned_deployment_id: pin.map(|pin| pin.deployment_id.to_string()),
            pinned_protocol_version: pin.map(|pin| pin.service_protocol_version.as_repr() as u32),
            retry_count_since_last_stored_entry: self
                .invocation_task
                .retry_count_since_last_stored_entry,
            // The durable attempt count is not plumbed to the invoker.
            attempt: 0,
        };
        shortcircuit!(self.write(http_stream_tx, Message::PreflightRequest(preflight_request)));

        // Bound the wait for the relay's answer.
        let timeout = if self.invocation_task.inactivity_timeout.is_zero() {
            self.invocation_task.abort_timeout
        } else {
            self.invocation_task.inactivity_timeout
        };
        let first_frame = tokio::time::timeout(timeout, async {
            loop {
                match http_stream_rx.next().await.transpose()? {
                    Some(DecoderStreamItem::Parts(parts)) => {
                        self.handle_preflight_response_headers(parts)?
                    }
                    Some(DecoderStreamItem::Message(_, message)) => return Ok(Some(message)),
                    None => return Ok(None),
                }
            }
        })
        .await;

        match shortcircuit!(first_frame.map_err(|_| InvokerError::PreflightTimeout(timeout.into())))
        {
            Ok(Some(Message::PreflightResponse(response))) => {
                validate_preflight_response(response, pin).into()
            }
            // The relay couldn't run the attempt: same handling as an SDK error, no pinning.
            Ok(Some(Message::Error(error))) => {
                TerminalLoopState::Failed(self.error_message_to_invoker_error(error))
            }
            Ok(Some(message)) => {
                TerminalLoopState::Failed(InvokerError::BadPreflightResponse(format!(
                    "expected a PreflightResponse or an ErrorMessage as first frame, got {:?}",
                    message.ty()
                )))
            }
            Ok(None) => TerminalLoopState::Failed(InvokerError::BadPreflightResponse(
                "the response ended before the PreflightResponse".to_owned(),
            )),
            Err(err) => TerminalLoopState::Failed(err),
        }
    }

    /// Pin the negotiated deployment and switch the codec to the negotiated protocol version.
    fn apply_preflight_response(
        &mut self,
        negotiated: &NegotiatedPreflight,
        first_pin: bool,
        decoder: &mut Decoder,
        attempt_span: &mut ServiceSpan,
    ) -> Result<(), InvokerError> {
        self.invocation_task
            .send_invoker_tx(InvocationTaskOutputInner::PinnedDeployment(
                PinnedDeployment::new(
                    negotiated.deployment_id,
                    negotiated.service_protocol_version,
                ),
                first_pin,
            ));
        if let Some(sdk_version) = &negotiated.sdk_version {
            self.invocation_task
                .send_invoker_tx(InvocationTaskOutputInner::ServerHeaderReceived(
                    sdk_version.clone(),
                ));
        }

        if negotiated.service_protocol_version < ServiceProtocolVersion::V4 {
            // Journal v1 is not supported by this runner.
            return Err(InvokerError::DeploymentDeprecated(
                self.invocation_task
                    .invocation_target
                    .service_name()
                    .to_string(),
                negotiated.deployment_id,
            ));
        }
        self.service_protocol_version = negotiated.service_protocol_version;
        self.encoder = Encoder::new(negotiated.service_protocol_version);
        decoder.set_service_protocol_version(negotiated.service_protocol_version);

        attempt_span.set_attribute(KeyValue::new(
            restate_tracing_instrumentation::semconv::attribute::RESTATE_DEPLOYMENT_ID,
            negotiated.deployment_id.to_string(),
        ));
        attempt_span.set_attribute(KeyValue::new(
            restate_tracing_instrumentation::semconv::attribute::RESTATE_DEPLOYMENT_SERVICE_PROTOCOL_VERSION,
            i64::from(negotiated.service_protocol_version.as_repr()),
        ));
        debug!(
            restate.invocation.id = %self.invocation_task.invocation_id,
            restate.deployment.id = %negotiated.deployment_id,
            deployment.service_protocol_version = %negotiated.service_protocol_version.as_repr(),
            deployment.protocol_type = ?negotiated.protocol_type,
            "Preflight negotiated the deployment"
        );
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_inner<Txn, S, IR>(
        &mut self,
        mut txn: Txn,
        protocol_type: ProtocolType,
        journal_metadata: JournalMetadata,
        state_read: Option<StatePreloadPolicy>,
        mut http_stream_tx: InvokerBodySender,
        decoder_stream: &mut S,
        invocation_reader: IR,
        outbound_budget: &mut LocalMemoryPool,
        attempt_span: &mut ServiceSpan,
    ) -> TerminalLoopState<()>
    where
        Txn: InvocationReaderTransaction,
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
        IR: InvocationReader,
    {
        let journal_size = journal_metadata.length;
        // === Replay phase (transaction alive) ===
        {
            // Read state for the START message. `Eager` preloads the full state; a lazy default with
            // a whitelist preloads only those keys. Both return the same `EagerState` stream type, so
            // the collection (inside `write_start`) is uniform.
            // Budget-gated: each entry takes a lease from the outbound budget.
            // Only keyed targets have state to preload; resolve (and clone) the ServiceId here, and
            // skip the read entirely when the target is not keyed.
            let state = if let Some(policy) = &state_read
                && let Some(service_id) =
                    self.invocation_task.invocation_target.as_keyed_service_id()
            {
                Some(shortcircuit!(
                    txn.read_state_budgeted(&service_id, policy, outbound_budget)
                        .map_err(InvokerError::from_state_reader)
                ))
            } else {
                None
            };

            // Send start message with the collected state (its merged lease travels with the frame)
            shortcircuit!(
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

            // Read journal stream from storage and execute the replay.
            // LocalMemoryPool-gated: each entry acquires a lease before it's sent.
            let journal_stream = shortcircuit!(
                txn.read_journal_budgeted(
                    &self.invocation_task.invocation_id,
                    journal_size,
                    journal_metadata.journal_kind,
                    outbound_budget,
                )
                .map_err(InvokerError::from_journal_reader)
            );
            shortcircuit!(
                self.replay_loop(
                    &mut http_stream_tx,
                    decoder_stream,
                    journal_stream,
                    journal_metadata.length
                )
                .await
            );
        }
        // === End replay phase - streams dropped, transaction can be dropped ===

        // Transaction dropped - RocksDB snapshot released!
        drop(txn);

        // Release excess local capacity accumulated during replay back to the
        // global pool before entering the bidi stream phase.
        outbound_budget.release_excess();

        // If we have the invoker_rx and the protocol type is bidi stream,
        // then we can use the bidi_stream loop reading the invoker_rx and the http_stream_rx
        if protocol_type == ProtocolType::BidiStream {
            trace!("Protocol is in bidi stream mode, will now start the send/receive loop");
            shortcircuit!(
                self.bidi_stream_loop(
                    http_stream_tx,
                    decoder_stream,
                    invocation_reader,
                    // The bidi stream loop will never read from a journal v1 as it will be migrated
                    // by the time the bidi stream loop needs to read notifications from it.
                    // todo remove once we drop support for journal v1
                    JournalKind::V2,
                    journal_size,
                    outbound_budget,
                    attempt_span
                )
                .await
            );
        } else {
            trace!(
                "Protocol is not in bidi stream mode, will now drop the sender side of the request"
            );
            // Drop the http_stream_tx.
            // This is required in HTTP/1.1 to let the deployment send the headers back
            drop(http_stream_tx)
        }

        // We don't have the invoker_rx, so we simply consume the response
        trace!("Sender side of the request has been dropped, now processing the response");

        self.response_stream_loop(decoder_stream, attempt_span)
            .await
    }

    fn prepare_request(
        path: PathAndQuery,
        endpoint: Endpoint,
        additional_headers: HashMap<HeaderName, HeaderValue>,
        content_type: HeaderValue,
        invocation_id: &InvocationId,
        parent_span_context: &SpanContext,
        service_key: Option<&ByteString>,
    ) -> (InvokerBodySender, Request<InvokerBodyType>) {
        // Use an unbounded channel: backpressure is provided by the memory budget
        // (each frame's Bytes embeds a LocalMemoryLease via from_owner) rather than
        // channel capacity.
        let (http_stream_tx, http_stream_rx) = mpsc::unbounded_channel();
        let req_body = new_invoker_body(http_stream_rx);

        let invocation_id_header_value = invocation_id_to_header_value(invocation_id);

        let mut headers = HeaderMap::from_iter([
            (http::header::CONTENT_TYPE, content_type.clone()),
            (http::header::ACCEPT, content_type),
            (INVOCATION_ID_HEADER_NAME, invocation_id_header_value),
        ]);
        if let Some(service_key) = service_key
            && let Ok(service_key_header) =
                HeaderValue::from_str(urlencoding::encode(service_key.as_str()).as_str())
        {
            headers.insert(SERVICE_KEY_HEADER_NAME, service_key_header);
        }

        // Inject OpenTelemetry context into the headers so the SDK sees the
        // per-attempt span as its parent.
        if parent_span_context.is_valid() {
            const SUPPORTED_VERSION: u8 = 0;
            let header_value = format!(
                "{:02x}-{}-{}-{:02x}",
                SUPPORTED_VERSION,
                parent_span_context.trace_id(),
                parent_span_context.span_id(),
                parent_span_context.trace_flags() & TraceFlags::SAMPLED
            );
            if let Ok(header_value) = HeaderValue::try_from(header_value) {
                headers.insert("traceparent", header_value);
            }
            if let Ok(tracestate) =
                HeaderValue::from_str(parent_span_context.trace_state().header().as_ref())
            {
                headers.insert("tracestate", tracestate);
            }
        }

        headers.extend(additional_headers);

        let mut request_parts = Parts::new(Method::Post, endpoint, path, headers);
        if let Some(service_key) = service_key {
            request_parts = request_parts.with_request_identity_sub_field(service_key.clone());
        }

        (http_stream_tx, Request::new(request_parts, req_body))
    }

    // --- Loops

    /// This loop concurrently pushes journal entries and waits for the response headers and end of replay.
    async fn replay_loop<JournalStream, S, E>(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        http_stream_rx: &mut S,
        journal_stream: JournalStream,
        expected_entries_count: u32,
    ) -> TerminalLoopState<()>
    where
        JournalStream: Stream<Item = Result<(JournalEntry, LocalMemoryLease), E>> + Unpin,
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
        E: InvocationReaderError,
    {
        let mut journal_stream = journal_stream.fuse();
        // In preflight mode the response head was already read during the preflight exchange.
        let mut got_headers = self.preflight;
        let mut sent_entries = 0;

        loop {
            tokio::select! {
                got_headers_res = http_stream_rx.next(), if !got_headers => {
                    // The reason we want to poll headers in this function is
                    // to exit early in case an error is returned during replays.
                    got_headers = true;
                    match shortcircuit!(got_headers_res.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2::unknown()))
                        },
                        Some(DecoderStreamItem::Parts(headers)) => {
                            shortcircuit!(self.handle_response_headers(headers));
                        }
                        Some(DecoderStreamItem::Message(_, _)) => {
                            panic!("Unexpected poll after the headers have been resolved already")
                        }
                    }
                },
                opt_je = journal_stream.next() => {
                    match opt_je {
                        Some(Ok((JournalEntry::JournalV2(entry), lease))) => {
                            sent_entries += 1;
                            shortcircuit!(self.write_entry_with_lease(http_stream_tx, entry.inner, Some(lease)));
                        }
                        Some(Ok((JournalEntry::JournalV1(old_entry), lease))) => {
                            sent_entries += 1;
                            if let journal::Entry::Input(input_entry) = shortcircuit!(old_entry.deserialize_entry::<ProtobufRawEntryCodec>()) {
                                shortcircuit!(self.write_entry_with_lease(
                                    http_stream_tx,
                                    Entry::Command(InputCommand {
                                        headers: input_entry.headers,
                                        payload: input_entry.value,
                                        name: Default::default()
                                    }.into()).encode::<ServiceProtocolV4Codec>(),
                                    Some(lease),
                                ));
                            } else {
                                panic!("This is unexpected, when an entry is stored with journal v1, only input entry is allowed!")
                            }
                        },
                        Some(Ok((JournalEntry::JournalV1Completion(_), _))) => {
                            panic!("Unexpected JournalV1Completion during replay: completion arrived before entry was stored")
                        }
                        Some(Err(e)) => {
                            return TerminalLoopState::from(
                                Err::<(), _>(InvokerError::from_journal_reader(e)),
                            );
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
    #[allow(clippy::too_many_arguments)]
    async fn bidi_stream_loop<S, IR>(
        &mut self,
        mut http_stream_tx: InvokerBodySender,
        http_stream_rx: &mut S,
        mut invocation_reader: IR,
        journal_kind: JournalKind,
        replayed_journal_length: EntryIndex,
        outbound_budget: &mut LocalMemoryPool,
        attempt_span: &mut ServiceSpan,
    ) -> TerminalLoopState<()>
    where
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
        IR: InvocationReader,
    {
        let mut release_interval = tokio::time::interval(Self::BUDGET_RELEASE_INTERVAL);
        release_interval.tick().await; // consume initial immediate tick
        let mut inactivity_timeout =
            std::pin::pin!(tokio::time::sleep(self.invocation_task.inactivity_timeout));
        loop {
            tokio::select! {
                opt_completion = self.invocation_task.invoker_rx.recv() => {
                    match opt_completion {
                        Some(Notification::Entry(entry_index)) if entry_index < replayed_journal_length => {
                            // The entry was already part of the replayed journal prefix, sending it
                            // again would deliver the same notification twice to the SDK.
                            //
                            // This happens because the invocation state machine starts forwarding
                            // notifications as soon as the attempt is started, while this task reads
                            // the journal metadata (and thus fixes the replay prefix) a bit later.
                            // Any entry appended in between is both replayed and forwarded.
                            debug!(
                                restate.journal.index = entry_index,
                                "Ignoring notification for an entry that was already replayed"
                            );
                        }
                        Some(Notification::Entry(entry_index)) => {
                            trace!(restate.journal.index = entry_index, "Reading entry from storage");
                            let (journal_entry, lease) = shortcircuit!(
                                invocation_reader
                                    .read_journal_entry_budgeted(
                                        &self.invocation_task.invocation_id,
                                        entry_index,
                                        journal_kind,
                                        outbound_budget,
                                    )
                                    .await
                                    .map_err(InvokerError::from_journal_reader)
                                    .and_then(|opt| opt.ok_or_else(|| InvokerError::JournalReader(
                                        anyhow::anyhow!(
                                            "journal entry {entry_index} not found for notification read"
                                        ),
                                    )))
                            );
                            let raw_entry = match journal_entry {
                                JournalEntry::JournalV2(stored) => stored.inner,
                                other => {
                                    panic!("v4+ protocol runner expected JournalV2 entry but got {other:?}")
                                }
                            };
                            trace!("Sending the entry to the wire");
                            shortcircuit!(self.write_entry_with_lease(&mut http_stream_tx, raw_entry, Some(lease)));
                        }
                        Some(Notification::CommandAck(entry_index)) => {
                            trace!("Sending the ack to the wire");
                            shortcircuit!(self.write(&mut http_stream_tx, Message::new_command_ack(entry_index)));
                        },
                        Some(Notification::ProposeRunCompletionAck(completion_id)) => {
                            trace!("Sending ProposeRunCompletionAck to the wire");
                            shortcircuit!(self.write(&mut http_stream_tx, Message::new_propose_run_completion_ack(completion_id)));
                        },
                        None => {
                            // Completion channel is closed,
                            // the invoker main loop won't send completions anymore.
                            // Response stream might still be open though.
                            return TerminalLoopState::Continue(())
                        },
                    }

                    inactivity_timeout.as_mut().reset(tokio::time::Instant::now() + self.invocation_task.inactivity_timeout);
                },
                chunk = http_stream_rx.next() => {
                    match shortcircuit!(chunk.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2::unknown()));
                        }
                        Some(DecoderStreamItem::Parts(parts)) => shortcircuit!(self.handle_response_headers(parts)),
                        Some(DecoderStreamItem::Message(message_header, message)) => {
                            shortcircuit!(self.handle_message(message_header, message, attempt_span));
                        }
                    }

                    inactivity_timeout.as_mut().reset(tokio::time::Instant::now() + self.invocation_task.inactivity_timeout);
                },
                _ = release_interval.tick() => {
                    outbound_budget.release_excess();
                },
                _ = &mut inactivity_timeout => {
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
        http_stream_rx: &mut S,
        attempt_span: &mut ServiceSpan,
    ) -> TerminalLoopState<()>
    where
        S: Stream<Item = Result<DecoderStreamItem, InvokerError>> + Unpin,
    {
        loop {
            tokio::select! {
                chunk = http_stream_rx.next() => {
                    // don't read again until all buffered messages has been consumed
                    // to force a back pressure on the read stream

                    match shortcircuit!(chunk.transpose()) {
                        None => {
                            return TerminalLoopState::Failed(InvokerError::SdkV2(SdkInvocationErrorV2::unknown()));
                        }
                        Some(DecoderStreamItem::Parts(parts)) => shortcircuit!(self.handle_response_headers(parts)),
                        Some(DecoderStreamItem::Message(message_header, message)) => {
                            shortcircuit!(self.handle_message(message_header, message, attempt_span));
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
        random_seed: u64,
    ) -> Result<(), InvokerError>
    where
        S: PinnableMemoryStream<Item = Result<(Bytes, Bytes, LocalMemoryLease), E>> + Send,
        E: InvocationReaderError,
    {
        // Collect state entries with size limit
        let (partial_state, state_map, state_lease) = collect_eager_state(
            state,
            self.invocation_task.eager_state_size_limit,
            |(key, value)| StateEntry { key, value },
        )
        .await?;

        let start_message = if self.service_protocol_version >= ServiceProtocolVersion::V7 {
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
                self.invocation_task.invocation_target.scope(),
                &self.invocation_task.limit_key,
                self.invocation_task.idempotency_key.as_ref(),
            )
        } else {
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
                // those fields were only introduced with service protocol V7
                None,
                &LimitKey::None,
                None,
            )
        };

        // Send the invoke frame with the merged state lease
        self.write_with_lease(http_stream_tx, start_message, state_lease)
    }

    fn write_entry_with_lease(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        entry: RawEntry,
        lease: Option<LocalMemoryLease>,
    ) -> Result<(), InvokerError> {
        // TODO(slinkydeveloper) could this code be improved a tad bit more introducing something to our magic macro in message_codec?
        match entry {
            RawEntry::Command(cmd) => {
                self.write_raw_with_lease(
                    http_stream_tx,
                    cmd.command_type().into(),
                    cmd.into_serialized_content(),
                    lease,
                )?;
                self.command_index += 1;
            }
            RawEntry::Notification(notif) => {
                self.write_raw_with_lease(
                    http_stream_tx,
                    notif.ty().into(),
                    notif.into_serialized_content(),
                    lease,
                )?;
            }
        }
        Ok(())
    }

    fn write(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        msg: Message,
    ) -> Result<(), InvokerError> {
        self.write_with_lease(http_stream_tx, msg, None)
    }

    fn write_with_lease(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        msg: Message,
        lease: Option<LocalMemoryLease>,
    ) -> Result<(), InvokerError> {
        trace!(restate.protocol.message = ?msg, "Sending message");
        let buf = self.encoder.encode(msg);

        let len = buf.len();
        if http_stream_tx.send(Ok(leased_frame(buf, lease))).is_err() {
            return Err(InvokerError::UnexpectedClosedRequestStream);
        };
        counter!(INVOKER_SENT_BYTES, "type" => self.deployment_type_str).increment(len as u64);
        Ok(())
    }

    fn write_raw_with_lease(
        &mut self,
        http_stream_tx: &mut InvokerBodySender,
        ty: MessageType,
        buf: Bytes,
        lease: Option<LocalMemoryLease>,
    ) -> Result<(), InvokerError> {
        trace!(restate.protocol.message = ?ty, "Sending message");
        let buf = self.encoder.encode_raw(ty, buf);
        let len = buf.len();

        if http_stream_tx.send(Ok(leased_frame(buf, lease))).is_err() {
            return Err(InvokerError::UnexpectedClosedRequestStream);
        };
        counter!(INVOKER_SENT_BYTES, "type" => self.deployment_type_str).increment(len as u64);
        Ok(())
    }

    fn handle_response_headers(
        &mut self,
        mut parts: http::response::Parts,
    ) -> Result<(), InvokerError> {
        self.check_response_status(&parts)?;
        if parts.status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
            // Probably an incompatible protocol version
            return Err(InvokerError::BadNegotiatedServiceProtocolVersion(
                self.service_protocol_version,
            ));
        }

        Self::check_content_type(
            &mut parts,
            service_protocol_version_to_header_value(self.service_protocol_version),
        )?;

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

    /// Response head of a preflight-mode request. A non-200 response means the relay refused the
    /// request before reading the preflight, it's handled as any other non-2xx response.
    fn handle_preflight_response_headers(
        &mut self,
        mut parts: http::response::Parts,
    ) -> Result<(), InvokerError> {
        self.check_response_status(&parts)?;
        if parts.status == StatusCode::UNSUPPORTED_MEDIA_TYPE {
            return Err(InvokerError::BadPreflightResponse(
                "the single endpoint doesn't support the preflight protocol (415)".to_owned(),
            ));
        }

        Self::check_content_type(&mut parts, PREFLIGHT_CONTENT_TYPE)
    }

    /// Maps non-2xx responses to errors, except 415 which depends on the request content type.
    fn check_response_status(&self, parts: &http::response::Parts) -> Result<(), InvokerError> {
        // if service is running behind a gateway, the service can be down
        // but we still get a response code from the gateway itself. In that
        // case we still need to return the proper error
        counter!(INVOKER_CLIENT_REQUESTS,
            "type" => self.deployment_type_str,
            "status_code" => parts.status.as_str().to_restring())
        .increment(1);
        if GATEWAY_ERRORS_CODES.contains(&parts.status) {
            return Err(InvokerError::ServiceUnavailable(parts.status));
        }

        if RATE_LIMITED_CODES.contains(&parts.status) {
            return Err(InvokerError::RateLimited {
                code: parts.status,
                retry_after: retry_after::parse_retry_after(parts),
            });
        }

        // otherwise we return generic UnexpectedResponse
        if !parts.status.is_success() {
            if parts.status == StatusCode::PAYLOAD_TOO_LARGE {
                return Err(InvokerError::ContentTooLarge);
            }
            if parts.status != StatusCode::UNSUPPORTED_MEDIA_TYPE {
                return Err(InvokerError::UnexpectedResponse(parts.status));
            }
        }

        Ok(())
    }

    fn check_content_type(
        parts: &mut http::response::Parts,
        expected_content_type: HeaderValue,
    ) -> Result<(), InvokerError> {
        match parts.headers.remove(http::header::CONTENT_TYPE) {
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
        Ok(())
    }

    fn handle_new_command(
        &mut self,
        mh: MessageHeader,
        command: RawCommand,
        attempt_span: &mut ServiceSpan,
        cmd_name: Option<String>,
    ) {
        if attempt_span.is_recording() {
            let mut attributes = vec![KeyValue::new(
                restate_tracing_instrumentation::semconv::attribute::RESTATE_JOURNAL_COMMAND_TYPE,
                command.ty().prometheus_label(),
            )];
            if let Some(cmd_name) = cmd_name
                && !cmd_name.is_empty()
            {
                attributes.push(KeyValue::new(
                    restate_tracing_instrumentation::semconv::attribute::RESTATE_JOURNAL_COMMAND_NAME,
                    cmd_name,
                ));
            }

            attempt_span.add_event(
                restate_tracing_instrumentation::semconv::event::RESTATE_INVOCATION_LIFECYCLE_NEW_COMMAND,
                attributes
            );
        }
        self.invocation_task
            .send_invoker_tx(InvocationTaskOutputInner::NewCommand {
                command_index: self.command_index,
                requested_ack: mh
                    .requested_ack()
                    .expect("All command messages support requested_ack"),
                command,
            });
        self.command_index += 1;
    }

    fn handle_message(
        &mut self,
        mh: MessageHeader,
        message: Message,
        attempt_span: &mut ServiceSpan,
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
            Message::ProposeRunCompletionAck(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::ProposeRunCompletionAck),
            ),
            // Preflight frames are only legal as the first frames of a preflight-mode stream,
            // which are consumed before the service protocol stream starts.
            Message::PreflightRequest(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::PreflightRequest),
            ),
            Message::PreflightResponse(_) => TerminalLoopState::Failed(
                InvokerError::UnexpectedMessageV4(MessageType::PreflightResponse),
            ),
            Message::Suspension(suspension) => self.handle_suspension_message(suspension),
            Message::AwaitingOn(awaiting_on) => self.handle_awaiting_on_message(awaiting_on),
            Message::Error(e) => self.handle_error_message(e),
            Message::End(_) => TerminalLoopState::Closed,

            // Run completion proposal
            Message::ProposeRunCompletion(run_completion) => {
                let notification: Entry = RunCompletion {
                    completion_id: run_completion.result_completion_id,
                    result: match shortcircuit!(
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
                attempt_span.add_event(
                    restate_tracing_instrumentation::semconv::event::RESTATE_INVOCATION_LIFECYCLE_RUN_ENDED,
                    vec![KeyValue::new(
                        restate_tracing_instrumentation::semconv::attribute::RESTATE_JOURNAL_NOTIFICATION_ID,
                        raw_notification.id().to_string(),
                    )],
                );

                self.invocation_task.send_invoker_tx(
                    InvocationTaskOutputInner::NewNotificationProposal {
                        notification: raw_notification,
                        requested_ack: mh
                            .requested_ack()
                            .expect("ProposeRunCompletion message supports requested_ack"),
                    },
                );

                TerminalLoopState::Continue(())
            }

            // Commands
            Message::OutputCommand(cmd) => {
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::Output, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::InputCommand(cmd) => {
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::Input, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetInvocationOutputCommand(cmd) => {
                // The macro registers `GetInvocationOutput Command` as `noparse`, so `cmd` is
                // the raw bytes from the wire. We decode a lite shadow type that only
                // covers the fields we actually validate (target/scope) and forward the
                // original bytes downstream to avoid a re-encode round trip.
                let parsed = crate::shortcircuit!(
                    proto_lite::GetInvocationOutputCommandMessageLite::decode(cmd.as_ref())
                        .map_err(|err| InvokerError::Encoding(GenericError::from(err).into()))
                );
                shortcircuit!(
                    self.validate_target(
                        parsed.target.as_ref(),
                        parsed.relay_effective_scope.as_deref()
                    )
                    .map_err(|err| {
                        InvokerError::CommandPrecondition(
                            self.command_index,
                            EntryType::Command(CommandType::GetInvocationOutput),
                            err,
                        )
                    })
                );
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetInvocationOutput, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::AttachInvocationCommand(cmd) => {
                // See `Message::GetInvocationOutputCommand` above for why we decode-then-forward.
                let parsed = shortcircuit!(
                    proto_lite::AttachInvocationCommandMessageLite::decode(cmd.as_ref())
                        .map_err(|err| InvokerError::Encoding(GenericError::from(err).into()))
                );
                shortcircuit!(
                    self.validate_target(
                        parsed.target.as_ref(),
                        parsed.relay_effective_scope.as_deref()
                    )
                    .map_err(|err| {
                        InvokerError::CommandPrecondition(
                            self.command_index,
                            EntryType::Command(CommandType::AttachInvocation),
                            err,
                        )
                    })
                );
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::AttachInvocation, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::RunCommand(cmd) => {
                let raw = RawCommand::new(CommandType::Run, cmd);
                let run_cmd: RunCommand = shortcircuit!(raw.decode::<ServiceProtocolV4Codec, _>());
                self.handle_new_command(mh, raw, attempt_span, Some(run_cmd.name.to_string()));
                TerminalLoopState::Continue(())
            }
            Message::SendSignalCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let raw = RawCommand::new(CommandType::SendSignal, cmd);
                let _: Entry = shortcircuit!(raw.decode::<ServiceProtocolV4Codec, _>());
                self.handle_new_command(mh, raw, attempt_span, None);
                TerminalLoopState::Continue(())
            }
            Message::OneWayCallCommand(raw) => {
                // Registered as `noparse`: decode to resolve the request, then forward the
                // original bytes downstream to avoid a re-encode round trip.
                let cmd = shortcircuit!(
                    <proto::OneWayCallCommandMessage as ServiceWireDecoder>::decode(
                        raw.clone(),
                        self.service_protocol_version
                    )
                    .map_err(|err| InvokerError::Encoder(
                        EncodingError::MessageEncoding(MessageType::OneWayCallCommand, err)
                    ))
                );
                let call_resolution = self.call_resolution(RelayCallSemantics {
                    target_type: cmd.relay_target_type,
                    effective_scope: cmd.relay_effective_scope,
                    idempotency_retention_millis: cmd.relay_idempotency_retention_millis,
                    workflow_completion_retention_millis: cmd
                        .relay_workflow_completion_retention_millis,
                    journal_retention_millis: cmd.relay_journal_retention_millis,
                });
                let request = shortcircuit!(
                    resolve_call_request(
                        self.invocation_task.schemas.live_load(),
                        InvokeRequest {
                            service_name: cmd.service_name.into(),
                            handler_name: cmd.handler_name.into(),
                            parameter: cmd.parameter,
                            headers: cmd.headers.into_iter().map(Into::into).collect(),
                            key: cmd.key.into(),
                            idempotency_key: cmd.idempotency_key.map(|s| s.into()),
                            scope: cmd.scope,
                            limit_key: cmd.limit_key,
                            span_relation: SpanRelation::Linked(
                                attempt_span.span_context().clone().into()
                            )
                        },
                        call_resolution,
                    )
                    .map_err(|e| InvokerError::CommandPrecondition(
                        self.command_index,
                        EntryType::Command(CommandType::OneWayCall),
                        e
                    ))
                );
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::OneWayCall, raw).with_command_specific_metadata(
                        RawCommandSpecificMetadata::CallOrSend(Box::new(CallOrSendMetadata {
                            invocation_id: request.invocation_id,
                            invocation_target: request.invocation_target,
                            span_context: request.span_context,
                            completion_retention_duration: request.completion_retention_duration,
                            journal_retention_duration: request.journal_retention_duration,
                        })),
                    ),
                    attempt_span,
                    Some(cmd.name),
                );
                TerminalLoopState::Continue(())
            }
            Message::CallCommand(raw) => {
                let cmd = shortcircuit!(
                    <proto::CallCommandMessage as ServiceWireDecoder>::decode(
                        raw.clone(),
                        self.service_protocol_version
                    )
                    .map_err(|err| InvokerError::Encoder(
                        EncodingError::MessageEncoding(MessageType::CallCommand, err)
                    ))
                );
                let call_resolution = self.call_resolution(RelayCallSemantics {
                    target_type: cmd.relay_target_type,
                    effective_scope: cmd.relay_effective_scope,
                    idempotency_retention_millis: cmd.relay_idempotency_retention_millis,
                    workflow_completion_retention_millis: cmd
                        .relay_workflow_completion_retention_millis,
                    journal_retention_millis: cmd.relay_journal_retention_millis,
                });
                let request = shortcircuit!(
                    resolve_call_request(
                        self.invocation_task.schemas.live_load(),
                        InvokeRequest {
                            service_name: cmd.service_name.into(),
                            handler_name: cmd.handler_name.into(),
                            parameter: cmd.parameter,
                            headers: cmd.headers.into_iter().map(Into::into).collect(),
                            key: cmd.key.into(),
                            idempotency_key: cmd.idempotency_key.map(|s| s.into()),
                            scope: cmd.scope,
                            limit_key: cmd.limit_key,
                            span_relation: SpanRelation::Parent(
                                attempt_span.span_context().clone().into()
                            )
                        },
                        call_resolution,
                    )
                    .map_err(|e| InvokerError::CommandPrecondition(
                        self.command_index,
                        EntryType::Command(CommandType::Call),
                        e
                    ))
                );
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::Call, raw).with_command_specific_metadata(
                        RawCommandSpecificMetadata::CallOrSend(Box::new(CallOrSendMetadata {
                            invocation_id: request.invocation_id,
                            invocation_target: request.invocation_target,
                            span_context: request.span_context,
                            completion_retention_duration: request.completion_retention_duration,
                            journal_retention_duration: request.journal_retention_duration,
                        })),
                    ),
                    attempt_span,
                    Some(cmd.name),
                );
                TerminalLoopState::Continue(())
            }
            Message::SleepCommand(cmd) => {
                let raw = RawCommand::new(CommandType::Sleep, cmd);
                let sleep_cmd: SleepCommand =
                    shortcircuit!(raw.decode::<ServiceProtocolV4Codec, _>());
                self.handle_new_command(mh, raw, attempt_span, Some(sleep_cmd.name.to_string()));
                TerminalLoopState::Continue(())
            }
            Message::CompletePromiseCommand(cmd) => {
                shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::CompletePromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::CompletePromise, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::PeekPromiseCommand(cmd) => {
                shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::PeekPromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::PeekPromise, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetPromiseCommand(cmd) => {
                shortcircuit!(check_workflow_type(
                    self.command_index,
                    &EntryType::Command(CommandType::GetPromise),
                    &self.invocation_task.invocation_target.service_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetPromise, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetEagerStateKeysCommand(cmd) => {
                shortcircuit!(can_read_state(
                    self.command_index,
                    &EntryType::Command(CommandType::GetEagerStateKeys),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetEagerStateKeys, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetEagerStateCommand(cmd) => {
                shortcircuit!(can_read_state(
                    self.command_index,
                    &EntryType::Command(CommandType::GetEagerState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetEagerState, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetLazyStateKeysCommand(cmd) => {
                shortcircuit!(can_read_state(
                    self.command_index,
                    &EntryType::Command(CommandType::GetLazyStateKeys),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetLazyStateKeys, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::ClearAllStateCommand(cmd) => {
                shortcircuit!(can_write_state(
                    self.command_index,
                    &EntryType::Command(CommandType::ClearAllState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::ClearAllState, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::ClearStateCommand(cmd) => {
                shortcircuit!(can_write_state(
                    self.command_index,
                    &EntryType::Command(CommandType::ClearState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::ClearState, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::SetStateCommand(cmd) => {
                shortcircuit!(can_write_state(
                    self.command_index,
                    &EntryType::Command(CommandType::SetState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::SetState, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::GetLazyStateCommand(cmd) => {
                shortcircuit!(can_read_state(
                    self.command_index,
                    &EntryType::Command(CommandType::GetLazyState),
                    &self
                        .invocation_task
                        .invocation_target
                        .invocation_target_ty(),
                ));
                self.handle_new_command(
                    mh,
                    RawCommand::new(CommandType::GetLazyState, cmd),
                    attempt_span,
                    None,
                );
                TerminalLoopState::Continue(())
            }
            Message::CompleteAwakeableCommand(cmd) => {
                // Verify the provided InvocationId is valid
                let raw = RawCommand::new(CommandType::CompleteAwakeable, cmd);
                let _: Entry = shortcircuit!(raw.decode::<ServiceProtocolV4Codec, _>());
                self.handle_new_command(mh, raw, attempt_span, None);
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

    fn handle_awaiting_on_message(
        &mut self,
        awaiting_on: proto::AwaitingOnMessage,
    ) -> TerminalLoopState<()> {
        // this message should mark this invocation as suspendable.
        // if it's not running any side effects.

        let Some(awaiting_on) = awaiting_on.awaiting_on else {
            return TerminalLoopState::Failed(InvokerError::EmptyAwaitingOnMessage);
        };

        if awaiting_on.is_too_deep(self.max_awaited_future_depth) {
            return TerminalLoopState::Failed(InvokerError::MaxFutureDepthReached {
                limit: self.max_awaited_future_depth,
            });
        }

        let unresolved_future: UnresolvedFuture = shortcircuit!(
            awaiting_on
                .try_into()
                .map_err(|e| InvokerError::Encoding(GenericError::from(e).into()))
        );
        self.invocation_task
            .send_invoker_tx(InvocationTaskOutputInner::AwaitingOn { unresolved_future });

        // todo(azmy): Handle awaiting on message
        //  Also verify that we keep correctly updated the InvocationStatusReportInner.last_awaiting_on_unresolved_future field!
        TerminalLoopState::Continue(())
    }

    fn handle_suspension_message(
        &mut self,
        suspension: proto::SuspensionMessage,
    ) -> TerminalLoopState<()> {
        let Some(awaiting_on) = suspension.awaiting_on else {
            return TerminalLoopState::Failed(InvokerError::EmptySuspensionMessage);
        };

        if awaiting_on.is_too_deep(self.max_awaited_future_depth) {
            return TerminalLoopState::Failed(InvokerError::MaxFutureDepthReached {
                limit: self.max_awaited_future_depth,
            });
        }

        let future: UnresolvedFuture = shortcircuit!(
            awaiting_on
                .try_into()
                .map_err(|e| InvokerError::Encoding(GenericError::from(e).into()))
        );

        // We currently don't support empty future set
        if future.is_empty() {
            return TerminalLoopState::Failed(InvokerError::EmptySuspensionMessage);
        }

        match self.service_protocol_version {
            ServiceProtocolVersion::Unspecified => unreachable!(),
            ServiceProtocolVersion::V1
            | ServiceProtocolVersion::V2
            | ServiceProtocolVersion::V3
            | ServiceProtocolVersion::V4
            | ServiceProtocolVersion::V5
            | ServiceProtocolVersion::V6 => TerminalLoopState::SuspendedV2(future.flatten()),
            ServiceProtocolVersion::V7 => TerminalLoopState::SuspendedV3(future),
        }
    }

    fn handle_error_message(&mut self, error: proto::ErrorMessage) -> TerminalLoopState<()> {
        TerminalLoopState::Failed(self.error_message_to_invoker_error(error))
    }

    fn error_message_to_invoker_error(&self, error: proto::ErrorMessage) -> InvokerError {
        let requested_error_behavior = match proto::ErrorBehavior::try_from(error.behavior)
            .unwrap_or(proto::ErrorBehavior::Retry)
        {
            proto::ErrorBehavior::Retry => {
                RequestedErrorBehavior::retry(error.next_retry_delay.map(Duration::from_millis))
            }
            proto::ErrorBehavior::Pause => RequestedErrorBehavior::Pause,
            proto::ErrorBehavior::Fail => RequestedErrorBehavior::Fail,
        };
        InvokerError::SdkV2(SdkInvocationErrorV2 {
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
            requested_error_behavior,
            error: InvocationError::from(error).into(),
        })
    }

    /// Validates the target of a `GetInvocationOutput` or `AttachInvocation` command.
    ///
    /// We only validate the syntactic shape of the target (invocation id parses, scope is a
    /// valid `RestrictedValue`). We do not check that the service/handler/workflow currently
    /// exists in the schema, because the user may legitimately query the output of an invocation
    /// whose service has since been removed from the registry.
    ///
    /// `relay_effective_scope` replaces the scope of workflow and idempotent request targets when
    /// the command is decoded (see the entry codec), so it's validated instead of the target's
    /// scope. Only the relay writes it, on preflight-mode streams: elsewhere it's rejected.
    fn validate_target(
        &self,
        target: Option<&proto_lite::TargetLite>,
        relay_effective_scope: Option<&str>,
    ) -> Result<(), CommandPreconditionError> {
        if relay_effective_scope.is_some() && !self.preflight {
            return Err(CommandPreconditionError::UnexpectedRelayField);
        }
        let validate_scope = |target_scope: Option<&String>| {
            match relay_effective_scope.or_else(|| target_scope.map(String::as_str)) {
                // "" is unscoped
                Some(scope) if !(relay_effective_scope.is_some() && scope.is_empty()) => {
                    RestrictedValue::new(scope)
                        .map(|_| ())
                        .map_err(|e| CommandPreconditionError::InvalidScope(scope.to_owned(), e))
                }
                _ => Ok(()),
            }
        };

        match target {
            None => {}
            Some(proto_lite::TargetLite::InvocationId(invocation_id)) => {
                invocation_id.parse::<InvocationId>().map_err(|err| {
                    CommandPreconditionError::InvalidInvocationId(invocation_id.clone(), err)
                })?;
            }
            Some(proto_lite::TargetLite::IdempotentRequestTarget(idempotent_request_target)) => {
                validate_scope(idempotent_request_target.scope.as_ref())?
            }
            Some(proto_lite::TargetLite::WorkflowTarget(workflow_target)) => {
                validate_scope(workflow_target.scope.as_ref())?
            }
        }

        Ok(())
    }

    /// How to resolve what the local registry doesn't decide about a callee.
    fn call_resolution(&self, relay: RelayCallSemantics) -> CallResolution {
        if self.preflight {
            CallResolution::Relay(relay)
        } else if Configuration::pinned().worker.invoker.is_preflight() {
            // A registered deployment calling an unregistered service: the relay didn't see the
            // call, and nothing else can tell its semantics.
            CallResolution::NotFound
        } else {
            CallResolution::Guess
        }
    }
}

/// The relay-only fields of a `CallCommandMessage`/`OneWayCallCommandMessage`.
struct RelayCallSemantics {
    target_type: i32,
    effective_scope: Option<String>,
    idempotency_retention_millis: Option<u64>,
    workflow_completion_retention_millis: Option<u64>,
    journal_retention_millis: Option<u64>,
}

impl RelayCallSemantics {
    fn retentions(&self) -> RequestedRetentions {
        // UINT64_MAX (or absent) means the server default.
        let retention = |millis: Option<u64>| {
            millis
                .filter(|millis| *millis != u64::MAX)
                .map(Duration::from_millis)
        };
        RequestedRetentions {
            idempotency: retention(self.idempotency_retention_millis),
            workflow_completion: retention(self.workflow_completion_retention_millis),
            journal: retention(self.journal_retention_millis),
        }
    }
}

/// How a call resolves what the local registry doesn't decide.
enum CallResolution {
    /// Preflight-mode stream: the relay decided the effective scope and the semantics of
    /// unregistered callees.
    Relay(RelayCallSemantics),
    /// Schemaless prototype modes: guess the target type from the key and the
    /// `x-restate-handler-kind` header.
    Guess,
    /// The callee doesn't exist.
    NotFound,
}

/// What the relay chose for this attempt, validated.
#[derive(Debug, PartialEq, Eq)]
struct NegotiatedPreflight {
    deployment_id: DeploymentId,
    service_protocol_version: ServiceProtocolVersion,
    protocol_type: ProtocolType,
    sdk_version: Option<String>,
}

/// Validates the relay's [`proto::PreflightResponseMessage`] against what the invoker sent: the
/// version must be within the advertised range, and both the deployment and the version must
/// match the pin, if any.
fn validate_preflight_response(
    response: proto::PreflightResponseMessage,
    pin: Option<&PinnedDeployment>,
) -> Result<NegotiatedPreflight, InvokerError> {
    let bad = |reason: String| InvokerError::BadPreflightResponse(reason);

    let deployment_id: DeploymentId = response.deployment_id.parse().map_err(|err| {
        bad(format!(
            "invalid deployment id '{}': {err}",
            response.deployment_id
        ))
    })?;
    let service_protocol_version = i32::try_from(response.protocol_version)
        .ok()
        .and_then(|v| ServiceProtocolVersion::try_from(v).ok())
        .filter(|v| {
            (MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION..=MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION)
                .contains(v)
        })
        .ok_or_else(|| {
            bad(format!(
                "protocol version {} is outside the advertised range [{}, {}]",
                response.protocol_version,
                MIN_DISCOVERABLE_SERVICE_PROTOCOL_VERSION.as_repr(),
                MAX_DISCOVERABLE_SERVICE_PROTOCOL_VERSION.as_repr()
            ))
        })?;
    let protocol_type = match preflight::ProtocolType::try_from(response.protocol_type) {
        Ok(preflight::ProtocolType::BidiStream) => ProtocolType::BidiStream,
        Ok(preflight::ProtocolType::RequestResponse) => ProtocolType::RequestResponse,
        Ok(preflight::ProtocolType::Unspecified) | Err(_) => {
            return Err(bad(format!(
                "unsupported protocol type {}",
                response.protocol_type
            )));
        }
    };
    if let Some(pin) = pin
        && (pin.deployment_id != deployment_id
            || pin.service_protocol_version != service_protocol_version)
    {
        return Err(bad(format!(
            "the invocation is pinned to deployment {} with protocol version {}, the relay chose deployment {deployment_id} with protocol version {}",
            pin.deployment_id,
            pin.service_protocol_version.as_repr(),
            service_protocol_version.as_repr()
        )));
    }

    Ok(NegotiatedPreflight {
        deployment_id,
        service_protocol_version,
        protocol_type,
        sdk_version: response.sdk_version,
    })
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
    scope: Option<String>,
    limit_key: Option<String>,
    span_relation: SpanRelation,
    parameter: Bytes,
}

fn resolve_call_request(
    invocation_target_resolver: &impl InvocationTargetResolver,
    mut request: InvokeRequest,
    mut call_resolution: CallResolution,
) -> Result<CallRequest, CommandPreconditionError> {
    // The relay's effective scope applies whoever resolves the callee.
    if let CallResolution::Relay(relay) = &mut call_resolution
        && let Some(effective_scope) = relay.effective_scope.take()
    {
        request.scope = Some(effective_scope);
    }

    let not_found = |request: &InvokeRequest| {
        CommandPreconditionError::ServiceHandlerNotFound(
            request.service_name.to_string(),
            request.handler_name.to_string(),
        )
    };

    // The local registry wins over the relay's decision.
    let meta = if invocation_target_resolver
        .resolve_latest_service_type(&request.service_name)
        .is_some()
    {
        invocation_target_resolver
            .resolve_latest_invocation_target(&request.service_name, &request.handler_name)
            .ok_or_else(|| not_found(&request))?
    } else {
        match call_resolution {
            CallResolution::Relay(relay) => {
                let target_ty = match proto::RelayTargetType::try_from(relay.target_type) {
                    Ok(proto::RelayTargetType::RelayTargetService) => InvocationTargetType::Service,
                    Ok(proto::RelayTargetType::RelayTargetVirtualObjectExclusive) => {
                        InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
                    }
                    Ok(proto::RelayTargetType::RelayTargetVirtualObjectShared) => {
                        InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Shared)
                    }
                    Ok(proto::RelayTargetType::RelayTargetWorkflow) => {
                        InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                    }
                    Ok(proto::RelayTargetType::RelayTargetWorkflowShared) => {
                        InvocationTargetType::Workflow(WorkflowHandlerType::Shared)
                    }
                    Ok(proto::RelayTargetType::RelayTargetNotFound) => {
                        return Err(not_found(&request));
                    }
                    Ok(proto::RelayTargetType::Unspecified) | Err(_) => {
                        return Err(CommandPreconditionError::UnresolvedCallTarget(
                            request.service_name.to_string(),
                            request.handler_name.to_string(),
                        ));
                    }
                };
                InvocationTargetMetadata::from_external_semantics(target_ty, relay.retentions())
            }
            CallResolution::Guess => {
                // The handler kind (shared vs exclusive) can't be derived without a schema, so
                // honor the well-known handler-kind header if the caller set it (defaults to
                // exclusive).
                let handler_kind = request
                    .headers
                    .iter()
                    .find(|h| h.name.eq_ignore_ascii_case(X_RESTATE_HANDLER_KIND_HEADER))
                    .map(|h| h.value.as_ref());
                InvocationTargetMetadata::from_external_semantics(
                    InvocationTargetType::schemaless(!request.key.is_empty(), handler_kind),
                    RequestedRetentions::default(),
                )
            }
            CallResolution::NotFound => return Err(not_found(&request)),
        }
    };

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
            request.key.clone(),
            request.handler_name,
            h_ty,
        ),
        InvocationTargetType::Workflow(h_ty) => InvocationTarget::workflow(
            request.service_name.clone(),
            request.key.clone(),
            request.handler_name,
            h_ty,
        ),
    }
    .with_scope(
        if let Some(scope) = request.scope
            && !scope.is_empty()
        {
            Some(
                Scope::try_new(&scope)
                    .map_err(|e| CommandPreconditionError::InvalidScope(scope, e))?,
            )
        } else {
            None
        },
    );

    let idempotency_key = if let Some(idempotency_key) = &request.idempotency_key {
        if idempotency_key.is_empty() {
            return Err(CommandPreconditionError::EmptyIdempotencyKey);
        }
        Some(idempotency_key.deref())
    } else {
        None
    };
    let limit_key: LimitKey<ReString> = if let Some(limit_key) = request.limit_key {
        limit_key
            .parse()
            .map_err(|e| CommandPreconditionError::InvalidLimitKey(limit_key, e))?
    } else {
        LimitKey::None
    };

    // Validate invariant: limit_key requires scope
    if !limit_key.is_empty() && invocation_target.scope().is_none() {
        return Err(CommandPreconditionError::LimitKeyWithoutScope);
    }

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
        limit_key,
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
        rx_counter: Counter,
    }
}

impl<S> DecoderStream<S> {
    fn new(
        inner: S,
        service_protocol_version: ServiceProtocolVersion,
        message_size_warning: NonZeroUsize,
        message_size_limit: NonZeroUsize,
        deployment_type_str: &'static str,
    ) -> Self {
        Self {
            inner,
            decoder: Decoder::new(
                service_protocol_version,
                message_size_warning,
                message_size_limit,
            ),
            rx_counter: counter!(INVOKER_RECEIVED_BYTES, "type" => deployment_type_str),
        }
    }

    fn has_remaining(&self) -> bool {
        self.decoder.has_remaining()
    }
}

impl<S> Stream for DecoderStream<S>
where
    S: Stream<Item = Result<ResponseChunk, InvokerError>>,
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
                            this.rx_counter.increment(buf.len() as u64);
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

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;

    use restate_types::invocation::ServiceType;
    use restate_types::retries::RetryIter;
    use restate_types::schema::invocation_target::{InvocationAttemptOptions, OnMaxAttempts};

    const DAY: Duration = Duration::from_secs(60 * 60 * 24);

    /// Local registry with a single virtual object `registered` with an exclusive handler `h`.
    struct TestSchemas;

    impl InvocationTargetResolver for TestSchemas {
        fn resolve_latest_invocation_target(
            &self,
            service_name: impl AsRef<str>,
            handler_name: impl AsRef<str>,
        ) -> Option<InvocationTargetMetadata> {
            (service_name.as_ref() == "registered" && handler_name.as_ref() == "h").then(|| {
                InvocationTargetMetadata::from_external_semantics(
                    InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive),
                    RequestedRetentions::default(),
                )
            })
        }

        fn resolve_latest_service_type(
            &self,
            service_name: impl AsRef<str>,
        ) -> Option<ServiceType> {
            (service_name.as_ref() == "registered").then_some(ServiceType::VirtualObject)
        }

        fn resolve_invocation_attempt_options(
            &self,
            _: &DeploymentId,
            _: impl AsRef<str>,
            _: impl AsRef<str>,
        ) -> Option<InvocationAttemptOptions> {
            None
        }

        fn resolve_invocation_retry_policy(
            &self,
            _: Option<&DeploymentId>,
            _: impl AsRef<str>,
            _: impl AsRef<str>,
        ) -> (RetryIter<'static>, OnMaxAttempts) {
            unimplemented!()
        }
    }

    fn request(service: &str, handler: &str, key: &str, scope: Option<&str>) -> InvokeRequest {
        InvokeRequest {
            service_name: service.into(),
            handler_name: handler.into(),
            headers: vec![],
            key: key.into(),
            idempotency_key: None,
            scope: scope.map(ToOwned::to_owned),
            limit_key: None,
            span_relation: SpanRelation::None,
            parameter: Bytes::new(),
        }
    }

    fn relay(target_type: proto::RelayTargetType, effective_scope: Option<&str>) -> CallResolution {
        CallResolution::Relay(RelayCallSemantics {
            target_type: target_type.into(),
            effective_scope: effective_scope.map(ToOwned::to_owned),
            idempotency_retention_millis: Some(u64::MAX),
            workflow_completion_retention_millis: Some(u64::MAX),
            journal_retention_millis: Some(u64::MAX),
        })
    }

    fn resolve(
        request: InvokeRequest,
        call_resolution: CallResolution,
    ) -> std::result::Result<CallRequest, CommandPreconditionError> {
        resolve_call_request(&TestSchemas, request, call_resolution)
    }

    fn scope(scope: &str) -> Option<Scope> {
        Some(Scope::try_new(scope).unwrap())
    }

    #[test]
    fn relay_type_and_effective_scope_build_the_target() {
        let call = resolve(
            request("checkout", "run", "", Some("sdk-scope")),
            relay(proto::RelayTargetType::RelayTargetService, Some("tenant")),
        )
        .unwrap();
        assert_eq!(
            call.invocation_target,
            InvocationTarget::service("checkout", "run").with_scope(scope("tenant"))
        );
    }

    #[test]
    fn empty_effective_scope_is_unscoped() {
        let call = resolve(
            request("obj", "get", "k", Some("sdk-scope")),
            relay(
                proto::RelayTargetType::RelayTargetVirtualObjectShared,
                Some(""),
            ),
        )
        .unwrap();
        assert_eq!(
            call.invocation_target,
            InvocationTarget::virtual_object("obj", "k", "get", VirtualObjectHandlerType::Shared)
        );
    }

    #[test]
    fn absent_effective_scope_keeps_the_command_scope() {
        let call = resolve(
            request("obj", "get", "k", Some("sdk-scope")),
            relay(proto::RelayTargetType::RelayTargetVirtualObjectShared, None),
        )
        .unwrap();
        assert_eq!(call.invocation_target.scope(), scope("sdk-scope").as_ref());
    }

    #[test]
    fn relay_retentions_apply_with_server_defaults() {
        let call = resolve(
            InvokeRequest {
                idempotency_key: Some("idem".into()),
                ..request("checkout", "run", "", None)
            },
            CallResolution::Relay(RelayCallSemantics {
                target_type: proto::RelayTargetType::RelayTargetService.into(),
                effective_scope: None,
                idempotency_retention_millis: Some(60_000),
                workflow_completion_retention_millis: Some(u64::MAX),
                journal_retention_millis: None,
            }),
        )
        .unwrap();
        // The journal retention (server default: 1 day) is capped by the completion retention.
        assert_eq!(
            (
                call.completion_retention_duration,
                call.journal_retention_duration
            ),
            (Duration::from_secs(60), Duration::from_secs(60))
        );
    }

    #[test]
    fn workflow_run_takes_the_workflow_completion_retention() {
        let call = resolve(
            request("wf", "run", "k", None),
            relay(proto::RelayTargetType::RelayTargetWorkflow, None),
        )
        .unwrap();
        assert_eq!(call.completion_retention_duration, DAY);
    }

    #[test]
    fn relay_service_with_key_is_rejected() {
        assert_that!(
            resolve(
                request("checkout", "run", "k", None),
                relay(proto::RelayTargetType::RelayTargetService, None),
            ),
            err(pat!(CommandPreconditionError::UnexpectedKey(_)))
        );
    }

    #[test]
    fn relay_not_found_is_service_handler_not_found() {
        assert_that!(
            resolve(
                request("unknown", "h", "", None),
                relay(proto::RelayTargetType::RelayTargetNotFound, None),
            ),
            err(pat!(CommandPreconditionError::ServiceHandlerNotFound(_, _)))
        );
    }

    #[test]
    fn relay_unspecified_is_unresolved() {
        assert_that!(
            resolve(
                request("unknown", "h", "", None),
                relay(proto::RelayTargetType::Unspecified, None),
            ),
            err(pat!(CommandPreconditionError::UnresolvedCallTarget(_, _)))
        );
    }

    #[test]
    fn relay_unknown_target_type_is_unresolved() {
        assert_that!(
            resolve(
                request("unknown", "h", "", None),
                CallResolution::Relay(RelayCallSemantics {
                    target_type: 42,
                    effective_scope: None,
                    idempotency_retention_millis: None,
                    workflow_completion_retention_millis: None,
                    journal_retention_millis: None,
                }),
            ),
            err(pat!(CommandPreconditionError::UnresolvedCallTarget(_, _)))
        );
    }

    #[test]
    fn local_registry_wins_over_relay() {
        let call = resolve(
            request("registered", "h", "k", None),
            relay(proto::RelayTargetType::RelayTargetService, None),
        )
        .unwrap();
        assert_eq!(
            call.invocation_target,
            InvocationTarget::virtual_object(
                "registered",
                "k",
                "h",
                VirtualObjectHandlerType::Exclusive
            )
        );
    }

    #[test]
    fn local_registry_callee_takes_the_effective_scope() {
        let call = resolve(
            request("registered", "h", "k", Some("sdk-scope")),
            relay(proto::RelayTargetType::RelayTargetService, Some("tenant")),
        )
        .unwrap();
        assert_eq!(call.invocation_target.scope(), scope("tenant").as_ref());
    }

    #[test]
    fn registered_service_unknown_handler_is_not_found_with_relay() {
        assert_that!(
            resolve(
                request("registered", "other", "k", None),
                relay(proto::RelayTargetType::RelayTargetService, None),
            ),
            err(pat!(CommandPreconditionError::ServiceHandlerNotFound(_, _)))
        );
    }

    #[test]
    fn registered_service_unknown_handler_is_not_found_without_relay() {
        assert_that!(
            resolve(
                request("registered", "other", "k", None),
                CallResolution::Guess
            ),
            err(pat!(CommandPreconditionError::ServiceHandlerNotFound(_, _)))
        );
    }

    #[test]
    fn call_resolution_from_non_preflight_stream_is_not_found() {
        assert_that!(
            resolve(request("unknown", "h", "k", None), CallResolution::NotFound),
            err(pat!(CommandPreconditionError::ServiceHandlerNotFound(_, _)))
        );
    }

    #[test]
    fn call_resolution_is_guessed_in_other_modes() {
        let call = resolve(request("unknown", "h", "k", None), CallResolution::Guess).unwrap();
        assert_eq!(
            call.invocation_target,
            InvocationTarget::virtual_object(
                "unknown",
                "k",
                "h",
                VirtualObjectHandlerType::Exclusive
            )
        );
    }

    fn preflight_response(
        deployment_id: impl ToString,
        protocol_version: u32,
        protocol_type: preflight::ProtocolType,
    ) -> proto::PreflightResponseMessage {
        proto::PreflightResponseMessage {
            deployment_id: deployment_id.to_string(),
            protocol_version,
            protocol_type: protocol_type.into(),
            sdk_version: Some("sdk/1.0".to_owned()),
        }
    }

    #[test]
    fn valid_preflight_response() {
        let deployment_id = DeploymentId::new();
        assert_eq!(
            validate_preflight_response(
                preflight_response(deployment_id, 6, preflight::ProtocolType::RequestResponse),
                None
            )
            .unwrap(),
            NegotiatedPreflight {
                deployment_id,
                service_protocol_version: ServiceProtocolVersion::V6,
                protocol_type: ProtocolType::RequestResponse,
                sdk_version: Some("sdk/1.0".to_owned()),
            }
        );
    }

    #[test]
    fn preflight_response_honoring_the_pin() {
        let deployment_id = DeploymentId::new();
        let pin = PinnedDeployment::new(deployment_id, ServiceProtocolVersion::V6);
        assert_that!(
            validate_preflight_response(
                preflight_response(deployment_id, 6, preflight::ProtocolType::BidiStream),
                Some(&pin)
            ),
            ok(anything())
        );
    }

    fn assert_bad_preflight_response(
        response: proto::PreflightResponseMessage,
        pin: Option<&PinnedDeployment>,
    ) {
        assert_that!(
            validate_preflight_response(response, pin),
            err(pat!(InvokerError::BadPreflightResponse(_)))
        );
    }

    #[test]
    fn preflight_version_below_the_advertised_range() {
        assert_bad_preflight_response(
            preflight_response(DeploymentId::new(), 4, preflight::ProtocolType::BidiStream),
            None,
        );
    }

    #[test]
    fn preflight_version_above_the_advertised_range() {
        assert_bad_preflight_response(
            preflight_response(DeploymentId::new(), 8, preflight::ProtocolType::BidiStream),
            None,
        );
    }

    #[test]
    fn preflight_without_protocol_type() {
        assert_bad_preflight_response(
            preflight_response(DeploymentId::new(), 6, preflight::ProtocolType::Unspecified),
            None,
        );
    }

    #[test]
    fn preflight_with_bad_deployment_id() {
        assert_bad_preflight_response(
            preflight_response("dp", 6, preflight::ProtocolType::BidiStream),
            None,
        );
    }

    #[test]
    fn preflight_with_another_deployment_than_the_pin() {
        let pin = PinnedDeployment::new(DeploymentId::new(), ServiceProtocolVersion::V6);
        assert_bad_preflight_response(
            preflight_response(DeploymentId::new(), 6, preflight::ProtocolType::BidiStream),
            Some(&pin),
        );
    }

    #[test]
    fn preflight_with_another_version_than_the_pin() {
        let deployment_id = DeploymentId::new();
        let pin = PinnedDeployment::new(deployment_id, ServiceProtocolVersion::V6);
        assert_bad_preflight_response(
            preflight_response(deployment_id, 7, preflight::ProtocolType::BidiStream),
            Some(&pin),
        );
    }
}
