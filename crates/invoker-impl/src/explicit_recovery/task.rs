// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One attempt: resolves the deployment and the version, records the start, reads the
//! snapshot, opens the stream and runs the runner.

use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderName, HeaderValue};
use http_body::Frame;
use tokio::sync::mpsc;
use tracing::{debug, instrument};

use restate_service_client::{Endpoint, Method, Parts, Request, ServiceClient};
use restate_service_protocol_explicit_recovery::codec::{Decoder, Message, encode};
use restate_service_protocol_explicit_recovery::proto::{self, StateScope, start_message};
use restate_service_protocol_explicit_recovery::{CONTENT_TYPE_V1, MAX_VERSION, MIN_VERSION};
use restate_types::identifiers::{InvocationId, ServiceId};
use restate_types::invocation::InvocationTarget;
use restate_types::journal_v2::SignalResult;
use restate_types::live::Live;
use restate_types::schema::deployment::{DeploymentResolver, DeploymentType, ProtocolType};
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::service_protocol::ProtocolFamily;
use restate_util_string::ReString;
use restate_worker_api::invoker::explicit_recovery::{
    ErEffectKind, ErStateScope, ErToInvoker, ExplicitRecoveryReader,
    ExplicitRecoveryReaderTransaction,
};

use super::actions::ActionContext;
use super::book::AttemptBook;
use super::runner::{ErReads, Runner};
use super::{EffectSink, ErTaskOutput, InputItem};
use crate::error::InvokerError;
use crate::invocation_task::{ResponseChunk, ResponseStream, new_invoker_body};

#[allow(clippy::declare_interior_mutable_const)]
const INVOCATION_ID_HEADER_NAME: HeaderName = HeaderName::from_static("x-restate-invocation-id");
#[allow(clippy::declare_interior_mutable_const)]
const SERVICE_KEY_HEADER_NAME: HeaderName = HeaderName::from_static("x-restate-service-key");

/// How long to wait for the SDK to close its side after the attempt ended.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// The seq of the attempt start: the first effect of every attempt.
const ATTEMPT_STARTED_SEQ: u64 = 1;

pub(crate) struct ErTask<Schemas> {
    pub client: ServiceClient,
    pub schemas: Live<Schemas>,
    pub invocation_id: InvocationId,
    pub invocation_target: InvocationTarget,
    pub attempt: u32,
    pub limit_key: Option<ReString>,
    pub idempotency_key: Option<ReString>,
    pub default_inactivity_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub eager_state_size_limit: usize,
    pub message_size_limit: usize,
    pub effects: EffectSink,
    pub notifications: mpsc::UnboundedReceiver<ErToInvoker>,
}

impl<Schemas> ErTask<Schemas>
where
    Schemas: DeploymentResolver + InvocationTargetResolver,
{
    #[instrument(
        level = "info",
        name = "invoker_explicit_recovery_task",
        fields(
            restate.invocation.id = %self.invocation_id,
            restate.invocation.target = %self.invocation_target,
            restate.invocation.attempt = self.attempt,
        ),
        skip_all,
    )]
    pub(crate) async fn run<R>(mut self, reader: R)
    where
        R: ExplicitRecoveryReader + Clone + Send,
    {
        if let Err(error) = self.run_inner(reader).await {
            debug!(%error, "Explicit recovery attempt failed");
            self.effects.terminal(ErTaskOutput::Failed(error));
        }
    }

    /// Runs the attempt, and reports how it ended unless it fails before the stream.
    async fn run_inner<R>(&mut self, mut reader: R) -> Result<(), InvokerError>
    where
        R: ExplicitRecoveryReader + Clone + Send,
    {
        let service_name = self.invocation_target.service_name().clone();
        let handler_name = self.invocation_target.handler_name().clone();

        // The snapshot the attempt starts from.
        let mut txn = reader
            .er_transaction()
            .map_err(|e| InvokerError::JournalReader(e.into()))?;
        let metadata = txn
            .read_er_metadata(&self.invocation_id)
            .await
            .map_err(|e| InvokerError::JournalReader(e.into()))?
            .filter(|m| m.status.attempt == self.attempt)
            .ok_or(InvokerError::NotInvoked)?;

        // The deployment: pinned by the first attempt (D7), else the latest.
        let schemas = self.schemas.live_load();
        let deployment = match metadata.status.pinned_deployment_id {
            Some(id) => schemas
                .get_deployment(&id)
                .ok_or(InvokerError::UnknownDeployment(id))?,
            None => schemas
                .resolve_latest_deployment_for_service(&service_name)
                .ok_or(InvokerError::NoDeploymentForService)?,
        };
        let options = schemas
            .resolve_invocation_attempt_options(&deployment.id, &service_name, &handler_name)
            .ok_or(InvokerError::NoDeploymentForService)?;
        // D36: the family is fixed at creation; the handler must still be on it.
        if options.protocol_family != ProtocolFamily::ExplicitRecovery {
            return Err(InvokerError::WrongProtocolFamily {
                service: service_name.to_string(),
                handler: handler_name.to_string(),
                expected: options.protocol_family,
                actual: ProtocolFamily::ExplicitRecovery,
            });
        }
        let version = deployment
            .explicit_recovery_protocol_versions
            .as_ref()
            .and_then(|range| {
                (MIN_VERSION..=MAX_VERSION)
                    .rev()
                    .find(|v| range.contains(v))
            })
            .ok_or_else(|| {
                InvokerError::ErNoCommonVersion(
                    deployment.id,
                    deployment.explicit_recovery_protocol_versions.clone(),
                )
            })?;
        let DeploymentType::Http {
            address,
            protocol_type: ProtocolType::BidiStream,
            http_version: http::Version::HTTP_2,
            auth,
        } = &deployment.ty
        else {
            return Err(InvokerError::ErNeedsHttp2Bidi(deployment.id));
        };
        let inactivity_timeout = options
            .inactivity_timeout
            .unwrap_or(self.default_inactivity_timeout);

        self.effects.effect(
            ATTEMPT_STARTED_SEQ,
            ErEffectKind::AttemptStarted {
                deployment_id: deployment.id,
                protocol_version: u16::try_from(version).unwrap_or(u16::MAX),
            },
        );

        // The input and the preloaded state: invocation scope first, then object scope, in one
        // size limit.
        let (headers, input) = txn
            .read_input(&self.invocation_id)
            .await
            .map_err(|e| InvokerError::JournalReader(e.into()))?
            .ok_or(InvokerError::ErMissingInput)?;
        let limit = self.eager_state_size_limit.min(self.message_size_limit);
        let (invocation_state, invocation_partial) = txn
            .read_invocation_state_preload(
                &self.invocation_id,
                &options.invocation_state_preload_policy,
                limit,
            )
            .await
            .map_err(|e| InvokerError::StateReader(e.into()))?;
        let used: usize = invocation_state
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum();
        let service_id = self.invocation_target.as_keyed_service_id();
        let (object_state, object_partial) = match &service_id {
            Some(service_id) => txn
                .read_object_state_preload(
                    service_id,
                    &options.state_preload_policy,
                    limit.saturating_sub(used),
                )
                .await
                .map_err(|e| InvokerError::StateReader(e.into()))?,
            // D59: no key, no object state.
            None => (vec![], true),
        };
        drop(txn);

        let entry = |scope: StateScope, (key, value): (Bytes, Bytes)| start_message::StateEntry {
            scope: scope.into(),
            key,
            value: Some(proto::Value { content: value }),
        };
        let start = proto::StartMessage {
            id: Bytes::copy_from_slice(&self.invocation_id.to_bytes()),
            debug_id: self.invocation_id.to_string(),
            key: self
                .invocation_target
                .key()
                .map(|k| k.to_string())
                .unwrap_or_default(),
            headers: headers
                .into_iter()
                .map(|h| proto::Header {
                    key: h.name.to_string(),
                    value: h.value.to_string(),
                })
                .collect(),
            input: Some(proto::Value { content: input }),
            scope: self.invocation_target.scope().map(|s| s.to_string()),
            limit_key: self.limit_key.as_ref().map(|k| k.to_string()),
            idempotency_key: self.idempotency_key.as_ref().map(|k| k.to_string()),
            attempt: self.attempt,
            next_result_id: metadata.status.next_result_id.get(),
            cancelled: metadata.status.cancelled,
            state_map: invocation_state
                .into_iter()
                .map(|e| entry(StateScope::Invocation, e))
                .chain(
                    object_state
                        .into_iter()
                        .map(|e| entry(StateScope::Object, e)),
                )
                .collect(),
            is_object_scope_partial_state: object_partial,
            is_invocation_scope_partial_state: invocation_partial,
            features: vec![],
        };

        // Open the stream.
        let path: PathAndQuery = format!("/invoke/{service_name}/{handler_name}")
            .try_into()
            .expect("valid invocation path");
        let content_type = HeaderValue::from_static(CONTENT_TYPE_V1);
        let mut request_headers = HeaderMap::from_iter([
            (http::header::CONTENT_TYPE, content_type.clone()),
            (http::header::ACCEPT, content_type),
            (
                INVOCATION_ID_HEADER_NAME,
                HeaderValue::try_from(self.invocation_id.to_string())
                    .expect("invocation id is a valid header"),
            ),
        ]);
        if let Some(key) = self.invocation_target.key()
            && let Ok(value) = HeaderValue::from_str(urlencoding::encode(key).as_ref())
        {
            request_headers.insert(SERVICE_KEY_HEADER_NAME, value);
        }
        request_headers.extend(deployment.additional_headers.clone());
        let mut parts = Parts::new(
            Method::Post,
            Endpoint::Http(address.clone(), Some(http::Version::HTTP_2), auth.clone()),
            path,
            request_headers,
        );
        if let Some(key) = self.invocation_target.key() {
            parts = parts.with_request_identity_sub_field(key.clone());
        }
        let (body_tx, body_rx) = mpsc::unbounded_channel();
        let request = Request::new(parts, new_invoker_body(body_rx));
        let response = ResponseStream::new(Box::pin(self.client.call(request)));
        let input_stream = ErDecoderStream::new(response, self.message_size_limit);

        body_tx
            .send(Ok(Frame::data(encode(&Message::Start(start)))))
            .map_err(|_| InvokerError::UnexpectedClosedRequestStream)?;

        let book = AttemptBook::new(
            metadata.status.next_result_id.get(),
            metadata.status.cancelled,
            ATTEMPT_STARTED_SEQ + 1,
        );
        let reads = StoreReads {
            reader,
            invocation_id: self.invocation_id,
            service_id,
        };
        let runner = Runner::new(
            ActionContext {
                invocation_id: self.invocation_id,
                invocation_target: &self.invocation_target,
                span_context: &metadata.span_context,
            },
            schemas,
            reads,
            book,
            self.effects.clone(),
            body_tx,
            inactivity_timeout,
            self.shutdown_timeout,
        );
        let notifications = std::mem::replace(&mut self.notifications, mpsc::unbounded_channel().1);
        let mut input_stream = Box::pin(input_stream);
        let output = runner.run(&mut input_stream, notifications).await;
        debug!(?output, "Explicit recovery attempt ended");
        self.effects.terminal(output);
        // The runtime closed its side; the SDK closes after it (D35). Drain what is left, so the
        // stream ends cleanly instead of being reset.
        let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
            while let Some(Ok(_)) = input_stream.next().await {}
        })
        .await;
        Ok(())
    }
}

/// Reads of the latest committed state, through the partition's storage.
struct StoreReads<R> {
    reader: R,
    invocation_id: InvocationId,
    service_id: Option<ServiceId>,
}

impl<R: ExplicitRecoveryReader + Send> ErReads for StoreReads<R> {
    async fn get_state(
        &mut self,
        scope: ErStateScope,
        key: &Bytes,
    ) -> Result<Option<Bytes>, InvokerError> {
        match scope {
            ErStateScope::Object => match &self.service_id {
                Some(service_id) => self.reader.read_object_state(service_id, key).await,
                None => return Ok(None),
            },
            ErStateScope::Invocation => {
                self.reader
                    .read_invocation_state(&self.invocation_id, key)
                    .await
            }
        }
        .map_err(|e| InvokerError::StateReader(e.into()))
    }

    async fn get_state_keys(&mut self, scope: ErStateScope) -> Result<Vec<Bytes>, InvokerError> {
        match scope {
            ErStateScope::Object => match &self.service_id {
                Some(service_id) => self.reader.read_object_state_keys(service_id).await,
                None => return Ok(vec![]),
            },
            ErStateScope::Invocation => {
                self.reader
                    .read_invocation_state_keys(&self.invocation_id)
                    .await
            }
        }
        .map_err(|e| InvokerError::StateReader(e.into()))
    }

    async fn read_signal(
        &mut self,
        name: &str,
        index: u32,
    ) -> Result<Option<SignalResult>, InvokerError> {
        self.reader
            .read_signal(&self.invocation_id, name, index)
            .await
            .map_err(|e| InvokerError::StateReader(e.into()))
    }
}

pin_project_lite::pin_project! {
    /// The SDK's side of the stream: the response headers, then messages, and `Partial` for the
    /// chunks that complete no message.
    struct ErDecoderStream<S> {
        #[pin]
        inner: S,
        decoder: Decoder,
    }
}

impl<S> ErDecoderStream<S> {
    fn new(inner: S, message_size_limit: usize) -> Self {
        Self {
            inner,
            decoder: Decoder::new(message_size_limit),
        }
    }
}

impl<S> Stream for ErDecoderStream<S>
where
    S: Stream<Item = Result<ResponseChunk, InvokerError>>,
{
    type Item = Result<InputItem, InvokerError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.decoder.consume_next() {
            Ok(Some(message)) => return Poll::Ready(Some(Ok(InputItem::Message(message)))),
            Ok(None) => {}
            Err(e) => return Poll::Ready(Some(Err(e.into()))),
        }
        match ready!(this.inner.as_mut().poll_next(cx)) {
            Some(Ok(ResponseChunk::Parts(parts))) => {
                Poll::Ready(Some(Ok(InputItem::Headers(parts))))
            }
            Some(Ok(ResponseChunk::Data(buf))) => {
                this.decoder.push(buf);
                Poll::Ready(Some(match this.decoder.consume_next() {
                    Ok(Some(message)) => Ok(InputItem::Message(message)),
                    Ok(None) => Ok(InputItem::Partial),
                    Err(e) => Err(e.into()),
                }))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}
