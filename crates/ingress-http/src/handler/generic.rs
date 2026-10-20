// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generic invoke endpoint: `POST /restate/generic/{call|send}`.
//!
//! The body is an envelope decided by a trusted middleware, carrying the target, its type,
//! scope, retentions, headers and payload. The runtime applies it without consulting the schema
//! registry for semantics.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use bytestring::ByteString;
use http::{Method, Request, Response, header};
use http_body_util::{BodyExt, Full};
use metrics::Label;
use serde::Deserialize;
use serde_with::serde_as;
use tracing::{debug, trace};
use ulid::Ulid;

use restate_tracing_instrumentation::semconv::attribute::RPC_SERVICE;
use restate_types::Scope;
use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{
    Header, InvocationRequest, InvocationRequestHeader, InvocationTarget, InvocationTargetType,
    SpanRelation, WorkflowHandlerType,
};
use restate_types::limit_key::LimitKey;
use restate_types::nodes_config::ClusterFeature;
use restate_types::schema::invocation_target::{
    InvocationTargetMetadata, InvocationTargetResolver, RequestedRetentions,
};
use restate_util_string::{ReString, RestateString};

use super::path_parsing::InvokeType;
use super::service_handler::record_request_metrics;
use super::tracing::prepare_tracing_span;
use super::{Handler, HandlerError, is_reserved_header_name};
use crate::RequestDispatcher;

pub(crate) const GENERIC_INVOCATION_CBOR: &str =
    "application/vnd.restate.generic-invocation.v1+cbor";
pub(crate) const GENERIC_INVOCATION_JSON: &str =
    "application/vnd.restate.generic-invocation.v1+json";

/// The generic invocation envelope, v1. `P` is the payload encoding: a byte string in CBOR, a
/// base64 string in JSON.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Envelope<P> {
    service: String,
    handler: String,
    target_type: String,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    limit_key: Option<String>,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default = "Option::default")]
    payload: Option<P>,
    #[serde(default)]
    delay_millis: Option<u64>,
    #[serde(default)]
    idempotency_retention_millis: Option<u64>,
    #[serde(default)]
    workflow_completion_retention_millis: Option<u64>,
    #[serde(default)]
    journal_retention_millis: Option<u64>,
}

#[serde_as]
#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct JsonPayload(#[serde_as(as = "serde_with::base64::Base64")] Vec<u8>);

impl From<JsonPayload> for Bytes {
    fn from(value: JsonPayload) -> Self {
        value.0.into()
    }
}

/// The envelope validated and mapped to the runtime's model.
#[derive(Debug)]
struct GenericInvocation {
    target: InvocationTarget,
    metadata: InvocationTargetMetadata,
    idempotency_key: Option<ByteString>,
    limit_key: LimitKey<ReString>,
    headers: Vec<Header>,
    payload: Bytes,
    delay: Option<Duration>,
}

fn decode_envelope(content_type: &str, body: &[u8]) -> Result<Envelope<Bytes>, HandlerError> {
    let bad = |err: &dyn std::fmt::Display| HandlerError::BadGenericInvocation(err.to_string());
    match content_type {
        GENERIC_INVOCATION_CBOR => ciborium::from_reader(body).map_err(|err| bad(&err)),
        GENERIC_INVOCATION_JSON => {
            let envelope: Envelope<JsonPayload> =
                serde_json::from_slice(body).map_err(|err| bad(&err))?;
            Ok(Envelope {
                service: envelope.service,
                handler: envelope.handler,
                target_type: envelope.target_type,
                key: envelope.key,
                scope: envelope.scope,
                limit_key: envelope.limit_key,
                idempotency_key: envelope.idempotency_key,
                headers: envelope.headers,
                payload: envelope.payload.map(Into::into),
                delay_millis: envelope.delay_millis,
                idempotency_retention_millis: envelope.idempotency_retention_millis,
                workflow_completion_retention_millis: envelope.workflow_completion_retention_millis,
                journal_retention_millis: envelope.journal_retention_millis,
            })
        }
        _ => Err(HandlerError::UnsupportedGenericInvocationContentType),
    }
}

impl TryFrom<(Envelope<Bytes>, &InvokeType)> for GenericInvocation {
    type Error = HandlerError;

    fn try_from(
        (envelope, invoke_ty): (Envelope<Bytes>, &InvokeType),
    ) -> Result<Self, Self::Error> {
        let bad = |reason: String| HandlerError::BadGenericInvocation(reason);

        let target_ty = InvocationTargetType::from_registry_name(&envelope.target_type)
            .ok_or_else(|| bad(format!("invalid targetType '{}'", envelope.target_type)))?;
        let scope = envelope
            .scope
            .map(|scope| Scope::try_new(&scope).map_err(HandlerError::BadScopeValue))
            .transpose()?;

        let target = match (target_ty, envelope.key) {
            (InvocationTargetType::Service, None) => {
                InvocationTarget::service(envelope.service, envelope.handler)
            }
            (InvocationTargetType::VirtualObject(handler_ty), Some(key)) => {
                InvocationTarget::virtual_object(
                    envelope.service,
                    key,
                    envelope.handler,
                    handler_ty,
                )
            }
            (InvocationTargetType::Workflow(handler_ty), Some(key)) => {
                InvocationTarget::workflow(envelope.service, key, envelope.handler, handler_ty)
            }
            (InvocationTargetType::Service, Some(_)) => {
                return Err(bad(
                    "key is not allowed with targetType 'service'".to_owned()
                ));
            }
            (_, None) => {
                return Err(bad(format!(
                    "key is required with targetType '{}'",
                    envelope.target_type
                )));
            }
        }
        .with_scope(scope);

        let limit_key: LimitKey<ReString> = match envelope.limit_key {
            Some(limit_key) => {
                limit_key
                    .parse()
                    .map_err(|e: restate_types::limit_key::ParseError| {
                        HandlerError::InvalidLimitKey(e.to_string())
                    })?
            }
            None => LimitKey::None,
        };
        if !limit_key.is_empty() && target.scope().is_none() {
            return Err(HandlerError::LimitKeyWithoutScope);
        }

        let idempotency_key = match envelope.idempotency_key {
            Some(key) if key.is_empty() => {
                return Err(bad("idempotencyKey must be non-empty".to_owned()));
            }
            Some(_)
                if target_ty == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow) =>
            {
                return Err(HandlerError::UnsupportedIdempotencyKey);
            }
            key => key.map(ByteString::from),
        };

        let delay = match (invoke_ty, envelope.delay_millis) {
            (InvokeType::Call, Some(_)) => return Err(HandlerError::UnsupportedDelay),
            (_, delay_millis) => delay_millis.map(Duration::from_millis),
        };

        let headers = envelope
            .headers
            .into_iter()
            .filter(|(name, _)| !is_reserved_header_name(name))
            .map(|(name, value)| Header::new(name, value))
            .collect();

        let metadata = InvocationTargetMetadata::from_external_semantics(
            target_ty,
            RequestedRetentions {
                idempotency: envelope
                    .idempotency_retention_millis
                    .map(Duration::from_millis),
                workflow_completion: envelope
                    .workflow_completion_retention_millis
                    .map(Duration::from_millis),
                journal: envelope.journal_retention_millis.map(Duration::from_millis),
            },
        );

        Ok(Self {
            target,
            metadata,
            idempotency_key,
            limit_key,
            headers,
            payload: envelope.payload.unwrap_or_default(),
            delay,
        })
    }
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_generic<B: http_body::Body>(
        self,
        req: Request<B>,
        invoke_ty: InvokeType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        if !Configuration::pinned().ingress.experimental_generic_invoke {
            return Err(HandlerError::NotFound);
        }
        if req.method() != Method::POST {
            return Err(HandlerError::MethodNotAllowed);
        }
        let content_type = req
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|ct| {
                ct.to_str()
                    .map_err(|e| HandlerError::BadHeader(header::CONTENT_TYPE, e))
            })
            .transpose()?
            .ok_or(HandlerError::UnsupportedGenericInvocationContentType)?
            .to_owned();

        let start_time = Instant::now();
        let mut labels = vec![
            Label::new(RPC_SERVICE, ""),
            Label::new("rpc.type", invoke_ty.as_static_str()),
        ];
        let result = async {
            let (parts, body) = req.into_parts();
            let body = body
                .collect()
                .await
                .map_err(|e| HandlerError::Body(e.into()))?
                .to_bytes();
            let invocation =
                GenericInvocation::try_from((decode_envelope(&content_type, &body)?, &invoke_ty))?;
            labels[0] = Label::new(RPC_SERVICE, invocation.target.service_name().to_string());
            self.process_generic_invocation(Request::from_parts(parts, ()), invoke_ty, invocation)
                .await
        }
        .await;

        record_request_metrics(labels, start_time, &result);
        result
    }

    async fn process_generic_invocation(
        self,
        req: Request<()>,
        invoke_ty: InvokeType,
        invocation: GenericInvocation,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let GenericInvocation {
            target,
            metadata,
            mut idempotency_key,
            limit_key,
            headers,
            payload,
            delay,
        } = invocation;

        // Same as the regular call/send paths: inject a random idempotency key for calls that
        // have none, if the `controlled-idempotent-sharding` is enabled.
        if self
            .cluster_features
            .contains(ClusterFeature::ControlledIdempotentSharding)
            && matches!(
                metadata.target_ty,
                InvocationTargetType::Service | InvocationTargetType::VirtualObject(_)
            )
            && idempotency_key.is_none()
        {
            idempotency_key = Some(Ulid::generate().to_string().into());
        }

        let invocation_retention = metadata.compute_retention(idempotency_key.is_some());
        let invocation_id = InvocationId::generate(&target, idempotency_key.as_deref());
        let ingress_span_context = prepare_tracing_span(&invocation_id, &target, &req);

        debug!(
            restate.invocation.id = %invocation_id,
            restate.invocation.target = %target.short(),
            "Processing generic invocation request"
        );
        trace!(rpc.request = ?payload);

        let mut invocation_request_header =
            InvocationRequestHeader::initialize(invocation_id, target);
        invocation_request_header.with_related_span(SpanRelation::parent(ingress_span_context));
        invocation_request_header.with_retention(invocation_retention);
        invocation_request_header.idempotency_key = idempotency_key;
        invocation_request_header.limit_key = limit_key;
        invocation_request_header.headers = headers;

        match invoke_ty {
            InvokeType::Call => {
                Self::handle_service_call(
                    Arc::new(InvocationRequest::new(invocation_request_header, payload)),
                    metadata,
                    self.dispatcher,
                )
                .await
            }
            InvokeType::Send => {
                invocation_request_header.execution_time =
                    delay.map(|d| SystemTime::now() + d).map(Into::into);
                Self::handle_service_send(
                    Arc::new(InvocationRequest::new(invocation_request_header, payload)),
                    self.dispatcher,
                )
                .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ciborium::Value;
    use serde_json::json;

    use restate_types::config::{Configuration, set_current_config};
    use restate_types::invocation::VirtualObjectHandlerType;
    use restate_util_time::FriendlyDuration;

    const DAY: Duration = Duration::from_secs(60 * 60 * 24);

    fn parse_json(
        envelope: serde_json::Value,
        invoke_ty: InvokeType,
    ) -> std::result::Result<GenericInvocation, HandlerError> {
        let body = serde_json::to_vec(&envelope).unwrap();
        GenericInvocation::try_from((decode_envelope(GENERIC_INVOCATION_JSON, &body)?, &invoke_ty))
    }

    fn parse_cbor(
        envelope: Vec<(&str, Value)>,
    ) -> std::result::Result<GenericInvocation, HandlerError> {
        let mut body = Vec::new();
        ciborium::into_writer(
            &Value::Map(
                envelope
                    .into_iter()
                    .map(|(k, v)| (Value::Text(k.to_owned()), v))
                    .collect(),
            ),
            &mut body,
        )
        .unwrap();
        GenericInvocation::try_from((
            decode_envelope(GENERIC_INVOCATION_CBOR, &body)?,
            &InvokeType::Call,
        ))
    }

    fn service_envelope() -> serde_json::Value {
        json!({"service": "greeter", "handler": "greet", "targetType": "service"})
    }

    fn with(
        mut envelope: serde_json::Value,
        key: &str,
        value: serde_json::Value,
    ) -> serde_json::Value {
        envelope[key] = value;
        envelope
    }

    #[test]
    fn cbor_envelope() {
        let invocation = parse_cbor(vec![
            ("service", Value::Text("counter".to_owned())),
            ("handler", Value::Text("get".to_owned())),
            (
                "targetType",
                Value::Text("virtual_object_shared".to_owned()),
            ),
            ("key", Value::Text("my-key".to_owned())),
            ("scope", Value::Text("tenant".to_owned())),
            ("payload", Value::Bytes(b"hello".to_vec())),
        ])
        .unwrap();
        assert_eq!(
            (invocation.target, invocation.payload),
            (
                InvocationTarget::virtual_object(
                    "counter",
                    "my-key",
                    "get",
                    VirtualObjectHandlerType::Shared
                )
                .with_scope(Some(Scope::try_new("tenant").unwrap())),
                Bytes::from_static(b"hello")
            )
        );
    }

    #[test]
    fn json_payload_is_base64() {
        let invocation = parse_json(
            with(service_envelope(), "payload", json!("aGVsbG8=")),
            InvokeType::Call,
        )
        .unwrap();
        assert_eq!(invocation.payload, Bytes::from_static(b"hello"));
    }

    #[test]
    fn absent_payload_is_empty() {
        let invocation = parse_json(service_envelope(), InvokeType::Call).unwrap();
        assert_eq!(invocation.payload, Bytes::new());
    }

    #[test]
    fn every_target_type() {
        let targets: Vec<_> = [
            ("service", None),
            ("virtual_object_exclusive", Some("k")),
            ("virtual_object_shared", Some("k")),
            ("workflow", Some("k")),
            ("workflow_shared", Some("k")),
        ]
        .into_iter()
        .map(|(target_type, key)| {
            let mut envelope = with(service_envelope(), "targetType", json!(target_type));
            if let Some(key) = key {
                envelope["key"] = json!(key);
            }
            parse_json(envelope, InvokeType::Send)
                .unwrap()
                .target
                .invocation_target_ty()
        })
        .collect();
        assert_eq!(
            targets,
            vec![
                InvocationTargetType::Service,
                InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive),
                InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Shared),
                InvocationTargetType::Workflow(WorkflowHandlerType::Workflow),
                InvocationTargetType::Workflow(WorkflowHandlerType::Shared),
            ]
        );
    }

    #[test]
    fn empty_key_is_allowed_for_keyed_targets() {
        let invocation = parse_json(
            with(
                with(
                    service_envelope(),
                    "targetType",
                    json!("virtual_object_exclusive"),
                ),
                "key",
                json!(""),
            ),
            InvokeType::Call,
        )
        .unwrap();
        assert_eq!(invocation.target.key().map(|k| k.as_ref()), Some(""));
    }

    #[test]
    fn reserved_headers_are_dropped() {
        let invocation = parse_json(
            with(
                service_envelope(),
                "headers",
                json!({"x-restate-foo": "bar", "my-header": "my-value"}),
            ),
            InvokeType::Call,
        )
        .unwrap();
        assert_eq!(
            invocation.headers,
            vec![Header::new("my-header", "my-value")]
        );
    }

    #[test]
    fn delay_on_send() {
        let invocation = parse_json(
            with(service_envelope(), "delayMillis", json!(1000)),
            InvokeType::Send,
        )
        .unwrap();
        assert_eq!(invocation.delay, Some(Duration::from_secs(1)));
    }

    #[test]
    fn default_retentions() {
        let invocation = parse_json(service_envelope(), InvokeType::Call).unwrap();
        assert_eq!(
            (
                invocation.metadata.completion_retention,
                invocation.metadata.journal_retention
            ),
            (DAY, DAY)
        );
    }

    #[test]
    fn workflow_run_takes_the_workflow_completion_retention() {
        let envelope = json!({
            "service": "wf", "handler": "run", "targetType": "workflow", "key": "k",
            "idempotencyRetentionMillis": 1000, "workflowCompletionRetentionMillis": 2000,
            "journalRetentionMillis": 0,
        });
        let invocation = parse_json(envelope, InvokeType::Call).unwrap();
        assert_eq!(
            (
                invocation.metadata.completion_retention,
                invocation.metadata.journal_retention
            ),
            (Duration::from_secs(2), Duration::ZERO)
        );
    }

    #[test]
    fn retentions_are_clamped_by_the_server_maximum() {
        let mut config = Configuration::default();
        config.invocation.max_idempotency_retention = Some(FriendlyDuration::from_secs(60));
        set_current_config(config);

        let invocation = parse_json(
            with(
                service_envelope(),
                "idempotencyRetentionMillis",
                json!(DAY.as_millis() as u64),
            ),
            InvokeType::Call,
        )
        .unwrap();
        assert_eq!(
            invocation.metadata.completion_retention,
            Duration::from_secs(60)
        );
    }

    fn assert_bad_request(envelope: serde_json::Value, invoke_ty: InvokeType) {
        let err = parse_json(envelope, invoke_ty).unwrap_err();
        assert_eq!(err.status_code(), http::StatusCode::BAD_REQUEST, "{err}");
    }

    #[test]
    fn unknown_key_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "retryPolicy", json!({})),
            InvokeType::Call,
        );
    }

    #[test]
    fn invalid_target_type_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "targetType", json!("virtual_object")),
            InvokeType::Call,
        );
    }

    #[test]
    fn key_on_service_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "key", json!("k")),
            InvokeType::Call,
        );
    }

    #[test]
    fn missing_key_on_keyed_target_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "targetType", json!("workflow")),
            InvokeType::Call,
        );
    }

    #[test]
    fn missing_required_field_is_rejected() {
        assert_bad_request(
            json!({"service": "greeter", "targetType": "service"}),
            InvokeType::Call,
        );
    }

    #[test]
    fn invalid_scope_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "scope", json!("")),
            InvokeType::Call,
        );
    }

    #[test]
    fn limit_key_without_scope_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "limitKey", json!("a")),
            InvokeType::Send,
        );
    }

    #[test]
    fn empty_idempotency_key_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "idempotencyKey", json!("")),
            InvokeType::Call,
        );
    }

    #[test]
    fn delay_on_call_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "delayMillis", json!(10)),
            InvokeType::Call,
        );
    }

    #[test]
    fn bad_base64_payload_is_rejected() {
        assert_bad_request(
            with(service_envelope(), "payload", json!("%%%")),
            InvokeType::Call,
        );
    }

    #[test]
    fn other_content_type_is_unsupported() {
        assert!(matches!(
            decode_envelope("application/json", b"{}"),
            Err(HandlerError::UnsupportedGenericInvocationContentType)
        ));
    }
}
