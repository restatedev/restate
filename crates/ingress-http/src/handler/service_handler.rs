// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use bytestring::ByteString;
use http::{HeaderMap, HeaderName, Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full};
use metrics::{counter, histogram};
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::{Instrument, debug, trace, trace_span};

use super::HandlerError;
use super::path_parsing::{InvokeType, ServiceRequestType, TargetType};
use super::tracing::prepare_tracing_span;
use super::{APPLICATION_JSON, Handler};
use crate::RequestDispatcher;
use crate::handler::responses::{IDEMPOTENCY_EXPIRES, X_RESTATE_ID};
use crate::metric_definitions::{INGRESS_REQUEST_DURATION, INGRESS_REQUESTS, REQUEST_COMPLETED};
use restate_types::identifiers::{InvocationId, WithInvocationId};
use restate_types::invocation::{
    Header, InvocationRequest, InvocationRequestHeader, InvocationTarget, InvocationTargetType,
    SpanRelation, WorkflowHandlerType,
};
use restate_types::schema::invocation_target::{
    InvocationTargetMetadata, InvocationTargetResolver,
};
use restate_types::time::MillisSinceEpoch;

pub(crate) const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
const DELAY_QUERY_PARAM: &str = "delay";
const X_RESTATE_INGRESS_PATH: ByteString = ByteString::from_static("x-restate-ingress-path");

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
pub(crate) enum SendStatus {
    Accepted,
    PreviouslyAccepted,
}

// IMPORTANT! If you touch this, please update crates/types/src/schema/openapi.rs too
#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
#[serde(rename_all = "camelCase")]
pub(crate) struct SendResponse {
    pub(crate) invocation_id: InvocationId,
    #[serde(
        with = "serde_with::As::<Option<serde_with::DisplayFromStr>>",
        skip_serializing_if = "Option::is_none",
        default
    )]
    execution_time: Option<humantime::Timestamp>,
    status: SendStatus,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_service_request<B: http_body::Body>(
        self,
        req: Request<B>,
        service_request: ServiceRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        let start_time = Instant::now();

        let ServiceRequestType {
            name: service_name,
            handler: handler_name,
            target,
            invoke_ty,
        } = service_request;

        let invocation_target_meta = if let Some(invocation_target) = self
            .schemas
            .pinned()
            .resolve_latest_invocation_target(&service_name, &handler_name)
        {
            if !invocation_target.public {
                return Err(HandlerError::PrivateService);
            }
            invocation_target
        } else {
            return Err(HandlerError::ServiceHandlerNotFound(
                service_name.clone(),
                handler_name.clone(),
            ));
        };

        // Check if Idempotency-Key is available
        let idempotency_key = parse_idempotency(req.headers())?;
        if idempotency_key.is_some()
            && invocation_target_meta.target_ty
                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            return Err(HandlerError::UnsupportedIdempotencyKey);
        }

        // Craft Invocation Target and Id
        let invocation_target = if let TargetType::Keyed { key } = target {
            match invocation_target_meta.target_ty {
                InvocationTargetType::VirtualObject(handler_ty) => {
                    InvocationTarget::virtual_object(
                        &*service_name,
                        key,
                        &*handler_name,
                        handler_ty,
                    )
                }
                InvocationTargetType::Workflow(handler_ty) => {
                    InvocationTarget::workflow(&*service_name, key, &*handler_name, handler_ty)
                }
                InvocationTargetType::Service => {
                    panic!(
                        "Unexpected keyed target, this should have been checked before in the path parsing."
                    )
                }
            }
        } else {
            InvocationTarget::service(&*service_name, &*handler_name)
        };
        let invocation_id = InvocationId::generate(&invocation_target, idempotency_key.as_deref());

        // Prepare the tracing span
        let runtime_span = tracing::info_span!(
            "ingress",
            restate.invocation.id = %invocation_id,
            restate.invocation.target = %invocation_target.short()
        );

        let result = async move {
            let ingress_span_context =
                prepare_tracing_span(&invocation_id, &invocation_target, &req);

            debug!("Processing ingress request");

            let (parts, body) = req.into_parts();

            // Check HTTP Method
            if parts.method != Method::GET && parts.method != Method::POST {
                return Err(HandlerError::MethodNotAllowed);
            }

            // Collect body
            let body = body
                .collect()
                .await
                .map_err(|e| HandlerError::Body(e.into()))?
                .to_bytes();
            trace!(rpc.request = ?body);

            // Validate content-type and body
            invocation_target_meta.input_rules.validate(
                parts
                    .headers
                    .get(header::CONTENT_TYPE)
                    .map(|h| {
                        h.to_str()
                            .map_err(|e| HandlerError::BadHeader(header::CONTENT_TYPE, e))
                    })
                    .transpose()?,
                &body,
            )?;

            // Parse delay query parameter
            let delay = parse_delay(parts.uri.query())?;

            // Get headers
            let headers = parse_headers(parts)?;

            // Prepare service invocation
            let mut invocation_request_header =
                InvocationRequestHeader::initialize(invocation_id, invocation_target);
            invocation_request_header.with_related_span(SpanRelation::Parent(ingress_span_context));
            invocation_request_header.with_retention(
                invocation_target_meta.compute_retention(idempotency_key.is_some()),
            );
            if let Some(key) = idempotency_key {
                invocation_request_header.idempotency_key = Some(key);
            }
            invocation_request_header.headers = headers;

            match invoke_ty {
                InvokeType::Call => {
                    if delay.is_some() {
                        return Err(HandlerError::UnsupportedDelay);
                    }
                    Self::handle_service_call(
                        Arc::new(InvocationRequest::new(invocation_request_header, body)),
                        invocation_target_meta,
                        self.dispatcher,
                    )
                    .await
                }
                InvokeType::Send => {
                    invocation_request_header.execution_time =
                        delay.map(|d| SystemTime::now() + d).map(Into::into);

                    Self::handle_service_send(
                        Arc::new(InvocationRequest::new(invocation_request_header, body)),
                        self.dispatcher,
                    )
                    .await
                }
            }
        }
        .instrument(runtime_span)
        .await;

        // Note that we only record (mostly) successful requests here. We might want to
        // change this in the _near_ future.
        histogram!(
            INGRESS_REQUEST_DURATION,
            "rpc.service" => service_name.clone(),
        )
        .record(start_time.elapsed());

        counter!(
            INGRESS_REQUESTS,
            "status" => REQUEST_COMPLETED,
            "rpc.service" => service_name,
        )
        .increment(1);
        result
    }

    async fn handle_service_call(
        invocation_request: Arc<InvocationRequest>,
        invocation_target_metadata: InvocationTargetMetadata,
        dispatcher: Dispatcher,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let response = dispatcher
            .call(invocation_request)
            .instrument(trace_span!("Waiting for response"))
            .await?;

        Self::reply_with_invocation_response(response, move |_| Ok(invocation_target_metadata))
    }

    async fn handle_service_send(
        invocation_request: Arc<InvocationRequest>,
        dispatcher: Dispatcher,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let invocation_id = invocation_request.invocation_id();

        // Send the service invocation, wait for the submit notification
        let response = dispatcher.send(invocation_request).await?;

        trace!("Complete external HTTP send request successfully");
        Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .header(header::CONTENT_TYPE, APPLICATION_JSON)
            .header(X_RESTATE_ID, invocation_id.to_string())
            .body(Full::new(
                serde_json::to_vec(&SendResponse {
                    invocation_id,
                    execution_time: response
                        .execution_time
                        .and_then(|m| {
                            if m == MillisSinceEpoch::UNIX_EPOCH {
                                // Ignore
                                None
                            } else {
                                Some(m)
                            }
                        })
                        .map(SystemTime::from)
                        .map(Into::into),
                    status: if response.is_new_invocation {
                        SendStatus::Accepted
                    } else {
                        SendStatus::PreviouslyAccepted
                    },
                })
                .unwrap()
                .into(),
            ))
            .unwrap())
    }
}

fn parse_headers(parts: http::request::Parts) -> Result<Vec<Header>, HandlerError> {
    let mut headers = Vec::with_capacity(1 + parts.headers.keys_len());

    if let Some(path_and_query) = parts.uri.path_and_query() {
        headers.push(Header::new(X_RESTATE_INGRESS_PATH, path_and_query.as_str()));
    }

    for (k, v) in parts.headers {
        let Some(k) = k else {
            continue;
        };

        if k == header::CONNECTION
            || k == header::HOST
            || k == IDEMPOTENCY_KEY
            || k == IDEMPOTENCY_EXPIRES
        {
            continue;
        }

        let value = v
            .to_str()
            .map_err(|e| HandlerError::BadHeader(k.clone(), e))?;
        headers.push(Header::new(k.as_str(), value))
    }

    Ok(headers)
}

#[serde_as]
#[derive(Deserialize)]
#[serde(transparent)]
struct DurationQueryParam(#[serde_as(as = "restate_serde_util::DurationString")] Duration);

fn parse_delay(query: Option<&str>) -> Result<Option<Duration>, HandlerError> {
    if query.is_none() {
        return Ok(None);
    }

    for (k, v) in url::form_urlencoded::parse(query.unwrap().as_bytes()) {
        if k.eq_ignore_ascii_case(DELAY_QUERY_PARAM) {
            return Ok(Some(
                DurationQueryParam::deserialize(v.as_ref().into_deserializer())
                    .map_err(|e: serde::de::value::Error| {
                        HandlerError::BadDelayDuration(e.to_string())
                    })?
                    .0,
            ));
        }
    }

    Ok(None)
}

fn parse_idempotency(headers: &HeaderMap) -> Result<Option<ByteString>, HandlerError> {
    let idempotency_key = if let Some(idempotency_key) = headers.get(IDEMPOTENCY_KEY) {
        ByteString::from(
            idempotency_key
                .to_str()
                .map_err(|e| HandlerError::BadHeader(IDEMPOTENCY_KEY, e))?,
        )
    } else {
        return Ok(None);
    };

    Ok(Some(idempotency_key))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay() {
        assert_eq!(
            parse_delay(Some("delay=PT60S")).unwrap().unwrap(),
            Duration::from_secs(60),
        );
        assert_eq!(
            parse_delay(Some("delay=60+sec")).unwrap().unwrap(),
            Duration::from_secs(60),
        );
        assert_eq!(
            parse_delay(Some("delay=60sec")).unwrap().unwrap(),
            Duration::from_secs(60),
        );
        assert_eq!(
            parse_delay(Some("delay=60ms")).unwrap().unwrap(),
            Duration::from_millis(60),
        );
        assert_eq!(
            parse_delay(Some("delay=60000ms")).unwrap().unwrap(),
            Duration::from_millis(60000),
        );
    }
}
