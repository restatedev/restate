// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use bytestring::ByteString;
use http::{header, HeaderMap, HeaderName, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use metrics::{counter, histogram};
use serde::de::IntoDeserializer;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use tracing::{info, trace, warn, Instrument};

use restate_ingress_dispatcher::{DispatchIngressRequest, IngressDispatcherRequest};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{
    Header, InvocationTarget, InvocationTargetType, ServiceInvocation, Source, SpanRelation,
    WorkflowHandlerType,
};
use restate_types::schema::invocation_target::{
    InvocationTargetMetadata, InvocationTargetResolver,
};

use super::path_parsing::{InvokeType, ServiceRequestType, TargetType};
use super::tracing::prepare_tracing_span;
use super::HandlerError;
use super::{Handler, APPLICATION_JSON};
use crate::handler::responses::{IDEMPOTENCY_EXPIRES, X_RESTATE_ID};
use crate::metric_definitions::{INGRESS_REQUESTS, INGRESS_REQUEST_DURATION, REQUEST_COMPLETED};

pub(crate) const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
const DELAY_QUERY_PARAM: &str = "delay";

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
pub(crate) enum SendStatus {
    Accepted,
    PreviouslyAccepted,
}

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

impl<Schemas, Dispatcher, StorageReader> Handler<Schemas, Dispatcher, StorageReader>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: DispatchIngressRequest + Clone + Send + Sync + 'static,
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
            return Err(HandlerError::NotFound);
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
                    panic!("Unexpected keyed target, this should have been checked before in the path parsing.")
                }
            }
        } else {
            InvocationTarget::service(&*service_name, &*handler_name)
        };
        let invocation_id = if let Some(ref idempotency_key) = idempotency_key {
            // We need this to make sure the internal services will deliver correctly this idempotent invocation always
            //  to the same partition. This piece of logic could be improved and moved into ingress-dispatcher with
            //  https://github.com/restatedev/restate/issues/1329
            InvocationId::generate_with_idempotency_key(&invocation_target, idempotency_key)
        } else {
            InvocationId::generate(&invocation_target)
        };

        // Prepare the tracing span
        let (ingress_span, ingress_span_context) =
            prepare_tracing_span(&invocation_id, &invocation_target, &req);

        let result = async move {
            info!("Processing ingress request");

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

            // Get headers
            let headers = parse_headers(parts.headers)?;

            // Parse delay query parameter
            let delay = parse_delay(parts.uri.query())?;

            // Prepare service invocation
            let mut service_invocation =
                ServiceInvocation::initialize(invocation_id, invocation_target, Source::Ingress);
            service_invocation.with_related_span(SpanRelation::Parent(ingress_span_context));
            service_invocation.completion_retention_time =
                invocation_target_meta.compute_retention(idempotency_key.is_some());
            if let Some(key) = idempotency_key {
                service_invocation.idempotency_key = Some(key);
            }
            service_invocation.headers = headers;
            service_invocation.argument = body;

            match invoke_ty {
                InvokeType::Call => {
                    if delay.is_some() {
                        return Err(HandlerError::UnsupportedDelay);
                    }
                    Self::handle_service_call(
                        service_invocation,
                        invocation_target_meta,
                        self.dispatcher,
                    )
                    .await
                }
                InvokeType::Send => {
                    service_invocation.execution_time =
                        delay.map(|d| SystemTime::now() + d).map(Into::into);

                    Self::handle_service_send(service_invocation, self.dispatcher).await
                }
            }
        }
        .instrument(ingress_span)
        .await;

        // Note that we only record (mostly) successful requests here. We might want to
        // change this in the _near_ future.
        histogram!(
            INGRESS_REQUEST_DURATION,
            "rpc.service" => service_name.clone(),
            "rpc.method" => handler_name.clone(),
        )
        .record(start_time.elapsed());

        counter!(
            INGRESS_REQUESTS,
            "status" => REQUEST_COMPLETED,
            "rpc.service" => service_name,
            "rpc.method" => handler_name,
        )
        .increment(1);
        result
    }

    async fn handle_service_call(
        service_invocation: ServiceInvocation,
        invocation_target_metadata: InvocationTargetMetadata,
        dispatcher: Dispatcher,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let invocation_id = service_invocation.invocation_id;
        let (invocation, ingress_correlation_id, response_rx) =
            IngressDispatcherRequest::invocation(service_invocation);

        if let Err(e) = dispatcher.dispatch_ingress_request(invocation).await {
            warn!(
                restate.invocation.id = %invocation_id,
                "Failed to dispatch ingress request: {}",
                e,
            );
            return Err(HandlerError::Unavailable);
        }

        // Wait on response
        let response = if let Ok(response) = response_rx.await {
            response
        } else {
            dispatcher.evict_pending_response(ingress_correlation_id);
            warn!("Response channel was closed");
            return Err(HandlerError::Unavailable);
        };

        Self::reply_with_invocation_response(
            response.result,
            Some(response.invocation_id.unwrap_or(invocation_id)),
            response.idempotency_expiry_time.as_deref(),
            move |_| Ok(invocation_target_metadata),
        )
    }

    async fn handle_service_send(
        service_invocation: ServiceInvocation,
        dispatcher: Dispatcher,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let invocation_id = service_invocation.invocation_id;
        let execution_time = service_invocation.execution_time;

        // Send the service invocation
        let (req, req_id, submit_notification_rx) =
            IngressDispatcherRequest::one_way_invocation(service_invocation);

        if let Err(e) = dispatcher.dispatch_ingress_request(req).await {
            warn!(
                restate.invocation.id = %invocation_id,
                "Failed to dispatch ingress request: {}",
                e,
            );
            return Err(HandlerError::Unavailable);
        }

        // Wait submit notification
        let submit_notification = if let Ok(response) = submit_notification_rx.await {
            response
        } else {
            dispatcher.evict_pending_submit_notification(req_id);
            warn!("Response channel was closed");
            return Err(HandlerError::Unavailable);
        };
        let submit_reattached_to_existing_invocation =
            submit_notification.invocation_id != invocation_id;

        trace!("Complete external HTTP send request successfully");
        Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .header(header::CONTENT_TYPE, APPLICATION_JSON)
            .header(X_RESTATE_ID, submit_notification.invocation_id.to_string())
            .body(Full::new(
                serde_json::to_vec(&SendResponse {
                    invocation_id: submit_notification.invocation_id,
                    execution_time: execution_time.map(SystemTime::from).map(Into::into),
                    status: if submit_reattached_to_existing_invocation {
                        SendStatus::PreviouslyAccepted
                    } else {
                        SendStatus::Accepted
                    },
                })
                .unwrap()
                .into(),
            ))
            .unwrap())
    }
}

fn parse_headers(headers: HeaderMap) -> Result<Vec<Header>, HandlerError> {
    headers
        .into_iter()
        .filter_map(|(k, v)| k.map(|k| (k, v)))
        // Filter out Connection, Host and idempotency headers
        .filter(|(k, _)| {
            k != header::CONNECTION
                && k != header::HOST
                && k != IDEMPOTENCY_KEY
                && k != IDEMPOTENCY_EXPIRES
        })
        .map(|(k, v)| {
            let value = v
                .to_str()
                .map_err(|e| HandlerError::BadHeader(k.clone(), e))?;
            Ok(Header::new(k.as_str(), value))
        })
        .collect()
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
