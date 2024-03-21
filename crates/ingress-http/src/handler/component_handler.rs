// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::path_parsing::{ComponentRequestType, InvokeType, TargetType};
use super::tracing::prepare_tracing_span;
use super::HandlerError;
use super::{Handler, APPLICATION_JSON};

use crate::metric_definitions::{INGRESS_REQUESTS, INGRESS_REQUEST_DURATION, REQUEST_COMPLETED};
use bytes::Bytes;
use http::{header, HeaderMap, HeaderName, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use metrics::{counter, histogram};
use restate_ingress_dispatcher::{DispatchIngressRequest, IdempotencyMode, IngressRequest};
use restate_schema_api::invocation_target::{InvocationTargetMetadata, InvocationTargetResolver};
use restate_types::identifiers::{FullInvocationId, InvocationId, ServiceId};
use restate_types::invocation::{Header, SpanRelation};
use serde::Serialize;
use std::num::ParseIntError;
use std::time::{Duration, Instant};
use tracing::{info, trace, warn, Instrument};

pub(crate) const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
const IDEMPOTENCY_RETENTION_PERIOD: HeaderName =
    HeaderName::from_static("idempotency-retention-period");
const IDEMPOTENCY_EXPIRES: HeaderName = HeaderName::from_static("idempotency-expires");

#[derive(Debug, Serialize)]
#[cfg_attr(test, derive(serde::Deserialize))]
#[serde(rename_all = "camelCase")]
pub(crate) struct SendResponse {
    invocation_id: InvocationId,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: DispatchIngressRequest + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_component_request<B: http_body::Body>(
        self,
        req: Request<B>,
        component_request: ComponentRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        let start_time = Instant::now();

        let ComponentRequestType {
            name: component_name,
            handler: handler_name,
            target,
            invoke_ty,
        } = component_request;

        let invocation_target_meta = if let Some(invocation_target) = self
            .schemas
            .resolve_latest_invocation_target(&component_name, &handler_name)
        {
            if !invocation_target.public {
                return Err(HandlerError::PrivateComponent);
            }
            invocation_target
        } else {
            return Err(HandlerError::NotFound);
        };

        // Craft FullInvocationId
        let fid = if let TargetType::VirtualObject { key } = target {
            FullInvocationId::generate(ServiceId::new(component_name.clone(), key))
        } else {
            FullInvocationId::generate(ServiceId::unkeyed(component_name.clone()))
        };

        // Prepare the tracing span
        let (ingress_span, ingress_span_context) = prepare_tracing_span(&fid, &handler_name, &req);

        let cloned_handler_name = handler_name.clone();
        let result = async move {
            info!("Processing ingress request");

            let (parts, body) = req.into_parts();

            // Check HTTP Method
            if parts.method != Method::GET && parts.method != Method::POST {
                return Err(HandlerError::MethodNotAllowed);
            }

            // Check if Idempotency-Key is available
            let idempotency_mode = parse_idempotency_key_and_retention_period(&parts.headers)?;

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

            let span_relation = SpanRelation::Parent(ingress_span_context);
            match invoke_ty {
                InvokeType::Call => {
                    Self::handle_component_call(
                        fid,
                        cloned_handler_name,
                        idempotency_mode,
                        body,
                        span_relation,
                        headers,
                        invocation_target_meta,
                        self.dispatcher,
                    )
                    .await
                }
                InvokeType::Send => {
                    Self::handle_component_send(
                        fid,
                        cloned_handler_name,
                        idempotency_mode,
                        body,
                        span_relation,
                        headers,
                        self.dispatcher,
                    )
                    .await
                }
            }
        }
        .instrument(ingress_span)
        .await;

        // Note that we only record (mostly) successful requests here. We might want to
        // change this in the _near_ future.
        histogram!(
            INGRESS_REQUEST_DURATION,
            "rpc.service" => component_name.clone(),
            "rpc.method" => handler_name.clone(),
        )
        .record(start_time.elapsed());

        counter!(
            INGRESS_REQUESTS,
            "status" => REQUEST_COMPLETED,
            "rpc.service" => component_name,
            "rpc.method" => handler_name,
        )
        .increment(1);
        result
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_component_call(
        fid: FullInvocationId,
        handler: String,
        idempotency_mode: IdempotencyMode,
        body: Bytes,
        span_relation: SpanRelation,
        headers: Vec<Header>,
        invocation_target_metadata: InvocationTargetMetadata,
        dispatcher: Dispatcher,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let invocation_id: InvocationId = fid.clone().into();
        let (invocation, response_rx) = IngressRequest::invocation(
            fid,
            handler,
            body,
            span_relation,
            idempotency_mode,
            headers,
        );
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
            dispatcher.evict_pending_response(&invocation_id);
            warn!("Response channel was closed");
            return Err(HandlerError::Unavailable);
        };

        // Prepare response metadata
        let mut response_builder = hyper::Response::builder();

        // Add idempotency expiry time if available
        if let Some(expiry_time) = response.idempotency_expiry_time() {
            response_builder = response_builder.header(IDEMPOTENCY_EXPIRES, expiry_time);
        }

        match response.into() {
            Ok(response_payload) => {
                trace!(rpc.response = ?response_payload, "Complete external HTTP request successfully");

                // Write out the content-type, if any
                if let Some(ct) = invocation_target_metadata
                    .output_rules
                    .infer_content_type(response_payload.is_empty())
                {
                    response_builder = response_builder.header(
                        header::CONTENT_TYPE,
                        // TODO we need this to_str().unwrap() because these two HeaderValue come from two different http crates
                        //  We can remove it once https://github.com/restatedev/restate/issues/96 is done
                        ct.to_str().unwrap(),
                    )
                }

                Ok(response_builder.body(Full::new(response_payload)).unwrap())
            }
            Err(error) => {
                info!(rpc.response = ?error, "Complete external HTTP request with a failure");
                Ok(HandlerError::Invocation(error).fill_builder(response_builder))
            }
        }
    }

    async fn handle_component_send(
        fid: FullInvocationId,
        handler: String,
        idempotency_mode: IdempotencyMode,
        body: Bytes,
        span_relation: SpanRelation,
        headers: Vec<Header>,
        dispatcher: Dispatcher,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        if !matches!(idempotency_mode, IdempotencyMode::None) {
            // TODO https://github.com/restatedev/restate/issues/1233
            return Err(HandlerError::SendAndIdempotencyKey);
        }

        let invocation_id = InvocationId::from(&fid);

        // Send the service invocation
        let invocation =
            IngressRequest::background_invocation(fid, handler, body, span_relation, None, headers);
        if let Err(e) = dispatcher.dispatch_ingress_request(invocation).await {
            warn!(
                restate.invocation.id = %invocation_id,
                "Failed to dispatch ingress request: {}",
                e,
            );
            return Err(HandlerError::Unavailable);
        }

        trace!("Complete external HTTP send request successfully");
        Ok(Response::builder()
            .status(StatusCode::ACCEPTED)
            .header(header::CONTENT_TYPE, APPLICATION_JSON)
            .body(Full::new(
                serde_json::to_vec(&SendResponse { invocation_id })
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
                && k != IDEMPOTENCY_RETENTION_PERIOD
        })
        .map(|(k, v)| {
            let value = v
                .to_str()
                .map_err(|e| HandlerError::BadHeader(k.clone(), e))?;
            Ok(Header::new(k.as_str(), value))
        })
        .collect()
}

fn parse_idempotency_key_and_retention_period(
    headers: &HeaderMap,
) -> Result<IdempotencyMode, HandlerError> {
    let idempotency_key = if let Some(idempotency_key) = headers.get(IDEMPOTENCY_KEY) {
        Bytes::copy_from_slice(idempotency_key.as_bytes())
    } else {
        return Ok(IdempotencyMode::None);
    };

    if let Some(retention_period_sec) = headers.get(IDEMPOTENCY_RETENTION_PERIOD) {
        let retention_period = Duration::from_secs(
            retention_period_sec
                .to_str()
                .map_err(|e| HandlerError::BadIdempotency(e.into()))?
                .parse()
                .map_err(|e: ParseIntError| HandlerError::BadIdempotency(e.into()))?,
        );
        Ok(IdempotencyMode::key(
            idempotency_key,
            Some(retention_period),
        ))
    } else {
        Ok(IdempotencyMode::key(idempotency_key, None))
    }
}
