// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{Handler, APPLICATION_JSON};
use super::HandlerError;
use super::path_parsing::{ComponentRequestType, InvokeType, TargetType};
use super::tracing::prepare_tracing_span;

use crate::metric_definitions::{INGRESS_REQUESTS, INGRESS_REQUEST_DURATION, REQUEST_COMPLETED};
use bytes::Bytes;
use http::{header, HeaderMap, HeaderName, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use metrics::{counter, histogram};
use restate_ingress_dispatcher::{IdempotencyMode, IngressRequest, IngressRequestSender};
use restate_schema_api::component::ComponentMetadataResolver;
use restate_types::identifiers::{FullInvocationId, InvocationId, ServiceId};
use restate_types::invocation::SpanRelation;
use serde::Serialize;
use std::num::ParseIntError;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace, warn, Instrument};

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

impl<Schemas> Handler<Schemas>
where
    Schemas: ComponentMetadataResolver + Clone + Send + Sync + 'static,
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

        if let Some(basic_component_metadata) = self
            .schemas
            .resolve_latest_component_handler(&component_name, &handler_name)
        {
            if !basic_component_metadata.public {
                return Err(HandlerError::PrivateComponent);
            }
        } else {
            return Err(HandlerError::NotFound);
        }

        // Craft FullInvocationId
        let fid = if let TargetType::VirtualObject { key } = target {
            FullInvocationId::generate(ServiceId::new(component_name.clone(), key))
        } else {
            FullInvocationId::generate(ServiceId::unkeyed(
                component_name.clone(),
            ))
        };

        // Prepare the tracing span
        let (ingress_span, ingress_span_context) = prepare_tracing_span(&fid, &handler_name, &req);

        let cloned_handler_name = handler_name.clone();
        let handle_fut = async move {
            info!("Processing ingress request");

            // Check HTTP Method
            if req.method() != Method::GET && req.method() != Method::POST {
                return Err(HandlerError::MethodNotAllowed);
            }

            // TODO validate content-type
            //  https://github.com/restatedev/restate/issues/1230

            // Check if Idempotency-Key is available
            let idempotency_mode = parse_idempotency_key_and_retention_period(req.headers())?;

            // Collect body
            let collected_request_bytes = req
                .into_body()
                .collect()
                .await
                .map_err(|e| HandlerError::Body(e.into()))?
                .to_bytes();
            trace!(rpc.request = ?collected_request_bytes);

            let span_relation = SpanRelation::Parent(ingress_span_context);

            match invoke_ty {
                InvokeType::Call => {
                    Self::handle_component_call(
                        fid,
                        cloned_handler_name,
                        idempotency_mode,
                        collected_request_bytes,
                        span_relation,
                        self.request_tx,
                    )
                    .await
                }
                InvokeType::Send => {
                    Self::handle_component_send(
                        fid,
                        cloned_handler_name,
                        idempotency_mode,
                        collected_request_bytes,
                        span_relation,
                        self.request_tx,
                    )
                    .await
                }
            }
        }
        .instrument(ingress_span);

        async move {
            let result = handle_fut.await;
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
        .await
    }

    async fn handle_component_call(
        fid: FullInvocationId,
        cloned_handler_name: String,
        idempotency_mode: IdempotencyMode,
        collected_request_bytes: Bytes,
        span_relation: SpanRelation,
        request_tx: IngressRequestSender,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let (invocation, response_rx) = IngressRequest::invocation(
            fid,
            cloned_handler_name,
            collected_request_bytes,
            span_relation,
            idempotency_mode,
        );
        if request_tx.send(invocation).is_err() {
            debug!("Ingress dispatcher is closed while there is still an invocation in flight.");
            return Err(HandlerError::Unavailable);
        }

        // Wait on response
        let response = if let Ok(response) = response_rx.await {
            response
        } else {
            warn!("Response channel was closed");
            return Err(HandlerError::Unavailable);
        };

        // Prepare response metadata
        let mut response_builder = hyper::Response::builder();

        // Add idempotency expiry time if available
        if let Some(expire_time) = response.idempotency_expire_time() {
            response_builder = response_builder.header(IDEMPOTENCY_EXPIRES, expire_time);
        }

        match response.into() {
            Ok(response_payload) => {
                trace!(rpc.response = ?response_payload, "Complete external HTTP request successfully");
                // TODO this is a temporary solution until we have some format awareness.
                //  https://github.com/restatedev/restate/issues/1230
                response_builder = response_builder.header(header::CONTENT_TYPE, APPLICATION_JSON);

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
        handler_name: String,
        idempotency_mode: IdempotencyMode,
        collected_request_bytes: Bytes,
        span_relation: SpanRelation,
        request_tx: IngressRequestSender,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        if !matches!(idempotency_mode, IdempotencyMode::None) {
            // TODO https://github.com/restatedev/restate/issues/1233
            return Err(HandlerError::SendAndIdempotencyKey);
        }

        let invocation_id = InvocationId::from(&fid);

        // Send the service invocation and wait on ack
        let (invocation, ack_rx) = IngressRequest::background_invocation(
            fid,
            handler_name,
            collected_request_bytes,
            span_relation,
            None,
        );
        if request_tx.send(invocation).is_err() {
            debug!("Ingress dispatcher is closed while there is still an invocation in flight.");
            return Err(HandlerError::Unavailable);
        }
        if ack_rx.await.is_err() {
            warn!("Response channel was closed");
            return Err(HandlerError::Unavailable);
        };

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
