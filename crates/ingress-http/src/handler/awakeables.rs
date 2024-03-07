// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::path_parsing::AwakeableRequestType;
use super::tracing::prepare_tracing_span;
use super::Handler;
use super::HandlerError;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use http_body_util::Full;
use prost::Message;
use restate_ingress_dispatcher::DispatchIngressRequest;
use restate_ingress_dispatcher::{IdempotencyMode, IngressRequest};
use restate_schema_api::component::ComponentMetadataResolver;
use restate_types::identifiers::InvocationId;
use restate_types::identifiers::{FullInvocationId, ServiceId};
use restate_types::invocation::SpanRelation;
use tracing::{info, trace, warn, Instrument};

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: ComponentMetadataResolver + Clone + Send + Sync + 'static,
    Dispatcher: DispatchIngressRequest + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_awakeable<B: http_body::Body>(
        self,
        req: Request<B>,
        awakeable_request_type: AwakeableRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        let fid =
            FullInvocationId::generate(ServiceId::unkeyed(restate_pb::AWAKEABLES_SERVICE_NAME));

        let handler_name: &'static str = match &awakeable_request_type {
            AwakeableRequestType::Resolve { .. } => restate_pb::AWAKEABLES_RESOLVE_HANDLER_NAME,
            AwakeableRequestType::Reject { .. } => restate_pb::AWAKEABLES_REJECT_HANDLER_NAME,
        };

        // Prepare the tracing span
        let (ingress_span, ingress_span_context) = prepare_tracing_span(&fid, handler_name, &req);

        let dispatcher = self.dispatcher.clone();
        async move {
            info!("Processing awakeables request");

            // Check HTTP Method
            if req.method() != Method::POST {
                return Err(HandlerError::MethodNotAllowed);
            }

            // TODO validate content-type
            //  https://github.com/restatedev/restate/issues/1230

            // Collect body
            let collected_request_bytes = req
                .into_body()
                .collect()
                .await
                .map_err(|e| HandlerError::Body(e.into()))?
                .to_bytes();
            trace!(rpc.request = ?collected_request_bytes);

            // Wrap payload in request object for awakeables built in service
            let payload = match awakeable_request_type {
                AwakeableRequestType::Resolve { awakeable_id } => {
                    restate_pb::restate::ResolveAwakeableRequest {
                        id: awakeable_id,
                        result: collected_request_bytes,
                    }
                    .encode_to_vec()
                }
                AwakeableRequestType::Reject { awakeable_id } => {
                    restate_pb::restate::RejectAwakeableRequest {
                        id: awakeable_id,
                        reason: String::from_utf8_lossy(&collected_request_bytes).to_string(),
                    }
                    .encode_to_vec()
                }
            };

            let invocation_id: InvocationId = fid.clone().into();
            let (invocation, response_rx) = IngressRequest::invocation(
                fid,
                handler_name,
                payload,
                SpanRelation::Linked(ingress_span_context),
                IdempotencyMode::None,
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

            match response.into() {
                Ok(_) => Ok(hyper::Response::builder()
                    .status(StatusCode::ACCEPTED)
                    .body(Full::default())
                    .unwrap()),
                Err(error) => Ok(HandlerError::Invocation(error).into_response()),
            }
        }
        .instrument(ingress_span)
        .await
    }
}
