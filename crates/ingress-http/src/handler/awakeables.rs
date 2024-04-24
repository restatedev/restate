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
use super::Handler;
use super::HandlerError;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use http_body_util::Full;
use restate_ingress_dispatcher::DispatchIngressRequest;
use restate_ingress_dispatcher::IngressDispatcherRequest;
use restate_schema_api::service::ServiceMetadataResolver;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_types::errors::{codes, InvocationError};
use restate_types::invocation::{InvocationResponse, ResponseResult};
use std::str::FromStr;
use tracing::{info, trace, warn};

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + Clone + Send + Sync + 'static,
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
        // Check HTTP Method
        if req.method() != Method::POST {
            return Err(HandlerError::MethodNotAllowed);
        }

        // Collect body
        let collected_request_bytes = req
            .into_body()
            .collect()
            .await
            .map_err(|e| HandlerError::Body(e.into()))?
            .to_bytes();
        trace!(rpc.request = ?collected_request_bytes);

        // Wrap payload in request object for awakeables built in service
        let (awakeable_identifier, result) = match awakeable_request_type {
            AwakeableRequestType::Resolve { awakeable_id } => (
                AwakeableIdentifier::from_str(&awakeable_id)
                    .map_err(|e| HandlerError::BadAwakeableId(awakeable_id, e))?,
                ResponseResult::from(Ok(collected_request_bytes)),
            ),
            AwakeableRequestType::Reject { awakeable_id } => (
                AwakeableIdentifier::from_str(&awakeable_id)
                    .map_err(|e| HandlerError::BadAwakeableId(awakeable_id, e))?,
                ResponseResult::from(Err(InvocationError::new(
                    codes::UNKNOWN,
                    String::from_utf8_lossy(&collected_request_bytes).to_string(),
                ))),
            ),
        };

        let (invocation_id, entry_index) = awakeable_identifier.into_inner();

        info!(
            restate.invocation.id = %invocation_id,
            restate.journal.index = entry_index,
            "Processing awakeables request"
        );

        let req = IngressDispatcherRequest::completion(InvocationResponse {
            id: invocation_id,
            entry_index,
            result,
        });
        if let Err(e) = self.dispatcher.dispatch_ingress_request(req).await {
            warn!(
                restate.invocation.id = %invocation_id,
                "Failed to dispatch awakeable completion: {}",
                e,
            );
            return Err(HandlerError::Unavailable);
        }

        Ok(hyper::Response::builder()
            .status(StatusCode::ACCEPTED)
            .body(Full::default())
            .unwrap())
    }
}
