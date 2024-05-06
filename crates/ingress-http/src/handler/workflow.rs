// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::path_parsing::WorkflowRequestType;
use super::Handler;
use super::HandlerError;

use crate::{GetOutputResult, InvocationStorageReader};
use bytes::Bytes;
use http::{header, Method, Request, Response};
use http_body_util::Full;
use restate_ingress_dispatcher::DispatchIngressRequest;
use restate_ingress_dispatcher::IngressDispatcherRequest;
use restate_schema_api::invocation_target::InvocationTargetResolver;
use restate_types::identifiers::ServiceId;
use restate_types::ingress::IngressResponseResult;
use restate_types::invocation::InvocationQuery;
use tracing::{info, trace, warn};

impl<Schemas, Dispatcher, StorageReader> Handler<Schemas, Dispatcher, StorageReader>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: DispatchIngressRequest + Clone + Send + Sync + 'static,
    StorageReader: InvocationStorageReader + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_workflow<B: http_body::Body>(
        self,
        req: Request<B>,
        workflow_request_type: WorkflowRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        match workflow_request_type {
            WorkflowRequestType::Attach(name, key) => {
                self.handle_workflow_attach(req, ServiceId::new(name, key))
                    .await
            }
            WorkflowRequestType::GetOutput(name, key) => {
                self.handle_workflow_get_output(req, ServiceId::new(name, key))
                    .await
            }
        }
    }

    pub(crate) async fn handle_workflow_attach<B: http_body::Body>(
        self,
        req: Request<B>,
        workflow_id: ServiceId,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        // Check HTTP Method
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }

        let (dispatcher_req, correlation_id, response_rx) =
            IngressDispatcherRequest::attach(InvocationQuery::Workflow(workflow_id.clone()));

        info!(
            restate.workflow.id = %workflow_id,
            "Processing workflow attach request"
        );

        if let Err(e) = self
            .dispatcher
            .dispatch_ingress_request(dispatcher_req)
            .await
        {
            warn!(
                restate.workflow.id = %workflow_id,
                "Failed to dispatch: {}",
                e,
            );
            return Err(HandlerError::Unavailable);
        }

        // Wait on response
        let response = if let Ok(response) = response_rx.await {
            response
        } else {
            self.dispatcher.evict_pending_response(correlation_id);
            warn!("Response channel was closed");
            return Err(HandlerError::Unavailable);
        };

        // Prepare response metadata
        let mut response_builder = hyper::Response::builder();

        // Add idempotency expiry time if available
        // TODO reintroduce this once available
        // if let Some(expiry_time) = response.idempotency_expiry_time() {
        //     response_builder = response_builder.header(IDEMPOTENCY_EXPIRES, expiry_time);
        // }

        match response.result {
            IngressResponseResult::Success(invocation_target, response_payload) => {
                trace!(rpc.response = ?response_payload, "Complete external HTTP request successfully");

                // Resolve invocation target metadata.
                // We need it for the output content type.
                let invocation_target_metadata = self
                    .schemas
                    .resolve_latest_invocation_target(
                        invocation_target.service_name(),
                        invocation_target.handler_name(),
                    )
                    .ok_or(HandlerError::NotFound)?;

                // Write out the content-type, if any
                // TODO fix https://github.com/restatedev/restate/issues/1496
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
            IngressResponseResult::Failure(error) => {
                info!(rpc.response = ?error, "Complete external HTTP request with a failure");
                Ok(HandlerError::Invocation(error).fill_builder(response_builder))
            }
        }
    }

    pub(crate) async fn handle_workflow_get_output<B: http_body::Body>(
        self,
        req: Request<B>,
        workflow_id: ServiceId,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        // Check HTTP Method
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }

        let response = match self
            .storage_reader
            .get_output(InvocationQuery::Workflow(workflow_id.clone()))
            .await
        {
            Ok(GetOutputResult::Ready(out)) => out,
            Ok(GetOutputResult::NotFound) => return Err(HandlerError::NotFound),
            Ok(GetOutputResult::NotReady) => return Err(HandlerError::NotReady),
            Err(e) => {
                warn!(
                    restate.workflow.id = %workflow_id,
                    "Failed to read output: {}",
                    e,
                );
                return Err(HandlerError::Unavailable);
            }
        };

        // Prepare response metadata
        let mut response_builder = hyper::Response::builder();

        // Add idempotency expiry time if available
        // TODO reintroduce this once available
        // if let Some(expiry_time) = response.idempotency_expiry_time() {
        //     response_builder = response_builder.header(IDEMPOTENCY_EXPIRES, expiry_time);
        // }

        match response.response {
            IngressResponseResult::Success(invocation_target, response_payload) => {
                trace!(rpc.response = ?response_payload, "Complete external HTTP request successfully");

                // Resolve invocation target metadata.
                // We need it for the output content type.
                let invocation_target_metadata = self
                    .schemas
                    .resolve_latest_invocation_target(
                        invocation_target.service_name(),
                        invocation_target.handler_name(),
                    )
                    .ok_or(HandlerError::NotFound)?;

                // Write out the content-type, if any
                // TODO fix https://github.com/restatedev/restate/issues/1496
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
            IngressResponseResult::Failure(error) => {
                info!(rpc.response = ?error, "Complete external HTTP request with a failure");
                Ok(HandlerError::Invocation(error).fill_builder(response_builder))
            }
        }
    }
}
