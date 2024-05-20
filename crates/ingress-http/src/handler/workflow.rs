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
use http::{Method, Request, Response};
use http_body_util::Full;
use restate_ingress_dispatcher::DispatchIngressRequest;
use restate_ingress_dispatcher::IngressDispatcherRequest;
use restate_schema_api::invocation_target::InvocationTargetResolver;
use restate_types::identifiers::ServiceId;
use restate_types::invocation::InvocationQuery;
use tracing::{info, warn};

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

        Self::reply_with_invocation_response(
            response.result,
            response.idempotency_expiry_time.as_deref(),
            move |invocation_target| {
                self.schemas
                    .resolve_latest_invocation_target(
                        invocation_target.service_name(),
                        invocation_target.handler_name(),
                    )
                    .ok_or(HandlerError::NotFound)
            },
        )
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

        Self::reply_with_invocation_response(response.response, None, move |invocation_target| {
            self.schemas
                .resolve_latest_invocation_target(
                    invocation_target.service_name(),
                    invocation_target.handler_name(),
                )
                .ok_or(HandlerError::NotFound)
        })
    }
}
