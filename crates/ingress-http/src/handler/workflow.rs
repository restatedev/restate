// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use http::{Method, Request, Response};
use http_body_util::Full;
use tracing::{trace, warn};

use restate_types::errors::GenericError;
use restate_types::identifiers::ServiceId;
use restate_types::invocation::InvocationQuery;
use restate_types::invocation::client::{AttachInvocationResponse, GetInvocationOutputResponse};
use restate_types::schema::invocation_target::InvocationTargetResolver;

use super::Handler;
use super::HandlerError;
use super::path_parsing::WorkflowRequestType;
use crate::RequestDispatcher;

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_workflow<B: http_body::Body>(
        self,
        req: Request<B>,
        workflow_request_type: WorkflowRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        match workflow_request_type {
            WorkflowRequestType::Attach(name, key) => {
                self.handle_workflow_attach(req, ServiceId::new(None, name.as_str(), key.as_str()))
                    .await
            }
            WorkflowRequestType::GetOutput(name, key) => {
                self.handle_workflow_get_output(
                    req,
                    ServiceId::new(None, name.as_str(), key.as_str()),
                )
                .await
            }
            WorkflowRequestType::Status(name, key) => {
                self.handle_invocation_get_status(
                    req,
                    InvocationQuery::Workflow(ServiceId::new(None, name.as_str(), key.as_str()))
                        .to_invocation_id(),
                )
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
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        // Check HTTP Method
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }

        trace!(
            restate.workflow.id = %workflow_id,
            "Processing workflow attach request"
        );

        // Wait on response
        let response = match self
            .dispatcher
            .attach_invocation(InvocationQuery::Workflow(workflow_id.clone()))
            .await
            .map_err(HandlerError::GenericReadDispatcherError)?
        {
            AttachInvocationResponse::NotFound => {
                return Err(HandlerError::InvocationNotFound);
            }
            AttachInvocationResponse::NotSupported => {
                return Err(HandlerError::UnsupportedGetOutput);
            }
            AttachInvocationResponse::Ready(response) => response,
        };

        Self::reply_with_invocation_response(response, move |invocation_target| {
            self.schemas
                .pinned()
                .resolve_latest_invocation_target(
                    invocation_target.service_name(),
                    invocation_target.handler_name(),
                )
                .ok_or(HandlerError::NotFound)
        })
    }

    pub(crate) async fn handle_workflow_get_output<B: http_body::Body>(
        self,
        req: Request<B>,
        workflow_id: ServiceId,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        // Check HTTP Method
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }

        let response = match self
            .dispatcher
            .get_invocation_output(InvocationQuery::Workflow(workflow_id.clone()))
            .await
        {
            Ok(GetInvocationOutputResponse::Ready(out)) => out,
            Ok(GetInvocationOutputResponse::NotFound) => {
                return Err(HandlerError::InvocationNotFound);
            }
            Ok(GetInvocationOutputResponse::NotReady) => return Err(HandlerError::NotReady),
            Ok(GetInvocationOutputResponse::NotSupported) => {
                return Err(HandlerError::UnsupportedGetOutput);
            }
            Err(e) => {
                warn!(
                    restate.workflow.id = %workflow_id,
                    "Failed to read output: {}",
                    e,
                );
                return Err(HandlerError::GenericReadDispatcherError(e));
            }
        };

        Self::reply_with_invocation_response(response, move |invocation_target| {
            self.schemas
                .pinned()
                .resolve_latest_invocation_target(
                    invocation_target.service_name(),
                    invocation_target.handler_name(),
                )
                .ok_or(HandlerError::NotFound)
        })
    }
}
