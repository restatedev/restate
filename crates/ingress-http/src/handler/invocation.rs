// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::path_parsing::InvocationRequestType;
use super::Handler;
use super::HandlerError;

use crate::{GetOutputResult, InvocationStorageReader};
use bytes::Bytes;
use http::{Method, Request, Response};
use http_body_util::Full;
use restate_ingress_dispatcher::DispatchIngressRequest;
use restate_ingress_dispatcher::IngressDispatcherRequest;
use restate_schema_api::invocation_target::InvocationTargetResolver;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationQuery;
use tracing::{info, warn};

impl<Schemas, Dispatcher, StorageReader> Handler<Schemas, Dispatcher, StorageReader>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: DispatchIngressRequest + Clone + Send + Sync + 'static,
    StorageReader: InvocationStorageReader + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_invocation<B: http_body::Body>(
        self,
        req: Request<B>,
        invocation_request_type: InvocationRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        match invocation_request_type {
            InvocationRequestType::Attach(id) => {
                let invocation_id = id
                    .parse()
                    .map_err(|e| HandlerError::BadInvocationId(id, e))?;
                self.handle_invocation_attach(req, invocation_id).await
            }
            InvocationRequestType::GetOutput(id) => {
                let invocation_id = id
                    .parse()
                    .map_err(|e| HandlerError::BadInvocationId(id, e))?;
                self.handle_invocation_get_output(req, invocation_id).await
            }
        }
    }

    pub(crate) async fn handle_invocation_attach<B: http_body::Body>(
        self,
        req: Request<B>,
        invocation_id: InvocationId,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        // Check HTTP Method
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }

        let (dispatcher_req, correlation_id, response_rx) =
            IngressDispatcherRequest::attach(InvocationQuery::Invocation(invocation_id));

        info!(
            restate.invocation.id = %invocation_id,
            "Processing invocation attach request"
        );

        if let Err(e) = self
            .dispatcher
            .dispatch_ingress_request(dispatcher_req)
            .await
        {
            warn!(
                restate.invocation.id = %invocation_id,
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

    pub(crate) async fn handle_invocation_get_output<B: http_body::Body>(
        self,
        req: Request<B>,
        invocation_id: InvocationId,
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
            .get_output(InvocationQuery::Invocation(invocation_id))
            .await
        {
            Ok(GetOutputResult::Ready(out)) => out,
            Ok(GetOutputResult::NotFound) => return Err(HandlerError::NotFound),
            Ok(GetOutputResult::NotReady) => return Err(HandlerError::NotReady),
            Err(e) => {
                warn!(
                    restate.invocation.id = %invocation_id,
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
