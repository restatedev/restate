// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::path_parsing::{InvocationRequestType, InvocationTargetType, TargetType};
use super::Handler;
use super::HandlerError;

use crate::{GetOutputResult, InvocationStorageReader};
use bytes::Bytes;
use http::{Method, Request, Response};
use http_body_util::Full;
use restate_ingress_dispatcher::DispatchIngressRequest;
use restate_ingress_dispatcher::IngressDispatcherRequest;
use restate_schema_api::invocation_target::InvocationTargetResolver;
use restate_types::identifiers::IdempotencyId;
use restate_types::invocation::InvocationQuery;
use tracing::warn;

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
            InvocationRequestType::Attach(invocation_target_type) => {
                self.handle_invocation_attach(
                    req,
                    Self::convert_to_invocation_query(invocation_target_type)?,
                )
                .await
            }
            InvocationRequestType::GetOutput(invocation_target_type) => {
                self.handle_invocation_get_output(
                    req,
                    Self::convert_to_invocation_query(invocation_target_type)?,
                )
                .await
            }
        }
    }

    pub(crate) fn convert_to_invocation_query(
        invocation_target_type: InvocationTargetType,
    ) -> Result<InvocationQuery, HandlerError> {
        match invocation_target_type {
            InvocationTargetType::InvocationId(id) => id
                .parse()
                .map(InvocationQuery::Invocation)
                .map_err(|e| HandlerError::BadInvocationId(id, e)),
            InvocationTargetType::IdempotencyId {
                name,
                target,
                handler,
                idempotency_id,
            } => Ok(InvocationQuery::IdempotencyId(IdempotencyId::new(
                name.into(),
                match target {
                    TargetType::Unkeyed => None,
                    TargetType::Keyed { key } => Some(key.into()),
                },
                handler.into(),
                idempotency_id.into(),
            ))),
        }
    }

    pub(crate) async fn handle_invocation_attach<B: http_body::Body>(
        self,
        req: Request<B>,
        invocation_query: InvocationQuery,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
    {
        // Check HTTP Method
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }

        let (dispatcher_req, correlation_id, response_rx) =
            IngressDispatcherRequest::attach(invocation_query.clone());

        if let Err(e) = self
            .dispatcher
            .dispatch_ingress_request(dispatcher_req)
            .await
        {
            warn!(
                restate.invocation.query = ?invocation_query,
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
            response.invocation_id,
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
        invocation_query: InvocationQuery,
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
            .get_output(invocation_query.clone())
            .await
        {
            Ok(GetOutputResult::Ready(out)) => out,
            Ok(GetOutputResult::NotFound) => return Err(HandlerError::NotFound),
            Ok(GetOutputResult::NotReady) => return Err(HandlerError::NotReady),
            Ok(GetOutputResult::NotSupported) => return Err(HandlerError::UnsupportedGetOutput),
            Err(e) => {
                warn!(
                    restate.invocation.query = ?invocation_query,
                    "Failed to read output: {}",
                    e,
                );
                return Err(HandlerError::Unavailable);
            }
        };

        Self::reply_with_invocation_response(
            response.response,
            response.invocation_id,
            None,
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
}
