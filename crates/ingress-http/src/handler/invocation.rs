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
use http_body_util::{BodyExt, Full};
use serde::Serialize;
use tracing::warn;

use super::HandlerError;
use super::path_parsing::{InvocationRequestType, InvocationTargetType, TargetType};
use super::{Handler, InvocationTargetRequest};
use crate::RequestDispatcher;
use crate::handler::responses::X_RESTATE_ID;
use restate_types::errors::{GenericError, InvocationError};
use restate_types::identifiers::{IdempotencyId, InvocationId};
use restate_types::invocation::client::{
    AttachInvocationResponse, GetInvocationOutputResponse, GetInvocationStatusResponse,
};
use restate_types::invocation::{InvocationQuery, client};
use restate_types::schema::invocation_target::InvocationTargetResolver;

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum InvocationStage {
    /// Means the invocation has been created but not yet started.
    Created,
    /// The invocation started and has not yet completed.
    Started,
    /// The invocation completed, with either success or failure.
    Completed,
}

#[derive(Debug, Serialize)]
pub struct InvocationStatusResponse {
    stage: InvocationStage,
    /// Filled if `stage = 'completed'` and the invocation failed
    error: Option<InvocationError>,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher>
where
    Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
{
    pub(crate) async fn handle_invocation<B: http_body::Body>(
        self,
        req: Request<B>,
        invocation_request_type: InvocationRequestType,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
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
            InvocationRequestType::Status(invocation_target_type) => {
                self.handle_invocation_get_status(
                    req,
                    Self::convert_to_invocation_query(invocation_target_type)?.to_invocation_id(),
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
                None,
            ))),
        }
    }

    pub(crate) async fn handle_invocation_attach<B: http_body::Body>(
        self,
        req: Request<B>,
        invocation_query: InvocationQuery,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }
        self.attach_invocation_query(invocation_query).await
    }

    pub(crate) async fn handle_invocation_get_output<B: http_body::Body>(
        self,
        req: Request<B>,
        invocation_query: InvocationQuery,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }
        self.get_invocation_output_query(invocation_query).await
    }

    pub(crate) async fn handle_invocation_get_status<B: http_body::Body>(
        self,
        req: Request<B>,
        invocation_id: InvocationId,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        if req.method() != Method::GET {
            return Err(HandlerError::MethodNotAllowed);
        }
        self.get_invocation_status(invocation_id).await
    }

    pub(crate) async fn handle_attach_by_target<B: http_body::Body>(
        self,
        req: Request<B>,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        let invocation_query = Self::parse_invocation_target_body(req).await?;
        self.attach_invocation_query(invocation_query).await
    }

    pub(crate) async fn handle_output_by_target<B: http_body::Body>(
        self,
        req: Request<B>,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        let invocation_query = Self::parse_invocation_target_body(req).await?;
        self.get_invocation_output_query(invocation_query).await
    }
    pub(crate) async fn handle_status_by_target<B: http_body::Body>(
        self,
        req: Request<B>,
    ) -> Result<Response<Full<Bytes>>, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        let invocation_query = Self::parse_invocation_target_body(req).await?;
        self.get_invocation_status(invocation_query.to_invocation_id())
            .await
    }

    async fn parse_invocation_target_body<B: http_body::Body>(
        req: Request<B>,
    ) -> Result<InvocationQuery, HandlerError>
    where
        <B as http_body::Body>::Error: Into<GenericError>,
    {
        if req.method() != Method::POST {
            return Err(HandlerError::MethodNotAllowed);
        }

        let body_bytes = req
            .into_body()
            .collect()
            .await
            .map_err(|e| HandlerError::Body(e.into()))?
            .to_bytes();

        let target_request: InvocationTargetRequest = serde_json::from_slice(&body_bytes)
            .map_err(|e| HandlerError::Body(anyhow::anyhow!("invalid request body: {e}").into()))?;

        target_request.into_invocation_query()
    }

    async fn attach_invocation_query(
        self,
        invocation_query: InvocationQuery,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let response = match self
            .dispatcher
            .attach_invocation(invocation_query.clone())
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

    async fn get_invocation_output_query(
        self,
        invocation_query: InvocationQuery,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let response = match self
            .dispatcher
            .get_invocation_output(invocation_query.clone())
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
                    restate.invocation.query = ?invocation_query,
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

    async fn get_invocation_status(
        self,
        invocation_id: InvocationId,
    ) -> Result<Response<Full<Bytes>>, HandlerError> {
        let response = match self.dispatcher.get_invocation_status(invocation_id).await {
            Ok(GetInvocationStatusResponse::Status(out)) => out,
            Ok(GetInvocationStatusResponse::NotFound) => {
                return Err(HandlerError::InvocationNotFound);
            }
            Err(e) => {
                warn!(
                    restate.invocation.id = ?invocation_id,
                    "Failed to read status: {}",
                    e,
                );
                return Err(HandlerError::GenericReadDispatcherError(e));
            }
        };

        let stage = match response.state {
            client::InvocationState::Scheduled | client::InvocationState::Inboxed => {
                InvocationStage::Created
            }
            client::InvocationState::Invoked
            | client::InvocationState::Suspended
            | client::InvocationState::Paused => InvocationStage::Started,
            client::InvocationState::Killed
            | client::InvocationState::Failed
            | client::InvocationState::Succeeded => InvocationStage::Completed,
        };

        let body = serde_json::to_vec(&InvocationStatusResponse {
            stage,
            error: response.error,
        })
        .unwrap();

        Ok(Response::builder()
            .header(X_RESTATE_ID, invocation_id.to_string())
            .body(Full::new(body.into()))
            .unwrap())
    }
}
