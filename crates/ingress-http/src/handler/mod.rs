// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod awakeables;
mod error;
mod health;
mod invocation;
mod lookup;
mod path_parsing;
mod responses;
mod service_handler;
#[cfg(test)]
mod tests;
mod tracing;
mod workflow;

use std::convert::Infallible;
use std::task::{Context, Poll};

use bytestring::ByteString;
use enumset::EnumSet;
use error::HandlerError;
use futures::FutureExt;
use futures::future::BoxFuture;
use http_body_util::Full;
use hyper::http::HeaderValue;
use hyper::{Request, Response};
use serde::Deserialize;

use restate_core::Metadata;
use restate_types::Scope;
use restate_types::errors::GenericError;
use restate_types::identifiers::{IdempotencyId, ServiceId};
use restate_types::invocation::InvocationQuery;
use restate_types::live::Live;
use restate_types::nodes_config::ClusterFeature;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::service::ServiceMetadataResolver;
use restate_util_string::{ReString, RestrictedValue};

use super::*;
use crate::handler::path_parsing::{
    AwakeableRequestType, InvocationRequestType, ServiceRequestType, WorkflowRequestType,
};

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");

enum RequestType {
    Health,
    OpenAPI,
    Awakeable(AwakeableRequestType),
    Invocation(InvocationRequestType),
    Service(ServiceRequestType),
    Workflow(WorkflowRequestType),
    /// `GET /restate/attach/{invocation_id}`
    Attach(InvocationId),
    /// `GET /restate/output/{invocation_id}`
    Output(InvocationId),
    /// `POST /restate/attach` with a body resolving to an invocation target
    AttachByTarget,
    /// `POST /restate/output` with a body resolving to an invocation target
    OutputByTarget,
    /// `POST /restate/lookup`
    Lookup,
}

#[derive(Clone)]
pub(crate) struct Handler<Schemas, Dispatcher> {
    schemas: Live<Schemas>,
    dispatcher: Dispatcher,
    cluster_features: EnumSet<ClusterFeature>,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher> {
    pub(crate) fn new(schemas: Live<Schemas>, dispatcher: Dispatcher) -> Self {
        let cluster_features = Metadata::with_current(|m| m.nodes_config_ref().features());

        Self {
            schemas,
            dispatcher,
            cluster_features,
        }
    }
}

impl<Schemas, Dispatcher, Body> tower::Service<Request<Body>> for Handler<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
    Body: http_body::Body + Send + 'static,
    <Body as http_body::Body>::Data: Send + 'static,
    <Body as http_body::Body>::Error: Into<GenericError>,
{
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let res = self.parse_path(req.uri());

        let mut this = self.clone();
        async move {
            match res? {
                RequestType::Health => this.handle_health(req),
                RequestType::OpenAPI => {
                    // TODO
                    Err(HandlerError::NotImplemented)
                }
                RequestType::Awakeable(awakeable_request) => {
                    this.handle_awakeable(req, awakeable_request).await
                }
                RequestType::Service(service_request) => {
                    this.handle_service_request(req, service_request).await
                }
                RequestType::Invocation(invocation_request) => {
                    this.handle_invocation(req, invocation_request).await
                }
                RequestType::Workflow(workflow_request) => {
                    this.handle_workflow(req, workflow_request).await
                }
                RequestType::Attach(invocation_id) => {
                    this.handle_invocation_attach(req, InvocationQuery::Invocation(invocation_id))
                        .await
                }
                RequestType::Output(invocation_id) => {
                    this.handle_invocation_get_output(
                        req,
                        InvocationQuery::Invocation(invocation_id),
                    )
                    .await
                }
                RequestType::AttachByTarget => this.handle_attach_by_target(req).await,
                RequestType::OutputByTarget => this.handle_output_by_target(req).await,
                RequestType::Lookup => this.handle_lookup(req).await,
            }
        }
        .map(|r| Ok::<_, Infallible>(r.unwrap_or_else(|e| e.into_response())))
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "target",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum InvocationTargetRequest {
    Workflow {
        #[serde(default)]
        scope: Option<ReString>,
        workflow_name: ReString,
        workflow_key: ReString,
    },
    IdempotentInvocation {
        #[serde(default)]
        scope: Option<ReString>,
        service: ReString,
        #[serde(default)]
        key: Option<ReString>,
        handler: ReString,
        idempotency_key: ReString,
    },
    Invocation {
        invocation_id: ReString,
    },
}

impl InvocationTargetRequest {
    pub(crate) fn into_invocation_query(self) -> Result<InvocationQuery, HandlerError> {
        let scope_value = match self {
            Self::Workflow { ref scope, .. } | Self::IdempotentInvocation { ref scope, .. } => {
                scope.clone()
            }
            Self::Invocation { .. } => None,
        };

        // Unfortunately, we cannot first check the existence of the service/handler or workflow
        // because it might have been removed from the Schema after an invocation having completed :-(
        // For such a check to work, we need to keep information about previously registered services
        // and workflows. Hence, we can only validate that the scope value and avoid interning.
        let scope = match scope_value {
            None => None,
            Some(s) => Some(Scope::new(
                RestrictedValue::new(s).map_err(HandlerError::BadScopeValue)?,
            )),
        };

        Ok(match self {
            Self::Workflow {
                workflow_name,
                workflow_key,
                ..
            } => InvocationQuery::Workflow(ServiceId::new(
                scope,
                ByteString::from(workflow_name.as_str()),
                ByteString::from(workflow_key.as_str()),
            )),
            Self::IdempotentInvocation {
                service,
                key,
                handler,
                idempotency_key,
                ..
            } => InvocationQuery::IdempotencyId(IdempotencyId::new(
                ByteString::from(service.as_str()),
                key.map(|s| ByteString::from(s.as_str())),
                ByteString::from(handler.as_str()),
                ByteString::from(idempotency_key.as_str()),
                scope,
            )),
            Self::Invocation { invocation_id } => InvocationQuery::Invocation(
                invocation_id
                    .parse()
                    .map_err(|e| HandlerError::BadInvocationId(invocation_id.to_string(), e))?,
            ),
        })
    }
}
