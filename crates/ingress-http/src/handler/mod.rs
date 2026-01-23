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
mod path_parsing;
mod responses;
mod service_handler;
#[cfg(test)]
mod tests;
mod tracing;
mod workflow;

use std::convert::Infallible;
use std::task::{Context, Poll};

use error::HandlerError;
use futures::FutureExt;
use futures::future::BoxFuture;
use http_body_util::Full;
use hyper::http::HeaderValue;
use hyper::{Request, Response};
use path_parsing::RequestType;
use restate_types::live::Live;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::service::ServiceMetadataResolver;

use super::*;

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");

#[derive(Clone)]
pub(crate) struct Handler<Schemas, Dispatcher> {
    schemas: Live<Schemas>,
    dispatcher: Dispatcher,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher> {
    pub(crate) fn new(schemas: Live<Schemas>, dispatcher: Dispatcher) -> Self {
        Self {
            schemas,
            dispatcher,
        }
    }
}

impl<Schemas, Dispatcher, Body> tower::Service<Request<Body>> for Handler<Schemas, Dispatcher>
where
    Schemas: ServiceMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: RequestDispatcher + Clone + Send + Sync + 'static,
    Body: http_body::Body + Send + 'static,
    <Body as http_body::Body>::Data: Send + 'static,
    <Body as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
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
            }
        }
        .map(|r| Ok::<_, Infallible>(r.unwrap_or_else(|e| e.into_response())))
        .boxed()
    }
}
