// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use error::HandlerError;
use futures::future::BoxFuture;
use futures::FutureExt;
use http_body_util::Full;
use hyper::http::HeaderValue;
use hyper::{Request, Response};
use path_parsing::RequestType;
use restate_ingress_dispatcher::DispatchIngressRequest;
use restate_schema_api::component::ComponentMetadataResolver;
use restate_schema_api::invocation_target::InvocationTargetResolver;
use std::convert::Infallible;
use std::task::{Context, Poll};

mod awakeables;
mod component_handler;
mod error;
mod health;
mod path_parsing;
#[cfg(test)]
mod tests;
mod tracing;

const APPLICATION_JSON: HeaderValue = HeaderValue::from_static("application/json");

#[derive(Clone)]
pub(crate) struct Handler<Schemas, Dispatcher> {
    schemas: Schemas,
    dispatcher: Dispatcher,
}

impl<Schemas, Dispatcher> Handler<Schemas, Dispatcher> {
    pub(crate) fn new(schemas: Schemas, dispatcher: Dispatcher) -> Self {
        Self {
            schemas,
            dispatcher,
        }
    }
}

impl<Schemas, Dispatcher, Body> tower::Service<Request<Body>> for Handler<Schemas, Dispatcher>
where
    Schemas: ComponentMetadataResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
    Dispatcher: DispatchIngressRequest + Clone + Send + Sync + 'static,
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

        let this = self.clone();
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
                RequestType::Component(component_request) => {
                    this.handle_component_request(req, component_request).await
                }
            }
        }
        .map(|r| Ok::<_, Infallible>(r.unwrap_or_else(|e| e.into_response())))
        .boxed()
    }
}
