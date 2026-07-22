// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod integration_svc;

use std::convert::Infallible;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use http::{Request, Response};
use http_body_util::BodyExt;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::body::Incoming;
use tower::ServiceExt;
use tower::util::BoxCloneService;

use restate_types::errors::GenericError;
use restate_types::live::Live;
use restate_types::schema::invocation_target::InvocationTargetResolver;

use super::*;

/// The canonical gRPC path prefix of the `IntegrationSvc` service. Requests whose
/// path starts with it are dispatched to the tonic integration server; everything
/// else falls through to the layered ingress handler.
pub(crate) const INTEGRATION_SERVICE_PREFIX: &str =
    "/dev.restate.ingress.integration.IntegrationSvc/";

/// Unified response body for both branches. The gRPC branch produces bodies that
/// fail with `tonic::Status`, so the error can't be `Infallible`; `GenericError`
/// covers both and satisfies the connection server's `Into<GenericError>` bound.
type RoutedBody = UnsyncBoxBody<Bytes, GenericError>;

/// The type-erased tonic integration server. Boxing keeps `IntegrationRouter` free of a
/// `Schemas` type parameter (the schema resolver is captured inside the server).
type GrpcService = BoxCloneService<Request<Incoming>, Response<tonic::body::Body>, Infallible>;

#[derive(Clone)]
pub(super) struct IntegrationRouter<S> {
    inner: S,
    grpc: GrpcService,
}

impl<S> IntegrationRouter<S> {
    pub(crate) fn new<Schemas>(inner: S, schemas: Live<Schemas>) -> Self
    where
        Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    {
        Self {
            inner,
            grpc: BoxCloneService::new(integration_svc::integration_server(schemas)),
        }
    }
}

impl<S, Body> tower::Service<Request<Incoming>> for IntegrationRouter<S>
where
    S: tower::Service<Request<Incoming>, Response = Response<Body>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    Body: http_body::Body<Data = Bytes, Error = Infallible> + Send + 'static,
{
    type Response = Response<RoutedBody>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        // Both inner services are always ready.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        if req.uri().path().starts_with(INTEGRATION_SERVICE_PREFIX) {
            // tonic routes on the full canonical path, so forward the request as-is.
            let grpc = self.grpc.clone();
            Box::pin(async move {
                let response = grpc.oneshot(req).await?;
                // Widen the gRPC body's `tonic::Status` error to the unified type.
                Ok(response.map(|body| body.map_err(Into::into).boxed_unsync()))
            })
        } else {
            let main = self.inner.clone();
            Box::pin(async move {
                let response = main.oneshot(req).await?;
                // The inner body is `Infallible`; widen it to the unified error type.
                Ok(response.map(|body| body.map_err(Into::into).boxed_unsync()))
            })
        }
    }
}
