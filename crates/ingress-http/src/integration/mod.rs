// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! # Integration ingress
//!
//! Hosts the `IntegrationSvc` gRPC service on the same socket as the regular HTTP
//! ingress. Integrations (e.g. a Kafka-to-Restate bridge) open a bidirectional
//! stream and push invocations into the WAL through this service, using an
//! application-level flow-control protocol.
//!
//! The public entry point is [`IntegrationRouter`], a [`tower::Service`] that
//! multiplexes a single connection between the gRPC integration service and the
//! wrapped HTTP ingress service based on the request `content-type`. The service
//! implementation lives in [`integration_svc`].

mod integration_svc;

use std::convert::Infallible;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use http::{Request, Response};
use hyper::body::Incoming;
use tower::ServiceExt;
use tower::util::BoxCloneService;

use restate_core::network::TransportConnect;
use restate_ingestion_client::IngestionClient;
use restate_types::live::Live;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_wal_protocol::Envelope;

use super::*;

/// The type-erased tonic integration server. Boxing keeps `IntegrationRouter` free of a
/// `Schemas` type parameter (the schema resolver is captured inside the server).
type GrpcService = BoxCloneService<Request<Incoming>, Response<tonic::body::Body>, Infallible>;

/// A [`tower::Service`] that fronts the ingress socket and dispatches each request
/// to one of two backends:
///
/// * `application/grpc` requests are forwarded to the tonic integration server
///   (`grpc`), when the integration feature is enabled.
/// * everything else is forwarded to the wrapped HTTP ingress service (`inner`).
///
/// Both branches are normalized to a `Response<tonic::body::Body>` so callers see a
/// single response type regardless of which backend handled the request. The gRPC
/// server is optional: when `IntegrationOptions::enabled` is `false` it is `None`
/// and every request falls through to `inner`.
#[derive(Clone)]
pub(super) struct IntegrationRouter<S> {
    inner: S,
    grpc: GrpcService,
}

impl<S> IntegrationRouter<S> {
    pub(crate) fn new<T, Schemas>(
        inner: S,
        ingestion_client: IngestionClient<T, Envelope>,
        schemas: Live<Schemas>,
    ) -> Self
    where
        T: TransportConnect,
        Schemas: InvocationTargetResolver + Clone + Send + Sync + 'static,
    {
        Self {
            inner,
            grpc: BoxCloneService::new(integration_svc::integration_server(
                ingestion_client,
                schemas,
            )),
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
    type Response = Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        // Both inner services are always ready.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        if is_grpc_request(&req) {
            // tonic routes on the full canonical path, so forward the request as-is.
            let grpc = self.grpc.clone();
            Box::pin(async move {
                let response = grpc.oneshot(req).await?;
                // Widen the gRPC body's `tonic::Status` error to the unified type.
                Ok(response)
            })
        } else {
            let main = self.inner.clone();
            Box::pin(async move {
                let response = main.oneshot(req).await?;
                Ok(response.map(|body| tonic::body::Body::new(body)))
            })
        }
    }
}

fn is_grpc_request<B>(req: &Request<B>) -> bool {
    req.headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/grpc"))
}
