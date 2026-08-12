// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! # Ingestion ingress
//!
//! Hosts the `IngestionSvc` gRPC service on the same socket as the regular HTTP
//! ingress. Integrations (e.g. a Kafka-to-Restate bridge) open a bidirectional
//! stream and push invocations into the WAL through this service, using an
//! application-level flow-control protocol.
//!
//! The public entry point is [`IngestionRouter`], a [`tower::Service`] that
//! multiplexes a single connection between the gRPC ingestion service and the
//! wrapped HTTP ingress service based on the request `content-type`. The service
//! implementation lives in [`ingestion_svc`].

mod ingestion_svc;

use std::convert::Infallible;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use http::{Request, Response};
use hyper::body::Incoming;
use tower::ServiceExt;

use super::*;

pub(crate) use ingestion_svc::ingestion_server;

/// A [`tower::Service`] that fronts the ingress socket and dispatches each request
/// to one of two backends based on a Picker.
///
/// Both branches are normalized to a `Response<tonic::body::Body>` so callers see a
/// single response type regardless of which backend handled the request.
#[derive(Clone)]
pub struct SteerRouter<L, R, P> {
    left: L,
    right: R,
    picker: P,
}

impl<L, R, P> SteerRouter<L, R, P> {
    pub fn new(left: L, right: R, picker: P) -> Self {
        Self {
            left,
            right,
            picker,
        }
    }
}

impl<L, R, P, LB, RB> tower::Service<Request<Incoming>> for SteerRouter<L, R, P>
where
    P: Picker,
    L: tower::Service<Request<Incoming>, Response = Response<LB>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    L::Future: Send + 'static,
    R: tower::Service<Request<Incoming>, Response = Response<RB>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    R::Future: Send + 'static,
    LB: http_body::Body<Data = Bytes> + Send + 'static,
    RB: http_body::Body<Data = Bytes> + Send + 'static,
    LB::Error: std::error::Error + Send + Sync,
    RB::Error: std::error::Error + Send + Sync,
{
    type Response = Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        // call function uses oneshot which also poll for readiness before
        // calling the service.
        // It's also impossible to know which service to poll for readiness since
        // we have no access to the request.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        match self.picker.pick(&req) {
            Decision::Left => {
                let s = self.left.clone();
                Box::pin(async move {
                    let response = s
                        .oneshot(req)
                        .await?
                        .map(|body| tonic::body::Body::new(body));
                    Ok(response)
                })
            }
            Decision::Right => {
                let s = self.right.clone();
                Box::pin(async move {
                    let response = s
                        .oneshot(req)
                        .await?
                        .map(|body| tonic::body::Body::new(body));
                    Ok(response)
                })
            }
        }
    }
}

pub enum Decision {
    Left,
    Right,
}

pub trait Picker {
    fn pick(&mut self, request: &Request<Incoming>) -> Decision;
}

impl<F> Picker for F
where
    F: Fn(&Request<Incoming>) -> Decision,
{
    fn pick(&mut self, request: &Request<Incoming>) -> Decision {
        self(request)
    }
}

pub fn is_grpc_request<B>(req: &Request<B>) -> bool {
    req.headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/grpc"))
}
