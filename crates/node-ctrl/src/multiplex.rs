// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
//

//! Mostly copied from axum example: https://github.com/tokio-rs/axum/blob/791d5038a927add3f8ff1d5c1010cd0b24fdda77/examples/rest-grpc-multiplex/src/multiplex_service.rs
//! License MIT

use axum::{
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
};
use futures::{future::BoxFuture, ready};
use hyper::Request;
use std::{
    convert::Infallible,
    task::{Context, Poll},
};
use tower::Service;

pub struct MultiplexService<A, B> {
    base: A,
    base_ready: bool,
    grpc: B,
    grpc_ready: bool,
}

impl<A, B> MultiplexService<A, B> {
    pub fn new(base: A, grpc: B) -> Self {
        Self {
            base,
            base_ready: false,
            grpc,
            grpc_ready: false,
        }
    }
}

impl<A, B> Clone for MultiplexService<A, B>
where
    A: Clone,
    B: Clone,
{
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            grpc: self.grpc.clone(),
            // the cloned services probably wont be ready
            base_ready: false,
            grpc_ready: false,
        }
    }
}

impl<A, B> Service<Request<hyper::Body>> for MultiplexService<A, B>
where
    A: Service<Request<hyper::Body>, Error = Infallible>,
    A::Response: IntoResponse,
    A::Future: Send + 'static,
    B: Service<Request<hyper::Body>>,
    B::Response: IntoResponse,
    B::Future: Send + 'static,
{
    type Response = Response;
    type Error = B::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // drive readiness for each inner service and record which is ready
        loop {
            match (self.base_ready, self.grpc_ready) {
                (true, true) => {
                    return Ok(()).into();
                }
                (false, _) => {
                    ready!(self.base.poll_ready(cx)).map_err(|err| match err {})?;
                    self.base_ready = true;
                }
                (_, false) => {
                    ready!(self.grpc.poll_ready(cx))?;
                    self.grpc_ready = true;
                }
            }
        }
    }

    fn call(&mut self, req: Request<hyper::Body>) -> Self::Future {
        // require users to call `poll_ready` first, if they don't we're allowed to panic
        // as per the `tower::Service` contract
        assert!(
            self.grpc_ready,
            "grpc service not ready. Did you forget to call `poll_ready`?"
        );
        assert!(
            self.base_ready,
            "http service not ready. Did you forget to call `poll_ready`?"
        );

        // if we get a grpc request call the grpc service, otherwise call the http service
        // when calling a service it becomes not-ready so we have drive readiness again
        if is_grpc_request(&req) {
            self.grpc_ready = false;
            let future = self.grpc.call(req);
            Box::pin(async move {
                let res = future.await?;
                Ok(res.into_response())
            })
        } else {
            self.base_ready = false;
            let future = self.base.call(req);
            Box::pin(async move {
                let res = future.await.map_err(|err| match err {})?;
                Ok(res.into_response())
            })
        }
    }
}

fn is_grpc_request<B>(req: &Request<B>) -> bool {
    req.headers()
        .get(CONTENT_TYPE)
        .map(|content_type| content_type.as_bytes())
        .filter(|content_type| content_type.starts_with(b"application/grpc"))
        .is_some()
}
