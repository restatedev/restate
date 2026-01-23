// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::metric_definitions::{INGRESS_REQUESTS, REQUEST_ADMITTED, REQUEST_RATE_LIMITED};
use futures::ready;
use http::{Request, Response, StatusCode};
use metrics::counter;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::{Layer, Service};
use tracing::warn;

// This service is inspired by tower-util LoadShed and ConcurrencyLimit, but returns a http response.

pub struct LoadShedLayer {
    semaphore: Arc<Semaphore>,
}

impl LoadShedLayer {
    pub fn new(permits: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
        }
    }
}

impl<S> Layer<S> for LoadShedLayer {
    type Service = LoadShed<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LoadShed::new(inner, self.semaphore.clone())
    }
}

#[derive(Debug, Clone)]
pub struct LoadShed<S> {
    inner: S,
    semaphore: Arc<Semaphore>,
}

impl<S> LoadShed<S> {
    pub fn new(inner: S, semaphore: Arc<Semaphore>) -> Self {
        LoadShed { inner, semaphore }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for LoadShed<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: Default,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        // Acquire the semaphore permit to check if we have available quota
        let permit = if let Ok(p) = self.semaphore.clone().try_acquire_owned() {
            p
        } else {
            warn!("No available quota to process the request");

            // Register request denied
            counter!(INGRESS_REQUESTS, "status" => REQUEST_RATE_LIMITED).increment(1);

            return ResponseFuture {
                state: ResponseState::Overloaded,
            };
        };

        // Register request admitted
        counter!(INGRESS_REQUESTS, "status" => REQUEST_ADMITTED).increment(1);

        ResponseFuture {
            state: ResponseState::Called {
                fut: self.inner.call(req),
                _permit: permit,
            },
        }
    }
}

pin_project! {
    pub struct ResponseFuture<F> {
        #[pin]
        state: ResponseState<F>,
    }
}

pin_project! {
    #[project = ResponseStateProj]
    enum ResponseState<F> {
        Called {
            #[pin]
            fut: F,
               // Keep this around so that it is dropped when the future completes
            _permit: OwnedSemaphorePermit,
        },
        Overloaded,
    }
}

impl<F, B, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: Default,
{
    type Output = Result<Response<B>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().state.project() {
            ResponseStateProj::Called { fut, .. } => Poll::Ready(ready!(fut.poll(cx))),
            ResponseStateProj::Overloaded => Poll::Ready(Ok(Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(Default::default())
                .unwrap())),
        }
    }
}
