// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tower::{Layer, Service};

use crate::drain::{IngressDrainHandle, RequestGuard};

#[derive(Clone)]
pub(crate) struct InFlightRequestsLayer(IngressDrainHandle);

impl InFlightRequestsLayer {
    pub(crate) fn new(drain: IngressDrainHandle) -> Self {
        Self(drain)
    }
}

impl<S> Layer<S> for InFlightRequestsLayer {
    type Service = InFlightRequests<S>;

    fn layer(&self, inner: S) -> Self::Service {
        InFlightRequests {
            inner,
            drain: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct InFlightRequests<S> {
    inner: S,
    drain: IngressDrainHandle,
}

impl<S, Request> Service<Request> for InFlightRequests<S>
where
    S: Service<Request>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = InFlightRequestFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        InFlightRequestFuture {
            inner: self.inner.call(request),
            _guard: self.drain.request_started(),
        }
    }
}

pin_project! {
    pub(crate) struct InFlightRequestFuture<F> {
        #[pin]
        inner: F,
        _guard: RequestGuard,
    }
}

impl<F> Future for InFlightRequestFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}
