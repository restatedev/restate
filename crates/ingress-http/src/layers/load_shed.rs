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
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::ready;
use http::{Request, Response};
use http_body::Body;
use http_body_util::Full;
use metrics::counter;
use pin_project_lite::pin_project;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower::{Layer, Service};
use tracing::warn;

use crate::handler::error::HandlerError;
use crate::metric_definitions::{INGRESS_REQUESTS, REQUEST_ADMITTED, REQUEST_RATE_LIMITED};

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
    ResBody: Body,
{
    type Response = Response<PermittedBody<ResBody>>;
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
                _permit: Some(permit),
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
            // Held for the duration of the call. On success it is handed over to the response
            // body (see `poll`); otherwise it is released when the future is dropped.
            _permit: Option<OwnedSemaphorePermit>,
        },
        Overloaded,
    }
}

impl<F, B, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: Body,
{
    type Output = Result<Response<PermittedBody<B>>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().state.project() {
            ResponseStateProj::Called { fut, _permit } => {
                // `PermittedBody` keeps the permit alive until the body itself is dropped, so a
                // long-lived stream (e.g. an ingestion stream) counts against the concurrency
                // limit for its whole duration.
                let response = ready!(fut.poll(cx))
                    .map(|response| response.map(|body| PermittedBody::new(body, _permit.take())));
                Poll::Ready(response)
            }
            ResponseStateProj::Overloaded => Poll::Ready(Ok(HandlerError::TooManyRequests
                .into_response()
                .map(|body| PermittedBody::full(body, None)))),
        }
    }
}

pin_project! {
    #[project = InnerBodyProj]
    enum InnerBody<B> where B: Body {
        Body{#[pin] body: B},
        Full{#[pin] body: Full<Bytes>},
    }
}

impl<B> Default for InnerBody<B>
where
    B: Body,
{
    fn default() -> Self {
        Self::Full {
            body: Default::default(),
        }
    }
}

pin_project! {
    #[derive(Default)]
    pub struct PermittedBody<B>  where B: Body{
        #[pin]
        body: InnerBody<B>,
        _permit: Option<OwnedSemaphorePermit>,
    }
}

impl<B> PermittedBody<B>
where
    B: Body,
{
    pub fn full(body: Full<Bytes>, permit: Option<OwnedSemaphorePermit>) -> Self {
        Self {
            body: InnerBody::Full { body },
            _permit: permit,
        }
    }

    pub fn new(body: B, permit: Option<OwnedSemaphorePermit>) -> Self {
        Self {
            body: InnerBody::Body { body },
            _permit: permit,
        }
    }
}

impl<B> Body for PermittedBody<B>
where
    B: Body,
    B::Error: From<io::Error>,
    B::Data: From<Bytes>,
{
    type Data = B::Data;
    type Error = B::Error;

    fn is_end_stream(&self) -> bool {
        match &self.body {
            InnerBody::Body { body } => body.is_end_stream(),
            InnerBody::Full { body } => body.is_end_stream(),
        }
    }

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match this.body.project() {
            InnerBodyProj::Body { body } => body.poll_frame(cx),
            InnerBodyProj::Full { body } => body
                .poll_frame(cx)
                .map_ok(|frame| frame.map_data(Self::Data::from))
                .map_err(|_| {
                    // this should not happen. Unfortunately tonic::Status does not have an implementation
                    // From<Infallible>, so we have to do this little dance.
                    Self::Error::from(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "should never fail",
                    ))
                }),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match &self.body {
            InnerBody::Body { body } => body.size_hint(),
            InnerBody::Full { body } => body.size_hint(),
        }
    }
}
