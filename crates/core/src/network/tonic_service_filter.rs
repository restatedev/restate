// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::future::{Either, Ready};
use http::Request;
use restate_types::health::HealthStatus;
use std::convert::Infallible;
use std::task::{Context, Poll};
use tonic::body::BoxBody;
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::Status;

/// A tonic service wrapper that filters requests based on a predicate. This can be used to
/// dynamically disable a service based on some condition.
#[derive(Clone)]
pub struct TonicServiceFilter<T, U> {
    inner: T,
    predicate: U,
}

impl<T, U> TonicServiceFilter<T, U> {
    pub fn new(inner: T, predicate: U) -> Self {
        Self { inner, predicate }
    }
}

impl<T, U> Service<Request<BoxBody>> for TonicServiceFilter<T, U>
where
    T: Service<Request<BoxBody>, Response = http::Response<BoxBody>, Error = Infallible>,
    U: Predicate,
{
    type Response = http::Response<BoxBody>;
    type Error = Infallible;
    type Future = Either<T::Future, Ready<Result<http::Response<BoxBody>, Infallible>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        match self.predicate.check(req) {
            Ok(req) => Either::Left(self.inner.call(req)),
            Err(status) => Either::Right(futures::future::ready(Ok(status.into_http()))),
        }
    }
}

/// Predicate for the [`TonicServiceFilter`].
pub trait Predicate {
    /// Checks whether the given request should be processed. Return the given `request` if it
    /// should be processed. Otherwise, return the [`tonic::Status`] with which the request should
    /// fail.
    fn check(&mut self, request: Request<BoxBody>) -> Result<Request<BoxBody>, tonic::Status>;
}

impl<T, U> NamedService for TonicServiceFilter<T, U>
where
    T: NamedService,
{
    const NAME: &'static str = T::NAME;
}

impl<F> Predicate for F
where
    F: FnMut(Request<BoxBody>) -> Result<Request<BoxBody>, tonic::Status>,
{
    fn check(&mut self, request: Request<BoxBody>) -> Result<Request<BoxBody>, tonic::Status> {
        self(request)
    }
}

/// [`Predicate`] implementation which waits for the given status before allowing requests.
#[derive(Clone, Debug)]
pub struct WaitForReady<T> {
    status: HealthStatus<T>,
    ready_status: T,
}

impl<T> WaitForReady<T> {
    pub fn new(status: HealthStatus<T>, ready_status: T) -> Self {
        WaitForReady {
            status,
            ready_status,
        }
    }
}

impl<T> Predicate for WaitForReady<T>
where
    T: PartialEq,
{
    fn check(&mut self, request: Request<BoxBody>) -> Result<Request<BoxBody>, Status> {
        if *self.status.get() == self.ready_status {
            Ok(request)
        } else {
            Err(Status::unavailable("svc is not ready yet"))
        }
    }
}
