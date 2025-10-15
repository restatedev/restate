// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    fmt,
    marker::PhantomData,
    path::PathBuf,
    pin::Pin,
    task::{self, Context, Poll},
};

use http::Uri;
use hyper_util::rt::TokioIo;
use pin_project::pin_project;
use tokio::net::UnixStream;
use tower::Service;

/// Unix domain socket connector for Hyper.
///
/// Can be used to create hyper clients that channel requests to the underlying unix-domain socket
#[derive(Clone)]
pub struct UnixSocketConnector(PathBuf);

impl UnixSocketConnector {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }
}

impl Service<Uri> for UnixSocketConnector {
    type Response = TokioIo<UnixStream>;
    type Error = BoxError;
    type Future = UnixConnecting;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    // Unix-socket based connections ignore "authority" and "scheme" parts of the URI
    fn call(&mut self, _dst: Uri) -> Self::Future {
        let socket_path = self.0.clone();

        let fut = async move {
            UnixStream::connect(socket_path)
                .await
                .map(TokioIo::new)
                .map_err(Into::into)
        };

        UnixConnecting {
            fut: Box::pin(fut),
            _marker: PhantomData,
        }
    }
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl fmt::Debug for UnixSocketConnector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("UnixConnector").finish_non_exhaustive()
    }
}

type ConnectResult = Result<TokioIo<UnixStream>, BoxError>;
type BoxConnecting = Pin<Box<dyn Future<Output = ConnectResult> + Send>>;

#[pin_project]
#[must_use = "futures do nothing unless polled"]
pub struct UnixConnecting<R = ()> {
    #[pin]
    fut: BoxConnecting,
    _marker: PhantomData<R>,
}

impl<R> Future for UnixConnecting<R> {
    type Output = ConnectResult;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        self.project().fut.poll(cx)
    }
}

impl<R> fmt::Debug for UnixConnecting<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad("UnixConnecting")
    }
}
