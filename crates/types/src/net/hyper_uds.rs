// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Heavily inspired by
// https://github.com/kristof-mattei/hyper-unix-socket/blob/c6c3df10c5bc6ace61469fe84d6eba4a043179c5/src/client.rs

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::task::{self, Context, Poll};

use hyper::Uri;
use hyper_util::rt::TokioIo;
use pin_project_lite::pin_project;
use tokio::net::UnixStream;
use tower_service::Service;

use super::UnixSocketConnection;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A Connector for a Socket.
#[derive(Clone)]
pub struct UnixSocketConnector<P> {
    socket_path: P,
}

impl<P: AsRef<Path>> UnixSocketConnector<P> {
    /// Construct a new `UnixStreamConnector`.
    #[must_use]
    pub fn new(socket_path: P) -> Self {
        Self { socket_path }
    }
}

impl<T: AsRef<Path>> From<T> for UnixSocketConnector<T> {
    fn from(args: T) -> UnixSocketConnector<T> {
        UnixSocketConnector { socket_path: args }
    }
}

impl<T> fmt::Debug for UnixSocketConnector<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("HttpsConnector").finish_non_exhaustive()
    }
}

impl<T> Service<Uri> for UnixSocketConnector<T>
where
    T: AsRef<Path> + Clone + Send,
{
    type Response = UnixSocketConnection;
    type Error = BoxError;
    type Future = UnixStreamConnecting;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Uri) -> Self::Future {
        let socket_path = self.socket_path.as_ref().to_owned();

        let fut = async move {
            UnixStream::connect(socket_path)
                .await
                .map(TokioIo::new)
                .map(Into::into)
                .map_err(Into::into)
        };

        UnixStreamConnecting {
            fut: Box::pin(fut),
            _marker: PhantomData,
        }
    }
}

type ConnectResult = Result<UnixSocketConnection, BoxError>;
type BoxConnecting = Pin<Box<dyn Future<Output = ConnectResult> + Send>>;

pin_project! {
    #[must_use = "futures do nothing unless polled"]
    pub struct UnixStreamConnecting<R = ()> {
        #[pin]
        fut: BoxConnecting,
        _marker: PhantomData<R>,
    }
}

impl<R> Future for UnixStreamConnecting<R> {
    type Output = ConnectResult;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        self.project().fut.poll(cx)
    }
}

impl<R> fmt::Debug for UnixStreamConnecting<R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.pad("UnixStreamConnecting")
    }
}
