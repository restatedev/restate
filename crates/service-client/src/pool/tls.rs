// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TLS connection layer for HTTP clients.
//!
//! Provides Tower middleware for establishing TLS connections using rustls

use std::{
    io, mem,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Duration,
};

use futures::FutureExt;
use http::Uri;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::client::TlsStream;
use tower::{Layer, Service};

use crate::pool::{IntoConnectionInfo, Schema};

use super::ConnectionInfo;

/// A Tower [`Layer`] that adds TLS support to transport services.
///
/// Wraps a TCP connector and performs TLS handshakes for HTTPS URIs,
/// passing through plain connections for HTTP URIs.
pub struct TlsConnectorLayer {
    config: ClientConfig,
    handshake_timeout: Duration,
}

impl TlsConnectorLayer {
    pub fn new(config: ClientConfig, handshake_timeout: Duration) -> Self {
        Self {
            config,
            handshake_timeout,
        }
    }
}
impl<S> Layer<S> for TlsConnectorLayer {
    type Service = TlsConnector<S>;
    fn layer(&self, inner: S) -> Self::Service {
        TlsConnector::new(inner, self.config.clone(), self.handshake_timeout)
    }
}

/// A service that optionally wraps connections in TLS.
///
/// For HTTPS URIs, performs a TLS handshake with ALPN support for both
/// "http/1.1" and "h2" protocols. For HTTP URIs, passes through the
/// plain connection. Uses native system certificates for validation.
#[derive(Clone)]
pub struct TlsConnector<S> {
    inner: S,
    connector: tokio_rustls::TlsConnector,
    handshake_timeout: Duration,
}

impl<S> TlsConnector<S> {
    pub fn new(inner: S, mut config: ClientConfig, handshake_timeout: Duration) -> Self {
        // only support h2
        config.alpn_protocols = vec!["h2".into()];

        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        Self {
            inner,
            connector,
            handshake_timeout,
        }
    }
}

impl<S> Service<Uri> for TlsConnector<S>
where
    S: Service<Uri, Error = io::Error> + Send + Clone + 'static,
    S::Response: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S::Future: Send,
{
    type Error = io::Error;
    type Future = TlsConnectorFuture<S::Future, S::Response>;
    type Response = MaybeTlsStream<S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let connection_info = req.get_connection_info();

        let mut this = mem::replace(self, self.clone());
        let fut = this.inner.call(req);

        TlsConnectorFutureInner::connecting(
            fut,
            connection_info,
            this.connector,
            self.handshake_timeout,
        )
        .into()
    }
}

/// Future returned by [`TlsConnectorService`] that resolves an inner connection future
/// and optionally upgrades it to TLS, yielding a [`MaybeTlsStream`].
///
// This wrapper exists to keep [`TlsConnectorFutureInner`] private; exposing the enum
// directly would make all its variants public as well.
#[pin_project::pin_project]
pub struct TlsConnectorFuture<F, IO> {
    #[pin]
    inner: TlsConnectorFutureInner<F, IO>,
}

impl<F, IO> From<TlsConnectorFutureInner<F, IO>> for TlsConnectorFuture<F, IO> {
    fn from(inner: TlsConnectorFutureInner<F, IO>) -> Self {
        Self { inner }
    }
}

impl<F, IO> Future for TlsConnectorFuture<F, IO>
where
    F: Future<Output = Result<IO, io::Error>>,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Result<MaybeTlsStream<IO>, io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

#[pin_project::pin_project(project = TlsConnectorFutureInnerProject)]
enum TlsConnectorFutureInner<F, IO> {
    /// Waiting for the TCP connection to be established.
    Connecting {
        #[pin]
        fut: F,
        connection_info: ConnectionInfo,
        connector: tokio_rustls::TlsConnector,
        handshake_timeout: Duration,
    },
    /// TCP connected, performing TLS handshake.
    Handshaking {
        // the Connect<IO> fut is too big.
        // clippy suggested to wrap it in a box
        fut: Pin<Box<tokio::time::Timeout<tokio_rustls::client::Connect<IO>>>>,
    },
}

impl<F, IO> TlsConnectorFutureInner<F, IO> {
    fn connecting(
        fut: F,
        connection_info: ConnectionInfo,
        connector: tokio_rustls::TlsConnector,
        handshake_timeout: Duration,
    ) -> Self {
        Self::Connecting {
            fut,
            connection_info,
            connector,
            handshake_timeout,
        }
    }
}

impl<F, IO> Future for TlsConnectorFutureInner<F, IO>
where
    F: Future<Output = Result<IO, io::Error>>,
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Result<MaybeTlsStream<IO>, io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self;

        loop {
            match this.as_mut().project() {
                TlsConnectorFutureInnerProject::Connecting {
                    fut,
                    connection_info,
                    connector,
                    handshake_timeout,
                } => match ready!(fut.poll(cx)) {
                    Err(err) => {
                        return Poll::Ready(Err(err));
                    }
                    Ok(socket) => {
                        let host = match connection_info.host() {
                            Some(host) => host,
                            None => {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "unknown host",
                                )));
                            }
                        };

                        match connection_info.schema() {
                            Schema::Unknown => {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "unknown schema",
                                )));
                            }
                            Schema::Http => {
                                return Poll::Ready(Ok(MaybeTlsStream::Plain(socket)));
                            }
                            Schema::Https => {
                                let fut = tokio::time::timeout(
                                    *handshake_timeout,
                                    connector.clone().connect(host.clone().into(), socket),
                                );

                                this.set(TlsConnectorFutureInner::Handshaking {
                                    fut: Box::pin(fut),
                                });
                            }
                        }
                    }
                },
                TlsConnectorFutureInnerProject::Handshaking { fut } => {
                    match ready!(fut.poll_unpin(cx)) {
                        Ok(Ok(stream)) => {
                            return Poll::Ready(Ok(MaybeTlsStream::TLS(Box::new(stream))));
                        }
                        Ok(Err(err)) => {
                            return Poll::Ready(Err(err));
                        }
                        Err(_) => {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "timeout during tls handshake",
                            )));
                        }
                    }
                }
            }
        }
    }
}

/// A stream that may or may not be wrapped in TLS.
///
/// Provides a unified interface for both encrypted (HTTPS) and plain (HTTP)
/// connections. Implements [`AsyncRead`] and [`AsyncWrite`] by delegating
/// to the inner stream.
pub enum MaybeTlsStream<S> {
    /// A TLS-encrypted stream (HTTPS).
    TLS(Box<TlsStream<S>>),
    /// A plain, unencrypted stream (HTTP).
    Plain(S),
}

impl<S> AsyncRead for MaybeTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(plain) => {
                let plain = Pin::new(plain);
                plain.poll_read(cx, buf)
            }
            MaybeTlsStream::TLS(tls) => {
                let tls = Pin::new(tls);
                tls.poll_read(cx, buf)
            }
        }
    }
}

impl<S> AsyncWrite for MaybeTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(plain) => {
                let plain = Pin::new(plain);
                plain.poll_write(cx, buf)
            }
            MaybeTlsStream::TLS(tls) => {
                let tls = Pin::new(tls);
                tls.poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(plain) => {
                let plain = Pin::new(plain);
                plain.poll_flush(cx)
            }
            MaybeTlsStream::TLS(tls) => {
                let tls = Pin::new(tls);
                tls.poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            MaybeTlsStream::Plain(plain) => {
                let plain = Pin::new(plain);
                plain.poll_shutdown(cx)
            }
            MaybeTlsStream::TLS(tls) => {
                let tls = Pin::new(tls);
                tls.poll_shutdown(cx)
            }
        }
    }
}
