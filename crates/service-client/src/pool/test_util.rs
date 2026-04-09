// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::future::BoxFuture;
use http::{Response, StatusCode, Uri};
use tokio::{io::DuplexStream, sync::Notify};
use tower::Service;

/// In-process h2 server configuration.
pub struct ServerConfig {
    pub max_concurrent_streams: u32,
}

/// A test connector that creates in-memory duplex streams and spawns an
/// h2 server on the other end.
#[derive(Clone)]
pub struct TestConnector {
    config: std::sync::Arc<ServerConfig>,
}

impl TestConnector {
    pub fn new(max_concurrent_streams: u32) -> Self {
        Self {
            config: std::sync::Arc::new(ServerConfig {
                max_concurrent_streams,
            }),
        }
    }
}

impl Service<Uri> for TestConnector {
    type Response = DuplexStream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Uri) -> Self::Future {
        let config = std::sync::Arc::clone(&self.config);
        Box::pin(async move {
            let (client, server) = tokio::io::duplex(64 * 1024);
            tokio::spawn(run_echo_server(server, config));
            Ok(client)
        })
    }
}

/// Runs an h2 server on the given stream. For each request, echoes the
/// request body back in the response and sends empty trailers when done.
pub async fn run_echo_server(stream: DuplexStream, config: std::sync::Arc<ServerConfig>) {
    let mut h2 = h2::server::Builder::new()
        .max_concurrent_streams(config.max_concurrent_streams)
        .handshake::<_, Bytes>(stream)
        .await
        .unwrap();

    while let Some(request) = h2.accept().await {
        let (request, mut send_response) = request.unwrap();
        tokio::spawn(async move {
            let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
            let mut send_stream = send_response.send_response(response, false).unwrap();
            let mut request_body = request.into_body();

            while let Some(data) = request_body.data().await {
                let data = data.unwrap();
                request_body
                    .flow_control()
                    .release_capacity(data.len())
                    .unwrap();

                send_stream.reserve_capacity(data.len());
                let _ = futures::future::poll_fn(|cx| send_stream.poll_capacity(cx)).await;
                if send_stream.send_data(data, false).is_err() {
                    return;
                }
            }

            let _ = send_stream.send_trailers(http::HeaderMap::new());
        });
    }
}

/// A test connector that blocks `call()` on a [`tokio::sync::Notify`] gate,
/// giving tests explicit control over when the handshake proceeds.
#[derive(Clone)]
pub struct ControlledConnector {
    server_config: Arc<ServerConfig>,
    gate: Arc<Notify>,
    force_error: Arc<AtomicBool>,
}

impl ControlledConnector {
    pub fn new(max_concurrent_streams: u32) -> (Self, Arc<tokio::sync::Notify>) {
        let gate = Arc::new(tokio::sync::Notify::new());
        let connector = Self {
            server_config: Arc::new(ServerConfig {
                max_concurrent_streams,
            }),
            gate: Arc::clone(&gate),
            force_error: Arc::new(AtomicBool::new(false)),
        };
        (connector, gate)
    }

    pub fn with_error(max_concurrent_streams: u32) -> (Self, Arc<Notify>) {
        let (connector, gate) = Self::new(max_concurrent_streams);
        connector.force_error.store(true, Ordering::Relaxed);
        (connector, gate)
    }
}

impl Service<Uri> for ControlledConnector {
    type Response = DuplexStream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Uri) -> Self::Future {
        let config = Arc::clone(&self.server_config);
        let gate = Arc::clone(&self.gate);
        let force_error = Arc::clone(&self.force_error);
        Box::pin(async move {
            gate.notified().await;
            if force_error.load(Ordering::Relaxed) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "forced error",
                ));
            }
            let (client, server) = tokio::io::duplex(64 * 1024);
            tokio::spawn(run_echo_server(server, config));
            Ok(client)
        })
    }
}
