// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{cancellation_watcher, task_center, ShutdownError, TaskCenter, TaskKind};
use http::Uri;
use hyper::body::{Body, Incoming};
use hyper::rt::{Read, Write};
use hyper_util::rt::TokioIo;
use restate_types::errors::GenericError;
use restate_types::net::{AdvertisedAddress, BindAddress};
use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio_util::net::Listener;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, instrument, Span};

pub fn create_tonic_channel_from_advertised_address(
    address: AdvertisedAddress,
) -> Result<Channel, http::Error> {
    let channel = match address {
        AdvertisedAddress::Uds(uds_path) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1")
                .expect("/ should be a valid Uri")
                .connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
                    let uds_path = uds_path.clone();
                    async move {
                        Ok::<_, io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                    }
                }))
        }
        AdvertisedAddress::Http(uri) => {
            // todo: Make the channel settings configurable
            Channel::builder(uri)
                .connect_timeout(Duration::from_secs(5))
                // todo: configure the channel from configuration file
                .http2_adaptive_window(true)
                .connect_lazy()
        }
    };
    Ok(channel)
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed binding to address '{address}': {source}")]
    TcpBinding {
        address: SocketAddr,
        #[source]
        source: io::Error,
    },
    #[error("failed opening uds '{uds_path}': {source}")]
    UdsBinding {
        uds_path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed handling hyper connection: {0}")]
    HandlingConnection(#[from] GenericError),
    #[error("failed listening on incoming connections: {0}")]
    Listening(#[from] io::Error),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

#[instrument(level = "info", skip_all, fields(server_name = %server_name, uds.path = tracing::field::Empty, net.host.addr = tracing::field::Empty, net.host.port = tracing::field::Empty))]
pub async fn run_hyper_server<S, B>(
    bind_address: &BindAddress,
    service: S,
    server_name: &'static str,
) -> Result<(), Error>
where
    S: hyper::service::Service<http::Request<Incoming>, Response = hyper::Response<B>>
        + Send
        + Clone
        + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    match bind_address {
        BindAddress::Uds(uds_path) => {
            let unix_listener = UnixListener::bind(uds_path).map_err(|err| Error::UdsBinding {
                uds_path: uds_path.clone(),
                source: err,
            })?;

            Span::current().record("uds.path", uds_path.display().to_string());
            info!("Server listening");

            run_listener_loop(unix_listener, service, server_name).await?;
        }
        BindAddress::Socket(socket_addr) => {
            let tcp_listener =
                TcpListener::bind(socket_addr)
                    .await
                    .map_err(|err| Error::TcpBinding {
                        address: *socket_addr,
                        source: err,
                    })?;

            let local_addr = tcp_listener.local_addr().map_err(|err| Error::TcpBinding {
                address: *socket_addr,
                source: err,
            })?;

            Span::current().record("net.host.addr", local_addr.ip().to_string());
            Span::current().record("net.host.port", local_addr.port());
            info!("Server listening");

            run_listener_loop(tcp_listener, service, server_name).await?;
        }
    }

    debug!("Stopped server");

    Ok(())
}

async fn run_listener_loop<L, S, B>(
    mut listener: L,
    service: S,
    server_name: &'static str,
) -> Result<(), Error>
where
    L: Listener,
    L::Io: Send + Unpin + 'static,
    L::Addr: Debug,
    S: hyper::service::Service<http::Request<Incoming>, Response = hyper::Response<B>>
        + Send
        + Clone
        + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut shutdown = std::pin::pin!(cancellation_watcher());
    let tc = task_center();
    let executor = TaskCenterExecutor::new(tc.clone(), server_name);
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                break;
            }
            incoming_connection = listener.accept() => {
                let (stream, remote_addr) = incoming_connection?;
                let io = TokioIo::new(stream);

                debug!("Accepting incoming connection from '{remote_addr:?}'.");

                tc.spawn_child(TaskKind::RpcConnection, server_name, None, handle_connection(
                    io,
                    service.clone(),
                    executor.clone(),
                ))?;
            }
        }
    }

    Ok(())
}

async fn handle_connection<S, B, I>(
    io: I,
    service: S,
    executor: TaskCenterExecutor,
) -> anyhow::Result<()>
where
    S: hyper::service::Service<http::Request<Incoming>, Response = hyper::Response<B>>
        + Send
        + Clone
        + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    I: Read + Write + Unpin + 'static,
{
    let builder = hyper_util::server::conn::auto::Builder::new(executor);
    let connection = builder.serve_connection(io, service);

    tokio::select! {
        res = connection => {
            // propagate errors
            res.map_err(Error::HandlingConnection)?;
        },
        _ = cancellation_watcher() => {}
    }

    Ok(())
}

#[derive(Clone)]
struct TaskCenterExecutor {
    task_center: TaskCenter,
    name: &'static str,
}

impl TaskCenterExecutor {
    fn new(task_center: TaskCenter, name: &'static str) -> Self {
        Self { task_center, name }
    }
}

impl<F> hyper::rt::Executor<F> for TaskCenterExecutor
where
    F: Future + 'static + Send,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        // ignore shutdown error
        let _ =
            self.task_center
                .spawn_child(TaskKind::RpcConnection, self.name, None, async move {
                    // ignore the future output
                    let _ = fut.await;
                    Ok(())
                });
    }
}
