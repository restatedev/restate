// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::{cancellation_watcher, task_center, ShutdownError, TaskCenter, TaskKind};
use http::Uri;
use hyper::body::{Body, Incoming};
use hyper::rt::{Read, Write};
use hyper_util::rt::TokioIo;
use tokio::io;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio_util::net::Listener;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, instrument, Span};

use restate_types::config::{MetadataStoreClientOptions, NetworkingOptions};
use restate_types::errors::GenericError;
use restate_types::net::{AdvertisedAddress, BindAddress};

pub fn create_tonic_channel_from_advertised_address<T: CommonClientConnectionOptions>(
    address: AdvertisedAddress,
    options: &T,
) -> Channel {
    match address {
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
        AdvertisedAddress::Http(uri) => Channel::builder(uri)
            .connect_timeout(options.connect_timeout())
            .http2_keep_alive_interval(options.keep_alive_interval())
            .keep_alive_timeout(options.keep_alive_timeout())
            .http2_adaptive_window(options.http2_adaptive_window())
            .connect_lazy(),
    }
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
    on_bind: impl Fn(),
    on_stop: impl Fn(),
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
            if uds_path.exists() {
                // if this fails, the following bind will fail, so its safe to ignore this error
                _ = std::fs::remove_file(uds_path);
            }
            let unix_listener = UnixListener::bind(uds_path).map_err(|err| Error::UdsBinding {
                uds_path: uds_path.clone(),
                source: err,
            })?;

            Span::current().record("uds.path", uds_path.display().to_string());
            info!("Server listening");
            on_bind();

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
            on_bind();

            run_listener_loop(tcp_listener, service, server_name).await?;
        }
    }
    on_stop();

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
    L::Addr: Send + Debug + 'static,
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
                debug!(?remote_addr, "Accepting incoming connection");

                tc.spawn_child(TaskKind::RpcConnection, server_name, None, handle_connection(
                    io,
                    service.clone(),
                    executor.clone(),
                    remote_addr,
                ))?;
            }
        }
    }

    Ok(())
}

async fn handle_connection<S, B, I, A>(
    io: I,
    service: S,
    executor: TaskCenterExecutor,
    remote_addr: A,
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
    A: Send + Debug,
{
    let builder = hyper_util::server::conn::auto::Builder::new(executor);
    let connection = builder.serve_connection(io, service);

    tokio::select! {
        res = connection => {
            if let Err(e) = res {
                if let Some(hyper_error) = e.downcast_ref::<hyper::Error>() {
                    if hyper_error.is_incomplete_message() {
                        debug!(?remote_addr, "Connection closed before request completed");
                    }
                } else {
                    anyhow::bail!(Error::HandlingConnection(e));
                }
            }
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

/// Helper trait to extract common client connection options from different configuration types.
pub trait CommonClientConnectionOptions {
    fn connect_timeout(&self) -> Duration;
    fn keep_alive_interval(&self) -> Duration;
    fn keep_alive_timeout(&self) -> Duration;
    fn http2_adaptive_window(&self) -> bool;
}

impl CommonClientConnectionOptions for NetworkingOptions {
    fn connect_timeout(&self) -> Duration {
        self.connect_timeout.into()
    }

    fn keep_alive_interval(&self) -> Duration {
        self.http2_keep_alive_interval.into()
    }

    fn keep_alive_timeout(&self) -> Duration {
        self.http2_keep_alive_timeout.into()
    }

    fn http2_adaptive_window(&self) -> bool {
        self.http2_adaptive_window
    }
}

impl CommonClientConnectionOptions for MetadataStoreClientOptions {
    fn connect_timeout(&self) -> Duration {
        self.metadata_store_connect_timeout.into()
    }

    fn keep_alive_interval(&self) -> Duration {
        self.metadata_store_keep_alive_interval.into()
    }

    fn keep_alive_timeout(&self) -> Duration {
        self.metadata_store_keep_alive_timeout.into()
    }

    fn http2_adaptive_window(&self) -> bool {
        true
    }
}
