// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use hyper::body::{Body, Incoming};
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::io;
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio_util::net::Listener;
use tonic::transport::{Channel, Endpoint};
use tracing::{Instrument, Span, debug, error_span, info, instrument, trace};

use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::net::connect_opts::CommonClientConnectionOptions;
use restate_types::net::{AdvertisedAddress, BindAddress};

use crate::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};

pub fn create_tonic_channel<T: CommonClientConnectionOptions + Send + Sync + ?Sized>(
    address: AdvertisedAddress,
    options: &T,
) -> Channel {
    let endpoint = match &address {
        AdvertisedAddress::Uds(_) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1").expect("/ should be a valid Uri")
        }
        AdvertisedAddress::Http(uri) => Channel::builder(uri.clone()),
    };

    let endpoint = apply_options(endpoint, options);

    match address {
        AdvertisedAddress::Uds(uds_path) => {
            endpoint.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
                let uds_path = uds_path.clone();
                async move {
                    Ok::<_, io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                }
            }))
        }
        AdvertisedAddress::Http(_) => endpoint.connect_lazy()
    }
}

fn apply_options<T: CommonClientConnectionOptions + Send + Sync + ?Sized>(
    endpoint: Endpoint,
    options: &T,
) -> Endpoint {
    if let Some(request_timeout) = options.request_timeout() {
        endpoint.timeout(request_timeout)
    } else {
        endpoint
    }
    .connect_timeout(options.connect_timeout())
    .http2_keep_alive_interval(options.keep_alive_interval())
    .keep_alive_timeout(options.keep_alive_timeout())
    .http2_adaptive_window(options.http2_adaptive_window())
    // this true by default, but this is to guard against any change in defaults
    .tcp_nodelay(true)
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

#[instrument(
    level = "error",
    name = "server",
    skip_all,
    fields(server_name = %server_name, uds.path = tracing::field::Empty, server.address = tracing::field::Empty, server.port = tracing::field::Empty, network.transport = tracing::field::Empty)
)]
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
            Span::current().record("network.transport", "unix");
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

            Span::current().record("server.address", local_addr.ip().to_string());
            Span::current().record("server.port", local_addr.port());
            Span::current().record("network.transport", "tcp");
            info!("Server listening");
            on_bind();

            run_listener_loop(tcp_listener, service, server_name).await?;
        }
    }
    on_stop();

    info!("Stopped listening");

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
    let mut configuration = Configuration::live();
    let mut shutdown = std::pin::pin!(cancellation_watcher());
    let graceful_shutdown = GracefulShutdown::new();
    let task_name: Arc<str> = Arc::from(format!("{server_name}-socket"));

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                debug!("Shutdown requested, will stop listening to new connections");
                drop(listener);
                break;
            }
            incoming_connection = listener.accept() => {
                let (stream, peer_addr) = incoming_connection?;
                let io = TokioIo::new(stream);

                let network_options = &configuration.live_load().networking;
                let mut builder = hyper_util::server::conn::auto::Builder::new(TaskCenterExecutor);
                builder
                    .http2()
                    .timer(hyper_util::rt::TokioTimer::default())
                    .adaptive_window(network_options.http2_adaptive_window)
                    .initial_connection_window_size(network_options.connection_window_size())
                    .initial_stream_window_size(network_options.stream_window_size())
                    .keep_alive_interval(Some(network_options.http2_keep_alive_interval.into()))
                    .keep_alive_timeout(network_options.http2_keep_alive_timeout.into());


                let socket_span = error_span!("SocketHandler", ?peer_addr);
                let connection = graceful_shutdown.watch(builder
                    .serve_connection(io, service.clone()).into_owned());

                TaskCenter::spawn(TaskKind::SocketHandler, task_name.clone(), async move {
                    trace!("New connection accepted");
                    if let Err(e) = connection.await {
                        if let Some(hyper_error) = e.downcast_ref::<hyper::Error>() {
                            if hyper_error.is_incomplete_message() {
                                debug!("Connection closed before request completed");
                            }
                        } else {
                            debug!("Connection terminated due to error: {e}");
                        }
                    } else {
                        trace!("Connection completed cleanly");
                    }
                    Ok(())
                }.instrument(socket_span))?;
            }
        }
    }

    debug!("Draining current connections");
    tokio::select! {
        () = graceful_shutdown.shutdown() => {
            debug!("All connections completed gracefully");

        },
        () = tokio::time::sleep(Duration::from_secs(30)) => {
            info!("Some connections are taking longer to drain, dropping them");
        }
    }

    Ok(())
}

#[derive(Clone, Default)]
struct TaskCenterExecutor;

impl<F> hyper::rt::Executor<F> for TaskCenterExecutor
where
    F: Future + 'static + Send,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        let _ = TaskCenter::spawn_child(TaskKind::H2ServerStream, "h2stream", async move {
            // ignore the future output
            let _ = fut.await;
            Ok(())
        });
    }
}
