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
use std::sync::Arc;
use std::time::Duration;

use http::Uri;
use hyper::body::{Body, Incoming};
use hyper_util::rt::TokioIo;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::io;
use tokio::net::UnixStream;
use tokio_util::either::Either;
use tonic::transport::{Channel, Endpoint};
use tracing::{Instrument, Span, debug, error_span, info, instrument, trace};

use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::net::address::{AdvertisedAddress, GrpcPort};
use restate_types::net::address::{ListenerPort, PeerNetAddress};
use restate_types::net::connect_opts::CommonClientConnectionOptions;
use restate_types::net::listener::Listeners;

use crate::{ShutdownError, TaskCenter, TaskKind, cancellation_watcher};

pub fn create_tonic_channel<
    T: CommonClientConnectionOptions + Send + Sync + ?Sized,
    P: ListenerPort + GrpcPort,
>(
    address: AdvertisedAddress<P>,
    options: &T,
) -> Channel {
    let address = address.into_address().expect("valid address");
    let endpoint = match &address {
        PeerNetAddress::Uds(_) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1").expect("/ should be a valid Uri")
        }
        PeerNetAddress::Http(uri) => Channel::builder(uri.clone()),
    };

    let endpoint = apply_options(endpoint, options);

    match address {
        PeerNetAddress::Uds(uds_path) => {
            endpoint.connect_with_connector_lazy(tower::service_fn(move |_: Uri| {
                let uds_path = uds_path.clone();
                async move {
                    Ok::<_, io::Error>(TokioIo::new(UnixStream::connect(uds_path).await?))
                }
            }))
        }
        PeerNetAddress::Http(_) => endpoint.connect_lazy()
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
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("failed handling hyper connection: {0}")]
    HandlingConnection(#[from] GenericError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

#[instrument(
    level = "error",
    name = "server",
    skip_all,
    fields(server_name = %P::NAME, uds.path = tracing::field::Empty, server.address = tracing::field::Empty, server.port = tracing::field::Empty)
)]
pub async fn run_hyper_server<P: ListenerPort, S, B>(
    listeners: Listeners<P>,
    service: S,
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
    if let Some(uds_path) = listeners.uds_address() {
        Span::current().record("uds.path", uds_path.display().to_string());
    }

    if let Some(socket_addr) = listeners.tcp_address() {
        Span::current().record("server.address", socket_addr.ip().to_string());
        Span::current().record("server.port", socket_addr.port());
    }

    info!("Server listening");
    on_bind();
    run_listener_loop(listeners, service, P::NAME).await?;
    on_stop();

    info!("Stopped listening");

    Ok(())
}

async fn run_listener_loop<P: ListenerPort, S, B>(
    mut listeners: Listeners<P>,
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
    let mut configuration = Configuration::live();
    let mut shutdown = std::pin::pin!(cancellation_watcher());
    let graceful_shutdown = GracefulShutdown::new();
    let task_name: Arc<str> = Arc::from(format!("{server_name}-socket"));

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                debug!("Shutdown requested, will stop listening to new connections");
                drop(listeners);
                break;
            }
            incoming_connection = listeners.accept() => {
                let (stream, peer_addr) = incoming_connection?;
                let socket_span = error_span!("SocketHandler", ?peer_addr);

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

                match stream {
                    Either::Left(tcp_stream) => {
                        // TCP SOCKET
                        let io = TokioIo::new(tcp_stream);
                        let connection = graceful_shutdown.watch(builder
                            .serve_connection(io, service.clone()).into_owned());
                        TaskCenter::spawn(TaskKind::SocketHandler, task_name.clone(), async move {
                            trace!("New tcp connection accepted");
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

                    },
                    Either::Right(unix_stream) => {
                        // UNIX SOCKET
                        let io = TokioIo::new(unix_stream);
                        let connection = graceful_shutdown.watch(builder
                            .serve_connection(io, service.clone()).into_owned());
                        TaskCenter::spawn(TaskKind::SocketHandler, task_name.clone(), async move {
                            trace!("New uds connection accepted");
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
