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
use tracing::{debug, error_span, info, instrument, trace, Instrument, Span};

use restate_types::config::{Configuration, MetadataClientOptions, NetworkingOptions};
use restate_types::errors::GenericError;
use restate_types::net::{AdvertisedAddress, BindAddress};

use crate::{cancellation_watcher, ShutdownError, TaskCenter, TaskKind};

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
    fields(server_name = %server_name, uds.path = tracing::field::Empty, net.host.addr = tracing::field::Empty, net.host.port = tracing::field::Empty)
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
    let mut configuration = Configuration::updateable();
    let mut shutdown = std::pin::pin!(cancellation_watcher());
    let graceful_shutdown = GracefulShutdown::new();
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
                    .keep_alive_interval(Some(network_options.http2_keep_alive_interval.into()))
                    .keep_alive_timeout(network_options.http2_keep_alive_timeout.into());


                let socket_span = error_span!("SocketHandler", ?peer_addr);
                let connection = graceful_shutdown.watch(builder
                    .serve_connection(io, service.clone()).into_owned());

                TaskCenter::spawn(TaskKind::SocketHandler, server_name, async move {
                    debug!("New connection accepted");
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
        _ = graceful_shutdown.shutdown() => {
            debug!("All connections completed gracefully");

        },
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
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
        let _ = TaskCenter::spawn_child(TaskKind::H2Stream, "h2stream", async move {
            // ignore the future output
            let _ = fut.await;
            Ok(())
        });
    }
}

/// Helper trait to extract common client connection options from different configuration types.
pub trait CommonClientConnectionOptions {
    fn connect_timeout(&self) -> Duration;
    fn request_timeout(&self) -> Option<Duration>;
    fn keep_alive_interval(&self) -> Duration;
    fn keep_alive_timeout(&self) -> Duration;
    fn http2_adaptive_window(&self) -> bool;
}

impl<T: CommonClientConnectionOptions> CommonClientConnectionOptions for &T {
    fn connect_timeout(&self) -> Duration {
        (*self).connect_timeout()
    }

    fn request_timeout(&self) -> Option<Duration> {
        (*self).request_timeout()
    }

    fn keep_alive_interval(&self) -> Duration {
        (*self).keep_alive_interval()
    }

    fn keep_alive_timeout(&self) -> Duration {
        (*self).keep_alive_timeout()
    }

    fn http2_adaptive_window(&self) -> bool {
        (*self).http2_adaptive_window()
    }
}

impl<T> CommonClientConnectionOptions for Arc<T>
where
    T: CommonClientConnectionOptions,
{
    fn connect_timeout(&self) -> Duration {
        (**self).connect_timeout()
    }

    fn request_timeout(&self) -> Option<Duration> {
        (**self).request_timeout()
    }

    fn keep_alive_interval(&self) -> Duration {
        (**self).keep_alive_interval()
    }

    fn keep_alive_timeout(&self) -> Duration {
        (**self).keep_alive_timeout()
    }

    fn http2_adaptive_window(&self) -> bool {
        (**self).http2_adaptive_window()
    }
}

impl CommonClientConnectionOptions for NetworkingOptions {
    fn connect_timeout(&self) -> Duration {
        self.connect_timeout.into()
    }

    fn request_timeout(&self) -> Option<Duration> {
        None
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

impl CommonClientConnectionOptions for MetadataClientOptions {
    fn connect_timeout(&self) -> Duration {
        self.connect_timeout.into()
    }

    fn request_timeout(&self) -> Option<Duration> {
        None
    }

    fn keep_alive_interval(&self) -> Duration {
        self.keep_alive_interval.into()
    }

    fn keep_alive_timeout(&self) -> Duration {
        self.keep_alive_timeout.into()
    }

    fn http2_adaptive_window(&self) -> bool {
        true
    }
}
