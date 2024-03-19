// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use http::Uri;
use hyper::body::HttpBody;
use hyper::server::accept::Accept;
use hyper::server::conn::AddrIncoming;
use restate_types::net::{AdvertisedAddress, BindAddress};
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Channel, Endpoint};
use tower::service_fn;
use tracing::info;

pub fn create_grpc_channel_from_advertised_address(
    address: AdvertisedAddress,
) -> Result<Channel, http::Error> {
    let channel = match address {
        AdvertisedAddress::Uds(uds_path) => {
            // dummy endpoint required to specify an uds connector, it is not used anywhere
            Endpoint::try_from("http://127.0.0.1")
                .expect("/ should be a valid Uri")
                .connect_with_connector_lazy(service_fn(move |_: Uri| {
                    UnixStream::connect(uds_path.clone())
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
        source: hyper::Error,
    },
    #[error("failed opening uds '{uds_path}': {source}")]
    UdsBinding {
        uds_path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("failed running grpc server: {0}")]
    Running(#[from] hyper::Error),
}

pub async fn run_hyper_server<S, B, F>(
    bind_address: BindAddress,
    service: S,
    shutdown_signal: F,
    server_name: &str,
) -> Result<(), Error>
where
    S: hyper::service::Service<http::Request<hyper::Body>, Response = hyper::Response<B>>
        + Send
        + Clone
        + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    F: Future<Output = ()>,
{
    match bind_address {
        BindAddress::Uds(uds_path) => {
            let unix_listener = UnixListener::bind(&uds_path).map_err(|err| Error::UdsBinding {
                uds_path: uds_path.clone(),
                source: err,
            })?;
            let acceptor =
                hyper::server::accept::from_stream(UnixListenerStream::new(unix_listener));

            info!(uds.path = %uds_path.display(), "Server '{}' listening", server_name);

            run_server(acceptor, service, shutdown_signal).await?
        }
        BindAddress::Socket(socket_addr) => {
            run_tcp_server(socket_addr, service, shutdown_signal, server_name).await?
        }
    }

    Ok(())
}

async fn run_tcp_server<S, B, F>(
    socket_addr: SocketAddr,
    service: S,
    shutdown_signal: F,
    server_name: &str,
) -> Result<(), Error>
where
    S: hyper::service::Service<http::Request<hyper::Body>, Response = hyper::Response<B>>
        + Send
        + Clone
        + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    F: Future<Output = ()>,
{
    let acceptor = AddrIncoming::bind(&socket_addr).map_err(|err| Error::TcpBinding {
        address: socket_addr,
        source: err,
    })?;

    info!(
        net.host.addr = %acceptor.local_addr().ip(),
        net.host.port = %acceptor.local_addr().port(),
        "Server '{}' listening", server_name
    );

    run_server(acceptor, service, shutdown_signal).await
}

async fn run_server<S, B, Conn, Err, F>(
    acceptor: impl Accept<Conn = Conn, Error = Err>,
    service: S,
    shutdown_signal: F,
) -> Result<(), Error>
where
    S: hyper::service::Service<http::Request<hyper::Body>, Response = hyper::Response<B>>
        + Send
        + Clone
        + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S::Future: Send,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    Err: Into<Box<dyn std::error::Error + Send + Sync>>,
    F: Future<Output = ()>,
{
    let server = hyper::Server::builder(acceptor).serve(tower::make::Shared::new(service));

    server
        .with_graceful_shutdown(shutdown_signal)
        .await
        .map_err(Error::Running)
}
