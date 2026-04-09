// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
pub mod conn;
#[cfg(any(test, feature = "test_util"))]
pub mod test_util;
pub mod tls;

use std::{
    io::{self, ErrorKind},
    net::IpAddr,
    str::FromStr,
    task::{Context, Poll},
    time::Duration,
};

use futures::future::BoxFuture;
use http::Uri;
use rustls::pki_types::{DnsName, ServerName};
use tokio::net::TcpStream;
use tower::Service;
use tracing::trace;

/// Errors that can occur during the lifecycle of an H2 connection.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    H2(#[from] h2::Error),
    #[error("connection is closed")]
    Closed,
    #[error("connection keep-alive timeout")]
    KeepAliveTimeout,
}

/// A Tower [`Service`] that establishes TCP connections to a given URI.
///
/// Extracts the host and port from the URI (defaulting to port 80 for HTTP
/// and 443 for HTTPS) and connects via [`TcpStream`].
#[derive(Debug, Clone, Copy)]
pub struct TcpConnector {
    connect_timeout: Duration,
}

impl TcpConnector {
    pub fn new(connect_timeout: Duration) -> Self {
        Self { connect_timeout }
    }
}

impl Service<Uri> for TcpConnector {
    type Response = TcpStream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let connect_timeout = self.connect_timeout;
        let fut = async move {
            let req = req.get_connection_info();
            trace!("connecting to {:?}:{:?}", req.host, req.port());

            let host = req
                .host()
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "unknown host name"))?;
            let port = req
                .port()
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "missing port number"))?;

            let stream = tokio::time::timeout(connect_timeout, async {
                match host {
                    Host::IpAddress(addr) => TcpStream::connect((*addr, port)).await,
                    Host::DnsName(dns) => TcpStream::connect((dns.as_ref(), port)).await,
                }
            })
            .await
            .map_err(|_| io::Error::new(ErrorKind::TimedOut, "connect timeout"))??;

            stream.set_nodelay(true)?;
            Ok(stream)
        };

        Box::pin(fut)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Host {
    IpAddress(IpAddr),
    DnsName(DnsName<'static>),
}

impl From<Host> for ServerName<'static> {
    fn from(value: Host) -> Self {
        match value {
            Host::IpAddress(addr) => ServerName::IpAddress(addr.into()),
            Host::DnsName(dns) => ServerName::DnsName(dns),
        }
    }
}

trait IntoConnectionInfo {
    fn get_connection_info(&self) -> ConnectionInfo;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Schema {
    Unknown,
    Http,
    Https,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConnectionInfo {
    schema: Schema,
    host: Option<Host>,
    port: Option<u16>,
}

impl ConnectionInfo {
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    pub fn host(&self) -> Option<&Host> {
        self.host.as_ref()
    }

    pub fn schema(&self) -> Schema {
        self.schema
    }
}

impl IntoConnectionInfo for Uri {
    fn get_connection_info(&self) -> ConnectionInfo {
        let (schema, default_port) = match self.scheme() {
            None => (Schema::Unknown, None),
            Some(schema) => match schema.as_str() {
                "http" => (Schema::Http, Some(80)),
                "https" => (Schema::Https, Some(443)),
                _ => (Schema::Unknown, None),
            },
        };

        let port = self.port_u16().or(default_port);
        let host = match self.host() {
            None => None,
            Some(host) => match std::net::IpAddr::from_str(host) {
                Ok(addr) => Some(Host::IpAddress(addr)),
                Err(_) => DnsName::try_from_str(host)
                    .ok()
                    .map(|x| Host::DnsName(x.to_owned())),
            },
        };

        ConnectionInfo { schema, host, port }
    }
}

impl<T> IntoConnectionInfo for http::Request<T> {
    fn get_connection_info(&self) -> ConnectionInfo {
        self.uri().get_connection_info()
    }
}
