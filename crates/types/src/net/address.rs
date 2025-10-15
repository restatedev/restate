// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::OnceLock;

use anyhow::Context;
use http::Uri;
use serde::{Deserialize, Serialize};

/// Trait for information about a port used by a component or a system service
pub trait ListenerPort: Clone + PartialEq + Eq {
    const NAME: &'static str;
    const DEFAULT_PORT: u16;

    const UDS_NAME: &'static str;
    /// Whether this port allows binding on an anonymous unix-socket or not.
    const IS_ANONYMOUS_UDS_ALLOWED: bool;

    fn default_port_str() -> &'static str {
        stringify!(Self::DEFAULT_PORT)
    }
}

/// Implemented on ports that support gRPC protocol
pub trait GrpcPort {}

//
// -- # Concrete Ports
//

/// HTTP Ingress Service 8080
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct HttpIngressPort;
impl ListenerPort for HttpIngressPort {
    const NAME: &'static str = "http-ingress-server";
    const DEFAULT_PORT: u16 = 8080;
    const UDS_NAME: &'static str = "ingress.sock";
    const IS_ANONYMOUS_UDS_ALLOWED: bool = true;
}

/// Admin HTTP Service 9070
/// Also hosts gRPC service for cluster controller
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct AdminPort;
impl ListenerPort for AdminPort {
    const NAME: &'static str = "admin-api-server";
    const DEFAULT_PORT: u16 = 9070;
    const UDS_NAME: &'static str = "admin.sock";
    const IS_ANONYMOUS_UDS_ALLOWED: bool = true;
}

/// gRPC port for control and introspection
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ControlPort;
impl ListenerPort for ControlPort {
    const NAME: &'static str = "control-server";
    const DEFAULT_PORT: u16 = 5123;
    const UDS_NAME: &'static str = "control.sock";
    const IS_ANONYMOUS_UDS_ALLOWED: bool = true;
}
impl GrpcPort for ControlPort {}

/// gRPC port for node-to-node message fabric
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FabricPort;
impl ListenerPort for FabricPort {
    const NAME: &'static str = "message-fabric-server";
    const DEFAULT_PORT: u16 = 5122;
    const UDS_NAME: &'static str = "fabric.sock";
    // this is disallowed for the message fabric since we must be able to acquire a
    // non-anonymous socket address to allow server-to-server communication.
    const IS_ANONYMOUS_UDS_ALLOWED: bool = false;
}
impl GrpcPort for FabricPort {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct TokioConsolePort;
impl ListenerPort for TokioConsolePort {
    const NAME: &'static str = "tokio-console-server";
    const DEFAULT_PORT: u16 = 6669;
    const UDS_NAME: &'static str = "tokio.sock";
    const IS_ANONYMOUS_UDS_ALLOWED: bool = false;
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::Display)]
pub enum SocketAddress {
    /// Unix domain socket
    #[display("unix:{}", _0.display())]
    Uds(PathBuf),
    /// Socket address (IP and port).
    #[display("{}", _0)]
    Socket(SocketAddr),
    #[display("<anonymous>")]
    Anonymous,
}

impl SocketAddress {
    pub fn from_path(path: Option<&Path>) -> Self {
        match path {
            Some(path) => SocketAddress::Uds(path.to_path_buf()),
            None => SocketAddress::Anonymous,
        }
    }
}

#[derive(
    Clone,
    Eq,
    PartialEq,
    Hash,
    derive_more::Display,
    derive_more::Debug,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
#[display("{inner}")]
#[debug("{inner}")]
pub struct BindAddress<P: ListenerPort> {
    inner: SocketAddr,
    _phantom: std::marker::PhantomData<P>,
}

/// Local interface address to listen on (INET sockets only)
/// tcp: bind_address (0.0.0.0:5122)
impl<P: ListenerPort> BindAddress<P> {
    pub const fn new(addr: SocketAddr) -> Self {
        Self {
            inner: addr,
            _phantom: std::marker::PhantomData,
        }
    }

    /// If `use_random_port` is true, the port will be chosen randomly unless `port` is set.
    pub fn from_parts(ip: Option<IpAddr>, port: Option<u16>, use_random_port: bool) -> Self {
        Self {
            inner: SocketAddr::new(
                ip.unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
                port.unwrap_or(if use_random_port { 0 } else { P::DEFAULT_PORT }),
            ),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Fills in the port with the default port for this listener port.
    pub const fn from_ip(ip_addr: IpAddr) -> Self {
        Self {
            inner: SocketAddr::new(ip_addr, P::DEFAULT_PORT),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn port(&self) -> u16 {
        self.inner.port()
    }

    pub fn inner(&self) -> &SocketAddr {
        &self.inner
    }

    pub fn into_inner(self) -> SocketAddr {
        self.inner
    }
}

impl<P: ListenerPort> Default for BindAddress<P> {
    fn default() -> Self {
        Self::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            P::DEFAULT_PORT,
        ))
    }
}

#[cfg(feature = "schemars")]
impl<P: ListenerPort> schemars::JsonSchema for BindAddress<P> {
    fn schema_name() -> String {
        format!("BindAddress-{}", P::NAME)
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        format!("{}::BindAddress[{}]", std::module_path!(), P::NAME).into()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema: schemars::schema::SchemaObject = generator.subschema_for::<String>().into();
        schema.format = Some("ip:port".to_owned());
        let metadata = schema.metadata();
        metadata.title = Some("Bind address".to_owned());
        metadata.description = Some(format!(
            "The local network address to bind on for {}. This service uses default port {} and \
                will create a unix-socket file at the data directory under the name `{}`",
            P::NAME,
            P::DEFAULT_PORT,
            P::UDS_NAME
        ));
        metadata.examples = vec![
            serde_json::Value::String(format!("0.0.0.0:{}", P::DEFAULT_PORT)),
            serde_json::Value::String(format!("127.0.0.1:{}", P::DEFAULT_PORT)),
        ];
        schema.into()
    }
}

/// Parsing from a string
///
/// ::1 is okay, we attach the default port
/// 127.0.0.1:8080 is okay, we use the input port
impl<P: ListenerPort> FromStr for BindAddress<P> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // try first to parse this as full socket address
        let socket_addr = s.parse::<SocketAddr>().or_else(|_| {
            s.parse::<IpAddr>()
                .map(|ip_addr| SocketAddr::new(ip_addr, P::DEFAULT_PORT))
        })?;

        Ok(Self {
            inner: socket_addr,
            _phantom: std::marker::PhantomData,
        })
    }
}

/// An address that describes how to reach a remote node.
#[derive(Debug, Clone, Eq, PartialEq, Hash, derive_more::Display)]
pub enum PeerNetAddress {
    /// Unix domain socket
    #[display("unix:{}", _0.display())]
    Uds(PathBuf),
    /// Uri
    #[display("{}", _0)]
    Http(Uri),
}

impl PeerNetAddress {
    /// Returns true if this is a Unix domain socket address
    pub fn is_unix_domain_socket(&self) -> bool {
        matches!(self, PeerNetAddress::Uds(_))
    }

    /// Returns true if this is an HTTP address
    pub fn is_http(&self) -> bool {
        matches!(self, PeerNetAddress::Http(_))
    }
}

#[derive(
    Hash,
    Clone,
    PartialEq,
    Eq,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
    derive_more::Display,
    derive_more::Debug,
)]
#[display("{inner}")]
#[debug("{inner}")]
pub struct AdvertisedAddress<P: ListenerPort> {
    inner: PeerNetAddress,
    _phantom: std::marker::PhantomData<P>,
}

#[cfg(feature = "schemars")]
impl<P: ListenerPort> schemars::JsonSchema for AdvertisedAddress<P> {
    fn schema_name() -> String {
        format!("AdvertisedAddress-{}", P::NAME)
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        format!("{}::AdvertisedAddress[{}]", std::module_path!(), P::NAME).into()
    }

    fn json_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema: schemars::schema::SchemaObject = generator.subschema_for::<String>().into();
        let metadata = schema.metadata();
        metadata.title = Some("advertised address".to_owned());
        metadata.description = Some(format!(
            "An externally accessible URI address for {}. This can be set to unix:restate-data/{} \
                to advertise the automatically created unix-socket instead of using tcp if needed",
            P::NAME,
            P::UDS_NAME
        ));
        metadata.examples = vec![
            serde_json::Value::String(format!("http//127.0.0.1:{}/", P::DEFAULT_PORT)),
            serde_json::Value::String("https://my-host/".to_owned()),
            serde_json::Value::String(format!("unix:/data/restate-data/{}", P::UDS_NAME)),
        ];
        schema.into()
    }
}

impl<P: ListenerPort> Default for AdvertisedAddress<P> {
    fn default() -> Self {
        Self {
            inner: PeerNetAddress::Http(
                format!("http://127.0.0.1:{}", P::DEFAULT_PORT)
                    .parse()
                    .unwrap(),
            ),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<P: ListenerPort> AdvertisedAddress<P> {
    pub fn derive_from_bind_address(address: SocketAddress, advertised_host: Option<&str>) -> Self {
        let inner = match address {
            SocketAddress::Socket(address) => {
                let routable_ip = || {
                    if address.ip().is_loopback() {
                        // If we are binding to loopback, we shouldn't use the public route-able IP
                        // since we are confident that it'll not be reachable. If this guess doesn't
                        // work for the user, they can always pass an explicit advertised address.
                        if address.ip().is_ipv4() {
                            // mirror the ip version of the bind address
                            "127.0.0.1"
                        } else {
                            "[::1]"
                        }
                    } else {
                        guess_my_routable_ip()
                    }
                };
                // do we have an input hostname?
                let hostname = advertised_host.unwrap_or_else(|| routable_ip());
                PeerNetAddress::Http(
                    format!("http://{hostname}:{}", address.port())
                        .parse()
                        .expect("valid uri"),
                )
            }
            SocketAddress::Uds(path) => {
                // it's a UDS address, we'll use the path.
                PeerNetAddress::Uds(path)
            }
            SocketAddress::Anonymous => {
                // In case this is an anonymous unix-socket, we'll fallback to a generic
                // localhost-based address without a port. The assumption is the caller
                // will proxy their request through the unix-socket and the host+scheme
                // part of the URI will be ignored by the server.
                PeerNetAddress::Http("http://localhost".parse().expect("valid uri"))
            }
        };

        Self {
            inner,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn new_uds(path: PathBuf) -> Self {
        Self {
            inner: PeerNetAddress::Uds(path),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_node_base_dir(dir: &Path) -> Self {
        Self {
            inner: PeerNetAddress::Uds(dir.join(P::UDS_NAME)),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn uri(&self) -> Option<&Uri> {
        match &self.inner {
            PeerNetAddress::Uds(_) => None,
            PeerNetAddress::Http(uri) => Some(uri),
        }
    }

    /// Validates and transforms the address into a [`PeerNetAddress`].
    ///
    /// This will validate if the unix-socket is accessible, and will rewrite the URI
    /// to be compliant with the listener port.
    ///
    /// NOTE: We don't perform this transformation and validation when parsing
    /// the input string is to to acheive two goals:
    ///
    /// 1. Avoid expensive and potentially unnecessary transformations when deserializing
    ///    (i.e. nodes-config)
    /// 2. To allow validation rules to evolve over time, parsing requirements are much
    ///    more relaxed (a uri, or a unix-socket path). So if we added support for other
    ///    schemes other than http/https, we wouldn't want deserialization to automatically
    ///    fail. Validation will only happen when the address is being used and in that case
    ///    we might be willing to provide a compatibility layer without mingling with the serialized
    ///    valued. In other words, when deserializing a value X, reserializing it will always be
    ///    as close as possible to the original value regardless of validation/transformation rules.
    pub fn into_address(self) -> Result<PeerNetAddress, anyhow::Error> {
        match self.inner {
            PeerNetAddress::Uds(path) => Ok(PeerNetAddress::Uds(path)),
            PeerNetAddress::Http(uri) => Self::parse_uri(uri),
        }
    }

    /// Ensures that the address is well formed and performs any necessary transformation.
    pub fn into_well_formed(self) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inner: self.into_address()?,
            _phantom: std::marker::PhantomData,
        })
    }

    /// The conversion rules are like this:
    /// - If the authority has no port, it's assumed to be the default port for the service. unless the
    ///   the scheme is defined.
    /// - If the URI has a scheme, it's used as-is. But only http and https are allowed.
    /// - If the URI has no authority, it's assumed to be 127.0.0.1.
    /// - If the URI has no scheme, it's assumed to be http and we set it to that.
    fn parse_uri(uri: Uri) -> Result<PeerNetAddress, anyhow::Error> {
        let mut parts = uri.into_parts();
        // First, validate that the scheme is either empty or http/https.
        if let Some(ref scheme) = parts.scheme
            && !(scheme == &http::uri::Scheme::HTTP || scheme == &http::uri::Scheme::HTTPS)
        {
            return Err(anyhow::anyhow!(
                "Unsupported URI scheme '{}'",
                scheme.as_str()
            ));
        }

        let Some(authority) = parts.authority else {
            return Err(anyhow::anyhow!("URI must have host and/or ports set"));
        };

        // Input like `:5122` is acceptable, we'll fill the host with `127.0.0.1`
        let host = if authority.host().is_empty() {
            "127.0.0.1"
        } else {
            authority.host()
        };

        // Reconstructing the authority
        parts.authority = Some(if parts.scheme.is_none() {
            format!("{host}:{}", authority.port_u16().unwrap_or(P::DEFAULT_PORT))
                .parse()
                .unwrap()
        } else if let Some(port) = authority.port_u16() {
            format!("{host}:{port}").parse().unwrap()
        } else {
            host.parse().unwrap()
        });

        // default to http if scheme is missing
        if parts.scheme.is_none() {
            parts.scheme = Some(http::uri::Scheme::HTTP);
        }

        if parts.path_and_query.is_none() {
            parts.path_and_query = Some(http::uri::PathAndQuery::from_str("/").unwrap());
        }

        Ok(PeerNetAddress::Http(Uri::from_parts(parts)?))
    }
}

impl<P: ListenerPort> FromStr for AdvertisedAddress<P> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if let Some(path) = trimmed.strip_prefix("unix:") {
            return Ok(Self {
                inner: parse_uds(path).map(PeerNetAddress::Uds)?,
                _phantom: std::marker::PhantomData,
            });
        }

        if trimmed.is_empty() {
            return Err(anyhow::anyhow!("address cannot be empty"));
        }

        Ok(Self {
            inner: PeerNetAddress::Http(parse_uri(trimmed)?),
            _phantom: std::marker::PhantomData,
        })
    }
}

/// A helper function that attempts to derive the public routable IP address of
/// the local machine. Falls back to `127.0.0.1` if the guessing fails.
fn guess_my_routable_ip() -> &'static str {
    static MY_IP: OnceLock<Option<String>> = OnceLock::new();
    static LOCALHOST: &str = "127.0.0.1";
    // guesses the publicly reachable IP address by creating a datagram socket
    // to 1.1.1.1 and then reading the source address of the response.
    // Note that this does not send any packets, but it will use the system's
    // routing table to determine the local interface that is used to reach the
    // default gateway.
    //
    // We fallback to `127.0.0.1` if we failed to guess the public IP address.
    MY_IP
        .get_or_init(|| {
            let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
            socket.connect("1.1.1.1:80").ok()?;
            let ip = socket.local_addr().ok()?.ip();
            let ip = if ip.is_ipv6() {
                // we need to wrap the IPv6 address in brackets to be compatible with
                // the URI specification.
                format!("[{}]", ip)
            } else {
                ip.to_string()
            };
            Some(ip)
        })
        .as_deref()
        .unwrap_or(LOCALHOST)
}

#[inline]
fn parse_uds(s: &str) -> Result<PathBuf, anyhow::Error> {
    s.parse::<PathBuf>()
        .with_context(|| format!("Failed to parse Unix domain socket path: '{s}'"))
}

#[inline]
fn parse_uri(s: &str) -> Result<Uri, anyhow::Error> {
    s.parse::<Uri>()
        .with_context(|| format!("Invalid URI format: '{s}'"))
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;

    use super::*;

    #[test]
    fn test_parse_bind_address() {
        let input = "127.0.0.1:8080";
        let addr = input
            .parse::<BindAddress<ControlPort>>()
            .expect("Failed to parse socket address");

        assert_eq!(addr.inner.ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(addr.inner.port(), 8080);

        let input = "[::1]:8080";
        let addr = input
            .parse::<BindAddress<ControlPort>>()
            .expect("Failed to parse socket address")
            .inner;

        assert_eq!(addr.ip(), IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)));
        assert_eq!(addr.port(), 8080);
    }

    #[test]
    fn test_parse_bind_address_with_default_port() {
        let input = "127.0.0.1";
        let result = match input.parse::<BindAddress<ControlPort>>() {
            Ok(addr) => addr.inner,
            Err(e) => panic!("Failed to parse address: {e:#}"),
        };

        assert_eq!(result.ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert_eq!(result.port(), 5122);

        let input = "::1";
        let result = match input.parse::<BindAddress<ControlPort>>() {
            Ok(addr) => addr.inner,
            Err(e) => panic!("Failed to parse address: {e:#}"),
        };

        assert_eq!(
            result.ip(),
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1))
        );
        assert_eq!(result.port(), 5122);
    }

    #[test]
    fn test_parse_bind_address_invalid() {
        let input = "unsupported:address";
        let result = input.parse::<BindAddress<ControlPort>>();
        assert!(
            result.is_err(),
            "Expected an error for invalid bind address"
        );
    }

    #[test]
    fn test_parse_advertised_address_host_port() {
        // stick the port next to the hostname
        let input = "localhost";
        let result = input.parse::<AdvertisedAddress<ControlPort>>().unwrap();
        assert_eq!(result.to_string(), "localhost");
        let address = result.into_address().unwrap();
        assert_eq!(address.to_string(), "http://localhost:5122/");

        // assume http and 127.0.0.1
        let input = ":5999";
        let result = input.parse::<AdvertisedAddress<ControlPort>>().unwrap();
        assert_eq!(result.to_string(), ":5999");
        let address = result.into_address().unwrap();
        assert_eq!(address.to_string(), "http://127.0.0.1:5999/");

        // assuming ingress users proxy on https and don't put the default port since the scheme is
        // explicit.
        let input = "https://localhost";
        let result = input.parse::<AdvertisedAddress<HttpIngressPort>>().unwrap();
        assert_eq!(result.to_string(), "https://localhost/");
        let address = result.into_address().unwrap();
        assert_eq!(address.to_string(), "https://localhost/");

        // scheme is missing, we assume http
        let input = "localhost:8888";
        let result = input.parse::<AdvertisedAddress<ControlPort>>().unwrap();
        assert_eq!(result.to_string(), "localhost:8888");
        let address = result.into_address().unwrap();
        assert_eq!(address.to_string(), "http://localhost:8888/");

        // ipv6 address, no port, use the default port
        let input = "[::1]";
        let result = input.parse::<AdvertisedAddress<ControlPort>>().unwrap();
        assert_eq!(result.to_string(), "[::1]");
        let address = result.into_address().unwrap();
        assert_eq!(address.to_string(), "http://[::1]:5122/");

        let input = "[::1]:8888";
        let result = input.parse::<AdvertisedAddress<ControlPort>>().unwrap();
        assert_eq!(result.to_string(), "[::1]:8888");
        let address = result.into_address().unwrap();
        assert_eq!(address.to_string(), "http://[::1]:8888/");

        let input = "https://[::1]:8888";
        let result = input.parse::<AdvertisedAddress<ControlPort>>().unwrap();
        assert_eq!(result.to_string(), "https://[::1]:8888/");
        let address = result.into_address().unwrap();
        assert_eq!(address.to_string(), "https://[::1]:8888/");

        let input = "unix:/data/file.sock";
        let result = input.parse::<AdvertisedAddress<ControlPort>>().unwrap();
        assert_eq!(result.to_string(), "unix:/data/file.sock");
        let address = result.into_address().unwrap();
        assert_eq!(address.to_string(), "unix:/data/file.sock");

        let input = "ftp://localhost:8080";
        let result = input.parse::<AdvertisedAddress<FabricPort>>().unwrap();
        assert_eq!(result.to_string(), "ftp://localhost:8080/");
        let address = result.into_address();
        assert!(
            address.is_err(),
            "Expected an error for unsupported URI scheme"
        );
        assert!(
            address
                .unwrap_err()
                .to_string()
                .contains("Unsupported URI scheme 'ftp'")
        );

        // this fails at parsing time
        let input = "";
        let result = input.parse::<AdvertisedAddress<FabricPort>>();
        assert!(result.is_err(), "Expected an error for empty input");
    }
}
