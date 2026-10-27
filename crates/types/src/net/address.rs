// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
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
    const SUPPORTS_TLS: bool = false;
    /// Address schemes that can be dialed on this port. Addresses with other schemes still
    /// parse and round-trip unchanged, but [`AdvertisedAddress::into_address`] rejects them.
    const SCHEMES: &'static [AddressScheme] = &[
        AddressScheme::Http,
        AddressScheme::Https,
        AddressScheme::Unix,
    ];

    const UDS_NAME: &'static str;
    /// Whether this port allows binding on an anonymous unix-socket or not.
    const IS_ANONYMOUS_UDS_ALLOWED: bool;

    fn default_port_str() -> &'static str;
}

/// Implemented on ports that support gRPC protocol
pub trait GrpcPort {}

/// A dialable address scheme. See [`ListenerPort::SCHEMES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressScheme {
    /// `http://` and addresses written without a scheme
    Http,
    /// `https://`
    Https,
    /// `unix:/path`
    Unix,
    /// `grpc+unix:/path`, an explicit spelling of `unix:` for gRPC ports
    GrpcUnix,
}

const GRPC_SCHEMES: &[AddressScheme] = &[
    AddressScheme::Http,
    AddressScheme::Https,
    AddressScheme::Unix,
    AddressScheme::GrpcUnix,
];

//
// -- # Concrete Ports
//

/// HTTP Ingress Service 8080
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "utoipa-schema", derive(utoipa::ToSchema))]
pub struct HttpIngressPort;
impl ListenerPort for HttpIngressPort {
    const NAME: &'static str = "http-ingress-server";
    const DEFAULT_PORT: u16 = 8080;
    const UDS_NAME: &'static str = "ingress.sock";
    const IS_ANONYMOUS_UDS_ALLOWED: bool = true;
    fn default_port_str() -> &'static str {
        "8080"
    }
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
    fn default_port_str() -> &'static str {
        "9070"
    }
}

/// gRPC port for control and introspection
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ControlPort;
impl ListenerPort for ControlPort {
    const NAME: &'static str = "control-server";
    const DEFAULT_PORT: u16 = 5122;
    // Control RPCs share the fabric listener and its TLS settings today. Keep this enabled so a
    // future control-typed listener does not silently serve plaintext or advertise `http://`.
    const SUPPORTS_TLS: bool = true;
    const SCHEMES: &'static [AddressScheme] = GRPC_SCHEMES;
    const UDS_NAME: &'static str = "control.sock";
    const IS_ANONYMOUS_UDS_ALLOWED: bool = true;
    fn default_port_str() -> &'static str {
        "5122"
    }
}
impl GrpcPort for ControlPort {}

/// gRPC port for node-to-node message fabric
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct FabricPort;
impl ListenerPort for FabricPort {
    const NAME: &'static str = "message-fabric-server";
    // todo: node-to-node fabric traffic is planned to move to its own port, 5123, in a future
    // release. 5122 stays the control port, because tools, health probes and metrics scrapers
    // are already configured against it.
    const DEFAULT_PORT: u16 = 5122;
    const SUPPORTS_TLS: bool = true;
    const SCHEMES: &'static [AddressScheme] = GRPC_SCHEMES;
    const UDS_NAME: &'static str = "fabric.sock";
    // this is disallowed for the message fabric since we must be able to acquire a
    // non-anonymous socket address to allow server-to-server communication.
    const IS_ANONYMOUS_UDS_ALLOWED: bool = false;
    fn default_port_str() -> &'static str {
        "5122"
    }
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
    fn default_port_str() -> &'static str {
        "6669"
    }
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
/// tcp: bind_address ([::]:5122)
impl<P: ListenerPort> BindAddress<P> {
    pub const fn new(addr: SocketAddr) -> Self {
        Self {
            inner: addr,
            _phantom: std::marker::PhantomData,
        }
    }

    /// If `use_random_port` is true, the port will be chosen randomly unless `port` is set.
    /// When no IP is specified, defaults to `::` (IPv6 unspecified) which on most systems
    /// creates a dual-stack socket accepting both IPv4 and IPv6 connections.
    pub fn from_parts(ip: Option<IpAddr>, port: Option<u16>, use_random_port: bool) -> Self {
        Self {
            inner: SocketAddr::new(
                ip.unwrap_or(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
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
    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("BindAddress-{}", P::NAME).into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        format!("{}::BindAddress[{}]", std::module_path!(), P::NAME).into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "title": "Bind address",
            "description": format!(
            "The local network address to bind on for {}. This service uses default port {} and \
                will create a unix-socket file at the data directory under the name `{}`",
            P::NAME,
            P::DEFAULT_PORT,
            P::UDS_NAME
        ),
            "examples": [format!("[::]:{}",P::DEFAULT_PORT), format!("0.0.0.0:{}", P::DEFAULT_PORT), format!("127.0.0.1:{}", P::DEFAULT_PORT)]
        })
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

    /// Returns true if this address uses the `https` scheme (TLS).
    pub fn is_tls(&self) -> bool {
        matches!(self, PeerNetAddress::Http(uri) if uri.scheme() == Some(&http::uri::Scheme::HTTPS))
    }
}

/// An advertised address exactly as it was parsed, so that it serializes back unchanged even when
/// this version cannot dial it.
///
/// Every scheme Restate defines takes one of two shapes: `scheme://authority[/path]`, stored as
/// [`RawAddress::Uri`], or `scheme:/absolute/path`. Both shapes must keep parsing for any scheme,
/// because a single unparseable address fails the decode of the whole `NodesConfiguration`.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum RawAddress {
    /// Anything `http::Uri` accepts, including `scheme://authority` with an unknown scheme.
    Uri(Uri),
    /// `unix:path` or `grpc+unix:path`
    Uds {
        spelling: UdsSpelling,
        path: PathBuf,
    },
    /// `scheme:/absolute/path` with a scheme this version does not know, such as
    /// `restate+unix:/run/ds.sock`. Kept verbatim and never dialed.
    UnknownPath(Box<str>),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, derive_more::Display)]
enum UdsSpelling {
    #[display("unix")]
    Unix,
    #[display("grpc+unix")]
    GrpcUnix,
}

impl std::fmt::Display for RawAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawAddress::Uri(uri) => write!(f, "{uri}"),
            RawAddress::Uds { spelling, path } => write!(f, "{spelling}:{}", path.display()),
            RawAddress::UnknownPath(raw) => f.write_str(raw),
        }
    }
}

impl From<PeerNetAddress> for RawAddress {
    fn from(address: PeerNetAddress) -> Self {
        match address {
            PeerNetAddress::Uds(path) => RawAddress::Uds {
                spelling: UdsSpelling::Unix,
                path,
            },
            PeerNetAddress::Http(uri) => RawAddress::Uri(uri),
        }
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
    inner: RawAddress,
    _phantom: std::marker::PhantomData<P>,
}

#[cfg(feature = "schemars")]
impl<P: ListenerPort> schemars::JsonSchema for AdvertisedAddress<P> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("AdvertisedAddress-{}", P::NAME).into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        format!("{}::AdvertisedAddress[{}]", std::module_path!(), P::NAME).into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "title": "advertised address",
            "description": format!(
            "An externally accessible URI address for {}. This can be set to unix:restate-data/{} \
                to advertise the automatically created unix-socket instead of using tcp if needed",
            P::NAME,
            P::UDS_NAME
        ),
            "examples": [format!("http//127.0.0.1:{}/", P::DEFAULT_PORT), "https://my-host/", format!("unix:/data/restate-data/{}", P::UDS_NAME)]
        })
    }
}

#[cfg(feature = "utoipa-schema")]
mod utoipa_schema {
    use crate::net::address::{AdvertisedAddress, ListenerPort};
    use std::borrow::Cow;
    use utoipa::__dev::ComposeSchema;
    use utoipa::ToSchema;
    use utoipa::openapi::{Object, RefOr, Schema, Type};

    impl<P: ListenerPort> ToSchema for AdvertisedAddress<P> {
        // This is only the prefix of the OpenAPI component name. Because `AdvertisedAddress` is
        // generic, utoipa appends the schema name of `P` when it registers the component, e.g.
        // `AdvertisedAddress_HttpIngressPort`. Every port therefore gets its own component, which
        // is needed because the description and examples in `compose` depend on `P`. The name
        // contains no dashes, which client generators reject.
        //
        // To expose an `AdvertisedAddress<P>` for a new port `P` in the Admin API, derive
        // `utoipa::ToSchema` for `P`, as `HttpIngressPort` does.
        fn name() -> Cow<'static, str> {
            "AdvertisedAddress".into()
        }
    }

    impl<P: ListenerPort> ComposeSchema for AdvertisedAddress<P> {
        fn compose(_new_generics: Vec<RefOr<Schema>>) -> RefOr<Schema> {
            Schema::Object(Object::builder().schema_type(Type::String).title(Some("advertised address")).description(Some(format!(
                "An externally accessible URI address for {}. This can be set to unix:restate-data/{} \
                to advertise the automatically created unix-socket instead of using tcp if needed",
                P::NAME,
                P::UDS_NAME
            ))).examples(vec![
                format!("http//127.0.0.1:{}/", P::DEFAULT_PORT),
                "https://my-host/".to_owned(),
                format!("unix:/data/restate-data/{}", P::UDS_NAME),
            ]).build()).into()
        }
    }
}

impl<P: ListenerPort> Default for AdvertisedAddress<P> {
    fn default() -> Self {
        Self {
            inner: RawAddress::Uri(
                format!("http://127.0.0.1:{}", P::DEFAULT_PORT)
                    .parse()
                    .unwrap(),
            ),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<P: ListenerPort> AdvertisedAddress<P> {
    pub fn derive_from_bind_address(
        address: SocketAddress,
        advertised_host: Option<&AdvertisedHost>,
        tls: bool,
    ) -> Self {
        let scheme = if tls { "https" } else { "http" };
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
                let hostname = advertised_host.map_or_else(routable_ip, AdvertisedHost::as_str);
                PeerNetAddress::Http(host_uri(scheme, hostname, address.port()).expect("valid uri"))
            }
            // it's a UDS address, we'll use the path.
            SocketAddress::Uds(path) => PeerNetAddress::Uds(path),
            SocketAddress::Anonymous => {
                // In case this is an anonymous unix-socket, we'll fallback to a generic
                // localhost-based address without a port. The assumption is the caller
                // will proxy their request through the unix-socket and the host+scheme
                // part of the URI will be ignored by the server.
                PeerNetAddress::Http(format!("{scheme}://localhost").parse().expect("valid uri"))
            }
        };

        Self {
            inner: inner.into(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn new_uds(path: PathBuf) -> Self {
        Self {
            inner: PeerNetAddress::Uds(path).into(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_node_base_dir(dir: &Path) -> Self {
        Self::new_uds(dir.join(P::UDS_NAME))
    }

    /// The URI form of this address; `None` for Unix sockets and other path forms.
    pub fn uri(&self) -> Option<&Uri> {
        match &self.inner {
            RawAddress::Uri(uri) => Some(uri),
            RawAddress::Uds { .. } | RawAddress::UnknownPath(_) => None,
        }
    }

    /// Whether Restate versions without `scheme:/path` support can decode this address.
    ///
    /// Those versions fail to decode a whole `NodesConfiguration` that contains such an address,
    /// so it must stay out of the scalar addresses published there until the compatibility
    /// floor includes this support.
    pub fn decodes_in_older_versions(&self) -> bool {
        match &self.inner {
            RawAddress::Uri(_) => true,
            RawAddress::Uds { spelling, .. } => *spelling == UdsSpelling::Unix,
            RawAddress::UnknownPath(_) => false,
        }
    }

    /// Validates and transforms the address into a [`PeerNetAddress`].
    ///
    /// This will validate if the unix-socket is accessible, and will rewrite the URI
    /// to be compliant with the listener port.
    ///
    /// NOTE: We don't perform this transformation and validation when parsing
    /// the input string is to acheive two goals:
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
            RawAddress::Uri(uri) => Self::parse_uri(uri),
            RawAddress::Uds { spelling, path } => {
                let scheme = match spelling {
                    UdsSpelling::Unix => AddressScheme::Unix,
                    UdsSpelling::GrpcUnix => AddressScheme::GrpcUnix,
                };
                if !P::SCHEMES.contains(&scheme) {
                    anyhow::bail!("'{spelling}:' addresses are not supported for {}", P::NAME);
                }
                Ok(PeerNetAddress::Uds(path))
            }
            RawAddress::UnknownPath(raw) => {
                anyhow::bail!("Unsupported address scheme in '{raw}'")
            }
        }
    }

    /// Ensures that the address is well formed and performs any necessary transformation.
    pub fn into_well_formed(self) -> Result<Self, anyhow::Error> {
        Ok(Self {
            inner: self.into_address()?.into(),
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
        // First, validate that the scheme is empty or one this port accepts.
        let scheme = match parts.scheme.as_ref().map(|scheme| scheme.as_str()) {
            None | Some("http") => AddressScheme::Http,
            Some("https") => AddressScheme::Https,
            Some(other) => anyhow::bail!("Unsupported URI scheme '{other}'"),
        };
        if !P::SCHEMES.contains(&scheme) {
            anyhow::bail!("{scheme:?} addresses are not supported for {}", P::NAME);
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

    /// Should used carefully, this makes one address from a given listener port act
    /// as if it was another listener port. Used when we want to fallback from one address type to
    /// another.
    pub(crate) fn coerce<T: ListenerPort>(self) -> AdvertisedAddress<T> {
        AdvertisedAddress {
            inner: self.inner,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl AdvertisedAddress<FabricPort> {
    /// This address as a control address. Control RPCs are served on the fabric listener, so
    /// every fabric address also reaches them; this is how the control address of a node that
    /// configured none is derived.
    pub fn into_control(self) -> AdvertisedAddress<ControlPort> {
        self.coerce()
    }
}

impl<P: ListenerPort> FromStr for AdvertisedAddress<P> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        let uds = |spelling, path| -> Result<RawAddress, anyhow::Error> {
            Ok(RawAddress::Uds {
                spelling,
                path: parse_uds(path)?,
            })
        };

        let inner = if let Some(path) = trimmed.strip_prefix("unix:") {
            uds(UdsSpelling::Unix, path)?
        } else if let Some(path) = trimmed.strip_prefix("grpc+unix:") {
            uds(UdsSpelling::GrpcUnix, path)?
        } else if trimmed.is_empty() {
            return Err(anyhow::anyhow!("address cannot be empty"));
        } else {
            match parse_uri(trimmed) {
                Ok(uri) => RawAddress::Uri(uri),
                Err(_) if is_absolute_path_form(trimmed) => RawAddress::UnknownPath(trimmed.into()),
                Err(err) => return Err(err),
            }
        };

        Ok(Self {
            inner,
            _phantom: std::marker::PhantomData,
        })
    }
}

/// Whether `s` has the shape `scheme:/absolute/path`, with an RFC 3986 scheme and a path that
/// starts with a single `/`. A `//` after the colon starts an authority, which is the URI shape.
fn is_absolute_path_form(s: &str) -> bool {
    let Some((scheme, rest)) = s.split_once(':') else {
        return false;
    };
    let mut chars = scheme.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        && rest.starts_with('/')
        && !rest.starts_with("//")
}

/// A host to advertise in place of the inferred one: a DNS name, an IPv4 address, or an IPv6
/// address in brackets. It never carries a scheme, user info, port or path, which an advertised
/// address sets instead.
///
/// Parsing validates it, so the advertised address derived from it is always a valid URI, and
/// DNS names follow the rule rustls applies to TLS server names, so TLS peers can dial it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    derive_more::Display,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schemars",
    schemars(
        transparent,
        description = "A host name, an IPv4 address, or an IPv6 address in brackets. It carries \
                       no scheme, port or path; set those with an advertised address instead.",
        example = &"node-7.example.com",
        example = &"10.1.7.3",
        example = &"[2001:db8::7]",
    )
)]
pub struct AdvertisedHost(String);

impl AdvertisedHost {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AdvertisedHost {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let host = s.trim();
        // `http::Uri` checks only the syntax of an authority, so `[gggg::1]` and `bad$name` pass
        // it. Require an IP address or a DNS name as rustls defines it, so TLS peers can dial the
        // derived address, and also check the longest URI that `derive_from_bind_address` can
        // build from this host, through the same helper, so that derivation cannot fail.
        let is_host = match host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
            Some(ipv6) => ipv6.parse::<Ipv6Addr>().is_ok(),
            None => host.parse::<Ipv4Addr>().is_ok() || validate_dns_name(host.as_bytes()).is_ok(),
        };
        match host_uri("https", host, u16::MAX) {
            Ok(uri) if is_host && uri.host() == Some(host) => Ok(Self(host.to_owned())),
            _ => anyhow::bail!(
                "'{s}' is not a host name or IP address. Write IPv6 addresses in brackets, such \
                 as [2001:db8::7], and use an advertised address to set a scheme, port or path"
            ),
        }
    }
}

/// The URI an advertised host and a bound port turn into.
fn host_uri(scheme: &str, host: &str, port: u16) -> Result<Uri, http::uri::InvalidUri> {
    format!("{scheme}://{host}:{port}").parse()
}

/// Mirrors the rustls error type so that [`validate_dns_name`] needs no edits.
struct InvalidDnsNameError;

/// Whether `input` is a DNS name as rustls defines it, so that every host [`AdvertisedHost`]
/// accepts is also a TLS server name.
///
/// Copied verbatim from `validate` in rustls-pki-types 1.15.1, `src/server_name.rs`
/// (<https://github.com/rustls/pki-types>, MIT OR Apache-2.0). Keep it identical to that
/// function rather than editing it, so the two cannot disagree; the parity test in
/// `restate_core::network::net_util` checks the agreement.
const fn validate_dns_name(input: &[u8]) -> Result<(), InvalidDnsNameError> {
    enum LabelState {
        /// The current label is still empty
        Start,
        /// The current label contains at least one non-digit
        HasLetter,
        /// The current label is non-empty and all-numeric so far
        AllNumeric,
        /// The last character was a hyphen
        EndsInHyphen,
    }

    /// "Labels must be 63 characters or less."
    const MAX_LABEL_LENGTH: usize = 63;

    /// https://devblogs.microsoft.com/oldnewthing/20120412-00/?p=7873
    const MAX_NAME_LENGTH: usize = 253;

    if input.is_empty() || input.len() > MAX_NAME_LENGTH {
        return Err(InvalidDnsNameError);
    }

    let mut start = 0;
    let mut state = LabelState::Start;
    let mut idx = 0;
    loop {
        let ch = match idx < input.len() {
            true => Some(input[idx]),
            false => None,
        };

        match ch {
            Some(b'a'..=b'z' | b'A'..=b'Z' | b'_') => state = LabelState::HasLetter,
            Some(b'0'..=b'9') => {
                state = match state {
                    LabelState::Start | LabelState::AllNumeric => LabelState::AllNumeric,
                    LabelState::HasLetter | LabelState::EndsInHyphen => LabelState::HasLetter,
                }
            }
            Some(b'.') | None => {
                let len = idx - start;
                if len == 0 || len > MAX_LABEL_LENGTH || matches!(state, LabelState::EndsInHyphen) {
                    return Err(InvalidDnsNameError);
                }

                // Break on a trailing dot as well as on the end of input, keeping
                // `state` intact for the final all-numeric check below.
                if idx + 1 >= input.len() {
                    break;
                }

                idx += 1;
                start = idx;
                state = LabelState::Start;
                continue;
            }
            Some(b'-') => match idx == start {
                true => return Err(InvalidDnsNameError),
                false => state = LabelState::EndsInHyphen,
            },
            _ => return Err(InvalidDnsNameError),
        }

        idx += 1;
    }

    // The final label (whether or not the name has a trailing dot) must
    // not be all-numeric, to avoid confusion with an IPv4 address.
    match state {
        LabelState::AllNumeric => Err(InvalidDnsNameError),
        _ => Ok(()),
    }
}

/// A helper function that attempts to derive the public routable IP address of
/// the local machine. Tries IPv6 first, then falls back to IPv4 for IPv4-only
/// environments. Falls back to `127.0.0.1` if all guessing fails.
fn guess_my_routable_ip() -> &'static str {
    static MY_IP: OnceLock<Option<String>> = OnceLock::new();
    static LOCALHOST: &str = "127.0.0.1";
    // Guesses the publicly reachable IP address by creating a datagram socket
    // and then reading the source address. This does not send any packets, but
    // it will use the system's routing table to determine the local interface
    // that is used to reach the default gateway.
    //
    // We try IPv6 first (connect to 2606:4700:4700::1111), then IPv4 (connect
    // to 1.1.1.1) for IPv4-only environments. We fall back to `127.0.0.1` if
    // both fail.
    MY_IP
        .get_or_init(|| guess_routable_ipv6().or_else(guess_routable_ipv4))
        .as_deref()
        .unwrap_or(LOCALHOST)
}

fn guess_routable_ipv4() -> Option<String> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("1.1.1.1:80").ok()?;
    Some(socket.local_addr().ok()?.ip().to_string())
}

fn guess_routable_ipv6() -> Option<String> {
    let socket = std::net::UdpSocket::bind("[::]:0").ok()?;
    socket.connect("[2606:4700:4700::1111]:80").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    // IPv6 addresses in URIs must be wrapped in brackets per RFC 3986
    Some(format!("[{ip}]"))
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
    use super::*;

    #[test]
    fn from_parts_defaults_to_ipv6_unspecified() {
        let addr = BindAddress::<ControlPort>::from_parts(None, None, false);
        assert_eq!(addr.inner.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.inner.port(), ControlPort::DEFAULT_PORT);

        // Explicit IPv4 still works
        let addr = BindAddress::<ControlPort>::from_parts(
            Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
            None,
            false,
        );
        assert_eq!(addr.inner.ip(), IpAddr::V4(Ipv4Addr::UNSPECIFIED));

        // Random port
        let addr = BindAddress::<ControlPort>::from_parts(None, None, true);
        assert_eq!(addr.inner.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.inner.port(), 0);
    }

    #[test]
    fn parse_bind_address() {
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
    fn parse_bind_address_with_default_port() {
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
    fn parse_bind_address_invalid() {
        let input = "unsupported:address";
        let result = input.parse::<BindAddress<ControlPort>>();
        assert!(
            result.is_err(),
            "Expected an error for invalid bind address"
        );
    }

    #[test]
    fn parse_advertised_address_host_port() {
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

    #[test]
    fn peer_net_address_is_tls() {
        // https scheme is TLS
        let addr: AdvertisedAddress<FabricPort> = "https://10.0.0.1:5122".parse().unwrap();
        let peer = addr.into_address().unwrap();
        assert!(peer.is_tls());

        // http scheme is not TLS
        let addr: AdvertisedAddress<FabricPort> = "http://10.0.0.1:5122".parse().unwrap();
        let peer = addr.into_address().unwrap();
        assert!(!peer.is_tls());

        // bare host (defaults to http) is not TLS
        let addr: AdvertisedAddress<FabricPort> = "10.0.0.1:5122".parse().unwrap();
        let peer = addr.into_address().unwrap();
        assert!(!peer.is_tls());

        // UDS is not TLS
        let addr: AdvertisedAddress<FabricPort> = "unix:/tmp/fabric.sock".parse().unwrap();
        let peer = addr.into_address().unwrap();
        assert!(!peer.is_tls());
    }

    #[test]
    fn derive_from_bind_address_tls_scheme() {
        let socket = SocketAddress::Socket("192.168.1.1:5122".parse().unwrap());

        // Without TLS — should produce http://
        let addr =
            AdvertisedAddress::<FabricPort>::derive_from_bind_address(socket.clone(), None, false);
        let peer = addr.into_address().unwrap();
        assert!(!peer.is_tls());
        assert!(peer.to_string().starts_with("http://"));

        // With TLS — should produce https://
        let addr = AdvertisedAddress::<FabricPort>::derive_from_bind_address(socket, None, true);
        let peer = addr.into_address().unwrap();
        assert!(peer.is_tls());
        assert!(peer.to_string().starts_with("https://"));
    }

    #[test]
    fn address_shapes_round_trip_and_dial_per_port() {
        // input, as dialed on a fabric port, dialable on the admin port, and whether versions
        // without `scheme:/path` support can decode it
        let cases = [
            ("https://h:5122", Some("https://h:5122/"), true, true),
            ("localhost:5122", Some("http://localhost:5122/"), true, true),
            ("unix:/f", Some("unix:/f"), true, true),
            ("grpc+unix:/f", Some("unix:/f"), false, false),
            ("restate+tcp://h:5222", None, false, true),
            ("restate+unix:/f", None, false, false),
            ("x-future.v2:/a/b", None, false, false),
        ];
        for (input, dialed, admin_dials, legacy) in cases {
            let fabric: AdvertisedAddress<FabricPort> = input.parse().unwrap();
            // Serialization goes through `Display`, so this is the metadata round trip.
            let reparsed = fabric.to_string().parse::<AdvertisedAddress<FabricPort>>();
            assert_eq!(reparsed.unwrap(), fabric, "{input}");
            if !input.contains("://") {
                assert_eq!(fabric.to_string(), input);
            }
            assert_eq!(fabric.decodes_in_older_versions(), legacy, "{input}");
            let fabric_dialed = fabric.into_address().map(|a| a.to_string());
            assert_eq!(fabric_dialed.ok().as_deref(), dialed, "{input}");
            let admin = input.parse::<AdvertisedAddress<AdminPort>>().unwrap();
            assert_eq!(admin.into_address().is_ok(), admin_dials, "{input}");
        }

        // Malformed input keeps failing at parse time; there is no catch-all.
        for input in ["", "  ", "vsock:3:5122", "https://bücher.example", "1x:/a"] {
            let parsed = input.parse::<AdvertisedAddress<FabricPort>>();
            assert!(parsed.is_err(), "{input}");
        }
    }

    #[test]
    fn advertised_host_accepts_only_bare_hosts() {
        let label_63 = "a".repeat(63);
        let name_253 = format!("{label_63}.{label_63}.{label_63}.{}", "a".repeat(61));
        assert_eq!(name_253.len(), 253);
        for input in [
            "node-7.example.com",
            "node_1",
            "host.",
            "10.1.7.3",
            "[2001:db8::7]",
            label_63.as_str(),
            name_253.as_str(),
        ] {
            let host: AdvertisedHost = input.parse().unwrap();
            assert_eq!(host.as_str(), input);
        }
        assert_eq!(
            " padded.example.com "
                .parse::<AdvertisedHost>()
                .unwrap()
                .as_str(),
            "padded.example.com"
        );

        // The first group used to panic or silently advertise a wrong address. The second group
        // parses as a URI host but is neither a DNS name nor an IP address, so TLS peers could
        // not dial it.
        let label_64 = "a".repeat(64);
        let name_254 = format!("{name_253}a");
        for input in [
            "",
            "bad host",
            "host:8443",
            "2001:db8::7",
            "user@host",
            "host/rpc",
            "http://h",
            "[gggg::1]",
            "[10.0.0.1]",
            "bad$name",
            "a-.b",
            "-a.b",
            "a..b",
            "host.123",
            label_64.as_str(),
            name_254.as_str(),
        ] {
            assert!(input.parse::<AdvertisedHost>().is_err(), "{input}");
        }

        let host = "[2001:db8::7]".parse().unwrap();
        let socket = SocketAddress::Socket("[::]:5122".parse().unwrap());
        let address =
            AdvertisedAddress::<FabricPort>::derive_from_bind_address(socket, Some(&host), false);
        assert_eq!(address.to_string(), "http://[2001:db8::7]:5122/");
    }
}
