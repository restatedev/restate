// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod codec;
pub mod connect_opts;
pub mod log_server;
pub mod metadata;
pub mod node;
pub mod partition_processor;
pub mod partition_processor_manager;
pub mod remote_query_scanner;
pub mod replicated_loglet;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Error};
use http::Uri;

use crate::config::InvalidConfigurationError;
pub use crate::protobuf::common::ProtocolVersion;
pub use crate::protobuf::common::ServiceTag;

pub static MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::V1;
pub static CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::V2;

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Hash,
    derive_more::Display,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub enum AdvertisedAddress {
    /// Unix domain socket
    #[display("unix:{}", _0.display())]
    Uds(PathBuf),
    /// Hostname or host:port pair
    #[display("{}", _0)]
    Http(Uri),
}

impl FromStr for AdvertisedAddress {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "" => Err(anyhow::anyhow!("Advertised address cannot be empty")),
            address if address.starts_with("unix:") => {
                parse_uds(&address[5..]).map(AdvertisedAddress::Uds)
            }
            address => parse_http(address),
        }
    }
}

fn parse_uds(s: &str) -> Result<PathBuf, Error> {
    s.parse::<PathBuf>()
        .with_context(|| format!("Failed to parse Unix domain socket path: '{s}'"))
}

fn parse_http(s: &str) -> Result<AdvertisedAddress, Error> {
    let uri = s
        .parse::<Uri>()
        .with_context(|| format!("Invalid URI format: '{s}'"))?;

    let mut parts = uri.into_parts();
    // default to http if scheme is missing
    if parts.scheme.is_none() {
        if let Some(authority) = &parts.authority {
            if authority.port().is_none() {
                // can not update just the port in place
                parts.authority = Some(format!("{authority}:5122").parse()?);
            }
        }
        parts.scheme = Some(http::uri::Scheme::HTTP);
    } else if parts
        .authority
        .as_ref()
        .is_some_and(|authority| authority.port().is_none())
    {
        let scheme = parts.scheme.as_ref().expect("some").as_str();
        let port = match scheme {
            "http" => 80,
            "https" => 443,
            _ => anyhow::bail!("Unsupported URI scheme '{}'", scheme),
        };
        // logging is not yet configured
        eprintln!(
            "The advertised address is configured with scheme {scheme}:// and no explicit port, implying port {port}",
        );
        parts.authority =
            Some(format!("{}:{port}", parts.authority.as_ref().expect("some")).parse()?);
    }

    if parts.path_and_query.is_none() {
        parts.path_and_query = Some(http::uri::PathAndQuery::from_str("/")?);
    }
    let uri = Uri::from_parts(parts)?;
    match uri.scheme_str() {
        Some("http") | Some("https") => Ok(AdvertisedAddress::Http(uri)),
        Some(other) => Err(anyhow::anyhow!("Unsupported URI scheme '{}'", other)),
        None => unreachable!(),
    }
}

impl AdvertisedAddress {
    /// Derives a `BindAddress` based on the advertised address
    pub fn derive_bind_address(&self) -> Result<BindAddress, InvalidConfigurationError> {
        match self {
            AdvertisedAddress::Http(uri) => {
                let ip = if uri.host().unwrap_or("").contains(':') {
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED)
                } else {
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                };

                let default_port = 5122;
                let port = match (uri.scheme_str(), uri.port_u16()) {
                    (None, None) => default_port,
                    (Some("http"), Some(port)) => port,
                    (Some("https"), _) => {
                        return Err(InvalidConfigurationError::DeriveBindAddress(
                            "Restate does not support HTTPS. If you are using a TLS-terminating \
                            reverse proxy, please set bind-address explicitly."
                                .to_owned(),
                        ));
                    }
                    (_, _) => unreachable!(), // parse_http rejects other protocols, and sets an explicit port
                };

                Ok(BindAddress::Socket(SocketAddr::new(ip, port)))
            }
            AdvertisedAddress::Uds(path) => Ok(BindAddress::Uds(path.clone())),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Eq,
    PartialEq,
    Hash,
    derive_more::Display,
    serde_with::SerializeDisplay,
    serde_with::DeserializeFromStr,
)]
pub enum BindAddress {
    /// Unix domain socket
    #[display("unix:{}", _0.display())]
    Uds(PathBuf),
    /// Socket address (IP and port).
    #[display("{}", _0)]
    Socket(SocketAddr),
}

impl FromStr for BindAddress {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.strip_prefix("unix:") {
            Some(path) => parse_uds(path).map(BindAddress::Uds),
            None => s
                .parse::<SocketAddr>()
                .map(BindAddress::Socket)
                .map_err(|e| Error::new(e).context("Failed to parse socket address")),
        }
    }
}

pub trait Service: Send + Unpin + 'static {
    const TAG: ServiceTag;
}

pub trait RpcRequest: codec::WireEncode + codec::WireDecode + Send + 'static {
    const TYPE: &str;
    type Service: Service;
    type Response: RpcResponse;
}

pub trait RpcResponse: codec::WireEncode + codec::WireDecode + Unpin + Send {
    type Service: Service;
}

pub trait WatchRequest: codec::WireEncode + codec::WireDecode + Send + 'static {
    const TYPE: &str;
    type Service: Service;
    type Response: WatchResponse;
}

pub trait WatchResponse: codec::WireEncode + codec::WireDecode + Unpin + Send {
    type Service: Service;
}

pub trait UnaryMessage: codec::WireEncode + codec::WireDecode + Send + 'static {
    const TYPE: &str;
    type Service: Service;
}

/// Implements default wire codec for a type
/// - Message type
///
/// Example:
/// ```ignore
///   default_wire_codec!(IngressMessage);
/// ```
macro_rules! default_wire_codec {
    (
        $message:ty
    ) => {
        impl $crate::net::codec::WireEncode for $message {
            fn encode_to_bytes(
                &self,
                _protocol_version: $crate::net::ProtocolVersion,
            ) -> Result<::bytes::Bytes, $crate::net::codec::EncodeError> {
                Ok(::bytes::Bytes::from(
                    $crate::net::codec::encode_as_flexbuffers(self),
                ))
            }
        }

        impl $crate::net::codec::WireDecode for $message {
            type Error = anyhow::Error;

            fn try_decode(
                buf: impl bytes::Buf,
                protocol_version: $crate::net::ProtocolVersion,
            ) -> Result<Self, anyhow::Error>
            where
                Self: Sized,
            {
                $crate::net::codec::decode_as_flexbuffers(buf, protocol_version)
            }
        }
    };
}

/// Implements bilrost wire codec for a type
/// - Message type
///
/// Example:
/// ```ignore
///   bilrost_wire_codec_with_v1_fallback!(IngressMessage);
/// ```
/// This codec will fallback automatically to flexbuffer
/// if remote beer is on V1.
#[allow(unused_macros)]
macro_rules! bilrost_wire_codec_with_v1_fallback {
    (
        $message:ty
    ) => {
        impl $crate::net::codec::WireEncode for $message {
            fn encode_to_bytes(
                &self,
                protocol_version: $crate::net::ProtocolVersion,
            ) -> Result<::bytes::Bytes, $crate::net::codec::EncodeError> {
                match protocol_version {
                    $crate::net::ProtocolVersion::Unknown => {
                        unreachable!("unknown protocol version should never be set")
                    }
                    $crate::net::ProtocolVersion::V1 => Ok(::bytes::Bytes::from(
                        $crate::net::codec::encode_as_flexbuffers(self),
                    )),
                    _ => {
                        if protocol_version < $crate::net::ProtocolVersion::V2 {
                            Err($crate::net::codec::EncodeError::IncompatibleVersion {
                                type_tag: stringify!($message),
                                min_required: $crate::net::ProtocolVersion::V2,
                                actual: protocol_version,
                            })
                        } else {
                            Ok($crate::net::codec::encode_as_bilrost(self))
                        }
                    }
                }
            }
        }

        impl $crate::net::codec::WireDecode for $message {
            type Error = anyhow::Error;

            fn try_decode(
                buf: impl bytes::Buf,
                protocol_version: $crate::net::ProtocolVersion,
            ) -> Result<Self, anyhow::Error>
            where
                Self: Sized,
            {
                match protocol_version {
                    $crate::net::ProtocolVersion::Unknown => {
                        ::anyhow::bail!("Unknown protocol version")
                    }
                    $crate::net::ProtocolVersion::V1 => {
                        $crate::net::codec::decode_as_flexbuffers(buf, protocol_version)
                    }
                    _ => $crate::net::codec::decode_as_bilrost(buf, protocol_version),
                }
            }
        }
    };
}

/// Implements bilrost wire codec for a type
/// - Message type
///
/// Example:
/// ```ignore
///   bilrost_wire_codec!(IngressMessage);
/// ```
#[allow(unused_macros)]
macro_rules! bilrost_wire_codec {
    (
        $message:ty
    ) => {
        impl $crate::net::codec::WireEncode for $message {
            fn encode_to_bytes(
                &self,
                protocol_version: $crate::net::ProtocolVersion,
            ) -> Result<::bytes::Bytes, $crate::net::codec::EncodeError> {
                if protocol_version < $crate::net::ProtocolVersion::V2 {
                    Err($crate::net::codec::EncodeError::IncompatibleVersion {
                        type_tag: stringify!($message),
                        min_required: $crate::net::ProtocolVersion::V2,
                        actual: protocol_version,
                    })
                } else {
                    Ok($crate::net::codec::encode_as_bilrost(self))
                }
            }
        }

        impl $crate::net::codec::WireDecode for $message {
            type Error = anyhow::Error;

            fn try_decode(
                buf: impl bytes::Buf,
                protocol_version: $crate::net::ProtocolVersion,
            ) -> Result<Self, anyhow::Error>
            where
                Self: Sized,
            {
                $crate::net::codec::decode_as_bilrost(buf, protocol_version)
            }
        }
    };
}

/// to define a service we need
/// - Service type
/// - Service Tag
///
/// Example:
/// ```ignore
///   define_service! {
///       @service = IngressService,
///       @tag = ServiceTag::Ingress,
///   }
/// ```
macro_rules! define_service {
    (
        @service = $service:ty,
        @tag = $tag:expr,
    ) => {
        impl $crate::net::Service for $service {
            const TAG: $crate::net::ServiceTag = $tag;
        }
    };
}

/// to define a unary message, we need
/// - Message type
/// - service type
///
/// Example:
/// ```ignore
///   define_unary_message! {
///       @message = IngressMessage,
///       @service = IngressService,
///   }
/// ```
macro_rules! define_unary_message {
    (
        @message = $message:ty,
        @service = $service:ty,
    ) => {
        impl $crate::net::UnaryMessage for $message {
            const TYPE: &str = stringify!($message);
            type Service = $service;
        }
    };
}

/// to define an RPC, we need
/// - Request type
/// - request service tag
/// - Service type
///
/// Example:
/// ```ignore
///   define_rpc! {
///       @request = AttachRequest,
///       @response = AttachResponse,
///       @service = ClusterControllerService,
///   }
/// ```
macro_rules! define_rpc {
    (
        @request = $request:ty,
        @response = $response:ty,
        @service = $service:ty,
    ) => {
        impl $crate::net::RpcRequest for $request {
            const TYPE: &str = stringify!($request);
            type Response = $response;
            type Service = $service;
        }

        impl $crate::net::RpcResponse for $response {
            type Service = $service;
        }
    };
}

#[allow(unused_imports)]
use {
    bilrost_wire_codec, bilrost_wire_codec_with_v1_fallback, default_wire_codec, define_rpc,
    define_service, define_unary_message,
};

#[cfg(test)]
mod tests {
    use super::*;

    use std::{net::Ipv6Addr, str::FromStr};

    #[test]
    fn test_parse_empty_input() {
        let result = AdvertisedAddress::from_str("");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Advertised address cannot be empty"
        );
    }

    #[test]
    fn test_parse_valid_uds() {
        let input = "unix:/path/to/socket";
        let addr = input
            .parse::<AdvertisedAddress>()
            .expect("Failed to parse UDS address");

        match addr {
            AdvertisedAddress::Uds(path) => assert_eq!(path, PathBuf::from("/path/to/socket")),
            _ => panic!("Expected Uds variant"),
        }
    }

    #[test]
    fn test_parse_valid_http_uri() {
        let result = AdvertisedAddress::from_str("http://localhost:8080");
        assert!(result.is_ok());
        if let AdvertisedAddress::Http(uri) = result.unwrap() {
            assert_eq!(uri.to_string(), "http://localhost:8080/");
        } else {
            panic!("Expected Http variant");
        }
    }

    #[test]
    fn test_parse_missing_scheme() {
        let result = AdvertisedAddress::from_str("localhost:8080");
        assert!(result.is_ok());
        if let AdvertisedAddress::Http(uri) = result.unwrap() {
            assert_eq!(uri.to_string(), "http://localhost:8080/");
        } else {
            panic!("Expected Http variant");
        }
    }

    #[test]
    fn test_parse_missing_scheme_with_path() {
        let result = AdvertisedAddress::from_str("localhost/data");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid URI format: 'localhost/data'")
        );
    }

    #[test]
    fn test_parse_unsupported_scheme() {
        let result = AdvertisedAddress::from_str("ftp://localhost");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unsupported URI scheme 'ftp'")
        );
    }

    #[test]
    fn test_parse_invalid_address() {
        let input = "";
        let result = input.parse::<AdvertisedAddress>();
        assert!(result.is_err(), "Expected an error for empty input");

        let input = "ftp://localhost:8080";
        let result = input.parse::<AdvertisedAddress>();
        assert!(
            result.is_err(),
            "Expected an error for unsupported URI scheme"
        );
    }

    #[test]
    fn test_derive_bind_address_http() {
        let input = "http://localhost:8080";
        let addr = input
            .parse::<AdvertisedAddress>()
            .expect("Failed to parse HTTP address");
        let bind_addr = addr.derive_bind_address().unwrap();

        match bind_addr {
            BindAddress::Socket(socket_addr) => {
                assert_eq!(
                    socket_addr,
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8080)
                )
            }
            _ => panic!("Expected Socket variant"),
        }
    }

    #[test]
    fn test_derive_bind_address_fallback_port() {
        let advertised_address = AdvertisedAddress::from_str("http://example.com").unwrap();
        let bind_address = advertised_address.derive_bind_address().unwrap();

        match bind_address {
            BindAddress::Socket(socket_addr) => {
                assert_eq!(socket_addr.port(), 80, "Expected port 80 for fallback");
                assert_eq!(
                    socket_addr.ip(),
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    "Expected IPv4 unspecified address"
                );
            }
            _ => panic!("Expected BindAddress::Socket"),
        }
    }

    #[test]
    fn derive_bind_address_host_only_no_protocol_uses_default_port() {
        let advertised_address = AdvertisedAddress::from_str("example.com").unwrap();
        let bind_address = advertised_address.derive_bind_address().unwrap();

        match bind_address {
            BindAddress::Socket(socket_addr) => {
                assert_eq!(socket_addr.port(), 5122, "Expected port 5122 for fallback");
                assert_eq!(
                    socket_addr.ip(),
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    "Expected IPv4 unspecified address"
                );
            }
            _ => panic!("Expected BindAddress::Socket"),
        }
    }

    #[test]
    fn derive_bind_address_https_not_possible() {
        // we only call this if bind-address is unset; so it's still possible to advertise HTTPS, we
        // just can't infer the bind address from such a URL
        let advertised_address = AdvertisedAddress::from_str("https://example.com").unwrap();
        let bind_address = advertised_address.derive_bind_address();
        assert!(bind_address.is_err())
    }

    #[test]
    fn test_derive_bind_address_uds() {
        let input = "unix:/path/to/socket";
        let addr = input
            .parse::<AdvertisedAddress>()
            .expect("Failed to parse UDS address");
        let bind_addr = addr.derive_bind_address().unwrap();

        match bind_addr {
            BindAddress::Uds(path) => assert_eq!(path, PathBuf::from("/path/to/socket")),
            _ => panic!("Expected Uds variant"),
        }
    }
    #[test]
    fn test_derive_bind_address_ipv6() {
        // Create an IPv6 advertised address
        let address = "http://[::1]:8080"; // IPv6 localhost with port 8080
        let advertised_address =
            AdvertisedAddress::from_str(address).expect("Failed to parse IPv6 URI");

        // Derive the bind address
        let bind_address = advertised_address.derive_bind_address().unwrap();

        // Check that it matches the expected IPv6 bind address
        match bind_address {
            BindAddress::Socket(socket_addr) => {
                assert_eq!(socket_addr.ip(), IpAddr::V6(Ipv6Addr::UNSPECIFIED));
                assert_eq!(socket_addr.port(), 8080);
            }
            _ => panic!("Expected BindAddress::Socket with IPv6, got {bind_address:?}"),
        }
    }

    #[test]
    fn test_parse_bind_address_socket() {
        let input = "127.0.0.1:8080";
        let addr = input
            .parse::<BindAddress>()
            .expect("Failed to parse Socket address");

        match addr {
            BindAddress::Socket(socket_addr) => {
                assert_eq!(socket_addr, "127.0.0.1:8080".parse().unwrap())
            }
            _ => panic!("Expected Socket variant"),
        }
    }

    #[test]
    fn test_parse_bind_address_uds() {
        let input = "unix:/path/to/socket";
        let addr = input
            .parse::<BindAddress>()
            .expect("Failed to parse UDS address");

        match addr {
            BindAddress::Uds(path) => assert_eq!(path, PathBuf::from("/path/to/socket")),
            _ => panic!("Expected Uds variant"),
        }
    }

    #[test]
    fn test_parse_bind_address_invalid() {
        let input = "unsupported:address";
        let result = input.parse::<BindAddress>();
        assert!(
            result.is_err(),
            "Expected an error for invalid bind address"
        );
    }
}
