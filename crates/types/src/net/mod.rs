// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod cluster_state;
pub mod codec;
mod error;
pub mod log_server;
pub mod metadata;
pub mod node;
pub mod partition_processor;
pub mod partition_processor_manager;
pub mod remote_query_scanner;
pub mod replicated_loglet;

use anyhow::{Context, Error};
// re-exports for convenience
pub use error::*;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

use http::Uri;

use self::codec::{Targeted, WireEncode};
pub use crate::protobuf::common::ProtocolVersion;
pub use crate::protobuf::common::TargetName;

pub static MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::Flexbuffers;
pub static CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::Flexbuffers;

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

    match uri.scheme_str() {
        Some("http") | Some("https") => Ok(AdvertisedAddress::Http(uri)),
        Some(other) => Err(anyhow::anyhow!("Unsupported URI scheme '{}'", other)),
        None => Err(anyhow::anyhow!("Missing URI scheme in: '{}'", s)),
    }
}

impl AdvertisedAddress {
    /// Derives a `BindAddress` based on the advertised address
    pub fn derive_bind_address(&self) -> BindAddress {
        match self {
            AdvertisedAddress::Http(uri) => {
                let port = uri
                    .authority()
                    .and_then(|auth| auth.port_u16())
                    .unwrap_or(80); // HTTP default port is 80 if unspecified

                let ip = if uri.host().unwrap_or("").contains(':') {
                    IpAddr::V6(Ipv6Addr::UNSPECIFIED)
                } else {
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                };

                BindAddress::Socket(SocketAddr::new(ip, port))
            }
            AdvertisedAddress::Uds(path) => BindAddress::Uds(path.clone()),
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
pub trait RpcRequest: Targeted {
    type ResponseMessage: Targeted + WireEncode;
}

// to define a message, we need
// - Message type
// - message target
//
// Example:
// ```
//   define_message! {
//       @message = IngressMessage,
//       @target = TargetName::Ingress,
//   }
// ```
macro_rules! define_message {
    (
        @message = $message:ty,
        @target = $target:expr,
    ) => {
        impl $crate::net::Targeted for $message {
            const TARGET: $crate::net::TargetName = $target;
            fn kind(&self) -> &'static str {
                stringify!($message)
            }
        }

        impl $crate::net::codec::WireEncode for $message {
            fn encode<B: bytes::BufMut>(
                self,
                buf: &mut B,
                protocol_version: $crate::net::ProtocolVersion,
            ) -> Result<(), $crate::net::CodecError> {
                // serialize message into buf
                $crate::net::codec::encode_default(self, buf, protocol_version)
            }
        }

        impl $crate::net::codec::WireDecode for $message {
            fn decode<B: bytes::Buf>(
                buf: &mut B,
                protocol_version: $crate::net::ProtocolVersion,
            ) -> Result<Self, $crate::net::CodecError>
            where
                Self: Sized,
            {
                $crate::net::codec::decode_default(buf, protocol_version)
            }
        }
    };
}

// to define an RPC, we need
// - Request type
// - request target
// - Response type
// - response Target
//
// Example:
// ```
//   define_rpc! {
//       @request = AttachRequest,
//       @response = AttachResponse,
//       @request_target = TargetName::ClusterController,
//       @response_target = TargetName::AttachResponse,
//   }
// ```
macro_rules! define_rpc {
    (
        @request = $request:ty,
        @response = $response:ty,
        @request_target = $request_target:expr,
        @response_target = $response_target:expr,
    ) => {
        impl $crate::net::RpcRequest for $request {
            type ResponseMessage = $response;
        }

        $crate::net::define_message! {
            @message = $request,
            @target = $request_target,
        }

        $crate::net::define_message! {
            @message = $response,
            @target = $response_target,
        }
    };
}

#[allow(unused_imports)]
use {define_message, define_rpc};

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
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Missing URI scheme in: 'localhost:8080'"
        );
    }

    #[test]
    fn test_parse_unsupported_scheme() {
        let result = AdvertisedAddress::from_str("ftp://localhost");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported URI scheme 'ftp'"));
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
        let bind_addr = addr.derive_bind_address();

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
        // Case with no port specified, should fallback to 80
        let advertised_address = AdvertisedAddress::from_str("http://example.com").unwrap();
        let bind_address = advertised_address.derive_bind_address();

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
    fn test_derive_bind_address_uds() {
        let input = "unix:/path/to/socket";
        let addr = input
            .parse::<AdvertisedAddress>()
            .expect("Failed to parse UDS address");
        let bind_addr = addr.derive_bind_address();

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
        let bind_address = advertised_address.derive_bind_address();

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

    #[test]
    fn test_invalid_advertised_address() {
        // Test case for an invalid AdvertisedAddress string
        let result = AdvertisedAddress::from_str("invalid-address");

        // Parsing should fail, resulting in an error
        assert!(result.is_err(), "Expected an error for invalid address");
    }
}
