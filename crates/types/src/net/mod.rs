// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod cluster_controller;
pub mod codec;
mod error;
pub mod ingress;
#[cfg(feature = "replicated-loglet")]
pub mod log_server;
pub mod metadata;
pub mod partition_processor_manager;
#[cfg(feature = "replicated-loglet")]
pub mod replicated_loglet;

use anyhow::Context;
// re-exports for convenience
pub use error::*;

use std::net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr};
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
    /// Hostname or host:port pair, or any unrecognizable string.
    #[display("{}", _0)]
    Http(Uri),
}

impl FromStr for AdvertisedAddress {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().is_empty() {
            return Err(anyhow::anyhow!("Advertised address input cannot be empty"));
        }

        if let Some(stripped_address) = s.strip_prefix("unix:") {
            parse_uds(stripped_address)
        } else {
            parse_http(s)
        }
    }
}

fn parse_uds(s: &str) -> Result<AdvertisedAddress, anyhow::Error> {
    let path = s
        .parse::<PathBuf>()
        .with_context(|| format!("Failed to parse Unix domain socket path: '{}'", s))?;

    if path.exists() {
        Ok(AdvertisedAddress::Uds(path))
    } else {
        Err(anyhow::anyhow!(
            "Unix domain socket path does not exist: '{}'",
            path.display()
        ))
    }
}

fn parse_http(s: &str) -> Result<AdvertisedAddress, anyhow::Error> {
    let uri = s
        .parse::<Uri>()
        .with_context(|| format!("Failed to parse as URI: '{}'", s))?;

    match uri.scheme_str() {
        Some("http") | Some("https") => Ok(AdvertisedAddress::Http(uri)),
        Some(other) => Err(anyhow::anyhow!("Unsupported URI scheme '{}'", other)),
        None => Err(anyhow::anyhow!("Missing URI scheme in: '{}'", s)),
    }
}

impl AdvertisedAddress {
    // Helper function to extract the port from the AdvertisedAddress
    pub fn derive_bind_address(&self) -> Option<BindAddress> {
        match self {
            AdvertisedAddress::Http(uri) => {
                // Try to extract the port from the URI
                if let Some(authority) = uri.authority() {
                    if let Some(port) = authority.port_u16() {
                        // Derive the BindAddress with 0.0.0.0 and the same port
                        let socket_addr =
                            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
                        return Some(BindAddress::Socket(socket_addr));
                    }
                }
                None
            }
            AdvertisedAddress::Uds(path) => {
                // Return the same path as the BindAddress for Uds
                Some(BindAddress::Uds(path.clone()))
            }
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
    /// Socket addr.
    #[display("{}", _0)]
    Socket(SocketAddr),
}

impl FromStr for BindAddress {
    type Err = AddrParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(stripped_address) = s.strip_prefix("unix:") {
            Ok(BindAddress::Uds(
                stripped_address.parse().expect("infallible"),
            ))
        } else {
            // try to parse as a URI
            Ok(BindAddress::Socket(s.parse()?))
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
#[allow(unused_macros)]
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

    use std::str::FromStr;

    #[test]
    fn test_parse_empty_input() {
        let result = AdvertisedAddress::from_str("");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Advertised address input cannot be empty"
        );
    }

    #[test]
    fn test_parse_valid_uds() {
        // Create a temporary directory and use a socket file inside it
        let tmp_dir = tempfile::tempdir().unwrap();
        let socket_path = tmp_dir.path().join("socket");

        // Create the mock UDS file so that the path exists
        std::fs::File::create(&socket_path).unwrap();

        // test with the valid UDS path
        let input = format!("unix:{}", socket_path.display());
        let result = AdvertisedAddress::from_str(&input);

        assert!(result.is_ok(), "Expected Ok but got Err: {:?}", result);
        if let AdvertisedAddress::Uds(path) = result.unwrap() {
            assert_eq!(path, socket_path);
        } else {
            panic!("Expected Uds variant");
        }
    }

    #[test]
    fn test_parse_nonexistent_uds_path() {
        let result = AdvertisedAddress::from_str("unix:/invalid/path");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unix domain socket path does not exist"));
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
    fn test_derive_bind_address_http_with_port() {
        // Test case for AdvertisedAddress::Http with a valid host and port
        let advertised_address = AdvertisedAddress::from_str("http://127.0.0.1:1337").unwrap();

        // Derive the bind address
        let bind_address = advertised_address.derive_bind_address().unwrap();

        // Expected bind address
        let expected_bind_address =
            BindAddress::Socket(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 1337));

        assert_eq!(bind_address, expected_bind_address);
    }

    #[test]
    fn test_derive_bind_address_http_without_port() {
        // Test case for AdvertisedAddress::Http without a port
        let advertised_address = AdvertisedAddress::from_str("http://localhost").unwrap();

        // Deriving a bind address should return None since no port is provided
        let bind_address = advertised_address.derive_bind_address();
        assert!(bind_address.is_none());
    }

    #[test]
    fn test_derive_bind_address_unix_socket() {
        // Test case for AdvertisedAddress::Uds (Unix domain socket)
        let advertised_address = AdvertisedAddress::Uds(PathBuf::from("/tmp/socket"));

        // Deriving a bind address for a Unix socket should return None
        let bind_address = advertised_address.derive_bind_address();
        assert!(bind_address.is_none());
    }
    #[test]
    fn test_invalid_advertised_address() {
        // Test case for an invalid AdvertisedAddress string
        let result = AdvertisedAddress::from_str("invalid-address");

        // Parsing should fail, resulting in an error
        assert!(result.is_err(), "Expected an error for invalid address");
    }
}
