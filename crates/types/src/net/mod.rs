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

// re-exports for convenience
pub use error::*;

use std::net::{AddrParseError, SocketAddr};
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
    #[display("unix:{}", "_0.display()")]
    Uds(PathBuf),
    /// Hostname or host:port pair, or any unrecognizable string.
    #[display("{}", _0)]
    Http(Uri),
}

impl FromStr for AdvertisedAddress {
    type Err = http::uri::InvalidUri;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(stripped_address) = s.strip_prefix("unix:") {
            Ok(AdvertisedAddress::Uds(
                stripped_address.parse().expect("infallible"),
            ))
        } else {
            // try to parse as a URI
            Ok(AdvertisedAddress::Http(s.parse()?))
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
    #[display("unix:{}", "_0.display()")]
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
                &self,
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
    use http::Uri;

    use super::*;

    // test parsing [`AdvertisedAddress`]
    #[test]
    fn test_parse_network_address() -> anyhow::Result<()> {
        let tcp: AdvertisedAddress = "127.0.0.1:5123".parse()?;
        restate_test_util::assert_eq!(tcp, AdvertisedAddress::Http("127.0.0.1:5123".parse()?));

        let tcp: AdvertisedAddress = "unix:/tmp/unix.socket".parse()?;
        restate_test_util::assert_eq!(
            tcp,
            AdvertisedAddress::Uds("/tmp/unix.socket".parse().unwrap())
        );

        let tcp: AdvertisedAddress = "localhost:5123".parse()?;
        restate_test_util::assert_eq!(tcp, AdvertisedAddress::Http("localhost:5123".parse()?));

        let tcp: AdvertisedAddress = "https://localhost:5123".parse()?;
        restate_test_util::assert_eq!(
            tcp,
            AdvertisedAddress::Http(Uri::from_static("https://localhost:5123"))
        );

        Ok(())
    }
}
