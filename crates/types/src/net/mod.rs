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
pub mod metadata;
pub mod partition_processor_manager;

// re-exports for convenience
pub use error::*;

use http::Uri;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

use super::GenerationalNodeId;
pub use crate::protobuf::common::ProtocolVersion;
pub use crate::protobuf::common::TargetName;

use self::codec::Targeted;
use self::codec::WireDecode;

pub static MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::Flexbuffers;
pub static CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::Flexbuffers;

/// Used to identify a request in a RPC-style call going through Networking.
#[derive(
    Debug,
    derive_more::Display,
    PartialEq,
    Eq,
    Clone,
    Copy,
    Hash,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct RequestId(u64);
impl RequestId {
    pub fn new() -> Self {
        Default::default()
    }
}

impl Default for RequestId {
    fn default() -> Self {
        use std::sync::atomic::AtomicUsize;
        static NEXT_REQUEST_ID: AtomicUsize = AtomicUsize::new(1);
        RequestId(
            NEXT_REQUEST_ID
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .try_into()
                .unwrap(),
        )
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

/// A wrapper for a message that includes the sender id
pub struct MessageEnvelope<M> {
    peer: GenerationalNodeId,
    connection_id: u64,
    body: M,
}

impl<M: WireDecode> MessageEnvelope<M> {
    pub fn new(peer: GenerationalNodeId, connection_id: u64, body: M) -> Self {
        Self {
            peer,
            connection_id,
            body,
        }
    }
}

impl<M> MessageEnvelope<M> {
    pub fn connection_id(&self) -> u64 {
        self.connection_id
    }

    pub fn split(self) -> (GenerationalNodeId, M) {
        (self.peer, self.body)
    }

    pub fn body(&self) -> &M {
        &self.body
    }
}

impl<M: RpcMessage> MessageEnvelope<M> {
    /// A unique identifier used by RPC-style messages to correlated requests and responses
    pub fn correlation_id(&self) -> M::CorrelationId {
        self.body.correlation_id()
    }
}

pub trait RpcMessage {
    type CorrelationId: Clone + Send + Eq + PartialEq + std::fmt::Debug + std::hash::Hash;
    fn correlation_id(&self) -> Self::CorrelationId;
}

pub trait RpcRequest: RpcMessage + Targeted {
    type Response: RpcMessage + Targeted;
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
            type Response = $response;
        }

        impl $crate::net::RpcMessage for $request {
            type CorrelationId = $crate::net::RequestId;

            fn correlation_id(&self) -> Self::CorrelationId {
                self.request_id
            }
        }

        impl $crate::net::RpcMessage for $response {
            type CorrelationId = $crate::net::RequestId;

            fn correlation_id(&self) -> Self::CorrelationId {
                self.request_id
            }
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

    #[test]
    fn test_request_id() {
        let request_id1 = RequestId::new();
        let request_id2 = RequestId::new();
        let request_id3 = RequestId::default();
        assert!(request_id1.0 < request_id2.0 && request_id2.0 < request_id3.0);
    }
}
