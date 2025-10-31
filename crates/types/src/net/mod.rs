// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod address;
pub mod codec;
pub mod connect_opts;
pub mod listener;
pub mod log_server;
pub mod metadata;
pub mod node;
pub mod partition_processor;
pub mod partition_processor_manager;
pub mod remote_query_scanner;
pub mod replicated_loglet;

pub use restate_ty::net::ProtocolVersion;
pub use restate_ty::net::ServiceTag;

pub static MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::V2;
pub static CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::V2;

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
use {bilrost_wire_codec, default_wire_codec, define_rpc, define_service, define_unary_message};
