// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod codec;
pub mod common;
mod error;
pub mod ingress;
pub mod metadata;
pub mod node;

// re-exports for convenience
pub use common::CURRENT_PROTOCOL_VERSION;
pub use common::MIN_SUPPORTED_PROTOCOL_VERSION;
pub use error::*;

use restate_types::GenerationalNodeId;

use self::codec::Targeted;
use self::codec::WireDecode;

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
        impl crate::codec::Targeted for $message {
            const TARGET: TargetName = $target;
            fn kind(&self) -> &'static str {
                stringify!($message)
            }
        }

        impl crate::codec::WireEncode for $message {
            fn encode<B: bytes::BufMut>(
                &self,
                buf: &mut B,
                protocol_version: crate::common::ProtocolVersion,
            ) -> Result<(), crate::CodecError> {
                // serialize message into buf
                crate::codec::encode_default(self, buf, protocol_version)
            }
        }

        impl crate::codec::WireDecode for $message {
            fn decode<B: bytes::Buf>(
                buf: &mut B,
                protocol_version: crate::common::ProtocolVersion,
            ) -> Result<Self, crate::CodecError>
            where
                Self: Sized,
            {
                crate::codec::decode_default(buf, protocol_version)
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
        impl crate::RpcRequest for $request {
            type Response = $response;
        }

        impl crate::RpcMessage for $request {
            type CorrelationId = crate::common::RequestId;

            fn correlation_id(&self) -> Self::CorrelationId {
                self.request_id
            }
        }

        impl crate::RpcMessage for $response {
            type CorrelationId = crate::common::RequestId;

            fn correlation_id(&self) -> Self::CorrelationId {
                self.request_id
            }
        }

        crate::define_message! {
            @message = $request,
            @target = $request_target,
        }

        crate::define_message! {
            @message = $response,
            @target = $response_target,
        }
    };
}

#[allow(unused_imports)]
use {define_message, define_rpc};
