// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut, Bytes};

use restate_types::bilrost_storage_encode_decode;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};

/// Pause an invocation rpc triggered from external sources (http ingress)
#[derive(Debug, Clone, bilrost::Message)]
pub struct PauseInvocationRpcRequest {
    #[bilrost(tag(1))]
    pub invocation_id: InvocationId,
    #[bilrost(tag(2))]
    pub request_id: PartitionProcessorRpcRequestId,
}

bilrost_storage_encode_decode!(PauseInvocationRpcRequest);

impl PauseInvocationRpcRequest {
    pub fn bilrost_encode<B: BufMut>(&self, b: &mut B) -> Result<(), bilrost::EncodeError> {
        bilrost::Message::encode(self, b)
    }

    pub fn encoded_len(&self) -> usize {
        bilrost::Message::encoded_len(self)
    }

    pub fn bilrost_encode_to_bytes(&self) -> Bytes {
        bilrost::Message::encode_to_bytes(self)
    }

    pub fn bilrost_decode<B: Buf>(buf: B) -> Result<Self, bilrost::DecodeError> {
        bilrost::OwnedMessage::decode(buf)
    }
}
