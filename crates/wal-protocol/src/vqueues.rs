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
use restate_types::vqueues::VQueueId;

/// Note: Within a single command, all VQueue IDs must be on the same partition-key
#[derive(Debug, Clone, bilrost::Message)]
pub struct VQueuesPauseCommand {
    #[bilrost(tag(1))]
    pub vqueues: Vec<VQueueId>,
}

bilrost_storage_encode_decode!(VQueuesPauseCommand);

/// Note: Within a single command, all VQueue IDs must be on the same partition-key
#[derive(Debug, Clone, bilrost::Message)]
pub struct VQueuesResumeCommand {
    #[bilrost(tag(1))]
    pub vqueues: Vec<VQueueId>,
}

bilrost_storage_encode_decode!(VQueuesResumeCommand);

impl VQueuesPauseCommand {
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

impl VQueuesResumeCommand {
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
