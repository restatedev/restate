// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use bytes::{Buf, BufMut};
use restate_types::identifiers::{PartitionId, PartitionKey};
use serde::{Deserialize, Serialize};

use crate::codec::{decode_default, encode_default, Targeted, WireDecode, WireEncode};
use crate::common::{ProtocolVersion, RequestId, TargetName};
use crate::CodecError;

// Sent *to* worker nodes
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    derive_more::From,
    strum_macros::EnumIs,
    strum_macros::IntoStaticStr,
)]
pub enum WorkerMessage {
    AttachmentResponse(AttachmentResponse),
    GetPartitionStatus,
}

impl Targeted for WorkerMessage {
    const TARGET: TargetName = TargetName::Worker;

    fn kind(&self) -> &'static str {
        self.into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentResponse {
    pub request_id: RequestId,
    pub actions: Vec<Action>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub enum RunMode {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    RunPartition(RunPartition),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunPartition {
    pub partition_id: PartitionId,
    pub key_range_inclusive: KeyRange,
    pub mode: RunMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRange {
    pub from: PartitionKey,
    pub to: PartitionKey,
}

impl From<KeyRange> for RangeInclusive<PartitionKey> {
    fn from(val: KeyRange) -> Self {
        RangeInclusive::new(val.from, val.to)
    }
}

impl WireEncode for WorkerMessage {
    fn encode<B: BufMut>(
        &self,
        buf: &mut B,
        protocol_version: ProtocolVersion,
    ) -> Result<(), CodecError> {
        // serialize message into buf
        encode_default(self, buf, protocol_version)
    }
}

impl WireDecode for WorkerMessage {
    fn decode<B: Buf>(buf: &mut B, protocol_version: ProtocolVersion) -> Result<Self, CodecError>
    where
        Self: Sized,
    {
        decode_default(buf, protocol_version)
    }
}
