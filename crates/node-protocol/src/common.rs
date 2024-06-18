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
use std::sync::atomic::AtomicUsize;

use restate_types::identifiers::PartitionKey;

include!(concat!(env!("OUT_DIR"), "/dev.restate.common.rs"));

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
        static NEXT_REQUEST_ID: AtomicUsize = AtomicUsize::new(1);
        RequestId(
            NEXT_REQUEST_ID
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .try_into()
                .unwrap(),
        )
    }
}

pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/common_descriptor.bin"));

impl ProtocolVersion {
    pub fn is_supported(&self) -> bool {
        *self >= MIN_SUPPORTED_PROTOCOL_VERSION && *self <= CURRENT_PROTOCOL_VERSION
    }
}

impl From<Version> for restate_types::Version {
    fn from(version: Version) -> Self {
        restate_types::Version::from(version.value)
    }
}

impl From<restate_types::Version> for Version {
    fn from(version: restate_types::Version) -> Self {
        Version {
            value: version.into(),
        }
    }
}

impl From<NodeId> for restate_types::NodeId {
    fn from(node_id: NodeId) -> Self {
        restate_types::NodeId::new(node_id.id, node_id.generation)
    }
}

impl From<restate_types::NodeId> for NodeId {
    fn from(node_id: restate_types::NodeId) -> Self {
        NodeId {
            id: node_id.id().into(),
            generation: node_id.as_generational().map(|g| g.generation()),
        }
    }
}

impl From<restate_types::PlainNodeId> for NodeId {
    fn from(node_id: restate_types::PlainNodeId) -> Self {
        let id: u32 = node_id.into();
        NodeId {
            id,
            generation: None,
        }
    }
}

impl From<restate_types::GenerationalNodeId> for NodeId {
    fn from(node_id: restate_types::GenerationalNodeId) -> Self {
        NodeId {
            id: node_id.raw_id(),
            generation: Some(node_id.generation()),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KeyRange {
    pub from: PartitionKey,
    pub to: PartitionKey,
}

impl From<KeyRange> for RangeInclusive<PartitionKey> {
    fn from(val: KeyRange) -> Self {
        RangeInclusive::new(val.from, val.to)
    }
}

impl From<restate_types::logs::Lsn> for Lsn {
    fn from(lsn: restate_types::logs::Lsn) -> Self {
        let value: u64 = lsn.into();
        Lsn { value }
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(generation) = self.generation {
            write!(f, "N{}:{}", self.id, generation)
        } else {
            write!(f, "N{}", self.id)
        }
    }
}

impl std::fmt::Display for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

impl std::fmt::Display for LeaderEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}", self.value)
    }
}

impl From<restate_types::identifiers::LeaderEpoch> for LeaderEpoch {
    fn from(epoch: restate_types::identifiers::LeaderEpoch) -> Self {
        let value: u64 = epoch.into();
        LeaderEpoch { value }
    }
}

// write tests for RequestId
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_request_id() {
        let request_id1 = RequestId::new();
        let request_id2 = RequestId::new();
        let request_id3 = RequestId::default();
        assert!(request_id1.0 < request_id2.0 && request_id2.0 < request_id3.0);
    }
}
