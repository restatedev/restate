// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::RngCore;

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
        RequestId(rand::thread_rng().next_u64())
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
