// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use crate::Merge;

pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/common_descriptor.bin"));

include!(concat!(env!("OUT_DIR"), "/restate.common.rs"));

impl ProtocolVersion {
    pub const MIN_SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::V2;
    pub const CURRENT_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::V2;

    pub fn is_supported(&self) -> bool {
        *self >= ProtocolVersion::MIN_SUPPORTED_PROTOCOL_VERSION
            && *self <= ProtocolVersion::CURRENT_PROTOCOL_VERSION
    }
}

impl From<crate::GenerationalNodeId> for GenerationalNodeId {
    fn from(value: crate::GenerationalNodeId) -> Self {
        Self {
            id: value.id(),
            generation: value.generation(),
        }
    }
}

impl std::fmt::Display for GenerationalNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&crate::GenerationalNodeId::from(*self), f)
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

impl MetadataServerStatus {
    /// Returns true if the metadata store is running which means that it is either a member or
    /// a standby node.
    pub fn is_running(&self) -> bool {
        matches!(
            self,
            MetadataServerStatus::Member | MetadataServerStatus::Standby
        )
    }
}

impl Merge for NodeStatus {
    fn merge(&mut self, other: Self) -> bool {
        if other > *self {
            *self = other;
            return true;
        }
        false
    }
}
