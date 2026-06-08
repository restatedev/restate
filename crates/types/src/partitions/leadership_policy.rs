// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::PlainNodeId;
use crate::locality::NodeLocation;

/// Policy controlling leader election for a partition.
/// Stored on [`super::EpochMetadata`] and survives reconfigurations.
///
/// Since v1.7.0
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LeadershipPolicy {
    /// If set, leader election is frozen. The current leader stays;
    /// no new leader is elected even if the current leader dies.
    /// Freeze takes precedence over affinity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freeze: Option<ElectionFreeze>,

    /// Preferred leader affinity. The scheduler will prefer nodes
    /// matching this affinity if they are alive and in the replica set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affinity: Option<LeaderAffinity>,
}

impl LeadershipPolicy {
    pub fn is_default(&self) -> bool {
        self == &LeadershipPolicy::default()
    }
}

/// Operator-initiated freeze of leader election.
///
/// Since v1.7.0
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ElectionFreeze {
    /// Human-readable reason for the freeze (for observability).
    pub reason: String,
}

/// Affinity expression for leader selection.
/// New variants may be added in future versions (requires version gap).
///
/// Since v1.7.0
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LeaderAffinity {
    /// Prefer a specific node by ID.
    Node(PlainNodeId),
    /// Prefer any node whose location matches this prefix
    /// (e.g., `"us-east-1"` for region, `"us-east-1.az2"` for zone).
    Location(NodeLocation),
}
