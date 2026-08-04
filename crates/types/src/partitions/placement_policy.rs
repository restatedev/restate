// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Policy controlling automatic placement of a partition.
/// Stored on [`super::EpochMetadata`] and survives reconfigurations.
///
/// Since v1.7.3
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlacementPolicy {
    /// If set, the cluster controller will not automatically change the replica set. An already
    /// pending reconfiguration may still complete.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freeze: Option<PlacementFreeze>,
}

impl PlacementPolicy {
    /// True if the placement is currently frozen (the scheduler should no longer change the
    /// partition placement).
    pub fn is_frozen(&self) -> bool {
        self.freeze.is_some()
    }

    pub fn is_default(&self) -> bool {
        self == &PlacementPolicy::default()
    }
}

/// Operator-initiated freeze of automatic partition placement.
///
/// Since v1.7.3
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlacementFreeze {
    /// Human-readable reason for the freeze (for observability).
    pub reason: String,
}
