// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Source requirements and extensible replica-placement options. These describe
//! serving eligibility, not snapshot isolation or read freshness.

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, bilrost::Enumeration)]
pub enum PartitionSource {
    #[default]
    Storage = 0,
    LeaderLive = 1,
}

/// Storage-only placement policy. Future follower eligibility belongs here and
/// must be enforced both by the locator and by worker-local binding.
#[non_exhaustive]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, bilrost::Message)]
pub struct StoragePlacementOptions {
    /// Require an observed leader instead of permitting routing's alive-replica
    /// fallback. This does not establish a linearizable read.
    #[bilrost(1)]
    pub require_leader: bool,
}

/// Part of the placement memoization key and the serialized worker contract.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, bilrost::Message)]
pub struct PartitionPlacement {
    #[bilrost(1)]
    pub source: PartitionSource,
    #[bilrost(2)]
    pub options: StoragePlacementOptions,
}

impl PartitionPlacement {
    pub fn requires_leader(self) -> bool {
        self.source == PartitionSource::LeaderLive || self.options.require_leader
    }
}
