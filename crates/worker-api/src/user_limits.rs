// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_limiter::Level;
use restate_types::identifiers::PartitionKey;

/// A snapshot of a single counter in the user-limiter trie, exposed to external
/// introspection surfaces (e.g. the `sys_user_limits` DataFusion table).
///
/// Each entry represents one counter at a specific level of the scope → L1 → L2
/// hierarchy, together with the rule (if any) that bounds it and the number of
/// vqueues currently waiting behind it.
#[derive(Debug, Clone)]
pub struct UserLimitCounterEntry {
    /// Partition that owns this counter. Used to route the row through
    /// DataFusion's partition-aware scan.
    pub partition_key: PartitionKey,
    /// Scope component of the counter (the first trie level).
    pub scope: String,
    /// Level-1 key component. Present for `Level1` and `Level2` counters.
    pub l1: Option<String>,
    /// Level-2 key component. Present only for `Level2` counters.
    pub l2: Option<String>,
    /// The hierarchy level this counter sits at.
    pub level: Level,
    /// Current usage (concurrent in-flight permits) at this counter.
    pub usage: u32,
    /// Configured concurrency limit for this counter. `None` means unlimited
    /// (either no rule matched, or the matching rule leaves concurrency undefined).
    pub concurrency_limit: Option<u32>,
    /// Human-readable form of the rule that applies at this counter, if any.
    /// `None` when the counter has no matching rule (i.e. is unlimited).
    pub rule_pattern: Option<String>,
    /// Number of vqueues currently waiting behind this counter.
    pub num_waiters: u64,
}
