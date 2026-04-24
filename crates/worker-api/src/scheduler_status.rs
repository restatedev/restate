// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::RoughTimestamp;
use restate_limiter::{Level, LimitKey, RuleHandle};
use restate_storage_api::vqueue_table::stats::WaitStats;
use restate_types::time::MillisSinceEpoch;
use restate_types::{LockName, Scope};
use restate_util_string::ReString;

/// A public view of the scheduler's status of a single vqueue.
///
/// This struct provides introspection into the current scheduling state, and
/// wait statistics for a vqueue.
#[derive(Debug, Clone, Default)]
pub struct VQueueSchedulerStatus {
    /// Statistics about the wait time experienced by the head item in the vqueue.
    pub wait_stats: WaitStats,
    /// Number of items remaining in the running stage.
    pub remaining_running: u32,
    /// Number of items waiting in the inbox stage.
    pub waiting_inbox: u64,
    /// The current scheduling status of this vqueue.
    pub status: SchedulingStatus,
}

/// The current scheduling status of a vqueue.
///
/// This enum represents the various states a vqueue can be in from the
/// scheduler's perspective.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SchedulingStatus {
    #[default]
    /// The vqueue is not tracked by the scheduler (e.g., it has no items).
    Dormant,
    /// The vqueue is empty.
    Empty,
    /// The vqueue head is ready to be scheduled and it's in the inbox/running stage.
    Ready,
    /// The vqueue is scheduled to be woken up at the given time because the head
    /// item is scheduled to run at that time.
    Scheduled {
        /// When the head item becomes visible.
        at: RoughTimestamp,
    },
    /// The vqueue is blocked on invoker global capacity.
    BlockedOn(ResourceKind),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceKind {
    /// Waiting to acquire a lock of a VO.
    Lock {
        scope: Option<Scope>,
        lock_name: LockName,
    },
    /// Waiting to acquire invoker concurrency capacity
    InvokerConcurrency,
    /// Waiting to acquire invoker throttling tokens.
    InvokerThrottling {
        /// Best-effort estimate for when this queue can retry token acquisition.
        ///
        /// `None` means no estimate is currently available.
        estimated_retry_at: Option<MillisSinceEpoch>,
    },
    /// Invoker needs to allocate memory for an invocation
    InvokerMemory,
    /// Waiting for deployment-level concurrency tokens to be available
    DeploymentConcurrency,
    /// Waiting for user-defined concurrency to be acquired.
    /// Carries routing info so the eligibility tracker can return it for waiter removal.
    LimitKeyConcurrency {
        scope: Scope,
        limit_key: LimitKey<ReString>,
        blocked_level: Level,
        /// Handle to the blocking rule. Resolve via the rules store for display.
        /// May be stale if the rule was removed since blocking.
        blocked_rule: Option<RuleHandle>,
    },
}
