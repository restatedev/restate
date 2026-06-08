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
use restate_types::vqueues::EntryId;
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
    /// `EntryId` of the head item, if the scheduler has already advanced to it.
    /// `None` means the head has not been materialized yet (no storage IO has
    /// been done to look it up) — not that there isn't one.
    pub head_entry_id: Option<EntryId>,
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
    /// The vqueue is blocked on a resource that prevents the head from running.
    ///
    /// Carries a [`BlockedResource`] (not the internal [`ResourceKind`]) so that
    /// external consumers — e.g. the `sys_scheduler` DataFusion table — get a
    /// resolved view: rule handles are already looked up into their pattern
    /// strings at status-construction time.
    BlockedOn(BlockedResource),
}

impl SchedulingStatus {
    pub const fn name(&self) -> &'static str {
        match self {
            SchedulingStatus::Dormant => "dormant",
            SchedulingStatus::Empty => "empty",
            SchedulingStatus::Ready => "ready",
            SchedulingStatus::Scheduled { .. } => "scheduled",
            SchedulingStatus::BlockedOn(_) => "blocked",
        }
    }
}

/// Internal routing tag used by the scheduler to decide where a blocked vqueue
/// should wait and where to wake it from. Carries a [`RuleHandle`] rather than a
/// resolved pattern because the scheduler needs stable identity to match rule
/// updates; the handle is resolved to a display string (see [`BlockedResource`])
/// only when status is reported out.
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

impl ResourceKind {
    /// Resolve the internal `ResourceKind` into a [`BlockedResource`] suitable
    /// for external reporting. `resolve_rule` turns a `RuleHandle` into its
    /// pattern string — callers pass a closure that consults the rules store
    /// (typically `UserLimiter::resolve_rule`). A `None` return means the rule
    /// has been removed since the vqueue was registered as blocked.
    pub fn to_blocked_resource(
        &self,
        resolve_rule: impl FnOnce(RuleHandle) -> Option<ReString>,
    ) -> BlockedResource {
        match self {
            ResourceKind::Lock { scope, lock_name } => BlockedResource::Lock {
                scope: scope.clone(),
                lock_name: lock_name.clone(),
            },
            ResourceKind::InvokerConcurrency => BlockedResource::InvokerConcurrency,
            ResourceKind::InvokerThrottling { estimated_retry_at } => {
                BlockedResource::InvokerThrottling {
                    estimated_retry_at: *estimated_retry_at,
                }
            }
            ResourceKind::InvokerMemory => BlockedResource::InvokerMemory,
            ResourceKind::DeploymentConcurrency => BlockedResource::DeploymentConcurrency,
            ResourceKind::LimitKeyConcurrency {
                scope,
                limit_key,
                blocked_level,
                blocked_rule,
            } => BlockedResource::LimitKeyConcurrency {
                scope: scope.clone(),
                limit_key: limit_key.clone(),
                blocked_level: *blocked_level,
                blocked_rule: blocked_rule.and_then(resolve_rule),
            },
        }
    }
}

/// Public, display-ready view of the resource a vqueue is blocked on.
///
/// Mirrors the shape of the internal [`ResourceKind`] but carries a resolved
/// rule-pattern string rather than an opaque [`RuleHandle`], so downstream
/// consumers (DataFusion tables, CLI, …) don't need to reach into the rules
/// store to render it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockedResource {
    /// Waiting to acquire the lock of a virtual object.
    Lock {
        scope: Option<Scope>,
        lock_name: LockName,
    },
    /// Waiting on global invoker concurrency capacity.
    InvokerConcurrency,
    /// Waiting on node-level invoker throttling tokens.
    InvokerThrottling {
        /// Best-effort estimate for when this queue can retry token acquisition.
        estimated_retry_at: Option<MillisSinceEpoch>,
    },
    /// Waiting on the invoker memory pool.
    InvokerMemory,
    /// Waiting on deployment-level concurrency capacity.
    DeploymentConcurrency,
    /// Waiting on user-defined concurrency limits.
    LimitKeyConcurrency {
        scope: Scope,
        limit_key: LimitKey<ReString>,
        blocked_level: Level,
        /// Display form of the rule that's holding this queue back. `None` if
        /// the rule was removed since the queue became blocked.
        blocked_rule: Option<ReString>,
    },
}

impl std::fmt::Display for BlockedResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlockedResource::Lock { scope, lock_name } => match scope {
                Some(scope) => write!(f, "Lock(scope={scope}, name={lock_name})"),
                None => write!(f, "Lock(name={lock_name})"),
            },
            BlockedResource::InvokerConcurrency => f.write_str("InvokerConcurrency"),
            BlockedResource::InvokerThrottling { estimated_retry_at } => match estimated_retry_at {
                Some(retry_at) => write!(f, "InvokerThrottling(retry_at_ts={})", retry_at.as_u64()),
                None => f.write_str("InvokerThrottling"),
            },
            BlockedResource::InvokerMemory => f.write_str("InvokerMemory"),
            BlockedResource::DeploymentConcurrency => f.write_str("DeploymentConcurrency"),
            BlockedResource::LimitKeyConcurrency {
                scope,
                limit_key,
                blocked_level,
                blocked_rule,
            } => {
                write!(
                    f,
                    "LimitKeyConcurrency({scope}/{limit_key}, level={blocked_level}"
                )?;
                match blocked_rule {
                    Some(rule) => write!(f, ", rule={rule})"),
                    None => write!(f, ", rule=[removed])"),
                }
            }
        }
    }
}
