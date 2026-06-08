// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Add;

use restate_clock::time::MillisSinceEpoch;
use restate_clock::{RoughTimestamp, UniqueTimestamp};

/// Some stats that are collected from the scheduler. Emitted along the item
/// at decision time.
///
/// Encoding is fixed which mimics the simplicity and efficiency of a Vec<(StatKey, Value)>
/// on the wire.
#[derive(Debug, Clone, Copy, Default, bilrost::Message)]
pub struct WaitStats {
    /// Total milliseconds the item spent waiting on global invoker capacity
    #[bilrost(tag(1), encoding(fixed))]
    pub blocked_on_invoker_concurrency_ms: u32,
    /// Total milliseconds the item was blocked on user-defined per-vqueue
    /// throttling rules.
    #[bilrost(tag(2), encoding(fixed))]
    pub blocked_on_throttling_rules_ms: u32,
    /// Total milliseconds the item was blocked on node-level invoker throttling.
    #[bilrost(tag(3), encoding(fixed))]
    pub blocked_on_invoker_throttling_ms: u32,
    /// Total milliseconds the item spent waiting on invoker memory pool
    #[bilrost(tag(4), encoding(fixed))]
    pub blocked_on_invoker_memory_ms: u32,
    /// Total milliseconds the item spent waiting on user-defined concurrency limits
    #[bilrost(tag(5), encoding(fixed))]
    pub blocked_on_concurrency_rules_ms: u32,
    /// Total milliseconds the item spent waiting to acquire a virtual object lock
    #[bilrost(tag(6), encoding(fixed))]
    pub blocked_on_lock_ms: u32,
    /// Total milliseconds the item spent blocked on deployment concurrency capacity
    #[bilrost(tag(8), encoding(fixed))]
    pub blocked_on_deployment_concurrency_ms: u32,
}

macro_rules! ema_merge {
    ($this:ident, $other:ident, $( $attr:ident, )+) => {
        $(
            // It's intentional: to reflect positive changes from zero quicker than the decay,
            // otherwise it'll take long until users start observing.
            $this.$attr = if $this.$attr == 0 {
                $other.$attr
            } else {
                // Exponential moving average with alpha=0.05.
                (($this.$attr as f64 * 0.95) + ($other.$attr as f64 * 0.05)).ceil() as u32
            };
        )+
    };
}

impl WaitStats {
    /// Merge the input WaitStats into self. Self holds EMA values and `other`
    /// is a data point appended to it.
    pub fn ema_apply(&mut self, other: WaitStats) {
        ema_merge!(
            self,
            other,
            blocked_on_invoker_concurrency_ms,
            blocked_on_throttling_rules_ms,
            blocked_on_invoker_throttling_ms,
            blocked_on_invoker_memory_ms,
            blocked_on_concurrency_rules_ms,
            blocked_on_lock_ms,
            blocked_on_deployment_concurrency_ms,
        );
    }
}

impl Add<WaitStats> for WaitStats {
    type Output = WaitStats;

    fn add(self, rhs: WaitStats) -> Self::Output {
        Self::Output {
            blocked_on_deployment_concurrency_ms: self
                .blocked_on_deployment_concurrency_ms
                .saturating_add(rhs.blocked_on_deployment_concurrency_ms),
            blocked_on_invoker_concurrency_ms: self
                .blocked_on_invoker_concurrency_ms
                .saturating_add(rhs.blocked_on_invoker_concurrency_ms),
            blocked_on_throttling_rules_ms: self
                .blocked_on_throttling_rules_ms
                .saturating_add(rhs.blocked_on_throttling_rules_ms),
            blocked_on_invoker_throttling_ms: self
                .blocked_on_invoker_throttling_ms
                .saturating_add(rhs.blocked_on_invoker_throttling_ms),
            blocked_on_invoker_memory_ms: self
                .blocked_on_invoker_memory_ms
                .saturating_add(rhs.blocked_on_invoker_memory_ms),
            blocked_on_concurrency_rules_ms: self
                .blocked_on_concurrency_rules_ms
                .saturating_add(rhs.blocked_on_concurrency_rules_ms),
            blocked_on_lock_ms: self
                .blocked_on_lock_ms
                .saturating_add(rhs.blocked_on_lock_ms),
        }
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct EntryStatistics {
    /// Creation timestamp of the entry.
    #[bilrost(tag(1), encoding(fixed))]
    pub created_at: UniqueTimestamp,
    /// Timestamp of the last stage transition.
    ///
    /// This is always initialized to `created_at` and updated on every stage move.
    #[bilrost(tag(2), encoding(fixed))]
    pub transitioned_at: UniqueTimestamp,
    /// How many times did we move this entry to the run queue?
    /// '0` means that it's never been started.
    #[bilrost(tag(3), encoding(fixed))]
    pub num_attempts: u32,
    #[bilrost(tag(4), encoding(fixed))]
    pub num_paused: u32,
    #[bilrost(tag(5), encoding(fixed))]
    pub num_suspensions: u32,
    #[bilrost(tag(6), encoding(fixed))]
    pub num_yields: u32,
    /// Number of times this entry has yielded due to an error
    #[bilrost(tag(7), encoding(fixed))]
    pub num_errors: u32,
    /// Timestamp of the first attempt to run this entry
    #[bilrost(tag(8), encoding(fixed))]
    pub first_attempt_at: Option<UniqueTimestamp>,
    /// Timestamp of the last attempt to run this entry
    #[bilrost(tag(9), encoding(fixed))]
    pub latest_attempt_at: Option<UniqueTimestamp>,
    /// Earliest timestamp at which the first run can realistically start.
    ///
    /// This is computed once at enqueue-time as
    /// `max(created_at, original_run_at)`.
    ///
    /// We clamp to `created_at` when `original_run_at` is in the past to avoid
    /// inflating the first-attempt wait time.
    #[bilrost(tag(10), encoding(fixed))]
    pub first_runnable_at: MillisSinceEpoch,

    /// Stats emitted by the scheduler of the last run attempt
    #[bilrost(tag(11))]
    pub latest_attempt_wait_stats: WaitStats,

    // # Cumulative Stats Over All Attempts
    /// Will be saturating to the max possible values (u32, 49-ish days)
    #[bilrost(tag(12))]
    pub total_wait_stats: WaitStats,
}

impl EntryStatistics {
    pub fn new(created_at: UniqueTimestamp, original_run_at: RoughTimestamp) -> Self {
        let first_runnable_at = created_at
            .to_unix_millis()
            .max(original_run_at.as_unix_millis());

        Self {
            created_at,
            transitioned_at: created_at,
            num_attempts: 0,
            num_paused: 0,
            num_suspensions: 0,
            num_yields: 0,
            num_errors: 0,
            first_attempt_at: None,
            latest_attempt_at: None,
            first_runnable_at,
            latest_attempt_wait_stats: WaitStats::default(),
            total_wait_stats: WaitStats::default(),
        }
    }
}
