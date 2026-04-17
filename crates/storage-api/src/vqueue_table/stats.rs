// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::time::MillisSinceEpoch;
use restate_clock::{RoughTimestamp, UniqueTimestamp};

/// Some stats that are collected from the scheduler. Emitted along the item
/// at decision time.
#[derive(Debug, Clone, Default, bilrost::Message)]
pub struct WaitStats {
    /// Total milliseconds the item spent waiting on global invoker capacity
    #[bilrost(tag(1))]
    pub blocked_on_global_capacity_ms: u32,
    /// Total milliseconds the item was throttled on vqueue's "start" token bucket
    #[bilrost(tag(2))]
    pub vqueue_start_throttling_ms: u32,
    /// Total milliseconds the item was throttled on global "run" token bucket
    #[bilrost(tag(3))]
    pub global_invoker_throttling_ms: u32,
    /// Total milliseconds the item spent waiting on invoker memory pool
    #[bilrost(tag(4))]
    pub blocked_on_invoker_memory_ms: u32,
    /// Total milliseconds the item spent waiting on user-defined concurrency limits
    #[bilrost(tag(5))]
    pub blocked_on_user_limit_ms: u32,
    /// Total milliseconds the item spent waiting to acquire a virtual object lock
    #[bilrost(tag(6))]
    pub blocked_on_lock_ms: u32,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct EntryStatistics {
    /// Creation timestamp of the entry.
    #[bilrost(tag(1))]
    pub created_at: UniqueTimestamp,
    /// Timestamp of the last stage transition.
    ///
    /// This is always initialized to `created_at` and updated on every stage move.
    #[bilrost(tag(2))]
    pub transitioned_at: UniqueTimestamp,
    /// How many times did we move this entry to the run queue?
    /// '0` means that it's never been started.
    #[bilrost(tag(3))]
    pub num_attempts: u32,
    #[bilrost(tag(4))]
    pub num_paused: u32,
    #[bilrost(tag(5))]
    pub num_suspensions: u32,
    #[bilrost(tag(6))]
    pub num_yields: u32,
    /// Timestamp of the first attempt to run this entry
    #[bilrost(tag(7))]
    pub first_attempt_at: Option<UniqueTimestamp>,
    /// Timestamp of the last attempt to run this entry
    #[bilrost(tag(8))]
    pub latest_attempt_at: Option<UniqueTimestamp>,
    /// Earliest timestamp at which the first run can realistically start.
    ///
    /// This is computed once at enqueue-time as
    /// `max(created_at, original_run_at)`.
    ///
    /// We clamp to `created_at` when `original_run_at` is in the past to avoid
    /// inflating the first-attempt wait time.
    #[bilrost(tag(9))]
    pub first_runnable_at: MillisSinceEpoch,
    // todo:
    // pub time_spent_running: u32,
    // pub time_spent_parked: u32,
    // pub time_spent_ready_in_inbox: u32,
    // pub time_spent_waiting_for_retry: u32,
    // pub last_updated_at: MillisSinceEpoch,
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
            first_attempt_at: None,
            latest_attempt_at: None,
            first_runnable_at,
        }
    }
}
