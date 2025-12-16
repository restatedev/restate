// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use smallvec::SmallVec;

use restate_clock::time::MillisSinceEpoch;
use restate_types::clock::UniqueTimestamp;
use restate_types::vqueue::EffectivePriority;

use super::{Stage, VisibleAt};

#[derive(Debug, Default, Clone, bilrost::Message)]
pub struct VQueueStatistics {
    /// The time spend in the queue before the first attempt to run. Measured by EMA of time
    /// from initial scheduled run time to first "dequeue/start".
    #[bilrost(tag(1))]
    pub(crate) avg_queue_duration_ms: u32,
    /// Timestamp of the last successful enqueue.
    #[bilrost(tag(2))]
    pub(crate) last_enqueued_at: Option<MillisSinceEpoch>,
    /// The timestamp of the last start of a new entry.
    #[bilrost(tag(3))]
    pub(crate) last_start_at: Option<MillisSinceEpoch>,
    #[bilrost(tag(4))]
    pub(crate) last_completion_at: Option<MillisSinceEpoch>,
    /// The timestamp of the last run attempt of a previously started entry.
    #[bilrost(tag(5))]
    pub(crate) last_resume_at: Option<MillisSinceEpoch>,
}

impl VQueueStatistics {
    fn update_avg_queue_duration(&mut self, latency_ms: u64) {
        let new_avg: u64 = if self.avg_queue_duration_ms == 0 {
            latency_ms
        } else {
            // exponential moving average
            ((self.avg_queue_duration_ms as f64 * 0.95) + (latency_ms as f64 * 0.05)).ceil() as u64
        };
        self.avg_queue_duration_ms = u32::try_from(new_avg).unwrap_or(u32::MAX);
    }
}

#[derive(Debug, Default, Clone, bilrost::Message)]
pub struct VQueueMeta {
    /// if true, the vqueue is paused, we don't pop entries from it until it's resumed.
    #[bilrost(tag(1))]
    is_paused: bool,
    /// Total number of entries (ready + paused + running + suspended + scheduled), but it doesn't
    /// include completed or failed entries. This is the length that is used to reject new invocations
    /// being added to the vqueue. The capacity configuration will limit this value.
    #[bilrost(tag(2))]
    pub(crate) length: u32,
    /// Number of concurrency tokens being used
    #[bilrost(tag(3))]
    pub(crate) num_tokens_used: u32,
    /// The number of entries waiting to be dequeued. The vector index implies the priority
    #[bilrost(tag(4), encoding(packed))]
    pub(crate) num_waiting: [u32; EffectivePriority::NUM_PRIORITIES],
    #[bilrost(tag(5))]
    pub(crate) num_running: u32,
    /// The zero time point of the "starts" token bucket
    #[bilrost(tag(6))]
    pub(crate) start_tb_zero_time: f64,
    #[bilrost(tag(7))]
    pub(crate) stats: VQueueStatistics,
}

impl VQueueMeta {
    pub fn tokens_used(&self) -> u32 {
        self.num_tokens_used
    }

    pub fn len(&self) -> u32 {
        self.length
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn total_waiting(&self) -> u32 {
        self.num_waiting.iter().sum()
    }

    fn increment_running(&mut self) {
        self.num_running += 1;
    }

    fn decrement_running(&mut self) {
        self.num_running -= 1;
    }

    /// A vqueue is considered active when it's of interest to the scheduler.
    ///
    /// The scheduler cares about vqueues that have entries that are already running or that are waiting
    /// to run. With some special rules to consider when the queue is paused. When the vqueue is
    /// paused, the scheduler will only be interested in its "running" entries and not in its
    /// waiting entries. Therefore, it will remain to be "active" as long as it has running
    /// entries. Once running entries are moved to waiting or completed, the vqueue is be
    /// considered dormant until it's unpaused.
    pub fn is_active(&self) -> bool {
        self.num_running > 0 || (self.total_waiting() > 0 && !self.is_paused())
    }

    pub fn num_waiting(&self, priority: EffectivePriority) -> u32 {
        self.num_waiting[priority as usize]
    }

    pub fn num_running(&self) -> u32 {
        self.num_running
    }

    pub fn stats(&self) -> &VQueueStatistics {
        &self.stats
    }

    pub fn last_enqueued_ts(&self) -> Option<MillisSinceEpoch> {
        self.stats.last_enqueued_at
    }

    pub fn last_start_ts(&self) -> Option<MillisSinceEpoch> {
        self.stats.last_start_at
    }

    /// Used to rehydrate the run/start token bucket on schedulers
    pub fn start_tb_zero_time(&self) -> f64 {
        self.start_tb_zero_time
    }

    pub fn is_paused(&self) -> bool {
        self.is_paused
    }

    fn add_to_waiting(&mut self, priority: EffectivePriority) {
        self.num_waiting[priority as usize] += 1;
    }

    fn remove_from_waiting(&mut self, priority: EffectivePriority) {
        self.num_waiting[priority as usize] -= 1;
    }

    fn acquire_token(&mut self) {
        self.num_tokens_used += 1;
    }

    fn release_token(&mut self) {
        self.num_tokens_used = self.num_tokens_used.saturating_sub(1);
    }

    pub fn apply_updates(&mut self, updates: &VQueueMetaUpdates) -> anyhow::Result<()> {
        for update in updates.updates.iter() {
            self.apply_update(update)?;
        }
        Ok(())
    }

    #[inline]
    fn apply_update(&mut self, update: &Update) -> anyhow::Result<()> {
        let now = update.ts;
        // Note to future authors: This match needs to continue to work even when
        // processing old/deprecated/removed actions. Therefore, removed actions should
        // not be removed from the enum to avoid falling into the Unknown case.
        match update.action {
            Action::Unknown => {
                anyhow::bail!("Unrecognized vqueue action: {update:?}")
            }
            Action::EnqueueNew { priority } => {
                debug_assert!(priority.is_new());
                self.length += 1;
                self.add_to_waiting(priority);
                self.stats.last_enqueued_at = Some(now.to_unix_millis());
            }
            Action::StartAttempt {
                visible_at,
                priority,
                updated_start_tb_zero_time: updated_run_token_bucket_zero_time,
            } => {
                if priority.is_new() {
                    self.stats.last_start_at = Some(now.to_unix_millis());
                } else {
                    self.stats.last_resume_at = Some(now.to_unix_millis());
                }

                self.increment_running();
                self.remove_from_waiting(priority);

                if priority.is_new()
                    && let VisibleAt::At(visible_since) = visible_at
                {
                    // Only measure queue latency for new items and only consider the item
                    // queuing from the moment it became visible, not the creation ts.
                    let latency_ms = now.to_unix_millis().saturating_sub_ms(visible_since);
                    self.stats.update_avg_queue_duration(latency_ms);
                }

                // Update the run token bucket's zero time if it was supplied
                if let Some(updated_run_token_bucket_zero_time) = updated_run_token_bucket_zero_time
                {
                    self.start_tb_zero_time = updated_run_token_bucket_zero_time;
                }
                if !priority.token_held() {
                    self.acquire_token();
                }
            }
            Action::Park {
                should_release_concurrency_token,
                priority,
                previous_stage,
            } => {
                debug_assert!(self.length > 0);
                match previous_stage {
                    Stage::Unknown => {
                        anyhow::bail!("Unknown stage for vqueue entry park action: {update:?}");
                    }
                    Stage::Inbox => {
                        self.remove_from_waiting(priority);
                    }
                    Stage::Run => {
                        self.decrement_running();
                    }
                    Stage::Park => {
                        // do nothing.
                    }
                }

                if should_release_concurrency_token && priority.token_held() {
                    // Release the token immediately on park if this entry doesn't require
                    // holding while parked.
                    self.release_token();
                }
            }
            Action::WakeUp { priority } => {
                debug_assert!(self.length > 0);
                self.add_to_waiting(priority);
            }
            Action::YieldRunning => {
                debug_assert!(self.length > 0);
                self.decrement_running();
                self.add_to_waiting(EffectivePriority::TokenHeld);
            }
            Action::Complete {
                previous_stage,
                priority,
            } => {
                debug_assert!(self.length > 0);
                self.length -= 1;
                self.stats.last_completion_at = Some(now.to_unix_millis());
                match previous_stage {
                    Stage::Unknown => {
                        anyhow::bail!("Unknown stage for vqueue entry complete action: {update:?}")
                    }
                    Stage::Inbox => {
                        self.remove_from_waiting(priority);
                    }
                    Stage::Run => {
                        self.decrement_running();
                    }
                    Stage::Park => {
                        // do nothing.
                    }
                }

                if priority.token_held() {
                    self.release_token();
                }
            }
        }
        Ok(())
    }
}

/// A collection of differential updates to the vqueue meta data structure.
///
/// Those updates can be applied to the storage layer via a merge operator and at the same
/// time they can be accepted by the vqueue's cache to keep them in sync.
#[derive(Clone, Default, bilrost::Message)]
pub struct VQueueMetaUpdates {
    #[bilrost(1)]
    pub updates: SmallVec<[Update; VQueueMetaUpdates::INLINED_UPDATES]>,
}

impl VQueueMetaUpdates {
    pub const INLINED_UPDATES: usize = 1;

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            updates: SmallVec::with_capacity(capacity),
        }
    }

    #[inline(always)]
    pub fn push(&mut self, ts: UniqueTimestamp, action: Action) {
        self.updates.push(Update { ts, action });
    }

    pub fn len(&self) -> usize {
        self.updates.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Update> {
        self.updates.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    pub fn extend(&mut self, other: Self) {
        self.updates.extend(other.updates);
    }
}

#[derive(Debug, Clone, Default, bilrost::Oneof, bilrost::Message)]
pub enum Action {
    #[default]
    #[bilrost(empty)]
    Unknown,
    /// Entry is being enqueued for the first time
    #[bilrost(tag(2), message)]
    EnqueueNew { priority: EffectivePriority },
    /// An entry from inbox stage is being moved to Run
    #[bilrost(tag(3), message)]
    StartAttempt {
        visible_at: VisibleAt,
        priority: EffectivePriority,
        updated_start_tb_zero_time: Option<f64>,
    },
    #[bilrost(tag(4), message)]
    Park {
        priority: EffectivePriority,
        should_release_concurrency_token: bool,
        previous_stage: Stage,
    },
    // Wake up after pause or suspend.
    #[bilrost(tag(5), message)]
    WakeUp { priority: EffectivePriority },
    // Item moved from running back to waiting
    #[bilrost(tag(6), message)]
    YieldRunning,
    // Execution has ended (failed, succeeded, killed, etc.)
    #[bilrost(tag(7), message)]
    Complete {
        // Must be the latest priority assigned to the entry (effective priority)
        priority: EffectivePriority,
        previous_stage: Stage,
    },
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct Update {
    #[bilrost(tag(1))]
    pub(super) ts: UniqueTimestamp,
    #[bilrost(oneof(2, 3, 4, 5, 6, 7))]
    pub(super) action: Action,
}
