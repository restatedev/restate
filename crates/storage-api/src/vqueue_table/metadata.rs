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

use restate_types::clock::UniqueTimestamp;
use restate_types::vqueue::EffectivePriority;

use super::VisibleAt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VQueueStatus {
    /// Enabled (not-paused) and has items to process
    Active,
    /// Regardless whether it's paused or not, it's empty (nothing to process)
    Empty,
    /// Paused indicates it's non-empty but paused (should not process its items)
    Paused,
}

#[derive(Debug, Default, Clone, bilrost::Message)]
pub struct VQueueStatistics {
    /// The time spend in the queue before the first attempt to run. Measured by EMA of time
    /// from "enqueue" to "dequeue/start".
    #[bilrost(1)]
    pub(crate) avg_queue_duration_ms: u32,
    /// Timestamp of the last successful enqueue.
    #[bilrost(2)]
    pub(crate) last_enqueued_at: Option<UniqueTimestamp>,
    #[bilrost(3)]
    pub(crate) last_start_at: Option<UniqueTimestamp>,
    #[bilrost(4)]
    pub(crate) last_completion_at: Option<UniqueTimestamp>,
    #[bilrost(5)]
    pub(crate) num_running: u32,
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

    fn increment_running(&mut self) {
        self.num_running += 1;
    }

    fn decrement_running(&mut self) {
        self.num_running -= 1;
    }
}

#[derive(Debug, Default, Clone, bilrost::Message)]
pub struct VQueueMeta {
    /// if true, the vqueue is paused, we don't pop entries from it until it's resumed.
    #[bilrost(1)]
    is_paused: bool,
    /// Total number of entries (ready + paused + running + suspended + scheduled), but it doesn't
    /// include completed or failed entries. This is the length that is used to reject new invocations
    /// being added to the vqueue. The capacity configuration will limit this value.
    /// in-flight can be calculated by length - num_pending. = suspended + paused + running.
    #[bilrost(2)]
    pub(crate) stats: VQueueStatistics,
    #[bilrost(3)]
    pub(crate) length: u32,
    /// Number of concurrency tokens being used
    #[bilrost(4)]
    pub(crate) num_tokens_used: u32,
    /// The number of entries waiting to be dequeued. The vector index implies the priority
    #[bilrost(5)]
    pub(crate) num_waiting: smallvec::SmallVec<[u32; EffectivePriority::NUM_PRIORITIES]>,
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

    pub fn status(&self) -> VQueueStatus {
        if self.is_empty() {
            VQueueStatus::Empty
        } else if self.is_paused() {
            VQueueStatus::Paused
        } else {
            VQueueStatus::Active
        }
    }

    pub fn num_waiting(&self, priority: EffectivePriority) -> u32 {
        self.num_waiting
            .get(priority as usize)
            .copied()
            .unwrap_or_default()
    }

    pub fn num_running(&self) -> u32 {
        self.stats.num_running
    }

    pub fn stats(&self) -> &VQueueStatistics {
        &self.stats
    }

    pub fn last_enqueued_ts(&self) -> Option<UniqueTimestamp> {
        self.stats.last_enqueued_at
    }

    pub fn is_paused(&self) -> bool {
        self.is_paused
    }

    fn add_to_waiting(&mut self, priority: EffectivePriority) {
        self.num_waiting.resize(priority as usize + 1, 0);
        self.num_waiting[priority as usize] += 1;
    }

    fn remove_from_waiting(&mut self, priority: EffectivePriority) {
        assert!(self.num_waiting.len() > priority as usize);
        debug_assert!(self.num_waiting[priority as usize] > 0);
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
                self.stats.last_enqueued_at = Some(now);
            }
            Action::StartAttempt {
                visible_at,
                priority,
            } => {
                self.stats.last_start_at = Some(now);
                self.stats.increment_running();
                if !priority.token_held() {
                    self.acquire_token();
                }
                self.remove_from_waiting(priority);

                if priority.is_new()
                    && let VisibleAt::At(visible_since) = visible_at
                {
                    // Only measure queue latency for new items and only consider the item
                    // queuing from the if enqueued_at is set. For entries that were emplaced
                    // moment it became visible, not the creation ts.
                    let latency_ms = now.milliseconds_since(visible_since);
                    self.stats.update_avg_queue_duration(latency_ms);
                }
            }
            Action::Park {
                should_release_concurrency_token,
                priority,
                was_running,
            } => {
                debug_assert!(self.length > 0);
                if was_running {
                    self.stats.decrement_running();
                } else {
                    self.remove_from_waiting(priority);
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
            Action::YieldRunning { priority } => {
                debug_assert!(self.length > 0);
                self.stats.decrement_running();
                self.add_to_waiting(priority);
            }
            Action::Complete {
                was_waiting,
                priority,
            } => {
                debug_assert!(self.length > 0);
                self.length -= 1;
                self.stats.last_completion_at = Some(now);
                if was_waiting {
                    self.remove_from_waiting(priority);
                } else {
                    self.stats.decrement_running();
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
    #[bilrost(tag(1))]
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
    #[bilrost(tag(3), message)]
    /// An entry from inbox stage is being moved to Run
    StartAttempt {
        visible_at: VisibleAt,
        priority: EffectivePriority,
    },
    #[bilrost(tag(4), message)]
    Park {
        priority: EffectivePriority,
        should_release_concurrency_token: bool,
        was_running: bool,
    },
    // Wake up after pause or suspend.
    #[bilrost(tag(5), message)]
    WakeUp { priority: EffectivePriority },
    // Item moved from running back to waiting
    #[bilrost(tag(6), message)]
    YieldRunning { priority: EffectivePriority },
    // Execution has ended (failed, succeeded, killed, etc.)
    #[bilrost(tag(7), message)]
    Complete {
        // Must be the latest priority assigned to the entry (effective priority)
        priority: EffectivePriority,
        was_waiting: bool,
    },
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct Update {
    #[bilrost(1)]
    pub(super) ts: UniqueTimestamp,
    #[bilrost(oneof(2, 3, 4, 5, 6, 7))]
    pub(super) action: Action,
}
