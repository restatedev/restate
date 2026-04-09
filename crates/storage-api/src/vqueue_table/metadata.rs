// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use smallvec::SmallVec;

use restate_clock::WallClock;
use restate_clock::time::MillisSinceEpoch;
use restate_limiter::LimitKey;
use restate_types::clock::UniqueTimestamp;
use restate_types::{LockName, Scope};
use restate_util_string::ReString;

use super::Stage;

#[derive(Debug, Clone, bilrost::Message)]
pub struct VQueueStatistics {
    /// Creation time of this vqueue metadata record.
    #[bilrost(tag(1))]
    pub(crate) created_at: MillisSinceEpoch,
    /// EMA of first-attempt wait time.
    ///
    /// For an entry's first transition to `Run`, this tracks
    /// `run_started_at - first_runnable_at`, where
    /// `first_runnable_at = max(created_at, original_run_at)`.
    ///
    /// This indicates how long brand new work waits before it gets its first execution slot.
    #[bilrost(tag(2))]
    pub(crate) avg_queue_duration_ms: u64,
    /// Last timestamp an entry was moved into `Inbox`.
    ///
    /// This includes initial enqueue as well as transitions that re-enqueue entries
    /// (`Park -> Inbox` and `Run/Inbox -> Inbox`).
    #[bilrost(tag(3))]
    pub(crate) last_enqueued_at: Option<MillisSinceEpoch>,
    /// Last timestamp an entry had its first transition to `Run`.
    ///
    /// This marks when a new entry starts for the first time.
    #[bilrost(tag(4))]
    pub(crate) last_start_at: Option<MillisSinceEpoch>,
    /// Last timestamp an entry completed (`next_stage = Archive`).
    #[bilrost(tag(5))]
    pub(crate) last_completion_at: Option<MillisSinceEpoch>,
    /// Last timestamp an entry transitioned to `Run`.
    ///
    /// This includes both first starts and retries/resumes.
    #[bilrost(tag(6))]
    pub(crate) last_attempt_at: Option<MillisSinceEpoch>,
    /// Number of entries currently in `Inbox`.
    #[bilrost(tag(7))]
    pub(crate) num_inbox: u64,
    /// Number of entries currently in `Park`.
    #[bilrost(tag(8))]
    pub(crate) num_parked: u64,
    /// Number of entries currently in `Run`.
    #[bilrost(tag(9))]
    pub(crate) num_running: u64,
    /// EMA of how long entries stay in `Inbox` before transitioning out of it.
    #[bilrost(tag(10))]
    pub(crate) avg_inbox_duration_ms: u64,
    /// EMA of how long entries stay in `Run` before transitioning out of it.
    #[bilrost(tag(11))]
    pub(crate) avg_run_duration_ms: u64,
    /// EMA of how long entries stay in `Park` before transitioning out of it.
    #[bilrost(tag(12))]
    pub(crate) avg_park_duration_ms: u64,
    /// EMA of end-to-end entry lifetime from first-runnable time to completion.
    #[bilrost(tag(13))]
    pub(crate) avg_end_to_end_duration_ms: u64,
}

impl VQueueStatistics {
    fn new(created_at: MillisSinceEpoch) -> Self {
        Self {
            created_at,
            avg_queue_duration_ms: 0,
            last_enqueued_at: None,
            last_start_at: None,
            last_completion_at: None,
            last_attempt_at: None,
            num_inbox: 0,
            num_parked: 0,
            num_running: 0,
            avg_inbox_duration_ms: 0,
            avg_run_duration_ms: 0,
            avg_park_duration_ms: 0,
            avg_end_to_end_duration_ms: 0,
        }
    }

    fn update_avg_queue_duration(&mut self, latency_ms: u64) {
        self.avg_queue_duration_ms = Self::ema(self.avg_queue_duration_ms, latency_ms);
    }

    fn update_avg_inbox_duration(&mut self, latency_ms: u64) {
        self.avg_inbox_duration_ms = Self::ema(self.avg_inbox_duration_ms, latency_ms);
    }

    fn update_avg_run_duration(&mut self, latency_ms: u64) {
        self.avg_run_duration_ms = Self::ema(self.avg_run_duration_ms, latency_ms);
    }

    fn update_avg_park_duration(&mut self, latency_ms: u64) {
        self.avg_park_duration_ms = Self::ema(self.avg_park_duration_ms, latency_ms);
    }

    fn update_avg_end_to_end_duration(&mut self, latency_ms: u64) {
        self.avg_end_to_end_duration_ms = Self::ema(self.avg_end_to_end_duration_ms, latency_ms);
    }

    fn ema(previous: u64, sample_ms: u64) -> u64 {
        if previous == 0 {
            sample_ms
        } else {
            // Exponential moving average with alpha=0.05.
            ((previous as f64 * 0.95) + (sample_ms as f64 * 0.05)).ceil() as u64
        }
    }

    fn record_stage_exit(&mut self, stage: Stage, stage_dwell_ms: u64) {
        match stage {
            Stage::Unknown | Stage::Archive => {}
            Stage::Inbox => self.update_avg_inbox_duration(stage_dwell_ms),
            Stage::Run => self.update_avg_run_duration(stage_dwell_ms),
            Stage::Park => self.update_avg_park_duration(stage_dwell_ms),
        }
    }

    pub const fn created_at(&self) -> MillisSinceEpoch {
        self.created_at
    }

    pub const fn avg_queue_duration_ms(&self) -> u64 {
        self.avg_queue_duration_ms
    }

    pub const fn avg_inbox_duration_ms(&self) -> u64 {
        self.avg_inbox_duration_ms
    }

    pub const fn avg_run_duration_ms(&self) -> u64 {
        self.avg_run_duration_ms
    }

    pub const fn avg_park_duration_ms(&self) -> u64 {
        self.avg_park_duration_ms
    }

    pub const fn avg_end_to_end_duration_ms(&self) -> u64 {
        self.avg_end_to_end_duration_ms
    }

    pub const fn last_enqueued_at(&self) -> Option<MillisSinceEpoch> {
        self.last_enqueued_at
    }

    pub const fn last_start_at(&self) -> Option<MillisSinceEpoch> {
        self.last_start_at
    }

    pub const fn last_attempt_at(&self) -> Option<MillisSinceEpoch> {
        self.last_attempt_at
    }

    pub const fn last_completion_at(&self) -> Option<MillisSinceEpoch> {
        self.last_completion_at
    }

    pub const fn num_inbox(&self) -> u64 {
        self.num_inbox
    }

    pub const fn num_parked(&self) -> u64 {
        self.num_parked
    }

    pub const fn num_running(&self) -> u64 {
        self.num_running
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct VQueueMeta {
    /// if true, the vqueue is paused, we don't pop entries from it until it's resumed.
    #[bilrost(tag(1))]
    is_paused: bool,

    #[bilrost(tag(2))]
    pub(crate) stats: VQueueStatistics,
    #[bilrost(tag(3))]
    pub(crate) scope: Option<Scope>,
    #[bilrost(tag(4))]
    pub(crate) limit_key: LimitKey<ReString>,
    #[bilrost(tag(5))]
    lock_name: Option<LockName>,
}

impl VQueueMeta {
    pub fn new(
        scope: Option<Scope>,
        limit_key: LimitKey<ReString>,
        lock_name: Option<LockName>,
    ) -> Self {
        Self {
            is_paused: false,
            stats: VQueueStatistics::new(WallClock::recent_ms()),
            scope,
            limit_key,
            lock_name,
        }
    }

    pub fn scope(&self) -> &Option<Scope> {
        &self.scope
    }

    pub fn scope_ref(&self) -> &Option<Scope> {
        &self.scope
    }

    pub fn requires_locking(&self) -> bool {
        self.lock_name.is_some()
    }

    pub fn lock_name(&self) -> &Option<LockName> {
        &self.lock_name
    }

    pub fn limit_key(&self) -> &LimitKey<ReString> {
        &self.limit_key
    }

    /// Total number of entries (ready + paused + running + suspended + scheduled), but it doesn't
    /// include completed or failed entries. This is the length that is used to reject new invocations
    /// being added to the vqueue. The capacity configuration will limit this value.
    pub fn len(&self) -> u64 {
        self.stats.num_inbox + self.stats.num_running + self.stats.num_parked
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn total_waiting(&self) -> u64 {
        self.stats.num_inbox
    }

    fn decrement_stage(&mut self, stage: Stage) {
        match stage {
            Stage::Inbox => self.stats.num_inbox -= 1,
            Stage::Park => self.stats.num_parked -= 1,
            Stage::Run => self.stats.num_running -= 1,
            Stage::Archive => {}
            _ => {}
        }
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
        self.stats.num_running > 0 || (self.stats.num_inbox > 0 && !self.is_paused())
    }

    pub fn num_running(&self) -> u32 {
        self.stats
            .num_running
            .try_into()
            .expect("cannot run more than u32::MAX items concurrently")
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

    pub fn is_paused(&self) -> bool {
        self.is_paused
    }

    pub fn apply_update(&mut self, update: &Update) -> anyhow::Result<()> {
        let now = update.ts;
        let now_ms = now.to_unix_millis();
        // Note to future authors: This match needs to continue to work even when
        // processing old/deprecated/removed actions. Therefore, removed actions should
        // not be removed from the enum to avoid falling into the Unknown case.
        match update.action {
            Action::Unknown => {
                anyhow::bail!("Unrecognized vqueue action: {update:?}")
            }
            Action::PauseVQueue {} => {
                self.is_paused = true;
            }
            Action::ResumeVQueue {} => {
                self.is_paused = false;
            }
            Action::Move {
                prev_stage,
                next_stage,
                ref metrics,
            } => {
                if let Some(previous_stage) = prev_stage {
                    let stage_dwell_ms =
                        now_ms.saturating_sub_ms(metrics.last_transition_at.to_unix_millis());
                    self.stats.record_stage_exit(previous_stage, stage_dwell_ms);
                    self.decrement_stage(previous_stage);
                }

                match next_stage {
                    Stage::Unknown => {}
                    Stage::Inbox => {
                        self.stats.num_inbox += 1;
                        self.stats.last_enqueued_at = Some(now_ms);
                    }
                    Stage::Run => {
                        self.stats.num_running += 1;
                        self.stats.last_attempt_at = Some(now_ms);

                        if !metrics.has_started {
                            let first_wait_ms = now_ms.saturating_sub_ms(metrics.first_runnable_at);
                            self.stats.update_avg_queue_duration(first_wait_ms);

                            let ts = Some(now_ms);
                            self.stats.last_start_at = ts;
                        }
                    }
                    Stage::Park => {
                        self.stats.num_parked += 1;
                    }
                    Stage::Archive => {
                        // We do not keep a counter for archived items as we can't cheaply
                        // decrement it when we delete them (through compaction filter for example).
                        self.stats.last_completion_at = Some(now_ms);
                        let end_to_end_ms = now_ms.saturating_sub_ms(metrics.first_runnable_at);
                        self.stats.update_avg_end_to_end_duration(end_to_end_ms);
                    }
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
#[derive(Clone, Default, Debug, bilrost::Message)]
pub struct VQueueMetaUpdates {
    #[bilrost(1)]
    pub updates: SmallVec<[Update; VQueueMetaUpdates::INLINED_UPDATES]>,
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct MoveMetrics {
    /// Timestamp of the entry's previous stage transition.
    #[bilrost(tag(1))]
    pub last_transition_at: UniqueTimestamp,
    /// Whether the entry has started at least once before this transition.
    #[bilrost(tag(2))]
    pub has_started: bool,
    /// Earliest timestamp at which the entry can realistically start.
    #[bilrost(tag(3))]
    pub first_runnable_at: MillisSinceEpoch,
}

impl VQueueMetaUpdates {
    pub const INLINED_UPDATES: usize = 1;

    pub fn new(update: Update) -> Self {
        let updates = smallvec::smallvec_inline![update];
        Self { updates }
    }

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
    /// An item has moved from one stage to another
    ///
    /// if previous_stage is None, the item is new.
    /// if new_stage is Archive, the item has completed.
    #[bilrost(tag(2), message)]
    Move {
        prev_stage: Option<Stage>,
        next_stage: Stage,
        metrics: MoveMetrics,
    },
    #[bilrost(tag(3), message)]
    PauseVQueue {},
    #[bilrost(tag(4), message)]
    ResumeVQueue {},
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct Update {
    #[bilrost(tag(1))]
    pub(super) ts: UniqueTimestamp,
    #[bilrost(oneof(2, 3, 4))]
    pub(super) action: Action,
}

impl Update {
    #[inline]
    pub fn new(ts: UniqueTimestamp, action: Action) -> Self {
        Self { ts, action }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE_TS_MS: u64 = 1_744_000_000_000;

    fn ts(unix_ms: u64) -> UniqueTimestamp {
        UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(unix_ms))
    }

    fn metrics(
        last_transition_at_ms: u64,
        first_runnable_at_ms: u64,
        has_started: bool,
    ) -> MoveMetrics {
        MoveMetrics {
            last_transition_at: ts(last_transition_at_ms),
            has_started,
            first_runnable_at: MillisSinceEpoch::new(first_runnable_at_ms),
        }
    }

    #[test]
    fn first_run_wait_uses_created_at_when_original_run_at_is_in_the_past() {
        let mut meta = VQueueMeta::new(None, LimitKey::None, None);
        let created_at = ts(BASE_TS_MS + 10_000);

        meta.apply_update(&Update::new(
            created_at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 10_000, BASE_TS_MS + 10_000, false),
            },
        ))
        .unwrap();

        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 12_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Run,
                metrics: metrics(BASE_TS_MS + 10_000, BASE_TS_MS + 10_000, false),
            },
        ))
        .unwrap();

        assert_eq!(meta.stats.avg_queue_duration_ms, 2_000);
        assert_eq!(
            meta.stats.last_start_at,
            Some(MillisSinceEpoch::new(BASE_TS_MS + 12_000))
        );
        assert_eq!(
            meta.stats.last_attempt_at,
            Some(MillisSinceEpoch::new(BASE_TS_MS + 12_000))
        );

        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 13_000),
            Action::Move {
                prev_stage: Some(Stage::Run),
                next_stage: Stage::Run,
                metrics: metrics(BASE_TS_MS + 12_000, BASE_TS_MS + 10_000, true),
            },
        ))
        .unwrap();

        assert_eq!(meta.stats.avg_queue_duration_ms, 2_000);
    }

    #[test]
    fn first_run_wait_uses_original_run_at_when_it_is_in_the_future() {
        let mut meta = VQueueMeta::new(None, LimitKey::None, None);
        let created_at = ts(BASE_TS_MS + 10_000);

        meta.apply_update(&Update::new(
            created_at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 10_000, BASE_TS_MS + 12_000, false),
            },
        ))
        .unwrap();

        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 14_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Run,
                metrics: metrics(BASE_TS_MS + 10_000, BASE_TS_MS + 12_000, false),
            },
        ))
        .unwrap();

        assert_eq!(meta.stats.avg_queue_duration_ms, 2_000);
    }

    #[test]
    fn stage_and_end_to_end_emas_are_updated_on_stage_exits() {
        let mut meta = VQueueMeta::new(None, LimitKey::None, None);
        let created_at = ts(BASE_TS_MS + 1_000);

        meta.apply_update(&Update::new(
            created_at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 1_000, BASE_TS_MS + 2_000, false),
            },
        ))
        .unwrap();

        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 2_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Run,
                metrics: metrics(BASE_TS_MS + 1_000, BASE_TS_MS + 2_000, false),
            },
        ))
        .unwrap();

        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 5_000),
            Action::Move {
                prev_stage: Some(Stage::Run),
                next_stage: Stage::Park,
                metrics: metrics(BASE_TS_MS + 2_000, BASE_TS_MS + 2_000, true),
            },
        ))
        .unwrap();

        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 9_000),
            Action::Move {
                prev_stage: Some(Stage::Park),
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 5_000, BASE_TS_MS + 2_000, true),
            },
        ))
        .unwrap();

        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 15_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Archive,
                metrics: metrics(BASE_TS_MS + 9_000, BASE_TS_MS + 2_000, true),
            },
        ))
        .unwrap();

        assert_eq!(meta.stats.avg_inbox_duration_ms, 1_250);
        assert_eq!(meta.stats.avg_run_duration_ms, 3_000);
        assert_eq!(meta.stats.avg_park_duration_ms, 4_000);
        assert_eq!(meta.stats.avg_end_to_end_duration_ms, 13_000);

        assert_eq!(meta.stats.num_inbox, 0);
        assert_eq!(meta.stats.num_running, 0);
        assert_eq!(meta.stats.num_parked, 0);
        assert_eq!(
            meta.stats.last_completion_at,
            Some(MillisSinceEpoch::new(BASE_TS_MS + 15_000))
        );
    }
}
