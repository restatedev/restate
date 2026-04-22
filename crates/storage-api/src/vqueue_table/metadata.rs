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

use restate_clock::time::MillisSinceEpoch;
use restate_limiter::LimitKey;
use restate_types::clock::UniqueTimestamp;
use restate_types::{LockName, LockNameRef, Scope, ServiceName};
use restate_util_string::ReString;

use super::Stage;

#[derive(Debug, Clone, bilrost::Message)]
pub struct VQueueStatistics {
    /// Creation time of this vqueue metadata record.
    #[bilrost(tag(1))]
    pub(crate) created_at: UniqueTimestamp,
    /// Exponential moving average (EMA) of first-attempt wait time.
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
    /// This covers items enqueued for the first time only.
    #[bilrost(tag(3))]
    pub(crate) last_enqueued_at: Option<UniqueTimestamp>,
    /// Last timestamp an entry had its first transition to `Run`.
    ///
    /// This marks when a new entry starts for the first time.
    #[bilrost(tag(4))]
    pub(crate) last_start_at: Option<UniqueTimestamp>,
    /// Last timestamp an entry completed (transitioned into `Finished`)
    #[bilrost(tag(5))]
    pub(crate) last_finish_at: Option<UniqueTimestamp>,
    /// Last timestamp an entry transitioned to `Run`.
    ///
    /// This includes both first starts and retries/resumes.
    #[bilrost(tag(6))]
    pub(crate) last_attempt_at: Option<UniqueTimestamp>,
    /// Number of entries currently in `inbox` stage.
    #[bilrost(tag(7))]
    pub(crate) num_inbox: u64,
    /// Number of entries currently in `suspended` stage.
    #[bilrost(tag(8))]
    pub(crate) num_suspended: u64,
    /// Number of entries currently in `paused` stage.
    #[bilrost(tag(9))]
    pub(crate) num_paused: u64,
    /// Number of entries currently in `running` stage.
    #[bilrost(tag(10))]
    pub(crate) num_running: u64,
    /// How many entries are in the `Finish` stage. When deleting entries from
    /// the `Finished` stage, we should decrement this counter. The vqueue becomes
    /// obsolete when it's completely empty (all counters are zero).
    #[bilrost(tag(11))]
    pub(crate) num_finished: u64,
    /// Exponential moving average (EMA) of how long entries stay in `Inbox` before transitioning out of it.
    #[bilrost(tag(12))]
    pub(crate) avg_inbox_duration_ms: u64,
    /// Exponential moving average (EMA) of how long entries stay in `Run` before transitioning out of it.
    #[bilrost(tag(13))]
    pub(crate) avg_run_duration_ms: u64,
    /// Exponential moving average (EMA) of how long entries stay in `Suspended` before transitioning out of it.
    #[bilrost(tag(14))]
    pub(crate) avg_suspension_duration_ms: u64,
    /// Exponential moving average (EMA) of end-to-end entry lifetime from first-runnable time to completion.
    /// Note that this only tracks entries that were not killed/cancelled or failed/paused.
    #[bilrost(tag(15))]
    pub(crate) avg_end_to_end_duration_ms: u64,
}

impl VQueueStatistics {
    fn new(created_at: UniqueTimestamp) -> Self {
        Self {
            created_at,
            avg_queue_duration_ms: 0,
            last_enqueued_at: None,
            last_start_at: None,
            last_finish_at: None,
            last_attempt_at: None,
            num_inbox: 0,
            num_suspended: 0,
            num_paused: 0,
            num_running: 0,
            num_finished: 0,
            avg_inbox_duration_ms: 0,
            avg_run_duration_ms: 0,
            avg_suspension_duration_ms: 0,
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

    fn update_avg_suspension_duration(&mut self, latency_ms: u64) {
        self.avg_suspension_duration_ms = Self::ema(self.avg_suspension_duration_ms, latency_ms);
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
            Stage::Unknown | Stage::Finished => {}
            Stage::Inbox => self.update_avg_inbox_duration(stage_dwell_ms),
            Stage::Running => self.update_avg_run_duration(stage_dwell_ms),
            Stage::Suspended => self.update_avg_suspension_duration(stage_dwell_ms),
            Stage::Paused => { /* tracking avg paused time is pointless */ }
        }
    }

    pub const fn created_at(&self) -> UniqueTimestamp {
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

    pub const fn avg_suspension_duration_ms(&self) -> u64 {
        self.avg_suspension_duration_ms
    }

    pub const fn avg_end_to_end_duration_ms(&self) -> u64 {
        self.avg_end_to_end_duration_ms
    }

    pub const fn last_enqueued_at(&self) -> Option<UniqueTimestamp> {
        self.last_enqueued_at
    }

    pub const fn last_start_at(&self) -> Option<UniqueTimestamp> {
        self.last_start_at
    }

    pub const fn last_attempt_at(&self) -> Option<UniqueTimestamp> {
        self.last_attempt_at
    }

    pub const fn last_finish_at(&self) -> Option<UniqueTimestamp> {
        self.last_finish_at
    }

    pub const fn num_inbox(&self) -> u64 {
        self.num_inbox
    }

    pub const fn num_paused(&self) -> u64 {
        self.num_paused
    }

    pub const fn num_suspended(&self) -> u64 {
        self.num_suspended
    }

    pub const fn num_running(&self) -> u64 {
        self.num_running
    }

    pub const fn num_finished(&self) -> u64 {
        self.num_finished
    }
}

/// How vqueue metadata links to services
#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message)]
pub enum VQueueLinkRef<'a> {
    /// The vqueue is unlinked
    #[bilrost(empty)]
    None,
    /// The vqueue is linked to a lock (service + key)
    #[bilrost(tag(5))]
    Lock(LockNameRef<'a>),
    /// The vqueue is linked to a certain service
    #[bilrost(tag(6))]
    Service(&'a str),
}

/// How vqueue metadata links to services
#[derive(Debug, Clone, bilrost::Oneof, bilrost::Message)]
pub enum VQueueLink {
    /// The vqueue is unlinked
    #[bilrost(empty)]
    None,
    /// The vqueue is linked to a lock (service + key)
    #[bilrost(tag(5))]
    Lock(LockName),
    /// The vqueue is linked to a certain service
    #[bilrost(tag(6))]
    Service(ServiceName),
}

/// Borrowing version of VQueueMeta.
///
/// NOTE: keep in-sync with [`VQueueMeta`]
#[derive(Debug, Clone, bilrost::Message)]
pub struct VQueueMetaRef<'a> {
    /// if true, the vqueue is paused, we don't pop entries from it until it's resumed.
    #[bilrost(tag(1))]
    pub queue_is_paused: bool,

    #[bilrost(tag(2))]
    pub stats: VQueueStatistics,
    #[bilrost(tag(3))]
    pub scope: Option<&'a str>,
    #[bilrost(tag(4))]
    pub limit_key: LimitKey<&'a str>,
    #[bilrost(oneof(5, 6))]
    pub link: VQueueLinkRef<'a>,
}

impl<'a> VQueueMetaRef<'a> {
    /// A vqueue is considered active when it's of interest to the scheduler.
    ///
    /// The scheduler cares about vqueues that have entries that are already running or that are waiting
    /// to run. With some special rules to consider when the queue is paused. When the vqueue is
    /// paused, the scheduler will only be interested in its "running" entries and not in its
    /// waiting entries. Therefore, it will remain to be "active" as long as it has running
    /// entries. Once running entries are moved to waiting or completed, the vqueue is be
    /// considered dormant until it's unpaused.
    pub fn is_active(&self) -> bool {
        self.stats.num_running > 0 || (self.stats.num_inbox > 0 && !self.queue_is_paused)
    }

    pub fn lock_name(&self) -> Option<&LockNameRef<'_>> {
        match self.link {
            VQueueLinkRef::Lock(ref lock_name) => Some(lock_name),
            _ => None,
        }
    }

    pub fn service_name(&self) -> Option<&str> {
        match self.link {
            VQueueLinkRef::Lock(ref lock_name) => Some(lock_name.service_name()),
            VQueueLinkRef::Service(service) => Some(service),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct VQueueMeta {
    /// if true, the vqueue is paused, we don't pop entries from it until it's resumed.
    #[bilrost(tag(1))]
    queue_is_paused: bool,

    #[bilrost(tag(2))]
    pub(crate) stats: VQueueStatistics,
    #[bilrost(tag(3))]
    pub(crate) scope: Option<Scope>,
    #[bilrost(tag(4))]
    pub(crate) limit_key: LimitKey<ReString>,
    #[bilrost(oneof(5, 6))]
    pub(crate) link: VQueueLink,
}

impl VQueueMeta {
    pub fn new(
        at: UniqueTimestamp,
        scope: Option<Scope>,
        limit_key: LimitKey<ReString>,
        link: VQueueLink,
    ) -> Self {
        Self {
            queue_is_paused: false,
            stats: VQueueStatistics::new(at),
            scope,
            limit_key,
            link,
        }
    }

    pub fn scope(&self) -> &Option<Scope> {
        &self.scope
    }

    pub fn scope_ref(&self) -> &Option<Scope> {
        &self.scope
    }

    pub fn lock_name(&self) -> Option<&LockName> {
        match self.link {
            VQueueLink::Lock(ref lock_name) => Some(lock_name),
            _ => None,
        }
    }

    pub fn service_name(&self) -> Option<&ServiceName> {
        match self.link {
            VQueueLink::Service(ref service_name) => Some(service_name),
            VQueueLink::Lock(ref lock) => Some(lock.service_name()),
            _ => None,
        }
    }

    pub fn limit_key(&self) -> &LimitKey<ReString> {
        &self.limit_key
    }

    /// Total number of entries (ready + paused + running + suspended + scheduled), but it doesn't
    /// include completed or failed entries. This is the length that is used to reject new invocations
    /// being added to the vqueue. The capacity configuration will limit this value.
    pub fn len(&self) -> u64 {
        self.stats.num_inbox
            + self.stats.num_running
            + self.stats.num_paused
            + self.stats.num_suspended
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn total_waiting(&self) -> u64 {
        self.stats.num_inbox
    }

    pub fn is_inbox_empty(&self) -> bool {
        self.stats.num_inbox == 0
    }

    fn decrement_stage(&mut self, stage: Stage) {
        match stage {
            Stage::Inbox => self.stats.num_inbox = self.stats.num_inbox.saturating_sub(1),
            Stage::Suspended => {
                self.stats.num_suspended = self.stats.num_suspended.saturating_sub(1)
            }
            Stage::Paused => self.stats.num_paused = self.stats.num_paused.saturating_sub(1),
            Stage::Running => self.stats.num_running = self.stats.num_running.saturating_sub(1),
            Stage::Finished => self.stats.num_finished = self.stats.num_finished.saturating_sub(1),
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
        self.stats.num_running > 0 || (self.stats.num_inbox > 0 && !self.queue_is_paused)
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

    pub fn last_enqueued_ts(&self) -> Option<UniqueTimestamp> {
        self.stats.last_enqueued_at
    }

    pub fn last_start_ts(&self) -> Option<UniqueTimestamp> {
        self.stats.last_start_at
    }

    pub fn queue_is_paused(&self) -> bool {
        self.queue_is_paused
    }

    pub fn apply_update(&mut self, update: &Update) {
        let now = update.ts;
        let now_ms = now.to_unix_millis();
        // Note to future authors: This match needs to continue to work even when
        // processing old/deprecated/removed actions. Therefore, removed actions should
        // not be removed from the enum to avoid falling into the Unknown case.
        match update.action {
            Action::Unknown => {
                panic!("Unrecognized vqueue action: {update:?}")
            }
            Action::PauseVQueue {} => {
                self.queue_is_paused = true;
            }
            Action::ResumeVQueue {} => {
                self.queue_is_paused = false;
            }
            Action::RemoveEntry { stage } => {
                self.decrement_stage(stage);
            }
            Action::Move {
                prev_stage,
                next_stage,
                ref metrics,
            } => {
                if let Some(previous_stage) = prev_stage {
                    let stage_dwell_ms = now.saturating_sub_ms(metrics.last_transition_at);
                    self.stats.record_stage_exit(previous_stage, stage_dwell_ms);
                    self.decrement_stage(previous_stage);
                }

                match next_stage {
                    Stage::Unknown => unreachable!(),
                    Stage::Inbox => {
                        self.stats.num_inbox += 1;
                        if prev_stage.is_none() {
                            self.stats.last_enqueued_at = Some(now);
                        }
                    }
                    Stage::Running => {
                        self.stats.num_running += 1;
                        self.stats.last_attempt_at = Some(now);

                        if !metrics.has_started {
                            let first_wait_ms = now_ms.saturating_sub_ms(metrics.first_runnable_at);
                            self.stats.update_avg_queue_duration(first_wait_ms);

                            self.stats.last_start_at = Some(now);
                        }
                    }
                    Stage::Paused => {
                        self.stats.num_paused += 1;
                    }
                    Stage::Suspended => {
                        self.stats.num_suspended += 1;
                    }
                    Stage::Finished => {
                        self.stats.num_finished += 1;
                        self.stats.last_finish_at = Some(now);
                        if matches!(prev_stage, Some(Stage::Running)) {
                            // Only track entries finishing normally.
                            let end_to_end_ms = now_ms.saturating_sub_ms(metrics.first_runnable_at);
                            self.stats.update_avg_end_to_end_duration(end_to_end_ms);
                        }
                    }
                }
            }
        }
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
    /// if new_stage is Finished, the item has completed.
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
    #[bilrost(tag(5), message)]
    /// An item or have been removed from the (stage)
    RemoveEntry { stage: Stage },
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct Update {
    #[bilrost(tag(1))]
    pub(super) ts: UniqueTimestamp,
    #[bilrost(oneof(2, 3, 4, 5))]
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
    fn avg_queue_duration_tracks_first_attempt_wait() {
        let created_at = ts(BASE_TS_MS + 10_000);
        let mut meta = VQueueMeta::new(created_at, None, LimitKey::None, VQueueLink::None);

        // Enqueue: caller has already computed
        // first_runnable_at = max(created_at, original_run_at).
        meta.apply_update(&Update::new(
            created_at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 10_000, BASE_TS_MS + 12_000, false),
            },
        ));

        // First transition to Running: first-attempt wait is
        // now(14_000) - first_runnable_at(12_000) = 2_000 ms.
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 14_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                metrics: metrics(BASE_TS_MS + 10_000, BASE_TS_MS + 12_000, false),
            },
        ));

        assert_eq!(meta.stats.avg_queue_duration_ms, 2_000);
        assert_eq!(meta.stats.last_start_at, Some(ts(BASE_TS_MS + 14_000)));
        assert_eq!(meta.stats.last_attempt_at, Some(ts(BASE_TS_MS + 14_000)));

        // A subsequent Running→Running transition (has_started = true) must
        // not touch avg_queue_duration_ms or last_start_at — those only track
        // the first attempt. last_attempt_at does advance.
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 15_000),
            Action::Move {
                prev_stage: Some(Stage::Running),
                next_stage: Stage::Running,
                metrics: metrics(BASE_TS_MS + 14_000, BASE_TS_MS + 12_000, true),
            },
        ));

        assert_eq!(meta.stats.avg_queue_duration_ms, 2_000);
        assert_eq!(meta.stats.last_start_at, Some(ts(BASE_TS_MS + 14_000)));
        assert_eq!(meta.stats.last_attempt_at, Some(ts(BASE_TS_MS + 15_000)));
    }

    #[test]
    fn stage_emas_update_on_transitions() {
        // Inbox→Running→Suspended→Inbox→Finished exercises every tracked
        // stage-dwell EMA. Inbox gets two samples so the EMA blend path
        // (not just the initial assignment) is covered.
        let created_at = ts(BASE_TS_MS + 1_000);
        let mut meta = VQueueMeta::new(created_at, None, LimitKey::None, VQueueLink::None);

        meta.apply_update(&Update::new(
            created_at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 1_000, BASE_TS_MS + 2_000, false),
            },
        ));
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 2_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                metrics: metrics(BASE_TS_MS + 1_000, BASE_TS_MS + 2_000, false),
            },
        ));
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 5_000),
            Action::Move {
                prev_stage: Some(Stage::Running),
                next_stage: Stage::Suspended,
                metrics: metrics(BASE_TS_MS + 2_000, BASE_TS_MS + 2_000, true),
            },
        ));
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 9_000),
            Action::Move {
                prev_stage: Some(Stage::Suspended),
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 5_000, BASE_TS_MS + 2_000, true),
            },
        ));
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 15_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Finished,
                metrics: metrics(BASE_TS_MS + 9_000, BASE_TS_MS + 2_000, true),
            },
        ));

        // Inbox: EMA(0, 1_000) = 1_000, then EMA(1_000, 6_000) = 1_250.
        assert_eq!(meta.stats.avg_inbox_duration_ms, 1_250);
        assert_eq!(meta.stats.avg_run_duration_ms, 3_000);
        assert_eq!(meta.stats.avg_suspension_duration_ms, 4_000);

        assert_eq!(meta.stats.num_inbox, 0);
        assert_eq!(meta.stats.num_running, 0);
        assert_eq!(meta.stats.num_suspended, 0);
        assert_eq!(meta.stats.num_finished, 1);
        assert_eq!(meta.stats.last_finish_at, Some(ts(BASE_TS_MS + 15_000)));
    }

    #[test]
    fn end_to_end_updates_only_on_finish_from_running() {
        // Finish-from-Running updates avg_end_to_end_duration_ms.
        let created_at = ts(BASE_TS_MS);
        let mut running_finish =
            VQueueMeta::new(created_at, None, LimitKey::None, VQueueLink::None);
        running_finish.apply_update(&Update::new(
            created_at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS, BASE_TS_MS, false),
            },
        ));
        running_finish.apply_update(&Update::new(
            ts(BASE_TS_MS + 1_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                metrics: metrics(BASE_TS_MS, BASE_TS_MS, false),
            },
        ));
        running_finish.apply_update(&Update::new(
            ts(BASE_TS_MS + 5_000),
            Action::Move {
                prev_stage: Some(Stage::Running),
                next_stage: Stage::Finished,
                metrics: metrics(BASE_TS_MS + 1_000, BASE_TS_MS, true),
            },
        ));
        // end_to_end = now_ms(5_000) - first_runnable_at(0) = 5_000.
        assert_eq!(running_finish.stats.avg_end_to_end_duration_ms, 5_000);

        // Finish from anywhere other than Running (e.g. cancel from Inbox)
        // must leave avg_end_to_end_duration_ms untouched.
        let mut inbox_finish = VQueueMeta::new(created_at, None, LimitKey::None, VQueueLink::None);
        inbox_finish.apply_update(&Update::new(
            created_at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS, BASE_TS_MS, false),
            },
        ));
        inbox_finish.apply_update(&Update::new(
            ts(BASE_TS_MS + 10_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Finished,
                metrics: metrics(BASE_TS_MS, BASE_TS_MS, false),
            },
        ));
        assert_eq!(inbox_finish.stats.avg_end_to_end_duration_ms, 0);
    }

    #[test]
    fn vqueue_meta_borrowed_decode_is_correct() {
        use bilrost::{BorrowedMessage, Message};

        let owned = VQueueMeta {
            queue_is_paused: true,
            stats: VQueueStatistics {
                created_at: ts(BASE_TS_MS + 1),
                avg_queue_duration_ms: 2,
                last_enqueued_at: Some(ts(BASE_TS_MS + 3)),
                last_start_at: Some(ts(BASE_TS_MS + 4)),
                last_finish_at: Some(ts(BASE_TS_MS + 5)),
                last_attempt_at: Some(ts(BASE_TS_MS + 6)),
                num_inbox: 7,
                num_paused: 8,
                num_suspended: 800,
                num_running: 9,
                num_finished: 10,
                avg_inbox_duration_ms: 11,
                avg_run_duration_ms: 12,
                avg_suspension_duration_ms: 13,
                avg_end_to_end_duration_ms: 14,
            },
            scope: Some(Scope::new("scope-a")),
            limit_key: "tenant-1/user-1".parse::<LimitKey<ReString>>().unwrap(),
            link: VQueueLink::Lock(LockName::parse("service-a/key-a").unwrap()),
        };

        let encoded = owned.encode_to_bytes();
        let borrowed = VQueueMetaRef::decode_borrowed(&encoded).unwrap();

        assert_eq!(borrowed.queue_is_paused, owned.queue_is_paused);

        assert_eq!(borrowed.scope, Some("scope-a"));
        assert_eq!(borrowed.limit_key.to_string(), "tenant-1/user-1");
        assert_eq!(
            borrowed.limit_key.level1().map(|value| value.as_str()),
            Some("tenant-1")
        );
        assert_eq!(
            borrowed.limit_key.level2().map(|value| value.as_str()),
            Some("user-1")
        );
        let lock_name = borrowed.lock_name().expect("lock_name should exist");
        assert_eq!(lock_name.service_name(), "service-a");
        assert_eq!(lock_name.key(), "key-a");

        let service_name = borrowed.service_name().expect("service_name should exist");
        assert_eq!(service_name, "service-a");

        // just a few sanity checks for stats. Those are owned anyway.
        assert_eq!(borrowed.stats.created_at(), owned.stats.created_at());
        assert_eq!(
            borrowed.stats.avg_queue_duration_ms(),
            owned.stats.avg_queue_duration_ms()
        );
        assert_eq!(borrowed.stats.num_inbox(), owned.stats.num_inbox());
        assert_eq!(borrowed.is_active(), owned.is_active());
    }
}
