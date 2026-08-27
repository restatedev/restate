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
use restate_limiter::LimitKey;
use restate_types::clock::UniqueTimestamp;
use restate_types::{LockName, LockNameRef, Scope, ServiceName};
use restate_util_string::ReString;

use super::Stage;
use super::stats::WaitStats;

#[derive(Debug, Clone, bilrost::Message)]
pub struct VQueueStatistics {
    /// Creation time of this vqueue metadata record.
    #[bilrost(tag(1), encoding(fixed))]
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
    #[bilrost(tag(3), encoding(fixed))]
    pub(crate) last_enqueued_at: Option<UniqueTimestamp>,
    /// Last timestamp an entry had its first transition to `Run`.
    ///
    /// This marks when a new entry starts for the first time.
    #[bilrost(tag(4), encoding(fixed))]
    pub(crate) last_start_at: Option<UniqueTimestamp>,
    /// Last timestamp an entry completed (transitioned into `Finished`)
    #[bilrost(tag(5), encoding(fixed))]
    pub(crate) last_finish_at: Option<UniqueTimestamp>,
    /// Last timestamp an entry transitioned to `Run`.
    ///
    /// This includes both first starts and retries/resumes.
    #[bilrost(tag(6), encoding(fixed))]
    pub(crate) last_attempt_at: Option<UniqueTimestamp>,
    /// Number of entries currently in `inbox` stage.
    #[bilrost(tag(7), encoding(fixed))]
    pub(crate) num_inbox: u64,
    /// Number of entries currently in `suspended` stage.
    #[bilrost(tag(8), encoding(fixed))]
    pub(crate) num_suspended: u64,
    /// Number of entries currently in `paused` stage.
    #[bilrost(tag(9), encoding(fixed))]
    pub(crate) num_paused: u64,
    /// Number of entries currently in `running` stage.
    #[bilrost(tag(10), encoding(fixed))]
    pub(crate) num_running: u64,
    /// How many entries are in the `Finish` stage. When deleting entries from
    /// the `Finished` stage, we should decrement this counter. The vqueue becomes
    /// obsolete when it's completely empty (all counters are zero).
    #[bilrost(tag(11), encoding(fixed))]
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

    /// Exponential moving average (EMA) of the various statistics
    /// emitted by the scheduler while attempting to run items
    /// from this queue.
    #[bilrost(tag(16))]
    pub(crate) avg_wait_stats: WaitStats,
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
            avg_wait_stats: WaitStats::default(),
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

    pub const fn avg_wait_stats(&self) -> &WaitStats {
        &self.avg_wait_stats
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

    fn increment_stage(&mut self, stage: Stage) {
        match stage {
            Stage::Inbox => self.stats.num_inbox += 1,
            Stage::Suspended => self.stats.num_suspended += 1,
            Stage::Paused => self.stats.num_paused += 1,
            Stage::Running => self.stats.num_running += 1,
            Stage::Finished => self.stats.num_finished += 1,
            Stage::Unknown => unreachable!(),
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
        visit_update(update, |effect| match effect {
            UpdateEffect::QueuePaused(paused) => self.queue_is_paused = paused,
            UpdateEffect::DecrementStage(stage) => self.decrement_stage(stage),
            UpdateEffect::IncrementStage(stage) => self.increment_stage(stage),
            UpdateEffect::LastEnqueuedAt(ts) => self.stats.last_enqueued_at = Some(ts),
            UpdateEffect::LastStartAt(ts) => self.stats.last_start_at = Some(ts),
            UpdateEffect::LastAttemptAt(ts) => self.stats.last_attempt_at = Some(ts),
            UpdateEffect::LastFinishAt(ts) => self.stats.last_finish_at = Some(ts),
            UpdateEffect::QueueDuration(sample) => self.stats.update_avg_queue_duration(sample),
            UpdateEffect::InboxDuration(sample) => self.stats.update_avg_inbox_duration(sample),
            UpdateEffect::RunDuration(sample) => self.stats.update_avg_run_duration(sample),
            UpdateEffect::SuspensionDuration(sample) => {
                self.stats.update_avg_suspension_duration(sample)
            }
            UpdateEffect::EndToEndDuration(sample) => {
                self.stats.update_avg_end_to_end_duration(sample)
            }
            UpdateEffect::WaitStats(sample) => self.stats.avg_wait_stats.ema_apply(sample),
        });
    }
}

#[derive(Clone, Copy)]
enum UpdateEffect {
    QueuePaused(bool),
    DecrementStage(Stage),
    IncrementStage(Stage),
    LastEnqueuedAt(UniqueTimestamp),
    LastStartAt(UniqueTimestamp),
    LastAttemptAt(UniqueTimestamp),
    LastFinishAt(UniqueTimestamp),
    QueueDuration(u64),
    InboxDuration(u64),
    RunDuration(u64),
    SuspensionDuration(u64),
    EndToEndDuration(u64),
    WaitStats(WaitStats),
}

fn visit_update(update: &Update, mut visitor: impl FnMut(UpdateEffect)) {
    let now = update.ts;
    let now_ms = now.to_unix_millis();

    // Note to future authors: This match needs to continue to work even when
    // processing old/deprecated/removed actions. Therefore, removed actions should
    // not be removed from the enum to avoid falling into the Unknown case.
    match update.action {
        Action::Unknown => panic!("Unrecognized vqueue action: {update:?}"),
        Action::PauseVQueue {} => visitor(UpdateEffect::QueuePaused(true)),
        Action::ResumeVQueue {} => visitor(UpdateEffect::QueuePaused(false)),
        Action::RemoveEntry { stage } => visitor(UpdateEffect::DecrementStage(stage)),
        Action::Move {
            prev_stage,
            next_stage,
            ref metrics,
        } => {
            if let Some(previous_stage) = prev_stage {
                let stage_dwell_ms = now.saturating_sub_ms(metrics.last_transition_at);
                match previous_stage {
                    Stage::Inbox => visitor(UpdateEffect::InboxDuration(stage_dwell_ms)),
                    Stage::Running => visitor(UpdateEffect::RunDuration(stage_dwell_ms)),
                    Stage::Suspended => visitor(UpdateEffect::SuspensionDuration(stage_dwell_ms)),
                    Stage::Unknown | Stage::Paused | Stage::Finished => {}
                }
                visitor(UpdateEffect::DecrementStage(previous_stage));
            }

            if matches!(next_stage, Stage::Unknown) {
                unreachable!();
            }
            visitor(UpdateEffect::IncrementStage(next_stage));

            match next_stage {
                Stage::Unknown => unreachable!(),
                Stage::Inbox if prev_stage.is_none() => {
                    visitor(UpdateEffect::LastEnqueuedAt(now));
                }
                Stage::Running => {
                    visitor(UpdateEffect::LastAttemptAt(now));
                    if let Some(wait_stats) = metrics.scheduler_wait_stats {
                        visitor(UpdateEffect::WaitStats(wait_stats));
                    }

                    if !metrics.has_started {
                        visitor(UpdateEffect::QueueDuration(
                            now_ms.saturating_sub_ms(metrics.first_runnable_at),
                        ));
                        visitor(UpdateEffect::LastStartAt(now));
                    }
                }
                Stage::Finished => {
                    visitor(UpdateEffect::LastFinishAt(now));
                    if matches!(prev_stage, Some(Stage::Running)) {
                        visitor(UpdateEffect::EndToEndDuration(
                            now_ms.saturating_sub_ms(metrics.first_runnable_at),
                        ));
                    }
                }
                Stage::Inbox | Stage::Paused | Stage::Suspended => {}
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, bilrost::Message)]
#[bilrost(distinguished)]
struct CounterUpdate {
    #[bilrost(tag(1))]
    subtract: u64,
    #[bilrost(tag(2))]
    add: u64,
}

impl CounterUpdate {
    fn try_decrement(&mut self) -> bool {
        if self.add > 0 {
            self.add -= 1;
            true
        } else if let Some(subtract) = self.subtract.checked_add(1) {
            self.subtract = subtract;
            true
        } else {
            false
        }
    }

    fn try_increment(&mut self) -> bool {
        if let Some(add) = self.add.checked_add(1) {
            self.add = add;
            true
        } else {
            false
        }
    }

    fn try_compose(self, next: Self) -> Option<Self> {
        let cancelled = self.add.min(next.subtract);
        Some(Self {
            subtract: self.subtract.checked_add(next.subtract - cancelled)?,
            add: next.add.checked_add(self.add - cancelled)?,
        })
    }

    fn try_apply(self, value: u64) -> Option<u64> {
        value.saturating_sub(self.subtract).checked_add(self.add)
    }
}

/// A compact, composable sequence of [`Update`]s used as a RocksDB merge operand.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
#[bilrost(distinguished)]
pub struct UpdateBatch {
    #[bilrost(tag(1))]
    queue_is_paused: Option<bool>,
    #[bilrost(tag(2))]
    inbox_count: CounterUpdate,
    #[bilrost(tag(3))]
    suspended_count: CounterUpdate,
    #[bilrost(tag(4))]
    paused_count: CounterUpdate,
    #[bilrost(tag(5))]
    running_count: CounterUpdate,
    #[bilrost(tag(6))]
    finished_count: CounterUpdate,
    #[bilrost(tag(7), encoding(fixed))]
    last_enqueued_at: Option<UniqueTimestamp>,
    #[bilrost(tag(8), encoding(fixed))]
    last_start_at: Option<UniqueTimestamp>,
    #[bilrost(tag(9), encoding(fixed))]
    last_attempt_at: Option<UniqueTimestamp>,
    #[bilrost(tag(10), encoding(fixed))]
    last_finish_at: Option<UniqueTimestamp>,
    #[bilrost(tag(11))]
    queue_duration_samples: Vec<u64>,
    #[bilrost(tag(12))]
    inbox_duration_samples: Vec<u64>,
    #[bilrost(tag(13))]
    run_duration_samples: Vec<u64>,
    #[bilrost(tag(14))]
    suspension_duration_samples: Vec<u64>,
    #[bilrost(tag(15))]
    end_to_end_duration_samples: Vec<u64>,
    #[bilrost(tag(16))]
    wait_stats_samples: Vec<WaitStats>,
}

impl UpdateBatch {
    /// Appends an update, returning false if its counter effect cannot be represented.
    pub fn try_push(&mut self, update: &Update) -> bool {
        let mut success = true;
        visit_update(update, |effect| {
            if success {
                success = self.try_apply_effect(effect);
            }
        });
        success
    }

    /// Appends another batch, returning false if the composed counters overflow.
    pub fn try_append(&mut self, mut next: Self) -> bool {
        let Some(inbox_count) = self.inbox_count.try_compose(next.inbox_count) else {
            return false;
        };
        let Some(suspended_count) = self.suspended_count.try_compose(next.suspended_count) else {
            return false;
        };
        let Some(paused_count) = self.paused_count.try_compose(next.paused_count) else {
            return false;
        };
        let Some(running_count) = self.running_count.try_compose(next.running_count) else {
            return false;
        };
        let Some(finished_count) = self.finished_count.try_compose(next.finished_count) else {
            return false;
        };

        self.inbox_count = inbox_count;
        self.suspended_count = suspended_count;
        self.paused_count = paused_count;
        self.running_count = running_count;
        self.finished_count = finished_count;
        self.queue_is_paused = next.queue_is_paused.or(self.queue_is_paused);
        self.last_enqueued_at = next.last_enqueued_at.or(self.last_enqueued_at);
        self.last_start_at = next.last_start_at.or(self.last_start_at);
        self.last_attempt_at = next.last_attempt_at.or(self.last_attempt_at);
        self.last_finish_at = next.last_finish_at.or(self.last_finish_at);
        self.queue_duration_samples
            .append(&mut next.queue_duration_samples);
        self.inbox_duration_samples
            .append(&mut next.inbox_duration_samples);
        self.run_duration_samples
            .append(&mut next.run_duration_samples);
        self.suspension_duration_samples
            .append(&mut next.suspension_duration_samples);
        self.end_to_end_duration_samples
            .append(&mut next.end_to_end_duration_samples);
        self.wait_stats_samples.append(&mut next.wait_stats_samples);
        true
    }

    /// Applies this batch to vqueue metadata.
    pub fn try_apply(&self, meta: &mut VQueueMeta) -> bool {
        let Some(num_inbox) = self.inbox_count.try_apply(meta.stats.num_inbox) else {
            return false;
        };
        let Some(num_suspended) = self.suspended_count.try_apply(meta.stats.num_suspended) else {
            return false;
        };
        let Some(num_paused) = self.paused_count.try_apply(meta.stats.num_paused) else {
            return false;
        };
        let Some(num_running) = self.running_count.try_apply(meta.stats.num_running) else {
            return false;
        };
        let Some(num_finished) = self.finished_count.try_apply(meta.stats.num_finished) else {
            return false;
        };

        meta.stats.num_inbox = num_inbox;
        meta.stats.num_suspended = num_suspended;
        meta.stats.num_paused = num_paused;
        meta.stats.num_running = num_running;
        meta.stats.num_finished = num_finished;
        if let Some(queue_is_paused) = self.queue_is_paused {
            meta.queue_is_paused = queue_is_paused;
        }
        if let Some(last_enqueued_at) = self.last_enqueued_at {
            meta.stats.last_enqueued_at = Some(last_enqueued_at);
        }
        if let Some(last_start_at) = self.last_start_at {
            meta.stats.last_start_at = Some(last_start_at);
        }
        if let Some(last_attempt_at) = self.last_attempt_at {
            meta.stats.last_attempt_at = Some(last_attempt_at);
        }
        if let Some(last_finish_at) = self.last_finish_at {
            meta.stats.last_finish_at = Some(last_finish_at);
        }
        for &sample in &self.queue_duration_samples {
            meta.stats.update_avg_queue_duration(sample);
        }
        for &sample in &self.inbox_duration_samples {
            meta.stats.update_avg_inbox_duration(sample);
        }
        for &sample in &self.run_duration_samples {
            meta.stats.update_avg_run_duration(sample);
        }
        for &sample in &self.suspension_duration_samples {
            meta.stats.update_avg_suspension_duration(sample);
        }
        for &sample in &self.end_to_end_duration_samples {
            meta.stats.update_avg_end_to_end_duration(sample);
        }
        for &sample in &self.wait_stats_samples {
            meta.stats.avg_wait_stats.ema_apply(sample);
        }
        true
    }

    fn try_apply_effect(&mut self, effect: UpdateEffect) -> bool {
        match effect {
            UpdateEffect::QueuePaused(paused) => self.queue_is_paused = Some(paused),
            UpdateEffect::DecrementStage(stage) => {
                return match stage {
                    Stage::Inbox => self.inbox_count.try_decrement(),
                    Stage::Running => self.running_count.try_decrement(),
                    Stage::Suspended => self.suspended_count.try_decrement(),
                    Stage::Paused => self.paused_count.try_decrement(),
                    Stage::Finished => self.finished_count.try_decrement(),
                    Stage::Unknown => true,
                };
            }
            UpdateEffect::IncrementStage(stage) => {
                return match stage {
                    Stage::Inbox => self.inbox_count.try_increment(),
                    Stage::Running => self.running_count.try_increment(),
                    Stage::Suspended => self.suspended_count.try_increment(),
                    Stage::Paused => self.paused_count.try_increment(),
                    Stage::Finished => self.finished_count.try_increment(),
                    Stage::Unknown => unreachable!(),
                };
            }
            UpdateEffect::LastEnqueuedAt(ts) => self.last_enqueued_at = Some(ts),
            UpdateEffect::LastStartAt(ts) => self.last_start_at = Some(ts),
            UpdateEffect::LastAttemptAt(ts) => self.last_attempt_at = Some(ts),
            UpdateEffect::LastFinishAt(ts) => self.last_finish_at = Some(ts),
            UpdateEffect::QueueDuration(sample) => self.queue_duration_samples.push(sample),
            UpdateEffect::InboxDuration(sample) => self.inbox_duration_samples.push(sample),
            UpdateEffect::RunDuration(sample) => self.run_duration_samples.push(sample),
            UpdateEffect::SuspensionDuration(sample) => {
                self.suspension_duration_samples.push(sample)
            }
            UpdateEffect::EndToEndDuration(sample) => self.end_to_end_duration_samples.push(sample),
            UpdateEffect::WaitStats(sample) => self.wait_stats_samples.push(sample),
        }
        true
    }
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct MoveMetrics {
    /// Timestamp of the entry's previous stage transition.
    #[bilrost(tag(1), encoding(fixed))]
    pub last_transition_at: UniqueTimestamp,
    /// Whether the entry has started at least once before this transition.
    #[bilrost(tag(2))]
    pub has_started: bool,
    /// Earliest timestamp at which the entry can realistically start.
    #[bilrost(tag(3), encoding(fixed))]
    pub first_runnable_at: MillisSinceEpoch,

    /// The scheduler wait stats. Only populated when this transition is driven by
    /// the scheduler.
    #[bilrost(tag(4))]
    pub scheduler_wait_stats: Option<WaitStats>,
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
        #[bilrost(encoding(fixed))]
        prev_stage: Option<Stage>,
        #[bilrost(encoding(fixed))]
        next_stage: Stage,
        metrics: MoveMetrics,
    },
    #[bilrost(tag(3), message)]
    PauseVQueue {},
    #[bilrost(tag(4), message)]
    ResumeVQueue {},
    #[bilrost(tag(5))]
    /// An item or have been removed from the (stage)
    RemoveEntry { stage: Stage },
}

#[derive(Debug, Clone, bilrost::Message)]
pub struct Update {
    #[bilrost(tag(1), encoding(fixed))]
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
    use bilrost::Message;

    use restate_util_string::RestateString;

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
            scheduler_wait_stats: None,
        }
    }

    fn metrics_with_wait(
        last_transition_at_ms: u64,
        first_runnable_at_ms: u64,
        has_started: bool,
        blocked_on_concurrency_rules_ms: u32,
        blocked_on_invoker_throttling_ms: u32,
    ) -> MoveMetrics {
        MoveMetrics {
            scheduler_wait_stats: Some(WaitStats {
                blocked_on_concurrency_rules_ms,
                blocked_on_invoker_throttling_ms,
                ..WaitStats::default()
            }),
            ..metrics(last_transition_at_ms, first_runnable_at_ms, has_started)
        }
    }

    #[test]
    fn counter_update_preserves_saturating_operation_order() {
        for operations in 0_u16..=u8::MAX.into() {
            let mut update = CounterUpdate::default();
            for index in 0..8 {
                let success = if operations & (1 << index) == 0 {
                    update.try_decrement()
                } else {
                    update.try_increment()
                };
                assert!(success);
            }

            for initial in 0_u64..=8 {
                let mut expected = initial;
                for index in 0..8 {
                    if operations & (1 << index) == 0 {
                        expected = expected.saturating_sub(1);
                    } else {
                        expected += 1;
                    }
                }
                assert_eq!(update.try_apply(initial), Some(expected));
            }

            for split in 0..=8 {
                let mut first = CounterUpdate::default();
                let mut second = CounterUpdate::default();
                for index in 0..8 {
                    let update = if index < split {
                        &mut first
                    } else {
                        &mut second
                    };
                    let success = if operations & (1 << index) == 0 {
                        update.try_decrement()
                    } else {
                        update.try_increment()
                    };
                    assert!(success);
                }
                let composed = first
                    .try_compose(second)
                    .expect("short counter updates should compose");
                for initial in 0_u64..=8 {
                    assert_eq!(composed.try_apply(initial), update.try_apply(initial));
                }
            }
        }

        let mut same_stage_move = CounterUpdate::default();
        assert!(same_stage_move.try_decrement());
        assert!(same_stage_move.try_increment());
        assert_eq!(same_stage_move.try_apply(0), Some(1));
        assert_eq!(same_stage_move.try_apply(1), Some(1));
    }

    #[test]
    fn update_batch_matches_sequential_updates_and_composition() {
        let created_at = ts(BASE_TS_MS);
        let mut initial = VQueueMeta::new(created_at, None, LimitKey::None, VQueueLink::None);
        initial.queue_is_paused = true;
        initial.stats.avg_queue_duration_ms = 500;
        initial.stats.avg_inbox_duration_ms = 600;
        initial.stats.avg_run_duration_ms = 700;
        initial.stats.avg_suspension_duration_ms = 800;
        initial.stats.avg_end_to_end_duration_ms = 900;
        initial.stats.avg_wait_stats = WaitStats {
            blocked_on_concurrency_rules_ms: 1_000,
            blocked_on_invoker_throttling_ms: 500,
            ..WaitStats::default()
        };

        let updates = [
            Update::new(
                ts(BASE_TS_MS + 1_000),
                Action::Move {
                    prev_stage: Some(Stage::Inbox),
                    next_stage: Stage::Inbox,
                    metrics: metrics(BASE_TS_MS, BASE_TS_MS, true),
                },
            ),
            Update::new(
                ts(BASE_TS_MS + 2_000),
                Action::Move {
                    prev_stage: None,
                    next_stage: Stage::Inbox,
                    metrics: metrics(BASE_TS_MS + 2_000, BASE_TS_MS + 2_000, false),
                },
            ),
            Update::new(
                ts(BASE_TS_MS + 3_000),
                Action::Move {
                    prev_stage: Some(Stage::Inbox),
                    next_stage: Stage::Running,
                    metrics: metrics_with_wait(
                        BASE_TS_MS + 2_000,
                        BASE_TS_MS + 2_000,
                        false,
                        2_000,
                        200,
                    ),
                },
            ),
            Update::new(
                ts(BASE_TS_MS + 4_000),
                Action::Move {
                    prev_stage: Some(Stage::Running),
                    next_stage: Stage::Suspended,
                    metrics: metrics(BASE_TS_MS + 3_000, BASE_TS_MS + 2_000, true),
                },
            ),
            Update::new(
                ts(BASE_TS_MS + 5_000),
                Action::Move {
                    prev_stage: Some(Stage::Suspended),
                    next_stage: Stage::Inbox,
                    metrics: metrics(BASE_TS_MS + 4_000, BASE_TS_MS + 2_000, true),
                },
            ),
            Update::new(
                ts(BASE_TS_MS + 6_000),
                Action::Move {
                    prev_stage: Some(Stage::Inbox),
                    next_stage: Stage::Running,
                    metrics: metrics_with_wait(
                        BASE_TS_MS + 5_000,
                        BASE_TS_MS + 2_000,
                        true,
                        0,
                        1_000,
                    ),
                },
            ),
            Update::new(
                ts(BASE_TS_MS + 7_000),
                Action::Move {
                    prev_stage: Some(Stage::Running),
                    next_stage: Stage::Finished,
                    metrics: metrics(BASE_TS_MS + 6_000, BASE_TS_MS + 2_000, true),
                },
            ),
            Update::new(
                ts(BASE_TS_MS + 8_000),
                Action::RemoveEntry {
                    stage: Stage::Finished,
                },
            ),
            Update::new(ts(BASE_TS_MS + 9_000), Action::PauseVQueue {}),
            Update::new(ts(BASE_TS_MS + 10_000), Action::ResumeVQueue {}),
        ];

        let mut expected = initial.clone();
        for update in &updates {
            expected.apply_update(update);
        }

        let mut batch = UpdateBatch::default();
        for update in &updates {
            assert!(batch.try_push(update));
        }
        let mut actual = initial.clone();
        assert!(batch.try_apply(&mut actual));
        assert_eq!(actual.encode_to_bytes(), expected.encode_to_bytes());

        let mut first = UpdateBatch::default();
        for update in &updates[..4] {
            assert!(first.try_push(update));
        }
        let mut second = UpdateBatch::default();
        for update in &updates[4..] {
            assert!(second.try_push(update));
        }
        assert!(first.try_append(second));
        let mut composed = initial;
        assert!(first.try_apply(&mut composed));
        assert_eq!(composed.encode_to_bytes(), expected.encode_to_bytes());
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
    fn blocking_emas_sample_every_run_attempt() {
        // The `avg_wait_stats` EMAs are sampled on EVERY Inbox→Running
        // transition that carries scheduler wait stats, not just the first
        // attempt like `avg_queue_duration_ms`. This test pins that
        // distinction down.
        let created_at = ts(BASE_TS_MS + 1_000);
        let mut meta = VQueueMeta::new(created_at, None, LimitKey::None, VQueueLink::None);

        // Enqueue.
        meta.apply_update(&Update::new(
            created_at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 1_000, BASE_TS_MS + 1_000, false),
            },
        ));

        // First Inbox→Running: 1_000 ms on concurrency rules, 500 ms on global
        // invoker throttling. First sample — EMA equals the sample.
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 2_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                metrics: metrics_with_wait(
                    BASE_TS_MS + 1_000,
                    BASE_TS_MS + 1_000,
                    false,
                    1_000,
                    500,
                ),
            },
        ));
        assert_eq!(
            meta.stats.avg_wait_stats.blocked_on_concurrency_rules_ms,
            1_000
        );
        assert_eq!(
            meta.stats.avg_wait_stats.blocked_on_invoker_throttling_ms,
            500
        );
        assert_eq!(meta.stats.avg_queue_duration_ms, 1_000);

        // Yield back Running→Inbox. Neither the new EMAs nor the old
        // `avg_queue_duration_ms` should move — this is not a Running arm.
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 3_000),
            Action::Move {
                prev_stage: Some(Stage::Running),
                next_stage: Stage::Inbox,
                metrics: metrics(BASE_TS_MS + 2_000, BASE_TS_MS + 1_000, true),
            },
        ));
        assert_eq!(
            meta.stats.avg_wait_stats.blocked_on_concurrency_rules_ms,
            1_000
        );
        assert_eq!(
            meta.stats.avg_wait_stats.blocked_on_invoker_throttling_ms,
            500
        );

        // Second Inbox→Running (a retry, `has_started = true`): 2_000 ms /
        // 0 ms. The new EMAs MUST continue sampling even though has_started.
        // With α = 0.05: 1000*0.95 + 2000*0.05 = 1050, 500*0.95 + 0*0.05 = 475.
        meta.apply_update(&Update::new(
            ts(BASE_TS_MS + 4_000),
            Action::Move {
                prev_stage: Some(Stage::Inbox),
                next_stage: Stage::Running,
                metrics: metrics_with_wait(BASE_TS_MS + 3_000, BASE_TS_MS + 1_000, true, 2_000, 0),
            },
        ));
        assert_eq!(
            meta.stats.avg_wait_stats.blocked_on_concurrency_rules_ms,
            1_050
        );
        assert_eq!(
            meta.stats.avg_wait_stats.blocked_on_invoker_throttling_ms,
            475
        );

        // `avg_queue_duration_ms` must NOT have moved on the retry — the
        // first-attempt gate (`has_started = false`) is still the existing
        // behavior for that EMA.
        assert_eq!(meta.stats.avg_queue_duration_ms, 1_000);
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
        use bilrost::BorrowedMessage;

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
                avg_wait_stats: WaitStats {
                    blocked_on_concurrency_rules_ms: 15,
                    blocked_on_invoker_throttling_ms: 16,
                    ..WaitStats::default()
                },
            },
            scope: Some(Scope::try_from_static("scope-a").unwrap()),
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
