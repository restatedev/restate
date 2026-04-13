// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use hashbrown::HashSet;
use metrics::counter;
use tokio::time::Instant;

use restate_clock::RoughTimestamp;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{self, VQueueMeta};
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, Stage, VQueueStore, WaitStats};
use restate_types::vqueues::VQueueId;

use crate::metric_definitions::{
    VQUEUE_GLOBAL_THROTTLE_WAIT_MS, VQUEUE_INVOKER_CONCURRENCY_WAIT_MS,
    VQUEUE_INVOKER_MEMORY_WAIT_MS, VQUEUE_LOCAL_THROTTLE_WAIT_MS,
};
use crate::scheduler::queue::QueueItem;

use super::clock::SchedulerClock;
use super::queue::Queue;
use super::resource_manager::{AcquireOutcome, PermitBuilder, ReservedResources, ResourceKind};
use super::{ResourceManager, RunAction, ThrottleScope, VQueueHandle, YieldAction};

const QUANTUM: i32 = 1;

pub(super) enum Pop {
    /// The queue needs to receive more credits to be driven.
    NeedsCredit,
    /// An action is ready to be executed.
    Run(RunAction, ReservedResources),
    /// Yielding can place back an item from the run queue to inbox, or from inbox to inbox
    /// but with a different run_at time.
    Yield(YieldAction),
    // may get used in the future if per-vqueue throttling is implemented through the
    // same delayed queue of eligibility tracker.
    #[allow(dead_code)]
    Throttle {
        delay: Duration,
        /// the reason for throttling
        scope: ThrottleScope,
    },
    /// Queue needs to be moved out of the ready ring. The queue will be woken up
    /// by the resource manager.
    Blocked(ResourceKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::IsVariant)]
pub(super) enum Eligibility {
    Eligible,
    EligibleAt(RoughTimestamp),
    NotEligible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DetailedEligibility {
    EligibleRunning,
    EligibleInbox,
    Scheduled(RoughTimestamp),
    Empty,
}

impl DetailedEligibility {
    #[inline(always)]
    pub fn as_compact(&self) -> Eligibility {
        match self {
            DetailedEligibility::EligibleRunning | DetailedEligibility::EligibleInbox => {
                Eligibility::Eligible
            }
            DetailedEligibility::Scheduled(ts) => Eligibility::EligibleAt(*ts),
            DetailedEligibility::Empty => Eligibility::NotEligible,
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    last_blocked_on_invoker_concurrency: Option<tokio::time::Instant>,
    blocked_on_invoker_concurrency_micros: u32,
    last_blocked_on_invoker_memory: Option<tokio::time::Instant>,
    blocked_on_invoker_memory_micros: u32,
    local_start_throttling_micros: u32,
    global_throttling_micros: u32,
}
impl Stats {
    fn reset(&mut self) {
        self.last_blocked_on_invoker_concurrency = None;
        self.blocked_on_invoker_concurrency_micros = 0;
        self.last_blocked_on_invoker_memory = None;
        self.blocked_on_invoker_memory_micros = 0;
        self.local_start_throttling_micros = 0;
        self.global_throttling_micros = 0;
    }

    fn finalize(&mut self) -> WaitStats {
        // ensures that the last capacity/memory-blocked segment is accounted for
        self.record_invoker_memory_delay(false);
        // ensures that the last capacity-blocked segment is accounted for
        self.record_invoker_concurrency_delay(false);

        let stats = WaitStats {
            blocked_on_global_capacity_ms: self.blocked_on_invoker_concurrency_micros / 1000,
            vqueue_start_throttling_ms: self.local_start_throttling_micros / 1000,
            blocked_on_invoker_memory_ms: self.blocked_on_invoker_memory_micros / 1000,
            global_invoker_throttling_ms: self.global_throttling_micros / 1000,
        };
        self.reset();

        stats
    }

    pub fn snapshot(&self) -> WaitStats {
        let blocked_on_global_capacity_micros =
            if let Some(last) = self.last_blocked_on_invoker_concurrency {
                let delay = last.elapsed();
                self.blocked_on_invoker_concurrency_micros
                    .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
            } else {
                self.blocked_on_invoker_concurrency_micros
            };

        let blocked_on_invoker_memory_micros =
            if let Some(last) = self.last_blocked_on_invoker_memory {
                let delay = last.elapsed();
                self.blocked_on_invoker_memory_micros
                    .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
            } else {
                self.blocked_on_invoker_memory_micros
            };

        WaitStats {
            blocked_on_global_capacity_ms: blocked_on_global_capacity_micros / 1000,
            vqueue_start_throttling_ms: self.local_start_throttling_micros / 1000,
            global_invoker_throttling_ms: self.global_throttling_micros / 1000,
            blocked_on_invoker_memory_ms: blocked_on_invoker_memory_micros / 1000,
        }
    }
}

impl Stats {
    #[allow(dead_code)]
    fn record_start_throttling_delay(&mut self, delay: &Duration) {
        self.record_invoker_concurrency_delay(false);
        counter!(VQUEUE_LOCAL_THROTTLE_WAIT_MS).increment(delay.as_millis() as u64);
        self.local_start_throttling_micros = self
            .local_start_throttling_micros
            .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
    }

    #[allow(dead_code)]
    fn record_global_throttling_delay(&mut self, delay: &Duration) {
        self.record_invoker_concurrency_delay(false);
        counter!(VQUEUE_GLOBAL_THROTTLE_WAIT_MS).increment(delay.as_millis() as u64);
        self.global_throttling_micros = self
            .global_throttling_micros
            .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
    }

    fn record_invoker_concurrency_delay(&mut self, is_now_blocked: bool) {
        let last = self.last_blocked_on_invoker_concurrency.take();
        self.last_blocked_on_invoker_concurrency = if is_now_blocked {
            Some(Instant::now())
        } else {
            None
        };
        if let Some(last) = last {
            let delay = last.elapsed();
            counter!(VQUEUE_INVOKER_CONCURRENCY_WAIT_MS).increment(delay.as_millis() as u64);
            self.blocked_on_invoker_concurrency_micros = self
                .blocked_on_invoker_concurrency_micros
                .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
        }
    }

    fn record_invoker_memory_delay(&mut self, is_now_blocked: bool) {
        let last = self.last_blocked_on_invoker_memory.take();
        self.last_blocked_on_invoker_memory = if is_now_blocked {
            Some(Instant::now())
        } else {
            None
        };
        if let Some(last) = last {
            let delay = last.elapsed();
            counter!(VQUEUE_INVOKER_MEMORY_WAIT_MS).increment(delay.as_millis() as u64);
            self.blocked_on_invoker_memory_micros = self
                .blocked_on_invoker_memory_micros
                .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
        }
    }
}

#[derive(derive_more::Debug)]
pub struct VQueueState<S: VQueueStore> {
    pub handle: VQueueHandle,
    pub qid: VQueueId,
    meta: VQueueMeta,
    deficit: i32,
    #[debug(skip)]
    unconfirmed_assignments: HashSet<EntryKey>,
    #[debug(skip)]
    queue: Queue<S>,
    head_stats: Stats,
    #[debug(skip)]
    current_permit: PermitBuilder,
}

impl<S: VQueueStore> VQueueState<S> {
    pub fn new(qid: VQueueId, handle: VQueueHandle, meta: VQueueMeta) -> Self {
        let queue = Queue::new(meta.num_running());
        Self {
            handle,
            qid,
            meta,
            deficit: QUANTUM,
            unconfirmed_assignments: HashSet::new(),
            queue,
            head_stats: Stats::default(),
            current_permit: Default::default(),
        }
    }

    pub fn apply_meta_update(&mut self, update: &metadata::Update) {
        self.meta.apply_update(update)
    }

    pub fn new_empty(qid: VQueueId, handle: VQueueHandle, meta: VQueueMeta) -> Self {
        Self {
            qid,
            handle,
            meta,
            unconfirmed_assignments: HashSet::new(),
            queue: Queue::new_closed(),
            deficit: 0,
            head_stats: Stats::default(),
            current_permit: Default::default(),
        }
    }

    pub fn try_pop(
        &mut self,
        cx: &mut std::task::Context<'_>,
        storage: &S,
        resources: &mut ResourceManager,
    ) -> Result<Pop, StorageError> {
        let (inbox_head_key, inbox_head_value, is_running) = match self.queue.head() {
            Some(QueueItem::Inbox { key, value }) => (key, value, false),
            Some(QueueItem::Running { key, value }) => (key, value, true),
            e @ Some(QueueItem::None) | e @ None => {
                unreachable!(
                    "cannot pop from empty queue or attempted to pop before polling {:?}/{e:?}",
                    self.handle
                )
            }
        };

        let item_weight = inbox_head_value.weight().get() as i32;
        if self.deficit < item_weight {
            // give credit.
            self.deficit += QUANTUM;
            return Ok(Pop::NeedsCredit);
        } else {
            self.deficit -= item_weight;
        }

        if is_running {
            let action = YieldAction {
                prev_stage: Stage::Running,
                key: *inbox_head_key,
                run_at: None,
            };
            self.queue
                .advance(storage, &self.unconfirmed_assignments, &self.qid)?;
            return Ok(Pop::Yield(action));
        }

        match resources.poll_acquire_permit(
            cx,
            self.handle,
            &self.meta,
            inbox_head_key,
            &inbox_head_value.metadata,
            &mut self.current_permit,
        ) {
            AcquireOutcome::Acquired(resources) => {
                self.unconfirmed_assignments.insert(*inbox_head_key);

                let action = RunAction {
                    key: *inbox_head_key,
                    wait_stats: self.head_stats.finalize(),
                };

                self.queue
                    .advance(storage, &self.unconfirmed_assignments, &self.qid)?;
                Ok(Pop::Run(action, resources))
            }
            AcquireOutcome::BlockedOn(resource) => {
                // based on the resource we are blocked on, we might collect some stats
                match resource {
                    ResourceKind::InvokerConcurrency => {
                        self.head_stats.record_invoker_concurrency_delay(true);
                    }
                    ResourceKind::InvokerThrottling => {
                        // self.head_stats.record_start_throttling_delay(delay);
                    }
                    _ => {}
                }
                Ok(Pop::Blocked(resource))
            }
        }
    }

    pub fn is_dormant(&self) -> bool {
        // We hold on to the vqueue until we confirm/reject all pending assignments. If we didn't
        // do so, we risk revisiting/redequeuing the unconfirmed items if the vqueue popped back to life
        // (i.e., on enqueue). This is the reason why we check for `unconfirmed_assignments`
        (self.queue.is_empty() || self.meta.total_waiting() == 0 || self.meta.queue_is_paused())
            && self.unconfirmed_assignments.is_empty()
    }

    pub fn poll_eligibility(&mut self, storage: &S) -> Result<DetailedEligibility, StorageError> {
        self.queue
            .advance_if_needed(storage, &self.unconfirmed_assignments, &self.qid)?;

        Ok(self.check_eligibility())
    }

    pub fn check_eligibility(&self) -> DetailedEligibility {
        let inbox_head_key = match self.queue.head() {
            Some(QueueItem::Running { .. }) => return DetailedEligibility::EligibleRunning,
            Some(QueueItem::Inbox { key, .. }) => key,
            Some(QueueItem::None) => return DetailedEligibility::Empty,
            None if self.queue.remaining_in_running_stage() > 0 => {
                return DetailedEligibility::EligibleRunning;
            }
            None if self.meta.total_waiting() > 0 => return DetailedEligibility::EligibleInbox,
            None => return DetailedEligibility::Empty,
        };

        // Only applies to inboxed items.
        if inbox_head_key.run_at() > SchedulerClock.now_millis() {
            return DetailedEligibility::Scheduled(inbox_head_key.run_at());
        }

        DetailedEligibility::EligibleInbox
    }

    pub fn notify_removed(
        &mut self,
        resource_manager: &mut ResourceManager,
        key: &EntryKey,
    ) -> bool {
        self.remove_from_unconfirmed_assignments(key);
        if self.queue.remove(key) {
            self.head_stats.reset();
            self.current_permit.revert(resource_manager);

            true
        } else {
            false
        }
    }

    pub fn remove_from_unconfirmed_assignments(&mut self, key: &EntryKey) -> bool {
        self.unconfirmed_assignments.remove(key)
    }

    /// Returns true if the head was changed
    pub fn notify_enqueued(
        &mut self,
        resource_manager: &mut ResourceManager,
        key: EntryKey,
        value: EntryValue,
    ) -> bool {
        if self.queue.enqueue(key, value) {
            self.head_stats.reset();
            self.current_permit.revert(resource_manager);
            true
        } else {
            false
        }
    }

    pub fn get_head_wait_stats(&self) -> WaitStats {
        self.head_stats.snapshot()
    }

    /// How many items left in the running stage
    pub fn num_remaining_in_running_stage(&self) -> u32 {
        self.queue.remaining_in_running_stage()
    }

    pub fn num_waiting_inbox(&self) -> u64 {
        self.meta
            .total_waiting()
            .saturating_sub(self.unconfirmed_assignments.len() as u64)
    }
}
