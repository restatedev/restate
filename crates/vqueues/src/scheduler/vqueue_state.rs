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

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{self, VQueueMeta};
use restate_storage_api::vqueue_table::{VQueueEntry, VQueueStore, VisibleAt, WaitStats};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;

use crate::metric_definitions::{
    VQUEUE_GLOBAL_THROTTLE_WAIT_MS, VQUEUE_INVOKER_CONCURRENCY_WAIT_MS,
    VQUEUE_INVOKER_MEMORY_WAIT_MS, VQUEUE_LOCAL_THROTTLE_WAIT_MS,
};
use crate::scheduler::Action;
use crate::scheduler::queue::QueueItem;

use super::clock::SchedulerClock;
use super::queue::Queue;
use super::resource_manager::{AcquireOutcome, PermitBuilder, ReservedResources, ResourceKind};
use super::{Entry, ResourceManager, ThrottleScope, VQueueHandle};

const QUANTUM: i32 = 1;

pub(super) enum Pop<Item> {
    /// The queue needs to receive more credits to be driven.
    NeedsCredit,
    Item {
        action: Action,
        /// Reserved resources for this item. `None` for actions that don't
        /// require global resources (e.g. yield, state mutations).
        resources: Option<ReservedResources>,
        entry: Entry<Item>,
    },
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
    EligibleAt(MillisSinceEpoch),
    NotEligible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DetailedEligibility {
    EligibleRunning,
    EligibleInbox,
    Scheduled(MillisSinceEpoch),
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
    // contains hashes (unique_hash)
    deficit: i32,
    #[debug(skip)]
    unconfirmed_assignments: HashSet<u64>,
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
        self.meta
            .apply_update(update)
            .expect("vqueue cache cannot be updated with invalid updates");
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
    ) -> Result<Pop<S::Item>, StorageError> {
        let (inbox_head, is_running) = match self.queue.head() {
            Some(QueueItem::Inbox(item)) => (item, false),
            Some(QueueItem::Running(item)) => (item, true),
            e @ Some(QueueItem::None) | e @ None => {
                unreachable!(
                    "cannot pop from empty queue or attempted to pop before polling {:?}/{e:?}",
                    self.handle
                )
            }
        };

        let item_weight = inbox_head.weight().get() as i32;
        if self.deficit < item_weight {
            // give credit.
            self.deficit += QUANTUM;
            return Ok(Pop::NeedsCredit);
        } else {
            self.deficit -= item_weight;
        }

        if is_running {
            let result = Pop::Item {
                action: Action::Yield,
                resources: None,
                entry: Entry {
                    item: inbox_head.clone(),
                    stats: self.head_stats.finalize(),
                },
            };
            self.queue
                .advance(storage, &self.unconfirmed_assignments, &self.qid)?;
            return Ok(result);
        }

        match resources.poll_acquire_permit(
            cx,
            self.handle,
            &self.meta,
            inbox_head,
            &mut self.current_permit,
        ) {
            AcquireOutcome::Acquired(resources) => {
                self.unconfirmed_assignments
                    .insert(inbox_head.unique_hash());

                let entry = Entry {
                    item: inbox_head.clone(),
                    stats: self.head_stats.finalize(),
                };
                self.queue
                    .advance(storage, &self.unconfirmed_assignments, &self.qid)?;

                Ok(Pop::Item {
                    action: Action::MoveToRun,
                    resources: Some(resources),
                    entry,
                })
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
        (self.queue.is_empty() || self.meta.total_waiting() == 0 || self.meta.is_paused())
            && self.unconfirmed_assignments.is_empty()
    }

    pub fn poll_eligibility(&mut self, storage: &S) -> Result<DetailedEligibility, StorageError> {
        self.queue
            .advance_if_needed(storage, &self.unconfirmed_assignments, &self.qid)?;

        Ok(self.check_eligibility())
    }

    pub fn check_eligibility(&self) -> DetailedEligibility {
        let inbox_head = match self.queue.head() {
            Some(QueueItem::Running(_)) => return DetailedEligibility::EligibleRunning,
            Some(QueueItem::Inbox(item)) => item,
            Some(QueueItem::None) => return DetailedEligibility::Empty,
            None if self.queue.remaining_in_running_stage() > 0 => {
                return DetailedEligibility::EligibleRunning;
            }
            None if self.meta.total_waiting() > 0 => return DetailedEligibility::EligibleInbox,
            None => return DetailedEligibility::Empty,
        };

        // Only applies to inboxed items.
        if let VisibleAt::At(ts) = inbox_head.visible_at()
            && ts > SchedulerClock.now_millis()
        {
            return DetailedEligibility::Scheduled(ts);
        }

        DetailedEligibility::EligibleInbox
    }

    pub fn notify_removed(&mut self, item_hash: u64) -> Option<PermitBuilder> {
        self.remove_from_unconfirmed_assignments(item_hash);
        if self.queue.remove(item_hash) {
            self.head_stats.reset();
            Some(self.current_permit.take())
        } else {
            None
        }
    }

    pub fn remove_from_unconfirmed_assignments(&mut self, item_hash: u64) -> bool {
        self.unconfirmed_assignments.remove(&item_hash)
    }

    /// Returns true if the head was changed
    pub fn notify_enqueued(&mut self, item: &S::Item) -> Option<PermitBuilder> {
        if self.queue.enqueue(item) {
            self.head_stats.reset();
            Some(self.current_permit.take())
        } else {
            None
        }
    }

    pub fn get_head_wait_stats(&self) -> WaitStats {
        self.head_stats.snapshot()
    }

    /// How many items left in the running stage
    pub fn num_remaining_in_running_stage(&self) -> u32 {
        self.queue.remaining_in_running_stage()
    }

    pub fn num_waiting_inbox(&self) -> u32 {
        self.meta
            .total_waiting()
            .saturating_sub(self.unconfirmed_assignments.len() as u32)
    }
}
