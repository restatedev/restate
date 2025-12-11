// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::task::Poll;
use std::time::Duration;

use gardal::LocalTokenBucket;
use hashbrown::HashSet;
use metrics::counter;
use tokio::time::Instant;
use tracing::{debug, trace};

use restate_futures_util::concurrency::{Concurrency, Permit};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{
    EntryKind, VQueueEntry, VQueueStore, VisibleAt, WaitStats,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::vqueue::VQueueId;

use crate::metric_definitions::{
    VQUEUE_GLOBAL_THROTTLE_WAIT_MS, VQUEUE_INVOKER_CAPACITY_WAIT_MS, VQUEUE_LOCAL_THROTTLE_WAIT_MS,
};
use crate::scheduler::Action;
use crate::scheduler::queue::QueueItem;
use crate::vqueue_config::VQueueConfig;

use super::clock::SchedulerClock;
use super::queue::Queue;
use super::{Entry, GlobalTokenBucket, ThrottleScope, VQueueHandle};

const QUANTUM: i32 = 1;

pub(super) enum Pop<Item> {
    DeficitExhausted,
    Item {
        action: Action,
        entry: Entry<Item>,
        updated_zt: Option<f64>,
    },
    Throttle {
        delay: Duration,
        /// the reason for throttling
        scope: ThrottleScope,
    },
    BlockedOnCapacity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::IsVariant)]
pub(super) enum Eligibility {
    Eligible,
    EligibleAt(UniqueTimestamp),
    NotEligible,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DetailedEligibility {
    EligibleRunning,
    EligibleInbox,
    Scheduled(UniqueTimestamp),
    Paused,
    Empty,
    WaitingConcurrencyTokens,
}

impl DetailedEligibility {
    #[inline(always)]
    pub fn as_compact(&self) -> Eligibility {
        match self {
            DetailedEligibility::EligibleRunning | DetailedEligibility::EligibleInbox => {
                Eligibility::Eligible
            }
            DetailedEligibility::Scheduled(ts) => Eligibility::EligibleAt(*ts),
            DetailedEligibility::Paused
            | DetailedEligibility::Empty
            | DetailedEligibility::WaitingConcurrencyTokens => Eligibility::NotEligible,
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    last_blocked_on_global_capacity: Option<tokio::time::Instant>,
    blocked_on_global_capacity_micros: u32,
    local_start_throttling_micros: u32,
    global_throttling_micros: u32,
}
impl Stats {
    fn reset(&mut self) {
        self.last_blocked_on_global_capacity = None;
        self.blocked_on_global_capacity_micros = 0;
        self.local_start_throttling_micros = 0;
        self.global_throttling_micros = 0;
    }

    fn finalize(&mut self) -> WaitStats {
        // ensures that the last capacity-blocked segment is accounted for
        self.record_global_capacity_delay(false);

        let stats = WaitStats {
            blocked_on_global_capacity_ms: self.blocked_on_global_capacity_micros / 1000,
            vqueue_start_throttling_ms: self.local_start_throttling_micros / 1000,
            global_throttling_ms: self.global_throttling_micros / 1000,
        };
        self.reset();

        stats
    }

    pub fn snapshot(&self) -> WaitStats {
        let blocked_on_global_capacity_micros =
            if let Some(last) = self.last_blocked_on_global_capacity {
                let delay = last.elapsed();
                self.blocked_on_global_capacity_micros
                    .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
            } else {
                self.blocked_on_global_capacity_micros
            };

        WaitStats {
            blocked_on_global_capacity_ms: blocked_on_global_capacity_micros / 1000,
            vqueue_start_throttling_ms: self.local_start_throttling_micros / 1000,
            global_throttling_ms: self.global_throttling_micros / 1000,
        }
    }
}

impl Stats {
    fn record_start_throttling_delay(&mut self, delay: &Duration) {
        self.record_global_capacity_delay(false);
        counter!(VQUEUE_LOCAL_THROTTLE_WAIT_MS).increment(delay.as_millis() as u64);
        self.local_start_throttling_micros = self
            .local_start_throttling_micros
            .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
    }

    fn record_global_throttling_delay(&mut self, delay: &Duration) {
        self.record_global_capacity_delay(false);
        counter!(VQUEUE_GLOBAL_THROTTLE_WAIT_MS).increment(delay.as_millis() as u64);
        self.global_throttling_micros = self
            .global_throttling_micros
            .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
    }

    fn record_global_capacity_delay(&mut self, is_now_blocked: bool) {
        let last = self.last_blocked_on_global_capacity.take();
        self.last_blocked_on_global_capacity = if is_now_blocked {
            Some(Instant::now())
        } else {
            None
        };
        if let Some(last) = last {
            let delay = last.elapsed();
            counter!(VQUEUE_INVOKER_CAPACITY_WAIT_MS).increment(delay.as_millis() as u64);
            self.blocked_on_global_capacity_micros = self
                .blocked_on_global_capacity_micros
                .saturating_add(delay.as_micros().try_into().unwrap_or(u32::MAX))
        }
    }
}

#[derive(derive_more::Debug)]
pub struct VQueueState<S: VQueueStore> {
    pub handle: VQueueHandle,
    pub qid: VQueueId,
    // contains hashes (unique_hash)
    deficit: i32,
    #[debug(skip)]
    // Run token bucket is used to throttle the rate of "start" attempts of this
    // vqueue.
    start_tb: Option<LocalTokenBucket<SchedulerClock>>,
    unconfirmed_assignments: HashSet<u64>,
    #[debug(skip)]
    queue: Queue<S>,
    head_stats: Stats,
}

impl<S: VQueueStore> VQueueState<S> {
    pub fn new(
        qid: VQueueId,
        handle: VQueueHandle,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> Self {
        let start_tb = config.start_rate_limit().map(|limit| {
            LocalTokenBucket::with_zero_time(*limit, SchedulerClock, meta.start_tb_zero_time())
        });

        Self {
            qid,
            handle,
            start_tb,
            unconfirmed_assignments: HashSet::new(),
            queue: Queue::new(meta.num_running()),
            deficit: QUANTUM,
            head_stats: Stats::default(),
        }
    }

    pub fn new_empty(
        qid: VQueueId,
        handle: VQueueHandle,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> Self {
        let start_tb = config.start_rate_limit().map(|limit| {
            LocalTokenBucket::with_zero_time(*limit, SchedulerClock, meta.start_tb_zero_time())
        });
        Self {
            qid,
            handle,
            start_tb,
            unconfirmed_assignments: HashSet::new(),
            queue: Queue::new_closed(),
            deficit: 0,
            head_stats: Stats::default(),
        }
    }

    pub fn pop_unchecked(
        &mut self,
        cx: &mut std::task::Context<'_>,
        storage: &S,
        concurrency_limiter: &mut Concurrency,
        global_throttling: Option<&GlobalTokenBucket>,
    ) -> Result<Pop<S::Item>, StorageError> {
        let (inbox_head, is_running) = match self.queue.head() {
            Some(QueueItem::Inbox(item)) => (item, false),
            Some(QueueItem::Running(item)) => (item, true),
            Some(QueueItem::None) | None => {
                unreachable!("cannot pop from empty queue or attempted to pop before polling")
            }
        };

        let item_weight = inbox_head.weight().get() as i32;
        if self.deficit < item_weight {
            // give credit.
            self.deficit += QUANTUM;
            return Ok(Pop::DeficitExhausted);
        } else {
            self.deficit -= item_weight;
        }

        if is_running {
            // switch between these to change the behavior as needed
            // Action::ResumeAlreadyRunning;
            // Note that resumption requires acquiring concurrency permits similar to
            // MoveToRunning. This is currently not implemented since (at the moment) we
            // only support yielding.
            let result = Pop::Item {
                action: Action::Yield,
                entry: Entry {
                    item: inbox_head.clone(),
                    stats: self.head_stats.finalize(),
                    permit: Permit::new_empty(),
                },
                updated_zt: None,
            };
            self.queue
                .advance(storage, &self.unconfirmed_assignments, &self.qid)?;
            return Ok(result);
        }

        let has_tb_token = if inbox_head.priority().is_new()
            && let Some(ref start_tb) = self.start_tb
        {
            // Checking for VQueue starts throttling
            if let Err(rate_limited) = start_tb.try_consume_one() {
                let delay = rate_limited.earliest_retry_after();
                self.head_stats.record_start_throttling_delay(&delay);
                trace!(
                    "[vqueue] Need to throttle start from vqueue {:?} for {delay:?}",
                    self.qid,
                );
                return Ok(Pop::Throttle {
                    delay,
                    scope: ThrottleScope::VQueue,
                });
            }
            true
        } else {
            // no throttling or the item has already been started before
            false
        };

        let permit = match inbox_head.kind() {
            EntryKind::Invocation => {
                let Poll::Ready(permit) = concurrency_limiter.poll_acquire(cx) else {
                    // Waker will be notified when capacity is available.
                    // if we have start tokens, we should return them.
                    if has_tb_token && let Some(ref start_tb) = self.start_tb {
                        start_tb.add_tokens(1.0);
                    }
                    self.head_stats.record_global_capacity_delay(true);
                    trace!("vqueue {:?} is blocked on global capacity", self.qid);
                    return Ok(Pop::BlockedOnCapacity);
                };
                permit
            }
            EntryKind::StateMutation | EntryKind::Unknown => Permit::new_empty(),
        };

        if let Some(global_throttling) = global_throttling
            && let Err(rate_limited) = global_throttling.try_consume_one()
        {
            let delay = rate_limited.earliest_retry_after();
            self.head_stats.record_global_throttling_delay(&delay);

            trace!(
                "[global] Need to throttle start from vqueue {:?} for {delay:?}",
                self.qid,
            );
            // return the start token to the vqueue if we have one.
            if has_tb_token && let Some(ref start_tb) = self.start_tb {
                start_tb.add_tokens(1.0);
            }
            // NOTE: the concurrency limiter's permit will be dropped here.
            return Ok(Pop::Throttle {
                delay,
                scope: ThrottleScope::Global,
            });
        }

        self.unconfirmed_assignments
            .insert(inbox_head.unique_hash());
        let result = Pop::Item {
            action: Action::MoveToRunning,
            entry: Entry {
                item: inbox_head.clone(),
                stats: self.head_stats.finalize(),
                permit,
            },
            updated_zt: self
                .start_tb
                .as_ref()
                .map(|start_tb| start_tb.get_zero_time()),
        };

        self.queue
            .advance(storage, &self.unconfirmed_assignments, &self.qid)?;

        Ok(result)
    }

    pub fn is_dormant(&self, meta: &VQueueMeta) -> bool {
        (self.queue.is_empty() || meta.total_waiting() == 0)
            && self.unconfirmed_assignments.is_empty()
    }

    pub fn poll_eligibility(
        &mut self,
        storage: &S,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> Result<DetailedEligibility, StorageError> {
        self.queue
            .advance_if_needed(storage, &self.unconfirmed_assignments, &self.qid)?;

        Ok(self.check_eligibility(now, meta, config))
    }

    pub fn check_eligibility(
        &self,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> DetailedEligibility {
        if meta.is_paused() {
            return DetailedEligibility::Paused;
        }

        let inbox_head = match self.queue.head() {
            Some(QueueItem::Running(_)) => return DetailedEligibility::EligibleRunning,
            Some(QueueItem::Inbox(item)) => item,
            Some(QueueItem::None) => return DetailedEligibility::Empty,
            None if self.queue.remaining_in_running_stage() > 0 => {
                return DetailedEligibility::EligibleRunning;
            }
            None if meta.total_waiting() > 0 => return DetailedEligibility::EligibleInbox,
            None => return DetailedEligibility::Empty,
        };

        // Only applies to inboxed items.
        if let VisibleAt::At(ts) = inbox_head.visible_at()
            && ts > now
        {
            return DetailedEligibility::Scheduled(ts);
        }

        if inbox_head.is_token_held() || self.has_available_tokens(meta, config) {
            DetailedEligibility::EligibleInbox
        } else {
            DetailedEligibility::WaitingConcurrencyTokens
        }
    }

    pub fn notify_removed(&mut self, item_hash: u64) {
        self.remove_from_unconfirmed_assignments(item_hash);
        if self.queue.remove(item_hash) {
            self.head_stats.reset();
        }
    }

    pub fn remove_from_unconfirmed_assignments(&mut self, item_hash: u64) -> bool {
        self.unconfirmed_assignments.remove(&item_hash)
    }

    /// Returns true if the head was changed
    pub fn notify_enqueued(&mut self, item: &S::Item) -> bool {
        if self.queue.enqueue(item) {
            self.head_stats.reset();
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

    pub fn num_waiting_inbox(&self, meta: &VQueueMeta) -> u32 {
        meta.total_waiting()
            .saturating_sub(self.unconfirmed_assignments.len() as u32)
    }

    pub fn num_tokens_used(&self, meta: &VQueueMeta) -> u32 {
        meta.tokens_used() + self.unconfirmed_assignments.len() as u32
    }

    fn has_available_tokens(&self, meta: &VQueueMeta, config: &VQueueConfig) -> bool {
        config
            .concurrency()
            .is_none_or(|limit| self.num_tokens_used(meta) < limit.get())
    }
}
