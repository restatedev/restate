// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;
use std::pin::Pin;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use hashbrown::HashMap;
use metrics::counter;
use pin_project::pin_project;
use slotmap::SlotMap;
use tokio::time::Instant;
use tracing::warn;
use tracing::{info, trace};

use restate_futures_util::concurrency::Concurrency;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_types::vqueue::VQueueId;

use crate::EventDetails;
use crate::VQueueEvent;
use crate::VQueuesMeta;
use crate::metric_definitions::VQUEUE_ENQUEUE;
use crate::metric_definitions::VQUEUE_RUN_CONFIRMED;
use crate::metric_definitions::VQUEUE_RUN_REJECTED;
use crate::scheduler::eligible::EligibilityTracker;
use crate::scheduler::vqueue_state::Pop;

use super::Decision;
use super::GlobalTokenBucket;
use super::VQueueHandle;
use super::vqueue_state::VQueueState;

#[pin_project]
pub struct DRRScheduler<S: VQueueStore> {
    concurrency_limiter: Concurrency<()>,
    global_throttling: Option<GlobalTokenBucket>,
    // sorted by queue_id
    eligible: EligibilityTracker,
    /// Mapping of vqueue_id -> handle for active vqueues
    id_lookup: HashMap<VQueueId, VQueueHandle>,
    q: SlotMap<VQueueHandle, VQueueState<S>>,
    /// Waker to be notified when scheduler is potentially able to scheduler more work
    waker: Waker,
    /// Time of the last memory reporting and memory compaction
    last_report: Instant,

    /// Limits the number of queues picked per scheduler's poll
    limit_qid_per_poll: NonZeroU16,
    /// Limits the number of items included in a single decision across all queues
    max_items_per_decision: NonZeroU16,

    // SAFETY NOTE: **must** Keep this at the end since it needs to outlive all readers.
    storage: S,
}

impl<S> DRRScheduler<S>
where
    S: VQueueStore,
    S::Item: std::fmt::Debug,
{
    pub fn new(
        limit_qid_per_poll: NonZeroU16,
        max_items_per_decision: NonZeroU16,
        concurrency_limiter: Concurrency<()>,
        global_throttling: Option<GlobalTokenBucket>,
        storage: S,
        vqueues: VQueuesMeta<'_>,
    ) -> Self {
        let mut total_running = 0;
        let mut total_waiting = 0;

        let num_active = vqueues.num_active();
        let mut q = SlotMap::with_capacity_and_key(num_active);
        let mut id_lookup = HashMap::with_capacity(num_active);
        let mut eligible: EligibilityTracker = EligibilityTracker::with_capacity(num_active);

        for (qid, meta) in vqueues.iter_active_vqueues() {
            let config = vqueues.config_pool().find(&qid.parent);
            total_running += meta.num_running();
            total_waiting += meta.total_waiting();
            let handle = q.insert_with_key(|handle| VQueueState::new(*qid, handle, meta, config));
            id_lookup.insert(*qid, handle);
            // We init all active vqueues as eligible first
            eligible.insert_eligible(handle);
        }

        info!(
            "Scheduler started. num_vqueues={}, total_running_items={total_running}, total_waiting_items={total_waiting}",
            q.len(),
        );

        Self {
            concurrency_limiter,
            global_throttling,
            id_lookup,
            q,
            eligible,
            waker: Waker::noop().clone(),
            last_report: Instant::now(),
            storage,
            limit_qid_per_poll,
            max_items_per_decision,
        }
    }

    fn report(&mut self) {
        trace!(
            "DRR scheduler report: eligible={}, queue_states_len={}",
            self.eligible.len(),
            self.q.len(),
        );
    }

    pub fn poll_schedule_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        vqueues: VQueuesMeta<'_>,
    ) -> Poll<Result<Decision<S::Item>, StorageError>> {
        let mut decision = Decision::default();
        let mut items_collected = 0;
        let mut first_blocked = None;

        loop {
            self.eligible.poll_delayed(cx);
            // bail if we exhausted coop budget.
            let coop = match tokio::task::coop::poll_proceed(cx) {
                Poll::Ready(coop) => coop,
                Poll::Pending => break,
            };
            // stop when we have enough queues picked
            if decision.num_queues() >= self.limit_qid_per_poll.get() as usize
                || items_collected >= self.max_items_per_decision.get() as usize
            {
                trace!(
                    "Reached limits of a single DRR decision. num_items={} qids_in_decision={}",
                    decision.total_items(),
                    decision.num_queues()
                );
                break;
            }

            let this = self.as_mut().project();

            let Some(handle) = this.eligible.next_eligible(this.storage, this.q, vqueues)? else {
                trace!(
                    "No more eligible vqueues, {:?}, states_len={}, states_capacity={}",
                    this.eligible,
                    this.q.len(),
                    this.q.capacity()
                );
                break;
            };

            if first_blocked.is_some_and(|first| first == handle) {
                // do not check the same vqueue twice for capacity in the same poll.
                break;
            }

            let qstate = this.q.get_mut(handle).unwrap();

            match qstate.pop_unchecked(
                cx,
                this.storage,
                this.concurrency_limiter,
                this.global_throttling.as_ref(),
            )? {
                Pop::DeficitExhausted => {
                    this.eligible.rotate_one();
                    continue;
                }
                Pop::Item {
                    action,
                    entry,
                    updated_zt,
                } => {
                    coop.made_progress();
                    items_collected += 1;
                    decision.push(&qstate.qid, action, entry, updated_zt);
                    // We need to set the state so we check eligibility and setup
                    // necessary schedules when we poll the queue again.
                    this.eligible.front_needs_poll();
                }
                Pop::Throttle { delay, scope } => {
                    this.eligible.front_throttled(delay, scope);
                    continue;
                }
                Pop::BlockedOnCapacity => {
                    if first_blocked.is_none() {
                        first_blocked = Some(handle);
                    }
                    this.eligible.front_blocked();
                    // stay in ready ring
                    this.eligible.rotate_one();
                    continue;
                }
            }
        }

        if decision.is_empty() {
            if self.last_report.elapsed() >= Duration::from_secs(10) {
                vqueues.report();
                self.report();
                // also report vqueues states
                self.last_report = Instant::now();
                self.eligible.compact_memory();
            }

            self.waker.clone_from(cx.waker());
            Poll::Pending
        } else {
            decision.report_metrics();
            // for every queue in the decision, we need to update the token bucket's zero time.

            Poll::Ready(Ok(decision))
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn on_inbox_event(
        &mut self,
        vqueues: VQueuesMeta<'_>,
        event: &VQueueEvent<S::Item>,
    ) -> Result<(), StorageError> {
        let qid = event.qid;
        match event.details {
            EventDetails::Enqueued(ref item) => {
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                let qstate = match self.id_lookup.get(&qid) {
                    Some(handle) => match self.q.get_mut(*handle) {
                        Some(qstate) => qstate,
                        None => {
                            let handle = self.q.insert_with_key(|handle| {
                                VQueueState::new_empty(qid, handle, meta, config)
                            });
                            self.id_lookup.insert(qid, handle);
                            self.q.get_mut(handle).unwrap()
                        }
                    },
                    None => {
                        let handle = self.q.insert_with_key(|handle| {
                            VQueueState::new_empty(qid, handle, meta, config)
                        });
                        self.id_lookup.insert(qid, handle);
                        self.q.get_mut(handle).unwrap()
                    }
                };

                counter!(VQUEUE_ENQUEUE).increment(1);
                if qstate.notify_enqueued(item)
                    && self.eligible.refresh_membership(qstate, meta, config)
                {
                    self.wake_up();
                }
            }
            EventDetails::RunAttemptConfirmed { item_hash } => {
                let Some(handle) = self.id_lookup.get(&qid) else {
                    return Ok(());
                };
                let Some(qstate) = self.q.get_mut(*handle) else {
                    return Ok(());
                };
                if qstate.remove_from_unconfirmed_assignments(item_hash) {
                    counter!(VQUEUE_RUN_CONFIRMED).increment(1);
                }
            }
            EventDetails::RunAttemptRejected { item_hash } => {
                let Some(handle) = self.id_lookup.get(&qid) else {
                    return Ok(());
                };
                let Some(qstate) = self.q.get_mut(*handle) else {
                    return Ok(());
                };
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                if qstate.remove_from_unconfirmed_assignments(item_hash) {
                    counter!(VQUEUE_RUN_REJECTED).increment(1);

                    if qstate.is_empty(meta) {
                        // retire the vqueue state
                        self.q.remove(*handle);
                        self.id_lookup.remove(&qid);
                    } else if self.eligible.refresh_membership(qstate, meta, config) {
                        self.wake_up();
                    }
                }
            }
            EventDetails::Removed { item_hash } => {
                let Some(handle) = self.id_lookup.get(&qid) else {
                    return Ok(());
                };
                let Some(qstate) = self.q.get_mut(*handle) else {
                    return Ok(());
                };
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                let _ = qstate.remove_from_unconfirmed_assignments(item_hash);
                qstate.notify_removed(item_hash);

                if qstate.is_empty(meta) {
                    // retire the vqueue state
                    self.q.remove(*handle);
                    self.id_lookup.remove(&qid);
                } else if self.eligible.refresh_membership(qstate, meta, config) {
                    self.wake_up();
                }
            }
        }
        Ok(())
    }

    fn wake_up(&mut self) {
        self.waker.wake_by_ref();
    }
}
