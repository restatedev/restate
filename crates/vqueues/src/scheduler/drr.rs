// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::num::NonZeroU16;
use std::pin::Pin;
use std::task::Poll;
use std::task::Waker;

use hashbrown::HashMap;
use hashbrown::hash_map;
use pin_project::pin_project;
use tracing::{info, trace};

use restate_futures_util::concurrency::Concurrency;
use restate_futures_util::concurrency::Permit;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_types::clock::UniqueTimestamp;
use restate_types::vqueue::VQueueId;

use crate::EventDetails;
use crate::VQueueEvent;
use crate::VQueuesMeta;
use crate::scheduler::Assignments;
use crate::scheduler::vqueue_state::Eligibility;

use super::Decision;
use super::vqueue_state::VQueueState;

/// Capacity to maintain for N vqueues (N=100)
const MIN_VQUEUES_CAPACITY: usize = 100;

#[pin_project]
pub struct DRRScheduler<S: VQueueStore, Token> {
    /// Limits the number of queues picked per scheduler's poll
    limit_qid_per_poll: NonZeroU16,
    concurrency_limiter: Concurrency<Token>,
    eligible: VecDeque<VQueueId>,
    /// Mapping of vqueue_id -> vqueue_state for possibly eligible vqueues
    q: HashMap<VQueueId, VQueueState<S>>,
    unconfirmed_capacity_permits: Permit<Token>,
    // Wraps to zero after 16::MAX.
    global_sched_round: u16,
    /// How many vqueues we need to still need to go visit before we start a new round
    remaining_in_round: usize,
    /// Waker to be notified when scheduler is potentially able to scheduler more work
    waker: Waker,
    /// Time of the last memory reporting and memory compaction
    last_report: Option<UniqueTimestamp>,
    // SAFETY NOTE: **must** Keep this at the end since it needs to outlive all readers.
    storage: S,
}

impl<S, Token> DRRScheduler<S, Token>
where
    S: VQueueStore,
    S::Item: std::fmt::Debug,
{
    pub fn new(
        limit_qid_per_poll: NonZeroU16,
        concurrency_limiter: Concurrency<Token>,
        storage: S,
        vqueues: VQueuesMeta<'_>,
    ) -> Self {
        let mut total_running = 0;
        let mut total_waiting = 0;

        let q: HashMap<_, _> = vqueues
            .iter_vqueues()
            .filter(|(_, meta)| meta.num_running() > 0 || meta.total_waiting() > 0)
            .map(|(qid, meta)| {
                total_running += meta.num_running();
                total_waiting += meta.total_waiting();
                let vqueue = VQueueState::new(*qid, meta.num_running() > 0);
                (*qid, vqueue)
            })
            .collect();

        // We init all active vqueues as eligible first
        let eligible = q.keys().copied().collect();

        info!(
            "Scheduler started. num_vqueues={}, total_running_items={total_running}, total_waiting_items={total_waiting}",
            q.len(),
        );

        Self {
            limit_qid_per_poll,
            concurrency_limiter,
            q,
            eligible,
            global_sched_round: 0,
            remaining_in_round: 0,
            unconfirmed_capacity_permits: Permit::new_empty(),
            waker: Waker::noop().clone(),
            last_report: None,
            storage,
        }
    }

    fn report(&mut self) {
        trace!(
            "DRR scheduler report: eligible={}, queue_states_len={}, queue_states_mem={}bytes, unconfirmed_capacity_permits={:?}, global_sched_round={}",
            self.eligible.len(),
            self.q.len(),
            self.q.allocation_size(),
            self.unconfirmed_capacity_permits,
            self.global_sched_round,
        );
    }

    pub fn poll_schedule_next(
        mut self: Pin<&mut Self>,
        now: UniqueTimestamp,
        cx: &mut std::task::Context<'_>,
        vqueues: VQueuesMeta<'_>,
    ) -> Poll<Result<Decision<S::Item>, StorageError>> {
        enum Outcome {
            ContinueRound,
            Abort,
        }

        if self
            .last_report
            .is_none_or(|t| now.milliseconds_since(t) >= 10000)
        {
            vqueues.report();
            self.report();
            // also report vqueues states
            self.last_report = Some(now);
            // compact memory
            self.q.shrink_to(MIN_VQUEUES_CAPACITY);
        }

        let configs = vqueues.config_pool();
        let mut decision: HashMap<VQueueId, Assignments<S::Item>> = HashMap::new();

        'scheduler: loop {
            if self.remaining_in_round == 0 && !self.eligible.is_empty() {
                // bump global scheduling round, reset remaining.
                self.remaining_in_round = self.eligible.len();
                self.global_sched_round = self.global_sched_round.wrapping_add(1);
            } else if self.eligible.is_empty() {
                break 'scheduler;
            }

            while self.remaining_in_round > 0 {
                // bail if we exhausted coop budget.
                let coop = match tokio::task::coop::poll_proceed(cx) {
                    Poll::Ready(coop) => coop,
                    Poll::Pending => break 'scheduler,
                };
                // stop when we have enough queues picked
                if decision.len() >= self.limit_qid_per_poll.get() as usize {
                    break 'scheduler;
                }

                let this = self.as_mut().project();
                let qid = this.eligible.front().copied().unwrap();
                let Some(qstate) = this.q.get_mut(&qid) else {
                    // the vqueue has been removed probably it's empty therefore, it's
                    // safe to assume that it's not eligible.
                    *this.remaining_in_round -= 1;
                    this.eligible.pop_front();
                    continue;
                };

                let meta = vqueues.get_vqueue(&qid).unwrap();
                let config = configs.find(&qid.parent);

                qstate.deficit.adjust(*this.global_sched_round);
                let mut assignments = Assignments::default();
                let outcome = 'single_vqueue: loop {
                    // get concurrency token, but only if it's still eligible.
                    qstate.poll_head(this.storage, meta)?;
                    match qstate.check_eligibility(now, meta, config) {
                        Eligibility::NotEligible | Eligibility::EligibleAt(_) => {
                            // not eligible,
                            *this.remaining_in_round -= 1;
                            qstate.deficit.reset();
                            this.eligible.pop_front();
                            // todo: remember to set the last_round correctly when becoming eligible again.
                            // the correct last_round == the current global round.
                            // todo: schedule a timer if it's eligible at a later time.
                            break 'single_vqueue Outcome::ContinueRound;
                        }
                        Eligibility::Eligible if !qstate.deficit.can_spend() => {
                            // we'll get credit on the next round
                            *this.remaining_in_round -= 1;
                            this.eligible.rotate_left(1);
                            break 'single_vqueue Outcome::ContinueRound;
                        }
                        Eligibility::Eligible => { /* fallthrough */ }
                    }

                    // Acquire a global concurrency token
                    if this
                        .concurrency_limiter
                        .poll_and_merge(cx, this.unconfirmed_capacity_permits)
                        .is_pending()
                    {
                        // Waker will be notified when capacity is available.
                        // will abort the entire scheduler loop after recording the accumulated
                        // assignments.
                        break 'single_vqueue Outcome::Abort;
                    }
                    // we have concurrency token, let's pick an item
                    qstate.pop_unchecked(this.storage, &mut assignments)?;
                    qstate.deficit.consume_one();
                };

                if !assignments.is_empty() {
                    coop.made_progress();
                    // merge into the overall decision
                    match decision.entry(qid) {
                        hash_map::Entry::Occupied(mut entry) => {
                            entry.get_mut().merge(assignments);
                        }
                        hash_map::Entry::Vacant(entry) => {
                            entry.insert(assignments);
                        }
                    }
                }

                // Depending on whether we stopped because we cannot acquire concurrency
                // permit or because we exhausted the inbox/budget, we decide to bail out
                // of the scheduler loop or not.
                match outcome {
                    // keep driving the scheduler loop
                    Outcome::ContinueRound => {}
                    Outcome::Abort => {
                        break 'scheduler;
                    }
                }
            }
        }

        if decision.is_empty() {
            self.waker.clone_from(cx.waker());
            Poll::Pending
        } else {
            Poll::Ready(Ok(Decision(decision)))
        }
    }

    pub fn confirm_assignment(&mut self, qid: &VQueueId, item_hash: u64) -> Option<Permit<Token>> {
        if let Some(qstate) = self.q.get_mut(qid)
            && qstate.remove_from_unconfirmed_assignments(item_hash)
        {
            debug_assert!(!self.unconfirmed_capacity_permits.is_empty());
            Some(self.unconfirmed_capacity_permits.split(1).expect(
                "trying to confirm a vqueue item after consuming all allotted concurrency tokens!",
            ))
        } else {
            None
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn on_inbox_event(
        &mut self,
        now: UniqueTimestamp,
        vqueues: VQueuesMeta<'_>,
        event: &VQueueEvent<S::Item>,
    ) -> Result<(), StorageError> {
        let qid = event.qid;
        match event.details {
            EventDetails::Enqueued(ref item) => {
                let qstate = self
                    .q
                    .entry_ref(&qid)
                    .or_insert_with(|| VQueueState::new_empty(qid, self.global_sched_round));

                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                qstate.notify_enqueued(item);
                if qstate.check_eligibility(now, meta, config).is_eligible()
                    && !self.eligible.contains(&qid)
                {
                    self.eligible.push_back(qid);
                    // make sure that last_round is set correctly such that next scheduler round we
                    // credit it correctly.
                    // General remarks:
                    // -last_round < current_round
                    //   - if not in current eligibility list, it means we are sure the scheduler didn't touch
                    //   it. We need to add it to the eligible list. Safe to add to the end, and set last_round = current_round.
                    //   - if it's in the current eligibility list. We are good.
                    // - last_round == current_round
                    //     - if it's in the eligble list, we are scanning it right now (head)
                    //     - if it's not, it means we decided that it's not eligible in this round. If we think
                    //     it should be eligible, we re-add it to the end and set last_round = current (leave
                    //     as is basically, just add it back to eligble).
                    //  - if last_round > current_round
                    //    How did this happen? this is invalid state. Of course, we are taking into account the
                    //    wrapping nature of the round counter whenever we compare. In any case, we should
                    //    reset it to the correct value.
                    qstate.deficit.set_last_round(self.global_sched_round);
                    self.wake_up();
                }
            }
            EventDetails::RunAttemptRejected { item_hash } => {
                let Some(qstate) = self.q.get_mut(&qid) else {
                    return Ok(());
                };
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                if qstate.remove_from_unconfirmed_assignments(item_hash) {
                    // drop the already acquired permit
                    let _ = self.unconfirmed_capacity_permits.split(1);
                    if qstate.check_eligibility(now, meta, config).is_eligible() {
                        if !self.eligible.contains(&qid) {
                            self.eligible.push_back(qid);
                            qstate.deficit.set_last_round(self.global_sched_round);
                            self.wake_up();
                        }
                    } else if qstate.is_empty(meta) {
                        // retire the vqueue state
                        self.q.remove(&qid);
                    }
                }
            }
            EventDetails::Removed { item_hash } => {
                let Some(qstate) = self.q.get_mut(&qid) else {
                    return Ok(());
                };
                let meta = vqueues.get_vqueue(&qid).unwrap();

                if qstate.remove_from_unconfirmed_assignments(item_hash) {
                    // drop the already acquired permit
                    let _ = self.unconfirmed_capacity_permits.split(1);
                } else {
                    qstate.notify_removed(item_hash);
                }
                if qstate.is_empty(meta) {
                    // retire the vqueue state
                    self.q.remove(&qid);
                }
            }
        }
        Ok(())
    }

    fn wake_up(&mut self) {
        self.waker.wake_by_ref();
    }
}
