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
use std::time::Duration;

use hashbrown::HashMap;
use hashbrown::hash_map;
use metrics::counter;
use pin_project::pin_project;
use tokio::time::Instant;
use tokio_util::time::DelayQueue;
use tracing::{info, trace};

use restate_futures_util::concurrency::Concurrency;
use restate_futures_util::concurrency::Permit;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_types::clock::UniqueTimestamp;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueue::VQueueId;

use crate::EventDetails;
use crate::VQueueEvent;
use crate::VQueuesMeta;
use crate::metric_definitions::VQUEUE_ENQUEUE;
use crate::metric_definitions::VQUEUE_RUN_CONFIRMED;
use crate::metric_definitions::VQUEUE_RUN_REJECTED;
use crate::scheduler::Assignments;
use crate::scheduler::vqueue_state::Eligibility;

use super::Decision;
use super::clock::SchedulerClock;
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
    delayed_eligibility: DelayQueue<VQueueId>,
    // Wraps to zero after 16::MAX.
    global_sched_round: u16,
    /// How many vqueues we need to still need to go visit before we start a new round
    remaining_in_round: usize,
    /// Waker to be notified when scheduler is potentially able to scheduler more work
    waker: Waker,
    datum: SchedulerClock,
    /// Time of the last memory reporting and memory compaction
    last_report: Instant,

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

        let datum = SchedulerClock::new(
            UniqueTimestamp::from_unix_millis(MillisSinceEpoch::now())
                .expect("clock does not overflow"),
        );
        // Makes sure we use the same clock datum for the internal timer wheel and for our
        // own eligibility checks.
        let start = datum.origin_instant();
        let delayed_eligibility = DelayQueue::with_capacity(MIN_VQUEUES_CAPACITY);

        Self {
            limit_qid_per_poll,
            concurrency_limiter,
            q,
            eligible,
            global_sched_round: 0,
            remaining_in_round: 0,
            delayed_eligibility,
            unconfirmed_capacity_permits: Permit::new_empty(),
            waker: Waker::noop().clone(),
            datum,
            last_report: start,
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
        cx: &mut std::task::Context<'_>,
        vqueues: VQueuesMeta<'_>,
    ) -> Poll<Result<Decision<S::Item>, StorageError>> {
        enum Outcome {
            ContinueRound,
            Abort,
        }

        if self.last_report.elapsed() >= Duration::from_secs(10) {
            vqueues.report();
            self.report();
            // also report vqueues states
            self.last_report = Instant::now();
            // compact memory
            self.q.shrink_to(MIN_VQUEUES_CAPACITY);
            self.delayed_eligibility.compact();
        }

        let configs = vqueues.config_pool();
        let mut decision: HashMap<VQueueId, Assignments<S::Item>> = HashMap::new();

        'scheduler: loop {
            if self.remaining_in_round == 0 {
                // Pop all eligible vqueues that were delayed since we are starting a new round
                // Once we hit pending, the waker will be registered.
                let previous_round = self.global_sched_round;
                while let Poll::Ready(Some(expired)) = self.delayed_eligibility.poll_expired(cx) {
                    let Some(qstate) = self.q.get_mut(expired.get_ref()) else {
                        // the vqueue is gone/empty, ignore.
                        continue;
                    };
                    qstate.drop_scheduled_wake_up();
                    qstate.deficit.set_last_round(previous_round);
                    let vqueue_id = expired.into_inner();
                    if !self.eligible.contains(&vqueue_id) {
                        self.eligible.push_back(vqueue_id);
                    }
                }

                if !self.eligible.is_empty() {
                    // bump global scheduling round, reset remaining.
                    self.remaining_in_round = self.eligible.len();
                    self.global_sched_round = self.global_sched_round.wrapping_add(1);
                } else {
                    break 'scheduler;
                }
            }

            let now = self.datum.now_ts();
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
                        Eligibility::NotEligible => {
                            // not eligible,
                            *this.remaining_in_round -= 1;
                            qstate.on_not_eligible(this.delayed_eligibility);
                            this.eligible.pop_front();
                            break 'single_vqueue Outcome::ContinueRound;
                        }
                        Eligibility::EligibleAt(wake_up_at) => {
                            *this.remaining_in_round -= 1;
                            qstate.maybe_schedule_wakeup(
                                this.datum.ts_to_future_instant(wake_up_at),
                                this.delayed_eligibility,
                            );
                            this.eligible.pop_front();
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
                    // todo consider not requiring concurrency tokens for state mutations
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
            let decision = Decision(decision);
            decision.report_metrics();
            Poll::Ready(Ok(decision))
        }
    }

    pub fn confirm_assignment(&mut self, qid: &VQueueId, item_hash: u64) -> Option<Permit<Token>> {
        if let Some(qstate) = self.q.get_mut(qid)
            && qstate.remove_from_unconfirmed_assignments(item_hash)
        {
            counter!(VQUEUE_RUN_CONFIRMED).increment(1);
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

                counter!(VQUEUE_ENQUEUE).increment(1);
                if !qstate.notify_enqueued(item) {
                    // no impact on eligibility, no need to spend more cycles.
                    return Ok(());
                }

                let now_ts = self.datum.now_ts();
                match qstate.check_eligibility(now_ts, meta, config) {
                    Eligibility::Eligible if !self.eligible.contains(&qid) => {
                        // Make eligible immediately.
                        qstate.deficit.set_last_round(self.global_sched_round);
                        self.eligible.push_back(qid);
                        self.wake_up();
                    }
                    Eligibility::EligibleAt(eligiblility_ts) if !self.eligible.contains(&qid) => {
                        qstate.maybe_schedule_wakeup(
                            self.datum.ts_to_future_instant(eligiblility_ts),
                            &mut self.delayed_eligibility,
                        );
                    }
                    _ => { /* do nothing */ }
                }
            }
            EventDetails::RunAttemptRejected { item_hash } => {
                let Some(qstate) = self.q.get_mut(&qid) else {
                    return Ok(());
                };
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                if qstate.remove_from_unconfirmed_assignments(item_hash) {
                    counter!(VQUEUE_RUN_REJECTED).increment(1);
                    // drop the already acquired permit
                    let _ = self.unconfirmed_capacity_permits.split(1);

                    if qstate
                        .check_eligibility(self.datum.now_ts(), meta, config)
                        .is_eligible()
                    {
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
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                if qstate.remove_from_unconfirmed_assignments(item_hash) {
                    // drop the already acquired permit
                    let _ = self.unconfirmed_capacity_permits.split(1);
                } else if qstate.notify_removed(item_hash) {
                    match qstate.check_eligibility(self.datum.now_ts(), meta, config) {
                        Eligibility::Eligible if !self.eligible.contains(&qid) => {
                            // Make eligible immediately.
                            qstate.deficit.set_last_round(self.global_sched_round);
                            self.eligible.push_back(qid);
                            self.waker.wake_by_ref();
                        }
                        Eligibility::EligibleAt(eligiblility_ts)
                            if !self.eligible.contains(&qid) =>
                        {
                            qstate.maybe_schedule_wakeup(
                                self.datum.ts_to_future_instant(eligiblility_ts),
                                &mut self.delayed_eligibility,
                            );
                        }
                        _ => { /* do nothing */ }
                    }
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
