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
use std::pin::Pin;
use std::task::Poll;
use std::task::Waker;

use hashbrown::HashMap;
use pin_project::pin_project;
use tracing::info;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_types::clock::UniqueTimestamp;
use restate_types::vqueue::VQueueId;

use crate::EventDetails;
use crate::InboxEvent;
use crate::VQueuesMeta;
use crate::scheduler::vqueue_state;

use super::Decision;
use super::vqueue_state::VQueueState;

#[pin_project]
pub struct DRRScheduler<S: VQueueStore> {
    eligible: VecDeque<VQueueId>,
    // mapping of vqueue_id -> vqueue_state for all non-empty vqueues
    q: HashMap<VQueueId, VQueueState<S>>,
    global_sched_round: i32,
    waker: Waker,
    // NOTE: Keep this at the end since it needs to outlive all readers.
    storage: S,
}

impl<S> DRRScheduler<S>
where
    S: VQueueStore,
    S::Item: std::fmt::Debug,
{
    pub fn new(storage: S, vqueues: VQueuesMeta<'_>) -> Self {
        let num_active_vqueues = vqueues.num_active_vqueues();
        let mut q = HashMap::with_capacity(num_active_vqueues);
        let mut eligible = VecDeque::with_capacity(num_active_vqueues);

        for (qid, meta) in vqueues.iter_vqueues() {
            let vqueue = VQueueState::new();
            info!(
                "ACTIVE Vqueue {:?} (waiting={}, running={})",
                qid,
                vqueue.num_ready(meta),
                meta.num_running()
            );

            // We init all active vqueues as eligible first
            eligible.push_front(*qid);
            q.insert(*qid, vqueue);
        }

        Self {
            q,
            eligible,
            global_sched_round: 1,
            waker: Waker::noop().clone(),
            storage,
        }
    }

    pub fn poll_schedule_next(
        mut self: Pin<&mut Self>,
        now: UniqueTimestamp,
        cx: &mut std::task::Context<'_>,
        vqueues: VQueuesMeta<'_>,
    ) -> Poll<Result<Decision<S::Item>, StorageError>> {
        let configs = vqueues.config_pool();

        self.global_sched_round += 1;
        for iteration in 1..=self.eligible.len() {
            let this = self.as_mut().project();
            let Some(qid) = this.eligible.pop_front() else {
                break;
            };

            let meta = vqueues.get_vqueue(&qid).unwrap();
            let qstate = this.q.get_mut(&qid).unwrap();
            let config = configs.find(&qid.parent);

            let decision = qstate.pick_next(
                now,
                &qid,
                this.storage,
                meta,
                config,
                *this.global_sched_round,
            )?;

            match qstate.status() {
                vqueue_state::Status::Eligible => {
                    if qstate.can_spend_credit() {
                        info!(
                            "vqueue {:?} can still spend credit, decision_size={}",
                            qid,
                            decision.len()
                        );
                        this.eligible.push_front(qid);
                    } else {
                        info!(
                            "vqueue {:?} has no credits left but it's still eligible, decision_size={}",
                            qid,
                            decision.len()
                        );
                        this.eligible.push_back(qid);
                    }
                }
                vqueue_state::Status::EligibleAt(_ts) => {
                    // todo: need the timer queue.
                    todo!()
                }
                vqueue_state::Status::WaitingServiceCapacity => {
                    todo!()
                }
                vqueue_state::Status::WaitingConcurrency
                | vqueue_state::Status::Paused
                | vqueue_state::Status::Empty => {
                    // that's okay, we already popped it from the eligible ring
                }
            }

            // do we have a decision?
            if !decision.is_empty() {
                info!(
                    "[{iteration}] scheduler_decision: returning: {:?}",
                    decision
                );
                return Poll::Ready(Ok(decision));
            }
        }

        self.waker.clone_from(cx.waker());
        Poll::Pending
    }

    fn refresh_eligibility(&mut self, qid: &VQueueId, now: UniqueTimestamp) {
        let Some(qs) = self.q.get(qid) else {
            return;
        };

        if qs.maybe_eligible(now) {
            if !self.eligible.contains(qid) {
                info!("vqueue became eligible {:?}", qid);
                self.eligible.push_back(*qid);
                self.wake_up();
            }
            return;
        }

        if let Some(pos) = self.eligible.iter().position(|&x| &x == qid) {
            info!("vqueue is now not eligible {:?}", qid);
            self.eligible.remove(pos);
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn on_inbox_event(
        &mut self,
        now: UniqueTimestamp,
        vqueues: VQueuesMeta<'_>,
        event: &InboxEvent<S::Item>,
    ) -> Result<(), StorageError> {
        let qid = event.qid;
        let qstate = self
            .q
            .entry_ref(&qid)
            .or_insert_with(|| VQueueState::new_empty(self.global_sched_round));
        let config = vqueues.config_pool().find(&qid.parent);
        let meta = vqueues.get_vqueue(&qid).unwrap();
        let ts = event.ts;
        match event.details {
            EventDetails::Enqueued(ref item) => {
                if qstate.notify_enqueued(item, now, meta, config) {
                    self.refresh_eligibility(&qid, ts);
                }
            }
            EventDetails::RunAttemptPermitted { item_hash } => {
                if qstate.notify_assignment_confirmed(item_hash, now, meta, config) {
                    self.refresh_eligibility(&qid, ts);
                }
            }
            EventDetails::RunAttemptRejected { item_hash } => {
                if qstate.notify_assignment_rejected(item_hash, now, meta, config) {
                    self.refresh_eligibility(&qid, ts);
                }
            }
            EventDetails::Removed { item_hash } => {
                info!("removing from inbox: {item_hash}");
                qstate.notify_removed(item_hash, now, meta, config);
                info!("removing complete, refreshing eligibility");
                self.refresh_eligibility(&qid, ts);
            }
        }
        Ok(())
    }

    fn wake_up(&mut self) {
        self.waker.wake_by_ref();
    }
}
