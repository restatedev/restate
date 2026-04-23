// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::pin::Pin;

use std::future::poll_fn;
use std::task::Poll;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::scheduler::{RunAction, SchedulerAction, YieldAction};
use restate_storage_api::vqueue_table::{EntryKey, ScanVQueueTable, VQueueStore};
use restate_types::vqueues::VQueueId;
use restate_worker_api::{SchedulingStatus, VQueueSchedulerStatus};

use crate::VQueueEvent;
use crate::VQueuesMetaCache;
use crate::metric_definitions::publish_scheduler_decision_metrics;

use self::drr::DRRScheduler;
use self::resource_manager::{PermitBuilder, ReservedResources};
use self::vqueue_state::DetailedEligibility;

mod clock;
mod drr;
mod eligible;
mod queue;
mod queue_meta;
mod resource_manager;
mod vqueue_state;

// Re-exports
pub use self::queue_meta::{MetaLiteUpdate, VQueueMetaLite};
pub use resource_manager::ResourceManager;

type UnconfirmedAssignments = hashbrown::HashMap<EntryKey, PermitBuilder>;

slotmap::new_key_type! { pub(crate) struct VQueueHandle; }

fn status_from_detailed_eligibility(value: DetailedEligibility) -> SchedulingStatus {
    match value {
        DetailedEligibility::EligibleRunning | DetailedEligibility::EligibleInbox => {
            SchedulingStatus::Ready
        }
        DetailedEligibility::Scheduled(ts) => SchedulingStatus::Scheduled { at: ts },
        DetailedEligibility::Empty => SchedulingStatus::Empty,
    }
}

#[derive(Default, Debug)]
pub struct Decisions {
    // Vqueue ids are ordered by partition key, this enables us to group
    // vqueues within the same partition key together when scanning through
    // the scheduler's decisions.
    pub qids: BTreeMap<VQueueId, Vec<SchedulerAction>>,
    /// running items
    pub num_run: u32,
    /// Items in run queue that need to go back to waiting inbox
    pub num_yield: u32,
}

impl Decisions {
    pub fn push(&mut self, qid: &VQueueId, action: impl Into<SchedulerAction>) {
        let action = action.into();
        match action {
            SchedulerAction::Unknown => unreachable!(),
            SchedulerAction::Yield(_) => self.num_yield += 1,
            SchedulerAction::Run(_) => self.num_run += 1,
        }

        if let Some(actions) = self.qids.get_mut(qid) {
            actions.push(action);
        } else {
            self.qids.insert(qid.clone(), vec![action]);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.qids.is_empty()
    }

    /// The number of vqueues in this decision
    pub fn num_queues(&self) -> usize {
        self.qids.len()
    }

    #[cfg(test)]
    pub fn num_run(&self) -> usize {
        self.num_run as usize
    }

    #[cfg(test)]
    pub fn num_yield(&self) -> usize {
        self.num_yield as usize
    }

    /// Total number of items in all queues
    pub fn total_items(&self) -> usize {
        self.num_run as usize + self.num_yield as usize
    }

    pub fn report_metrics(&self) {
        publish_scheduler_decision_metrics(self.num_run, self.num_yield);
    }
}

enum State<S: VQueueStore> {
    Active(Pin<Box<DRRScheduler<S>>>),
    Disabled,
}

pub struct SchedulerService<S: VQueueStore> {
    state: State<S>,
}

impl<S: VQueueStore> SchedulerService<S> {
    pub fn new_disabled() -> Self
    where
        S: ScanVQueueTable,
    {
        Self {
            state: State::<S>::Disabled,
        }
    }

    pub async fn create(
        resource_manager: ResourceManager,
        storage: S,
        vqueues_cache: &VQueuesMetaCache,
    ) -> Result<Self, StorageError>
    where
        S: ScanVQueueTable,
    {
        // We maintain a clone of the vqueue cache that's snapshotted at the time of creation
        // of the scheduler. The clone is then kept in-sync by applying the events emitted
        // by the partition processor.
        let state = State::Active(Box::pin(DRRScheduler::new(
            // this assumes a worst case of 2 assignment segments per queue.
            // This limits the total number of commands we send via propose_many to bifrost.
            //
            // Note that propose_many will error out if the number of commands is greater than the
            // channel's max capacity.
            NonZeroU16::new(25).unwrap(),
            // currently constant but we can make it configurable if needed
            NonZeroU16::new(1000).unwrap(),
            resource_manager,
            storage,
            vqueues_cache.view(),
        )));
        Ok(Self { state })
    }

    pub fn on_inbox_event(&mut self, event: VQueueEvent) {
        if let State::Active(ref mut drr_scheduler) = self.state {
            drr_scheduler.as_mut().on_inbox_event(event);
        }
    }

    /// Return reserved resources (concurrency permit + memory lease) for a given
    /// item hash if it was assigned by the scheduler.
    ///
    /// Resources will not be returned if the unconfirmed assignment was rejected or removed.
    pub fn confirm_run_attempt(
        &mut self,
        qid: &VQueueId,
        key: &EntryKey,
    ) -> Option<ReservedResources> {
        if let State::Active(ref mut drr_scheduler) = self.state {
            drr_scheduler.as_mut().confirm_run_attempt(qid, key)
        } else {
            None
        }
    }

    pub async fn schedule_next(&mut self) -> Result<Decisions, StorageError> {
        poll_fn(|cx| self.poll_schedule_next(cx)).await
    }

    pub fn poll_schedule_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Decisions, StorageError>> {
        match self.state {
            // if scheduler is disabled, we always return pending.
            State::Disabled => Poll::Pending,
            State::Active(ref mut drr_scheduler) => drr_scheduler.as_mut().poll_schedule_next(cx),
        }
    }

    /// Returns the scheduling status for a specific vqueue.
    pub fn get_status(&self, qid: &VQueueId) -> VQueueSchedulerStatus {
        match self.state {
            State::Disabled => VQueueSchedulerStatus::default(),
            State::Active(ref drr_scheduler) => drr_scheduler.get_status(qid),
        }
    }

    /// Returns an iterator over the scheduling status of all tracked vqueues.
    ///
    /// Returns `None` if the scheduler is disabled.
    pub fn iter_status(&self) -> Option<impl Iterator<Item = (VQueueId, VQueueSchedulerStatus)>> {
        match self.state {
            State::Disabled => None,
            State::Active(ref drr_scheduler) => Some(drr_scheduler.iter_status()),
        }
    }
}
