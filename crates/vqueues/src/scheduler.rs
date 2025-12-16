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

use std::future::poll_fn;
use std::task::Poll;

use hashbrown::HashMap;
use smallvec::SmallVec;

use restate_futures_util::concurrency::{Concurrency, Permit};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{ScanVQueueTable, VQueueEntry, VQueueStore, WaitStats};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueue::VQueueId;

use crate::VQueueEvent;
use crate::metric_definitions::publish_scheduler_decision_metrics;
use crate::{VQueuesMeta, VQueuesMetaMut};

use self::drr::DRRScheduler;
use self::vqueue_state::DetailedEligibility;

mod clock;
mod drr;
mod eligible;
mod queue;
mod vqueue_state;

const INLINED_SIZE: usize = 4;

slotmap::new_key_type! { struct VQueueHandle; }

// Token bucket used for throttling over all vqueues
type GlobalTokenBucket<C = gardal::TokioClock> =
    gardal::TokenBucket<gardal::PaddedAtomicSharedStorage, C>;

/// Indicates the pause state of a vqueue.
#[derive(Debug, Clone, Default)]
pub enum IsPaused {
    #[default]
    /// The vqueue is enabled
    No,
    /// The vqueue is paused directly by the user
    Directly,
    /// The vqueue is paused because its parent is paused
    ViaParent,
}

impl IsPaused {
    pub fn yes(&self) -> bool {
        matches!(self, IsPaused::Directly | IsPaused::ViaParent)
    }
}

/// A public view of the scheduler's status of a single vqueue.
///
/// This struct provides introspection into the current scheduling state, and
/// wait statistics for a vqueue.
#[derive(Debug, Clone, Default)]
pub struct VQueueSchedulerStatus {
    /// Whether the vqueue is paused or not.
    pub is_paused: IsPaused,
    /// Statistics about the wait time experienced by the head item in the vqueue.
    pub wait_stats: WaitStats,
    /// Number of items remaining in the running stage.
    pub remaining_running: u32,
    /// Number of items waiting in the inbox stage.
    pub waiting_inbox: u32,
    /// Number of concurrency tokens currently in use by this vqueue.
    pub tokens_used: u32,
    /// The current scheduling status of this vqueue.
    pub status: SchedulingStatus,
}

/// The current scheduling status of a vqueue.
///
/// This enum represents the various states a vqueue can be in from the
/// scheduler's perspective.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SchedulingStatus {
    #[default]
    /// The vqueue is not tracked by the scheduler (e.g., it has no items).
    Dormant,
    /// The vqueue is empty.
    Empty,
    /// The vqueue head is ready to be scheduled and it's in the inbox/running stage.
    Ready,
    /// The vqueue is throttled until the specified time.
    Throttled {
        /// When the throttle expires.
        until: MillisSinceEpoch,
        /// The scope of throttling (global or per-vqueue).
        scope: ThrottleScope,
    },
    /// The vqueue is scheduled to be woken up at the given time because the head
    /// item is scheduled to run at that time.
    Scheduled {
        /// When the head item becomes visible.
        at: MillisSinceEpoch,
    },
    /// The vqueue is blocked on invoker global capacity.
    BlockedOnCapacity,
    /// The vqueue is waiting for concurrency tokens. Concurrency tokens are released
    /// when currently running items are completed or (in some cases) when running items
    /// are parked.
    WaitingConcurrencyTokens,
}

impl From<DetailedEligibility> for SchedulingStatus {
    fn from(value: DetailedEligibility) -> Self {
        match value {
            DetailedEligibility::EligibleRunning | DetailedEligibility::EligibleInbox => {
                SchedulingStatus::Ready
            }
            DetailedEligibility::Scheduled(ts) => SchedulingStatus::Scheduled { at: ts },
            DetailedEligibility::WaitingConcurrencyTokens => {
                SchedulingStatus::WaitingConcurrencyTokens
            }
            DetailedEligibility::Empty => SchedulingStatus::Empty,
        }
    }
}

/// The scope at which throttling is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleScope {
    /// Throttling is applied globally across all vqueues.
    Global,
    /// Throttling is applied to a specific vqueue.
    VQueue,
}

#[derive(Debug)]
pub struct Entry<Item> {
    pub item: Item,
    pub stats: WaitStats,
    pub permit: Permit,
}
impl<Item> Entry<Item> {
    pub fn split(self) -> (Item, WaitStats, Permit) {
        (self.item, self.stats, self.permit)
    }
}

#[derive(Debug)]
pub struct Assignments<Item> {
    latest_run_tb_zero_time: Option<f64>,

    // In the overwhelming majority of cases, we will have only one segment (inbox or running)
    // and in rare cases we may have both. If we (in the future) support sending the three actions
    // on the same scheduler's poll, we'll accept to allocated in those rare cases where the three actions
    // are simulatenously picked.
    segments: SmallVec<[AssignmentSegment<Item>; 2]>,
}

impl<Item> Default for Assignments<Item> {
    fn default() -> Self {
        Self {
            latest_run_tb_zero_time: None,
            segments: smallvec::smallvec![],
        }
    }
}

impl<Item> Assignments<Item> {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (Action, &[Entry<Item>])> {
        self.segments
            .iter()
            .map(|segment| (segment.action, segment.items.as_slice()))
    }

    pub fn into_iter_per_action(
        self,
    ) -> impl ExactSizeIterator<Item = (Action, impl ExactSizeIterator<Item = Entry<Item>>)> {
        self.segments
            .into_iter()
            .map(|segment| (segment.action, segment.items.into_iter()))
    }

    pub fn into_iter_all(self) -> impl IntoIterator {
        self.segments.into_iter().flat_map(|segment| segment.items)
    }

    pub fn push(&mut self, action: Action, entry: Entry<Item>) {
        // manipulate the last segment if the action is the same.
        if let Some(last_segment) = self.segments.last_mut()
            && last_segment.action == action
        {
            last_segment.items.push(entry);
        } else {
            self.segments.push(AssignmentSegment {
                action,
                items: smallvec::smallvec![entry],
            });
        }
    }

    /// Allows us to track the token bucket's zero time across scheduler runs.
    pub fn set_latest_run_tb_zero_time(&mut self, zero_time: Option<f64>) {
        self.latest_run_tb_zero_time = zero_time;
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn updated_tb_zero_time(&self) -> Option<f64> {
        self.latest_run_tb_zero_time
    }
}

#[derive(Debug)]
pub struct AssignmentSegment<Item> {
    pub action: Action,
    pub items: SmallVec<[Entry<Item>; INLINED_SIZE]>,
}

impl<Item> AssignmentSegment<Item> {
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Action {
    /// Items are in inbox, let's move them to running queue.
    MoveToRunning,
    /// Items are already in running queue, execute them now.
    ResumeAlreadyRunning,
    /// Items was already in running queue and we want them to yield back to the inbox
    Yield,
}

#[derive(derive_more::IntoIterator, derive_more::Debug)]
pub struct Decision<Item> {
    #[into_iterator]
    q: HashMap<VQueueId, Assignments<Item>>,
    /// items running for the first time
    num_start: u16,
    /// running items previously started
    num_run: u16,
    /// running items that should continue to run
    num_resume: u16,
    /// Items in run queue that need to go back to waiting inbox
    num_yield: u16,
}

impl<Item> Default for Decision<Item> {
    fn default() -> Self {
        Self {
            q: HashMap::default(),
            num_start: 0,
            num_run: 0,
            num_resume: 0,
            num_yield: 0,
        }
    }
}

impl<Item: VQueueEntry> Decision<Item> {
    pub fn push(
        &mut self,
        qid: &VQueueId,
        action: Action,
        entry: Entry<Item>,
        updated_zt: Option<f64>,
    ) {
        let assignments = self.q.entry_ref(qid).or_default();
        assignments.set_latest_run_tb_zero_time(updated_zt);
        match action {
            Action::ResumeAlreadyRunning => self.num_resume += 1,
            Action::Yield => self.num_yield += 1,
            Action::MoveToRunning if entry.item.priority().is_new() => self.num_start += 1,
            Action::MoveToRunning => self.num_run += 1,
        }
        assignments.push(action, entry);
    }

    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }

    /// The number of vqueues in this decision
    pub fn num_queues(&self) -> usize {
        self.q.len()
    }

    /// Returns an iterator over the vqueue IDs in this decision.
    pub fn iter_qids(&self) -> impl Iterator<Item = &VQueueId> {
        self.q.keys()
    }

    #[cfg(test)]
    pub fn num_start(&self) -> usize {
        self.num_start as usize
    }

    #[cfg(test)]
    pub fn num_run(&self) -> usize {
        self.num_run as usize
    }

    #[cfg(test)]
    pub fn num_resume(&self) -> usize {
        self.num_resume as usize
    }

    #[cfg(test)]
    pub fn num_yield(&self) -> usize {
        self.num_yield as usize
    }

    /// Total number of items in all queues
    pub fn total_items(&self) -> usize {
        self.num_start as usize
            + self.num_run as usize
            + self.num_resume as usize
            + self.num_yield as usize
    }

    pub fn report_metrics(&self) {
        publish_scheduler_decision_metrics(
            self.num_start,
            self.num_run,
            self.num_yield,
            self.num_resume,
        );
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
        concurrency: Concurrency,
        global_throttling: Option<GlobalTokenBucket>,
        storage: S,
        vqueues_cache: &mut VQueuesMetaMut,
    ) -> Result<Self, StorageError>
    where
        S: ScanVQueueTable,
    {
        // We need to load all active vqueues (non-empty) and in particular vqueues
        // that have already running entries.

        // We do not want to discard the state of the cache. We want to respect whatever
        // was there before becoming a leader since we know it's up-to-date.
        vqueues_cache.load_all_active_vqueues(&storage).await?;

        let state = State::Active(Box::pin(DRRScheduler::new(
            // this assumes a worst case of 2 assignment segments per queue.
            // This limits the total number of commands we send via propose_many to bifrost.
            //
            // Note that propose_many will error out if the number of commands is greater than the
            // channel's max capacity.
            NonZeroU16::new(25).unwrap(),
            // currently constant but we can make it configurable if needed
            NonZeroU16::new(1000).unwrap(),
            concurrency,
            global_throttling,
            storage,
            vqueues_cache.view(),
        )));
        Ok(Self { state })
    }

    pub fn on_inbox_event(
        &mut self,
        vqueues: VQueuesMeta<'_>,
        event: &VQueueEvent<S::Item>,
    ) -> Result<(), StorageError> {
        if let State::Active(ref mut drr_scheduler) = self.state {
            drr_scheduler.as_mut().on_inbox_event(vqueues, event)?;
        }
        Ok(())
    }

    pub async fn schedule_next(
        &mut self,
        vqueues: VQueuesMeta<'_>,
    ) -> Result<Decision<S::Item>, StorageError> {
        poll_fn(|cx| self.poll_schedule_next(cx, vqueues)).await
    }

    pub fn poll_schedule_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
        vqueues: VQueuesMeta<'_>,
    ) -> Poll<Result<Decision<S::Item>, StorageError>> {
        match self.state {
            // if scheduler is disabled, we always return pending.
            State::Disabled => Poll::Pending,
            State::Active(ref mut drr_scheduler) => {
                drr_scheduler.as_mut().poll_schedule_next(cx, vqueues)
            }
        }
    }

    /// Returns the scheduling status for a specific vqueue.
    ///
    /// Returns `None` if the scheduler is disabled or the vqueue is not tracked.
    pub fn get_status(
        &self,
        qid: &VQueueId,
        vqueues: VQueuesMeta<'_>,
    ) -> Option<VQueueSchedulerStatus> {
        match self.state {
            State::Disabled => None,
            State::Active(ref drr_scheduler) => drr_scheduler.get_status(qid, vqueues),
        }
    }

    /// Returns an iterator over the scheduling status of all tracked vqueues.
    ///
    /// Returns `None` if the scheduler is disabled.
    pub fn iter_status(
        &self,
        vqueues: VQueuesMeta<'_>,
    ) -> Option<impl Iterator<Item = (VQueueId, VQueueSchedulerStatus)>> {
        match self.state {
            State::Disabled => None,
            State::Active(ref drr_scheduler) => Some(drr_scheduler.iter_status(vqueues)),
        }
    }
}
