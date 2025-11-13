// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;

use std::future::poll_fn;
use std::task::Poll;

use smallvec::SmallVec;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{ScanVQueueTable, VQueueStore};
use restate_types::clock::UniqueTimestamp;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueue::VQueueId;

use crate::InboxEvent;
use crate::{VQueuesMeta, VQueuesMetaMut};

use self::drr::DRRScheduler;

mod capacity;
mod drr;
mod vqueue_state;

const INLINED_SIZE: usize = 4;

#[derive(Debug, Clone)]
pub struct Assignment<Item> {
    pub qid: VQueueId,
    pub items: SmallVec<[Item; INLINED_SIZE]>,
}

impl<Item> Assignment<Item> {
    pub fn new(qid: VQueueId) -> Self {
        Self {
            qid,
            items: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    fn push(&mut self, head: Item) {
        self.items.push(head);
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

#[derive(Debug)]
pub struct Decision<Item> {
    pub action: Action,
    pub assignment: Assignment<Item>,
}

impl<Item> Decision<Item> {
    pub fn is_empty(&self) -> bool {
        self.assignment.is_empty()
    }
    pub fn len(&self) -> usize {
        self.assignment.len()
    }
}

enum State<S: VQueueStore> {
    // a scheduler that tries to resume already running entries and if no capacity is available, it
    // will place them back on the waiting queue.
    Active(Pin<Box<DRRScheduler<S>>>),
    Disabled,
}

pub struct SchedulerService<S: VQueueStore> {
    state: State<S>,
}

impl<S> SchedulerService<S>
where
    S: VQueueStore,
    S::Item: std::fmt::Debug,
{
    pub fn new_disabled() -> Self
    where
        S: ScanVQueueTable,
    {
        Self {
            state: State::Disabled,
        }
    }

    pub async fn create(
        storage: S,
        vqueues_cache: &mut VQueuesMetaMut,
    ) -> Result<Self, StorageError>
    where
        S: ScanVQueueTable,
    {
        // we need to load all active vqueues (non-empty) and in particular vqueues
        // that have already running entries.

        // We do not want to discard the state of the cache. We want to respect whatever
        // was there before becoming a leader since we know it's up-to-date.
        vqueues_cache.load_all_active_vqueues(&storage).await?;

        let state = State::Active(Box::pin(DRRScheduler::new(storage, vqueues_cache.view())));
        Ok(Self { state })
    }

    pub fn on_inbox_event(
        &mut self,
        now: UniqueTimestamp,
        vqueues: VQueuesMeta<'_>,
        event: &InboxEvent<S::Item>,
    ) -> Result<(), StorageError> {
        if let State::Active(ref mut drr_scheduler) = self.state {
            drr_scheduler.as_mut().on_inbox_event(now, vqueues, event)?;
        }
        Ok(())
    }

    pub async fn schedule_next(
        &mut self,
        vqueues: VQueuesMeta<'_>,
    ) -> Result<Decision<S::Item>, StorageError> {
        let now = UniqueTimestamp::from_unix_millis(MillisSinceEpoch::now()).unwrap();
        poll_fn(|cx| self.poll_schedule_next(now, cx, vqueues)).await
    }

    pub fn poll_schedule_next(
        &mut self,
        now: UniqueTimestamp,
        cx: &mut std::task::Context<'_>,
        vqueues: VQueuesMeta<'_>,
    ) -> Poll<Result<Decision<S::Item>, StorageError>> {
        match self.state {
            // if scheduler is disabled, we always return pending.
            State::Disabled => Poll::Pending,
            State::Active(ref mut drr_scheduler) => {
                drr_scheduler.as_mut().poll_schedule_next(now, cx, vqueues)
            }
        }
    }
}
