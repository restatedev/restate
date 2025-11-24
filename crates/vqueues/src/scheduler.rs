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
use restate_storage_api::vqueue_table::{ScanVQueueTable, VQueueStore};
use restate_types::vqueue::VQueueId;

use crate::VQueueEvent;
use crate::{VQueuesMeta, VQueuesMetaMut};

use self::drr::DRRScheduler;

mod clock;
mod drr;
mod vqueue_state;

const INLINED_SIZE: usize = vqueue_state::QUANTUM as usize;

#[derive(Debug)]
pub struct Assignments<Item> {
    // In the overwhelming majority of cases, we will have only one segment (inbox or running)
    // and in rare cases we may have both. If we (in the future) support sending the three actions
    // on the same scheduler's poll, we'll accept to allocated in those rare cases where the three actions
    // are simulatenously picked.
    segments: SmallVec<[AssignmentSegment<Item>; 2]>,
}

impl<Item> Default for Assignments<Item> {
    fn default() -> Self {
        Self {
            segments: smallvec::smallvec![],
        }
    }
}

impl<Item> Assignments<Item> {
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (Action, &[Item])> {
        self.segments
            .iter()
            .map(|segment| (segment.action, segment.items.as_slice()))
    }

    pub fn into_iter_per_action(
        self,
    ) -> impl ExactSizeIterator<Item = (Action, impl ExactSizeIterator<Item = Item>)> {
        self.segments
            .into_iter()
            .map(|segment| (segment.action, segment.items.into_iter()))
    }

    pub fn push(&mut self, action: Action, item: Item) {
        // manipulate the last segment if the action is the same.
        if let Some(last_segment) = self.segments.last_mut()
            && last_segment.action == action
        {
            last_segment.items.push(item);
        } else {
            self.segments.push(AssignmentSegment {
                action,
                items: smallvec::smallvec![item],
            });
        }
    }

    pub fn merge(&mut self, other: Self) {
        self.segments.extend(other.segments);
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }
}

#[derive(Debug)]
pub struct AssignmentSegment<Item> {
    pub action: Action,
    pub items: SmallVec<[Item; INLINED_SIZE]>,
}

impl<Item> AssignmentSegment<Item> {
    pub fn new(action: Action, item: Item) -> Self {
        Self {
            action,
            items: smallvec::smallvec![item],
        }
    }

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

#[derive(derive_more::IntoIterator, Debug)]
pub struct Decision<Item>(HashMap<VQueueId, Assignments<Item>>);

impl<Item> Decision<Item> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

enum State<S: VQueueStore, Token> {
    Active(Pin<Box<DRRScheduler<S, Token>>>),
    Disabled,
}

pub struct SchedulerService<S: VQueueStore, Token> {
    state: State<S, Token>,
}

impl<S, Token> SchedulerService<S, Token>
where
    S: VQueueStore,
    S::Item: std::fmt::Debug,
{
    pub fn new_disabled() -> Self
    where
        S: ScanVQueueTable,
    {
        Self {
            state: State::<S, Token>::Disabled,
        }
    }

    pub async fn create(
        concurrency: Concurrency<Token>,
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
            concurrency,
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

    pub fn confirm_assignment(&mut self, qid: &VQueueId, item_hash: u64) -> Option<Permit<Token>> {
        match self.state {
            State::Disabled => None,
            State::Active(ref mut drr_scheduler) => {
                drr_scheduler.as_mut().confirm_assignment(qid, item_hash)
            }
        }
    }
}
