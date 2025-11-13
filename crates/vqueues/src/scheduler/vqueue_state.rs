// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hashbrown::HashSet;
use tracing::info;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{VQueueCursor, VQueueEntry, VQueueStore, VisibleAt};
use restate_types::clock::UniqueTimestamp;
use restate_types::vqueue::VQueueId;

use super::Assignment;
use super::Decision;
use crate::scheduler::Action;
use crate::vqueue_config::VQueueConfig;

// weight of any vqueue compared to others; This can be later used to determine the relative
// priority/cost between vqueues.
const QUANTA: i32 = super::INLINED_SIZE as i32;
// how much credit do we consume per item.
const COST: i32 = 1;

#[derive(derive_more::Debug)]
enum Reader<S: VQueueStore> {
    // Reader was never opened
    #[debug("New")]
    New,
    #[debug("Running")]
    Running(S::RunningReader),
    #[debug("Inbox")]
    Inbox(S::InboxReader),
    // reader was closed due to vqueue being empty. Note that this only happens
    // after we read the running and inbox.
    //
    // We can transition back to Reader::Inbox if new items have been added to the inbox
    // but we should not return to `Running`.
    #[debug("Closed")]
    Closed,
}

enum QueueItem<Item> {
    Running(Item),
    Inboxed(Item),
    None,
}

#[derive(Debug)]
enum HeadStatus<Item> {
    /// We need a seek+read to know the head.
    Unknown,
    /// The current cursor's head
    Known(Item),
    /// We know that we've reached the end of the vqueue
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Status {
    // if Eligible, we will keep keep picking from the same vqueue on the next
    // round
    Eligible,
    EligibleAt(UniqueTimestamp),
    WaitingServiceCapacity,
    WaitingConcurrency,
    Empty,
    Paused,
}

#[derive(derive_more::Debug)]
pub struct VQueueState<S: VQueueStore> {
    #[debug(skip)]
    reader: Reader<S>,
    status: Status,
    // contains hashes (unique_hash)
    unconfirmed_assignments: HashSet<u64>,
    deficit: i32, // DRR, we take from it when we pick an entry and we add to it when vqueue is
    last_round: i32, // lazy accrual
    head: HeadStatus<S::Item>,
}

impl<S> VQueueState<S>
where
    S: VQueueStore,
    S::Item: std::fmt::Debug,
{
    pub fn new() -> Self {
        Self {
            status: Status::Eligible,
            reader: Reader::New,
            unconfirmed_assignments: HashSet::new(),
            deficit: 0,
            last_round: 0,
            head: HeadStatus::Unknown,
        }
    }

    pub fn new_empty(last_round: i32) -> Self {
        Self {
            status: Status::Empty,
            reader: Reader::Closed,
            unconfirmed_assignments: HashSet::new(),
            deficit: 0,
            last_round,
            head: HeadStatus::Empty,
        }
    }

    #[inline(always)]
    pub fn can_spend_credit(&self) -> bool {
        self.deficit >= COST
    }

    pub fn pick_next(
        &mut self,
        now: UniqueTimestamp,
        qid: &VQueueId,
        storage: &S,
        meta: &VQueueMeta,
        config: &VQueueConfig,
        global_sched_round: i32,
    ) -> Result<Decision<S::Item>, StorageError> {
        let mut action = Action::MoveToRunning;
        let mut assignment = Assignment::new(*qid);
        // after finishing picking, we need to know whether we are still eligible (now), or (later)
        // or if we lost eligibility.
        self.deficit += (global_sched_round - self.last_round) * QUANTA;
        // cap credit to 10 times quanta
        self.deficit = self.deficit.min(QUANTA * 10);
        self.last_round = global_sched_round;

        while self.can_spend_credit() {
            match self.peek_if_eligible(now, storage, qid, meta, config)? {
                QueueItem::Running(head) => {
                    debug_assert!(
                        matches!(action, Action::ResumeAlreadyRunning | Action::Yield)
                            || assignment.is_empty()
                    );
                    // switch between these to change the behavior as needed
                    action = Action::ResumeAlreadyRunning;
                    // action = Action::Yield;

                    // add the item to the assignment
                    assignment.push(head);
                    self.advance(storage, qid, meta)?;
                    self.deficit -= COST;
                }
                QueueItem::Inboxed(head) => {
                    // If we have been reading "running" items, we would like to stop when
                    // we switch into inboxed since they will return a different decision.
                    if matches!(action, Action::ResumeAlreadyRunning | Action::Yield) {
                        break;
                    }

                    self.unconfirmed_assignments.insert(head.unique_hash());
                    assignment.push(head);
                    self.advance(storage, qid, meta)?;
                    self.deficit -= COST;
                }
                QueueItem::None => {
                    break;
                }
            }
        }

        Ok(Decision { action, assignment })
    }

    pub fn status(&self) -> Status {
        self.status
    }

    pub fn maybe_eligible(&self, now: UniqueTimestamp) -> bool {
        match self.status {
            Status::Eligible => true,
            Status::EligibleAt(at) if now >= at => true,
            _ => false,
        }
    }

    fn peek_if_eligible(
        &mut self,
        now: UniqueTimestamp,
        storage: &S,
        qid: &VQueueId,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> Result<QueueItem<S::Item>, StorageError> {
        let inbox_head = loop {
            match self.head {
                HeadStatus::Unknown => self.advance(storage, qid, meta)?,
                HeadStatus::Known(ref item) => {
                    if matches!(self.reader, Reader::Running(_)) {
                        // Running entries are always eligible to run
                        self.status = Status::Eligible;
                        return Ok(QueueItem::Running(item.clone()));
                    } else {
                        break item;
                    }
                }
                HeadStatus::Empty => {
                    self.status = Status::Empty;
                    return Ok(QueueItem::None);
                }
            }
        };

        if meta.is_paused() {
            self.status = Status::Paused;
            return Ok(QueueItem::None);
        }

        // Only applies to inboxed items.
        if let VisibleAt::At(ts) = inbox_head.visible_at()
            && ts > now
        {
            self.status = Status::EligibleAt(ts);
            return Ok(QueueItem::None);
        }

        // todo: this is where more checks for eligibility should happen
        if inbox_head.is_token_held() || self.has_available_tokens(meta, config) {
            self.status = Status::Eligible;
            Ok(QueueItem::Inboxed(inbox_head.clone()))
        } else {
            self.status = Status::WaitingConcurrency;
            // change not eligible to carry the reason maybe?
            Ok(QueueItem::None)
        }
    }

    fn advance(
        &mut self,
        storage: &S,
        qid: &VQueueId,
        meta: &VQueueMeta,
    ) -> Result<(), StorageError> {
        loop {
            match self.reader {
                Reader::New if meta.num_running() > 0 => {
                    info!("Seeking to first running entry");
                    let mut it = storage.new_run_reader(qid);
                    it.seek_to_first();
                    let item = it.peek()?;
                    if let Some(item) = item {
                        info!("Found a running entry, {item:?}");
                        self.head = HeadStatus::Known(item);
                        self.reader = Reader::Running(it);
                        break;
                    } else {
                        info!("No running entries");
                        // move to inbox reading
                        self.head = HeadStatus::Unknown;
                        self.reader = Reader::Closed;
                    }
                }
                Reader::New => {
                    // create new inbox reader
                    self.reader = Reader::Closed;
                }
                Reader::Running(ref mut it) => {
                    info!("Advancing to the next running entry");
                    it.advance();
                    let item = it.peek()?;
                    if let Some(item) = item {
                        info!("Found more running entries");
                        self.head = HeadStatus::Known(item);
                        break;
                    } else {
                        info!("Ran out of running entries");
                        // move to inbox reading
                        self.head = HeadStatus::Unknown;
                        self.reader = Reader::Closed;
                    }
                }
                Reader::Inbox(ref mut it) => {
                    info!("Advancing to the next inbox entry");
                    it.advance();
                    let item = it.peek()?;
                    if let Some(item) = item {
                        if self.unconfirmed_assignments.contains(&item.unique_hash()) {
                            info!("skipping over an unconfirmed assignment (inbox)");
                            continue;
                        }
                        info!("Found inbox entry {item:?}");
                        self.head = HeadStatus::Known(item);
                        break;
                    } else {
                        info!("Ran out of inbox entries");
                        // we are done reading inbox
                        self.head = HeadStatus::Empty;
                        self.reader = Reader::Closed;
                        break;
                    }
                }
                Reader::Closed => {
                    match self.head {
                        HeadStatus::Unknown => {
                            info!("creating inbox reader");
                            let mut it = storage.new_inbox_reader(qid);
                            it.seek_to_first();
                            let item = it.peek()?;
                            self.reader = Reader::Inbox(it);
                            if let Some(item) = item {
                                if self.unconfirmed_assignments.contains(&item.unique_hash()) {
                                    info!(
                                        "[headstatus=unknown] skipping over an unconfirmed assignment (inbox)"
                                    );
                                    continue;
                                }
                                info!("Found inbox entry {item:?}");
                                self.head = HeadStatus::Known(item);
                                break;
                            } else {
                                info!("Ran out of inbox entries");
                                self.head = HeadStatus::Empty;
                                self.reader = Reader::Closed;
                            }
                        }
                        HeadStatus::Known(ref item) => {
                            // seek to known head first, then advance.
                            let mut it = storage.new_inbox_reader(qid);
                            it.seek_after(qid, item);
                            let item = it.peek()?;
                            self.reader = Reader::Inbox(it);
                            if let Some(item) = item {
                                if self.unconfirmed_assignments.contains(&item.unique_hash()) {
                                    continue;
                                }
                                self.head = HeadStatus::Known(item);
                                break;
                            } else {
                                self.head = HeadStatus::Empty;
                                self.reader = Reader::Closed;
                            }
                        }
                        HeadStatus::Empty => {
                            // do nothing.
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn maybe_refresh_status(
        &mut self,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) {
        if meta.is_paused() {
            self.status = Status::Paused;
            return;
        }

        let inbox_head = match self.head {
            HeadStatus::Unknown => return,
            HeadStatus::Known(ref item) => {
                if matches!(self.reader, Reader::Running(_)) {
                    // Running entries are always eligible to run
                    self.status = Status::Eligible;
                    return;
                }
                item
            }
            HeadStatus::Empty => {
                self.status = Status::Empty;
                return;
            }
        };

        // Only applies to inboxed items.
        if let VisibleAt::At(ts) = inbox_head.visible_at()
            && ts > now
        {
            self.status = Status::EligibleAt(ts);
            return;
        }

        // todo: this is where more checks for eligibility should happen
        if inbox_head.is_token_held() || self.has_available_tokens(meta, config) {
            self.status = Status::Eligible;
        } else {
            self.status = Status::WaitingConcurrency;
        }
    }

    pub fn num_ready(&self, meta: &VQueueMeta) -> u32 {
        // ready are items we can ship.
        meta.total_waiting() - self.unconfirmed_assignments.len() as u32
    }

    pub fn has_available_tokens(&self, meta: &VQueueMeta, config: &VQueueConfig) -> bool {
        let tokens_used = meta.tokens_used() + self.unconfirmed_assignments.len() as u32;
        config
            .concurrency()
            .is_none_or(|limit| tokens_used < limit.get())
    }

    pub fn notify_assignment_confirmed(
        &mut self,
        item_hash: u64,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> bool {
        if self.unconfirmed_assignments.remove(&item_hash) {
            info!("assignment confirmed: {:?}", item_hash);
            self.maybe_refresh_status(now, meta, config);
            return true;
        }
        false
    }

    pub fn notify_assignment_rejected(
        &mut self,
        item_hash: u64,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> bool {
        if self.unconfirmed_assignments.remove(&item_hash) {
            info!("assignment rejected: {:?}", item_hash);
            self.maybe_refresh_status(now, meta, config);
            return true;
        }
        false
    }

    pub fn notify_removed(
        &mut self,
        item_hash: u64,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) {
        // Can this be the known head?
        // Yes. Perhaps it expired/ended externally.
        if let HeadStatus::Known(ref item) = self.head
            && item.unique_hash() == item_hash
        {
            info!("removing from inbox, it was the previous head!");
            self.head = HeadStatus::Unknown;
            // Ensure that next advance would re-seek to the newly added item
            self.reader = Reader::Closed;
            // become eligible so we poll the head again.
            self.status = Status::Eligible;
        }
        // was it unconfirmed?
        self.unconfirmed_assignments.remove(&item_hash);
        self.maybe_refresh_status(now, meta, config);
    }

    pub fn notify_enqueued(
        &mut self,
        item: &S::Item,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> bool {
        match (&self.head, &self.reader) {
            // we are only unknown if we are new and didn't read the running list yet,
            // we might also be un a limbo state if advance() failed.
            (_, Reader::New | Reader::Running(_)) => { /* do nothing */ }
            (HeadStatus::Unknown, _) => { /* do nothing */ }
            (HeadStatus::Empty, _) => {
                self.reader = Reader::Closed;
                // number of tokens used decreased.
                self.unconfirmed_assignments.remove(&item.unique_hash());
                self.head = HeadStatus::Known(item.clone());
                self.maybe_refresh_status(now, meta, config);
                return true;
            }
            (HeadStatus::Known(current), Reader::Inbox(_) | Reader::Closed) => {
                self.unconfirmed_assignments.remove(&item.unique_hash());
                if item < current {
                    self.head = HeadStatus::Known(item.clone());
                    // Ensure that next advance would re-seek to the newly added item
                    self.reader = Reader::Closed;
                }
                self.maybe_refresh_status(now, meta, config);
                return true;
            }
        }
        false
    }
}
