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
use tracing::trace;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{VQueueMeta, VQueueStatus};
use restate_storage_api::vqueue_table::{VQueueCursor, VQueueEntry, VQueueStore, VisibleAt};
use restate_types::clock::UniqueTimestamp;
use restate_types::vqueue::VQueueId;

use super::Assignments;
use crate::scheduler::Action;
use crate::vqueue_config::VQueueConfig;

pub(super) const QUANTUM: i32 = 4;

#[derive(derive_more::Debug)]
enum Reader<S: VQueueStore> {
    /// Reader was never opened and has already running items to go through first
    #[debug("NewWithRunning")]
    NewWithRunning,
    /// Reader was never opened and does not need to scan running items
    #[debug("NewSkipRunning")]
    NewSkipRunning,
    #[debug("Running")]
    Running(S::RunningReader),
    #[debug("Inbox")]
    Inbox(S::InboxReader),
    // We can transition back to Reader::Inbox if new items have been added to the inbox
    // but we should never return to `Running`.
    #[debug("Closed")]
    Closed,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::IsVariant)]
pub(super) enum Eligibility {
    Eligible,
    EligibleAt(UniqueTimestamp),
    NotEligible,
}

#[derive(derive_more::Debug)]
pub struct VQueueState<S: VQueueStore> {
    qid: VQueueId,
    #[debug(skip)]
    reader: Reader<S>,
    // contains hashes (unique_hash)
    unconfirmed_assignments: HashSet<u64>,
    pub(super) deficit: Deficit,
    head: HeadStatus<S::Item>,
}

impl<S> VQueueState<S>
where
    S: VQueueStore,
    S::Item: std::fmt::Debug,
{
    pub fn new(qid: VQueueId, has_running_items: bool) -> Self {
        Self {
            qid,
            reader: if has_running_items {
                Reader::NewWithRunning
            } else {
                Reader::NewSkipRunning
            },
            unconfirmed_assignments: HashSet::new(),
            deficit: Deficit::new(0),
            head: HeadStatus::Unknown,
        }
    }

    pub fn new_empty(qid: VQueueId, current_round: u16) -> Self {
        Self {
            qid,
            reader: Reader::Closed,
            unconfirmed_assignments: HashSet::new(),
            deficit: Deficit::new(current_round),
            head: HeadStatus::Empty,
        }
    }

    /// Pops the head, unchecked.
    ///
    /// This must be used after `poll_head()` of the queue.
    ///
    /// Panics if the queue's head is unknown or empty.
    pub fn pop_unchecked(
        &mut self,
        storage: &S,
        assignments: &mut Assignments<S::Item>,
    ) -> Result<(), StorageError> {
        match self.head {
            HeadStatus::Unknown => {
                panic!("poll_head must be called before pop_unchecked");
            }
            HeadStatus::Empty => {
                panic!("vqueue is empty");
            }
            HeadStatus::Known(ref item) => {
                if matches!(self.reader, Reader::Running(_)) {
                    // switch between these to change the behavior as needed
                    // Action::ResumeAlreadyRunning;
                    assignments.push(Action::Yield, item.clone());
                    self.advance(storage)?;
                } else {
                    self.unconfirmed_assignments.insert(item.unique_hash());
                    assignments.push(Action::MoveToRunning, item.clone());
                    self.advance(storage)?;
                }
            }
        }

        Ok(())
    }

    pub fn poll_head(&mut self, storage: &S, meta: &VQueueMeta) -> Result<(), StorageError> {
        // Keep advancing until the head is known
        while matches!(self.head, HeadStatus::Unknown) && !meta.is_paused() {
            self.advance(storage)?;
        }

        Ok(())
    }

    fn advance(&mut self, storage: &S) -> Result<(), StorageError> {
        loop {
            match self.reader {
                Reader::NewWithRunning => {
                    let mut it = storage.new_run_reader(&self.qid);
                    it.seek_to_first();
                    let item = it.peek()?;
                    if let Some(item) = item {
                        self.head = HeadStatus::Known(item);
                        self.reader = Reader::Running(it);
                        break;
                    } else {
                        // move to inbox reading
                        self.head = HeadStatus::Unknown;
                        self.reader = Reader::Closed;
                    }
                }
                Reader::NewSkipRunning => {
                    // create new inbox reader
                    self.reader = Reader::Closed;
                }
                Reader::Running(ref mut it) => {
                    it.advance();
                    let item = it.peek()?;
                    if let Some(item) = item {
                        self.head = HeadStatus::Known(item);
                        break;
                    } else {
                        // move to inbox reading
                        self.head = HeadStatus::Unknown;
                        self.reader = Reader::Closed;
                    }
                }
                Reader::Inbox(ref mut it) => {
                    it.advance();
                    let item = it.peek()?;
                    if let Some(item) = item {
                        if self.unconfirmed_assignments.contains(&item.unique_hash()) {
                            trace!("skipping over an unconfirmed assignment (inbox)");
                            continue;
                        }
                        self.head = HeadStatus::Known(item);
                        break;
                    } else {
                        // we are done reading inbox
                        self.head = HeadStatus::Empty;
                        self.reader = Reader::Closed;
                        break;
                    }
                }
                Reader::Closed => {
                    match self.head {
                        HeadStatus::Unknown => {
                            let mut it = storage.new_inbox_reader(&self.qid);
                            it.seek_to_first();
                            let item = it.peek()?;
                            self.reader = Reader::Inbox(it);
                            if let Some(item) = item {
                                if self.unconfirmed_assignments.contains(&item.unique_hash()) {
                                    trace!(
                                        "[HeadStatus=unknown] skipping over an unconfirmed assignment (inbox)"
                                    );
                                    continue;
                                }
                                self.head = HeadStatus::Known(item);
                                break;
                            } else {
                                self.head = HeadStatus::Empty;
                                self.reader = Reader::Closed;
                            }
                        }
                        HeadStatus::Known(ref item) => {
                            // seek to known head first, then advance.
                            let mut it = storage.new_inbox_reader(&self.qid);
                            it.seek_after(&self.qid, item);
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

    pub fn is_empty(&self, meta: &VQueueMeta) -> bool {
        (matches!(self.head, HeadStatus::Empty) || meta.total_waiting() == 0)
            && self.unconfirmed_assignments.is_empty()
    }

    pub fn check_eligibility(
        &mut self,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> Eligibility {
        if meta.is_paused() {
            return Eligibility::NotEligible;
        }

        let inbox_head = match self.head {
            HeadStatus::Unknown
                if matches!(meta.status(), VQueueStatus::Active)
                    && self.num_waiting(meta) > 0
                    && self.has_available_tokens(meta, config) =>
            {
                return Eligibility::Eligible;
            }
            HeadStatus::Unknown => {
                return Eligibility::NotEligible;
            }
            HeadStatus::Known(_) if matches!(self.reader, Reader::Running(_)) => {
                // Running entries are always eligible to run
                return Eligibility::Eligible;
            }
            HeadStatus::Known(ref item) => item,
            HeadStatus::Empty => {
                return Eligibility::NotEligible;
            }
        };

        // Only applies to inboxed items.
        if let VisibleAt::At(ts) = inbox_head.visible_at()
            && ts > now
        {
            return Eligibility::EligibleAt(ts);
        }

        // todo: this is where more checks for eligibility should happen
        if inbox_head.is_token_held() || self.has_available_tokens(meta, config) {
            Eligibility::Eligible
        } else {
            Eligibility::NotEligible
        }
    }

    pub fn num_waiting(&self, meta: &VQueueMeta) -> u32 {
        // ready are items we can ship.
        meta.total_waiting()
            .saturating_sub(self.unconfirmed_assignments.len() as u32)
    }

    pub fn has_available_tokens(&self, meta: &VQueueMeta, config: &VQueueConfig) -> bool {
        let tokens_used = meta.tokens_used() + self.unconfirmed_assignments.len() as u32;
        config
            .concurrency()
            .is_none_or(|limit| tokens_used < limit.get())
    }

    pub fn notify_removed(&mut self, item_hash: u64) {
        // Can this be the known head?
        // Yes. Perhaps it expired/ended externally.
        if let HeadStatus::Known(ref item) = self.head
            && item.unique_hash() == item_hash
        {
            trace!("Removing from inbox, it was the previous head!");
            self.head = HeadStatus::Unknown;
            // Ensure that next advance would re-seek to the newly added item
            self.reader = Reader::Closed;
        }
    }

    pub fn remove_from_unconfirmed_assignments(&mut self, item_hash: u64) -> bool {
        self.unconfirmed_assignments.remove(&item_hash)
    }

    pub fn notify_enqueued(&mut self, item: &S::Item) {
        match (&self.head, &self.reader) {
            // we are only unknown if we are new and didn't read the running list yet,
            // we might also be in a limbo state if advance() failed.
            (_, Reader::NewWithRunning | Reader::NewSkipRunning | Reader::Running(_)) => { /* do nothing */
            }
            (HeadStatus::Unknown, _) => { /* do nothing */ }
            (HeadStatus::Empty, _) => {
                self.reader = Reader::Closed;
                self.head = HeadStatus::Known(item.clone());
            }
            (HeadStatus::Known(current), Reader::Inbox(_) | Reader::Closed) => {
                if item < current {
                    self.head = HeadStatus::Known(item.clone());
                    // Ensure that next advance would re-seek to the newly added item
                    self.reader = Reader::Closed;
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Deficit {
    deficit: i32,
    last_round: u16,
}

impl Deficit {
    /// How much do we cost each queue item
    const COST_PER_ITEM: i32 = 1;

    pub const fn new(last_round: u16) -> Self {
        Self {
            deficit: 0,
            last_round,
        }
    }

    pub fn can_spend(&self) -> bool {
        self.deficit >= Self::COST_PER_ITEM
    }

    pub const fn adjust(&mut self, current_round: u16) {
        // a trick to avoid branching at u16 boundaries.
        // This will return 1 (as expected) if:
        // - current_round = 0, and last_round = u16::MAX
        // - current_round = u16::MAX, and last_round = u16::MAX - 1;
        self.deficit += current_round.wrapping_sub(self.last_round) as i32 * QUANTUM;
        self.last_round = current_round;
    }

    pub const fn set_last_round(&mut self, last_round: u16) {
        self.last_round = last_round;
    }

    pub const fn consume_one(&mut self) {
        self.deficit -= Self::COST_PER_ITEM;
    }

    pub const fn reset(&mut self) {
        self.deficit = 0;
    }
}
