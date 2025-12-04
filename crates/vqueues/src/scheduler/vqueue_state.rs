// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::task::Poll;

use hashbrown::HashSet;
use tracing::{debug, error};

use restate_futures_util::concurrency::{Concurrency, Permit};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{VQueueCursor, VQueueEntry, VQueueStore, VisibleAt};
use restate_types::clock::UniqueTimestamp;
use restate_types::vqueue::VQueueId;

use crate::scheduler::Action;
use crate::vqueue_config::VQueueConfig;

use super::VQueueHandle;

const QUANTUM: i32 = 1;

#[derive(derive_more::Debug)]
enum Reader<S: VQueueStore> {
    /// Reader was never opened and might need to scan running items
    New { already_running: u32 },
    #[debug("Running")]
    Running {
        remaining: u32,
        reader: S::RunningReader,
    },
    #[debug("Inbox")]
    Inbox(S::InboxReader),
    // We can transition back to Reader::Inbox if new items have been added to the inbox
    // but we should never return to `Running`.
    #[debug("Closed")]
    Closed,
}

pub enum Pop<Item> {
    DeficitExhausted,
    Item {
        action: Action,
        permit: Permit<()>,
        item: Item,
    },
    #[allow(dead_code)]
    Throttle {
        delay_ms: u64,
    },
    BlockedOnCapacity,
}

#[derive(Debug)]
enum Head<Item> {
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
    pub handle: VQueueHandle,
    pub qid: VQueueId,
    #[debug(skip)]
    reader: Reader<S>,
    // contains hashes (unique_hash)
    deficit: i32,
    unconfirmed_assignments: HashSet<u64>,
    head: Head<S::Item>,
}

impl<S> VQueueState<S>
where
    S: VQueueStore,
    S::Item: std::fmt::Debug,
{
    pub fn new(qid: VQueueId, handle: VQueueHandle, already_running: u32) -> Self {
        Self {
            qid,
            handle,
            reader: Reader::New { already_running },
            unconfirmed_assignments: HashSet::new(),
            head: Head::Unknown,
            deficit: QUANTUM,
        }
    }

    pub fn new_empty(qid: VQueueId, handle: VQueueHandle) -> Self {
        Self {
            qid,
            handle,
            reader: Reader::Closed,
            unconfirmed_assignments: HashSet::new(),
            head: Head::Empty,
            deficit: 0,
        }
    }

    pub fn is_active(&self, meta: &VQueueMeta) -> bool {
        match self.reader {
            Reader::New { already_running } if already_running > 0 => true,
            Reader::Running { remaining, .. } if remaining > 0 => true,
            _ => {
                !self.unconfirmed_assignments.is_empty()
                    || (self.num_waiting(meta) > 0 && !meta.is_paused())
            }
        }
    }

    pub fn poll_head(&mut self, storage: &S, meta: &VQueueMeta) -> Result<(), StorageError> {
        // Keep advancing until the head is known
        while matches!(self.head, Head::Unknown) && self.is_active(meta) {
            self.advance(storage)?;
        }

        Ok(())
    }

    pub fn pop_unchecked(
        &mut self,
        cx: &mut std::task::Context<'_>,
        storage: &S,
        concurrency_limiter: &mut Concurrency<()>,
    ) -> Result<Pop<S::Item>, StorageError> {
        let Head::Known(inbox_head) = &self.head else {
            panic!("pop_unchecked was called on empty/unknown head");
        };

        let item_weight = inbox_head.weight().get() as i32;
        if self.deficit < item_weight {
            // give credit.
            self.deficit += QUANTUM;
            return Ok(Pop::DeficitExhausted);
        } else {
            self.deficit -= item_weight;
        }

        if matches!(self.reader, Reader::Running { .. }) {
            // switch between these to change the behavior as needed
            // Action::ResumeAlreadyRunning;
            // Note that resumption requires acquiring concurrency permits similar to
            // MoveToRunning. This is currently not implemented since (at the moment) we
            // only support yielding.
            let result = Pop::Item {
                action: Action::Yield,
                permit: Permit::new_empty(),
                item: inbox_head.clone(),
            };
            self.advance(storage)?;
            return Ok(result);
        }

        // todo: Check for throttling
        let Poll::Ready(permit) = concurrency_limiter.poll_acquire(cx) else {
            // Waker will be notified when capacity is available.
            return Ok(Pop::BlockedOnCapacity);
        };

        // pop head ends
        self.unconfirmed_assignments
            .insert(inbox_head.unique_hash());
        let result = Pop::Item {
            action: Action::MoveToRunning,
            permit,
            item: inbox_head.clone(),
        };
        self.advance(storage)?;

        Ok(result)
    }

    fn advance(&mut self, storage: &S) -> Result<(), StorageError> {
        loop {
            match self.reader {
                Reader::New { already_running } if already_running > 0 => {
                    let mut reader = storage.new_run_reader(&self.qid);
                    reader.seek_to_first();
                    let item = reader.peek()?;
                    if let Some(item) = item {
                        self.head = Head::Known(item);
                        self.reader = Reader::Running {
                            remaining: already_running,
                            reader,
                        };
                        break;
                    } else {
                        error!(
                            "vqueue {:?} has no running items but its metadata says that it has {already_running} running items",
                            self.qid
                        );
                        debug_assert!(already_running > 0);
                        // move to inbox reading
                        self.head = Head::Unknown;
                        self.reader = Reader::Closed;
                    }
                }
                Reader::New { .. } => {
                    // create new inbox reader
                    self.reader = Reader::Closed;
                }
                Reader::Running {
                    ref mut reader,
                    ref mut remaining,
                } => {
                    reader.advance();
                    *remaining = remaining.saturating_sub(1);
                    let item = reader.peek()?;
                    if let Some(item) = item {
                        debug_assert!(*remaining > 0);
                        self.head = Head::Known(item);
                        break;
                    } else {
                        debug_assert_eq!(0, *remaining);
                        // move to inbox reading
                        self.head = Head::Unknown;
                        self.reader = Reader::Closed;
                    }
                }
                Reader::Inbox(ref mut reader) => {
                    reader.advance();
                    let item = reader.peek()?;
                    if let Some(item) = item {
                        if self.unconfirmed_assignments.contains(&item.unique_hash()) {
                            continue;
                        }
                        self.head = Head::Known(item);
                        break;
                    } else {
                        // we are done reading inbox
                        self.head = Head::Empty;
                        self.reader = Reader::Closed;
                        break;
                    }
                }
                Reader::Closed => {
                    match self.head {
                        Head::Unknown => {
                            let mut reader = storage.new_inbox_reader(&self.qid);
                            reader.seek_to_first();
                            let item = reader.peek()?;
                            self.reader = Reader::Inbox(reader);
                            if let Some(item) = item {
                                if self.unconfirmed_assignments.contains(&item.unique_hash()) {
                                    continue;
                                }
                                self.head = Head::Known(item);
                                break;
                            } else {
                                self.head = Head::Empty;
                                self.reader = Reader::Closed;
                            }
                        }
                        Head::Known(ref item) => {
                            // seek to known head first, then advance.
                            let mut reader = storage.new_inbox_reader(&self.qid);
                            reader.seek_after(&self.qid, item);
                            let item = reader.peek()?;
                            self.reader = Reader::Inbox(reader);
                            if let Some(item) = item {
                                if self.unconfirmed_assignments.contains(&item.unique_hash()) {
                                    continue;
                                }
                                self.head = Head::Known(item);
                                break;
                            } else {
                                self.head = Head::Empty;
                                self.reader = Reader::Closed;
                            }
                        }
                        Head::Empty => {
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
        (matches!(self.head, Head::Empty) || meta.total_waiting() == 0)
            && self.unconfirmed_assignments.is_empty()
    }

    pub fn check_eligibility(
        &self,
        now: UniqueTimestamp,
        meta: &VQueueMeta,
        config: &VQueueConfig,
    ) -> Eligibility {
        if !self.is_active(meta) {
            return Eligibility::NotEligible;
        }

        let inbox_head = match self.head {
            Head::Unknown if self.has_available_tokens(meta, config) => {
                return Eligibility::Eligible;
            }
            Head::Unknown => {
                return Eligibility::NotEligible;
            }
            Head::Known(_) if matches!(self.reader, Reader::Running { .. }) => {
                // Running entries are always eligible to run
                return Eligibility::Eligible;
            }
            Head::Known(ref item) => item,
            Head::Empty => {
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
        if let Head::Known(ref item) = self.head
            && item.unique_hash() == item_hash
        {
            self.head = Head::Unknown;
            // Ensure that next advance would re-seek to the newly added item
            self.reader = Reader::Closed;
        }
    }

    pub fn remove_from_unconfirmed_assignments(&mut self, item_hash: u64) -> bool {
        self.unconfirmed_assignments.remove(&item_hash)
    }

    /// Returns true if the head was changed
    pub fn notify_enqueued(&mut self, item: &S::Item) -> bool {
        match (&self.head, &self.reader) {
            // we are only unknown if we are new and didn't read the running list yet,
            // we might also be in a limbo state if advance() failed.
            (_, Reader::New { .. } | Reader::Running { .. }) => { /* do nothing */ }
            (Head::Unknown, _) => { /* do nothing */ }
            (Head::Empty, _) => {
                self.reader = Reader::Closed;
                self.head = Head::Known(item.clone());
                return true;
            }
            (Head::Known(current), Reader::Inbox(_) | Reader::Closed) => {
                if item < current {
                    self.head = Head::Known(item.clone());
                    // Ensure that next advance would re-seek to the newly added item
                    self.reader = Reader::Closed;
                    return true;
                }
            }
        }
        false
    }
}
