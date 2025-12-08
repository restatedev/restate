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
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{VQueueCursor, VQueueEntry, VQueueStore};
use restate_types::vqueue::VQueueId;
use tracing::error;

#[derive(Debug)]
enum Head<Item> {
    /// We need a seek+read to know the head.
    Unknown,
    /// The current cursor's head
    Known(Item),
    /// We know that we've reached the end of the vqueue
    Empty,
}

pub enum QueueItem<Item> {
    New(Item),
    Running(Item),
    None,
}

#[derive(derive_more::Debug)]
pub(crate) enum Reader<S: VQueueStore> {
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

pub(crate) struct Queue<S: VQueueStore> {
    head: Head<S::Item>,
    reader: Reader<S>,
}

impl<S: VQueueStore> Queue<S> {
    /// Creates a new queue that must first go through the given number of running items
    /// before it switches to reading the waiting inbox.
    pub fn new(num_running: u32) -> Self {
        Self {
            head: Head::Unknown,
            reader: Reader::New {
                already_running: num_running,
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn reader(&self) -> &Reader<S> {
        &self.reader
    }

    /// Creates an empty queue
    pub fn new_closed() -> Self {
        Self {
            head: Head::Empty,
            reader: Reader::Closed,
        }
    }

    /// If the queue is known to be empty (no more items to dequeue)
    pub fn is_empty(&self) -> bool {
        matches!(self.head, Head::Empty)
    }

    pub fn remove(&mut self, item_hash: u64) -> bool {
        // Can this be the known head?
        // Yes. Perhaps it expired/ended externally.
        if let Head::Known(ref item) = self.head
            && item.unique_hash() == item_hash
        {
            self.head = Head::Unknown;
            // Ensure that next advance would re-seek to the newly added item
            self.reader = Reader::Closed;
            true
        } else {
            false
        }
    }

    /// Returns true if the head was changed
    pub fn enqueue(&mut self, item: &S::Item) -> bool {
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

    /// Returns the head if known, or None if the queue needs advancing
    pub fn head(&self) -> Option<QueueItem<&S::Item>> {
        match (&self.head, &self.reader) {
            (Head::Unknown, _) => None,
            (_, Reader::New { .. }) => None,
            (Head::Known(item), Reader::Running { .. }) => Some(QueueItem::Running(item)),
            (Head::Known(item), Reader::Inbox(_) | Reader::Closed) => Some(QueueItem::New(item)),
            (Head::Empty, _) => Some(QueueItem::None),
        }
    }

    pub fn advance_if_needed(
        &mut self,
        storage: &S,
        skip: &HashSet<u64>,
        qid: &VQueueId,
    ) -> Result<QueueItem<&S::Item>, StorageError> {
        // Keep advancing until the head is known
        while matches!(self.head, Head::Unknown) {
            self.advance(storage, skip, qid)?;
        }

        match (&self.head, &self.reader) {
            (Head::Unknown, _) => unreachable!("head must be known"),
            (_, Reader::New { .. }) => unreachable!("reader cannot be new after poll"),
            (Head::Known(item), Reader::Running { .. }) => Ok(QueueItem::Running(item)),
            (Head::Known(item), Reader::Inbox(_) | Reader::Closed) => Ok(QueueItem::New(item)),
            (Head::Empty, _) => Ok(QueueItem::None),
        }
    }

    pub fn advance(
        &mut self,
        storage: &S,
        skip: &HashSet<u64>,
        qid: &VQueueId,
    ) -> Result<(), StorageError> {
        loop {
            match self.reader {
                Reader::New { already_running } if already_running > 0 => {
                    let mut reader = storage.new_run_reader(qid);
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
                            qid
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
                        if skip.contains(&item.unique_hash()) {
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
                            let mut reader = storage.new_inbox_reader(qid);
                            reader.seek_to_first();
                            let item = reader.peek()?;
                            self.reader = Reader::Inbox(reader);
                            if let Some(item) = item {
                                if skip.contains(&item.unique_hash()) {
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
                            let mut reader = storage.new_inbox_reader(qid);
                            reader.seek_after(qid, item);
                            let item = reader.peek()?;
                            self.reader = Reader::Inbox(reader);
                            if let Some(item) = item {
                                if skip.contains(&item.unique_hash()) {
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
}
