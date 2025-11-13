// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::vqueue::{EffectivePriority, VQueueId};

use super::{EntryCard, EntryKind, InboxEntry, VisibleAt};
use crate::Result;

/// Storage for vqueue heads (e.g., RocksDB `ready_idx`).
pub trait VQueueStore {
    type Item: VQueueEntry;
    type RunningReader: VQueueCursor<Item = Self::Item> + Send + Unpin;
    type InboxReader: VQueueCursor<Item = Self::Item> + Send + Unpin;

    fn new_run_reader(&self, qid: &VQueueId) -> Self::RunningReader;
    fn new_inbox_reader(&self, qid: &VQueueId) -> Self::InboxReader;
}

pub trait VQueueEntry: PartialOrd + PartialEq + Eq + Clone {
    fn unique_hash(&self) -> u64;
    fn priority(&self) -> EffectivePriority;
    fn visible_at(&self) -> VisibleAt;
    fn kind(&self) -> EntryKind;
    fn is_token_held(&self) -> bool;
}

/// Iterator over vqueue entries
pub trait VQueueCursor {
    type Item: PartialOrd + PartialEq + Eq + Clone;
    /// Moves the cursor to the beginning (min key) of the vqueue
    fn seek_to_first(&mut self);
    /// Moves the cursor to point strictly after `item`
    fn seek_after(&mut self, qid: &VQueueId, item: &Self::Item);
    /// Peek item without advancing the cursor
    fn peek(&mut self) -> Result<Option<Self::Item>>;
    /// Advancing the cursor. If this fails, the error is returned on the next call to peek()
    fn advance(&mut self);
}

/// Carries the key-card and entry metadata in a single struct
#[derive(Debug, Clone)]
pub struct InboxItem {
    pub card: EntryCard,
    pub entry: InboxEntry,
}

impl PartialEq for InboxItem {
    fn eq(&self, other: &Self) -> bool {
        self.card == other.card
    }
}

impl Eq for InboxItem {}

impl PartialOrd for InboxItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.card.partial_cmp(&other.card)
    }
}

impl VQueueEntry for InboxItem {
    #[inline(always)]
    fn unique_hash(&self) -> u64 {
        self.card.unique_hash()
    }

    fn priority(&self) -> EffectivePriority {
        self.card.priority
    }

    fn visible_at(&self) -> VisibleAt {
        self.card.visible_at
    }

    fn kind(&self) -> EntryKind {
        self.card.kind
    }

    #[inline(always)]
    fn is_token_held(&self) -> bool {
        self.entry.is_token_held()
    }
}
