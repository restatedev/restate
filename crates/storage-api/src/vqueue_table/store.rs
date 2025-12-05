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

use restate_types::vqueue::{EffectivePriority, VQueueId};

use super::{EntryCard, EntryKind, VisibleAt};
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
    // Weight is as a proxy for _costing_ dequeueing an item. Lower weight means
    // lower cost and higher chances to be dequeued.
    fn weight(&self) -> NonZeroU16;
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

impl VQueueEntry for EntryCard {
    #[inline(always)]
    fn unique_hash(&self) -> u64 {
        self.unique_hash()
    }

    fn priority(&self) -> EffectivePriority {
        self.priority
    }

    fn visible_at(&self) -> VisibleAt {
        self.visible_at
    }

    fn kind(&self) -> EntryKind {
        self.kind
    }

    fn weight(&self) -> NonZeroU16 {
        let weight: u16 = match self.kind {
            EntryKind::Unknown | EntryKind::StateMutation => 1,
            // The reasoning here is to give queues that need to resume invocations more
            // priority than queues that need to start new ones.
            // Those weights are provisional and can be changed any time, do not make any
            // hard assumptions about them.
            EntryKind::Invocation => match self.priority {
                EffectivePriority::TokenHeld
                | EffectivePriority::Started
                | EffectivePriority::System => 1,
                EffectivePriority::UserHigh => 2,
                EffectivePriority::UserDefault => 2,
            },
        };

        // Safety: All values are positive numbers as shown in the match above.
        unsafe { NonZeroU16::new_unchecked(weight) }
    }

    #[inline(always)]
    fn is_token_held(&self) -> bool {
        self.priority.token_held()
    }
}
