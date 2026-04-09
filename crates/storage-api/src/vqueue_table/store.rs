// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::vqueue::VQueueId;

use super::{EntryKey, EntryValue};
use crate::Result;

/// Storage for vqueue heads (e.g., RocksDB `ready_idx`).
pub trait VQueueStore {
    type RunningReader: VQueueCursor + Send + Unpin;
    type InboxReader: VQueueCursor + Send + Unpin;

    fn new_run_reader(&self, qid: &VQueueId) -> Self::RunningReader;
    fn new_inbox_reader(&self, qid: &VQueueId) -> Self::InboxReader;
}

/// Iterator over vqueue entries
pub trait VQueueCursor {
    /// Moves the cursor to the beginning (min key) of the vqueue
    fn seek_to_first(&mut self);
    /// Moves the cursor to point strictly after `item`
    fn seek_after(&mut self, qid: &VQueueId, item: &EntryKey);
    /// Returns the current key under cursor
    fn current_key(&mut self) -> Result<Option<EntryKey>>;
    /// Returns the current value under cursor
    fn current_value(&mut self) -> Result<Option<EntryValue>>;
    /// Peek item without advancing the cursor
    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>>;
    /// Advancing the cursor. If this fails, the error is returned on the next call to peek()
    fn advance(&mut self);
}
