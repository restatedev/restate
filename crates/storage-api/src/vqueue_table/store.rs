// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::vqueues::VQueueId;

use crate::StorageError;

use super::{EntryKey, EntryValue};

pub struct Options {
    /// Allows blocking IO when we need to read from the storage
    /// operations that block should be performed on IO threads.
    ///
    /// When set to false, seek and read operations may return `WouldBlock` if
    /// the operation cannot be performed without blocking.
    pub allow_blocking_io: bool,
}

/// Storage for vqueue heads (e.g., RocksDB `ready_idx`).
pub trait VQueueStore {
    type RunningReader: VQueueRunningCursor + Send + Unpin;
    type InboxReader: VQueueCursor + Send + Unpin + 'static;

    fn new_run_reader(&self, qid: &VQueueId) -> Self::RunningReader;
    fn new_inbox_reader(&self, qid: &VQueueId, opts: Options) -> Self::InboxReader;
}

/// Iterator over "waiting inbox" vqueue entries
pub trait VQueueCursor {
    /// Moves the cursor to the beginning (min key) of the vqueue
    fn seek_to_first(&mut self);
    /// Moves the cursor to point strictly after `item`
    fn seek_after(&mut self, item: &EntryKey);
    /// Advancing the cursor.
    fn advance(&mut self);
    /// Returns the current key under cursor
    fn current_key(&mut self) -> Result<Option<EntryKey>, CursorError>;
    /// Returns the current value under cursor
    fn current_value(&mut self) -> Result<Option<EntryValue>, CursorError>;
    /// Peek item without advancing the cursor
    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>, CursorError>;
}

/// Iterator over already running vqueue entries
pub trait VQueueRunningCursor {
    /// Moves the cursor to the beginning (min key) of the vqueue
    fn seek_to_first(&mut self);
    /// Peek item without advancing the cursor
    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>, StorageError>;
    /// Advancing the cursor. If this fails, the error is returned on the next call to peek()
    fn advance(&mut self);
}

#[derive(Debug, thiserror::Error)]
pub enum CursorError {
    #[error("operation cannot be completed without block this thread")]
    WouldBlock,
    #[error(transparent)]
    Other(#[from] StorageError),
}
