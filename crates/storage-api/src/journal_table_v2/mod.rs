// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::Result;
use futures_util::Stream;
use restate_types::identifiers::{EntryIndex, InvocationId, JournalEntryId, PartitionKey};
use restate_types::journal_v2::raw::{RawCommand, RawEntry};
use restate_types::journal_v2::{CompletionId, NotificationId};
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;

pub trait ReadOnlyJournalTable {
    fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
    ) -> impl Future<Output = Result<Option<RawEntry>>> + Send;

    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, RawEntry)>> + Send>;

    fn all_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send>;

    fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> impl Future<Output = Result<HashMap<NotificationId, EntryIndex>>> + Send;

    fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> impl Future<Output = Result<Option<RawCommand>>> + Send;
}

pub trait JournalTable: ReadOnlyJournalTable {
    /// Related completion ids to this RawEntry, used to build the internal index
    fn put_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
        entry: &RawEntry,
        related_completion_ids: &[CompletionId],
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_journal(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    /// This operation compacts the journal, while retaining the given `notifications_to_retain`.
    /// It also cleanups the internal notification indexes by looking at the ids in `notification_ids_to_cleanup`.
    ///
    /// ## Arguments
    ///
    /// * `compaction_starting_point`: Starting point of the compaction, included. Everything before will be left untouched.
    /// * `notifications_to_retain`: Notifications to retain. Invariant: Each entry index in this slice MUST be >= compaction_starting_point
    /// * `notification_ids_to_forget`: Hint of notification ids to forget, including both notifications ids of notifications existing in the journal,
    ///     and completion ids of completions not in the journal, for which we're cleaning the related command.
    ///     This is needed to cleanup the internal notification indexes.
    /// * `journal_length`: Total journal length when this operation is fired up.
    ///
    /// ## Example
    ///
    /// 1. Starting journal: `[0, 1, 2, 3, 4, 5]`
    /// 2. Compact journal with `compaction_starting_point = 2` and `entries_to_retain = [3, 4]`
    /// 3. Final journal: `[0, 1, 3, 4]`
    fn compact_journal(
        &mut self,
        invocation_id: InvocationId,
        compaction_starting_point: EntryIndex,
        notifications_to_retain: &[EntryIndex],
        notification_ids_to_forget: &[NotificationId],
        journal_length: EntryIndex,
    ) -> impl Future<Output = Result<()>> + Send;
}
