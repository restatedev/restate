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
use restate_types::identifiers::{EntryIndex, InvocationId, PartitionKey, WithInvocationId};
use restate_types::invocation::InvocationEpoch;
use restate_types::journal_v2::raw::{RawCommand, RawEntry};
use restate_types::journal_v2::{CompletionId, NotificationId};
use std::collections::HashMap;
use std::future::Future;
use std::ops::RangeInclusive;

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
pub struct JournalEntryId {
    invocation_id: InvocationId,
    invocation_epoch: InvocationEpoch,
    journal_index: EntryIndex,
}

impl JournalEntryId {
    pub const fn from_parts(
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        journal_index: EntryIndex,
    ) -> Self {
        Self {
            invocation_id,
            invocation_epoch,
            journal_index,
        }
    }

    pub fn invocation_epoch(&self) -> InvocationEpoch {
        self.invocation_epoch
    }

    pub fn journal_index(&self) -> EntryIndex {
        self.journal_index
    }
}

impl From<(InvocationId, InvocationEpoch, EntryIndex)> for JournalEntryId {
    fn from(value: (InvocationId, InvocationEpoch, EntryIndex)) -> Self {
        Self::from_parts(value.0, value.1, value.2)
    }
}

impl WithInvocationId for JournalEntryId {
    fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }
}

pub trait ReadOnlyJournalTable {
    /// Get an entry from the latest/current journal.
    fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) -> impl Future<Output = Result<Option<RawEntry>>> + Send;

    fn get_journal_entry_for_epoch(
        &mut self,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        entry_index: EntryIndex,
    ) -> impl Future<Output = Result<Option<RawEntry>>> + Send;

    /// Get the latest/current journal.
    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, RawEntry)>> + Send>;

    /// Tell me if there's at least one entry here!
    fn has_journal(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = Result<bool>> + Send;

    /// Returns all the journals, including the archived ones.
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

/// ## Latest and archived journals
///
/// Current journals and archived journals are stored in separate key ranges.
/// Archived journals are immutable and cannot outlive the "latest" journal.
pub trait JournalTable: ReadOnlyJournalTable {
    /// Related completion ids to this RawEntry, used to build the internal index.
    fn put_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        current_invocation_epoch: InvocationEpoch,
        index: u32,
        entry: &RawEntry,
        related_completion_ids: &[CompletionId],
    ) -> impl Future<Output = Result<()>> + Send;

    /// Archive the current journal to the given invocation epoch.
    ///
    /// This won't affect the latest/current journal.
    fn archive_journal_to_epoch(
        &mut self,
        invocation_id: &InvocationId,
        invocation_epoch: InvocationEpoch,
        length: EntryIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Update epoch numbers for the given journal range.
    ///
    /// This will affect only the latest/current journal.
    fn update_current_journal_epoch(
        &mut self,
        invocation_id: &InvocationId,
        new_epoch: InvocationEpoch,
        length: EntryIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Delete the journal. If no epoch is provided, remove the latest/current.
    fn delete_journal(
        &mut self,
        invocation_id: InvocationId,
        invocation_epoch: Option<InvocationEpoch>,
        length: EntryIndex,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Delete the given journal range in the latest/current journal.
    fn delete_journal_range(
        &mut self,
        invocation_id: InvocationId,
        from_included: EntryIndex,
        to_excluded: EntryIndex,
        notification_ids_to_cleanup: &[NotificationId],
    ) -> impl Future<Output = Result<()>> + Send;
}
