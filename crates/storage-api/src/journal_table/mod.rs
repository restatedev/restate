// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use futures::Stream;

use restate_memory::{LocalMemoryLease, LocalMemoryPool};
use restate_types::identifiers::{EntryIndex, InvocationId, JournalEntryId, PartitionKey};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{CompletionResult, EntryType};

use crate::protobuf_types::PartitionStoreProtobufValue;
use crate::{BudgetedReadError, Result};

/// Different types of journal entries persisted by the runtime
#[derive(Debug, Clone, PartialEq, Eq)]
// todo: fix this and box the large variant (EnrichedRawEntry is 304 bytes)
#[allow(clippy::large_enum_variant)]
pub enum JournalEntry {
    Entry(EnrichedRawEntry),
    Completion(CompletionResult),
}

impl JournalEntry {
    pub fn entry_type(&self) -> JournalEntryType {
        match self {
            JournalEntry::Entry(entry) => JournalEntryType::Entry(entry.header().as_entry_type()),
            JournalEntry::Completion(_) => JournalEntryType::Completion,
        }
    }

    pub fn is_resumable(&self) -> bool {
        match self {
            JournalEntry::Entry(entry) => entry.header().is_completed().unwrap_or(true),
            JournalEntry::Completion(_) => false,
        }
    }
}

impl PartitionStoreProtobufValue for JournalEntry {
    type ProtobufType = crate::protobuf_types::v1::JournalEntry;
}

#[derive(Debug, PartialEq, Eq)]
pub enum JournalEntryType {
    Entry(EntryType),
    Completion,
}

pub trait ReadJournalTable {
    fn get_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
    ) -> impl Future<Output = Result<Option<JournalEntry>>> + Send;

    /// Returns a lazy stream over journal entries.
    /// The stream borrows from `self` only, not from `invocation_id`.
    fn get_journal<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send + 'a>;

    /// Budget-gated point read of a single journal entry.
    ///
    /// Fetches raw bytes from the store, peeks the serialized size, acquires a
    /// [`LocalMemoryLease`] from `budget`, and only then decodes the entry.
    fn get_journal_entry_budgeted(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
        budget: &mut LocalMemoryPool,
    ) -> impl Future<
        Output = std::result::Result<Option<(JournalEntry, LocalMemoryLease)>, BudgetedReadError>,
    > + Send;

    /// Budget-gated journal stream.
    ///
    /// Each entry's raw byte size is peeked from the underlying store **before**
    /// deserialization. A [`LocalMemoryLease`] is acquired from `budget` for that
    /// size, and only then is the entry decoded. This prevents memory allocation
    /// when the budget is exhausted.
    fn get_journal_budgeted<'a>(
        &'a self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl Stream<
            Item = std::result::Result<
                (EntryIndex, JournalEntry, LocalMemoryLease),
                BudgetedReadError,
            >,
        > + Send
        + 'a,
    >;
}

pub trait ScanJournalTable {
    fn for_each_journal<
        F: FnMut((JournalEntryId, JournalEntry)) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait WriteJournalTable {
    fn put_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
        journal_entry: &JournalEntry,
    ) -> Result<()>;

    fn delete_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<()>;
}
