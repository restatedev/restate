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

use restate_types::identifiers::{EntryIndex, InvocationId, JournalEntryId, PartitionKey};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{CompletionResult, EntryType};

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;

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

    fn get_journal(
        &self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send>;
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
