// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{protobuf_storage_encode_decode, Result};
use futures_util::Stream;
use restate_types::identifiers::{EntryIndex, InvocationId, JournalEntryId, PartitionKey};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{CompletionResult, EntryType};
use std::future::Future;
use std::ops::RangeInclusive;

/// Different types of journal entries persisted by the runtime
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, PartialEq, Eq)]
pub enum JournalEntryType {
    Entry(EntryType),
    Completion,
}

protobuf_storage_encode_decode!(JournalEntry);

pub trait ReadOnlyJournalTable {
    fn get_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
    ) -> impl Future<Output = Result<Option<JournalEntry>>> + Send;

    fn get_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send;

    fn all_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(JournalEntryId, JournalEntry)>> + Send;
}

pub trait JournalTable: ReadOnlyJournalTable {
    fn put_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
        journal_entry: &JournalEntry,
    ) -> impl Future<Output = ()> + Send;

    fn delete_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> impl Future<Output = ()> + Send;
}
