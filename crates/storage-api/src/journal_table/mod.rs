// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{GetFuture, GetStream, PutFuture};
use restate_types::identifiers::{EntryIndex, ServiceId};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::CompletionResult;

/// Different types of journal entries persisted by the runtime
#[derive(Debug, Clone)]
pub enum JournalEntry {
    Entry(EnrichedRawEntry),
    Completion(CompletionResult),
}

pub trait JournalTable {
    fn put_journal_entry(
        &mut self,
        service_id: &ServiceId,
        journal_index: u32,
        journal_entry: JournalEntry,
    ) -> PutFuture;

    fn get_journal_entry(
        &mut self,
        service_id: &ServiceId,
        journal_index: u32,
    ) -> GetFuture<Option<JournalEntry>>;

    fn get_journal(
        &mut self,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> GetStream<JournalEntry>;

    fn delete_journal(&mut self, service_id: &ServiceId, journal_length: EntryIndex) -> PutFuture;
}
