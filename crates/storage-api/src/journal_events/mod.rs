// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use crate::Result;
use crate::protobuf_types::PartitionStoreProtobufValue;
use restate_types::identifiers::{EntryIndex, InvocationId, JournalEventId, PartitionKey};
use restate_types::journal_events::raw::RawEvent;
use restate_types::time::MillisSinceEpoch;

pub trait ReadOnlyJournalEventsTable {
    fn get_journal_event(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
    ) -> impl Future<Output = Result<Option<StoredEvent>>> + Send;

    fn get_journal_events(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredEvent)>> + Send>;
}

pub trait ScanJournalEventsTable {
    fn for_each_journal_event<
        F: FnMut((JournalEventId, StoredEvent)) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait JournalEventsTable: ReadOnlyJournalEventsTable {
    fn put_journal_event(
        &mut self,
        invocation_id: InvocationId,
        event_index: u32,
        entry: &StoredEvent,
    ) -> impl Future<Output = Result<()>> + Send;

    fn delete_journal_events(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub append_time: MillisSinceEpoch,
    /// This field is added by the partition processor
    /// to establish the total order between journal entries and events.
    pub after_journal_entry_index: EntryIndex,
    pub event: RawEvent,
}

impl StoredEvent {
    pub fn new(
        append_time: MillisSinceEpoch,
        after_journal_entry_index: EntryIndex,
        event: impl Into<RawEvent>,
    ) -> Self {
        Self {
            append_time,
            after_journal_entry_index,
            event: event.into(),
        }
    }
}

impl PartitionStoreProtobufValue for StoredEvent {
    type ProtobufType = crate::protobuf_types::v1::Event;
}
