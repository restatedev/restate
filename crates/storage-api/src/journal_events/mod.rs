// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::Stream;
use std::ops::RangeInclusive;

use crate::Result;
use restate_types::identifiers::{EntryIndex, InvocationId, PartitionKey};
use restate_types::journal_events::raw::RawEvent;
use restate_types::time::MillisSinceEpoch;

pub trait ReadJournalEventsTable {
    fn get_journal_events(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<impl Stream<Item = Result<EventView>> + Send>;
}

#[derive(Debug, Clone)]
pub enum ScanJournalEventsTableRange {
    PartitionKey(RangeInclusive<PartitionKey>),
    InvocationId(RangeInclusive<InvocationId>),
}

pub trait ScanJournalEventsTable {
    fn for_each_journal_event<
        F: FnMut((InvocationId, EventView)) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: ScanJournalEventsTableRange,
        f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send>;
}

pub trait WriteJournalEventsTable {
    fn put_journal_event(
        &mut self,
        invocation_id: InvocationId,
        event: EventView,
        lsn: u64,
    ) -> Result<()>;

    fn delete_journal_events(&mut self, invocation_id: InvocationId) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventView {
    pub event: RawEvent,
    /// This field is added by the partition processor
    /// to establish the total order between journal entries and events.
    pub after_journal_entry_index: EntryIndex,
    pub append_time: MillisSinceEpoch,
}

impl EventView {
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
