// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Cursor;
use std::ops::RangeInclusive;

use crate::TableKind::JournalEvent;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan, TableScanIterationDecision,
};
use futures::Stream;
use futures_util::stream;
use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::journal_events::{
    JournalEventsTable, ReadOnlyJournalEventsTable, ScanJournalEventsTable, StoredEvent,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{
    EntryIndex, InvocationId, InvocationUuid, JournalEventId, PartitionKey, WithPartitionKey,
};

define_table_key!(
    JournalEvent,
    KeyKind::JournalEvent,
    JournalEventKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        event_index: u32
    )
);

fn write_journal_event_key(invocation_id: &InvocationId, event_index: u32) -> JournalEventKey {
    JournalEventKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .event_index(event_index)
}

fn put_journal_event<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    event_index: u32,
    event: &StoredEvent,
) -> Result<()> {
    storage.put_kv(write_journal_event_key(invocation_id, event_index), event)
}

fn get_journal_event<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    event_index: u32,
) -> Result<Option<StoredEvent>> {
    let _x = RocksDbPerfGuard::new("get-journal-event");
    let key = write_journal_event_key(invocation_id, event_index);
    storage.get_value(key)
}

fn get_journal_events<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    events_length: EntryIndex,
) -> Result<Vec<Result<(EntryIndex, StoredEvent)>>> {
    let _x = RocksDbPerfGuard::new("get-journal-events");
    let key = JournalEventKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let mut n = 0;
    storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(invocation_id.partition_key(), key),
        move |k, mut v| {
            let key = JournalEventKey::deserialize_from(&mut Cursor::new(k)).map(|journal_key| {
                journal_key
                    .event_index
                    .expect("The event index must be part of the journal key.")
            });
            let event =
                StoredEvent::decode(&mut v).map_err(|error| StorageError::Generic(error.into()));

            let result = key.and_then(|key| event.map(|entry| (key, entry)));

            n += 1;
            if n < events_length {
                TableScanIterationDecision::Emit(result)
            } else {
                TableScanIterationDecision::BreakWith(result)
            }
        },
    )
}

fn delete_journal_events<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    events_length: EntryIndex,
) -> Result<()> {
    let _x = RocksDbPerfGuard::new("delete-journal-events");

    let mut key = write_journal_event_key(invocation_id, 0);
    let k = &mut key;
    for event_index in 0..events_length {
        k.event_index = Some(event_index);
        storage.delete_key(k)?;
    }

    Ok(())
}

impl ReadOnlyJournalEventsTable for PartitionStore {
    async fn get_journal_event(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
    ) -> Result<Option<StoredEvent>> {
        self.assert_partition_key(&invocation_id)?;
        get_journal_event(self, &invocation_id, index)
    }

    fn get_journal_events(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredEvent)>> + Send> {
        Ok(stream::iter(get_journal_events(
            self,
            &invocation_id,
            length,
        )?))
    }
}

impl ScanJournalEventsTable for PartitionStore {
    fn scan_journal_events(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> Result<impl Stream<Item = Result<(JournalEventId, StoredEvent)>> + Send> {
        self.run_iterator(
            "df-journal-events",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<JournalEventKey>(range),
            |(mut key, mut value)| {
                let journal_event_key = JournalEventKey::deserialize_from(&mut key)?;
                let journal_event = StoredEvent::decode(&mut value)
                    .map_err(|err| StorageError::Conversion(err.into()))?;

                let (partition_key, invocation_uuid, event_index) =
                    journal_event_key.into_inner_ok_or()?;

                Ok((
                    JournalEventId::from_parts(
                        InvocationId::from_parts(partition_key, invocation_uuid),
                        event_index,
                    ),
                    journal_event,
                ))
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadOnlyJournalEventsTable for PartitionStoreTransaction<'_> {
    async fn get_journal_event(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
    ) -> Result<Option<StoredEvent>> {
        self.assert_partition_key(&invocation_id)?;
        get_journal_event(self, &invocation_id, index)
    }

    fn get_journal_events(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredEvent)>> + Send> {
        Ok(stream::iter(get_journal_events(
            self,
            &invocation_id,
            length,
        )?))
    }
}

impl JournalEventsTable for PartitionStoreTransaction<'_> {
    async fn put_journal_event(
        &mut self,
        invocation_id: InvocationId,
        event_index: u32,
        entry: &StoredEvent,
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        put_journal_event(self, &invocation_id, event_index, entry)
    }

    async fn delete_journal_events(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        delete_journal_events(self, &invocation_id, length)
    }
}
