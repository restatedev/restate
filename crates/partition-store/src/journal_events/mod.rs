// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::TableKind::{JournalEvent, State};
use crate::error::break_on_err;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::{
    PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan, TableScanIterationDecision,
};
use futures::Stream;
use futures_util::stream;
use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::journal_events::{
    EventView, ReadJournalEventsTable, ScanJournalEventsTable, WriteJournalEventsTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::protobuf_types::v1::Event as PbEvent;
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::journal_events::EventType;
use restate_types::journal_events::raw::RawEvent;
use restate_types::time::MillisSinceEpoch;
use std::ops::RangeInclusive;

define_table_key!(
    JournalEvent,
    KeyKind::JournalEvent,
    JournalEventKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        event_type: u8,
        timestamp: u64,
        lsn: u64
    )
);

/// Just a wrapper for the PartitionStoreProtobufValue trait
#[derive(Clone)]
struct EventWrapper(PbEvent);

impl From<PbEvent> for EventWrapper {
    fn from(t: PbEvent) -> Self {
        Self(t)
    }
}
impl From<EventWrapper> for PbEvent {
    fn from(value: EventWrapper) -> Self {
        value.0
    }
}
impl PartitionStoreProtobufValue for EventWrapper {
    type ProtobufType = PbEvent;
}

fn write_journal_event_key(
    invocation_id: &InvocationId,
    event_type: u8,
    timestamp: u64,
    lsn: u64,
) -> JournalEventKey {
    JournalEventKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .event_type(event_type)
        .timestamp(timestamp)
        .lsn(lsn)
}

fn put_journal_event<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    event: EventView,
    lsn: u64,
) -> Result<()> {
    let (event_ty, event_value) = event.event.into_inner();
    storage.put_kv_proto(
        write_journal_event_key(
            invocation_id,
            event_ty as u8,
            event.append_time.as_u64(),
            lsn,
        ),
        &EventWrapper(PbEvent {
            content: event_value,
            after_journal_entry_index: event.after_journal_entry_index,
        }),
    )
}

fn get_journal_events<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<Vec<Result<EventView>>> {
    let _x = RocksDbPerfGuard::new("get-journal-events");
    let key = JournalEventKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(invocation_id.partition_key(), key),
        move |mut k, mut v| {
            TableScanIterationDecision::Emit(
                JournalEventKey::deserialize_from(&mut k)
                    .and_then(|k| k.into_inner_ok_or())
                    .and_then(|k_elements| {
                        EventWrapper::decode(&mut v).map(|value| (k_elements, value.0))
                    })
                    .map(
                        |(
                            (_, _, event_type, timestamp, _),
                            PbEvent {
                                content,
                                after_journal_entry_index,
                            },
                        )| EventView {
                            event: RawEvent::new(
                                EventType::from_repr(event_type).unwrap_or(EventType::Unknown),
                                content,
                            ),
                            after_journal_entry_index,
                            append_time: MillisSinceEpoch::new(timestamp),
                        },
                    ),
            )
        },
    )
}

fn delete_journal_events<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<()> {
    let _x = RocksDbPerfGuard::new("delete-journal-events");

    let prefix_key = JournalEventKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let keys = storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(invocation_id.partition_key(), prefix_key),
        |k, _| TableScanIterationDecision::Emit(Ok(Box::from(k))),
    )?;

    for k in keys {
        let key = k?;
        storage.delete_cf(State, &key)?;
    }

    Ok(())
}

impl ReadJournalEventsTable for PartitionStore {
    fn get_journal_events(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<impl Stream<Item = Result<EventView>> + Send> {
        Ok(stream::iter(get_journal_events(self, &invocation_id)?))
    }
}

impl ScanJournalEventsTable for PartitionStore {
    fn for_each_journal_event<
        F: FnMut((InvocationId, EventView)) -> std::ops::ControlFlow<()> + Send + Sync + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        self.iterator_for_each(
            "df-journal-events",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<JournalEventKey>(range),
            move |(mut key, mut value)| {
                let event_key = break_on_err(JournalEventKey::deserialize_from(&mut key))?;
                let (pk, inv_uuid, event_type, timestamp, _) =
                    break_on_err(event_key.into_inner_ok_or())?;
                let stored_event = break_on_err(EventWrapper::decode(&mut value))?.0;

                f((
                    InvocationId::from_parts(pk, inv_uuid),
                    EventView {
                        event: RawEvent::new(
                            EventType::from_repr(event_type).unwrap_or(EventType::Unknown),
                            stored_event.content,
                        ),
                        after_journal_entry_index: stored_event.after_journal_entry_index,
                        append_time: MillisSinceEpoch::new(timestamp),
                    },
                ))
                .map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadJournalEventsTable for PartitionStoreTransaction<'_> {
    fn get_journal_events(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<impl Stream<Item = Result<EventView>> + Send> {
        Ok(stream::iter(get_journal_events(self, &invocation_id)?))
    }
}

impl WriteJournalEventsTable for PartitionStoreTransaction<'_> {
    fn put_journal_event(
        &mut self,
        invocation_id: InvocationId,
        event: EventView,
        lsn: u64,
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        put_journal_event(self, &invocation_id, event, lsn)
    }

    fn delete_journal_events(&mut self, invocation_id: InvocationId) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        delete_journal_events(self, &invocation_id)
    }
}
