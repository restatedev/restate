// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::codec::ProtoValue;
use crate::keys::define_table_key;
use crate::keys::TableKey;
use crate::owned_iter::OwnedIterator;
use crate::scan::TableScan::PartitionKeyRange;
use crate::TableKind::Journal;
use crate::{RocksDBStorage, RocksDBTransaction, StorageAccess};
use crate::{TableScan, TableScanIterationDecision};
use bytes::Bytes;
use bytestring::ByteString;
use futures::Stream;
use futures_util::stream;
use prost::Message;
use restate_storage_api::journal_table::{JournalEntry, JournalTable};
use restate_storage_api::{Result, StorageError};
use restate_storage_proto::storage;
use restate_types::identifiers::{EntryIndex, PartitionKey, ServiceId, WithPartitionKey};
use std::io::Cursor;
use std::ops::RangeInclusive;

define_table_key!(
    Journal,
    JournalKey(
        partition_key: PartitionKey,
        service_name: ByteString,
        service_key: Bytes,
        journal_index: u32
    )
);

fn write_journal_entry_key(service_id: &ServiceId, journal_index: u32) -> JournalKey {
    JournalKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
        .journal_index(journal_index)
}

fn put_journal_entry<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    journal_index: u32,
    journal_entry: JournalEntry,
) {
    let key = JournalKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
        .journal_index(journal_index);

    let value = ProtoValue(storage::v1::JournalEntry::from(journal_entry));

    storage.put_kv(key, value);
}

fn get_journal_entry<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    journal_index: u32,
) -> Result<Option<JournalEntry>> {
    let key = JournalKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone())
        .journal_index(journal_index);

    storage.get_blocking(key, move |_k, v| {
        if v.is_none() {
            return Ok(None);
        }
        let proto = storage::v1::JournalEntry::decode(v.unwrap())
            .map_err(|err| StorageError::Generic(err.into()))?;

        JournalEntry::try_from(proto)
            .map_err(StorageError::from)
            .map(Some)
    })
}

fn get_journal<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    journal_length: EntryIndex,
) -> Vec<Result<(EntryIndex, JournalEntry)>> {
    let key = JournalKey::default()
        .partition_key(service_id.partition_key())
        .service_name(service_id.service_name.clone())
        .service_key(service_id.key.clone());

    let mut n = 0;
    storage.for_each_key_value_in_place(TableScan::KeyPrefix(key), move |k, v| {
        let key = JournalKey::deserialize_from(&mut Cursor::new(k)).map(|journal_key| {
            journal_key
                .journal_index
                .expect("The journal index must be part of the journal key.")
        });
        let entry = storage::v1::JournalEntry::decode(v)
            .map_err(|error| StorageError::Generic(error.into()))
            .and_then(|entry| JournalEntry::try_from(entry).map_err(Into::into));

        let result = key.and_then(|key| entry.map(|entry| (key, entry)));

        n += 1;
        if n < journal_length {
            TableScanIterationDecision::Emit(result)
        } else {
            TableScanIterationDecision::BreakWith(result)
        }
    })
}

fn delete_journal<S: StorageAccess>(
    storage: &mut S,
    service_id: &ServiceId,
    journal_length: EntryIndex,
) {
    let mut key = write_journal_entry_key(service_id, 0);
    let k = &mut key;
    for journal_index in 0..journal_length {
        k.journal_index = Some(journal_index);
        storage.delete_key(k);
    }
}

impl JournalTable for RocksDBStorage {
    async fn put_journal_entry(
        &mut self,
        service_id: &ServiceId,
        journal_index: u32,
        journal_entry: JournalEntry,
    ) {
        put_journal_entry(self, service_id, journal_index, journal_entry)
    }

    async fn get_journal_entry(
        &mut self,
        service_id: &ServiceId,
        journal_index: u32,
    ) -> Result<Option<JournalEntry>> {
        get_journal_entry(self, service_id, journal_index)
    }

    fn get_journal(
        &mut self,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send {
        stream::iter(get_journal(self, service_id, journal_length))
    }

    async fn delete_journal(&mut self, service_id: &ServiceId, journal_length: EntryIndex) {
        delete_journal(self, service_id, journal_length)
    }
}

impl<'a> JournalTable for RocksDBTransaction<'a> {
    async fn put_journal_entry(
        &mut self,
        service_id: &ServiceId,
        journal_index: u32,
        journal_entry: JournalEntry,
    ) {
        put_journal_entry(self, service_id, journal_index, journal_entry)
    }

    async fn get_journal_entry(
        &mut self,
        service_id: &ServiceId,
        journal_index: u32,
    ) -> Result<Option<JournalEntry>> {
        get_journal_entry(self, service_id, journal_index)
    }

    fn get_journal(
        &mut self,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, JournalEntry)>> + Send {
        stream::iter(get_journal(self, service_id, journal_length))
    }

    async fn delete_journal(&mut self, service_id: &ServiceId, journal_length: EntryIndex) {
        delete_journal(self, service_id, journal_length)
    }
}

#[derive(Debug)]
pub struct OwnedJournalRow {
    pub partition_key: PartitionKey,
    pub service: ByteString,
    pub service_key: Bytes,
    pub journal_index: u32,
    pub journal_entry: JournalEntry,
}

impl RocksDBStorage {
    pub fn all_journal(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Iterator<Item = OwnedJournalRow> + '_ {
        let iter = self.iterator_from(PartitionKeyRange::<JournalKey>(range));
        OwnedIterator::new(iter).map(|(mut key, value)| {
            let journal_key = JournalKey::deserialize_from(&mut key)
                .expect("journal key must deserialize into JournalKey");
            let journal_entry = storage::v1::JournalEntry::decode(value)
                .expect("journal entry must deserialize into JournalEntry");
            let journal_entry = JournalEntry::try_from(journal_entry)
                .expect("journal entry must convert from proto");
            OwnedJournalRow {
                partition_key: journal_key
                    .partition_key
                    .expect("journal key must have a partition key"),
                service: journal_key
                    .service_name
                    .expect("journal key must have a service name"),
                service_key: journal_key
                    .service_key
                    .expect("journal key must have a service key"),
                journal_index: journal_key
                    .journal_index
                    .expect("journal key must have an index"),
                journal_entry,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::journal_table::write_journal_entry_key;
    use crate::keys::TableKey;
    use bytes::Bytes;
    use restate_types::identifiers::ServiceId;

    fn journal_entry_key(service_id: &ServiceId, journal_index: u32) -> Bytes {
        write_journal_entry_key(service_id, journal_index)
            .serialize()
            .freeze()
    }

    #[test]
    fn journal_keys_sort_lex() {
        //
        // across services
        //
        assert!(
            journal_entry_key(&ServiceId::with_partition_key(1337, "svc-1", ""), 0)
                < journal_entry_key(&ServiceId::with_partition_key(1337, "svc-2", ""), 0)
        );
        //
        // same service across keys
        //
        assert!(
            journal_entry_key(&ServiceId::with_partition_key(1337, "svc-1", "a"), 0)
                < journal_entry_key(&ServiceId::with_partition_key(1337, "svc-1", "b"), 0)
        );
        //
        // within the same service and key
        //
        let mut previous_key =
            journal_entry_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"), 0);
        for i in 1..300 {
            let current_key =
                journal_entry_key(&ServiceId::with_partition_key(1337, "svc-1", "key-a"), i);
            assert!(previous_key < current_key);
            previous_key = current_key;
        }
    }
}
