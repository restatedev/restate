// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::TableKey;
use crate::keys::{define_table_key, KeyKind};
use crate::owned_iter::OwnedIterator;
use crate::scan::TableScan::FullScanPartitionKeyRange;
use crate::TableKind::Journal;
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess};
use crate::{TableScan, TableScanIterationDecision};
use futures::Stream;
use futures_util::stream;
use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::journal_table_v2::{
    JournalTable, NotificationsIndex, ReadOnlyJournalTable, StoredEntry,
};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::{
    EntryIndex, InvocationId, InvocationUuid, JournalEntryId, PartitionKey, WithPartitionKey,
};
use restate_types::journal_v2::raw::{RawEntry, RawEntryInner};
use restate_types::journal_v2::NotificationId;
use restate_types::storage::StorageCodec;
use std::collections::HashMap;
use std::io::Cursor;
use std::ops::RangeInclusive;

define_table_key!(
    Journal,
    KeyKind::JournalV2,
    JournalKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        journal_index: u32
    )
);

define_table_key!(
    Journal,
    KeyKind::JournalNotificationsIndex,
    JournalNotificationsIndexKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid
    )
);

fn write_journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> JournalKey {
    JournalKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
        .journal_index(journal_index)
}

fn write_journal_notifications_index(invocation_id: &InvocationId) -> JournalNotificationsIndexKey {
    JournalNotificationsIndexKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid())
}

fn put_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
    journal_entry: &RawEntry,
) -> Result<()> {
    let key = write_journal_entry_key(invocation_id, journal_index);

    if let RawEntryInner::Notification(notification) = &journal_entry.inner {
        let key = write_journal_notifications_index(&invocation_id);
        let mut notifications_index: NotificationsIndex =
            storage.get_value(key.clone())?.unwrap_or_default();
        notifications_index
            .0
            .insert(notification.id(), journal_index);
        storage.put_kv(key, &notifications_index);
    }

    // TODO completely unnecessary clone if we sort out the organization of
    //  these wrapper types within this module, rather than in storage-api
    storage.put_kv(key, &StoredEntry(journal_entry.clone()));

    Ok(())
}

fn get_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
) -> Result<Option<RawEntry>> {
    let key = write_journal_entry_key(invocation_id, journal_index);
    let opt: Option<StoredEntry> = storage.get_value(key)?;
    Ok(opt.map(|e| e.0))
}

fn get_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Vec<Result<(EntryIndex, RawEntry)>> {
    let _x = RocksDbPerfGuard::new("get-journal");
    let key = JournalKey::default()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let mut n = 0;
    storage.for_each_key_value_in_place(
        TableScan::SinglePartitionKeyPrefix(invocation_id.partition_key(), key),
        move |k, mut v| {
            let key = JournalKey::deserialize_from(&mut Cursor::new(k)).map(|journal_key| {
                journal_key
                    .journal_index
                    .expect("The journal index must be part of the journal key.")
            });
            let entry = StorageCodec::decode::<StoredEntry, _>(&mut v)
                .map_err(|error| StorageError::Generic(error.into()));

            let result = key.and_then(|key| entry.map(|entry| (key, entry.0)));

            n += 1;
            if n < journal_length {
                TableScanIterationDecision::Emit(result)
            } else {
                TableScanIterationDecision::BreakWith(result)
            }
        },
    )
}

fn all_journals<S: StorageAccess>(
    storage: &S,
    range: RangeInclusive<PartitionKey>,
) -> impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send + '_ {
    let iter = storage.iterator_from(FullScanPartitionKeyRange::<JournalKey>(range));
    stream::iter(OwnedIterator::new(iter).map(|(mut key, mut value)| {
        let journal_key = JournalKey::deserialize_from(&mut key)?;
        let journal_entry = StorageCodec::decode::<StoredEntry, _>(&mut value)
            .map_err(|err| StorageError::Conversion(err.into()))?;

        let (partition_key, invocation_uuid, entry_index) = journal_key.into_inner_ok_or()?;

        Ok((
            JournalEntryId::from_parts(
                InvocationId::from_parts(partition_key, invocation_uuid),
                entry_index,
            ),
            journal_entry.0,
        ))
    }))
}

fn delete_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) {
    let mut key = write_journal_entry_key(invocation_id, 0);
    let k = &mut key;
    for journal_index in 0..journal_length {
        k.journal_index = Some(journal_index);
        storage.delete_key(k);
    }

    // Delete notifications index
    storage.delete_key(&write_journal_notifications_index(invocation_id));
}

fn get_notifications_index<S: StorageAccess>(
    storage: &mut S,
    invocation_id: InvocationId,
) -> Result<HashMap<NotificationId, EntryIndex>> {
    let key = write_journal_notifications_index(&invocation_id);
    let opt: Option<NotificationsIndex> = storage.get_value(key)?;
    Ok(opt.map(|e| e.0).unwrap_or_default())
}

impl ReadOnlyJournalTable for PartitionStore {
    async fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
    ) -> Result<Option<RawEntry>> {
        self.assert_partition_key(&invocation_id);
        let _x = RocksDbPerfGuard::new("get-journal-entry");
        get_journal_entry(self, &invocation_id, journal_index)
    }

    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, RawEntry)>> + Send {
        self.assert_partition_key(&invocation_id);
        stream::iter(get_journal(self, &invocation_id, journal_length))
    }

    fn all_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send {
        all_journals(self, range)
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, EntryIndex>> {
        get_notifications_index(self, invocation_id)
    }
}

impl<'a> ReadOnlyJournalTable for PartitionStoreTransaction<'a> {
    async fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
    ) -> Result<Option<RawEntry>> {
        self.assert_partition_key(&invocation_id);
        let _x = RocksDbPerfGuard::new("get-journal-entry");
        get_journal_entry(self, &invocation_id, journal_index)
    }

    fn get_journal(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, RawEntry)>> + Send {
        self.assert_partition_key(&invocation_id);
        stream::iter(get_journal(self, &invocation_id, journal_length))
    }

    fn all_journals(
        &self,
        range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = Result<(JournalEntryId, RawEntry)>> + Send {
        all_journals(self, range)
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, EntryIndex>> {
        get_notifications_index(self, invocation_id)
    }
}

impl<'a> JournalTable for PartitionStoreTransaction<'a> {
    async fn put_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
        journal_entry: &RawEntry,
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id);
        put_journal_entry(self, &invocation_id, journal_index, journal_entry)
    }

    async fn delete_journal(&mut self, invocation_id: InvocationId, journal_length: EntryIndex) {
        self.assert_partition_key(&invocation_id);
        let _x = RocksDbPerfGuard::new("delete-journal");
        delete_journal(self, &invocation_id, journal_length)
    }
}

#[cfg(test)]
mod tests {
    use super::write_journal_entry_key;
    use crate::keys::TableKey;
    use bytes::Bytes;
    use restate_types::identifiers::{InvocationId, InvocationUuid};

    fn journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> Bytes {
        write_journal_entry_key(invocation_id, journal_index)
            .serialize()
            .freeze()
    }

    #[test]
    fn journal_keys_sort_lex() {
        //
        // across invocations
        //
        assert!(
            journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
                0
            ) < journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(2)),
                0
            )
        );
        //
        // within the same service and key
        //
        let mut previous_key = journal_entry_key(
            &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
            0,
        );
        for i in 1..300 {
            let current_key = journal_entry_key(
                &InvocationId::from_parts(1337, InvocationUuid::from_u128(1)),
                i,
            );
            assert!(previous_key < current_key);
            previous_key = current_key;
        }
    }
}
