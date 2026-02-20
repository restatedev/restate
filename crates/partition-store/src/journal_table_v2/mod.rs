// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use anyhow::anyhow;
use futures::Stream;
use futures_util::stream;
use rocksdb::{DBAccess, DBRawIteratorWithThreadMode};

use restate_memory::{LocalMemoryLease, LocalMemoryPool};
use restate_rocksdb::{Priority, RocksDbPerfGuard};
use restate_storage_api::journal_table_v2::{
    JournalEntryIndex, ReadJournalTable, ScanJournalTable, ScanJournalTableRange, StoredEntry,
    WriteJournalTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{BudgetedReadError, Result, StorageError};
use restate_types::identifiers::{
    EntryIndex, InvocationId, InvocationUuid, JournalEntryId, PartitionKey, WithPartitionKey,
};
use restate_types::journal_v2::raw::{RawCommand, RawEntry};
use restate_types::journal_v2::{CompletionId, EntryMetadata, NotificationId};
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};

use crate::TableKind::Journal;
use crate::keys::{KeyKind, TableKey, define_table_key};
use crate::owned_iter::OwnedIterator;
use crate::{PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan, break_on_err};

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
    KeyKind::JournalV2CompletionIdToCommandIndex,
    JournalCompletionIdToCommandIndexKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        completion_id: CompletionId
    )
);

define_table_key!(
    Journal,
    KeyKind::JournalV2NotificationIdToNotificationIndex,
    JournalNotificationIdToNotificationIndexKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        notification_id: NotificationId
    )
);

/// Lazy iterator over journal entries. Supports two-phase iteration:
/// [`peek_value_size`](Self::peek_value_size) to inspect the raw byte size before
/// decoding, then [`decode_and_advance`](Self::decode_and_advance) to decode and
/// move forward. Also implements [`Iterator`] for convenience.
pub struct JournalEntryIter<'a, DB: DBAccess> {
    iter: DBRawIteratorWithThreadMode<'a, DB>,
    remaining: u32,
}

impl<'a, DB: DBAccess> JournalEntryIter<'a, DB> {
    fn new(iter: DBRawIteratorWithThreadMode<'a, DB>, journal_length: EntryIndex) -> Self {
        Self {
            iter,
            remaining: journal_length,
        }
    }

    /// Returns the raw serialized byte size of the value at the current iterator
    /// position without decoding or advancing. Returns `None` when exhausted.
    pub fn peek_value_size(&self) -> Option<Result<usize>> {
        if self.remaining == 0 {
            return None;
        }
        match self.iter.item() {
            Some((_k, v)) => Some(Ok(v.len())),
            None => self
                .iter
                .status()
                .err()
                .map(|err| Err(StorageError::Generic(err.into()))),
        }
    }

    /// Decodes the current entry and advances the iterator.
    /// Must only be called after [`peek_value_size`](Self::peek_value_size) returned
    /// `Some(Ok(_))`.
    pub fn decode_and_advance(&mut self) -> Option<Result<(EntryIndex, StoredRawEntry)>> {
        if self.remaining == 0 {
            return None;
        }

        let Some((mut k, mut v)) = self.iter.item() else {
            return self
                .iter
                .status()
                .err()
                .map(|err| Err(StorageError::Generic(err.into())));
        };

        let key_result =
            JournalKey::deserialize_from(&mut k).map(|journal_key| journal_key.journal_index);
        let entry_result = StoredEntry::decode(&mut v).map_err(|e| StorageError::Generic(e.into()));

        self.iter.next();
        self.remaining -= 1;

        Some(key_result.and_then(|key| entry_result.map(|entry| (key, entry.0))))
    }
}

impl<DB: DBAccess> Iterator for JournalEntryIter<'_, DB> {
    type Item = Result<(EntryIndex, StoredRawEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.decode_and_advance()
    }
}

fn write_journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> JournalKey {
    JournalKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
        journal_index,
    }
}

fn put_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
    journal_entry: &StoredRawEntry,
    related_completion_ids: &[CompletionId],
) -> Result<()> {
    if let RawEntry::Notification(notification) = &journal_entry.inner {
        storage.put_kv_proto(
            JournalNotificationIdToNotificationIndexKey {
                partition_key: invocation_id.partition_key(),
                invocation_uuid: invocation_id.invocation_uuid(),
                notification_id: notification.id(),
            },
            &JournalEntryIndex(journal_index),
        )?;
    } else if let RawEntry::Command(_) = &journal_entry.inner {
        for completion_id in related_completion_ids {
            storage.put_kv_proto(
                JournalCompletionIdToCommandIndexKey {
                    partition_key: invocation_id.partition_key(),
                    invocation_uuid: invocation_id.invocation_uuid(),
                    completion_id: *completion_id,
                },
                &JournalEntryIndex(journal_index),
            )?;
        }
    }

    storage.put_kv_proto(
        write_journal_entry_key(invocation_id, journal_index),
        &StoredEntry(journal_entry.clone()),
    )
}

fn get_journal_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
) -> Result<Option<StoredRawEntry>> {
    let key = write_journal_entry_key(invocation_id, journal_index);
    let opt: Option<StoredEntry> = storage.get_value_proto(key)?;
    Ok(opt.map(|e| e.0))
}

fn get_journal<'a, S: StorageAccess>(
    storage: &'a S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<JournalEntryIter<'a, S::DBAccess<'a>>> {
    let _x = RocksDbPerfGuard::new("get-journal-iter-setup");
    let key = JournalKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let iter = storage.iterator_from(TableScan::SinglePartitionKeyPrefix(
        invocation_id.partition_key(),
        key,
    ))?;

    Ok(JournalEntryIter::new(iter, journal_length))
}

fn delete_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<()> {
    let _x = RocksDbPerfGuard::new("delete-journal");

    let mut key = write_journal_entry_key(invocation_id, 0);
    let k = &mut key;
    for journal_index in 0..journal_length {
        k.journal_index = journal_index;
        storage.delete_key(k)?;
    }

    // Delete the indexes
    let notification_id_to_notification_index =
        JournalNotificationIdToNotificationIndexKey::builder()
            .partition_key(invocation_id.partition_key())
            .invocation_uuid(invocation_id.invocation_uuid());
    let notification_id_index = OwnedIterator::new(storage.iterator_from(
        TableScan::SinglePartitionKeyPrefix(
            invocation_id.partition_key(),
            notification_id_to_notification_index.clone(),
        ),
    )?)
    .map(|(mut key, _)| {
        let journal_key = JournalNotificationIdToNotificationIndexKey::deserialize_from(&mut key)?;
        let (_, _, notification_id) = journal_key.split();
        Ok(notification_id)
    })
    .collect::<Result<Vec<_>>>()?;
    for notification_id in notification_id_index {
        storage.delete_key(
            &notification_id_to_notification_index
                .clone()
                .notification_id(notification_id)
                .into_complete()
                .unwrap(),
        )?;
    }

    let completion_id_to_command_index = JournalCompletionIdToCommandIndexKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    let completion_id_index =
        OwnedIterator::new(storage.iterator_from(TableScan::SinglePartitionKeyPrefix(
            invocation_id.partition_key(),
            completion_id_to_command_index.clone(),
        ))?)
        .map(|(mut key, _)| {
            let journal_key = JournalCompletionIdToCommandIndexKey::deserialize_from(&mut key)?;
            let (_, _, completion_id) = journal_key.split();
            Ok(completion_id)
        })
        .collect::<Result<Vec<_>>>()?;
    for completion_id in completion_id_index {
        storage.delete_key(
            &completion_id_to_command_index
                .clone()
                .completion_id(completion_id)
                .into_complete()
                .unwrap(),
        )?;
    }

    Ok(())
}

/// Scans for and removes orphaned `JournalCompletionIdToCommandIndex` (`jc`) entries.
///
/// A `jc` entry is orphaned if no corresponding `JournalKey` (`j2`) entries exist for that
/// invocation, meaning the journal has already been deleted. These orphans were caused by a
/// bug in `delete_journal` that used the wrong scan prefix when cleaning up `jc` entries.
///
/// Only the keys for a single invocation are held in memory at any time to avoid unbounded
/// memory usage on large stores.
///
/// The `is_cancelled` predicate is checked when moving to a new invocation. If it returns
/// `true`, the scan stops early and the returned `cancelled` flag is set to `true`.
pub fn cleanup_orphaned_completion_id_index_entries(
    storage: &mut PartitionStore,
    is_cancelled: impl Fn() -> bool,
) -> Result<OrphanCleanupResult> {
    let _x = RocksDbPerfGuard::new("cleanup-orphaned-jc-entries");

    let mut deleted_entries: usize = 0;
    let mut affected_invocations: usize = 0;
    let mut cancelled = false;

    let scan_store = storage.clone();
    let partition_key_range = scan_store.partition_key_range().clone();
    let scan = TableScan::FullScanPartitionKeyRange::<JournalCompletionIdToCommandIndexKeyBuilder>(
        partition_key_range,
    );
    let iter = OwnedIterator::new(scan_store.iterator_from(scan)?);

    let mut current_invocation: Option<(PartitionKey, InvocationUuid, bool)> = None;

    for (mut key_bytes, _) in iter {
        let jc_key = JournalCompletionIdToCommandIndexKey::deserialize_from(&mut key_bytes)?;

        let is_orphan = match &current_invocation {
            Some((pk, uuid, orphan))
                if *pk == jc_key.partition_key && *uuid == jc_key.invocation_uuid =>
            {
                // Same invocation as before -- reuse the cached result.
                *orphan
            }
            _ => {
                // Check cancellation at invocation boundaries.
                if is_cancelled() {
                    cancelled = true;
                    break;
                }
                // New invocation -- check if its journal still exists.
                let orphan =
                    !has_journal_entries(storage, jc_key.partition_key, jc_key.invocation_uuid)?;
                if orphan {
                    affected_invocations += 1;
                }
                current_invocation = Some((jc_key.partition_key, jc_key.invocation_uuid, orphan));
                orphan
            }
        };

        if is_orphan {
            storage.delete_key(&jc_key)?;
            deleted_entries += 1;
        }
    }

    Ok(OrphanCleanupResult {
        deleted_entries,
        affected_invocations,
        cancelled,
    })
}

pub struct OrphanCleanupResult {
    pub deleted_entries: usize,
    pub affected_invocations: usize,
    pub cancelled: bool,
}

/// Returns true if any `j2` journal entries exist for the given invocation.
fn has_journal_entries(
    storage: &mut PartitionStore,
    partition_key: PartitionKey,
    invocation_uuid: InvocationUuid,
) -> Result<bool> {
    let prefix = JournalKey::builder()
        .partition_key(partition_key)
        .invocation_uuid(invocation_uuid);
    let iter = storage.iterator_from(TableScan::SinglePartitionKeyPrefix(partition_key, prefix))?;
    Ok(iter.item().is_some())
}

fn get_notifications_index<S: StorageAccess>(
    storage: &mut S,
    invocation_id: InvocationId,
) -> Result<HashMap<NotificationId, EntryIndex>> {
    let key = JournalNotificationIdToNotificationIndexKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    let iter = storage.iterator_from(TableScan::SinglePartitionKeyPrefix(
        invocation_id.partition_key(),
        key,
    ))?;
    OwnedIterator::new(iter)
        .map(|(mut key, mut value)| {
            let journal_key =
                JournalNotificationIdToNotificationIndexKey::deserialize_from(&mut key)?;
            let index = JournalEntryIndex::decode(&mut value)
                .map_err(|err| StorageError::Conversion(err.into()))?;

            let (_, _, notification_id) = journal_key.split();

            Ok((notification_id, index.0))
        })
        .collect()
}

fn get_command_by_completion_id<S: StorageAccess>(
    storage: &mut S,
    invocation_id: InvocationId,
    completion_id: CompletionId,
) -> Result<Option<(StoredRawEntryHeader, RawCommand)>> {
    let _x = RocksDbPerfGuard::new("get-command-by-completion-id");

    // Access the index
    let completion_id_to_command_index = JournalCompletionIdToCommandIndexKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
        completion_id,
    };
    let opt: Option<JournalEntryIndex> = storage.get_value_proto(completion_id_to_command_index)?;
    if opt.is_none() {
        return Ok(None);
    }

    // Now access the entry
    let journal_index = opt.unwrap().0;
    let key = write_journal_entry_key(&invocation_id, journal_index);
    let opt: Option<StoredEntry> = storage.get_value_proto(key)?;
    if opt.is_none() {
        return Ok(None);
    }

    let entry = opt.unwrap().0;
    let entry_ty = entry.ty();
    let command = entry.inner.try_as_command().ok_or_else(|| {
        StorageError::Conversion(anyhow!(
            "Entry is expected to be a command, but is {entry_ty}"
        ))
    })?;

    Ok(Some((entry.header, command)))
}

fn has_completion<S: StorageAccess>(
    storage: &mut S,
    invocation_id: InvocationId,
    completion_id: CompletionId,
) -> Result<bool> {
    let _x = RocksDbPerfGuard::new("has-completion");

    // Access the index
    let key = JournalNotificationIdToNotificationIndexKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
        notification_id: NotificationId::CompletionId(completion_id),
    };
    Ok(storage
        .get_value_proto::<_, JournalEntryIndex>(key)?
        .is_some())
}

impl ReadJournalTable for PartitionStore {
    async fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
    ) -> Result<Option<StoredRawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbPerfGuard::new("get-journal-entry");
        get_journal_entry(self, &invocation_id, journal_index)
    }

    fn get_journal(
        &self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredRawEntry)>> + Send> {
        self.assert_partition_key(&invocation_id)?;
        Ok(stream::iter(get_journal(
            self,
            &invocation_id,
            journal_length,
        )?))
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, EntryIndex>> {
        get_notifications_index(self, invocation_id)
    }

    async fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> Result<Option<(StoredRawEntryHeader, RawCommand)>> {
        get_command_by_completion_id(self, invocation_id, notification_id)
    }

    async fn has_completion(
        &mut self,
        invocation_id: InvocationId,
        completion_id: CompletionId,
    ) -> Result<bool> {
        has_completion(self, invocation_id, completion_id)
    }

    fn get_journal_budgeted<'a>(
        &'a self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl Stream<
            Item = std::result::Result<
                (EntryIndex, StoredRawEntry, LocalMemoryLease),
                BudgetedReadError,
            >,
        > + Send
        + 'a,
    > {
        self.assert_partition_key(&invocation_id)?;
        let iter = get_journal(self, &invocation_id, journal_length)?;
        Ok(budgeted_journal_v2_stream(iter, budget))
    }
}

impl ScanJournalTable for PartitionStore {
    fn for_each_journal<
        F: FnMut(
                (restate_types::identifiers::JournalEntryId, StoredRawEntry),
            ) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: ScanJournalTableRange,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        let scan = match range {
            ScanJournalTableRange::PartitionKey(partition_key) => {
                TableScan::FullScanPartitionKeyRange::<JournalKeyBuilder>(partition_key)
            }
            ScanJournalTableRange::InvocationId(invocation_id) => {
                let start = JournalKey::builder()
                    .partition_key(invocation_id.start().partition_key())
                    .invocation_uuid(invocation_id.start().invocation_uuid());

                let end = JournalKey::builder()
                    .partition_key(invocation_id.end().partition_key())
                    .invocation_uuid(invocation_id.end().invocation_uuid());

                TableScan::KeyRangeInclusiveInSinglePartition(self.partition_id(), start, end)
            }
        };

        self.iterator_for_each(
            "df-v2-journal",
            Priority::Low,
            scan,
            move |(mut key, mut value)| {
                let journal_key = break_on_err(JournalKey::deserialize_from(&mut key))?;
                let journal_entry = break_on_err(
                    StoredEntry::decode(&mut value)
                        .map_err(|err| StorageError::Conversion(err.into())),
                )?;

                let (partition_key, invocation_uuid, entry_index) = journal_key.split();

                let journal_entry_id = JournalEntryId::from_parts(
                    InvocationId::from_parts(partition_key, invocation_uuid),
                    entry_index,
                );

                f((journal_entry_id, journal_entry.0)).map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

impl ReadJournalTable for PartitionStoreTransaction<'_> {
    async fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
    ) -> Result<Option<StoredRawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbPerfGuard::new("get-journal-entry");
        get_journal_entry(self, &invocation_id, journal_index)
    }

    fn get_journal(
        &self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredRawEntry)>> + Send> {
        self.assert_partition_key(&invocation_id)?;
        Ok(stream::iter(get_journal(
            self,
            &invocation_id,
            journal_length,
        )?))
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, EntryIndex>> {
        get_notifications_index(self, invocation_id)
    }

    async fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> Result<Option<(StoredRawEntryHeader, RawCommand)>> {
        get_command_by_completion_id(self, invocation_id, notification_id)
    }

    async fn has_completion(
        &mut self,
        invocation_id: InvocationId,
        completion_id: CompletionId,
    ) -> Result<bool> {
        has_completion(self, invocation_id, completion_id)
    }

    fn get_journal_budgeted<'a>(
        &'a self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
        budget: &'a mut LocalMemoryPool,
    ) -> Result<
        impl Stream<
            Item = std::result::Result<
                (EntryIndex, StoredRawEntry, LocalMemoryLease),
                BudgetedReadError,
            >,
        > + Send
        + 'a,
    > {
        self.assert_partition_key(&invocation_id)?;
        let iter = get_journal(self, &invocation_id, journal_length)?;
        Ok(budgeted_journal_v2_stream(iter, budget))
    }
}

/// Pre-deserialization budgeted journal V2 stream (peek → reserve → decode).
fn budgeted_journal_v2_stream<'a, DB: DBAccess + Send>(
    iter: JournalEntryIter<'a, DB>,
    budget: &'a mut LocalMemoryPool,
) -> impl Stream<
    Item = std::result::Result<(EntryIndex, StoredRawEntry, LocalMemoryLease), BudgetedReadError>,
> + Send
+ 'a {
    futures::stream::unfold((iter, budget), |(mut iter, budget)| async move {
        let raw_size = match iter.peek_value_size() {
            Some(Ok(size)) => size,
            Some(Err(e)) => return Some((Err(e.into()), (iter, budget))),
            None => return None,
        };

        let lease = match budget.reserve(raw_size).await {
            Ok(lease) => lease,
            Err(e) => return Some((Err(e.into()), (iter, budget))),
        };

        match iter.decode_and_advance() {
            Some(Ok((idx, entry))) => Some((Ok((idx, entry, lease)), (iter, budget))),
            Some(Err(e)) => Some((Err(e.into()), (iter, budget))),
            None => None,
        }
    })
}

impl WriteJournalTable for PartitionStoreTransaction<'_> {
    fn put_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        index: u32,
        entry: &StoredRawEntry,
        related_completion_ids: &[CompletionId],
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        put_journal_entry(self, &invocation_id, index, entry, related_completion_ids)
    }

    fn delete_journal(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Result<()> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbPerfGuard::new("delete-journal");
        delete_journal(self, &invocation_id, journal_length)
    }
}

#[cfg(test)]
mod tests {

    use super::write_journal_entry_key;

    use crate::keys::TableKeyPrefix;
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
