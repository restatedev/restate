// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Journal V2 storage and its table-specific lookup keys.
//!
//! [`JournalKey`] and [`JournalCompletionIdToCommandIndexKey`] use fixed-width fields.
//! [`JournalNotificationIdToNotificationIndexKey`] instead allows a raw signal-name
//! suffix: names compare lexicographically there, but the codec consumes the rest
//! of the key and is not a composable, prefix-free ordered-field codec. Keep
//! `notification_id` last. See the [key encoding families](crate::keys#two-encoding-families).

use std::collections::HashMap;

use anyhow::anyhow;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use futures_util::stream;

use restate_memory::{ClaimedMemoryPool, LocalMemoryLease, LocalMemoryPool};
use restate_rocksdb::{Priority, RocksDbReadPerfGuard};
use restate_storage_api::blob_table::{BlobBuilder, WriteBlobTable};
use restate_storage_api::journal_table_v2::{
    BudgetedJournalItem, JournalEntryIndex, NotificationEntryIndex, ReadJournalTable,
    ScanJournalTable, ScanJournalTableRange, StoredEntry, StoredEntryLite,
    StoredUnhydratedRawEntry, WriteJournalTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{BudgetedReadError, Result, StorageError};
use restate_types::blob::{BlobId, BlobKind, RelatedBlob};
use restate_types::identifiers::{
    EntryIndex, InvocationId, InvocationUuid, JournalEntryId, PartitionKey, WithPartitionKey,
};
use restate_types::journal_v2::{CompletionId, EntryMetadata, EntryType, NotificationId};
use restate_types::storage::StoredRawEntry;

use crate::TableKind::Journal;
use crate::blob_table::{BlobReader, get_blob_payload};
use crate::budgeted_reads::{MAX_INLINE_ROWS, budgeted_get};
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, define_table_key};
use crate::owned_iter::OwnedIterator;
use crate::scan::PhysicalScan;
use crate::{
    PartitionStore, PartitionStoreSnapshot, PartitionStoreTransaction, StorageAccess,
    StorageIterator, StorageRead, TableScan, TableScanIterationDecision, break_on_err,
};

define_table_key!(
    Journal,
    KeyKind::JournalV2,
    JournalKey(partition_key: PartitionKey, invocation_uuid: InvocationUuid, journal_index: u32)
);

define_table_key!(
    Journal,
    KeyKind::JournalV2CompletionIdToCommandIndex,
    JournalCompletionIdToCommandIndexKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        completion_id: CompletionId,
    )
);

impl JournalCompletionIdToCommandIndexKey {
    pub const fn serialized_length_fixed() -> usize {
        KeyKind::SERIALIZED_LENGTH
            + std::mem::size_of::<PartitionKey>()
            + InvocationUuid::RAW_BYTES_LEN
            + std::mem::size_of::<CompletionId>()
    }
}

define_table_key!(
    Journal,
    KeyKind::JournalV2NotificationIdToNotificationIndex,
    JournalNotificationIdToNotificationIndexKey(
        partition_key: PartitionKey,
        invocation_uuid: InvocationUuid,
        notification_id: NotificationId,
    )
);

/// Lazy iterator over journal V2 entries. Exposes [`peek_item`](Self::peek_item)
/// for zero-copy access to raw key/value slices and [`advance`](Self::advance)
/// to move forward. Also implements [`Iterator`] for convenience.
pub(crate) struct JournalEntryIter<I> {
    iter: I,
    remaining: u32,
}

impl<I: StorageIterator> JournalEntryIter<I> {
    fn new(iter: I, journal_length: EntryIndex) -> Self {
        Self {
            iter,
            remaining: journal_length,
        }
    }

    /// Returns the raw `(key, value)` byte slices at the current iterator
    /// position without decoding or advancing. Returns `None` when exhausted.
    pub fn peek_item(&self) -> Option<Result<(&[u8], &[u8])>> {
        if self.remaining == 0 {
            return None;
        }
        self.iter.item().transpose()
    }

    /// Advances the iterator to the next entry.
    pub fn advance(&mut self) {
        self.iter.next();
        self.remaining -= 1;
    }
}

/// Decodes a V2 journal key/value pair from raw byte slices.
fn decode_journal_entry_v2(k: &[u8], v: &[u8]) -> Result<(EntryIndex, StoredUnhydratedRawEntry)> {
    let mut k = k;
    let mut v = v;
    let index = JournalKey::deserialize_from(&mut k)?.journal_index;
    let entry = StoredEntry::decode(&mut v).map_err(|e| StorageError::Generic(e.into()))?;
    Ok((index, entry.0))
}

impl<I: StorageIterator> Iterator for JournalEntryIter<I> {
    type Item = Result<(EntryIndex, StoredUnhydratedRawEntry)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = match self.peek_item()? {
            Ok(item) => item,
            Err(e) => return Some(Err(e)),
        };
        let result = decode_journal_entry_v2(k, v);
        self.advance();
        Some(result)
    }
}

fn write_journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> JournalKey {
    JournalKey {
        partition_key: invocation_id.partition_key(),
        invocation_uuid: invocation_id.invocation_uuid(),
        journal_index,
    }
}

/// Writes the entry, storing its content in a blob if it's larger than `inline_entry_threshold`.
///
/// See [`WriteJournalTable::put_journal_entry`]: an entry must be written only once, as the blob id
/// is derived from it.
fn put_journal_entry<S: StorageAccess + WriteBlobTable>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
    journal_entry: &StoredRawEntry,
    related_completion_ids: &[CompletionId],
    inline_entry_threshold: Option<usize>,
) -> Result<Option<BlobId>> {
    let entry = match inline_entry_threshold {
        Some(threshold) if journal_entry.inner.serialized_length() > threshold => {
            StoredUnhydratedRawEntry::store_content(journal_entry.clone(), |content| {
                let id = BlobId::generate_journal_entry_blob_id(
                    journal_entry.header.append_time,
                    journal_index,
                    invocation_id,
                );
                let size = storage.put_blob(
                    id,
                    BlobBuilder::new(BlobKind::Journal).build(content.as_ref()),
                )?;
                Ok::<_, StorageError>(RelatedBlob { id, size })
            })?
        }
        _ => journal_entry.clone().into(),
    };
    let blob_id = entry.content_blob().map(|blob| blob.id);
    put_stored_entry(
        storage,
        invocation_id,
        journal_index,
        entry,
        related_completion_ids,
    )?;
    Ok(blob_id)
}

/// Writes the entry as it is, along with its indexes.
fn put_stored_entry<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
    entry: StoredUnhydratedRawEntry,
    related_completion_ids: &[CompletionId],
) -> Result<()> {
    if let (Some(notification_id), Some(result_variant)) =
        (entry.notification_id(), entry.notification_result_variant())
    {
        storage.put_kv_proto(
            JournalNotificationIdToNotificationIndexKey {
                partition_key: invocation_id.partition_key(),
                invocation_uuid: invocation_id.invocation_uuid(),
                notification_id,
            },
            &NotificationEntryIndex {
                entry_index: journal_index,
                result_variant,
            },
        )?;
    } else {
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

    storage.put_kv_proto_owned(
        write_journal_entry_key(invocation_id, journal_index),
        StoredEntry(entry),
    )
}

fn get_journal_entry<S: StorageRead>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
) -> Result<Option<StoredUnhydratedRawEntry>> {
    let key = write_journal_entry_key(invocation_id, journal_index);
    let opt: Option<StoredEntry> = storage.get_value_proto(key)?;
    Ok(opt.map(|e| e.0))
}

fn hydrate_journal_entry<S: StorageRead>(
    storage: &S,
    entry: StoredUnhydratedRawEntry,
) -> Result<StoredRawEntry> {
    let entry = match entry.try_into_inline() {
        Ok(entry) => return Ok(entry),
        Err(entry) => entry,
    };
    let blob = entry.content_blob().expect("the content is in a blob");
    // The blob outlives the journal entries referencing it
    let content = get_blob_payload(storage, blob.id)?.ok_or(StorageError::DataIntegrityError)?;
    Ok(entry.hydrate_with(content))
}

/// What a budgeted read reserves for a journal entry: its stored value and, if its content is
/// stored in a blob, that content too. Reserving both up front lets the read hydrate the entry
/// under one lease, without waiting for budget in between.
fn budgeted_entry_cost(mut value: &[u8]) -> usize {
    let stored = value.len();
    // A value that doesn't decode here fails when the entry is decoded.
    let content = StoredEntryLite::decode(&mut value)
        .ok()
        .and_then(|entry| entry.content_blob)
        // A size that doesn't fit in memory fails the reservation with `OutOfMemory`.
        .map_or(0, |blob| usize::try_from(blob.size).unwrap_or(usize::MAX));
    stored.saturating_add(content)
}

fn get_journal<'a, S: StorageRead>(
    storage: &'a S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<JournalEntryIter<S::Iterator<'a>>> {
    let _x = RocksDbReadPerfGuard::new("get-journal-iter-setup");
    let key = JournalKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let iter = storage.iterator_from(TableScan::Prefix(key))?;

    Ok(JournalEntryIter::new(iter, journal_length))
}

fn journal_physical_scan(invocation_id: &InvocationId) -> PhysicalScan<Bytes> {
    let key = JournalKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    TableScan::Prefix(key).into()
}

fn delete_journal<S: StorageAccess + WriteBlobTable>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<()> {
    let _x = RocksDbReadPerfGuard::new("delete-journal");

    // Collect the blobs referenced by the entries before deleting them. Only the content blob is
    // decoded, the entry content is skipped.
    let mut content_blob_ids = Vec::new();
    {
        let mut iter = get_journal(storage, invocation_id, journal_length)?;
        while let Some(item) = iter.peek_item() {
            let (_, mut value) = item?;
            content_blob_ids.extend(
                StoredEntryLite::decode(&mut value)?
                    .content_blob
                    .map(|blob| blob.id),
            );
            iter.advance();
        }
    }

    let mut key = write_journal_entry_key(invocation_id, 0);
    let k = &mut key;
    for journal_index in 0..journal_length {
        k.journal_index = journal_index;
        storage.delete_key(k)?;
    }

    for blob_id in content_blob_ids {
        storage.decrement_ref_count(blob_id, 1);
    }

    // Delete the indexes
    let notification_id_to_notification_index =
        JournalNotificationIdToNotificationIndexKey::builder()
            .partition_key(invocation_id.partition_key())
            .invocation_uuid(invocation_id.invocation_uuid());
    let notification_id_index = OwnedIterator::new(storage.iterator_from(TableScan::Prefix(
        notification_id_to_notification_index.clone(),
    ))?)
    .map(|item| {
        let (key, _) = item?;
        let journal_key =
            JournalNotificationIdToNotificationIndexKey::deserialize_from(&mut key.as_ref())?;
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
    let completion_id_index = OwnedIterator::new(
        storage.iterator_from(TableScan::Prefix(completion_id_to_command_index.clone()))?,
    )
    .map(|item| {
        let (key, _) = item?;
        let journal_key =
            JournalCompletionIdToCommandIndexKey::deserialize_from(&mut key.as_ref())?;
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
    let _x = RocksDbReadPerfGuard::new("cleanup-orphaned-jc-entries");

    let scan_store = storage.clone();
    let partition_key_range = scan_store.partition_key_range();
    let scan = TableScan::ScanPartitionKeyRange::<JournalCompletionIdToCommandIndexKeyBuilder>(
        partition_key_range,
    );
    let mut iter = scan_store.iterator_from(scan)?;

    cleanup_orphaned_completion_id_index_entries_inner(
        storage,
        || match iter.item() {
            Some((mut key_bytes, _)) => {
                let jc_key =
                    JournalCompletionIdToCommandIndexKey::deserialize_from(&mut key_bytes)?;
                iter.next();
                Ok(Some(jc_key))
            }
            None => {
                iter.status()
                    .map_err(|err| StorageError::Generic(err.into()))?;
                Ok(None)
            }
        },
        is_cancelled,
        has_journal_entry_zero,
        |storage, key| storage.delete_key(key),
    )
}

fn cleanup_orphaned_completion_id_index_entries_inner<S>(
    storage: &mut S,
    mut next_entry: impl FnMut() -> Result<Option<JournalCompletionIdToCommandIndexKey>>,
    is_cancelled: impl Fn() -> bool,
    mut has_journal: impl FnMut(&mut S, PartitionKey, InvocationUuid) -> Result<bool>,
    mut delete_entry: impl FnMut(&mut S, &JournalCompletionIdToCommandIndexKey) -> Result<()>,
) -> Result<OrphanCleanupResult> {
    let mut scanned_entries: usize = 0;
    let mut scanned_invocations: usize = 0;
    let mut deleted_entries: usize = 0;
    let mut affected_invocations: usize = 0;
    let mut cancelled = false;

    let mut current_invocation: Option<(PartitionKey, InvocationUuid, bool)> = None;

    while let Some(jc_key) = next_entry()? {
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
                let orphan = !has_journal(storage, jc_key.partition_key, jc_key.invocation_uuid)?;
                scanned_invocations += 1;
                if orphan {
                    affected_invocations += 1;
                }
                current_invocation = Some((jc_key.partition_key, jc_key.invocation_uuid, orphan));
                orphan
            }
        };
        scanned_entries += 1;

        if is_orphan {
            delete_entry(storage, &jc_key)?;
            deleted_entries += 1;
        }
    }

    Ok(OrphanCleanupResult {
        scanned_entries,
        scanned_invocations,
        deleted_entries,
        affected_invocations,
        cancelled,
    })
}

pub struct OrphanCleanupResult {
    pub scanned_entries: usize,
    pub scanned_invocations: usize,
    pub deleted_entries: usize,
    pub affected_invocations: usize,
    pub cancelled: bool,
}

/// Returns true if the ownership sentinel `j2[0]` exists for the given invocation.
fn has_journal_entry_zero<S: StorageAccess>(
    storage: &mut S,
    partition_key: PartitionKey,
    invocation_uuid: InvocationUuid,
) -> Result<bool> {
    storage.get_kv_raw(
        JournalKey {
            partition_key,
            invocation_uuid,
            journal_index: 0,
        },
        |_, value| Ok(value.is_some()),
    )
}

fn get_notifications_index<S: StorageRead>(
    storage: &mut S,
    invocation_id: InvocationId,
) -> Result<HashMap<NotificationId, NotificationEntryIndex>> {
    let key = JournalNotificationIdToNotificationIndexKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    let iter = storage.iterator_from(TableScan::Prefix(key))?;
    OwnedIterator::new(iter)
        .map(|item| {
            let (key, mut value) = item?;
            let journal_key =
                JournalNotificationIdToNotificationIndexKey::deserialize_from(&mut key.as_ref())?;
            let index = NotificationEntryIndex::decode(&mut value)
                .map_err(|err| StorageError::Conversion(err.into()))?;

            let (_, _, notification_id) = journal_key.split();

            Ok((notification_id, index))
        })
        .collect()
}

fn get_command_by_completion_id<S: StorageRead>(
    storage: &mut S,
    invocation_id: InvocationId,
    completion_id: CompletionId,
) -> Result<Option<StoredUnhydratedRawEntry>> {
    let _x = RocksDbReadPerfGuard::new("get-command-by-completion-id");

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
    if !matches!(entry_ty, EntryType::Command(_)) {
        return Err(StorageError::Conversion(anyhow!(
            "Entry is expected to be a command, but is {entry_ty}"
        )));
    }

    Ok(Some(entry))
}

fn has_completion<S: StorageRead>(
    storage: &mut S,
    invocation_id: InvocationId,
    completion_id: CompletionId,
) -> Result<bool> {
    let _x = RocksDbReadPerfGuard::new("has-completion");

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
    ) -> Result<Option<StoredUnhydratedRawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("get-journal-entry");
        get_journal_entry(self, &invocation_id, journal_index)
    }

    fn get_journal(
        &self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredUnhydratedRawEntry)>> + Send> {
        self.assert_partition_key(&invocation_id)?;
        Ok(stream::iter(get_journal(
            self,
            &invocation_id,
            journal_length,
        )?))
    }

    fn hydrate_journal_entry(&self, entry: StoredUnhydratedRawEntry) -> Result<StoredRawEntry> {
        hydrate_journal_entry(self, entry)
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, NotificationEntryIndex>> {
        get_notifications_index(self, invocation_id)
    }

    async fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> Result<Option<StoredUnhydratedRawEntry>> {
        get_command_by_completion_id(self, invocation_id, notification_id)
    }

    async fn has_completion(
        &mut self,
        invocation_id: InvocationId,
        completion_id: CompletionId,
    ) -> Result<bool> {
        has_completion(self, invocation_id, completion_id)
    }
}

impl ScanJournalTable for PartitionStore {
    fn for_each_journal<
        F: FnMut(
                (
                    restate_types::identifiers::JournalEntryId,
                    StoredUnhydratedRawEntry,
                ),
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
                TableScan::ScanPartitionKeyRange::<JournalKeyBuilder>(partition_key)
            }
            ScanJournalTableRange::InvocationId(invocation_id) => {
                let start_partition_key = invocation_id.start().partition_key();
                let end_partition_key = invocation_id.end().partition_key();
                let start = JournalKey::builder()
                    .partition_key(start_partition_key)
                    .invocation_uuid(invocation_id.start().invocation_uuid());

                let end = JournalKey::builder()
                    .partition_key(end_partition_key)
                    .invocation_uuid(invocation_id.end().invocation_uuid());

                TableScan::RangeInclusive(start, end)
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
    ) -> Result<Option<StoredUnhydratedRawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("get-journal-entry");
        get_journal_entry(self, &invocation_id, journal_index)
    }

    fn get_journal(
        &self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredUnhydratedRawEntry)>> + Send> {
        self.assert_partition_key(&invocation_id)?;
        Ok(stream::iter(get_journal(
            self,
            &invocation_id,
            journal_length,
        )?))
    }

    fn hydrate_journal_entry(&self, entry: StoredUnhydratedRawEntry) -> Result<StoredRawEntry> {
        hydrate_journal_entry(self, entry)
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, NotificationEntryIndex>> {
        get_notifications_index(self, invocation_id)
    }

    async fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> Result<Option<StoredUnhydratedRawEntry>> {
        get_command_by_completion_id(self, invocation_id, notification_id)
    }

    async fn has_completion(
        &mut self,
        invocation_id: InvocationId,
        completion_id: CompletionId,
    ) -> Result<bool> {
        has_completion(self, invocation_id, completion_id)
    }
}

impl ReadJournalTable for PartitionStoreSnapshot<'_> {
    async fn get_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
    ) -> Result<Option<StoredUnhydratedRawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("get-journal-entry");
        get_journal_entry(self, &invocation_id, journal_index)
    }

    fn get_journal(
        &self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Result<impl Stream<Item = Result<(EntryIndex, StoredUnhydratedRawEntry)>> + Send> {
        self.assert_partition_key(&invocation_id)?;
        Ok(stream::iter(get_journal(
            self,
            &invocation_id,
            journal_length,
        )?))
    }

    fn hydrate_journal_entry(&self, entry: StoredUnhydratedRawEntry) -> Result<StoredRawEntry> {
        hydrate_journal_entry(self, entry)
    }

    async fn get_notifications_index(
        &mut self,
        invocation_id: InvocationId,
    ) -> Result<HashMap<NotificationId, NotificationEntryIndex>> {
        get_notifications_index(self, invocation_id)
    }

    async fn get_command_by_completion_id(
        &mut self,
        invocation_id: InvocationId,
        notification_id: CompletionId,
    ) -> Result<Option<StoredUnhydratedRawEntry>> {
        get_command_by_completion_id(self, invocation_id, notification_id)
    }

    async fn has_completion(
        &mut self,
        invocation_id: InvocationId,
        completion_id: CompletionId,
    ) -> Result<bool> {
        has_completion(self, invocation_id, completion_id)
    }
}

impl PartitionStore {
    /// Budget-gated point read of a single journal entry.
    ///
    /// Fetches raw bytes from the store, peeks the serialized size, acquires a
    /// [`LocalMemoryLease`] from `budget`, and only then decodes the entry.
    pub async fn get_journal_v2_entry_budgeted(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
        budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(StoredRawEntry, LocalMemoryLease)>, BudgetedReadError> {
        self.assert_partition_key(&invocation_id)?;
        let key = write_journal_entry_key(&invocation_id, journal_index);
        let key = {
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };
        let storage: &Self = self;
        budgeted_get(
            storage,
            "get-journal-entry-budgeted",
            KeyKind::JournalV2,
            &key,
            budget,
            budgeted_entry_cost,
            |mut value, lease| {
                let entry =
                    StoredEntry::decode(&mut value).map_err(|e| StorageError::Generic(e.into()))?;
                Ok((
                    hydrate_with_reader(&mut BlobReader::from(storage.partition_db()), entry.0)?,
                    lease,
                ))
            },
        )
        .await
    }
}

impl PartitionStoreSnapshot<'_> {
    /// Budget-gated journal stream.
    ///
    /// Each entry's raw byte size is peeked **before** deserialization. A
    /// [`LocalMemoryLease`] is acquired from `budget` for that size, and only
    /// then is the entry decoded. This prevents memory allocation when the
    /// budget is exhausted.
    ///
    /// `budget` may be lent to a storage worker until the stream is done or
    /// dropped: the stream keeps it borrowed, and claiming it again afterwards
    /// waits for it to come back.
    pub fn get_journal_v2_budgeted<'a>(
        &'a self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
        budget: ClaimedMemoryPool<'a>,
    ) -> Result<
        impl Stream<Item = std::result::Result<BudgetedJournalItem, BudgetedReadError>> + Send + 'a,
    > {
        self.assert_partition_key(&invocation_id)?;
        if journal_length == 0 {
            return Ok(stream::empty().left_stream());
        }
        let mut remaining = journal_length;
        // The scan outlives this borrow of the snapshot, so it reads blobs with its own reader.
        let reader = self.owned_reader();
        let scan = self.prepare_budgeted_scan(
            "get-journal-v2-budgeted",
            journal_physical_scan(&invocation_id),
            budget,
            |_key, value| budgeted_entry_cost(value),
            move |key, value, lease| {
                let row = decode_journal_entry_v2(key, value).and_then(|(index, entry)| {
                    let entry = hydrate_with_reader(&mut BlobReader::Snapshot(&reader), entry)?;
                    Ok((index, entry, lease))
                });
                remaining -= 1;
                // Stop at the last entry rather than stepping past it.
                if remaining == 0 {
                    TableScanIterationDecision::BreakWith(row)
                } else {
                    TableScanIterationDecision::Emit(row)
                }
            },
        );
        let rows = scan.run_cached_then_offload(MAX_INLINE_ROWS, self.executor().low_pri())?;
        Ok(rows.right_stream())
    }
}

/// Like [`hydrate_journal_entry`], reading the blob with `blob_reader`.
fn hydrate_with_reader(
    blob_reader: &mut BlobReader<'_>,
    entry: StoredUnhydratedRawEntry,
) -> Result<StoredRawEntry> {
    let entry = match entry.try_into_inline() {
        Ok(entry) => return Ok(entry),
        Err(entry) => entry,
    };
    let blob = entry.content_blob().expect("the content is in a blob");
    let content = blob_reader
        .get_payload(blob.id)?
        .ok_or(StorageError::DataIntegrityError)?;
    Ok(entry.hydrate_with(content))
}

impl WriteJournalTable for PartitionStoreTransaction<'_> {
    fn put_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        index: u32,
        entry: &StoredRawEntry,
        related_completion_ids: &[CompletionId],
    ) -> Result<Option<BlobId>> {
        self.assert_partition_key(invocation_id)?;
        let inline_entry_threshold = self.settings().inline_entry_threshold;
        put_journal_entry(
            self,
            invocation_id,
            index,
            entry,
            related_completion_ids,
            inline_entry_threshold,
        )
    }

    fn copy_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        index: u32,
        entry: StoredUnhydratedRawEntry,
        related_completion_ids: &[CompletionId],
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        let content_blob = entry.content_blob();
        put_stored_entry(self, invocation_id, index, entry, related_completion_ids)?;
        // Released when this journal is deleted
        if let Some(blob) = content_blob {
            self.increment_ref_count(blob.id, 1);
        }
        Ok(())
    }

    fn delete_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("delete-journal");
        delete_journal(self, invocation_id, journal_length)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::VecDeque;

    use bytes::Bytes;

    use restate_storage_api::StorageError;
    use restate_types::identifiers::{InvocationId, InvocationUuid, WithPartitionKey};

    use super::{
        JournalCompletionIdToCommandIndexKey, cleanup_orphaned_completion_id_index_entries_inner,
        write_journal_entry_key,
    };
    use crate::keys::EncodeTableKeyPrefix;

    fn journal_entry_key(invocation_id: &InvocationId, journal_index: u32) -> Bytes {
        write_journal_entry_key(invocation_id, journal_index)
            .serialize()
            .freeze()
    }

    fn completion_index_key(
        invocation_id: &InvocationId,
        completion_id: u32,
    ) -> JournalCompletionIdToCommandIndexKey {
        JournalCompletionIdToCommandIndexKey {
            partition_key: invocation_id.partition_key(),
            invocation_uuid: invocation_id.invocation_uuid(),
            completion_id,
        }
    }

    #[derive(Default)]
    struct CleanupTestStorage {
        lookups: Vec<(u64, InvocationUuid)>,
        deleted: Vec<JournalCompletionIdToCommandIndexKey>,
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

    #[test]
    fn orphan_cleanup_preserves_live_entries_and_deletes_orphans() {
        let live_invocation = InvocationId::from_parts(1, InvocationUuid::from_u128(1));
        let orphan_invocation = InvocationId::from_parts(1, InvocationUuid::from_u128(2));
        let live_entries = [
            completion_index_key(&live_invocation, 0),
            completion_index_key(&live_invocation, 1),
        ];
        let orphan_entries = [
            completion_index_key(&orphan_invocation, 0),
            completion_index_key(&orphan_invocation, 1),
        ];
        let mut entries = VecDeque::from([
            live_entries[0].clone(),
            live_entries[1].clone(),
            orphan_entries[0].clone(),
            orphan_entries[1].clone(),
        ]);
        let mut storage = CleanupTestStorage::default();

        let result = cleanup_orphaned_completion_id_index_entries_inner(
            &mut storage,
            || Ok(entries.pop_front()),
            || false,
            |storage, partition_key, invocation_uuid| {
                storage.lookups.push((partition_key, invocation_uuid));
                Ok(invocation_uuid == live_invocation.invocation_uuid())
            },
            |storage, key| {
                storage.deleted.push(key.clone());
                Ok(())
            },
        )
        .expect("cleanup succeeds");

        assert_eq!(result.scanned_entries, 4);
        assert_eq!(result.scanned_invocations, 2);
        assert_eq!(result.deleted_entries, 2);
        assert_eq!(result.affected_invocations, 1);
        assert!(!result.cancelled);
        assert_eq!(
            storage.lookups,
            vec![
                (
                    live_invocation.partition_key(),
                    live_invocation.invocation_uuid()
                ),
                (
                    orphan_invocation.partition_key(),
                    orphan_invocation.invocation_uuid()
                ),
            ]
        );
        assert_eq!(storage.deleted, orphan_entries);
    }

    #[test]
    fn orphan_cleanup_fails_closed_and_reports_cancellation() {
        let first_invocation = InvocationId::from_parts(1, InvocationUuid::from_u128(1));
        let second_invocation = InvocationId::from_parts(1, InvocationUuid::from_u128(2));

        let mut point_read_entries = VecDeque::from([completion_index_key(&first_invocation, 0)]);
        let mut storage = CleanupTestStorage::default();
        let point_read_result = cleanup_orphaned_completion_id_index_entries_inner(
            &mut storage,
            || Ok(point_read_entries.pop_front()),
            || false,
            |_, _, _| Err(StorageError::Generic(anyhow::anyhow!("point read failed"))),
            |storage, key| {
                storage.deleted.push(key.clone());
                Ok(())
            },
        );
        assert!(point_read_result.is_err());
        assert!(storage.deleted.is_empty());

        let mut scan_results = VecDeque::from([
            Ok(Some(completion_index_key(&first_invocation, 0))),
            Err(StorageError::Generic(anyhow::anyhow!("scan failed"))),
        ]);
        let mut storage = CleanupTestStorage::default();
        let scan_result = cleanup_orphaned_completion_id_index_entries_inner(
            &mut storage,
            || scan_results.pop_front().unwrap_or(Ok(None)),
            || false,
            |_, _, _| Ok(true),
            |storage, key| {
                storage.deleted.push(key.clone());
                Ok(())
            },
        );
        assert!(scan_result.is_err());
        assert!(storage.deleted.is_empty());

        let mut cancellation_entries = VecDeque::from([
            completion_index_key(&first_invocation, 0),
            completion_index_key(&first_invocation, 1),
            completion_index_key(&second_invocation, 0),
        ]);
        let cancellation_checks = Cell::new(0);
        let mut storage = CleanupTestStorage::default();
        let cancellation_result = cleanup_orphaned_completion_id_index_entries_inner(
            &mut storage,
            || Ok(cancellation_entries.pop_front()),
            || {
                let checks = cancellation_checks.get();
                cancellation_checks.set(checks + 1);
                checks == 1
            },
            |_, _, _| Ok(false),
            |storage, key| {
                storage.deleted.push(key.clone());
                Ok(())
            },
        )
        .expect("cancellation returns the partial cleanup result");

        assert_eq!(cancellation_result.scanned_entries, 2);
        assert_eq!(cancellation_result.scanned_invocations, 1);
        assert_eq!(cancellation_result.deleted_entries, 2);
        assert_eq!(cancellation_result.affected_invocations, 1);
        assert!(cancellation_result.cancelled);
    }
}
