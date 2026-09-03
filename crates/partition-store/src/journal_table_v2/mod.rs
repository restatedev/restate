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
use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures_util::stream;
use rocksdb::{DBAccess, DBRawIteratorWithThreadMode, WriteBatch};

use restate_memory::{LocalMemoryLease, LocalMemoryPool};
use restate_rocksdb::{IoMode, Priority, RocksDbReadPerfGuard};
use restate_storage_api::journal_table_v2::{
    JournalEntryIndex, NotificationEntryIndex, ReadJournalTable, ScanJournalTable,
    ScanJournalTableRange, StoredEntry, WriteJournalTable,
};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_storage_api::{BudgetedReadError, Result, StorageError};
use restate_types::config::Configuration;
use restate_types::identifiers::{
    EntryIndex, InvocationId, InvocationUuid, JournalEntryId, PartitionKey, WithPartitionKey,
};
use restate_types::journal_v2::raw::{RawCommand, RawEntry};
use restate_types::journal_v2::{CompletionId, EntryMetadata, NotificationId};
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};

use crate::TableKind::Journal;
use crate::fsm_table::append_jc_orphan_cleanup_done_to_wb;
use crate::keys::{DecodeTableKey, EncodeTableKey, KeyKind, define_table_key};
use crate::owned_iter::OwnedIterator;
use crate::{
    PartitionDb, PartitionStore, PartitionStoreTransaction, StorageAccess, TableScan, break_on_err,
    convert_to_upper_bound,
};

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

/// Lazy iterator over journal V2 entries. Exposes [`peek_item`](Self::peek_item)
/// for zero-copy access to raw key/value slices and [`advance`](Self::advance)
/// to move forward. Also implements [`Iterator`] for convenience.
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

    /// Returns the raw `(key, value)` byte slices at the current iterator
    /// position without decoding or advancing. Returns `None` when exhausted.
    pub fn peek_item(&self) -> Option<Result<(&[u8], &[u8])>> {
        if self.remaining == 0 {
            return None;
        }
        match self.iter.item() {
            Some((k, v)) => Some(Ok((k, v))),
            None => self
                .iter
                .status()
                .err()
                .map(|err| Err(StorageError::Generic(err.into()))),
        }
    }

    /// Advances the iterator to the next entry.
    pub fn advance(&mut self) {
        self.iter.next();
        self.remaining -= 1;
    }
}

/// Decodes a V2 journal key/value pair from raw byte slices.
fn decode_journal_entry_v2(k: &[u8], v: &[u8]) -> Result<(EntryIndex, StoredRawEntry)> {
    let mut k = k;
    let mut v = v;
    let index = JournalKey::deserialize_from(&mut k)?.journal_index;
    let entry = StoredEntry::decode(&mut v).map_err(|e| StorageError::Generic(e.into()))?;
    Ok((index, entry.0))
}

impl<DB: DBAccess> Iterator for JournalEntryIter<'_, DB> {
    type Item = Result<(EntryIndex, StoredRawEntry)>;

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
            &NotificationEntryIndex {
                entry_index: journal_index,
                result_variant: notification.result_variant(),
            },
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

    storage.put_kv_proto_owned(
        write_journal_entry_key(invocation_id, journal_index),
        StoredEntry(journal_entry.clone()),
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

/// Budget-gated point read with a unified reserve-read-adjust loop (V2 journal).
///
/// See the V1 counterpart in `journal_table/mod.rs` for the full design
/// rationale. The only difference is the decode step: V2 entries go through
/// `StoredEntry::decode` and are unwrapped to `StoredRawEntry`.
async fn get_journal_entry_budgeted<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_index: u32,
    budget: &mut LocalMemoryPool,
) -> std::result::Result<Option<(StoredRawEntry, LocalMemoryLease)>, BudgetedReadError> {
    let key = write_journal_entry_key(invocation_id, journal_index);

    // Serialize key once — reused for all reads.
    let buf = {
        let key_buf = storage.cleared_key_buffer_mut(key.serialized_length());
        key.serialize_to(key_buf);
        key_buf.split()
    };

    let mut lease = budget.empty_lease();

    loop {
        // Read raw value from RocksDB.
        // RocksDbPerfGuard is !Send and must not live across .await.
        let deficit = {
            let _x = RocksDbReadPerfGuard::new("get-journal-entry-budgeted");
            let Some(pinned) = storage.get(Journal, &buf)? else {
                return Ok(None);
            };

            let raw_size = pinned.as_ref().len();
            if raw_size <= lease.size() {
                // Lease already covers (or exceeds) the value — shrink and decode.
                lease.shrink(lease.size() - raw_size);
                let mut slice = pinned.as_ref();
                let entry = StoredEntry::decode(&mut slice)
                    .map_err(|e| BudgetedReadError::Storage(StorageError::Generic(e.into())))?;
                return Ok(Some((entry.0, lease)));
            }

            // Need more budget. Try synchronous top-up first.
            let deficit = raw_size - lease.size();
            if let Some(extra) = budget.try_reserve(deficit) {
                lease.merge(extra);
                let mut slice = pinned.as_ref();
                let entry = StoredEntry::decode(&mut slice)
                    .map_err(|e| BudgetedReadError::Storage(StorageError::Generic(e.into())))?;
                return Ok(Some((entry.0, lease)));
            }

            deficit
        };

        // Pinned slice dropped — safe to .await now.
        let extra = budget.reserve(deficit, lease.size()).await?;
        lease.merge(extra);
    }
}

fn get_journal<'a, S: StorageAccess>(
    storage: &'a S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<JournalEntryIter<'a, S::DBAccess<'a>>> {
    let _x = RocksDbReadPerfGuard::new("get-journal-iter-setup");
    let key = JournalKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());

    let iter = storage.iterator_from(TableScan::Prefix(key))?;

    Ok(JournalEntryIter::new(iter, journal_length))
}

fn delete_journal<S: StorageAccess>(
    storage: &mut S,
    invocation_id: &InvocationId,
    journal_length: EntryIndex,
) -> Result<()> {
    let _x = RocksDbReadPerfGuard::new("delete-journal");

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
    let notification_id_index = OwnedIterator::new(storage.iterator_from(TableScan::Prefix(
        notification_id_to_notification_index.clone(),
    ))?)
    .map(|item| {
        let (mut key, _) = item?;
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
    let completion_id_index = OwnedIterator::new(
        storage.iterator_from(TableScan::Prefix(completion_id_to_command_index.clone()))?,
    )
    .map(|item| {
        let (mut key, _) = item?;
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

/// How many entries of an invocation the orphan scan steps over before it seeks past the rest.
pub(crate) const INVOCATION_ENTRIES_BEFORE_SEEK: usize = 16;

/// Scans a bounded number of invocations for orphaned
/// `JournalCompletionIdToCommandIndex` (`jc`) entries.
///
/// A `jc` entry is orphaned if no corresponding `JournalKey` (`j2`) entries exist for that
/// invocation, meaning the journal has already been deleted. These orphans were caused by a
/// bug in `delete_journal` that used the wrong scan prefix when cleaning up `jc` entries.
///
/// Only a single invocation is inspected at a time, so memory usage is independent of the store
/// size. `is_cancelled` is checked once per invocation to bound how long the scan keeps running
/// after the cleanup has been stopped.
pub fn scan_orphaned_completion_id_index_entries(
    storage: &mut PartitionStore,
    resume_after: Option<InvocationId>,
    invocation_limit: usize,
    is_cancelled: impl Fn() -> bool,
) -> Result<OrphanCleanupScanChunk> {
    assert!(invocation_limit > 0);
    let _x = RocksDbReadPerfGuard::new("scan-orphaned-jc-entries");

    let scan_store = storage.clone();
    let scan = TableScan::ScanPartitionKeyRange::<JournalCompletionIdToCommandIndexKeyBuilder>(
        scan_store.partition_key_range(),
    );
    // todo makes an unnecessary seek if resume_after is Some
    let mut iter = scan_store.iterator_from(scan)?;

    // reused for the prefix comparisons and seeks below
    let mut key_buf = BytesMut::new();
    if let Some(resume_after) = resume_after {
        write_completion_id_index_upper_bound(&mut key_buf, resume_after);
        iter.seek(&key_buf);
    }

    let mut candidates = Vec::new();
    let mut scanned_invocations = 0;
    let mut next = ScanCursor::Complete;

    while scanned_invocations < invocation_limit {
        if is_cancelled() {
            next = ScanCursor::Cancelled;
            break;
        }

        let Some((mut key_bytes, _)) = iter.item() else {
            iter.status()
                .map_err(|err| StorageError::Generic(err.into()))?;
            break;
        };

        let jc_key = JournalCompletionIdToCommandIndexKey::deserialize_from(&mut key_bytes)?;
        let invocation_id = InvocationId::from_parts(jc_key.partition_key, jc_key.invocation_uuid);
        if !has_journal_entry_zero(storage, jc_key.partition_key, jc_key.invocation_uuid)? {
            candidates.push(invocation_id);
        }
        scanned_invocations += 1;
        next = ScanCursor::ResumeAfter(invocation_id);

        // Skip the remaining entries of this invocation. This iterator uses total order seek, so
        // a seek has to reposition in every memtable and level without the help of the prefix
        // bloom filters, which is a lot more work than advancing within the current block. Step
        // over short invocations and only pay for the seek if there are many entries left.
        write_completion_id_index_prefix(&mut key_buf, invocation_id);
        let mut steps = 0;
        loop {
            iter.next();
            let Some((key_bytes, _)) = iter.item() else {
                iter.status()
                    .map_err(|err| StorageError::Generic(err.into()))?;
                break;
            };
            if !key_bytes.starts_with(&key_buf) {
                break;
            }
            steps += 1;
            if steps == INVOCATION_ENTRIES_BEFORE_SEEK {
                write_completion_id_index_upper_bound(&mut key_buf, invocation_id);
                iter.seek(&key_buf);
                break;
            }
        }
    }

    Ok(OrphanCleanupScanChunk {
        candidates,
        scanned_invocations,
        next,
    })
}

pub struct OrphanCleanupScanChunk {
    /// Invocations whose journal is gone but which still have `jc` entries.
    pub candidates: Vec<InvocationId>,
    pub scanned_invocations: usize,
    /// Where the next chunk has to continue.
    pub next: ScanCursor,
}

/// Position at which the scan stopped.
pub enum ScanCursor {
    /// Pass this back as `resume_after` to continue with the next chunk.
    ResumeAfter(InvocationId),
    /// The whole index has been scanned.
    Complete,
    /// The scan stopped early because it was cancelled.
    Cancelled,
}

pub struct OrphanCleanupApplyResult {
    pub deleted_invocations: usize,
    pub recreated_invocations: usize,
}

/// Deletes the `jc` entries of the given candidates.
///
/// Journal ownership is rechecked for every candidate, so invocations whose journal was recreated
/// between the scan and now are skipped instead of losing their index entries. Must be called
/// from the partition processor which owns the store, so that no journal can be written
/// concurrently to this recheck.
pub async fn apply_orphaned_completion_id_index_cleanup(
    storage: &mut PartitionStore,
    candidates: &[InvocationId],
) -> Result<OrphanCleanupApplyResult> {
    let partition_db = storage.partition_db().clone();
    let cf_handle = partition_db.cf_handle().clone();
    let mut wb = WriteBatch::default();
    let mut deleted_invocations = 0;
    let mut recreated_invocations = 0;

    for invocation_id in candidates {
        if has_journal_entry_zero(
            storage,
            invocation_id.partition_key(),
            invocation_id.invocation_uuid(),
        )? {
            recreated_invocations += 1;
            continue;
        }

        let (start, end) = completion_id_index_invocation_range(*invocation_id);
        wb.delete_range_cf(&cf_handle, start, end);
        deleted_invocations += 1;
    }

    if !wb.is_empty() {
        commit_orphan_cleanup_write_batch(&partition_db, wb).await?;
    }

    Ok(OrphanCleanupApplyResult {
        deleted_invocations,
        recreated_invocations,
    })
}

/// Persists that the one-time cleanup of orphaned `jc` entries has completed.
///
/// This deliberately goes through the same write batch path as
/// [`apply_orphaned_completion_id_index_cleanup`] instead of a plain put. Both writes disable the
/// WAL and every batch is awaited before the next one is issued, so RocksDB's FIFO memtable flush
/// order guarantees that this marker can only become durable after all preceding range deletes
/// are. A WAL-enabled put could be replayed after a crash which lost the deletes, and the cleanup
/// would never run again.
pub async fn mark_orphaned_completion_id_index_cleanup_done(
    storage: &PartitionStore,
) -> Result<()> {
    let partition_db = storage.partition_db();
    let mut wb = WriteBatch::default();
    append_jc_orphan_cleanup_done_to_wb(partition_db.cf_handle(), &mut wb, storage.partition_id())?;
    commit_orphan_cleanup_write_batch(partition_db, wb).await
}

async fn commit_orphan_cleanup_write_batch(
    partition_db: &PartitionDb,
    wb: WriteBatch,
) -> Result<()> {
    // Written from the partition processor's main loop, so it must not block on IO. Handled the
    // same way as `PartitionStoreTransaction::commit`: try to write inline and move the write to
    // the background storage pool if it would stall. The partition processor waits for the write
    // either way, hence the high priority; the cleanup is throttled by how many invocations it
    // hands over per tick, not by the pool it runs on.
    let io_mode = if Configuration::pinned()
        .worker
        .storage
        .always_commit_in_background
    {
        IoMode::AlwaysBackground
    } else {
        IoMode::Default
    };
    let mut opts = rocksdb::WriteOptions::default();
    // We disable WAL since bifrost is our durable distributed log.
    opts.disable_wal(true);
    partition_db
        .rocksdb()
        .write_batch("jc-orphan-cleanup", Priority::High, io_mode, opts, wb)
        .await
        .map_err(|error| StorageError::Generic(error.into()))?;
    Ok(())
}

/// Writes the key prefix shared by all `jc` entries of the given invocation into `buf`.
fn write_completion_id_index_prefix(buf: &mut BytesMut, invocation_id: InvocationId) {
    // fully qualified because `EncodeTableKey` provides the same methods for complete keys
    use crate::keys::EncodeTableKeyPrefix as Prefix;

    let prefix = JournalCompletionIdToCommandIndexKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    buf.clear();
    buf.reserve(Prefix::serialized_length(&prefix));
    Prefix::serialize_to(&prefix, buf);
}

/// Key range covering all `jc` entries of the given invocation.
fn completion_id_index_invocation_range(invocation_id: InvocationId) -> (Bytes, Bytes) {
    let mut start = BytesMut::new();
    write_completion_id_index_prefix(&mut start, invocation_id);
    let mut end = start.clone();
    assert!(convert_to_upper_bound(&mut end));
    (start.freeze(), end.freeze())
}

/// Writes the first key sorting after all `jc` entries of the given invocation into `buf`.
fn write_completion_id_index_upper_bound(buf: &mut BytesMut, invocation_id: InvocationId) {
    write_completion_id_index_prefix(buf, invocation_id);
    assert!(convert_to_upper_bound(buf));
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

fn get_notifications_index<S: StorageAccess>(
    storage: &mut S,
    invocation_id: InvocationId,
) -> Result<HashMap<NotificationId, NotificationEntryIndex>> {
    let key = JournalNotificationIdToNotificationIndexKey::builder()
        .partition_key(invocation_id.partition_key())
        .invocation_uuid(invocation_id.invocation_uuid());
    let iter = storage.iterator_from(TableScan::Prefix(key))?;
    OwnedIterator::new(iter)
        .map(|item| {
            let (mut key, mut value) = item?;
            let journal_key =
                JournalNotificationIdToNotificationIndexKey::deserialize_from(&mut key)?;
            let index = NotificationEntryIndex::decode(&mut value)
                .map_err(|err| StorageError::Conversion(err.into()))?;

            let (_, _, notification_id) = journal_key.split();

            Ok((notification_id, index))
        })
        .collect()
}

fn get_command_by_completion_id<S: StorageAccess>(
    storage: &mut S,
    invocation_id: InvocationId,
    completion_id: CompletionId,
) -> Result<Option<(StoredRawEntryHeader, RawCommand)>> {
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
    ) -> Result<Option<StoredRawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("get-journal-entry");
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
    ) -> Result<HashMap<NotificationId, NotificationEntryIndex>> {
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

    async fn get_journal_entry_budgeted(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
        budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(StoredRawEntry, LocalMemoryLease)>, BudgetedReadError> {
        self.assert_partition_key(&invocation_id)?;
        get_journal_entry_budgeted(self, &invocation_id, journal_index, budget).await
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
    ) -> Result<Option<StoredRawEntry>> {
        self.assert_partition_key(&invocation_id)?;
        let _x = RocksDbReadPerfGuard::new("get-journal-entry");
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
    ) -> Result<HashMap<NotificationId, NotificationEntryIndex>> {
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

    async fn get_journal_entry_budgeted(
        &mut self,
        invocation_id: InvocationId,
        journal_index: u32,
        budget: &mut LocalMemoryPool,
    ) -> std::result::Result<Option<(StoredRawEntry, LocalMemoryLease)>, BudgetedReadError> {
        self.assert_partition_key(&invocation_id)?;
        get_journal_entry_budgeted(self, &invocation_id, journal_index, budget).await
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

/// Wraps a [`JournalEntryIter`] into an async [`Stream`] that acquires a memory
/// lease from `budget` **before** decoding each entry.
///
/// See the V1 counterpart in `journal_table/mod.rs` for the full design
/// rationale — identical fast/slow path with `try_reserve` + `reserve`.
fn budgeted_journal_v2_stream<'a, DB: DBAccess + Send>(
    iter: JournalEntryIter<'a, DB>,
    budget: &'a mut LocalMemoryPool,
) -> impl Stream<
    Item = std::result::Result<(EntryIndex, StoredRawEntry, LocalMemoryLease), BudgetedReadError>,
> + Send
+ 'a {
    futures::stream::unfold((iter, budget), |(mut iter, budget)| async move {
        let mut lease = budget.empty_lease();
        loop {
            let deficit = {
                let (k, v) = match iter.peek_item() {
                    Some(Ok(item)) => item,
                    Some(Err(e)) => return Some((Err(e.into()), (iter, budget))),
                    None => return None,
                };

                let raw_size = v.len();
                if raw_size <= lease.size() {
                    lease.shrink(lease.size() - raw_size);
                    match decode_journal_entry_v2(k, v) {
                        Ok((idx, entry)) => {
                            iter.advance();
                            return Some((Ok((idx, entry, lease)), (iter, budget)));
                        }
                        Err(e) => return Some((Err(e.into()), (iter, budget))),
                    }
                }

                let deficit = raw_size - lease.size();
                if let Some(extra) = budget.try_reserve(deficit) {
                    lease.merge(extra);
                    match decode_journal_entry_v2(k, v) {
                        Ok((idx, entry)) => {
                            iter.advance();
                            return Some((Ok((idx, entry, lease)), (iter, budget)));
                        }
                        Err(e) => return Some((Err(e.into()), (iter, budget))),
                    }
                }

                deficit
            };

            let extra = match budget.reserve(deficit, lease.size()).await {
                Ok(l) => l,
                Err(e) => return Some((Err(e.into()), (iter, budget))),
            };
            lease.merge(extra);
        }
    })
}

impl WriteJournalTable for PartitionStoreTransaction<'_> {
    fn put_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        index: u32,
        entry: &StoredRawEntry,
        related_completion_ids: &[CompletionId],
    ) -> Result<()> {
        self.assert_partition_key(invocation_id)?;
        put_journal_entry(self, invocation_id, index, entry, related_completion_ids)
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
    use bytes::Bytes;

    use restate_types::identifiers::{InvocationId, InvocationUuid};

    use super::{completion_id_index_invocation_range, write_journal_entry_key};
    use crate::keys::EncodeTableKeyPrefix;

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

    #[test]
    fn completion_index_invocation_ranges_do_not_overlap() {
        let first = InvocationId::from_parts(1, InvocationUuid::from_u128(1));
        let second = InvocationId::from_parts(1, InvocationUuid::from_u128(2));
        let last_in_partition = InvocationId::from_parts(1, InvocationUuid::from_u128(u128::MAX));
        let first_in_next_partition = InvocationId::from_parts(2, InvocationUuid::from_u128(1));
        let (first_start, first_end) = completion_id_index_invocation_range(first);
        let (second_start, second_end) = completion_id_index_invocation_range(second);
        let (_, last_in_partition_end) = completion_id_index_invocation_range(last_in_partition);
        let (first_in_next_partition_start, _) =
            completion_id_index_invocation_range(first_in_next_partition);

        assert!(first_start < first_end);
        assert!(first_end <= second_start);
        assert!(second_start < second_end);
        assert!(last_in_partition_end < first_in_next_partition_start);
    }
}
