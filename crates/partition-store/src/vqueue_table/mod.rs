// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod entry;
mod entry_index;
mod inbox;
mod inbox_reader;
mod indexed_read;
mod input;
mod key_codec;
mod meta_binding;
mod meta_cursor;
mod metadata;
mod read_cursor;
mod reader;
mod running_reader;
mod stage_index;
mod stage_reads;
mod stage_rows;
mod stats;
#[cfg(test)]
mod touched_by_tests;

use std::num::NonZeroUsize;
use std::ops::ControlFlow;

pub use entry::{EntryStatusKey, EntryStatusKeyRef};
pub(crate) use entry_index::{
    ENTRY_BY_CREATED, ENTRY_BY_STATUS_CREATED, ENTRY_BY_STATUS_TRANSITIONED, ENTRY_INDEXES,
};
pub use entry_index::{
    EntryByCreated, EntryByCreatedKey, EntryByStatusCreated, EntryByStatusCreatedKey,
    EntryByStatusTransitioned, EntryByStatusTransitionedKey, EntryTargetValue, EntryTargetValueRef,
    hour_bucket, status_indexes_hold,
};
pub use inbox::InboxKey;
pub use indexed_read::{LanesRead, StatusCreatedRead, StatusTransitionedRead};
pub use input::InputPayloadKey;
pub use meta_binding::{VQueueMetaBinding, VQueueMetaTable};
pub use meta_cursor::{VQueueMetaCursor, VQueueMetaRecords};
pub use metadata::*;
pub use read_cursor::{
    EntryKeyCursor, EntryKeyRecords, EntryKeyRow, EntryKeyScan, EntryStatusCursor, EntryStatusKeys,
    EntryStatusRecords, EntryStatusScan,
};
pub use stage_index::{
    FinishedIndexBinding, FinishedIndexTable, InboxIndexBinding, InboxIndexTable,
    PausedIndexBinding, PausedIndexTable, RunningIndexBinding, RunningIndexTable,
    SuspendedIndexBinding, SuspendedIndexTable,
};
pub use stage_reads::{StagePrefilter, StageReadCounters, StageReads};
pub use stage_rows::{
    StageRowCounters, StageRowRecords, StageRowRef, StageRowScan, StageRowsCursor,
};
pub(crate) use stats::{DEPLOYMENT_STATS, SERVICE_STATS, STATUS_GAUGES, VIRTUAL_OBJECT_STATS};

use anyhow::Context;
use bilrost::{BorrowedMessage, Message, OwnedMessage};
use bytes::BytesMut;
use rocksdb::{ReadOptions, WriteBatch, WriteOptions};
use tracing::error;

use restate_rocksdb::{CfName, RocksDbReadPerfGuard};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_api::vqueue_table::metadata::{VQueueMeta, VQueueMetaRef};
use restate_storage_api::vqueue_table::{
    EntryChange, EntryContext, EntryKey, EntryStateRef, EntryStatusHeader, EntryValue,
    ReadVQueueTable, ScanVQueueTable, Stage, StoredEntryRef, VQueueDisposition, WriteVQueueTable,
};
use restate_storage_api::vqueue_table::{RawStatusHeader, RawStatusHeaderRef};
use restate_types::config::Configuration;
use restate_types::identifiers::BaseEntryId;
use restate_types::vqueues::{CanonicalEntryId, EntryId, EntryKind, Seq, VQueueId};

use self::entry::entry_status_header_from_raw;
use self::inbox::InboxKeyRef;
use self::key_codec::HasLock;
use crate::keys::{
    DecodeTableKey, EncodeTableKey, EncodeTableKeyPrefix, FixedLengthTableKey, KeyDecode, KeyKind,
};
use crate::scan::TableScan;
use crate::vqueue_table::input::InputPayloadKeyRef;
use crate::{
    PartitionDb, PartitionStoreTransaction, ReadSnapshot, Result, StorageAccess, StorageRead,
};

impl ScanVQueueTable for PartitionDb {
    fn scan_active_vqueues(
        &self,
        mut on_item: impl FnMut(VQueueId, VQueueMeta),
    ) -> Result<(), StorageError> {
        const BATCH_SIZE: usize = 1000;
        // read the active vqueues list
        let mut iterator_opts = ReadOptions::default();
        // NOTE: Cannot use key prefixes because the prefix length doesn't match our start
        // key.
        // iterator_opts.set_prefix_same_as_start(true);
        iterator_opts.set_async_io(true);
        // Do not remove this!
        iterator_opts.set_total_order_seek(true);
        // this is not the place to be concerned about corruption, we favor speed
        // over safety for this particular use-case.
        iterator_opts.set_verify_checksums(false);

        // We know how big the prefix is
        let mut key_buf = [0u8; ActiveKey::by_partition_prefix_len()];
        {
            // Serialize prefix bytes
            // so we go directly to custom serialization because ActiveKey calls for
            // the entire VQueueId, but we only want to supply the partition-key portion
            // of it.
            let mut key_buf = key_buf.as_mut();
            ActiveKey::serialize_key_kind(&mut key_buf);
            crate::keys::serialize(&self.partition().key_range.start(), &mut key_buf);
        }

        // setting iterator bounds.
        iterator_opts.set_iterate_lower_bound(key_buf);

        // the end prefix is one byte beyond the max partition key on this key kind prefix.
        {
            // Serialize prefix bytes
            // so we go directly to custom serialization because ActiveKey calls for
            // the entire VQueueId, but we only want to supply the partition-key portion
            // of it.
            let mut key_buf = key_buf.as_mut();
            ActiveKey::serialize_key_kind(&mut key_buf);
            crate::keys::serialize(&self.partition().key_range.end(), &mut key_buf);
        }
        let _success = crate::convert_to_upper_bound(&mut key_buf);
        debug_assert!(_success);
        iterator_opts.set_iterate_upper_bound(key_buf);

        let rocksdb = self.rocksdb().inner().as_raw_db();

        let cf = self.cf_handle_for_key_kind(<ActiveKey as EncodeTableKey>::KEY_KIND);

        let mut it = rocksdb.raw_iterator_cf_opt(cf, iterator_opts);

        it.seek_to_first();

        let mut meta_keys_bytes_buf =
            BytesMut::with_capacity(BATCH_SIZE * MetaKey::serialized_length_fixed());
        // read items, and every 1000 we batch them up and perform a multi-get

        let mut meta_keys_bytes: Vec<BytesMut> = Vec::with_capacity(BATCH_SIZE);
        let mut queue_ids: Vec<VQueueId> = Vec::with_capacity(BATCH_SIZE);
        let mut end = false;

        // Escape hatch for orphaned active markers (meta is missing), e.g. caused by a
        // duplicated put that survived a single delete. If more than one put is present, an
        // older put may resurface later and requires enabling the escape hatch again.
        let remove_orphans = Configuration::pinned()
            .common
            .experimental
            .is_vqueue_orphaned_active_marker_removal_enabled();
        let mut orphans = WriteBatch::default();

        while !end {
            match it.key() {
                Some(mut key) => {
                    let meta_key_bytes = {
                        let meta_key = MetaKey::from(ActiveKey::deserialize_from(&mut key)?);
                        EncodeTableKey::serialize_to(&meta_key, &mut meta_keys_bytes_buf);
                        queue_ids.push(VQueueId::from(meta_key));
                        meta_keys_bytes_buf.split()
                    };
                    meta_keys_bytes.push(meta_key_bytes);
                    it.next();
                }
                None => {
                    it.status()
                        .context("failed to scan active vqueues")
                        .map_err(StorageError::Generic)?;
                    end = true;
                }
            }

            // every 1000, we perform a multi-get to fetch the vqueues from rocksdb.
            if meta_keys_bytes.len() == BATCH_SIZE || (end && !meta_keys_bytes.is_empty()) {
                let mut readopts = ReadOptions::default();
                readopts.set_async_io(true);
                let results =
                    rocksdb.batched_multi_get_cf_opt(cf, &meta_keys_bytes, true, &readopts);
                meta_keys_bytes.clear();

                for (queue_id, result) in queue_ids.drain(..).zip(results) {
                    let result = result.context("failed to get active vqueue in multi-get")?;
                    let Some(meta) = result else {
                        if remove_orphans {
                            error!(
                                "Removing orphaned active vqueue {queue_id:?} which was not found in vqueue_meta index"
                            );
                            let mut key_buffer = [0u8; ActiveKey::serialized_length_fixed()];
                            ActiveKeyRef::builder()
                                .qid(&queue_id)
                                .serialize_to(&mut key_buffer.as_mut());
                            orphans.single_delete_cf(cf, key_buffer);
                            continue;
                        }
                        // data integrity error, active vqueues must be present in meta
                        error!(
                            "Active vqueues must be present in meta. Active vqueue {queue_id:?} was not found in vqueue_meta index during the batched read"
                        );
                        return Err(StorageError::DataIntegrityError);
                    };
                    let meta = VQueueMeta::decode(&mut meta.as_ref())?;
                    on_item(queue_id, meta);
                }
            }
        }

        if !orphans.is_empty() {
            // Like regular partition store commits, skip the WAL. The flush makes the removal
            // durable so that the escape hatch can be disabled again.
            let mut write_options = WriteOptions::default();
            write_options.disable_wal(true);
            let rocksdb = self.rocksdb().inner();
            rocksdb
                .write_batch(&orphans, &write_options)
                .context("failed to remove orphaned active vqueues")?;
            rocksdb
                .flush_memtables(&[CfName::from(self.partition().cf_name())], true)
                .context("failed to flush removal of orphaned active vqueues")?;
        }
        Ok(())
    }
}

// Helpers for internal management of vqueues
impl PartitionStoreTransaction<'_> {
    /// Adds a vqueue to the list of active vqueues
    ///
    /// A vqueue is considered active when it's of interest to the scheduler.
    ///
    /// The scheduler cares about vqueues that have entries that are already running or that are waiting
    /// to run. With some special rules to consider when the queue is paused. When the vqueue is
    /// paused, the scheduler will only be interested in its "running" entries and not in its
    /// waiting entries. Therefore, it will remain to be "active" as long as it has running
    /// entries. Once running entries are moved to waiting or completed, the vqueue is be
    /// considered dormant until it's unpaused.
    ///
    /// A vqueue that is "not" active does not mean it's "empty". It could be paused or
    /// only contains entries in parked or completed states. As such, it's not considered
    /// by the scheduler and it's considered "dormant".
    fn mark_vqueue_as_active(&mut self, qid: &VQueueId) {
        let mut key_buffer = [0u8; ActiveKey::serialized_length_fixed()];
        ActiveKeyRef::builder()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        self.raw_put_cf(KeyKind::VQueueActive, key_buffer, []);
    }

    /// Removes the vqueue from the list of active vqueues
    ///
    /// A dormant vqueue is not necessarily `empty`. It's a vqueue _might_ have items (or not)
    /// in parked or completed states, or it might have waiting items in its inbox but the
    /// vqueue is paused and would not be visible to the scheduler.
    fn mark_vqueue_as_dormant(&mut self, qid: &restate_types::vqueues::VQueueId) {
        let mut key_buffer = [0u8; ActiveKey::serialized_length_fixed()];
        ActiveKeyRef::builder()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        self.raw_single_delete_cf(KeyKind::VQueueActive, key_buffer);
    }
}

impl WriteVQueueTable for PartitionStoreTransaction<'_> {
    fn create_vqueue(&mut self, qid: &VQueueId, meta: &VQueueMeta) {
        let key_buffer = MetaKey::from(qid).to_bytes();
        let value_buf = {
            let value_buf = self.cleared_value_buffer_mut(meta.encoded_len());
            // unwrap is safe because we know the buffer is big enough.
            meta.encode(value_buf).unwrap();
            value_buf.split()
        };

        self.raw_put_cf(KeyKind::VQueueMeta, key_buffer, value_buf);
    }

    fn update_vqueue(
        &mut self,
        qid: &VQueueId,
        meta: &mut VQueueMeta,
        update: &restate_storage_api::vqueue_table::metadata::Update,
    ) -> VQueueDisposition {
        // Vqueues that was touched more than 1 hour ago will always be fully written.
        const HOUR_MS: u64 = const { 60 * 60 * 1000 };

        // Otherwise, full writes are sampled with the configured probability
        // rather than merged.
        let full_write_threshold =
            (self.settings().vqueue_meta_full_write_probability * u64::MAX as f64) as u64;

        // Mutate the VQueue metadata
        let was_active_before = meta.is_active();
        let should_write_full = restate_util_random::pseudo_random() < full_write_threshold
            || update.ts.saturating_sub_ms(meta.stats().last_modified_at()) > HOUR_MS;
        meta.apply_update(update);
        let is_active_now = meta.is_active();

        // Update active queue index
        match (was_active_before, is_active_now) {
            (false, true) => {
                self.mark_vqueue_as_active(qid);
            }
            (true, false) => {
                self.mark_vqueue_as_dormant(qid);
            }
            (_, _) => {}
        }

        if meta.is_obsolete() {
            self.delete_vqueue(qid);
            VQueueDisposition::Purged
        } else {
            let key_buffer = MetaKey::from(qid).to_bytes();
            // Rarely modified vqueues will not otherwise get full writes. Collapse
            // their merge chain when the metadata has not been touched for an hour.
            if should_write_full {
                // full write
                let value_buf = {
                    let value_buf = self.cleared_value_buffer_mut(meta.encoded_len());
                    // unwrap is safe because we know the buffer is big enough.
                    meta.encode(value_buf).unwrap();
                    value_buf.split()
                };
                self.raw_put_cf(KeyKind::VQueueMeta, key_buffer, value_buf);
            } else {
                self.raw_merge_cf(
                    KeyKind::VQueueMeta,
                    key_buffer,
                    update.encode_contiguous().into_vec(),
                );
            }

            VQueueDisposition::Retained
        }
    }

    fn delete_vqueue(&mut self, qid: &VQueueId) {
        // Cannot use single delete: the meta key is written once with put and
        // updated many times with merge afterwards.
        self.raw_delete_cf(KeyKind::VQueueMeta, MetaKey::from(qid).to_bytes());
    }

    fn put_vqueue_inbox(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
        value: &EntryValue,
    ) {
        let key_buffer = inbox::encode_stage_key(stage, qid, key);

        let value_buf = {
            let value_buf = self.cleared_value_buffer_mut(value.encoded_len());
            // unwrap is safe because we know the buffer is big enough.
            value.encode(value_buf).unwrap();
            value_buf.split()
        };

        self.raw_put_cf(inbox::stage_key_kind(stage), key_buffer, value_buf)
    }

    fn get_vqueue_inbox(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
    ) -> Result<Option<EntryValue>> {
        let key_buffer = inbox::encode_stage_key(stage, qid, key);

        Ok(match self.get(inbox::stage_key_kind(stage), key_buffer)? {
            Some(raw_value) => Some(EntryValue::decode(&mut raw_value.as_ref())?),
            None => None,
        })
    }

    fn delete_vqueue_inbox(&mut self, qid: &VQueueId, stage: Stage, key: &EntryKey) {
        self.raw_single_delete_cf(
            inbox::stage_key_kind(stage),
            inbox::encode_stage_key(stage, qid, key),
        );
    }

    fn create_vqueue_entry_status(&mut self, context: &EntryContext<'_>, after: EntryStateRef<'_>) {
        self.apply_entry_change(context, &EntryChange::Insert { after });
    }

    fn update_vqueue_entry_status(
        &mut self,
        context: &EntryContext<'_>,
        before: StoredEntryRef<'_>,
        after: EntryStateRef<'_>,
    ) {
        assert_eq!(
            before.state().entry_key.entry_id(),
            after.entry_key.entry_id(),
            "entry identity cannot change"
        );
        self.apply_entry_change(context, &EntryChange::Update { before, after });
    }

    fn delete_vqueue_entry_status(
        &mut self,
        context: &EntryContext<'_>,
        before: StoredEntryRef<'_>,
    ) {
        self.apply_entry_change(context, &EntryChange::Delete { before });
    }

    fn put_vqueue_input_payload<E>(
        &mut self,
        qid: &VQueueId,
        seq: impl Into<Seq>,
        id: &EntryId,
        item: E,
    ) where
        E: Message,
    {
        let seq = seq.into();
        let mut key_buffer = [0u8; InputPayloadKey::serialized_length_fixed()];
        InputPayloadKeyRef::builder()
            .qid(qid)
            .seq(&seq)
            .id(id)
            .serialize_to(&mut key_buffer.as_mut());

        let value_buffer = self.cleared_value_buffer_mut(item.encoded_len());

        item.encode(value_buffer)
            .expect("enough space to encode item");
        let value = value_buffer.split();

        self.raw_put_cf(KeyKind::VQueueInput, key_buffer, value);
    }

    fn delete_vqueue_input_payload(&mut self, qid: &VQueueId, id: &CanonicalEntryId) {
        let key_buf = {
            let seq = id.seq();
            let key = InputPayloadKeyRef::builder()
                .qid(qid)
                .seq(&seq)
                .id(id.as_entry_id());
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        self.raw_single_delete_cf(KeyKind::VQueueInput, key_buf);
    }
}

impl PartitionStoreTransaction<'_> {
    /// The single source-write boundary for entry lifecycle changes.
    fn apply_entry_change(&mut self, context: &EntryContext<'_>, change: &EntryChange<'_>) {
        let state = match change {
            EntryChange::Insert { after } | EntryChange::Update { after, .. } => after,
            EntryChange::Delete { before } => before.state(),
        };
        let id = state.base_entry_id(context);
        let mut key_buffer = [0u8; EntryStatusKey::serialized_length_fixed()];
        EntryStatusKeyRef::builder()
            .id(&id)
            .serialize_to(&mut key_buffer.as_mut());
        #[cfg(any(test, feature = "test-util"))]
        if let Some(before) = change.stored_before() {
            assert!(
                self.touched_by_is_current(key_buffer, before.touched_by()),
                "the before-image must carry the entry's current stored tokens"
            );
        }

        let active = self.published_registry();
        if let Some(after) = change.after() {
            // Every write adopts the entry into each maintained index.
            self.put_entry_status(key_buffer, context.qid, after, active.touched_by_on_write());
        } else {
            // The status key is overwritten on transitions, so SingleDelete is invalid.
            self.raw_delete_cf(KeyKind::VQueueEntryStatus, key_buffer);
        }

        active.apply(self, context, change);
    }

    /// Stores `state` as the status of the entry at `key`, listing the jobs in
    /// `touched_by`. Index maintenance is the caller's: a write applies its change
    /// to every maintained index ([`Self::apply_entry_change`]), and a backfill
    /// applies the entry to its own indexes only.
    pub(crate) fn put_entry_status(
        &mut self,
        key: [u8; EntryStatusKey::serialized_length_fixed()],
        qid: &VQueueId,
        state: &EntryStateRef<'_>,
        touched_by: &[u8],
    ) {
        let header = RawStatusHeaderRef {
            qid: qid.into(),
            stage: state.stage,
            has_lock: state.entry_key.has_lock(),
            next_run_at: state.entry_key.run_at(),
            seq: state.entry_key.seq(),
            metadata: state.metadata.into(),
            stats: state.stats.clone(),
            status: state.status,
            touched_by,
        };
        let header_len = header.encoded_len();
        let value_buf = self.cleared_value_buffer_mut(
            header_len + bilrost::encoding::encoded_len_varint(header_len as u64),
        );
        // The buffer has enough capacity for the length-delimited header.
        header.encode_length_delimited(value_buf).unwrap();
        let value_buf = value_buf.split();
        self.raw_put_cf(KeyKind::VQueueEntryStatus, key, value_buf);
    }
}

#[cfg(any(test, feature = "test-util"))]
impl PartitionStoreTransaction<'_> {
    /// Whether a before-image carries the tokens stored for its entry,
    /// including earlier writes in this transaction. A stale before-image would
    /// lose or invent tokens. Tests only: production adds no read per write.
    pub(super) fn touched_by_is_current(
        &self,
        key: [u8; EntryStatusKey::serialized_length_fixed()],
        touched_by: restate_storage_api::migration_job::MigrationJobIds<'_>,
    ) -> bool {
        let stored = self
            .get(<EntryStatusKey as EncodeTableKey>::KEY_KIND, key)
            .expect("entry status is readable")
            .map(|value| RawStatusHeader::decode_stored(value.as_ref()).expect("valid header"));
        stored
            .as_ref()
            .map_or(&[][..], |header| header.touched_by.as_slice())
            == touched_by.as_bytes()
    }
}

/// Decodes a stored entry status, validating its tokens.
pub(crate) fn decode_entry_status(
    id: &BaseEntryId,
    value: &[u8],
) -> Result<restate_storage_api::vqueue_table::OwnedEntryStatusHeader, StorageError> {
    Ok(entry_status_header_from_raw(
        id,
        RawStatusHeader::decode_stored(value)?,
    ))
}

impl<'txn> ReadVQueueTable for PartitionStoreTransaction<'txn> {
    async fn get_vqueue(&self, qid: &VQueueId) -> Result<Option<VQueueMeta>, StorageError> {
        let _x = RocksDbReadPerfGuard::new("get-vqueue-meta");
        let mut key_buffer = [0u8; MetaKey::serialized_length_fixed()];
        MetaKeyRef::builder()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        let Some(raw_value) = self.get(<MetaKey as EncodeTableKey>::KEY_KIND, key_buffer)? else {
            return Ok(None);
        };

        Ok(Some(VQueueMeta::decode(&mut raw_value.as_ref())?))
    }

    async fn get_vqueue_entry_status(
        &self,
        id: &BaseEntryId,
    ) -> Result<Option<impl EntryStatusHeader + 'static + use<'txn>>> {
        let _x = RocksDbReadPerfGuard::new("get-vqueue-entry-status");
        let mut key_buffer = [0u8; EntryStatusKey::serialized_length_fixed()];
        EntryStatusKeyRef::builder()
            .id(id)
            .serialize_to(&mut key_buffer.as_mut());

        let Some(raw_value) = self.get(<EntryStatusKey as EncodeTableKey>::KEY_KIND, key_buffer)?
        else {
            return Ok(None);
        };

        let header = RawStatusHeader::decode_stored(raw_value.as_ref())?;

        Ok(Some(entry_status_header_from_raw(id, header)))
    }

    async fn find_inbox_state_mutation_key(
        &self,
        qid: &VQueueId,
        key: &EntryKey,
    ) -> Result<Option<EntryKey>> {
        let has_lock = HasLock::new(key.has_lock());
        let run_at = key.run_at();
        let seq = key.seq();
        let prefix = InboxKeyRef::builder()
            .qid(qid)
            .has_lock(&has_lock)
            .run_at(&run_at)
            .seq(&seq);

        // The seq of a state mutation is the lsn of its command. Other inbox entries with this seq
        // can't exist: invocations enqueued by the state machine use the lsn of their own command,
        // and migrated invocations use seq numbers from before the migration, which are smaller
        // than any lsn after it.
        let iterator = self.iterator_from(TableScan::Prefix(prefix))?;
        let Some(raw_key) = iterator.key() else {
            iterator
                .status()
                .map_err(|err| StorageError::Generic(err.into()))?;
            return Ok(None);
        };
        let entry_key =
            <EntryKey as KeyDecode>::decode(&mut &raw_key[InboxKey::offset_of_entry_key()..])?;

        Ok((entry_key.kind() == EntryKind::StateMutation).then_some(entry_key))
    }

    async fn get_vqueue_input_payload<E>(
        &self,
        qid: &VQueueId,
        seq: impl Into<Seq>,
        id: &EntryId,
    ) -> Result<Option<E>>
    where
        E: OwnedMessage,
    {
        let mut key_buffer = [0u8; InputPayloadKey::serialized_length_fixed()];
        let seq = seq.into();
        InputPayloadKeyRef::builder()
            .qid(qid)
            .seq(&seq)
            .id(id)
            .serialize_to(&mut key_buffer.as_mut());

        let Some(raw_value) =
            self.get(<InputPayloadKey as EncodeTableKey>::KEY_KIND, key_buffer)?
        else {
            return Ok(None);
        };

        Ok(Some(E::decode(&mut raw_value.as_ref())?))
    }
}

impl ReadSnapshot {
    /// Reads queue metadata from the admitted snapshot on the caller's thread.
    pub fn map_vqueue_meta_blocking<R>(
        &self,
        qid: &VQueueId,
        f: impl FnOnce(&VQueueMetaRef<'_>) -> R,
    ) -> Result<Option<R>> {
        let mut key = [0u8; MetaKey::serialized_length_fixed()];
        MetaKeyRef::builder()
            .qid(qid)
            .serialize_to(&mut key.as_mut());
        self.check_active()?;
        let reader = self.storage_snapshot().reader();
        let Some(value) = reader
            .get_pinned(KeyKind::VQueueMeta, &key)
            .map_err(|error| StorageError::Generic(error.into()))?
        else {
            return Ok(None);
        };
        Ok(Some(f(&VQueueMetaRef::decode_borrowed(value.as_ref())?)))
    }

    /// Scans or looks up entry-status records on the caller's thread. All reads
    /// use this snapshot, including auxiliary lookups performed by the callback.
    pub fn for_each_vqueue_entry_status_blocking(
        &self,
        filter: ScanEntryIdFilter,
        mut emit: impl for<'a> FnMut(
            &'a BaseEntryId,
            &'a RawStatusHeaderRef<'a>,
        ) -> ControlFlow<Result<()>>,
    ) -> Result<()> {
        use restate_storage_query_api::scan::{CursorStep, ReadCursor};

        let mut cursor = self.entry_status_cursor(filter)?;
        loop {
            match cursor.advance()? {
                CursorStep::Done => return Ok(()),
                CursorStep::Skipped => {}
                CursorStep::Record => {
                    let (id, header) = cursor.current()?;
                    if let ControlFlow::Break(result) = emit(&id, &header) {
                        return result;
                    }
                }
            }
        }
    }
}

/// Maximum number of vqueue-metadata keys passed to one RocksDB multi-get call.
const VQUEUE_META_MULTI_GET_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(500).unwrap();

/// Maximum number of entry-status keys passed to one RocksDB multi-get call.
const ENTRY_STATUS_MULTI_GET_BATCH_SIZE: NonZeroUsize = NonZeroUsize::new(500).unwrap();
