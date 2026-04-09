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
mod inbox;
mod items;
mod metadata;
mod reader;
mod running_reader;
mod waiting_reader;

use std::ops::RangeInclusive;

pub use entry::{EntryStateHeader, EntryStateKey};
pub use inbox::{ActiveKey, InboxKey};
pub use items::ItemsKey;
pub use metadata::*;

use anyhow::Context;
use bilrost::{BorrowedMessage, Message, OwnedMessage};
use bytes::{Buf, BufMut, BytesMut};
use tracing::error;

use restate_rocksdb::Priority;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{
    VQueueMeta, VQueueMetaBorrowed, VQueueMetaUpdates,
};
use restate_storage_api::vqueue_table::{
    AsEntryState, AsEntryStateHeader, EntryId, EntryKey, EntryValue, ReadVQueueTable,
    ScanVQueueMetaTable, ScanVQueueTable, Stage, WriteVQueueTable,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueue::VQueueId;
use rocksdb::{DBRawIteratorWithThreadMode, ReadOptions};

use self::entry::OwnedEntryState;
use crate::keys::{
    DecodeTableKey, EncodeTableKey, EncodeTableKeyPrefix, KeyDecode, KeyEncode, KeyKind,
};
use crate::scan::TableScan;
use crate::{
    PartitionDb, PartitionStore, PartitionStoreTransaction, Result, StorageAccess, TableKind,
    break_on_err,
};

impl KeyEncode for VQueueId {
    #[inline]
    fn encode<B: BufMut>(&self, target: &mut B) {
        self.encode_raw_bytes(target);
    }

    #[inline]
    fn serialized_length(&self) -> usize {
        Self::serialized_length_fixed()
    }
}

impl KeyDecode for VQueueId {
    #[inline]
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        Ok(VQueueId::from_raw_bytes(source))
    }
}

impl KeyEncode for Stage {
    fn encode<B: BufMut>(&self, target: &mut B) {
        target.put_u8(*self as u8);
    }

    fn serialized_length(&self) -> usize {
        std::mem::size_of::<u8>()
    }
}

impl KeyDecode for Stage {
    fn decode<B: Buf>(source: &mut B) -> crate::Result<Self> {
        let i: u8 = source.get_u8();
        Self::from_repr(i)
            .ok_or_else(|| StorageError::Generic(anyhow::anyhow!("Wrong value for Stage: {}", i)))
    }
}

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
            crate::keys::serialize(self.partition().key_range.start(), &mut key_buf);
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
            crate::keys::serialize(self.partition().key_range.end(), &mut key_buf);
        }
        let _success = crate::convert_to_upper_bound(&mut key_buf);
        debug_assert!(_success);
        iterator_opts.set_iterate_upper_bound(key_buf);

        let rocksdb = self.rocksdb().inner().as_raw_db();

        let cf = self.table_cf_handle(crate::TableKind::VQueue);

        let mut it = rocksdb.raw_iterator_cf_opt(cf, iterator_opts);

        it.seek_to_first();

        let mut meta_keys_bytes_buf =
            BytesMut::with_capacity(BATCH_SIZE * MetaKey::serialized_length_fixed());
        // read items, and every 1000 we batch them up and perform a multi-get

        let mut meta_keys_bytes: Vec<BytesMut> = Vec::with_capacity(BATCH_SIZE);
        let mut queue_ids: Vec<VQueueId> = Vec::with_capacity(BATCH_SIZE);
        let mut end = false;

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
        Ok(())
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
        update: &restate_storage_api::vqueue_table::metadata::Update,
    ) {
        let key_buffer = MetaKey::from(qid).to_bytes();
        let updates = VQueueMetaUpdates::new(update.clone());
        let value_buf = {
            let value_buf = self.cleared_value_buffer_mut(updates.encoded_len());
            // unwrap is safe because we know the buffer is big enough.
            updates.encode(value_buf).unwrap();
            value_buf.split()
        };

        self.raw_merge_cf(KeyKind::VQueueMeta, key_buffer, value_buf);
    }

    fn put_inbox_entry(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
        value: &EntryValue,
    ) {
        let mut key_buffer = [0u8; InboxKey::serialized_length_fixed()];
        InboxKey::builder_ref()
            .qid(qid)
            .stage(&stage)
            .entry_key(key)
            .serialize_to(&mut key_buffer.as_mut());

        let value_buf = {
            let value_buf = self.cleared_value_buffer_mut(value.encoded_len());
            // unwrap is safe because we know the buffer is big enough.
            value.encode(value_buf).unwrap();
            value_buf.split()
        };

        self.raw_put_cf(KeyKind::VQueueInbox, key_buffer, value_buf)
    }

    fn pop_inbox_entry(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
    ) -> Result<Option<EntryValue>> {
        let mut key_buffer = [0u8; InboxKey::serialized_length_fixed()];
        InboxKey::builder_ref()
            .qid(qid)
            .stage(&stage)
            .entry_key(key)
            .serialize_to(&mut key_buffer.as_mut());

        let current = match self.get(TableKind::VQueue, key_buffer)? {
            Some(raw_value) => Some(EntryValue::decode(&mut raw_value.as_ref())?),
            None => None,
        };

        if current.is_some() {
            self.raw_delete_cf(KeyKind::VQueueInbox, key_buffer);
        }
        Ok(current)
    }

    fn delete_inbox_entry(&mut self, qid: &VQueueId, stage: Stage, key: &EntryKey) {
        let mut key_buffer = [0u8; InboxKey::serialized_length_fixed()];
        InboxKey::builder_ref()
            .qid(qid)
            .stage(&stage)
            .entry_key(key)
            .serialize_to(&mut key_buffer.as_mut());

        self.raw_delete_cf(KeyKind::VQueueInbox, key_buffer);
    }

    fn mark_vqueue_as_active(&mut self, qid: &VQueueId) {
        let mut key_buffer = [0u8; ActiveKey::serialized_length_fixed()];
        ActiveKey::builder_ref()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        self.raw_put_cf(KeyKind::VQueueActive, key_buffer, []);
    }

    fn mark_vqueue_as_dormant(&mut self, qid: &restate_types::vqueue::VQueueId) {
        let mut key_buffer = [0u8; ActiveKey::serialized_length_fixed()];
        ActiveKey::builder_ref()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        self.raw_delete_cf(KeyKind::VQueueActive, key_buffer);
    }

    fn put_vqueue_entry_state<E>(
        &mut self,
        qid: &VQueueId,
        entry_id: &EntryId,
        stage: Stage,
        entry_key: &EntryKey,
        state: E,
    ) where
        E: bilrost::Message + bilrost::encoding::RawMessage,
    {
        let key_buf = {
            let partition_key = qid.partition_key();
            let key = EntryStateKey::builder_ref()
                .partition_key(&partition_key)
                .id(entry_id);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        let header = EntryStateHeader {
            qid: qid.clone(),
            stage,
            current_entry_key: *entry_key,
        };

        let value_buf = {
            let header_len = header.encoded_len();
            let header_len = header_len + bilrost::encoding::encoded_len_varint(header_len as u64);

            let state_len = state.encoded_len();
            let state_len = state_len + bilrost::encoding::encoded_len_varint(state_len as u64);

            let value_buf = self.cleared_value_buffer_mut(header_len + state_len);
            // unwrap is safe because we know the buffer is big enough.
            header.encode_length_delimited(value_buf).unwrap();
            state.encode_length_delimited(value_buf).unwrap();
            value_buf.split()
        };

        self.raw_put_cf(KeyKind::VQueueEntryState, key_buf, value_buf);
    }

    fn delete_vqueue_entry_state(&mut self, partition_key: PartitionKey, id: &EntryId) {
        let key_buf = {
            let key = EntryStateKey::builder_ref()
                .partition_key(&partition_key)
                .id(id);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        self.raw_delete_cf(KeyKind::VQueueEntryState, key_buf);
    }

    fn put_item<E>(&mut self, qid: &VQueueId, created_at: UniqueTimestamp, id: &EntryId, item: E)
    where
        E: Message,
    {
        let key_buf = {
            let key = ItemsKey::builder_ref()
                .qid(qid)
                .created_at(&created_at)
                .id(id);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        let value_buffer = self.cleared_value_buffer_mut(item.encoded_len());

        item.encode(value_buffer)
            .expect("enough space to encode item");
        let value = value_buffer.split();

        self.raw_put_cf(KeyKind::VQueueItems, key_buf, value);
    }

    fn delete_item(&mut self, qid: &VQueueId, created_at: UniqueTimestamp, id: &EntryId) {
        let key_buf = {
            let key = ItemsKey::builder_ref()
                .qid(qid)
                .created_at(&created_at)
                .id(id);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        self.raw_delete_cf(KeyKind::VQueueItems, key_buf);
    }
}

impl ReadVQueueTable for PartitionStoreTransaction<'_> {
    async fn get_vqueue(&mut self, qid: &VQueueId) -> Result<Option<VQueueMeta>, StorageError> {
        let mut key_buffer = [0u8; MetaKey::serialized_length_fixed()];
        // MetaKey is fixed size, every time we overwrite the same fixed key_buffer
        MetaKey::builder_ref()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        let Some(raw_value) = self.get(TableKind::VQueue, key_buffer)? else {
            return Ok(None);
        };

        Ok(Some(VQueueMeta::decode(&mut raw_value.as_ref())?))
    }

    async fn get_entry_state_header(
        &mut self,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> Result<Option<impl AsEntryStateHeader + 'static>> {
        let key_buf = {
            let key = EntryStateKey::builder_ref()
                .partition_key(&partition_key)
                .id(id);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        let Some(raw_value) = self.get(TableKind::VQueue, key_buf)? else {
            return Ok(None);
        };

        let slice = raw_value;
        let decoded = EntryStateHeader::decode_length_delimited(&mut slice.as_ref())?;
        Ok(Some(decoded))
    }

    async fn get_entry_state<E>(
        &mut self,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> Result<Option<impl AsEntryState<State = E> + 'static>>
    where
        E: bilrost::OwnedMessage + Send + Sized + 'static,
    {
        let key_buf = {
            let key = EntryStateKey::builder_ref()
                .partition_key(&partition_key)
                .id(id);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        let Some(raw_value) = self.get(TableKind::VQueue, key_buf)? else {
            return Ok(None);
        };

        let mut slice = raw_value.as_ref();
        let header = EntryStateHeader::decode_length_delimited(&mut slice)?;
        let state = E::decode_length_delimited(&mut slice)?;

        Ok(Some(OwnedEntryState { header, state }))
    }

    async fn get_item<E>(
        &mut self,
        qid: &VQueueId,
        created_at: UniqueTimestamp,
        id: &EntryId,
    ) -> Result<Option<E>>
    where
        E: OwnedMessage,
    {
        let key_buf = {
            let key = ItemsKey::builder_ref()
                .qid(qid)
                .created_at(&created_at)
                .id(id);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        let Some(raw_value) = self.get(TableKind::VQueue, key_buf)? else {
            return Ok(None);
        };

        Ok(Some(E::decode(&mut raw_value.as_ref())?))
    }
}

impl ScanVQueueMetaTable for PartitionStore {
    fn for_each_vqueue_meta<
        F: for<'a> FnMut((&'a VQueueId, &'a VQueueMetaBorrowed<'a>)) -> std::ops::ControlFlow<()>
            + Send
            + Sync
            + 'static,
    >(
        &self,
        range: RangeInclusive<PartitionKey>,
        mut f: F,
    ) -> Result<impl Future<Output = Result<()>> + Send> {
        self.iterator_for_each(
            "df-vqueue-meta",
            Priority::Low,
            TableScan::FullScanPartitionKeyRange::<MetaKey>(range),
            move |(mut key, value)| {
                let meta_key = break_on_err(MetaKey::deserialize_from(&mut key))?;
                let meta = break_on_err(
                    VQueueMetaBorrowed::decode_borrowed(value).map_err(StorageError::BilrostDecode),
                )?;

                let (vqueue_id,) = meta_key.split();
                f((&vqueue_id, &meta)).map_break(Ok)
            },
        )
        .map_err(|_| StorageError::OperationalError)
    }
}

// ## Safety
// The iterator is guaranteed to be dropped before the database is dropped, we hold to the
// PartitionDb in this struct for as long as the iterator is alive.
unsafe fn ignore_iterator_lifetime<'a>(
    iter: DBRawIteratorWithThreadMode<'a, rocksdb::DB>,
) -> DBRawIteratorWithThreadMode<'static, rocksdb::DB> {
    unsafe {
        std::mem::transmute::<
            DBRawIteratorWithThreadMode<'a, rocksdb::DB>,
            DBRawIteratorWithThreadMode<'static, rocksdb::DB>,
        >(iter)
    }
}
