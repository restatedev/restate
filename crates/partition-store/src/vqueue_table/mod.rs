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
mod input;
mod key_codec;
mod metadata;
mod reader;
mod running_reader;
mod waiting_reader;

use std::io::Cursor;

pub use entry::{EntryStatusKey, StatusHeaderRaw};
pub use inbox::InboxKey;
pub use input::InputPayloadKey;
pub use metadata::*;

use anyhow::Context;
use bilrost::{Message, OwnedMessage};
use bytes::BytesMut;
use rocksdb::{DBRawIteratorWithThreadMode, ReadOptions};
use tracing::error;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{VQueueMeta, VQueueMetaUpdates};
use restate_storage_api::vqueue_table::{
    EntryKey, EntryMetadata, EntryStatusHeader, EntryValue, LazyEntryStatus, ReadVQueueTable,
    ScanVQueueTable, Stage, Status, WriteVQueueTable, stats::EntryStatistics,
};
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::{EntryId, Seq, VQueueId};

use self::entry::{LazyEntryStatusHolder, OwnedEntryStatusHeader, StatusHeaderRawRef};
use crate::keys::{DecodeTableKey, EncodeTableKey, EncodeTableKeyPrefix, KeyKind};
use crate::{PartitionDb, PartitionStoreTransaction, Result, StorageAccess, TableKind};

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

        // Note: the key kind here is not used, so we always use the InboxStage key kind.
        self.raw_put_cf(KeyKind::VQueueInboxStage, key_buffer, value_buf)
    }

    fn get_vqueue_inbox(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        key: &EntryKey,
    ) -> Result<Option<EntryValue>> {
        let key_buffer = inbox::encode_stage_key(stage, qid, key);

        Ok(match self.get(TableKind::VQueue, key_buffer)? {
            Some(raw_value) => Some(EntryValue::decode(&mut raw_value.as_ref())?),
            None => None,
        })
    }

    fn delete_vqueue_inbox(&mut self, qid: &VQueueId, stage: Stage, key: &EntryKey) {
        // Note: the key kind here is not used, so we always use the InboxKey.
        self.raw_delete_cf(
            KeyKind::VQueueInboxStage,
            inbox::encode_stage_key(stage, qid, key),
        );
    }

    fn mark_vqueue_as_active(&mut self, qid: &VQueueId) {
        let mut key_buffer = [0u8; ActiveKey::serialized_length_fixed()];
        ActiveKey::builder_ref()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        self.raw_put_cf(KeyKind::VQueueActive, key_buffer, []);
    }

    fn mark_vqueue_as_dormant(&mut self, qid: &restate_types::vqueues::VQueueId) {
        let mut key_buffer = [0u8; ActiveKey::serialized_length_fixed()];
        ActiveKey::builder_ref()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        self.raw_delete_cf(KeyKind::VQueueActive, key_buffer);
    }

    fn put_vqueue_entry_status(
        &mut self,
        qid: &VQueueId,
        stage: Stage,
        entry_key: &EntryKey,
        metadata: &EntryMetadata,
        stats: EntryStatistics,
        status: Status,
    ) {
        let mut key_buffer = [0u8; EntryStatusKey::serialized_length_fixed()];
        EntryStatusKey::builder_ref()
            .partition_key(&qid.partition_key())
            .id(entry_key.entry_id())
            .serialize_to(&mut key_buffer.as_mut());

        let header = StatusHeaderRawRef {
            qid: qid.into(),
            stage,
            has_lock: entry_key.has_lock(),
            next_run_at: entry_key.run_at(),
            seq: entry_key.seq(),
            metadata: metadata.into(),
            stats,
            status,
        };

        let value_buf = {
            let header_len = header.encoded_len();
            let header_len = header_len + bilrost::encoding::encoded_len_varint(header_len as u64);

            let value_buf = self.cleared_value_buffer_mut(header_len);
            // unwrap is safe because we know the buffer is big enough.
            header.encode_length_delimited(value_buf).unwrap();
            value_buf.split()
        };

        self.raw_put_cf(KeyKind::VQueueEntryStatus, key_buffer, value_buf);
    }

    fn delete_vqueue_entry_status(&mut self, partition_key: PartitionKey, id: &EntryId) {
        let mut key_buffer = [0u8; EntryStatusKey::serialized_length_fixed()];
        EntryStatusKey::builder_ref()
            .partition_key(&partition_key)
            .id(id)
            .serialize_to(&mut key_buffer.as_mut());

        self.raw_delete_cf(KeyKind::VQueueEntryStatus, key_buffer);
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
        InputPayloadKey::builder_ref()
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

    fn delete_vqueue_input_payload(&mut self, qid: &VQueueId, seq: impl Into<Seq>, id: &EntryId) {
        let key_buf = {
            let seq = seq.into();
            let key = InputPayloadKey::builder_ref().qid(qid).seq(&seq).id(id);
            let key_buf = self.cleared_key_buffer_mut(key.serialized_length());
            key.serialize_to(key_buf);
            key_buf.split()
        };

        self.raw_delete_cf(KeyKind::VQueueInput, key_buf);
    }
}

impl ReadVQueueTable for PartitionStoreTransaction<'_> {
    async fn get_vqueue(&self, qid: &VQueueId) -> Result<Option<VQueueMeta>, StorageError> {
        let mut key_buffer = [0u8; MetaKey::serialized_length_fixed()];
        MetaKey::builder_ref()
            .qid(qid)
            .serialize_to(&mut key_buffer.as_mut());
        let Some(raw_value) = self.get(TableKind::VQueue, key_buffer)? else {
            return Ok(None);
        };

        Ok(Some(VQueueMeta::decode(&mut raw_value.as_ref())?))
    }

    async fn get_vqueue_entry_status(
        &self,
        partition_key: PartitionKey,
        id: &EntryId,
    ) -> Result<Option<impl EntryStatusHeader + 'static>> {
        let mut key_buffer = [0u8; EntryStatusKey::serialized_length_fixed()];
        EntryStatusKey::builder_ref()
            .partition_key(&partition_key)
            .id(id)
            .serialize_to(&mut key_buffer.as_mut());

        let Some(raw_value) = self.get(TableKind::VQueue, key_buffer)? else {
            return Ok(None);
        };

        Ok(Some(OwnedEntryStatusHeader::new(
            *id,
            StatusHeaderRaw::decode_length_delimited(&mut raw_value.as_ref())?,
        )))
    }

    // Currently unused but left for future use
    async fn get_vqueue_entry_status_lazy<'a>(
        &'a self,
        partition_key: PartitionKey,
        entry_id: &EntryId,
    ) -> Result<Option<impl LazyEntryStatus + 'a>> {
        let mut key_buffer = [0u8; EntryStatusKey::serialized_length_fixed()];
        EntryStatusKey::builder_ref()
            .partition_key(&partition_key)
            .id(entry_id)
            .serialize_to(&mut key_buffer.as_mut());

        let Some(raw_value) = self.get(TableKind::VQueue, key_buffer)? else {
            return Ok(None);
        };

        let mut cursor = Cursor::new(raw_value);
        // The cursor will be advanced by the header size so LazyEntryState
        // will be positioned to read the state next.
        let header = StatusHeaderRaw::decode_length_delimited(&mut cursor)?;

        Ok(Some(LazyEntryStatusHolder::new(*entry_id, header, cursor)))
    }

    // Left intentionally for future reference
    // async fn get_entry_state<I>(
    //     &self,
    //     id: I,
    // ) -> Result<Option<(impl EntryStatusHeader + 'static, I::State)>>
    // where
    //     I: IdentifiesEntry,
    //     I::State: EntryStatus + bilrost::OwnedMessage + Send + Sized + 'static,
    // {
    //     let mut key_buffer = [0u8; EntryStatusKey::serialized_length_fixed()];
    //     let entry_id = id.to_entry_id();
    //     EntryStatusKey::builder_ref()
    //         .partition_key(&id.partition_key())
    //         .id(&entry_id)
    //         .serialize_to(&mut key_buffer.as_mut());
    //
    //     let Some(raw_value) = self.get(TableKind::VQueue, key_buffer)? else {
    //         return Ok(None);
    //     };
    //
    //     let mut slice = raw_value.as_ref();
    //     let header = StatusHeaderRaw::decode_length_delimited(&mut slice)?;
    //     let state = I::State::decode_length_delimited(&mut slice)?;
    //
    //     Ok(Some((OwnedEntryStatusHeader::new(entry_id, header), state)))
    // }

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
        InputPayloadKey::builder_ref()
            .qid(qid)
            .seq(&seq)
            .id(id)
            .serialize_to(&mut key_buffer.as_mut());

        let Some(raw_value) = self.get(TableKind::VQueue, key_buffer)? else {
            return Ok(None);
        };

        Ok(Some(E::decode(&mut raw_value.as_ref())?))
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
