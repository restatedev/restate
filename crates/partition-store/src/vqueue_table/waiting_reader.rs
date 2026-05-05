// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bilrost::OwnedMessage;
use restate_rocksdb::RocksDb;
use rocksdb::DBRawIteratorWithThreadMode;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{
    CursorError, EntryKey, EntryValue, Options, Stage, VQueueCursor,
};
use restate_types::vqueues::VQueueId;

use crate::PartitionDb;
use crate::keys::{EncodeTableKeyPrefix, KeyDecode};
use crate::vqueue_table::InboxKey;
use crate::vqueue_table::inbox::InboxKeyRef;

pub struct VQueueWaitingReader {
    qid: VQueueId,
    it: DBRawIteratorWithThreadMode<'static, rocksdb::DB>,
    // Safety: This must be dropped last since the iterator references memory allocated by it.
    // This is only set to Some if the iterator is configured to run with blocking IO. The
    // assumption is that blocking iterators will run in background threads, so we need to pin
    // the database until the iterator is dropped.
    _db: Option<Arc<RocksDb>>,
}

impl VQueueWaitingReader {
    pub(crate) fn new(db: &PartitionDb, qid: &VQueueId, opts: Options) -> Self {
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_async_io(true);
        // this is not the place to be concerned about corruption, we favor speed
        // over safety for this particular use-case.
        readopts.set_verify_checksums(false);
        // We re-create this reader on every refill, so a fresh snapshot is what
        // we want. A tailing iterator would see new writes but is unsafe across
        // memtable flushes.
        // use prefix extractors for efficient filtering.
        readopts.set_prefix_same_as_start(true);

        if opts.allow_blocking_io {
            readopts.set_read_tier(rocksdb::ReadTier::All);
        } else {
            readopts.set_read_tier(rocksdb::ReadTier::BlockCache);
        }

        // we know how big the prefix is
        let mut key_buf = [0u8; InboxKey::by_qid_prefix_len()];
        InboxKeyRef::builder()
            .qid(qid)
            .serialize_to(&mut key_buf.as_mut());

        readopts.set_iterate_lower_bound(key_buf);
        let success = crate::convert_to_upper_bound(&mut key_buf);
        debug_assert!(success);
        readopts.set_iterate_upper_bound(key_buf);

        let it = db
            .rocksdb()
            .inner()
            .as_raw_db()
            .raw_iterator_cf_opt(db.cf_handle(), readopts);

        Self {
            qid: qid.clone(),
            // Safety:
            // The iterator is guaranteed to be dropped before the database is dropped, we hold to the
            // PartitionDb in the scheduler which pins the database and the column family.
            it: unsafe { super::ignore_iterator_lifetime(it) },
            _db: opts.allow_blocking_io.then(|| db.rocksdb().clone()),
        }
    }
}

impl VQueueCursor for VQueueWaitingReader {
    fn seek_to_first(&mut self) {
        self.it.seek_to_first();
    }

    fn seek_after(&mut self, key: &EntryKey) {
        tracing::trace!("Seeking after {key:?}");
        let mut key_buf = super::inbox::encode_stage_key(Stage::Inbox, &self.qid, key);
        let success = crate::convert_to_upper_bound(&mut key_buf);
        debug_assert!(success);
        self.it.seek(key_buf);
    }

    fn advance(&mut self) {
        self.it.next();
    }

    /// Returns the current key under cursor
    fn current_key(&mut self) -> Result<Option<EntryKey>, CursorError> {
        if let Some(key) = self.it.key() {
            debug_assert_eq!(key.len(), InboxKey::serialized_length_fixed());

            // The portion we are interested in is everything that represents the EntryKey
            let entry_key =
                <EntryKey as KeyDecode>::decode(&mut &key[InboxKey::offset_of_entry_key()..])?;
            Ok(Some(entry_key))
        } else {
            self.it.status().map_err(|err| match err.kind() {
                rocksdb::ErrorKind::Incomplete => CursorError::WouldBlock,
                _ => CursorError::Other(StorageError::Generic(err.into())),
            })?;
            // iterator is beyond the end, we can't peek
            Ok(None)
        }
    }
    /// Returns the current value under cursor
    fn current_value(&mut self) -> Result<Option<EntryValue>, CursorError> {
        if let Some(mut value) = self.it.value() {
            let value = EntryValue::decode(&mut value).map_err(StorageError::BilrostDecode)?;
            Ok(Some(value))
        } else {
            self.it.status().map_err(|err| match err.kind() {
                rocksdb::ErrorKind::Incomplete => CursorError::WouldBlock,
                _ => CursorError::Other(StorageError::Generic(err.into())),
            })?;
            // iterator is beyond the end, we can't peek
            Ok(None)
        }
    }

    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>, CursorError> {
        if let Some((key, mut value)) = self.it.item() {
            debug_assert_eq!(key.len(), InboxKey::serialized_length_fixed());
            let entry_key =
                <EntryKey as KeyDecode>::decode(&mut &key[InboxKey::offset_of_entry_key()..])?;

            let value = EntryValue::decode(&mut value).map_err(StorageError::BilrostDecode)?;

            Ok(Some((entry_key, value)))
        } else {
            self.it.status().map_err(|err| match err.kind() {
                rocksdb::ErrorKind::Incomplete => CursorError::WouldBlock,
                _ => CursorError::Other(StorageError::Generic(err.into())),
            })?;
            // iterator is beyond the end, we can't peek
            Ok(None)
        }
    }
}
