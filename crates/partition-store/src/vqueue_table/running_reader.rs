// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use bilrost::OwnedMessage;
use rocksdb::DBRawIteratorWithThreadMode;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, VQueueCursor};
use restate_types::vqueues::VQueueId;

use crate::PartitionDb;
use crate::keys::{EncodeTableKeyPrefix, KeyDecode};
use crate::vqueue_table::inbox::RunningKey;

pub struct VQueueRunningReader {
    it: DBRawIteratorWithThreadMode<'static, rocksdb::DB>,
}

impl VQueueRunningReader {
    pub(crate) fn new(db: &PartitionDb, qid: &VQueueId) -> Self {
        let mut readopts = rocksdb::ReadOptions::default();
        readopts.set_async_io(true);
        // this is not the place to be concerned about corruption, we favor speed
        // over safety for this particular use-case.
        readopts.set_verify_checksums(false);
        // use prefix extractors for efficient filtering.
        readopts.set_prefix_same_as_start(true);

        // we know how big the prefix is
        let mut key_buf = [0u8; RunningKey::by_qid_prefix_len()];
        RunningKey::builder_ref()
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
            // Safety:
            // The iterator is guaranteed to be dropped before the database is dropped, we hold to the
            // PartitionDb in the scheduler which pins the database and the column family.
            it: unsafe { super::ignore_iterator_lifetime(it) },
        }
    }
}

impl VQueueCursor for VQueueRunningReader {
    fn seek_to_first(&mut self) {
        self.it.seek_to_first();
    }

    fn seek_after(&mut self, _qid: &VQueueId, _key: &EntryKey) {
        panic!("seek_after is not supported for running snapshot reader");
    }

    fn advance(&mut self) {
        self.it.next();
    }

    /// Returns the current key under cursor
    fn current_key(&mut self) -> Result<Option<EntryKey>, StorageError> {
        if let Some(key) = self.it.key() {
            debug_assert_eq!(key.len(), RunningKey::serialized_length_fixed());

            // The portion we are interested in is everything that represents the EntryKey
            let entry_key =
                <EntryKey as KeyDecode>::decode(&mut &key[RunningKey::offset_of_entry_key()..])?;
            Ok(Some(entry_key))
        } else {
            // we reached the end (or an error). We cannot recover from this without seek.
            // todo: add support for iterator refresh().
            self.it
                .status()
                .context("peek into vqueue snapshot iterator")
                .map_err(StorageError::Generic)?;
            // iterator is beyond the end, we can't peek
            Ok(None)
        }
    }

    /// Returns the current value under cursor
    fn current_value(&mut self) -> Result<Option<EntryValue>, StorageError> {
        if let Some(mut value) = self.it.value() {
            let value = EntryValue::decode(&mut value)?;
            Ok(Some(value))
        } else {
            // we reached the end (or an error). We cannot recover from this without seek.
            // todo: add support for iterator refresh().
            self.it
                .status()
                .context("peek into vqueue snapshot iterator")
                .map_err(StorageError::Generic)?;
            // iterator is beyond the end, we can't peek
            Ok(None)
        }
    }

    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>, StorageError> {
        if let Some((key, mut value)) = self.it.item() {
            debug_assert_eq!(key.len(), RunningKey::serialized_length_fixed());

            let entry_key =
                <EntryKey as KeyDecode>::decode(&mut &key[RunningKey::offset_of_entry_key()..])?;
            let value = EntryValue::decode(&mut value)?;

            Ok(Some((entry_key, value)))
        } else {
            // we reached the end (or an error). We cannot recover from this without seek.
            // todo: add support for iterator refresh().
            self.it
                .status()
                .context("peek into vqueue snapshot iterator")
                .map_err(StorageError::Generic)?;
            // iterator is beyond the end, we can't peek
            Ok(None)
        }
    }
}
