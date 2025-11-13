// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use rocksdb::DBRawIteratorWithThreadMode;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryCard, Stage, VQueueCursor};
use restate_types::vqueue::VQueueId;

use crate::PartitionDb;
use crate::keys::{TableKey, TableKeyPrefix};
use crate::vqueue_table::InboxKey;

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
        // Do not remove this!
        readopts.set_total_order_seek(true);

        // we know how big the prefix is
        let mut key_buf = [0u8; InboxKey::by_stage_prefix_len()];
        InboxKey::builder()
            .partition_key(qid.partition_key)
            .parent(qid.parent)
            .instance(qid.instance)
            .stage(Stage::Run)
            .serialize_to(&mut key_buf.as_mut());

        readopts.set_iterate_lower_bound(key_buf);
        let success = super::convert_to_upper_bound(&mut key_buf);
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
    type Item = EntryCard;

    fn seek_to_first(&mut self) {
        self.it.seek_to_first();
    }

    fn seek_after(&mut self, _qid: &VQueueId, _item: &Self::Item) {
        panic!("seek_after is not supported for running snapshot reader");
    }

    fn advance(&mut self) {
        self.it.next();
    }

    fn peek(&mut self) -> Result<Option<EntryCard>, StorageError> {
        if let Some((mut key, _value)) = self.it.item() {
            let key = InboxKey::deserialize_from(&mut key)?;
            assert_eq!(*key.stage(), Stage::Run);
            Ok(Some(EntryCard::from(key)))
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
