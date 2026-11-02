// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bilrost::OwnedMessage;

use restate_rocksdb::RocksError;
use restate_rocksdb::reads::{OwnedReadIterator, ScopedReads};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{
    CursorError, EntryKey, EntryValue, Options, Stage, VQueueCursor,
};
use restate_types::vqueues::VQueueId;
use restate_util_lifecycle::Scope;

use crate::PartitionDb;
use crate::keys::{EncodeTableKey, EncodeTableKeyPrefix, KeyDecode};
use crate::partition_db::PhysicalPartition;
use crate::scan::PhysicalScan;
use crate::vqueue_table::InboxKey;
use crate::vqueue_table::inbox::InboxKeyRef;

/// Reads the entries waiting in a vqueue's inbox, in order.
///
/// It reads the partition's latest data as it was when the reader was created,
/// and holds its own clone of the partition's physical scope, so it can be kept
/// across pauses and moved between threads. It holds no lease: closing the
/// partition doesn't wait for it, but deleting the partition's column families
/// does.
pub struct VQueueWaitingReader {
    qid: VQueueId,
    it: OwnedReadIterator<Scope<PhysicalPartition>>,
}

impl VQueueWaitingReader {
    pub(crate) fn new(db: &PartitionDb, qid: &VQueueId, opts: Options) -> Self {
        let mut readopts = rocksdb::ReadOptions::default();
        // this is not the place to be concerned about corruption, we favor speed
        // over safety for this particular use-case.
        readopts.set_verify_checksums(false);

        if opts.allow_blocking_io {
            readopts.set_read_tier(rocksdb::ReadTier::All);
        } else {
            readopts.set_read_tier(rocksdb::ReadTier::BlockCache);
        }

        // Every entry of this vqueue's inbox: the scan sets the bounds, and decides
        // whether seeks can use the prefix extractor.
        let mut prefix = [0u8; InboxKey::by_qid_prefix_len()];
        InboxKeyRef::builder()
            .qid(qid)
            .serialize_to(&mut prefix.as_mut());
        let scan = PhysicalScan::Prefix(<InboxKey as EncodeTableKey>::KEY_KIND, prefix);
        scan.configure(&mut readopts);

        // The iterator sees the data as it was when it was created. We re-create
        // this reader on every refill, so that's what we want. It isn't positioned
        // yet: the caller seeks on a thread that may block.
        Self {
            qid: qid.clone(),
            it: db
                .physical_scope()
                .owned_iterator(scan.key_kind(), readopts),
        }
    }
}

impl VQueueCursor for VQueueWaitingReader {
    fn seek_to_first(&mut self) {
        self.it.seek_to_first();
    }

    fn seek_after(&mut self, key: &EntryKey) {
        let mut key_buf = super::inbox::encode_stage_key(Stage::Inbox, &self.qid, key);
        let success = crate::convert_to_upper_bound(&mut key_buf);
        debug_assert!(success);
        self.it.seek(&key_buf);
    }

    fn advance(&mut self) {
        self.it.next();
    }

    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>, CursorError> {
        match self.it.item() {
            Ok(Some((key, mut value))) => {
                debug_assert_eq!(key.len(), InboxKey::serialized_length_fixed());
                let entry_key =
                    <EntryKey as KeyDecode>::decode(&mut &key[InboxKey::offset_of_entry_key()..])?;

                let value = EntryValue::decode(&mut value).map_err(StorageError::BilrostDecode)?;

                Ok(Some((entry_key, value)))
            }
            // iterator is beyond the end, we can't peek
            Ok(None) => Ok(None),
            // A read restricted to the block cache needs data that isn't there.
            Err(RocksError::Other(err)) if err.kind() == rocksdb::ErrorKind::Incomplete => {
                Err(CursorError::WouldBlock)
            }
            Err(err) => Err(CursorError::Other(StorageError::Generic(err.into()))),
        }
    }
}
