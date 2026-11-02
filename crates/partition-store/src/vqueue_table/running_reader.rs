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

use restate_rocksdb::reads::{OwnedReadIterator, ScopedReads};
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, VQueueRunningCursor};
use restate_types::vqueues::VQueueId;
use restate_util_lifecycle::Scope;

use crate::PartitionDb;
use crate::keys::{EncodeTableKey, EncodeTableKeyPrefix, KeyDecode};
use crate::partition_db::PhysicalPartition;
use crate::scan::PhysicalScan;
use crate::vqueue_table::inbox::{RunningKey, RunningKeyRef};

/// Reads the entries of a vqueue that are already running, in order.
///
/// Like [`VQueueWaitingReader`](super::inbox_reader::VQueueWaitingReader), it
/// reads the partition's latest data as it was when the reader was created, and
/// holds its own clone of the partition's physical scope, but no lease.
pub struct VQueueRunningReader {
    it: OwnedReadIterator<Scope<PhysicalPartition>>,
}

impl VQueueRunningReader {
    pub(crate) fn new(db: &PartitionDb, qid: &VQueueId) -> Self {
        let mut readopts = rocksdb::ReadOptions::default();
        // this is not the place to be concerned about corruption, we favor speed
        // over safety for this particular use-case.
        readopts.set_verify_checksums(false);

        // Every running entry of this vqueue: the scan sets the bounds, and decides
        // whether seeks can use the prefix extractor.
        let mut prefix = [0u8; RunningKey::by_qid_prefix_len()];
        RunningKeyRef::builder()
            .qid(qid)
            .serialize_to(&mut prefix.as_mut());
        let scan = PhysicalScan::Prefix(<RunningKey as EncodeTableKey>::KEY_KIND, prefix);
        scan.configure(&mut readopts);

        // It isn't positioned yet: the caller seeks when it needs to.
        Self {
            it: db
                .physical_scope()
                .owned_iterator(scan.key_kind(), readopts),
        }
    }
}

impl VQueueRunningCursor for VQueueRunningReader {
    fn seek_to_first(&mut self) {
        self.it.seek_to_first();
    }

    fn advance(&mut self) {
        self.it.next();
    }

    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>, StorageError> {
        match self.it.item() {
            Ok(Some((key, mut value))) => {
                debug_assert_eq!(key.len(), RunningKey::serialized_length_fixed());

                let entry_key = <EntryKey as KeyDecode>::decode(
                    &mut &key[RunningKey::offset_of_entry_key()..],
                )?;
                let value = EntryValue::decode(&mut value)?;

                Ok(Some((entry_key, value)))
            }
            // iterator is beyond the end, we can't peek
            Ok(None) => Ok(None),
            // We cannot recover from an error without a new seek.
            Err(err) => Err(StorageError::Generic(
                anyhow::Error::from(err).context("peek into vqueue snapshot iterator"),
            )),
        }
    }
}
