// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_rocksdb::RocksDbPerfGuard;
use restate_storage_api::Result;
use restate_storage_api::deduplication_table::{
    DedupSequenceNumber, ProducerId, ReadDeduplicationTable, WriteDeduplicationTable,
};
use restate_types::identifiers::PartitionId;

use crate::TableKind::Deduplication;
use crate::keys::{KeyKind, define_table_key};
use crate::{PaddedPartitionId, PartitionStore, PartitionStoreTransaction, StorageAccess};

define_table_key!(
    Deduplication,
    KeyKind::Deduplication,
    DeduplicationKey(partition_id: PaddedPartitionId, producer_id: ProducerId)
);

fn get_dedup_sequence_number<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    producer_id: &ProducerId,
) -> Result<Option<DedupSequenceNumber>> {
    let _x = RocksDbPerfGuard::new("get-dedup-seq");
    let key = DeduplicationKey::default()
        .partition_id(partition_id.into())
        .producer_id(producer_id.clone());

    storage.get_value_proto(key)
}

impl ReadDeduplicationTable for PartitionStore {
    async fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<Option<DedupSequenceNumber>> {
        get_dedup_sequence_number(self, self.partition_id(), producer_id)
    }
}

impl ReadDeduplicationTable for PartitionStoreTransaction<'_> {
    async fn get_dedup_sequence_number(
        &mut self,
        producer_id: &ProducerId,
    ) -> Result<Option<DedupSequenceNumber>> {
        get_dedup_sequence_number(self, self.partition_id(), producer_id)
    }
}

impl WriteDeduplicationTable for PartitionStoreTransaction<'_> {
    fn put_dedup_seq_number(
        &mut self,
        producer_id: ProducerId,
        dedup_sequence_number: &DedupSequenceNumber,
    ) -> Result<()> {
        let key = DeduplicationKey::default()
            .partition_id(self.partition_id().into())
            .producer_id(producer_id);

        self.put_kv_proto(key, dedup_sequence_number)
    }
}
