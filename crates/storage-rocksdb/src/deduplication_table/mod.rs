// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::codec::StorageSerdeValue;
use crate::keys::{define_table_key, TableKey};
use crate::TableKind::Deduplication;
use crate::{
    RocksDBStorage, RocksDBTransaction, StorageAccess, TableScan, TableScanIterationDecision,
};
use futures::Stream;
use futures_util::stream;
use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, DeduplicationTable, ProducerId,
    ReadOnlyDeduplicationTable,
};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::PartitionId;
use restate_types::storage::StorageCodec;
use std::io::Cursor;

define_table_key!(
    Deduplication,
    DeduplicationKey(partition_id: PartitionId, producer_id: ProducerId)
);

fn get_dedup_sequence_number<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
    producer_id: &ProducerId,
) -> Result<Option<DedupSequenceNumber>> {
    let key = DeduplicationKey::default()
        .partition_id(partition_id)
        .producer_id(producer_id.clone());

    storage.get_blocking(key, move |_k, maybe_dedup_sequence_number| {
        if let Some(bytes) = maybe_dedup_sequence_number {
            Ok(Some(
                StorageCodec::decode::<DedupSequenceNumber>(bytes)
                    .map_err(|error| StorageError::Conversion(error.into()))?,
            ))
        } else {
            Ok(None)
        }
    })
}

fn get_all_sequence_numbers<S: StorageAccess>(
    storage: &mut S,
    partition_id: PartitionId,
) -> impl Stream<Item = Result<DedupInformation>> + Send {
    stream::iter(storage.for_each_key_value_in_place(
        TableScan::Partition::<DeduplicationKey>(partition_id),
        move |k, v| {
            let key =
                DeduplicationKey::deserialize_from(&mut Cursor::new(k)).map(|key| key.producer_id);

            let res = if let Ok(Some(producer_id)) = key {
                StorageCodec::decode::<DedupSequenceNumber>(v)
                    .map_err(|err| StorageError::Conversion(err.into()))
                    .map(|sequence_number| DedupInformation {
                        producer_id,
                        sequence_number,
                    })
            } else {
                Err(StorageError::DataIntegrityError)
            };
            TableScanIterationDecision::Emit(res)
        },
    ))
}

impl ReadOnlyDeduplicationTable for RocksDBStorage {
    async fn get_dedup_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producer_id: &ProducerId,
    ) -> Result<Option<DedupSequenceNumber>> {
        get_dedup_sequence_number(self, partition_id, producer_id)
    }

    fn get_all_sequence_numbers(
        &mut self,
        partition_id: PartitionId,
    ) -> impl Stream<Item = Result<DedupInformation>> + Send {
        get_all_sequence_numbers(self, partition_id)
    }
}

impl<'a> ReadOnlyDeduplicationTable for RocksDBTransaction<'a> {
    async fn get_dedup_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producer_id: &ProducerId,
    ) -> Result<Option<DedupSequenceNumber>> {
        get_dedup_sequence_number(self, partition_id, producer_id)
    }

    fn get_all_sequence_numbers(
        &mut self,
        partition_id: PartitionId,
    ) -> impl Stream<Item = Result<DedupInformation>> + Send {
        get_all_sequence_numbers(self, partition_id)
    }
}

impl<'a> DeduplicationTable for RocksDBTransaction<'a> {
    async fn put_dedup_seq_number(
        &mut self,
        partition_id: PartitionId,
        producer_id: ProducerId,
        dedup_sequence_number: DedupSequenceNumber,
    ) {
        let key = DeduplicationKey::default()
            .partition_id(partition_id)
            .producer_id(producer_id);
        self.put_kv(key, StorageSerdeValue(dedup_sequence_number));
    }
}
