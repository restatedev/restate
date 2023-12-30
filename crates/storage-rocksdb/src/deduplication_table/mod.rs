// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::keys::{define_table_key, TableKey};
use crate::TableKind::Deduplication;
use crate::{RocksDBTransaction, TableScan, TableScanIterationDecision};
use futures::Stream;
use restate_storage_api::deduplication_table::{DeduplicationTable, SequenceNumberSource};
use restate_storage_api::{Result, StorageError};
use restate_types::identifiers::PartitionId;
use std::io::Cursor;

define_table_key!(
    Deduplication,
    DeduplicationKey(partition_id: PartitionId, source: SequenceNumberSource)
);

impl<'a> DeduplicationTable for RocksDBTransaction<'a> {
    async fn get_sequence_number(
        &mut self,
        partition_id: PartitionId,
        source: SequenceNumberSource,
    ) -> Result<Option<u64>> {
        let key = DeduplicationKey::default()
            .partition_id(partition_id)
            .source(source);

        self.get_blocking(key, move |_k, maybe_sequence_number_slice| {
            let maybe_sequence_number = maybe_sequence_number_slice.map(|slice| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(slice.as_ref());
                u64::from_be_bytes(buf)
            });

            Ok(maybe_sequence_number)
        })
        .await
    }

    async fn put_sequence_number(
        &mut self,
        partition_id: PartitionId,
        source: SequenceNumberSource,
        sequence_number: u64,
    ) {
        let key = DeduplicationKey::default()
            .partition_id(partition_id)
            .source(source);
        self.put_kv(key, sequence_number);
    }

    fn get_all_sequence_numbers(
        &mut self,
        partition_id: PartitionId,
    ) -> impl Stream<Item = Result<(SequenceNumberSource, u64)>> + Send {
        self.for_each_key_value(
            TableScan::Partition::<DeduplicationKey>(partition_id),
            move |k, v| {
                let key =
                    DeduplicationKey::deserialize_from(&mut Cursor::new(k)).map(|key| key.source);

                let res = if let Ok(Some(source)) = key {
                    // read out the value
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(v);
                    let sequence_number = u64::from_be_bytes(buf);
                    Ok((source, sequence_number))
                } else {
                    Err(StorageError::DataIntegrityError)
                };
                TableScanIterationDecision::Emit(res)
            },
        )
    }
}
