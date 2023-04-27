use crate::keys::{define_table_key, TableKey};
use crate::TableKind::Deduplication;
use crate::{
    GetFuture, GetStream, PutFuture, RocksDBTransaction, TableScan, TableScanIterationDecision,
};
use restate_common::types::PartitionId;
use restate_storage_api::deduplication_table::DeduplicationTable;
use restate_storage_api::{ready, StorageError};
use std::io::Cursor;

define_table_key!(
    Deduplication,
    DeduplicationKey(
        partition_id: PartitionId,
        producing_partition_id: PartitionId
    )
);

impl DeduplicationTable for RocksDBTransaction {
    fn get_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producing_partition_id: PartitionId,
    ) -> GetFuture<Option<u64>> {
        let key = DeduplicationKey::default()
            .partition_id(partition_id)
            .producing_partition_id(producing_partition_id);

        self.get_blocking(key, move |_k, maybe_sequence_number_slice| {
            let maybe_sequence_number = maybe_sequence_number_slice.map(|slice| {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(slice.as_ref());
                u64::from_be_bytes(buf)
            });

            Ok(maybe_sequence_number)
        })
    }

    fn put_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producing_partition_id: PartitionId,
        sequence_number: u64,
    ) -> PutFuture {
        let key = DeduplicationKey::default()
            .partition_id(partition_id)
            .producing_partition_id(producing_partition_id);
        self.put_kv(key, sequence_number);
        ready()
    }

    fn get_all_sequence_numbers(
        &mut self,
        partition_id: PartitionId,
    ) -> GetStream<(PartitionId, u64)> {
        self.for_each_key_value(
            TableScan::Partition::<DeduplicationKey>(partition_id),
            move |k, v| {
                let key = DeduplicationKey::deserialize_from(&mut Cursor::new(k))
                    .map(|key| key.producing_partition_id);

                let res = if let Ok(Some(producing_partition_id)) = key {
                    // read out the value
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(v);
                    let sequence_number = u64::from_be_bytes(buf);
                    Ok((producing_partition_id, sequence_number))
                } else {
                    Err(StorageError::DataIntegrityError)
                };
                TableScanIterationDecision::Emit(res)
            },
        )
    }
}
