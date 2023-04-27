use crate::codec::ProtoValue;
use crate::keys::{define_table_key, TableKey};
use crate::TableKind::Outbox;
use crate::{GetFuture, PutFuture, RocksDBTransaction, TableScan, TableScanIterationDecision};
use futures_util::StreamExt;
use prost::Message;
use restate_common::types::{OutboxMessage, PartitionId};
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::{ready, StorageError};
use restate_storage_proto::storage;
use std::io::Cursor;
use std::ops::Range;

define_table_key!(
    Outbox,
    OutboxKey(partition_id: PartitionId, message_index: u64)
);

impl OutboxTable for RocksDBTransaction {
    fn add_message(
        &mut self,
        partition_id: PartitionId,
        message_index: u64,
        outbox_message: OutboxMessage,
    ) -> PutFuture {
        let key = OutboxKey::default()
            .partition_id(partition_id)
            .message_index(message_index);

        let value = ProtoValue(storage::v1::OutboxMessage::from(outbox_message));
        self.put_kv(key, value);

        ready()
    }

    fn get_next_outbox_message(
        &mut self,
        partition_id: PartitionId,
        next_sequence_number: u64,
    ) -> GetFuture<Option<(u64, OutboxMessage)>> {
        let start = OutboxKey::default()
            .partition_id(partition_id)
            .message_index(next_sequence_number);

        let end = OutboxKey::default()
            .partition_id(partition_id)
            .message_index(u64::MAX);

        let mut stream = self
            .for_each_key_value(TableScan::KeyRangeInclusive(start, end), |k, v| {
                TableScanIterationDecision::Emit(decode_key_value(k, v))
            });

        Box::pin(async move { stream.next().await.transpose() })
    }

    fn truncate_outbox(
        &mut self,
        partition_id: PartitionId,
        seq_to_truncate: Range<u64>,
    ) -> PutFuture {
        let mut key = OutboxKey::default().partition_id(partition_id);
        let mut k = &mut key;

        for seq in seq_to_truncate {
            k.message_index = Some(seq);
            self.delete_key(k);
        }

        ready()
    }
}

fn decode_key_value(k: &[u8], v: &[u8]) -> crate::Result<(u64, OutboxMessage)> {
    // decode key
    let key = OutboxKey::deserialize_from(&mut Cursor::new(k))?;
    let sequence_number = *key.message_index_ok_or()?;

    // decode value
    let decoded = storage::v1::OutboxMessage::decode(v)
        .map_err(|error| StorageError::Generic(error.into()))?;
    let outbox_message = OutboxMessage::try_from(decoded).map_err(StorageError::Conversion)?;

    Ok((sequence_number, outbox_message))
}
