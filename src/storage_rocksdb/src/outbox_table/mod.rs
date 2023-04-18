use crate::composite_keys::u64_pair;
use crate::TableKind::Outbox;
use crate::{write_proto_infallible, GetFuture, PutFuture, RocksDBTransaction};
use bytes::BufMut;
use prost::Message;
use restate_common::types::{OutboxMessage, PartitionId};
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::{ready, StorageError};
use restate_storage_proto::storage;
use std::ops::Range;

impl OutboxTable for RocksDBTransaction {
    fn add_message(
        &mut self,
        partition_id: PartitionId,
        message_index: u64,
        outbox_message: OutboxMessage,
    ) -> PutFuture {
        let key = self.key_buffer();
        key.put_u64(partition_id);
        key.put_u64(message_index);

        write_proto_infallible(
            self.value_buffer(),
            storage::v1::OutboxMessage::from(outbox_message),
        );
        self.put_kv_buffer(Outbox);

        ready()
    }

    fn get_next_outbox_message(
        &mut self,
        partition_id: PartitionId,
        next_sequence_number: u64,
    ) -> GetFuture<Option<(u64, OutboxMessage)>> {
        self.spawn_blocking(move |db| {
            let lo = u64_pair(partition_id, next_sequence_number);
            let hi = u64_pair(partition_id + 1, 0);

            let mut iterator = db.range_iterator(Outbox, lo..hi);
            iterator.seek(lo);

            if let Some((k, v)) = iterator.item() {
                // sequence number is the second component of the key
                // it starts at the 8th byte.
                assert_eq!(k.len(), 16);
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&k[8..]);
                let sequence_number = u64::from_be_bytes(buf);

                // the value is the protobuf message OutboxMessage.
                let outbox = OutboxMessage::try_from(
                    storage::v1::OutboxMessage::decode(v)
                        .map_err(|error| StorageError::Generic(error.into()))?,
                )?;
                Ok(Some((sequence_number, outbox)))
            } else {
                Ok(None)
            }
        })
    }

    fn truncate_outbox(
        &mut self,
        partition_id: PartitionId,
        seq_to_truncate: Range<u64>,
    ) -> PutFuture {
        let mut key = u64_pair(partition_id, 0);

        for seq in seq_to_truncate {
            key[8..].copy_from_slice(&seq.to_be_bytes());
            self.delete_key(Outbox, key);
        }

        ready()
    }
}
