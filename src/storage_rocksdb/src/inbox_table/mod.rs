use crate::composite_keys::write_delimited;
use crate::TableKind::Inbox;
use crate::{write_proto_infallible, GetFuture, PutFuture, RocksDBTransaction};
use bytes::{BufMut, BytesMut};
use common::types::{InboxEntry, PartitionKey, ServiceId, ServiceInvocation};
use prost::Message;
use storage_api::inbox_table::InboxTable;
use storage_api::{ready, GetStream, StorageError};
use storage_proto::storage;

#[inline]
fn write_message_key(
    key: &mut BytesMut,
    partition_key: PartitionKey,
    service_id: &ServiceId,
    sequence_number: u64,
) {
    key.put_u64(partition_key);
    write_delimited(&service_id.service_name, key);
    write_delimited(&service_id.key, key);
    key.put_u64(sequence_number);
}

#[inline]
fn write_inbox_key(
    aux_storage: &mut BytesMut,
    partition_key: PartitionKey,
    service_id: &ServiceId,
) {
    aux_storage.put_u64(partition_key);
    write_delimited(&service_id.service_name, aux_storage);
    write_delimited(&service_id.key, aux_storage);
}

#[inline]
fn message_sequence_number_from_slice(key: &[u8]) -> u64 {
    let mut buffer = [0u8; 8];
    let offset = key.len() - 8;
    buffer.copy_from_slice(&key[offset..]);
    u64::from_be_bytes(buffer)
}

impl InboxTable for RocksDBTransaction {
    fn put_invocation(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        InboxEntry {
            inbox_sequence_number,
            service_invocation,
        }: InboxEntry,
    ) -> PutFuture {
        write_message_key(
            self.key_buffer(),
            partition_key,
            service_id,
            inbox_sequence_number,
        );
        write_proto_infallible(
            self.value_buffer(),
            storage::v1::InboxEntry::from(service_invocation),
        );
        self.put_kv_buffer(Inbox);
        ready()
    }

    fn delete_invocation(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        sequence_number: u64,
    ) -> PutFuture {
        write_message_key(
            self.key_buffer(),
            partition_key,
            service_id,
            sequence_number,
        );
        self.delete_key_buffer(Inbox);

        ready()
    }

    fn peek_inbox(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetFuture<Option<InboxEntry>> {
        write_inbox_key(self.key_buffer(), partition_key, service_id);
        let key = self.clone_key_buffer();

        self.spawn_blocking(move |db| {
            let mut iterator = db.prefix_iterator(Inbox, key.clone());
            iterator.seek(&key);
            if let Some((k, v)) = iterator.item() {
                let inbox_entry = decode_inbox_key_value(k, v)?;
                Ok(Some(inbox_entry))
            } else {
                Ok(None)
            }
        })
    }

    fn inbox(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetStream<InboxEntry> {
        write_inbox_key(self.key_buffer(), partition_key, service_id);
        let key = self.clone_key_buffer();

        self.spawn_background_scan(move |db, tx| {
            let mut iterator = db.prefix_iterator(Inbox, key.clone());
            iterator.seek(&key);
            while let Some((k, v)) = iterator.item() {
                let res = decode_inbox_key_value(k, v);
                if tx.blocking_send(res).is_err() {
                    break;
                }
                iterator.next();
            }
        })
    }
}

fn decode_inbox_key_value(k: &[u8], v: &[u8]) -> crate::Result<InboxEntry> {
    let sequence_number = message_sequence_number_from_slice(k);

    let inbox_entry = ServiceInvocation::try_from(
        storage::v1::InboxEntry::decode(v).map_err(|error| StorageError::Generic(error.into()))?,
    )?;

    Ok(InboxEntry::new(sequence_number, inbox_entry))
}

#[cfg(test)]
mod tests {
    use crate::inbox_table::{
        message_sequence_number_from_slice, write_inbox_key, write_message_key,
    };
    use bytes::{Bytes, BytesMut};
    use common::types::{PartitionKey, ServiceId};

    fn message_key(
        partition_key: PartitionKey,
        service_id: &ServiceId,
        sequence_number: u64,
    ) -> Bytes {
        let mut message_key = BytesMut::new();
        write_message_key(&mut message_key, partition_key, service_id, sequence_number);
        message_key.freeze()
    }

    fn inbox_key(partition_key: PartitionKey, service_id: &ServiceId) -> Bytes {
        let mut message_key = BytesMut::new();
        write_inbox_key(&mut message_key, partition_key, service_id);
        message_key.split().freeze()
    }

    #[test]
    fn read_sequence_number() {
        let message_key = message_key(1337, &ServiceId::new("hi", "key"), 401234);

        let sequence_number = message_sequence_number_from_slice(&message_key);

        assert_eq!(sequence_number, 401234);
    }

    #[test]
    fn inbox_key_covers_all_messages_of_a_service() {
        let prefix_key = inbox_key(1337, &ServiceId::new("svc-1", "key-a"));

        let low_key = message_key(1337, &ServiceId::new("svc-1", "key-a"), 0);
        assert!(low_key.starts_with(&prefix_key));

        let high_key = message_key(1337, &ServiceId::new("svc-1", "key-a"), u64::MAX);
        assert!(high_key.starts_with(&prefix_key));
    }

    #[test]
    fn message_keys_sort_lex() {
        //
        // across services
        //
        assert!(
            message_key(1337, &ServiceId::new("svc-1", ""), 0)
                < message_key(1337, &ServiceId::new("svc-2", ""), 0)
        );
        //
        // same service across keys
        //
        assert!(
            message_key(1337, &ServiceId::new("svc-1", "a"), 0)
                < message_key(1337, &ServiceId::new("svc-1", "b"), 0)
        );
        //
        // within the same service and key
        //
        let mut previous_key = message_key(1337, &ServiceId::new("svc-1", "key-a"), 0);
        for i in 1..300 {
            let current_key = message_key(1337, &ServiceId::new("svc-1", "key-a"), i);
            assert!(previous_key < current_key);
            previous_key = current_key;
        }
    }
}
