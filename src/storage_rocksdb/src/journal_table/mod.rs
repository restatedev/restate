use crate::composite_keys::write_delimited;
use crate::TableKind::Journal;
use crate::{write_proto_infallible, GetFuture, PutFuture, RocksDBTransaction};
use bytes::{BufMut, BytesMut};
use common::types::{EntryIndex, JournalEntry, PartitionKey, ServiceId};
use prost::Message;
use storage_api::journal_table::JournalTable;
use storage_api::{ready, GetStream, StorageError};
use storage_proto::storage;

#[inline]
fn write_journal_entry_key(
    key: &mut BytesMut,
    partition_key: PartitionKey,
    service_id: &ServiceId,
    journal_index: u32,
) {
    key.put_u64(partition_key);
    write_delimited(&service_id.service_name, key);
    write_delimited(&service_id.key, key);
    key.put_u32(journal_index);
}

#[inline]
fn write_journal_key(key: &mut BytesMut, partition_key: PartitionKey, service_id: &ServiceId) {
    key.put_u64(partition_key);
    write_delimited(&service_id.service_name, key);
    write_delimited(&service_id.key, key);
}

impl JournalTable for RocksDBTransaction {
    fn put_journal_entry(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_index: u32,
        journal_entry: JournalEntry,
    ) -> PutFuture {
        write_journal_entry_key(self.key_buffer(), partition_key, service_id, journal_index);
        write_proto_infallible(
            self.value_buffer(),
            storage::v1::JournalEntry::from(journal_entry),
        );

        self.put_kv_buffer(Journal);

        ready()
    }

    fn get_journal_entry(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_index: u32,
    ) -> GetFuture<Option<JournalEntry>> {
        write_journal_entry_key(self.key_buffer(), partition_key, service_id, journal_index);
        let key = self.clone_key_buffer();
        self.spawn_blocking(move |db| {
            let proto = db.get_proto::<storage::v1::JournalEntry, _>(Journal, key)?;
            proto
                .map(JournalEntry::try_from)
                .transpose()
                .map_err(StorageError::from)
        })
    }

    fn get_journal(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> GetStream<'static, JournalEntry> {
        write_journal_key(self.key_buffer(), partition_key, service_id);
        let key = self.clone_key_buffer();

        self.spawn_background_scan(move |db, tx| {
            let mut iterator = db.prefix_iterator(Journal, key.clone());
            iterator.seek(key);

            for _ in 0..journal_length {
                match iterator.item() {
                    Some((_, v)) => {
                        let entry = storage::v1::JournalEntry::decode(v)
                            .map_err(|error| StorageError::Generic(error.into()))
                            .and_then(|entry| JournalEntry::try_from(entry).map_err(Into::into));
                        if tx.blocking_send(entry).is_err() {
                            break;
                        }
                        iterator.next()
                    }
                    None => {
                        panic!("Unexpected journal size");
                    }
                }
            }
        })
    }

    fn delete_journal(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> PutFuture {
        if journal_length > 1024 {
            // Use delete range for very large ranges.
            // It is not yet clear what is an exact cut of point.
            // TODO: make this a parameter.
            self.range_delete_journal(partition_key, service_id, journal_length);
        } else {
            write_journal_entry_key(self.key_buffer(), partition_key, service_id, 0);
            let mut key_buf = self.clone_key_buffer();
            let key = key_buf.as_mut();
            let journal_index_offset = key.len() - 4;
            for journal_index in 0..journal_length {
                key[journal_index_offset..].copy_from_slice(&journal_index.to_be_bytes());
                self.delete_key(Journal, &key);
            }
        }
        ready()
    }
}

impl RocksDBTransaction {
    fn range_delete_journal(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) {
        write_journal_entry_key(self.key_buffer(), partition_key, service_id, 0);
        let start_key = self.clone_key_buffer();

        write_journal_entry_key(
            self.key_buffer(),
            partition_key,
            service_id,
            journal_length + 1,
        );
        let end_key = self.clone_key_buffer();

        self.delete_range(Journal, start_key, end_key);
    }
}

#[cfg(test)]
mod tests {
    use crate::journal_table::{write_journal_entry_key, write_journal_key};
    use bytes::{Bytes, BytesMut};
    use common::types::{PartitionKey, ServiceId};

    fn journal_entry_key(
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_index: u32,
    ) -> Bytes {
        let mut key = BytesMut::new();
        write_journal_entry_key(&mut key, partition_key, service_id, journal_index);
        key.split().freeze()
    }

    fn journal_key(partition_key: PartitionKey, service_id: &ServiceId) -> Bytes {
        let mut key = BytesMut::new();
        write_journal_key(&mut key, partition_key, service_id);
        key.split().freeze()
    }

    #[test]
    fn journal_key_covers_all_entries_of_a_service() {
        let prefix_key = journal_key(1337, &ServiceId::new("svc-1", "key-a"));

        let low_key = journal_entry_key(1337, &ServiceId::new("svc-1", "key-a"), 0);
        assert!(low_key.starts_with(&prefix_key));

        let high_key = journal_entry_key(1337, &ServiceId::new("svc-1", "key-a"), u32::MAX);
        assert!(high_key.starts_with(&prefix_key));
    }

    #[test]
    fn journal_keys_sort_lex() {
        //
        // across services
        //
        assert!(
            journal_entry_key(1337, &ServiceId::new("svc-1", ""), 0)
                < journal_entry_key(1337, &ServiceId::new("svc-2", ""), 0)
        );
        //
        // same service across keys
        //
        assert!(
            journal_entry_key(1337, &ServiceId::new("svc-1", "a"), 0)
                < journal_entry_key(1337, &ServiceId::new("svc-1", "b"), 0)
        );
        //
        // within the same service and key
        //
        let mut previous_key = journal_entry_key(1337, &ServiceId::new("svc-1", "key-a"), 0);
        for i in 1..300 {
            let current_key = journal_entry_key(1337, &ServiceId::new("svc-1", "key-a"), i);
            assert!(previous_key < current_key);
            previous_key = current_key;
        }
    }
}
