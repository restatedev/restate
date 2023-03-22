use crate::{GetFuture, GetStream, PutFuture};
use common::types::{EntryIndex, PartitionKey, ServiceId};
use storage_proto::storage::v1::JournalEntry;

pub trait JournalTable {
    fn put_journal_entry(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_index: u32,
        journal_entry: JournalEntry,
    ) -> PutFuture;

    fn get_journal_entry(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_index: u32,
    ) -> GetFuture<Option<JournalEntry>>;

    fn get_journal(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> GetStream<JournalEntry>;

    fn delete_journal(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> PutFuture;
}
