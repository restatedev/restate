use crate::{GetFuture, GetStream, PutFuture};
use common::types::{PartitionKey, ServiceId};
use storage_proto::storage::v1::InboxEntry;

pub trait InboxTable {
    fn put_invocation(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        sequence_number: u64,
        entry: InboxEntry,
    ) -> PutFuture;

    fn delete_invocation(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
        sequence_number: u64,
    ) -> PutFuture;

    fn peek_inbox(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetFuture<Option<(u64, InboxEntry)>>;

    fn inbox(
        &mut self,
        partition_key: PartitionKey,
        service_id: &ServiceId,
    ) -> GetStream<(u64, InboxEntry)>;
}
