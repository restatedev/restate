use crate::{GetFuture, GetStream, PutFuture};
use restate_types::identifiers::PartitionId;

pub trait DeduplicationTable {
    fn get_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producing_partition_id: PartitionId,
    ) -> GetFuture<Option<u64>>;

    fn put_sequence_number(
        &mut self,
        partition_id: PartitionId,
        producing_partition_id: PartitionId,
        sequence_number: u64,
    ) -> PutFuture;

    fn get_all_sequence_numbers(
        &mut self,
        partition_id: PartitionId,
    ) -> GetStream<(PartitionId, u64)>;
}
