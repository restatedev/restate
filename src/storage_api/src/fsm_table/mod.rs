use crate::{GetFuture, GetStream, PutFuture};
use bytes::Bytes;
use common::types::PartitionId;

pub trait FsmTable {
    fn get(&mut self, partition_id: PartitionId, state_id: u64) -> GetFuture<Option<Bytes>>;

    fn put(
        &mut self,
        partition_id: PartitionId,
        state_id: u64,
        state_value: impl AsRef<[u8]>,
    ) -> PutFuture;

    fn clear(&mut self, partition_id: PartitionId, state_id: u64) -> PutFuture;

    fn get_all_states(&mut self, partition_id: PartitionId) -> GetStream<(u64, Bytes)>;
}
