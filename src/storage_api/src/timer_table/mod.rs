use crate::{GetStream, PutFuture};
use restate_common::types::{PartitionId, SequencedTimer, TimerKey};

pub trait TimerTable {
    fn add_timer(
        &mut self,
        partition_id: PartitionId,
        timer_key: &TimerKey,
        seq_timer: SequencedTimer,
    ) -> PutFuture;

    fn delete_timer(&mut self, partition_id: PartitionId, timer_key: &TimerKey) -> PutFuture;

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> GetStream<(TimerKey, SequencedTimer)>;
}
