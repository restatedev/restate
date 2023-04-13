use crate::{GetStream, PutFuture};
use common::types::{PartitionId, Timer, TimerKey};

pub trait TimerTable {
    fn add_timer(
        &mut self,
        partition_id: PartitionId,
        timer_key: &TimerKey,
        timer: Timer,
    ) -> PutFuture;

    fn delete_timer(&mut self, partition_id: PartitionId, timer_key: &TimerKey) -> PutFuture;

    fn next_timers_greater_than(
        &mut self,
        partition_id: PartitionId,
        exclusive_start: &TimerKey,
        limit: usize,
    ) -> GetStream<(TimerKey, Timer)>;
}
