use crate::{GetStream, PutFuture};
use common::types::{PartitionId, ServiceInvocationId};
use storage_proto::storage::v1::Timer;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimerKey {
    pub service_invocation_id: ServiceInvocationId,
    pub journal_index: u32,
    pub timestamp: u64,
}

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
