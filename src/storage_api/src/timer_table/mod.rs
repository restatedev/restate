use crate::{GetStream, PutFuture};
use restate_common::types::{PartitionId, ServiceInvocation, ServiceInvocationId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimerKey {
    pub service_invocation_id: ServiceInvocationId,
    pub journal_index: u32,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Timer {
    CompleteSleepEntry,
    Invoke(ServiceInvocation),
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
        exclusive_start: Option<&TimerKey>,
        limit: usize,
    ) -> GetStream<(TimerKey, Timer)>;
}
