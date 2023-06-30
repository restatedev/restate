use super::Effect;

use restate_types::identifiers::ServiceInvocationId;
use restate_types::identifiers::{EntryIndex, PartitionLeaderEpoch};
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::Completion;
use restate_types::journal::JournalMetadata;
use std::future::Future;
use tokio::sync::mpsc;

#[derive(Debug, Default)]
pub enum InvokeInputJournal {
    #[default]
    NoCachedJournal,
    CachedJournal(JournalMetadata, Vec<PlainRawEntry>),
}

// TODO move this to restate_errors, we have several copies of this type (e.g. NetworkNotRunning)
#[derive(Debug, thiserror::Error)]
#[error("invoker is not running")]
pub struct ServiceNotRunning;

pub trait ServiceHandle {
    type Future: Future<Output = Result<(), ServiceNotRunning>>;

    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future;

    fn resume(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future;

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) -> Self::Future;

    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) -> Self::Future;

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future;

    fn abort_invocation(
        &mut self,
        partition_leader_epoch: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) -> Self::Future;

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<Effect>,
    ) -> Self::Future;
}
