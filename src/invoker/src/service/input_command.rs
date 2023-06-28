use crate::{
    Effect, InvocationStatusReport, InvokeInputJournal, ServiceHandle, ServiceNotRunning,
    StatusHandle,
};
use futures::future::BoxFuture;
use futures::FutureExt;
use restate_common::journal::Completion;
use restate_common::types::{EntryIndex, PartitionLeaderEpoch, ServiceInvocationId};
use tokio::sync::mpsc;

// -- Input messages

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct InvokeCommand {
    pub(super) partition: PartitionLeaderEpoch,
    pub(super) service_invocation_id: ServiceInvocationId,
    #[serde(skip)]
    pub(super) journal: InvokeInputJournal,
}

#[derive(Debug)]
pub(crate) enum InputCommand {
    Invoke(InvokeCommand),
    Completion {
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    },
    StoredEntryAck {
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    },

    /// Abort specific invocation id
    Abort {
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    },

    /// Command used to clean up internal state when a partition leader is going away
    AbortAllPartition {
        partition: PartitionLeaderEpoch,
    },

    // needed for dynamic registration at Invoker
    RegisterPartition {
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<Effect>,
    },

    // Read status
    ReadStatus(restate_futures_util::command::Command<(), Vec<InvocationStatusReport>>),
}

// -- Handles implementations. This is just glue code between the Input<Command> and the interfaces

#[derive(Debug, Clone)]
pub struct ChannelServiceHandle {
    pub(super) input: mpsc::UnboundedSender<InputCommand>,
}

impl ServiceHandle for ChannelServiceHandle {
    type Future = futures::future::Ready<Result<(), ServiceNotRunning>>;

    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Invoke(InvokeCommand {
                    partition,
                    service_invocation_id,
                    journal,
                }))
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn resume(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Invoke(InvokeCommand {
                    partition,
                    service_invocation_id,
                    journal,
                }))
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Completion {
                    partition,
                    service_invocation_id,
                    completion,
                })
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::StoredEntryAck {
                    partition,
                    service_invocation_id,
                    entry_index,
                })
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::AbortAllPartition { partition })
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn abort_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::Abort {
                    partition,
                    service_invocation_id,
                })
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<Effect>,
    ) -> Self::Future {
        futures::future::ready(
            self.input
                .send(InputCommand::RegisterPartition { partition, sender })
                .map_err(|_| ServiceNotRunning),
        )
    }
}

pub struct ChannelStatusReader(pub(super) mpsc::UnboundedSender<InputCommand>);

impl StatusHandle for ChannelStatusReader {
    type Iterator = itertools::Either<
        std::iter::Empty<InvocationStatusReport>,
        std::vec::IntoIter<InvocationStatusReport>,
    >;
    type Future = BoxFuture<'static, Self::Iterator>;

    fn read_status(&self) -> Self::Future {
        let (cmd, rx) = restate_futures_util::command::Command::prepare(());
        if self.0.send(InputCommand::ReadStatus(cmd)).is_err() {
            return std::future::ready(itertools::Either::Left(std::iter::empty::<
                InvocationStatusReport,
            >()))
            .boxed();
        }
        async move {
            if let Ok(status_vec) = rx.await {
                itertools::Either::Right(status_vec.into_iter())
            } else {
                itertools::Either::Left(std::iter::empty::<InvocationStatusReport>())
            }
        }
        .boxed()
    }
}
