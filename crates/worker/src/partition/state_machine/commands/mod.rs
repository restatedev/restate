use super::actions::ActionCollector;
use crate::metric_definitions::PARTITION_APPLY_COMMAND;
use crate::partition::state_machine::tracing::{
    state_machine_apply_command_span, StateMachineSpanExt,
};
use crate::partition::state_machine::StateMachineContext;
use metrics::histogram;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::Transaction;
use restate_types::identifiers::InvocationId;
use restate_types::journal::raw::{RawEntryCodec, RawEntryCodecError};
use restate_wal_protocol::Command;
use std::marker::PhantomData;
use std::time::Instant;
use tracing::{Instrument, Span};

mod invoker;
mod purge_invocation_request;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to deserialize entry: {0}")]
    Codec(#[from] RawEntryCodecError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

/// Command context, used by the [`ApplicableCommand`] trait to access storage, generate actions and update the state machine context.
///
/// This also contains some helpers used by different [`ApplicableCommand`] implementations.
pub(super) struct CommandContext<'a, Storage, Actions, Codec> {
    pub(super) state_machine: &'a mut StateMachineContext,
    pub(super) transaction: &'a mut Storage,
    pub(super) actions: &'a mut Actions,

    _codec: PhantomData<Codec>,
}

impl<'a, Storage, Actions, Codec> CommandContext<'a, Storage, Actions, Codec> {
    pub(super) fn prepare(
        state_machine: &'a mut StateMachineContext,
        transaction: &'a mut Storage,
        actions: &'a mut Actions,
    ) -> Self {
        Self {
            state_machine,
            transaction,
            actions,
            _codec: Default::default(),
        }
    }
}

impl<'a, Storage: ReadOnlyInvocationStatusTable, Actions, Codec>
    CommandContext<'a, Storage, Actions, Codec>
{
    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> Result<InvocationStatus, Error> {
        Span::record_invocation_id(invocation_id);
        let status = self
            .transaction
            .get_invocation_status(invocation_id)
            .await?;
        if let Some(invocation_target) = status.invocation_target() {
            Span::record_invocation_target(invocation_target);
        }
        Ok(status)
    }
}

// TODO we need this trait only to workaround the fact that not all the FSM business logic has been moved to ApplicableCommand.
pub(super) trait MaybeApplicableCommand<Storage, Actions, Codec>
where
    Self: Sized,
{
    /// This returns another layer of result, where the first error case means that the new infra still cannot process the command.
    async fn apply(
        self,
        ctx: CommandContext<Storage, Actions, Codec>,
    ) -> Result<Result<(), Error>, Self>;
}

/// A command implementing this trait can be applied to the state machine.
pub(super) trait ApplicableCommand<Storage, Actions, Codec> {
    async fn apply(self, ctx: CommandContext<Storage, Actions, Codec>) -> Result<(), Error>;
}

impl<Storage, Actions, Codec> MaybeApplicableCommand<Storage, Actions, Codec> for Command
where
    Storage: Transaction + Send,
    Actions: ActionCollector,
    Codec: RawEntryCodec,
{
    async fn apply(
        self,
        ctx: CommandContext<'_, Storage, Actions, Codec>,
    ) -> Result<Result<(), Error>, Self> {
        let span = state_machine_apply_command_span(ctx.state_machine, &self);
        async {
            let start = Instant::now();
            let command_type = self.name();
            let res = match self {
                Command::PurgeInvocation(cmd) => Ok(cmd.apply(ctx).await),
                Command::InvokerEffect(cmd) => cmd.apply(ctx).await.map_err(Command::InvokerEffect),
                cmd => return Err(cmd),
            };
            histogram!(PARTITION_APPLY_COMMAND, "command" => command_type).record(start.elapsed());
            res
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod mocks {
    use super::*;

    use crate::partition::state_machine::Action;
    use restate_core::{task_center, TaskCenter, TaskCenterBuilder};
    use restate_partition_store::{
        OpenMode, PartitionStore, PartitionStoreManager, RocksDBTransaction,
    };
    use restate_rocksdb::RocksDbManager;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_storage_api::Transaction;
    use restate_types::arc_util::Constant;
    use restate_types::config::{CommonOptions, WorkerOptions};
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use std::fmt;
    use std::ops::RangeInclusive;
    use tracing::info;

    pub struct MockStateMachine {
        state_machine_context: StateMachineContext,
        // TODO for the time being we use rocksdb storage because we have no mocks for storage interfaces.
        //  Perhaps we could make these tests faster by having those.
        rocksdb_storage: PartitionStore,
    }

    impl MockStateMachine {
        pub async fn init() -> (TaskCenter, MockStateMachine) {
            let tc = TaskCenterBuilder::default()
                .default_runtime_handle(tokio::runtime::Handle::current())
                .build()
                .expect("task_center builds");
            let state_machine = tc
                .run_in_scope("mock-state-machine", None, MockStateMachine::new())
                .await;

            (tc, state_machine)
        }

        async fn new() -> Self {
            task_center().run_in_scope_sync("db-manager-init", None, || {
                RocksDbManager::init(Constant::new(CommonOptions::default()))
            });
            let worker_options = WorkerOptions::default();
            info!(
                "Using RocksDB temp directory {}",
                worker_options.storage.data_dir().display()
            );
            let manager = PartitionStoreManager::create(
                Constant::new(worker_options.storage.clone()),
                Constant::new(worker_options.storage.rocksdb.clone()),
                &[],
            )
            .await
            .unwrap();
            let rocksdb_storage = manager
                .open_partition_store(
                    PartitionId::MIN,
                    RangeInclusive::new(PartitionKey::MIN, PartitionKey::MAX),
                    OpenMode::CreateIfMissing,
                    &worker_options.storage.rocksdb,
                )
                .await
                .unwrap();

            Self {
                state_machine_context: StateMachineContext {
                    inbox_seq_number: 0,
                    outbox_seq_number: 0,
                    partition_key_range: PartitionKey::MIN..=PartitionKey::MAX,
                    is_leader: false,
                },
                rocksdb_storage,
            }
        }

        pub async fn maybe_apply<
            Cmd: for<'a> MaybeApplicableCommand<
                    RocksDBTransaction<'a>,
                    Vec<Action>,
                    ProtobufRawEntryCodec,
                > + fmt::Debug,
        >(
            &mut self,
            cmd: Cmd,
        ) -> Vec<Action> {
            let mut transaction = self.rocksdb_storage.transaction();
            let mut action_collector = vec![];
            MaybeApplicableCommand::<_, _, ProtobufRawEntryCodec>::apply(
                cmd,
                CommandContext::prepare(
                    &mut self.state_machine_context,
                    &mut transaction,
                    &mut action_collector,
                ),
            )
            .await
            .expect("This command is not yet supported by the new test infra")
            .unwrap();

            transaction.commit().await.unwrap();

            action_collector
        }

        pub async fn apply<
            Cmd: for<'a> ApplicableCommand<RocksDBTransaction<'a>, Vec<Action>, ProtobufRawEntryCodec>,
        >(
            &mut self,
            cmd: Cmd,
        ) -> Vec<Action> {
            let mut transaction = self.rocksdb_storage.transaction();
            let mut action_collector = vec![];
            ApplicableCommand::<_, _, ProtobufRawEntryCodec>::apply(
                cmd,
                CommandContext::prepare(
                    &mut self.state_machine_context,
                    &mut transaction,
                    &mut action_collector,
                ),
            )
            .await
            .unwrap();

            transaction.commit().await.unwrap();

            action_collector
        }

        pub fn storage(&mut self) -> &mut PartitionStore {
            &mut self.rocksdb_storage
        }
    }
}
