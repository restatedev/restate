// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod leader_state;
mod self_proposer;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem;
use std::ops::RangeInclusive;
use std::time::Duration;

use futures::{stream, StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::network::Reciprocal;
use restate_core::{my_node_id, ShutdownError, TaskCenter, TaskKind};
use restate_errors::NotRunningError;
use restate_invoker_api::InvokeInputJournal;
use restate_partition_store::PartitionStore;
use restate_storage_api::deduplication_table::EpochSequenceNumber;
use restate_storage_api::invocation_status_table::{
    InvokedOrKilledInvocationStatusLite, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::timer_table::{TimerKey, TimerTable};
use restate_timer::TokioClock;
use restate_types::errors::{GenericError, KILLED_INVOCATION_ERROR};
use restate_types::identifiers::{InvocationId, PartitionKey, PartitionProcessorRpcRequestId};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionLeaderEpoch};
use restate_types::message::MessageIndex;
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcResponse,
};
use restate_types::storage::StorageEncodeError;
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::Command;

use crate::partition::cleaner::Cleaner;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::leadership::leader_state::LeaderState;
use crate::partition::leadership::self_proposer::SelfProposer;
use crate::partition::shuffle::{OutboxReaderError, Shuffle, ShuffleMetadata};
use crate::partition::state_machine::Action;
use crate::partition::types::{InvokerEffect, InvokerEffectKind};
use crate::partition::{respond_to_rpc, shuffle};

type TimerService = restate_timer::TimerService<TimerKeyValue, TokioClock, TimerReader>;
type InvokerStream =
    stream::Chain<stream::Iter<std::vec::IntoIter<InvokerEffect>>, ReceiverStream<InvokerEffect>>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invoker is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Invoker(NotRunningError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
    #[error("failed writing to bifrost: {0}")]
    Bifrost(#[from] restate_bifrost::Error),
    #[error("failed serializing payload: {0}")]
    Codec(#[from] StorageEncodeError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
    #[error("error when self proposing")]
    SelfProposer,
    #[error("task '{name}' failed: {cause}")]
    TaskFailed {
        name: &'static str,
        cause: TaskTermination,
    },
}

impl Error {
    fn task_terminated_unexpectedly(name: &'static str) -> Self {
        Error::TaskFailed {
            name,
            cause: TaskTermination::Unexpected,
        }
    }

    fn task_failed(name: &'static str, err: impl Into<GenericError>) -> Self {
        Error::TaskFailed {
            name,
            cause: TaskTermination::Failure(err.into()),
        }
    }
}

#[derive(Debug, derive_more::Display)]
pub(crate) enum TaskTermination {
    #[display("unexpected termination")]
    Unexpected,
    #[display("{}", _0)]
    Failure(GenericError),
}

#[derive(Debug)]
pub(crate) enum ActionEffect {
    Invoker(restate_invoker_api::Effect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerKeyValue),
    ScheduleCleanupTimer(InvocationId, Duration),
    AwaitingRpcSelfProposeDone,
}
enum State {
    Follower,
    Candidate {
        leader_epoch: LeaderEpoch,
        // to be able to move out of it
        self_proposer: Option<SelfProposer>,
    },
    Leader(LeaderState),
}

impl State {
    fn leader_epoch(&self) -> Option<LeaderEpoch> {
        match self {
            State::Follower => None,
            State::Candidate { leader_epoch, .. } => Some(*leader_epoch),
            State::Leader(leader_state) => Some(leader_state.leader_epoch),
        }
    }
}

pub struct PartitionProcessorMetadata {
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
}

impl PartitionProcessorMetadata {
    pub const fn new(
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> Self {
        Self {
            partition_id,
            partition_key_range,
        }
    }
}

pub(crate) struct LeadershipState<I> {
    state: State,
    last_seen_leader_epoch: Option<LeaderEpoch>,

    partition_processor_metadata: PartitionProcessorMetadata,
    num_timers_in_memory_limit: Option<usize>,
    cleanup_interval: Duration,
    channel_size: usize,
    invoker_tx: I,
    bifrost: Bifrost,
}

impl<I> LeadershipState<I>
where
    I: restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        partition_processor_metadata: PartitionProcessorMetadata,
        num_timers_in_memory_limit: Option<usize>,
        cleanup_interval: Duration,
        channel_size: usize,
        invoker_tx: I,
        bifrost: Bifrost,
        last_seen_leader_epoch: Option<LeaderEpoch>,
    ) -> Self {
        Self {
            state: State::Follower,
            partition_processor_metadata,
            num_timers_in_memory_limit,
            cleanup_interval,
            channel_size,
            invoker_tx,
            bifrost,
            last_seen_leader_epoch,
        }
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader(_))
    }

    fn is_new_leader_epoch(&self, leader_epoch: LeaderEpoch) -> bool {
        if let Some(max_leader_epoch) = self.state.leader_epoch().or(self.last_seen_leader_epoch) {
            max_leader_epoch < leader_epoch
        } else {
            true
        }
    }

    #[instrument(level = "debug", skip_all, fields(leader_epoch = %leader_epoch))]
    pub async fn run_for_leader(&mut self, leader_epoch: LeaderEpoch) -> Result<(), Error> {
        if self.is_new_leader_epoch(leader_epoch) {
            self.become_follower().await;
            self.announce_leadership(leader_epoch).await?;
            debug!("Running for leadership.");
        } else {
            debug!("Asked to run for leadership with an outdated leader epoch. Ignoring, since futile.")
        }

        Ok(())
    }

    async fn announce_leadership(&mut self, leader_epoch: LeaderEpoch) -> Result<(), Error> {
        let announce_leader = Command::AnnounceLeader(AnnounceLeader {
            // todo: Still need to write generational id for supporting rolling back, can be removed
            //  with the next release.
            node_id: Some(my_node_id()),
            leader_epoch,
            partition_key_range: Some(
                self.partition_processor_metadata
                    .partition_key_range
                    .clone(),
            ),
        });

        let mut self_proposer = SelfProposer::new(
            self.partition_processor_metadata.partition_id,
            EpochSequenceNumber::new(leader_epoch),
            &self.bifrost,
        )?;

        self_proposer
            .propose(
                *self
                    .partition_processor_metadata
                    .partition_key_range
                    .start(),
                announce_leader,
            )
            .await?;

        self.state = State::Candidate {
            leader_epoch,
            self_proposer: Some(self_proposer),
        };

        Ok(())
    }

    pub async fn step_down(&mut self) {
        debug!("Stepping down. Being a role model for Joe.");
        self.become_follower().await
    }

    #[instrument(level = "debug", skip_all, fields(leader_epoch = %announce_leader.leader_epoch))]
    pub async fn on_announce_leader(
        &mut self,
        announce_leader: AnnounceLeader,
        partition_store: &mut PartitionStore,
    ) -> Result<bool, Error> {
        self.last_seen_leader_epoch = Some(announce_leader.leader_epoch);

        match &self.state {
            State::Follower => {
                debug!("Observed new leader. Staying an obedient follower.");
            }
            State::Candidate { leader_epoch, .. } => {
                match leader_epoch.cmp(&announce_leader.leader_epoch) {
                    Ordering::Less => {
                        debug!("Lost leadership campaign. Becoming an obedient follower.");
                        self.become_follower().await;
                    }
                    Ordering::Equal => {
                        debug!("Won the leadership campaign. Becoming the strong leader now.");
                        self.become_leader(partition_store).await?
                    }
                    Ordering::Greater => {
                        debug!("Observed an intermittent leader. Still believing to win the leadership campaign.");
                    }
                }
            }
            State::Leader(leader_state) => {
                match leader_state.leader_epoch.cmp(&announce_leader.leader_epoch) {
                    Ordering::Less => {
                        debug!(
                            my_leadership_epoch = %leader_state.leader_epoch,
                            new_leader_epoch = %announce_leader.leader_epoch,
                            "Every reign must end. Stepping down and becoming an obedient follower."
                        );
                        self.become_follower().await;
                    }
                    Ordering::Equal => {
                        warn!("Observed another leadership announcement for my own leadership. This should never happen and indicates a bug!");
                    }
                    Ordering::Greater => {
                        warn!("Observed a leadership announcement for an outdated epoch. This should never happen and indicates a bug!");
                    }
                }
            }
        }

        Ok(self.is_leader())
    }

    async fn become_leader(&mut self, partition_store: &mut PartitionStore) -> Result<(), Error> {
        if let State::Candidate {
            leader_epoch,
            self_proposer,
        } = &mut self.state
        {
            let invoker_rx = Self::resume_invoked_invocations(
                &mut self.invoker_tx,
                (
                    self.partition_processor_metadata.partition_id,
                    *leader_epoch,
                ),
                self.partition_processor_metadata
                    .partition_key_range
                    .clone(),
                partition_store,
                self.channel_size,
            )
            .await?;

            let timer_service = TimerService::new(
                TokioClock,
                self.num_timers_in_memory_limit,
                TimerReader::from(partition_store.clone()),
            );

            let (shuffle_tx, shuffle_rx) = mpsc::channel(self.channel_size);

            let shuffle = Shuffle::new(
                ShuffleMetadata::new(
                    self.partition_processor_metadata.partition_id,
                    *leader_epoch,
                ),
                OutboxReader::from(partition_store.clone()),
                shuffle_tx,
                self.channel_size,
                self.bifrost.clone(),
            );

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let shuffle_task_handle =
                TaskCenter::spawn_unmanaged(TaskKind::Shuffle, "shuffle", shuffle.run())?;

            let cleaner = Cleaner::new(
                self.partition_processor_metadata.partition_id,
                *leader_epoch,
                partition_store.clone(),
                self.bifrost.clone(),
                self.partition_processor_metadata
                    .partition_key_range
                    .clone(),
                self.cleanup_interval,
            );

            let cleaner_task_id =
                TaskCenter::spawn_child(TaskKind::Cleaner, "cleaner", cleaner.run())?;

            self.state = State::Leader(LeaderState::new(
                self.partition_processor_metadata.partition_id,
                *leader_epoch,
                *self
                    .partition_processor_metadata
                    .partition_key_range
                    .start(),
                shuffle_task_handle,
                cleaner_task_id,
                shuffle_hint_tx,
                timer_service,
                self_proposer.take().expect("must be present"),
                invoker_rx,
                shuffle_rx,
            ));

            Ok(())
        } else {
            unreachable!("Can only become the leader if I was the candidate before!");
        }
    }

    async fn resume_invoked_invocations(
        invoker_handle: &mut I,
        partition_leader_epoch: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        partition_store: &mut PartitionStore,
        channel_size: usize,
    ) -> Result<InvokerStream, Error> {
        let (invoker_tx, invoker_rx) = mpsc::channel(channel_size);

        let mut killed_invocations_effects = vec![];

        invoker_handle
            .register_partition(
                partition_leader_epoch,
                partition_key_range,
                InvokerStorageReader::new(partition_store.clone()),
                invoker_tx,
            )
            .await
            .map_err(Error::Invoker)?;

        {
            let invoked_or_killed_invocations = partition_store.all_invoked_or_killed_invocations();
            tokio::pin!(invoked_or_killed_invocations);

            let mut count = 0;
            while let Some(invoked_or_killed_invocation) =
                invoked_or_killed_invocations.next().await
            {
                let InvokedOrKilledInvocationStatusLite {
                    invocation_id,
                    invocation_target,
                    is_invoked,
                } = invoked_or_killed_invocation?;
                if is_invoked {
                    invoker_handle
                        .invoke(
                            partition_leader_epoch,
                            invocation_id,
                            invocation_target,
                            InvokeInputJournal::NoCachedJournal,
                        )
                        .await
                        .map_err(Error::Invoker)?;
                } else {
                    // For killed invocations, there's no need to go through the invoker
                    // We simply return here the effect as if the invoker produced that.
                    killed_invocations_effects.push(InvokerEffect {
                        invocation_id,
                        kind: InvokerEffectKind::Failed(KILLED_INVOCATION_ERROR),
                    });
                }
                count += 1;
            }
            debug!("Leader partition resumed {} invocations", count);
        }

        Ok(
            futures::stream::iter(killed_invocations_effects)
                .chain(ReceiverStream::new(invoker_rx)),
        )
    }

    async fn become_follower(&mut self) {
        let old_state = mem::replace(&mut self.state, State::Follower);

        match old_state {
            State::Follower => {
                // nothing to do :-)
            }
            State::Candidate { .. } => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                leader_state.stop(&mut self.invoker_tx).await;
            }
        }
    }

    pub async fn handle_actions(
        &mut self,
        actions: impl Iterator<Item = Action>,
    ) -> Result<(), Error> {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                leader_state
                    .handle_actions(&mut self.invoker_tx, actions)
                    .await?;
            }
        }

        Ok(())
    }

    /// Runs the leadership state tasks. This depends on the current state value:
    ///
    /// * Follower: Nothing to do
    /// * Candidate: Monitor appender task
    /// * Leader: Await action effects and monitor appender task
    pub async fn run(&mut self) -> Result<Vec<ActionEffect>, Error> {
        match &mut self.state {
            State::Follower => Ok(futures::future::pending::<Vec<_>>().await),
            State::Candidate { self_proposer, .. } => Err(self_proposer
                .as_mut()
                .expect("must be present")
                .join_on_err()
                .await
                .expect_err("never should never be returned")),
            State::Leader(leader_state) => leader_state.run().await,
        }
    }

    pub async fn handle_action_effects(
        &mut self,
        action_effects: impl IntoIterator<Item = ActionEffect>,
    ) -> Result<(), Error> {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                leader_state.handle_action_effects(action_effects).await?
            }
        }

        Ok(())
    }

    pub async fn handle_rpc_proposal_command(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        reciprocal: Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        partition_key: PartitionKey,
        cmd: Command,
    ) {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => {
                // Just fail the rpc
                respond_to_rpc(
                    reciprocal.prepare(Err(PartitionProcessorRpcError::NotLeader(
                        self.partition_processor_metadata.partition_id,
                    ))),
                );
            }
            State::Leader(leader_state) => {
                leader_state
                    .handle_rpc_proposal_command(request_id, reciprocal, partition_key, cmd)
                    .await;
            }
        }
    }

    /// Self propose to this partition, and register the reciprocal to respond asynchronously.
    pub async fn self_propose_and_respond_asynchronously(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        reciprocal: Reciprocal<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
    ) {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => respond_to_rpc(reciprocal.prepare(Err(
                PartitionProcessorRpcError::NotLeader(
                    self.partition_processor_metadata.partition_id,
                ),
            ))),
            State::Leader(leader_state) => {
                leader_state
                    .self_propose_and_respond_asynchronously(partition_key, cmd, reciprocal)
                    .await;
            }
        }
    }
}
#[derive(Debug, derive_more::From)]
struct TimerReader(PartitionStore);

impl restate_timer::TimerReader<TimerKeyValue> for TimerReader {
    async fn get_timers(
        &mut self,
        num_timers: usize,
        previous_timer_key: Option<TimerKey>,
    ) -> Vec<TimerKeyValue> {
        self.0
            .next_timers_greater_than(previous_timer_key.as_ref(), num_timers)
            .map(|result| result.map(|(timer_key, timer)| TimerKeyValue::new(timer_key, timer)))
            // TODO: Update timer service to maintain transaction while reading the timer stream: See https://github.com/restatedev/restate/issues/273
            // have to collect the stream because it depends on the local transaction
            .try_collect::<Vec<_>>()
            .await
            // TODO: Extend TimerReader to return errors: See https://github.com/restatedev/restate/issues/274
            .expect("timer deserialization should not fail")
    }
}

#[derive(Debug, derive_more::From)]
struct OutboxReader(PartitionStore);

impl shuffle::OutboxReader for OutboxReader {
    async fn get_next_message(
        &mut self,
        next_sequence_number: MessageIndex,
    ) -> Result<Option<(MessageIndex, OutboxMessage)>, OutboxReaderError> {
        let result = if let Some((message_index, outbox_message)) =
            self.0.get_next_outbox_message(next_sequence_number).await?
        {
            Some((message_index, outbox_message))
        } else {
            None
        };

        Ok(result)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskError {
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use crate::partition::leadership::{LeadershipState, PartitionProcessorMetadata, State};
    use assert2::let_assert;
    use restate_bifrost::Bifrost;
    use restate_core::{TaskCenter, TestCoreEnv};
    use restate_invoker_api::test_util::MockInvokerHandle;
    use restate_partition_store::{OpenMode, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{CommonOptions, RocksDbOptions, StorageOptions};
    use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
    use restate_types::live::Constant;
    use restate_types::logs::{KeyFilter, Lsn, SequenceNumber};
    use restate_types::GenerationalNodeId;
    use restate_wal_protocol::control::AnnounceLeader;
    use restate_wal_protocol::{Command, Envelope};
    use std::ops::RangeInclusive;
    use std::time::Duration;
    use test_log::test;
    use tokio_stream::StreamExt;

    const PARTITION_ID: PartitionId = PartitionId::MIN;
    const NODE_ID: GenerationalNodeId = GenerationalNodeId::new(0, 0);
    const PARTITION_KEY_RANGE: RangeInclusive<PartitionKey> = PartitionKey::MIN..=PartitionKey::MAX;
    const PARTITION_PROCESSOR_METADATA: PartitionProcessorMetadata =
        PartitionProcessorMetadata::new(PARTITION_ID, PARTITION_KEY_RANGE);

    #[test(restate_core::test)]
    async fn become_leader_then_step_down() -> googletest::Result<()> {
        let _env = TestCoreEnv::create_with_single_node(0, 0).await;
        let storage_options = StorageOptions::default();
        let rocksdb_options = RocksDbOptions::default();

        RocksDbManager::init(Constant::new(CommonOptions::default()));
        let bifrost = Bifrost::init_in_memory().await;

        let partition_store_manager = PartitionStoreManager::create(
            Constant::new(storage_options.clone()).boxed(),
            Constant::new(rocksdb_options.clone()).boxed(),
            &[(PARTITION_ID, PARTITION_KEY_RANGE)],
        )
        .await?;

        let invoker_tx = MockInvokerHandle::default();
        let mut state = LeadershipState::new(
            PARTITION_PROCESSOR_METADATA,
            None,
            Duration::from_secs(60 * 60),
            42,
            invoker_tx,
            bifrost.clone(),
            None,
        );

        assert!(matches!(state.state, State::Follower));

        let leader_epoch = LeaderEpoch::from(1);
        state.run_for_leader(leader_epoch).await?;

        assert!(matches!(state.state, State::Candidate { .. }));

        let record = bifrost
            .create_reader(PARTITION_ID.into(), KeyFilter::Any, Lsn::OLDEST, Lsn::MAX)
            .expect("valid reader")
            .next()
            .await
            .unwrap()?;

        let envelope = record.try_decode::<Envelope>().unwrap()?;

        let_assert!(Command::AnnounceLeader(announce_leader) = envelope.command);
        assert_eq!(
            announce_leader,
            AnnounceLeader {
                node_id: Some(NODE_ID),
                leader_epoch,
                partition_key_range: Some(PARTITION_KEY_RANGE),
            }
        );

        let mut partition_store = partition_store_manager
            .open_partition_store(
                PARTITION_ID,
                PARTITION_KEY_RANGE,
                OpenMode::CreateIfMissing,
                &rocksdb_options,
            )
            .await?;
        state
            .on_announce_leader(announce_leader, &mut partition_store)
            .await?;

        assert!(matches!(state.state, State::Leader(_)));

        state.step_down().await;

        assert!(matches!(state.state, State::Follower));

        TaskCenter::current()
            .shutdown_node("test_completed", 0)
            .await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
