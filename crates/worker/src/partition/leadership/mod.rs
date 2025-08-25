// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod durability_tracker;
mod leader_state;
mod self_proposer;
pub mod trim_queue;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::mem;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, instrument, warn};

use restate_bifrost::Bifrost;
use restate_core::network::{Oneshot, Reciprocal};
use restate_core::{ShutdownError, TaskCenter, TaskKind, my_node_id};
use restate_errors::NotRunningError;
use restate_invoker_api::InvokeInputJournal;
use restate_partition_store::PartitionStore;
use restate_storage_api::deduplication_table::EpochSequenceNumber;
use restate_storage_api::fsm_table::ReadOnlyFsmTable;
use restate_storage_api::invocation_status_table::{
    InvokedInvocationStatusLite, ScanInvocationStatusTable,
};
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::timer_table::{TimerKey, TimerTable};
use restate_timer::TokioClock;
use restate_types::GenerationalNodeId;
use restate_types::cluster::cluster_state::RunMode;
use restate_types::config::Configuration;
use restate_types::errors::GenericError;
use restate_types::identifiers::{InvocationId, PartitionKey, PartitionProcessorRpcRequestId};
use restate_types::identifiers::{LeaderEpoch, PartitionLeaderEpoch};
use restate_types::message::MessageIndex;
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcResponse,
};
use restate_types::partitions::Partition;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::retries::with_jitter;
use restate_types::storage::StorageEncodeError;
use restate_wal_protocol::Command;
use restate_wal_protocol::control::{AnnounceLeader, PartitionDurability};
use restate_wal_protocol::timer::TimerKeyValue;

use crate::partition::cleaner::Cleaner;
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::leadership::leader_state::LeaderState;
use crate::partition::leadership::self_proposer::SelfProposer;
use crate::partition::shuffle;
use crate::partition::shuffle::{OutboxReaderError, Shuffle, ShuffleMetadata};
use crate::partition::state_machine::Action;
use crate::partition::types::InvokerEffect;

use self::durability_tracker::DurabilityTracker;
use self::trim_queue::{LogTrimmer, TrimQueue};

type TimerService = restate_timer::TimerService<TimerKeyValue, TokioClock, TimerReader>;
type InvokerStream = ReceiverStream<InvokerEffect>;

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
    Invoker(Box<restate_invoker_api::Effect>),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerKeyValue),
    ScheduleCleanupTimer(InvocationId, Duration),
    PartitionMaintenance(PartitionDurability),
    AwaitingRpcSelfProposeDone,
}
enum State {
    Follower,
    Candidate {
        leader_epoch: LeaderEpoch,
        // to be able to move out of it
        self_proposer: Option<SelfProposer>,
    },
    Leader(Box<LeaderState>),
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

pub(crate) struct LeadershipState<I> {
    state: State,
    last_seen_leader_epoch: Option<LeaderEpoch>,

    partition: Arc<Partition>,
    invoker_tx: I,
    bifrost: Bifrost,
    #[allow(unused)]
    trim_queue: TrimQueue,
}

impl<I> LeadershipState<I>
where
    I: restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        partition: Arc<Partition>,
        invoker_tx: I,
        bifrost: Bifrost,
        last_seen_leader_epoch: Option<LeaderEpoch>,
        trim_queue: TrimQueue,
    ) -> Self {
        Self {
            state: State::Follower,
            partition,
            invoker_tx,
            bifrost,
            last_seen_leader_epoch,
            trim_queue,
        }
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self.state, State::Leader(_))
    }

    pub fn effective_mode(&self) -> RunMode {
        match self.state {
            State::Follower | State::Candidate { .. } => RunMode::Follower,
            State::Leader(_) => RunMode::Leader,
        }
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
            debug!(
                "Asked to run for leadership with an outdated leader epoch. Ignoring, since futile."
            )
        }

        Ok(())
    }

    async fn announce_leadership(&mut self, leader_epoch: LeaderEpoch) -> Result<(), Error> {
        let announce_leader = Command::AnnounceLeader(Box::new(AnnounceLeader {
            // todo: Still need to write generational id for supporting rolling back, can be removed
            //  with the next release.
            node_id: Some(my_node_id()),
            leader_epoch,
            partition_key_range: Some(self.partition.key_range.clone()),
        }));

        let mut self_proposer = SelfProposer::new(
            self.partition.partition_id,
            EpochSequenceNumber::new(leader_epoch),
            &self.bifrost,
        )?;

        self_proposer
            .propose(*self.partition.key_range.start(), announce_leader)
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

    pub async fn maybe_step_down(
        &mut self,
        new_leader_epoch: LeaderEpoch,
        new_leader_node: GenerationalNodeId,
    ) {
        match &self.state {
            State::Follower => {}
            State::Candidate { leader_epoch, .. } => match leader_epoch.cmp(&new_leader_epoch) {
                Ordering::Less => {
                    debug!(
                        "Lost leadership campaign. Conceding to {} at epoch {}",
                        new_leader_node, new_leader_epoch
                    );
                    self.become_follower().await;
                }
                Ordering::Equal => { /* nothing do to */ }
                Ordering::Greater => { /* we are in the future */ }
            },
            State::Leader(leader_state) => match leader_state.leader_epoch.cmp(&new_leader_epoch) {
                Ordering::Less => {
                    debug!(
                        my_leadership_epoch = %leader_state.leader_epoch,
                        %new_leader_epoch,
                        "Every reign must end. Stepping down and becoming an conceding to {} at epoch {}",
                        new_leader_node, new_leader_epoch
                    );
                    self.become_follower().await;
                }
                Ordering::Equal => {}
                Ordering::Greater => {}
            },
        }
    }

    #[instrument(level = "debug", skip_all, fields(leader_epoch = %announce_leader.leader_epoch))]
    pub async fn on_announce_leader(
        &mut self,
        announce_leader: &AnnounceLeader,
        partition_store: &mut PartitionStore,
        replica_set_states: &PartitionReplicaSetStates,
        config: &Configuration,
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
                        self.become_leader(partition_store, replica_set_states.clone(), config)
                            .await?
                    }
                    Ordering::Greater => {
                        debug!(
                            "Observed an intermittent leader. Still believing to win the leadership campaign."
                        );
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
                        warn!(
                            "Observed another leadership announcement for my own leadership. This should never happen and indicates a bug!"
                        );
                    }
                    Ordering::Greater => {
                        warn!(
                            "Observed a leadership announcement for an outdated epoch. This should never happen and indicates a bug!"
                        );
                    }
                }
            }
        }

        Ok(self.is_leader())
    }

    async fn become_leader(
        &mut self,
        partition_store: &mut PartitionStore,
        replica_set_states: PartitionReplicaSetStates,
        config: &Configuration,
    ) -> Result<(), Error> {
        if let State::Candidate {
            leader_epoch,
            self_proposer,
        } = &mut self.state
        {
            let invoker_rx = Self::resume_invoked_invocations(
                &mut self.invoker_tx,
                (self.partition.partition_id, *leader_epoch),
                self.partition.key_range.clone(),
                partition_store,
                config.worker.internal_queue_length(),
            )
            .await?;

            let timer_service = TimerService::new(
                TokioClock,
                config.worker.num_timers_in_memory_limit(),
                TimerReader::from(partition_store.clone()),
            );

            let (shuffle_tx, shuffle_rx) = mpsc::channel(config.worker.internal_queue_length());

            let shuffle = Shuffle::new(
                ShuffleMetadata::new(self.partition.partition_id, *leader_epoch),
                OutboxReader::from(partition_store.clone()),
                shuffle_tx,
                config.worker.internal_queue_length(),
                self.bifrost.clone(),
            );

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let shuffle_task_handle =
                TaskCenter::spawn_unmanaged(TaskKind::Shuffle, "shuffle", shuffle.run())?;

            let cleaner = Cleaner::new(
                self.partition.partition_id,
                *leader_epoch,
                partition_store.clone(),
                self.bifrost.clone(),
                self.partition.key_range.clone(),
                config.worker.cleanup_interval(),
            );

            let cleaner_task_id =
                TaskCenter::spawn_child(TaskKind::Cleaner, "cleaner", cleaner.run())?;

            let trimmer_task_id = LogTrimmer::spawn(
                self.bifrost.clone(),
                self.partition.log_id(),
                self.trim_queue.clone(),
            )?;

            let mut self_proposer = self_proposer.take().expect("must be present");
            self_proposer.mark_as_leader().await;

            let last_reported_durable_lsn = partition_store
                .get_partition_durability()
                .await?
                .map(|d| d.durable_point);

            let durability_tracker = DurabilityTracker::new(
                self.partition.partition_id,
                last_reported_durable_lsn,
                replica_set_states,
                partition_store.partition_db().watch_archived_lsn(),
                with_jitter(Duration::from_secs(5), 0.5),
            );

            self.state = State::Leader(Box::new(LeaderState::new(
                self.partition.partition_id,
                *leader_epoch,
                *self.partition.key_range.start(),
                shuffle_task_handle,
                cleaner_task_id,
                trimmer_task_id,
                shuffle_hint_tx,
                timer_service,
                self_proposer,
                invoker_rx,
                shuffle_rx,
                durability_tracker,
            )));

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
            let invoked_invocations = partition_store
                .scan_invoked_invocations()
                .map_err(Error::Storage)?;
            tokio::pin!(invoked_invocations);

            let mut count = 0;
            while let Some(invoked_invocation) = invoked_invocations.next().await {
                let InvokedInvocationStatusLite {
                    invocation_id,
                    invocation_target,
                    current_invocation_epoch,
                } = invoked_invocation?;
                invoker_handle
                    .invoke(
                        partition_leader_epoch,
                        invocation_id,
                        current_invocation_epoch,
                        invocation_target,
                        InvokeInputJournal::NoCachedJournal,
                    )
                    .await
                    .map_err(Error::Invoker)?;
                count += 1;
            }
            debug!("Leader partition resumed {} invocations", count);
        }

        Ok(ReceiverStream::new(invoker_rx))
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
}

impl<I> LeadershipState<I> {
    pub async fn handle_rpc_proposal_command(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        reciprocal: Reciprocal<
            Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
        partition_key: PartitionKey,
        cmd: Command,
    ) {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => {
                // Just fail the rpc
                reciprocal.send(Err(PartitionProcessorRpcError::NotLeader(
                    self.partition.partition_id,
                )))
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
        reciprocal: Reciprocal<
            Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
        success_response: PartitionProcessorRpcResponse,
    ) {
        match &mut self.state {
            State::Follower | State::Candidate { .. } => reciprocal.send(Err(
                PartitionProcessorRpcError::NotLeader(self.partition.partition_id),
            )),
            State::Leader(leader_state) => {
                leader_state
                    .self_propose_and_respond_asynchronously(
                        partition_key,
                        cmd,
                        reciprocal,
                        success_response,
                    )
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
            .expect("timers should be read from storage successfully")
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

#[cfg(test)]
mod tests {
    use crate::partition::leadership::trim_queue::TrimQueue;
    use crate::partition::leadership::{LeadershipState, State};
    use assert2::let_assert;
    use restate_bifrost::Bifrost;
    use restate_core::{TaskCenter, TestCoreEnv};
    use restate_invoker_api::test_util::MockInvokerHandle;
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_types::GenerationalNodeId;
    use restate_types::config::{CommonOptions, Configuration};
    use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
    use restate_types::live::Constant;
    use restate_types::logs::{KeyFilter, Lsn, SequenceNumber};
    use restate_types::partitions::Partition;
    use restate_types::partitions::state::PartitionReplicaSetStates;
    use restate_wal_protocol::control::AnnounceLeader;
    use restate_wal_protocol::{Command, Envelope};
    use std::ops::RangeInclusive;
    use std::sync::Arc;
    use test_log::test;
    use tokio_stream::StreamExt;

    const PARTITION_ID: PartitionId = PartitionId::MIN;
    const NODE_ID: GenerationalNodeId = GenerationalNodeId::new(0, 0);
    const PARTITION_KEY_RANGE: RangeInclusive<PartitionKey> = PartitionKey::MIN..=PartitionKey::MAX;
    const PARTITION: Partition = Partition::new(PARTITION_ID, PARTITION_KEY_RANGE);

    #[test(restate_core::test)]
    async fn become_leader_then_step_down() -> googletest::Result<()> {
        let env = TestCoreEnv::create_with_single_node(0, 0).await;

        RocksDbManager::init(Constant::new(CommonOptions::default()));
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;
        let replica_set_states = PartitionReplicaSetStates::default();

        let partition_store_manager = PartitionStoreManager::create().await?;

        let invoker_tx = MockInvokerHandle::default();
        let mut state = LeadershipState::new(
            Arc::new(PARTITION),
            invoker_tx,
            bifrost.clone(),
            None,
            TrimQueue::default(),
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
            *announce_leader,
            AnnounceLeader {
                node_id: Some(NODE_ID),
                leader_epoch,
                partition_key_range: Some(PARTITION_KEY_RANGE),
            }
        );

        let mut partition_store = partition_store_manager.open(&PARTITION, None).await?;
        state
            .on_announce_leader(
                &announce_leader,
                &mut partition_store,
                &replica_set_states,
                &Configuration::pinned(),
            )
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
