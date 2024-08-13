// Copyright (c) 2023-2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::OptionFuture;
use futures::{future, stream, StreamExt};
use metrics::counter;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, instrument, trace, warn};

use restate_bifrost::Bifrost;
use restate_core::network::NetworkSender;
use restate_core::{
    current_task_partition_id, metadata, task_center, ShutdownError, TaskId, TaskKind,
};
use restate_errors::NotRunningError;
use restate_invoker_api::InvokeInputJournal;
use restate_partition_store::PartitionStore;
use restate_storage_api::deduplication_table::{DedupInformation, EpochSequenceNumber};
use restate_timer::TokioClock;
use restate_types::identifiers::{InvocationId, PartitionKey};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionLeaderEpoch};
use restate_types::logs::LogId;
use restate_types::net::ingress;
use restate_types::storage::StorageEncodeError;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::timer::TimerKeyValue;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

use super::storage::invoker::InvokerStorageReader;
use crate::metric_definitions::PARTITION_HANDLE_LEADER_ACTIONS;
use crate::partition::action_effect_handler::ActionEffectHandler;
use crate::partition::shuffle::{HintSender, Shuffle, ShuffleMetadata};
use crate::partition::state_machine::Action;
use crate::partition::{shuffle, storage};

const BATCH_READY_UP_TO: usize = 10;

type PartitionStorage = storage::PartitionStorage<PartitionStore>;
type TimerService = restate_timer::TimerService<TimerKeyValue, TokioClock, PartitionStorage>;

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
}

#[derive(Debug)]
pub(crate) enum ActionEffect {
    Invoker(restate_invoker_api::Effect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerKeyValue),
    ScheduleCleanupTimer(InvocationId, Duration),
}

pub(crate) struct LeaderState {
    leader_epoch: LeaderEpoch,
    shuffle_hint_tx: HintSender,
    shuffle_task_id: TaskId,
    timer_service: Pin<Box<TimerService>>,
    action_effect_handler: ActionEffectHandler,
    action_effects: VecDeque<ActionEffect>,

    invoker_stream: ReceiverStream<restate_invoker_api::Effect>,
    shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
}

pub enum State {
    Follower,
    Candidate(LeaderEpoch),
    Leader(LeaderState),
}

impl State {
    fn leader_epoch(&self) -> Option<LeaderEpoch> {
        match self {
            State::Follower => None,
            State::Candidate(leader_epoch) => Some(*leader_epoch),
            State::Leader(leader_state) => Some(leader_state.leader_epoch),
        }
    }
}

pub struct PartitionProcessorMetadata {
    node_id: GenerationalNodeId,
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
}

impl PartitionProcessorMetadata {
    pub const fn new(
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> Self {
        Self {
            node_id,
            partition_id,
            partition_key_range,
        }
    }
}

pub(crate) struct LeadershipState<I, N> {
    state: State,
    last_seen_leader_epoch: Option<LeaderEpoch>,

    partition_processor_metadata: PartitionProcessorMetadata,
    num_timers_in_memory_limit: Option<usize>,
    channel_size: usize,
    invoker_tx: I,
    network_tx: N,
    bifrost: Bifrost,
}

impl<I, N> LeadershipState<I, N>
where
    I: restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>>,
    N: NetworkSender + 'static,
{
    pub(crate) fn new(
        partition_processor_metadata: PartitionProcessorMetadata,
        num_timers_in_memory_limit: Option<usize>,
        channel_size: usize,
        invoker_tx: I,
        bifrost: Bifrost,
        network_tx: N,
        last_seen_leader_epoch: Option<LeaderEpoch>,
    ) -> Self {
        Self {
            state: State::Follower,
            partition_processor_metadata,
            num_timers_in_memory_limit,
            channel_size,
            invoker_tx,
            bifrost,
            network_tx,
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
            self.become_follower().await?;
            self.state = State::Candidate(leader_epoch);
            self.announce_leadership(leader_epoch).await?;
            debug!("Running for leadership.");
        } else {
            debug!("Asked to run for leadership with an outdated leader epoch. Ignoring, since futile.")
        }

        Ok(())
    }

    async fn announce_leadership(&mut self, leader_epoch: LeaderEpoch) -> Result<(), Error> {
        let header = Header {
            dest: Destination::Processor {
                partition_key: *self
                    .partition_processor_metadata
                    .partition_key_range
                    .start(),
                dedup: Some(DedupInformation::self_proposal(EpochSequenceNumber::new(
                    leader_epoch,
                ))),
            },
            source: Source::Processor {
                partition_id: self.partition_processor_metadata.partition_id,
                partition_key: Some(
                    *self
                        .partition_processor_metadata
                        .partition_key_range
                        .start(),
                ),
                leader_epoch,
                node_id: self.partition_processor_metadata.node_id,
            },
        };

        let envelope = Envelope::new(
            header,
            Command::AnnounceLeader(AnnounceLeader {
                // todo: Still need to write generational id for supporting rolling back, can be removed
                //  with the next release.
                node_id: Some(self.partition_processor_metadata.node_id),
                leader_epoch,
            }),
        );

        self.bifrost
            .append(
                LogId::from(self.partition_processor_metadata.partition_id),
                Arc::new(envelope),
            )
            .await?;

        Ok(())
    }

    pub async fn step_down(&mut self) -> Result<(), Error> {
        debug!("Stepping down. Being a role model for Joe.");
        self.become_follower().await
    }

    #[instrument(level = "debug", skip_all, fields(leader_epoch = %announce_leader.leader_epoch))]
    pub async fn on_announce_leader(
        &mut self,
        announce_leader: AnnounceLeader,
        partition_storage: &mut PartitionStorage,
    ) -> Result<bool, Error> {
        self.last_seen_leader_epoch = Some(announce_leader.leader_epoch);

        match &self.state {
            State::Follower => {
                debug!("Observed new leader. Staying an obedient follower.");
            }
            State::Candidate(candidacy_epoch) => {
                match candidacy_epoch.cmp(&announce_leader.leader_epoch) {
                    Ordering::Less => {
                        debug!("Lost leadership campaign. Becoming an obedient follower.");
                        self.become_follower().await?;
                    }
                    Ordering::Equal => {
                        debug!("Won the leadership campaign. Becoming the strong leader now.");
                        self.become_leader(partition_storage).await?
                    }
                    Ordering::Greater => {
                        debug!("Observed an intermittent leader. Still believing to win the leadership campaign.");
                    }
                }
            }
            State::Leader(leader_state) => {
                match leader_state.leader_epoch.cmp(&announce_leader.leader_epoch) {
                    Ordering::Less => {
                        debug!("Every reign must end. Stepping down and becoming an obedient follower.");
                        self.become_follower().await?;
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

    async fn become_leader(
        &mut self,
        partition_storage: &mut PartitionStorage,
    ) -> Result<(), Error> {
        if let State::Candidate(leader_epoch) = self.state {
            let invoker_rx = Self::resume_invoked_invocations(
                &mut self.invoker_tx,
                (self.partition_processor_metadata.partition_id, leader_epoch),
                self.partition_processor_metadata
                    .partition_key_range
                    .clone(),
                partition_storage,
                self.channel_size,
            )
            .await?;

            let timer_service = Box::pin(TimerService::new(
                TokioClock,
                self.num_timers_in_memory_limit,
                partition_storage.clone(),
            ));

            let (shuffle_tx, shuffle_rx) = mpsc::channel(self.channel_size);

            let shuffle = Shuffle::new(
                ShuffleMetadata::new(
                    self.partition_processor_metadata.partition_id,
                    leader_epoch,
                    self.partition_processor_metadata.node_id,
                ),
                partition_storage.clone(),
                shuffle_tx,
                self.channel_size,
                self.bifrost.clone(),
            );

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let shuffle_task_id = task_center().spawn_child(
                TaskKind::Shuffle,
                "shuffle",
                Some(self.partition_processor_metadata.partition_id),
                shuffle.run(),
            )?;

            let action_effect_handler = ActionEffectHandler::new(
                self.partition_processor_metadata.partition_id,
                EpochSequenceNumber::new(leader_epoch),
                self.partition_processor_metadata
                    .partition_key_range
                    .clone(),
                self.bifrost.clone(),
                metadata(),
            );

            self.state = State::Leader(LeaderState {
                leader_epoch,
                shuffle_task_id,
                shuffle_hint_tx,
                timer_service,
                action_effect_handler,
                action_effects: VecDeque::default(),
                invoker_stream: ReceiverStream::new(invoker_rx),
                shuffle_stream: ReceiverStream::new(shuffle_rx),
            });

            Ok(())
        } else {
            unreachable!("Can only become the leader if I was the candidate before!");
        }
    }

    async fn resume_invoked_invocations(
        invoker_handle: &mut I,
        partition_leader_epoch: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        partition_storage: &mut PartitionStorage,
        channel_size: usize,
    ) -> Result<mpsc::Receiver<restate_invoker_api::Effect>, Error> {
        let (invoker_tx, invoker_rx) = mpsc::channel(channel_size);

        let storage = partition_storage.clone_storage();
        invoker_handle
            .register_partition(
                partition_leader_epoch,
                partition_key_range,
                InvokerStorageReader::new(storage),
                invoker_tx,
            )
            .await
            .map_err(Error::Invoker)?;

        {
            let invoked_invocations = partition_storage.scan_invoked_invocations();
            tokio::pin!(invoked_invocations);

            let mut count = 0;
            while let Some(invocation_id_and_target) = invoked_invocations.next().await {
                let (invocation_id, invocation_target) = invocation_id_and_target?;
                invoker_handle
                    .invoke(
                        partition_leader_epoch,
                        invocation_id,
                        invocation_target,
                        InvokeInputJournal::NoCachedJournal,
                    )
                    .await
                    .map_err(Error::Invoker)?;
                count += 1;
            }
            debug!("Leader partition resumed {} invocations", count);
        }

        Ok(invoker_rx)
    }

    async fn become_follower(&mut self) -> Result<(), Error> {
        if let State::Leader(LeaderState {
            leader_epoch,
            shuffle_task_id,
            ..
        }) = self.state
        {
            let shuffle_handle = OptionFuture::from(task_center().cancel_task(shuffle_task_id));

            let (shuffle_result, abort_result) = tokio::join!(
                shuffle_handle,
                self.invoker_tx.abort_all_partition((
                    self.partition_processor_metadata.partition_id,
                    leader_epoch
                )),
            );

            abort_result.map_err(Error::Invoker)?;

            if let Some(shuffle_result) = shuffle_result {
                shuffle_result.expect("graceful termination of shuffle task");
            }
        }

        self.state = State::Follower;
        Ok(())
    }

    pub async fn next_action_effects(&mut self) -> Option<Vec<ActionEffect>> {
        match &mut self.state {
            State::Follower | State::Candidate(_) => None,
            State::Leader(leader_state) => {
                let timer_stream = std::pin::pin!(stream::unfold(
                    &mut leader_state.timer_service,
                    |timer_service| async {
                        let timer_value = timer_service.as_mut().next_timer().await;
                        Some((ActionEffect::Timer(timer_value), timer_service))
                    }
                ));

                let invoker_stream = (&mut leader_state.invoker_stream).map(ActionEffect::Invoker);
                let shuffle_stream = (&mut leader_state.shuffle_stream).map(ActionEffect::Shuffle);
                let action_effects_stream =
                    stream::unfold(&mut leader_state.action_effects, |action_effects| {
                        let result = action_effects.pop_front();
                        future::ready(result.map(|r| (r, action_effects)))
                    })
                    .fuse();

                let all_streams = futures::stream_select!(
                    invoker_stream,
                    shuffle_stream,
                    timer_stream,
                    action_effects_stream
                );
                let mut all_streams = all_streams.ready_chunks(BATCH_READY_UP_TO);
                all_streams.next().await
            }
        }
    }

    pub async fn handle_actions(
        &mut self,
        actions: impl Iterator<Item = Action>,
    ) -> Result<(), Error> {
        match &mut self.state {
            State::Follower | State::Candidate(_) => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                for action in actions {
                    trace!(?action, "Apply action");
                    counter!(PARTITION_HANDLE_LEADER_ACTIONS, "action" =>
                        action.name())
                    .increment(1);
                    Self::handle_action(
                        action,
                        (
                            self.partition_processor_metadata.partition_id,
                            leader_state.leader_epoch,
                        ),
                        &mut self.invoker_tx,
                        &leader_state.shuffle_hint_tx,
                        leader_state.timer_service.as_mut(),
                        &mut leader_state.action_effects,
                        &self.network_tx,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_action(
        action: Action,
        partition_leader_epoch: PartitionLeaderEpoch,
        invoker_tx: &mut I,
        shuffle_hint_tx: &HintSender,
        mut timer_service: Pin<&mut TimerService>,
        actions_effects: &mut VecDeque<ActionEffect>,
        network_tx: &N,
    ) -> Result<(), Error> {
        match action {
            Action::Invoke {
                invocation_id,
                invocation_target,
                invoke_input_journal,
            } => invoker_tx
                .invoke(
                    partition_leader_epoch,
                    invocation_id,
                    invocation_target,
                    invoke_input_journal,
                )
                .await
                .map_err(Error::Invoker)?,
            Action::NewOutboxMessage {
                seq_number,
                message,
            } => shuffle_hint_tx.send(shuffle::NewOutboxMessage::new(seq_number, message)),
            Action::RegisterTimer { timer_value } => timer_service.as_mut().add_timer(timer_value),
            Action::DeleteTimer { timer_key } => timer_service.as_mut().remove_timer(timer_key),
            Action::AckStoredEntry {
                invocation_id,
                entry_index,
            } => {
                invoker_tx
                    .notify_stored_entry_ack(partition_leader_epoch, invocation_id, entry_index)
                    .await
                    .map_err(Error::Invoker)?;
            }
            Action::ForwardCompletion {
                invocation_id,
                completion,
            } => invoker_tx
                .notify_completion(partition_leader_epoch, invocation_id, completion)
                .await
                .map_err(Error::Invoker)?,
            Action::AbortInvocation(invocation_id) => invoker_tx
                .abort_invocation(partition_leader_epoch, invocation_id)
                .await
                .map_err(Error::Invoker)?,
            Action::IngressResponse(ingress_response) => {
                Self::send_ingress_message(
                    network_tx.clone(),
                    ingress_response.inner.invocation_id,
                    ingress_response.target_node,
                    ingress::IngressMessage::InvocationResponse(ingress_response.inner),
                )
                .await?;
            }
            Action::IngressSubmitNotification(attach_notification) => {
                Self::send_ingress_message(
                    network_tx.clone(),
                    Some(attach_notification.inner.original_invocation_id),
                    attach_notification.target_node,
                    ingress::IngressMessage::SubmittedInvocationNotification(
                        attach_notification.inner,
                    ),
                )
                .await?;
            }
            Action::ScheduleInvocationStatusCleanup {
                invocation_id,
                retention,
            } => {
                actions_effects
                    .push_back(ActionEffect::ScheduleCleanupTimer(invocation_id, retention));
            }
        }

        Ok(())
    }

    pub async fn handle_action_effect(
        &mut self,
        action_effects: impl IntoIterator<Item = ActionEffect>,
    ) -> anyhow::Result<()> {
        match &mut self.state {
            State::Follower | State::Candidate(_) => {
                // nothing to do :-)
            }
            State::Leader(leader_state) => {
                leader_state
                    .action_effect_handler
                    .handle(action_effects)
                    .await?
            }
        };

        Ok(())
    }

    async fn send_ingress_message(
        network_tx: N,
        invocation_id: Option<InvocationId>,
        target_node: GenerationalNodeId,
        ingress_message: ingress::IngressMessage,
    ) -> Result<(), Error> {
        // NOTE: We dispatch the response in a non-blocking task-center task to avoid
        // blocking partition processor. This comes with the risk of overwhelming the
        // runtime. This should be a temporary solution until we have a better way to
        // handle this case. Options are split into two categories:
        //
        // Category A) Do not block PP's loop if ingress is slow/unavailable
        //   - Add timeout to the disposable task to drop old/stale responses in congestion
        //   scenarios.
        //   - Limit the number of inflight ingress responses (per ingress node) by
        //   mapping node_id -> Vec<TaskId>
        // Category B) Enforce Back-pressure on PP if ingress is slow
        //   - Either directly or through a channel/buffer, block this loop if ingress node
        //   cannot keep up with the responses.
        //
        //  todo: Decide.
        let maybe_task = task_center().spawn_child(
            TaskKind::Disposable,
            "respond-to-ingress",
            current_task_partition_id(),
            {
                async move {
                    if let Err(e) = network_tx.send(target_node.into(), &ingress_message).await {
                        let invocation_id_str = invocation_id
                            .as_ref()
                            .map(|i| i.to_string())
                            .unwrap_or_default();
                        warn!(
                            ?e,
                            ingress.node_id = %target_node,
                            restate.invocation.id = %invocation_id_str,
                            "Failed to send ingress message, will drop the message on the floor"
                        );
                    }
                    Ok(())
                }
            },
        );

        if maybe_task.is_err() {
            let invocation_id_str = invocation_id
                .as_ref()
                .map(|i| i.to_string())
                .unwrap_or_default();
            trace!(
                restate.invocation.id = %invocation_id_str,
                "Partition processor is shutting down, we are not sending the message to ingress",
            );
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskError {
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}

#[cfg(test)]
mod tests {
    use crate::partition::leadership::{
        LeadershipState, PartitionProcessorMetadata, PartitionStorage, State,
    };
    use assert2::let_assert;
    use restate_bifrost::Bifrost;
    use restate_core::TestCoreEnv;
    use restate_invoker_api::test_util::MockInvokerHandle;
    use restate_partition_store::{OpenMode, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_types::config::{CommonOptions, RocksDbOptions, StorageOptions};
    use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey};
    use restate_types::live::Constant;
    use restate_types::logs::{Lsn, SequenceNumber};
    use restate_types::GenerationalNodeId;
    use restate_wal_protocol::control::AnnounceLeader;
    use restate_wal_protocol::{Command, Envelope};
    use std::ops::RangeInclusive;
    use test_log::test;

    const PARTITION_ID: PartitionId = PartitionId::MIN;
    const NODE_ID: GenerationalNodeId = GenerationalNodeId::new(0, 0);
    const PARTITION_KEY_RANGE: RangeInclusive<PartitionKey> = PartitionKey::MIN..=PartitionKey::MAX;
    const PARTITION_PROCESSOR_METADATA: PartitionProcessorMetadata =
        PartitionProcessorMetadata::new(NODE_ID, PARTITION_ID, PARTITION_KEY_RANGE);

    #[test(tokio::test)]
    async fn become_leader_then_step_down() -> googletest::Result<()> {
        let env = TestCoreEnv::create_with_mock_nodes_config(0, 0).await;
        let tc = env.tc.clone();
        let storage_options = StorageOptions::default();
        let rocksdb_options = RocksDbOptions::default();

        tc.run_in_scope_sync("db-manager-init", None, || {
            RocksDbManager::init(Constant::new(CommonOptions::default()))
        });

        let bifrost = tc
            .run_in_scope(
                "init bifrost",
                None,
                Bifrost::init_in_memory(env.metadata.clone()),
            )
            .await;

        tc.run_in_scope("test", None, async {
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
                42,
                invoker_tx,
                bifrost.clone(),
                env.network_sender.clone(),
                None,
            );

            assert!(matches!(state.state, State::Follower));

            let leader_epoch = LeaderEpoch::from(1);
            state.run_for_leader(leader_epoch).await?;

            assert!(matches!(state.state, State::Candidate(_)));

            let record = bifrost
                .read(PARTITION_ID.into(), Lsn::OLDEST)
                .await?
                .unwrap();

            let envelope = record.try_decode::<Envelope>().unwrap()?;

            let_assert!(Command::AnnounceLeader(announce_leader) = envelope.command);
            assert_eq!(
                announce_leader,
                AnnounceLeader {
                    node_id: Some(NODE_ID),
                    leader_epoch
                }
            );

            let storage = partition_store_manager
                .open_partition_store(
                    PARTITION_ID,
                    PARTITION_KEY_RANGE,
                    OpenMode::CreateIfMissing,
                    &rocksdb_options,
                )
                .await?;
            let mut partition_storage =
                PartitionStorage::new(PARTITION_ID, PARTITION_KEY_RANGE, storage);
            state
                .on_announce_leader(announce_leader, &mut partition_storage)
                .await?;

            assert!(matches!(state.state, State::Leader(_)));

            state.step_down().await?;

            assert!(matches!(state.state, State::Follower));

            googletest::Result::Ok(())
        })
        .await?;

        tc.shutdown_node("test_completed", 0).await;
        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
