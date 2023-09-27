// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::effects::{ActuatorMessage, StateStorage, StateStorageError};
use crate::partition::shuffle::Shuffle;
use crate::partition::{shuffle, storage, AckResponse, TimerValue};
use assert2::let_assert;
use futures::{future, Stream, StreamExt};
use restate_invoker_api::{InvokeInputJournal, ServiceNotRunning};
use restate_network::NetworkNotRunning;
use restate_timer::TokioClock;
use std::fmt::Debug;
use std::panic;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinError;

mod actuator;

use crate::partition::services::non_deterministic;
use crate::partition::state_machine::{StateReader, StateReaderError};
pub(crate) use actuator::{ActuatorMessageCollector, ActuatorOutput, ActuatorStream};
use restate_schema_impl::Schemas;
use restate_storage_api::status_table::InvocationStatus;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionLeaderEpoch, PeerId};
use restate_types::journal::raw::EntryHeader;
use restate_types::journal::EntryType;

pub(crate) trait InvocationReader {
    type InvokedInvocationStream<'a>: Stream<
        Item = Result<FullInvocationId, restate_storage_api::StorageError>,
    >
    where
        Self: 'a;

    fn scan_invoked_invocations(&mut self) -> Self::InvokedInvocationStream<'_>;
}

type PartitionStorage = storage::PartitionStorage<RocksDBStorage>;
type TimerService<'a> = restate_timer::TimerService<'a, TimerValue, TokioClock, PartitionStorage>;

pub(crate) struct LeaderState<'a> {
    leader_epoch: LeaderEpoch,
    shutdown_signal: drain::Signal,
    shuffle_hint_tx: mpsc::Sender<shuffle::NewOutboxMessage>,
    shuffle_handle: task::JoinHandle<Result<(), anyhow::Error>>,
    message_buffer: Vec<ActuatorMessage>,
    timer_service: Pin<Box<TimerService<'a>>>,
    non_deterministic_service_invoker: non_deterministic::ServiceInvoker<'a>,
}

pub(crate) struct FollowerState<I, N> {
    peer_id: PeerId,
    partition_id: PartitionId,
    timer_service_options: restate_timer::Options,
    channel_size: usize,
    invoker_tx: I,
    network_handle: N,
    ack_tx: restate_network::PartitionProcessorSender<AckResponse>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invoker is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Invoker(#[from] ServiceNotRunning),
    #[error("network is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Network(#[from] NetworkNotRunning),
    #[error("shuffle failed. This indicates a bug or the system is shutting down: {0}")]
    FailedShuffleTask(#[from] anyhow::Error),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
    #[error(transparent)]
    StateStorage(#[from] StateStorageError),
    #[error(transparent)]
    StateReader(#[from] StateReaderError),
}

pub(crate) enum LeadershipState<'a, InvokerInputSender, NetworkHandle> {
    Follower(FollowerState<InvokerInputSender, NetworkHandle>),

    Leader {
        follower_state: FollowerState<InvokerInputSender, NetworkHandle>,
        leader_state: LeaderState<'a>,
    },
}

impl<'a, InvokerInputSender, NetworkHandle> LeadershipState<'a, InvokerInputSender, NetworkHandle>
where
    InvokerInputSender: restate_invoker_api::ServiceHandle,
    NetworkHandle: restate_network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
{
    pub(crate) fn follower(
        peer_id: PeerId,
        partition_id: PartitionId,
        timer_service_options: restate_timer::Options,
        channel_size: usize,
        invoker_tx: InvokerInputSender,
        network_handle: NetworkHandle,
        ack_tx: restate_network::PartitionProcessorSender<AckResponse>,
    ) -> (ActuatorStream, Self) {
        (
            ActuatorStream::Follower,
            Self::Follower(FollowerState {
                peer_id,
                partition_id,
                timer_service_options,
                channel_size,
                invoker_tx,
                network_handle,
                ack_tx,
            }),
        )
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self, LeadershipState::Leader { .. })
    }

    pub(crate) async fn become_leader(
        self,
        leader_epoch: LeaderEpoch,
        partition_storage: &'a PartitionStorage,
        schemas: &'a Schemas,
    ) -> Result<
        (
            ActuatorStream,
            LeadershipState<'a, InvokerInputSender, NetworkHandle>,
        ),
        Error,
    > {
        if let LeadershipState::Follower { .. } = self {
            self.unchecked_become_leader(leader_epoch, partition_storage, schemas)
                .await
        } else {
            let (_, follower_state) = self.become_follower().await?;

            follower_state
                .unchecked_become_leader(leader_epoch, partition_storage, schemas)
                .await
        }
    }

    async fn unchecked_become_leader(
        self,
        leader_epoch: LeaderEpoch,
        partition_storage: &'a PartitionStorage,
        schemas: &'a Schemas,
    ) -> Result<
        (
            ActuatorStream,
            LeadershipState<'a, InvokerInputSender, NetworkHandle>,
        ),
        Error,
    > {
        if let LeadershipState::Follower(mut follower_state) = self {
            let (mut service_invoker, service_invoker_output_rx) =
                non_deterministic::ServiceInvoker::new(partition_storage, schemas);

            let invoker_rx = Self::resume_invoked_invocations(
                &mut follower_state.invoker_tx,
                &mut service_invoker,
                (follower_state.partition_id, leader_epoch),
                partition_storage,
                follower_state.channel_size,
            )
            .await?;

            let timer_service = Box::pin(
                follower_state
                    .timer_service_options
                    .build(partition_storage, TokioClock),
            );

            let (shuffle_tx, shuffle_rx) = mpsc::channel(follower_state.channel_size);

            let shuffle = Shuffle::new(
                follower_state.peer_id,
                follower_state.partition_id,
                partition_storage.clone(),
                follower_state.network_handle.create_shuffle_sender(),
                shuffle_tx,
                follower_state.channel_size,
            );

            follower_state
                .network_handle
                .register_shuffle(shuffle.peer_id(), shuffle.create_network_sender())
                .await?;

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let (shutdown_signal, shutdown_watch) = drain::channel();

            let shuffle_handle = tokio::spawn(shuffle.run(shutdown_watch));

            Ok((
                ActuatorStream::leader(invoker_rx, shuffle_rx, service_invoker_output_rx),
                LeadershipState::Leader {
                    follower_state,
                    leader_state: LeaderState {
                        leader_epoch,
                        shutdown_signal,
                        shuffle_hint_tx,
                        shuffle_handle,
                        timer_service,
                        non_deterministic_service_invoker: service_invoker,
                        // The max number of actuator messages should be 2 atm (e.g. RegisterTimer and
                        // AckStoredEntry)
                        message_buffer: Vec::with_capacity(2),
                    },
                },
            ))
        } else {
            unreachable!("This method should only be called if I am a follower!");
        }
    }

    async fn resume_invoked_invocations(
        invoker_handle: &mut InvokerInputSender,
        built_in_service_invoker: &mut non_deterministic::ServiceInvoker<'_>,
        partition_leader_epoch: PartitionLeaderEpoch,
        partition_storage: &PartitionStorage,
        channel_size: usize,
    ) -> Result<mpsc::Receiver<restate_invoker_api::Effect>, Error> {
        let (invoker_tx, invoker_rx) = mpsc::channel(channel_size);

        invoker_handle
            .register_partition(partition_leader_epoch, invoker_tx)
            .await?;

        let mut transaction = partition_storage.create_transaction();

        let mut built_in_invoked_services = Vec::new();

        {
            let invoked_invocations = transaction.scan_invoked_invocations();
            tokio::pin!(invoked_invocations);

            while let Some(full_invocation_id) = invoked_invocations.next().await {
                let full_invocation_id = full_invocation_id?;

                if !non_deterministic::ServiceInvoker::is_supported(
                    &full_invocation_id.service_id.service_name,
                ) {
                    invoker_handle
                        .invoke(
                            partition_leader_epoch,
                            full_invocation_id,
                            InvokeInputJournal::NoCachedJournal,
                        )
                        .await?;
                } else {
                    built_in_invoked_services.push(full_invocation_id);
                }
            }
        }

        for full_invocation_id in built_in_invoked_services {
            let input_entry = transaction
                .load_journal_entry(&full_invocation_id.service_id, 0)
                .await?
                .expect("first journal entry must be present; if not, this indicates a bug.");

            assert_eq!(
                input_entry.header.to_entry_type(),
                EntryType::Custom,
                "first journal entry must be a '{}' for built in services",
                EntryType::Custom
            );

            let status = transaction
                .get_invocation_status(&full_invocation_id.service_id)
                .await?;

            let_assert!(InvocationStatus::Invoked(metadata) = status);

            let method = metadata.journal_metadata.method;
            let response_sink = metadata.response_sink;
            let argument = input_entry.entry;
            built_in_service_invoker
                .invoke(
                    full_invocation_id,
                    &method,
                    metadata.journal_metadata.span_context,
                    response_sink,
                    argument,
                )
                .await;
        }

        transaction.commit().await?;

        Ok(invoker_rx)
    }

    pub(crate) async fn become_follower(
        self,
    ) -> Result<
        (
            ActuatorStream,
            LeadershipState<'a, InvokerInputSender, NetworkHandle>,
        ),
        Error,
    > {
        if let LeadershipState::Leader {
            follower_state:
                FollowerState {
                    peer_id,
                    partition_id,
                    channel_size,
                    timer_service_options: num_in_memory_timers,
                    mut invoker_tx,
                    network_handle,
                    ack_tx,
                },
            leader_state:
                LeaderState {
                    leader_epoch,
                    shutdown_signal,
                    shuffle_handle,
                    ..
                },
        } = self
        {
            // trigger shut down of all leadership tasks
            shutdown_signal.drain().await;

            let (shuffle_result, abort_result, network_unregister_result) = tokio::join!(
                shuffle_handle,
                invoker_tx.abort_all_partition((partition_id, leader_epoch)),
                network_handle.unregister_shuffle(peer_id),
            );

            abort_result?;
            network_unregister_result?;

            Self::unwrap_task_result(shuffle_result)?;

            Ok(Self::follower(
                peer_id,
                partition_id,
                num_in_memory_timers,
                channel_size,
                invoker_tx,
                network_handle,
                ack_tx,
            ))
        } else {
            Ok((ActuatorStream::Follower, self))
        }
    }

    fn unwrap_task_result<E>(result: Result<Result<(), E>, JoinError>) -> Result<(), E> {
        if let Err(err) = result {
            if err.is_panic() {
                panic::resume_unwind(err.into_panic());
            }

            Ok(())
        } else {
            result.unwrap()
        }
    }

    pub(crate) fn into_message_collector(
        self,
    ) -> ActuatorMessageCollector<'a, InvokerInputSender, NetworkHandle> {
        match self {
            LeadershipState::Follower(follower_state) => {
                ActuatorMessageCollector::Follower(follower_state)
            }
            LeadershipState::Leader {
                follower_state,
                mut leader_state,
            } => {
                leader_state.message_buffer.clear();
                ActuatorMessageCollector::Leader {
                    follower_state,
                    leader_state,
                }
            }
        }
    }

    pub(crate) async fn run_tasks(&mut self) -> TaskResult {
        match self {
            LeadershipState::Follower { .. } => future::pending().await,
            LeadershipState::Leader {
                leader_state:
                    LeaderState {
                        shuffle_handle,
                        timer_service,
                        ..
                    },
                ..
            } => {
                tokio::select! {
                    result = shuffle_handle => TaskResult::TerminatedTask(Self::into_task_result("shuffle", result)),
                    timer = timer_service.as_mut().next_timer() => TaskResult::Timer(timer)
                }
            }
        }
    }

    fn into_task_result<E: Into<anyhow::Error>>(
        name: &'static str,
        result: Result<Result<(), E>, JoinError>,
    ) -> TokioTaskResult {
        if let Err(err) = result {
            if err.is_panic() {
                panic::resume_unwind(err.into_panic());
            }

            TokioTaskResult::FailedTask {
                name,
                error: TaskError::Cancelled,
            }
        } else {
            let result = result.unwrap();

            result
                .err()
                .map(|err| TokioTaskResult::FailedTask {
                    name,
                    error: TaskError::Error(err.into()),
                })
                .unwrap_or(TokioTaskResult::TerminatedTask(name))
        }
    }
}

#[derive(Debug)]
pub(crate) enum TaskResult {
    TerminatedTask(TokioTaskResult),
    Timer(TimerValue),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TokioTaskResult {
    #[error("task '{0}' terminated unexpectedly")]
    TerminatedTask(&'static str),
    #[error("task '{name}' failed: {error}")]
    FailedTask {
        name: &'static str,
        error: TaskError,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskError {
    #[error("task was cancelled")]
    Cancelled,
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}
