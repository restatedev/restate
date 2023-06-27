use crate::partition::effects::ActuatorMessage;
use crate::partition::shuffle::Shuffle;
use crate::partition::{shuffle, storage, AckResponse, TimerValue};
use futures::{future, Stream, StreamExt};
use restate_common::types::{
    LeaderEpoch, PartitionId, PartitionLeaderEpoch, PeerId, ServiceInvocationId,
};
use restate_invoker::{InvokeInputJournal, ServiceNotRunning};
use restate_network::NetworkNotRunning;
use restate_timer::TokioClock;
use std::fmt::Debug;
use std::panic;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinError;

mod actuator;

pub(crate) use actuator::{ActuatorMessageCollector, ActuatorOutput, ActuatorStream};
use restate_storage_rocksdb::RocksDBStorage;

pub(crate) trait InvocationReader {
    type InvokedInvocationStream<'a>: Stream<
        Item = Result<ServiceInvocationId, restate_storage_api::StorageError>,
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
    InvokerInputSender: restate_invoker::ServiceHandle,
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
    ) -> Result<
        (
            ActuatorStream,
            LeadershipState<'a, InvokerInputSender, NetworkHandle>,
        ),
        Error,
    > {
        if let LeadershipState::Follower { .. } = self {
            self.unchecked_become_leader(leader_epoch, partition_storage)
                .await
        } else {
            let (_, follower_state) = self.become_follower().await?;

            follower_state
                .unchecked_become_leader(leader_epoch, partition_storage)
                .await
        }
    }

    async fn unchecked_become_leader(
        self,
        leader_epoch: LeaderEpoch,
        partition_storage: &'a PartitionStorage,
    ) -> Result<
        (
            ActuatorStream,
            LeadershipState<'a, InvokerInputSender, NetworkHandle>,
        ),
        Error,
    > {
        if let LeadershipState::Follower(mut follower_state) = self {
            let invoker_rx = Self::register_at_invoker(
                &mut follower_state.invoker_tx,
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
                ActuatorStream::leader(invoker_rx, shuffle_rx),
                LeadershipState::Leader {
                    follower_state,
                    leader_state: LeaderState {
                        leader_epoch,
                        shutdown_signal,
                        shuffle_hint_tx,
                        shuffle_handle,
                        timer_service,
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

    async fn register_at_invoker(
        invoker_handle: &mut InvokerInputSender,
        partition_leader_epoch: PartitionLeaderEpoch,
        partition_storage: &PartitionStorage,
        channel_size: usize,
    ) -> Result<mpsc::Receiver<restate_invoker::Effect>, Error> {
        let (invoker_tx, invoker_rx) = mpsc::channel(channel_size);

        invoker_handle
            .register_partition(partition_leader_epoch, invoker_tx)
            .await?;

        let mut transaction = partition_storage.create_transaction();

        {
            let invoked_invocations = transaction.scan_invoked_invocations();
            tokio::pin!(invoked_invocations);

            while let Some(service_invocation_id) = invoked_invocations.next().await {
                let service_invocation_id = service_invocation_id?;

                invoker_handle
                    .invoke(
                        partition_leader_epoch,
                        service_invocation_id,
                        InvokeInputJournal::NoCachedJournal,
                    )
                    .await?;
            }
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
