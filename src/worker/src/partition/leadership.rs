use crate::partition::effects::{ActuatorMessage, MessageCollector};
use crate::partition::shuffle::Shuffle;
use crate::partition::storage::PartitionStorage;
use crate::partition::{shuffle, AckResponse, TimerValue};
use futures::{future, Stream, StreamExt};
use restate_common::types::{
    LeaderEpoch, PartitionId, PartitionLeaderEpoch, PeerId, ServiceInvocationId,
};
use restate_common::utils::GenericError;
use restate_invoker::{InvokeInputJournal, InvokerInputSender, InvokerNotRunning};
use restate_network::NetworkNotRunning;
use restate_timer::TokioClock;
use std::fmt::Debug;
use std::ops::DerefMut;
use std::panic;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinError;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, info_span, trace, warn, warn_span};

pub(super) trait InvocationReader {
    type InvokedInvocationStream<'a>: Stream<
        Item = Result<ServiceInvocationId, restate_storage_api::StorageError>,
    >
    where
        Self: 'a;

    fn scan_invoked_invocations(&mut self) -> Self::InvokedInvocationStream<'_>;
}

type TimerService<'a, Storage> =
    restate_timer::TimerService<'a, TimerValue, TokioClock, PartitionStorage<Storage>>;

pub(super) struct LeaderState<'a, Storage>
where
    Storage: restate_storage_api::Storage + Send + Sync,
{
    leader_epoch: LeaderEpoch,
    shutdown_signal: drain::Signal,
    shuffle_hint_tx: mpsc::Sender<shuffle::NewOutboxMessage>,
    shuffle_handle: task::JoinHandle<Result<(), anyhow::Error>>,
    message_buffer: Vec<ActuatorMessage>,
    timer_service: Pin<Box<TimerService<'a, Storage>>>,
}

pub(super) struct FollowerState<I, N> {
    peer_id: PeerId,
    partition_id: PartitionId,
    timer_service_options: restate_timer::Options,
    channel_size: usize,
    invoker_tx: I,
    network_handle: N,
    ack_tx: restate_network::PartitionProcessorSender<AckResponse>,
}

pub(super) enum ActuatorMessageCollector<'a, I, N, S>
where
    S: restate_storage_api::Storage + Send + Sync,
{
    Leader {
        follower_state: FollowerState<I, N>,
        leader_state: LeaderState<'a, S>,
    },
    Follower(FollowerState<I, N>),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActuatorMessageCollectorError {
    #[error(transparent)]
    Invoker(#[from] InvokerNotRunning),
    #[error("failed to send ack response: {0}")]
    Ack(#[from] mpsc::error::SendError<AckResponse>),
}

impl<'a, I, N, S> ActuatorMessageCollector<'a, I, N, S>
where
    I: InvokerInputSender,
    N: restate_network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
    S: restate_storage_api::Storage + Send + Sync,
{
    pub(super) async fn send(
        self,
    ) -> Result<LeadershipState<'a, I, N, S>, ActuatorMessageCollectorError> {
        match self {
            ActuatorMessageCollector::Leader {
                mut follower_state,
                mut leader_state,
            } => {
                Self::send_actuator_messages(
                    (follower_state.partition_id, leader_state.leader_epoch),
                    &mut follower_state.invoker_tx,
                    &mut leader_state.shuffle_hint_tx,
                    leader_state.timer_service.as_mut(),
                    &follower_state.ack_tx,
                    leader_state.message_buffer.drain(..),
                )
                .await?;

                Ok(LeadershipState::Leader {
                    follower_state,
                    leader_state,
                })
            }
            ActuatorMessageCollector::Follower(follower_state) => {
                Ok(LeadershipState::Follower(follower_state))
            }
        }
    }

    async fn send_actuator_messages(
        partition_leader_epoch: PartitionLeaderEpoch,
        invoker_tx: &mut I,
        shuffle_hint_tx: &mut mpsc::Sender<shuffle::NewOutboxMessage>,
        mut timer_service: Pin<&mut TimerService<'a, S>>,
        ack_tx: &restate_network::PartitionProcessorSender<AckResponse>,
        messages: impl IntoIterator<Item = ActuatorMessage>,
    ) -> Result<(), ActuatorMessageCollectorError> {
        for message in messages.into_iter() {
            trace!(?message, "Send actuator message");

            match message {
                ActuatorMessage::Invoke {
                    service_invocation_id,
                    invoke_input_journal,
                } => {
                    invoker_tx
                        .invoke(
                            partition_leader_epoch,
                            service_invocation_id,
                            invoke_input_journal,
                        )
                        .await?
                }
                ActuatorMessage::NewOutboxMessage {
                    seq_number,
                    message,
                } => {
                    // it is ok if this message is not sent since it is only a hint
                    let _ = shuffle_hint_tx
                        .try_send(shuffle::NewOutboxMessage::new(seq_number, message));
                }
                ActuatorMessage::RegisterTimer { timer_value } => {
                    timer_service.as_mut().add_timer(timer_value)
                }
                ActuatorMessage::AckStoredEntry {
                    service_invocation_id,
                    entry_index,
                } => {
                    invoker_tx
                        .notify_stored_entry_ack(
                            partition_leader_epoch,
                            service_invocation_id,
                            entry_index,
                        )
                        .await?;
                }
                ActuatorMessage::ForwardCompletion {
                    service_invocation_id,
                    completion,
                } => {
                    invoker_tx
                        .notify_completion(
                            partition_leader_epoch,
                            service_invocation_id,
                            completion,
                        )
                        .await?
                }
                ActuatorMessage::CommitEndSpan {
                    service_invocation_id,
                    service_method,
                    span_context,
                    result,
                } => {
                    let span = match result {
                        Ok(_) => {
                            let span = info_span!(
                                "end_invocation",
                                rpc.service = %service_invocation_id.service_id.service_name,
                                rpc.method = %service_method,
                                restate.invocation.sid = %service_invocation_id,
                                restate.invocation.result = "Success"
                            );
                            info!(parent: &span, "Invocation succeeded");
                            span
                        }
                        Err((status_code, status_message)) => {
                            let span = warn_span!(
                                "end_invocation",
                                rpc.service = %service_invocation_id.service_id.service_name,
                                rpc.method = %service_method,
                                restate.invocation.sid = %service_invocation_id,
                                restate.invocation.result = "Failure"
                            );
                            warn!(
                                parent: &span,
                                "Invocation failed ({}): {}", status_code, status_message
                            );
                            span
                        }
                    };
                    span_context.as_parent().attach_to_span(&span);
                    let _ = span.enter();
                }
                ActuatorMessage::SendAckResponse(ack_response) => ack_tx.send(ack_response).await?,
                ActuatorMessage::AbortInvocation(service_invocation_id) => {
                    invoker_tx
                        .abort_invocation(partition_leader_epoch, service_invocation_id)
                        .await?
                }
            }
        }

        Ok(())
    }
}

impl<'a, I, N, S> MessageCollector for ActuatorMessageCollector<'a, I, N, S>
where
    S: restate_storage_api::Storage + Send + Sync,
{
    fn collect(&mut self, message: ActuatorMessage) {
        match self {
            ActuatorMessageCollector::Leader {
                leader_state: LeaderState { message_buffer, .. },
                ..
            } => {
                message_buffer.push(message);
            }
            ActuatorMessageCollector::Follower(..) => {}
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invoker is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Invoker(#[from] InvokerNotRunning),
    #[error("network is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Network(#[from] NetworkNotRunning),
    #[error("shuffle failed. This indicates a bug or the system is shutting down: {0}")]
    FailedShuffleTask(#[from] anyhow::Error),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

pub(super) enum LeadershipState<'a, InvokerInputSender, NetworkHandle, Storage>
where
    Storage: restate_storage_api::Storage + Send + Sync,
{
    Follower(FollowerState<InvokerInputSender, NetworkHandle>),

    Leader {
        follower_state: FollowerState<InvokerInputSender, NetworkHandle>,
        leader_state: LeaderState<'a, Storage>,
    },
}

impl<'a, InvokerInputSender, NetworkHandle, Storage>
    LeadershipState<'a, InvokerInputSender, NetworkHandle, Storage>
where
    InvokerInputSender: restate_invoker::InvokerInputSender,
    NetworkHandle: restate_network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
    Storage: restate_storage_api::Storage + Clone + Send + Sync + 'static,
{
    pub(super) fn follower(
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

    pub(super) fn is_leader(&self) -> bool {
        matches!(self, LeadershipState::Leader { .. })
    }

    pub(super) async fn become_leader(
        self,
        leader_epoch: LeaderEpoch,
        partition_storage: &'a PartitionStorage<Storage>,
    ) -> Result<
        (
            ActuatorStream,
            LeadershipState<'a, InvokerInputSender, NetworkHandle, Storage>,
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
        partition_storage: &'a PartitionStorage<Storage>,
    ) -> Result<
        (
            ActuatorStream,
            LeadershipState<'a, InvokerInputSender, NetworkHandle, Storage>,
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
        partition_storage: &PartitionStorage<Storage>,
        channel_size: usize,
    ) -> Result<mpsc::Receiver<restate_invoker::OutputEffect>, Error> {
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

    pub(super) async fn become_follower(
        self,
    ) -> Result<
        (
            ActuatorStream,
            LeadershipState<'a, InvokerInputSender, NetworkHandle, Storage>,
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

    pub(super) fn into_message_collector(
        self,
    ) -> ActuatorMessageCollector<'a, InvokerInputSender, NetworkHandle, Storage> {
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

    pub(super) async fn run_tasks(&mut self) -> TaskResult {
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

    fn into_task_result<E: Into<GenericError>>(
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

pub(super) enum TaskResult {
    TerminatedTask(TokioTaskResult),
    Timer(TimerValue),
}

#[derive(Debug, thiserror::Error)]
pub(super) enum TokioTaskResult {
    #[error("task '{0}' terminated unexpectedly")]
    TerminatedTask(&'static str),
    #[error("task '{name}' failed: {error}")]
    FailedTask {
        name: &'static str,
        error: TaskError,
    },
}

pub(super) enum ActuatorStream {
    Follower,
    Leader {
        invoker_stream: ReceiverStream<restate_invoker::OutputEffect>,
        shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
    },
}

impl ActuatorStream {
    fn leader(
        invoker_rx: mpsc::Receiver<restate_invoker::OutputEffect>,
        shuffle_rx: mpsc::Receiver<shuffle::OutboxTruncation>,
    ) -> Self {
        ActuatorStream::Leader {
            invoker_stream: ReceiverStream::new(invoker_rx),
            shuffle_stream: ReceiverStream::new(shuffle_rx),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum TaskError {
    #[error("task was cancelled")]
    Cancelled,
    #[error(transparent)]
    Error(#[from] GenericError),
}

#[derive(Debug)]
pub(super) enum ActuatorOutput {
    Invoker(restate_invoker::OutputEffect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerValue),
}

impl Stream for ActuatorStream {
    type Item = ActuatorOutput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut() {
            ActuatorStream::Follower => Poll::Pending,
            ActuatorStream::Leader {
                invoker_stream,
                shuffle_stream,
            } => {
                let invoker_stream = invoker_stream.map(ActuatorOutput::Invoker);
                let shuffle_stream = shuffle_stream.map(ActuatorOutput::Shuffle);

                let mut all_streams = futures::stream_select!(invoker_stream, shuffle_stream);
                Pin::new(&mut all_streams).poll_next(cx)
            }
        }
    }
}
