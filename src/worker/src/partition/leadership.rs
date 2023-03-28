use crate::partition::effects::{ActuatorMessage, MessageCollector};
use crate::partition::shuffle::{OutboxReader, Shuffle};
use crate::partition::{shuffle, Timer};
use crate::{TimerHandle, TimerOutput};
use common::types::{LeaderEpoch, PartitionId, PartitionLeaderEpoch, PeerId, ServiceInvocationId};
use common::utils::GenericError;
use futures::{future, Stream, StreamExt};
use invoker::{InvokeInputJournal, InvokerInputSender, InvokerNotRunning};
use network::NetworkNotRunning;
use std::fmt::Debug;
use std::ops::{Add, DerefMut};
use std::panic;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info_span, trace};

pub(super) trait InvocationReader {
    type InvokedInvocationStream: Stream<Item = ServiceInvocationId> + Unpin;

    fn scan_invoked_invocations(&self) -> Self::InvokedInvocationStream;
}

pub(super) trait TimerReader {
    type TimerStream: Stream<Item = Timer> + Unpin;

    fn scan_timers(&self) -> Self::TimerStream;
}

pub(super) struct LeaderState {
    leader_epoch: LeaderEpoch,
    shutdown_signal: drain::Signal,
    shuffle_hint_tx: mpsc::Sender<shuffle::NewOutboxMessage>,
    shuffle_handle: task::JoinHandle<Result<(), anyhow::Error>>,
    message_buffer: Vec<ActuatorMessage>,
}

pub(super) struct FollowerState<I, N> {
    peer_id: PeerId,
    partition_id: PartitionId,
    invoker_tx: I,
    network_handle: N,
    timer_handle: TimerHandle,
}

pub(super) enum ActuatorMessageCollector<I, N> {
    Leader {
        follower_state: FollowerState<I, N>,
        leader_state: LeaderState,
    },
    Follower(FollowerState<I, N>),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActuatorMessageCollectorError {
    #[error(transparent)]
    Invoker(#[from] InvokerNotRunning),
    #[error(transparent)]
    Timer(#[from] timer::Error),
}

impl<I, N> ActuatorMessageCollector<I, N>
where
    I: InvokerInputSender,
    N: network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
{
    pub(super) async fn send(self) -> Result<LeadershipState<I, N>, ActuatorMessageCollectorError> {
        match self {
            ActuatorMessageCollector::Leader {
                mut follower_state,
                mut leader_state,
            } => {
                Self::send_actuator_messages(
                    (follower_state.partition_id, leader_state.leader_epoch),
                    &mut follower_state.invoker_tx,
                    &follower_state.timer_handle,
                    &mut leader_state.shuffle_hint_tx,
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
        timer_handle: &TimerHandle,
        shuffle_hint_tx: &mut mpsc::Sender<shuffle::NewOutboxMessage>,
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
                ActuatorMessage::RegisterTimer {
                    service_invocation_id,
                    wake_up_time,
                    entry_index,
                } => {
                    timer_handle
                        .add_timer(
                            SystemTime::UNIX_EPOCH.add(Duration::from_millis(wake_up_time)),
                            partition_leader_epoch,
                            Timer::new(service_invocation_id, entry_index, wake_up_time),
                        )
                        .await?;
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
                    invocation_id,
                    span_context,
                    result,
                } => {
                    let span = match result {
                        Ok(_) => {
                            info_span!(
                                "end_invocation",
                                restate.invocation.id = %invocation_id,
                                restate.invocation.result = "success")
                        }
                        Err((status_code, status_message)) => {
                            info_span!("end_invocation",
                                restate.invocation.id = %invocation_id,
                                restate.invocation.result = "failure",
                                restate.result.failure.status_code = status_code,
                                restate.result.failure.status_message = status_message)
                        }
                    };
                    span_context.as_parent().attach_to_span(&span);
                    let _ = span.enter();
                }
            }
        }

        Ok(())
    }
}

impl<I, N> MessageCollector for ActuatorMessageCollector<I, N> {
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
    #[error(transparent)]
    Timer(#[from] timer::Error),
    #[error("shuffle failed. This indicates a bug or the system is shutting down: {0}")]
    FailedShuffleTask(#[from] anyhow::Error),
}

pub(super) enum LeadershipState<InvokerInputSender, NetworkHandle> {
    Follower(FollowerState<InvokerInputSender, NetworkHandle>),

    Leader {
        follower_state: FollowerState<InvokerInputSender, NetworkHandle>,
        leader_state: LeaderState,
    },
}

impl<InvokerInputSender, NetworkHandle> LeadershipState<InvokerInputSender, NetworkHandle>
where
    InvokerInputSender: invoker::InvokerInputSender,
    NetworkHandle: network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
{
    pub(super) fn follower(
        peer_id: PeerId,
        partition_id: PartitionId,
        invoker_tx: InvokerInputSender,
        network_handle: NetworkHandle,
        timer_handle: TimerHandle,
    ) -> (ActuatorStream, Self) {
        (
            ActuatorStream::Follower,
            Self::Follower(FollowerState {
                peer_id,
                partition_id,
                invoker_tx,
                network_handle,
                timer_handle,
            }),
        )
    }

    pub(super) async fn become_leader<SR>(
        self,
        leader_epoch: LeaderEpoch,
        state_reader: SR,
    ) -> Result<(ActuatorStream, Self), Error>
    where
        SR: InvocationReader + OutboxReader + TimerReader + Send + Sync + 'static,
    {
        if let LeadershipState::Follower { .. } = self {
            self.unchecked_become_leader(leader_epoch, state_reader)
                .await
        } else {
            let (_, follower_state) = self.become_follower().await?;

            follower_state
                .unchecked_become_leader(leader_epoch, state_reader)
                .await
        }
    }

    async fn unchecked_become_leader<SR>(
        self,
        leader_epoch: LeaderEpoch,
        state_reader: SR,
    ) -> Result<(ActuatorStream, Self), Error>
    where
        SR: OutboxReader + InvocationReader + TimerReader + Send + Sync + 'static,
    {
        if let LeadershipState::Follower(mut follower_state) = self {
            let invoker_rx = Self::register_at_invoker(
                &mut follower_state.invoker_tx,
                (follower_state.partition_id, leader_epoch),
                &state_reader,
            )
            .await?;

            let timer_rx = Self::register_at_timer_service(
                &follower_state.timer_handle,
                (follower_state.partition_id, leader_epoch),
                &state_reader,
            )
            .await?;

            let (shuffle_tx, shuffle_rx) = mpsc::channel(1);

            let shuffle = Shuffle::new(
                follower_state.peer_id,
                state_reader,
                follower_state.network_handle.create_shuffle_sender(),
                shuffle_tx,
            );

            follower_state
                .network_handle
                .register_shuffle(shuffle.peer_id(), shuffle.create_network_sender())
                .await?;

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let (shutdown_signal, shutdown_watch) = drain::channel();
            let shuffle_handle = tokio::spawn(shuffle.run(shutdown_watch));

            Ok((
                ActuatorStream::leader(invoker_rx, shuffle_rx, timer_rx),
                LeadershipState::Leader {
                    follower_state,
                    leader_state: LeaderState {
                        leader_epoch,
                        shutdown_signal,
                        shuffle_hint_tx,
                        shuffle_handle,
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

    async fn register_at_invoker<IR>(
        invoker_handle: &mut InvokerInputSender,
        partition_leader_epoch: PartitionLeaderEpoch,
        invocation_reader: &IR,
    ) -> Result<mpsc::Receiver<invoker::OutputEffect>, Error>
    where
        IR: InvocationReader,
    {
        let (invoker_tx, invoker_rx) = mpsc::channel(1);

        invoker_handle
            .register_partition(partition_leader_epoch, invoker_tx)
            .await?;

        let mut invoked_invocations = invocation_reader.scan_invoked_invocations();

        while let Some(service_invocation_id) = invoked_invocations.next().await {
            invoker_handle
                .invoke(
                    partition_leader_epoch,
                    service_invocation_id,
                    InvokeInputJournal::NoCachedJournal,
                )
                .await?;
        }

        Ok(invoker_rx)
    }

    async fn register_at_timer_service<TR>(
        timer_handle: &TimerHandle,
        partition_leader_epoch: PartitionLeaderEpoch,
        timer_reader: &TR,
    ) -> Result<mpsc::Receiver<TimerOutput>, Error>
    where
        TR: TimerReader,
    {
        let (timer_tx, timer_rx) = mpsc::channel(1);

        timer_handle
            .register(partition_leader_epoch, timer_tx)
            .await?;

        while let Some(Timer {
            service_invocation_id,
            wake_up_time,
            entry_index,
        }) = timer_reader.scan_timers().next().await
        {
            timer_handle
                .add_timer(
                    SystemTime::UNIX_EPOCH.add(Duration::from_millis(wake_up_time)),
                    partition_leader_epoch,
                    Timer::new(service_invocation_id, entry_index, wake_up_time),
                )
                .await?;
        }

        Ok(timer_rx)
    }

    pub(super) async fn become_follower(self) -> Result<(ActuatorStream, Self), Error> {
        if let LeadershipState::Leader {
            follower_state:
                FollowerState {
                    peer_id,
                    partition_id,
                    mut invoker_tx,
                    network_handle,
                    timer_handle,
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

            let (shuffle_result, abort_result, network_unregister_result, timer_unregister_result) = tokio::join!(
                shuffle_handle,
                invoker_tx.abort_all_partition((partition_id, leader_epoch)),
                network_handle.unregister_shuffle(peer_id),
                timer_handle.unregister((partition_id, leader_epoch)),
            );

            abort_result?;
            network_unregister_result?;
            timer_unregister_result?;

            if let Err(err) = shuffle_result {
                if err.is_panic() {
                    panic::resume_unwind(err.into_panic());
                }
            } else {
                shuffle_result.unwrap()?;
            }

            Ok(Self::follower(
                peer_id,
                partition_id,
                invoker_tx,
                network_handle,
                timer_handle,
            ))
        } else {
            Ok((ActuatorStream::Follower, self))
        }
    }

    pub(super) fn into_message_collector(
        self,
    ) -> ActuatorMessageCollector<InvokerInputSender, NetworkHandle> {
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
                leader_state: LeaderState { shuffle_handle, .. },
                ..
            } => {
                let result = shuffle_handle.await;

                if let Err(err) = result {
                    if err.is_panic() {
                        panic::resume_unwind(err.into_panic());
                    }

                    TaskResult::FailedTask(TaskError::Cancelled)
                } else {
                    let result = result.unwrap();

                    result
                        .err()
                        .map(|err| TaskResult::FailedTask(TaskError::Error(err.into())))
                        .unwrap_or(TaskResult::TerminatedTask)
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum TaskResult {
    #[error("task terminated unexpectedly")]
    TerminatedTask,
    #[error(transparent)]
    FailedTask(#[from] TaskError),
}

pub(super) enum ActuatorStream {
    Follower,
    Leader {
        invoker_stream: ReceiverStream<invoker::OutputEffect>,
        shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
        timer_stream: ReceiverStream<TimerOutput>,
    },
}

impl ActuatorStream {
    fn leader(
        invoker_rx: mpsc::Receiver<invoker::OutputEffect>,
        shuffle_rx: mpsc::Receiver<shuffle::OutboxTruncation>,
        timer_rx: mpsc::Receiver<TimerOutput>,
    ) -> Self {
        ActuatorStream::Leader {
            invoker_stream: ReceiverStream::new(invoker_rx),
            shuffle_stream: ReceiverStream::new(shuffle_rx),
            timer_stream: ReceiverStream::new(timer_rx),
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
    Invoker(invoker::OutputEffect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerOutput),
}

impl Stream for ActuatorStream {
    type Item = ActuatorOutput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut() {
            ActuatorStream::Follower => Poll::Pending,
            ActuatorStream::Leader {
                invoker_stream,
                shuffle_stream,
                timer_stream,
            } => {
                let invoker_stream = invoker_stream.map(ActuatorOutput::Invoker);
                let shuffle_stream = shuffle_stream.map(ActuatorOutput::Shuffle);
                let timer_stream = timer_stream.map(ActuatorOutput::Timer);

                let mut all_streams =
                    futures::stream_select!(invoker_stream, shuffle_stream, timer_stream);
                Pin::new(&mut all_streams).poll_next(cx)
            }
        }
    }
}
