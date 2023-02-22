use crate::partition::effects::{ActuatorMessage, MessageCollector};
use crate::partition::shuffle;
use crate::partition::shuffle::{OutboxReader, Shuffle};
use common::types::{LeaderEpoch, PartitionId, PartitionLeaderEpoch, PeerId, ServiceInvocationId};
use common::utils::GenericError;
use futures::{Stream, StreamExt};
use invoker::{InvokeInputJournal, InvokerInputSender, InvokerNotRunning};
use network::NetworkNotRunning;
use std::fmt::Debug;
use std::ops::DerefMut;
use std::panic;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task;
use tracing::trace;

pub(super) trait InvocationReader {
    type InvokedInvocationStream: Stream<Item = ServiceInvocationId> + Unpin;

    fn scan_invoked_invocations(&self) -> Self::InvokedInvocationStream;
}

pub(super) struct LeaderState {
    leader_epoch: LeaderEpoch,
    shutdown_signal: drain::Signal,
    invoker_rx: mpsc::Receiver<invoker::OutputEffect>,
    shuffle_rx: mpsc::Receiver<shuffle::OutboxTruncation>,
    shuffle_hint_tx: mpsc::Sender<shuffle::NewOutboxMessage>,
    shuffle_handle: task::JoinHandle<Result<(), anyhow::Error>>,
    message_buffer: Vec<ActuatorMessage>,
}

pub(super) struct FollowerState<I, N> {
    peer_id: PeerId,
    partition_id: PartitionId,
    invoker_tx: I,
    network_handle: N,
}

pub(super) enum ActuatorMessageCollector<I, N> {
    Leader {
        follower_state: FollowerState<I, N>,
        leader_state: LeaderState,
    },
    Follower(FollowerState<I, N>),
}

#[derive(Debug, thiserror::Error)]
#[error("failed sending actuator messages: {0}")]
pub(crate) struct SendError(#[from] GenericError);

impl<I, N> ActuatorMessageCollector<I, N>
where
    I: InvokerInputSender,
    N: network::NetworkHandle<shuffle::NetworkInput, shuffle::NetworkOutput>,
{
    pub(super) async fn send(self) -> Result<LeadershipState<I, N>, SendError> {
        match self {
            ActuatorMessageCollector::Leader {
                mut follower_state,
                mut leader_state,
            } => {
                Self::send_actuator_messages(
                    (follower_state.partition_id, leader_state.leader_epoch),
                    &mut follower_state.invoker_tx,
                    &mut leader_state.shuffle_hint_tx,
                    leader_state.message_buffer.drain(..),
                )
                .await
                .map_err(|err| SendError(err.into()))?;

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
        messages: impl IntoIterator<Item = ActuatorMessage>,
    ) -> Result<(), InvokerNotRunning> {
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
                ActuatorMessage::RegisterTimer { .. } => {
                    unimplemented!("we don't have a timer service yet :-(")
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
    NetworkHandle: network::NetworkHandle<shuffle::NetworkInput, shuffle::NetworkOutput>,
{
    pub(super) fn follower(
        peer_id: PeerId,
        partition_id: PartitionId,
        invoker_tx: InvokerInputSender,
        network_handle: NetworkHandle,
    ) -> Self {
        Self::Follower(FollowerState {
            peer_id,
            partition_id,
            invoker_tx,
            network_handle,
        })
    }

    pub(super) async fn become_leader<SR>(
        self,
        leader_epoch: LeaderEpoch,
        state_reader: SR,
    ) -> Result<Self, Error>
    where
        SR: InvocationReader + OutboxReader + Send + Sync + 'static,
    {
        if let LeadershipState::Follower { .. } = self {
            self.unchecked_become_leader(leader_epoch, state_reader)
                .await
        } else {
            self.become_follower()
                .await?
                .unchecked_become_leader(leader_epoch, state_reader)
                .await
        }
    }

    async fn unchecked_become_leader<SR>(
        self,
        leader_epoch: LeaderEpoch,
        state_reader: SR,
    ) -> Result<Self, Error>
    where
        SR: OutboxReader + InvocationReader + Send + Sync + 'static,
    {
        if let LeadershipState::Follower(mut follower_state) = self {
            let (invoker_tx, invoker_rx) = mpsc::channel(1);

            follower_state
                .invoker_tx
                .register_partition((follower_state.partition_id, leader_epoch), invoker_tx)
                .await?;

            let mut invoked_invocations = state_reader.scan_invoked_invocations();

            while let Some(service_invocation_id) = invoked_invocations.next().await {
                follower_state
                    .invoker_tx
                    .invoke(
                        (follower_state.partition_id, leader_epoch),
                        service_invocation_id,
                        InvokeInputJournal::NoCachedJournal,
                    )
                    .await?;
            }

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

            Ok(LeadershipState::Leader {
                follower_state,
                leader_state: LeaderState {
                    leader_epoch,
                    shutdown_signal,
                    invoker_rx,
                    shuffle_rx,
                    shuffle_hint_tx,
                    shuffle_handle,
                    // The max number of actuator messages should be 2 atm (e.g. RegisterTimer and
                    // AckStoredEntry)
                    message_buffer: Vec::with_capacity(2),
                },
            })
        } else {
            unreachable!("This method should only be called if I am a follower!");
        }
    }

    pub(super) async fn become_follower(self) -> Result<Self, Error> {
        if let LeadershipState::Leader {
            follower_state:
                FollowerState {
                    peer_id,
                    partition_id,
                    mut invoker_tx,
                    network_handle,
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

            let (shuffle_result, abort_result, unregister_result) = tokio::join!(
                shuffle_handle,
                invoker_tx.abort_all_partition((partition_id, leader_epoch)),
                network_handle.unregister_shuffle(peer_id),
            );

            abort_result?;
            unregister_result?;

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
            ))
        } else {
            Ok(self)
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

    pub(super) fn actuator_stream(
        &mut self,
    ) -> ActuatorStream<'_, InvokerInputSender, NetworkHandle> {
        ActuatorStream { inner: self }
    }
}

pub(super) struct ActuatorStream<'a, InvokerInputSender, NetworkHandle> {
    inner: &'a mut LeadershipState<InvokerInputSender, NetworkHandle>,
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
    ShuffleTaskTermination { error: Option<TaskError> },
}

impl<'a, InvokerInputSender, NetworkHandle> Stream
    for ActuatorStream<'a, InvokerInputSender, NetworkHandle>
{
    type Item = ActuatorOutput;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut().inner {
            LeadershipState::Leader {
                leader_state:
                    LeaderState {
                        invoker_rx,
                        shuffle_rx,
                        shuffle_handle,
                        ..
                    },
                ..
            } => {
                let invoker_stream = futures::stream::poll_fn(|cx| invoker_rx.poll_recv(cx))
                    .map(ActuatorOutput::Invoker);
                let shuffle_stream = futures::stream::poll_fn(|cx| shuffle_rx.poll_recv(cx))
                    .map(ActuatorOutput::Shuffle);
                let shuffle_handle = futures::stream::once(shuffle_handle).map(|result| {
                    if let Err(err) = result {
                        if err.is_panic() {
                            panic::resume_unwind(err.into_panic());
                        }
                        ActuatorOutput::ShuffleTaskTermination {
                            error: Some(TaskError::Cancelled),
                        }
                    } else {
                        let result = result.unwrap();

                        ActuatorOutput::ShuffleTaskTermination {
                            error: result.err().map(|err| TaskError::Error(err.into())),
                        }
                    }
                });

                let mut all_streams =
                    futures::stream_select!(invoker_stream, shuffle_stream, shuffle_handle);

                Pin::new(&mut all_streams).poll_next(cx)
            }
            LeadershipState::Follower { .. } => Poll::Pending,
        }
    }
}
