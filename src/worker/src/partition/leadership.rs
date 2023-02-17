use crate::partition::effects::{ActuatorMessage, MessageCollector};
use common::types::{LeaderEpoch, PartitionId, PartitionLeaderEpoch, PeerId, ServiceInvocationId};
use futures::{Stream, StreamExt};
use invoker::{InvokeInputJournal, InvokerInputSender};
use std::fmt::Debug;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tracing::trace;

pub(super) trait InvocationReader {
    type InvokedInvocationStream: Stream<Item = ServiceInvocationId> + Unpin;

    fn scan_invoked_invocations(&self) -> Self::InvokedInvocationStream;
}

pub(super) struct LeaderState {
    leader_epoch: LeaderEpoch,
    invoker_rx: mpsc::Receiver<invoker::OutputEffect>,
    message_buffer: Vec<ActuatorMessage>,
}

pub(super) struct FollowerState<I> {
    peer_id: PeerId,
    partition_id: PartitionId,
    invoker_tx: I,
}

pub(super) enum ActuatorMessageCollector<I> {
    Leader {
        follower_state: FollowerState<I>,
        leader_state: LeaderState,
    },
    Follower(FollowerState<I>),
}

impl<I> ActuatorMessageCollector<I>
where
    I: InvokerInputSender,
    I::Error: Debug,
{
    pub(super) async fn send(self) -> Result<LeadershipState<I>, I::Error> {
        match self {
            ActuatorMessageCollector::Leader {
                mut follower_state,
                mut leader_state,
            } => {
                Self::send_actuator_messages(
                    (follower_state.partition_id, leader_state.leader_epoch),
                    &mut follower_state.invoker_tx,
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
        messages: impl IntoIterator<Item = ActuatorMessage>,
    ) -> Result<(), I::Error> {
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
                ActuatorMessage::NewOutboxMessage(..) => {
                    todo!("Make use of this signal once the shuffle is there.")
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

impl<I> MessageCollector for ActuatorMessageCollector<I> {
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

pub(super) enum LeadershipState<InvokerInputSender> {
    Follower(FollowerState<InvokerInputSender>),

    Leader {
        follower_state: FollowerState<InvokerInputSender>,
        leader_state: LeaderState,
    },
}

impl<InvokerInputSender> LeadershipState<InvokerInputSender>
where
    InvokerInputSender: invoker::InvokerInputSender,
    InvokerInputSender::Error: Debug,
{
    pub(super) fn follower(
        peer_id: PeerId,
        partition_id: PartitionId,
        invoker_tx: InvokerInputSender,
    ) -> Self {
        Self::Follower(FollowerState {
            peer_id,
            partition_id,
            invoker_tx,
        })
    }

    pub(super) async fn become_leader<I: InvocationReader>(
        self,
        leader_epoch: LeaderEpoch,
        invocation_reader: &I,
    ) -> Self {
        if let LeadershipState::Follower { .. } = self {
            self.unchecked_become_leader(leader_epoch, invocation_reader)
                .await
        } else {
            self.become_follower()
                .await
                .unchecked_become_leader(leader_epoch, invocation_reader)
                .await
        }
    }

    async fn unchecked_become_leader<I: InvocationReader>(
        self,
        leader_epoch: LeaderEpoch,
        invocation_reader: &I,
    ) -> Self {
        if let LeadershipState::Follower(mut follower_state) = self {
            let (tx, rx) = mpsc::channel(1);

            follower_state
                .invoker_tx
                .register_partition((follower_state.partition_id, leader_epoch), tx)
                .await
                .expect("Invoker should be running");

            let mut invoked_invocations = invocation_reader.scan_invoked_invocations();

            while let Some(service_invocation_id) = invoked_invocations.next().await {
                follower_state
                    .invoker_tx
                    .invoke(
                        (follower_state.partition_id, leader_epoch),
                        service_invocation_id,
                        InvokeInputJournal::NoCachedJournal,
                    )
                    .await
                    .expect("Invoker should be running");
            }

            LeadershipState::Leader {
                follower_state,
                leader_state: LeaderState {
                    leader_epoch,
                    invoker_rx: rx,
                    // The max number of actuator messages should be 2 atm (e.g. RegisterTimer and
                    // AckStoredEntry)
                    message_buffer: Vec::with_capacity(2),
                },
            }
        } else {
            unreachable!("This method should only be called if I am a follower!");
        }
    }

    pub(super) async fn become_follower(self) -> Self {
        if let LeadershipState::Leader {
            follower_state:
                FollowerState {
                    peer_id,
                    partition_id,
                    mut invoker_tx,
                },
            leader_state: LeaderState { leader_epoch, .. },
        } = self
        {
            invoker_tx
                .abort_all_partition((partition_id, leader_epoch))
                .await
                .expect("Invoker should be running");
            Self::follower(peer_id, partition_id, invoker_tx)
        } else {
            self
        }
    }

    pub(super) fn into_message_collector(self) -> ActuatorMessageCollector<InvokerInputSender> {
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

    pub(super) fn actuator_stream(&mut self) -> ActuatorStream<'_, InvokerInputSender> {
        ActuatorStream { inner: self }
    }
}

pub(super) struct ActuatorStream<'a, InvokerInputSender> {
    inner: &'a mut LeadershipState<InvokerInputSender>,
}

impl<'a, InvokerInputSender> Stream for ActuatorStream<'a, InvokerInputSender> {
    type Item = invoker::OutputEffect;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut().inner {
            LeadershipState::Leader {
                leader_state: LeaderState { invoker_rx, .. },
                ..
            } => invoker_rx.poll_recv(cx),
            LeadershipState::Follower { .. } => Poll::Pending,
        }
    }
}
