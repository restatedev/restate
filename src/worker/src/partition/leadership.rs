use crate::partition::effects::{ActuatorMessage, MessageCollector};
use common::types::{LeaderEpoch, PartitionId, PartitionLeaderEpoch, ServiceInvocationId};
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

pub(super) enum ActuatorMessageCollector<I> {
    Leader {
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        invoker_tx: I,
        invoker_rx: mpsc::Receiver<invoker::OutputEffect>,
        message_buffer: Vec<ActuatorMessage>,
    },
    Follower(LeadershipState<I>),
}

impl<I> ActuatorMessageCollector<I>
where
    I: InvokerInputSender,
    I::Error: Debug,
{
    pub(super) async fn send(self) -> Result<LeadershipState<I>, I::Error> {
        match self {
            ActuatorMessageCollector::Leader {
                partition_id,
                leader_epoch,
                mut invoker_tx,
                invoker_rx,
                mut message_buffer,
            } => {
                Self::send_actuator_messages(
                    (partition_id, leader_epoch),
                    &mut invoker_tx,
                    message_buffer.drain(..),
                )
                .await?;

                Ok(LeadershipState::Leader {
                    partition_id,
                    leader_epoch,
                    invoker_tx,
                    invoker_rx,
                    message_buffer,
                })
            }
            ActuatorMessageCollector::Follower(leadership_state) => Ok(leadership_state),
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
            ActuatorMessageCollector::Leader { message_buffer, .. } => {
                message_buffer.push(message);
            }
            ActuatorMessageCollector::Follower(..) => {}
        }
    }
}

pub(super) enum LeadershipState<InvokerInputSender> {
    Follower {
        partition_id: PartitionId,
        invoker_tx: InvokerInputSender,
    },

    Leader {
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        invoker_rx: mpsc::Receiver<invoker::OutputEffect>,
        invoker_tx: InvokerInputSender,
        message_buffer: Vec<ActuatorMessage>,
    },
}

impl<InvokerInputSender> LeadershipState<InvokerInputSender>
where
    InvokerInputSender: invoker::InvokerInputSender,
    InvokerInputSender::Error: Debug,
{
    pub(super) fn follower(partition_id: PartitionId, invoker_tx: InvokerInputSender) -> Self {
        Self::Follower {
            partition_id,
            invoker_tx,
        }
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
        if let LeadershipState::Follower {
            partition_id,
            mut invoker_tx,
            ..
        } = self
        {
            let (tx, rx) = mpsc::channel(1);

            invoker_tx
                .register_partition((partition_id, leader_epoch), tx)
                .await
                .expect("Invoker should be running");

            let mut invoked_invocations = invocation_reader.scan_invoked_invocations();

            while let Some(service_invocation_id) = invoked_invocations.next().await {
                invoker_tx
                    .invoke(
                        (partition_id, leader_epoch),
                        service_invocation_id,
                        InvokeInputJournal::NoCachedJournal,
                    )
                    .await
                    .expect("Invoker should be running");
            }

            LeadershipState::Leader {
                partition_id,
                leader_epoch,
                invoker_rx: rx,
                invoker_tx,
                // The max number of actuator messages should be 2 atm (e.g. RegisterTimer and
                // AckStoredEntry)
                message_buffer: Vec::with_capacity(2),
            }
        } else {
            unreachable!("This method should only be called if I am a follower!");
        }
    }

    pub(super) async fn become_follower(self) -> Self {
        if let LeadershipState::Leader {
            partition_id,
            leader_epoch,
            mut invoker_tx,
            ..
        } = self
        {
            invoker_tx
                .abort_all_partition((partition_id, leader_epoch))
                .await
                .expect("Invoker should be running");
            Self::follower(partition_id, invoker_tx)
        } else {
            self
        }
    }

    pub(super) fn into_message_collector(self) -> ActuatorMessageCollector<InvokerInputSender> {
        match self {
            leadership_state @ LeadershipState::Follower { .. } => {
                ActuatorMessageCollector::Follower(leadership_state)
            }
            LeadershipState::Leader {
                partition_id,
                leader_epoch,
                invoker_tx,
                invoker_rx,
                mut message_buffer,
            } => {
                message_buffer.clear();
                ActuatorMessageCollector::Leader {
                    partition_id,
                    leader_epoch,
                    invoker_rx,
                    invoker_tx,
                    message_buffer,
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
            LeadershipState::Leader { invoker_rx, .. } => invoker_rx.poll_recv(cx),
            LeadershipState::Follower { .. } => Poll::Pending,
        }
    }
}
