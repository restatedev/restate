use crate::partition::effects::{ActuatorMessage, MessageCollector};
use crate::partition::leadership::{FollowerState, LeaderState, LeadershipState, TimerService};
use crate::partition::{shuffle, AckResponse, TimerValue};
use futures::{Stream, StreamExt};
use restate_common::types::PartitionLeaderEpoch;
use restate_invoker::{InvokerInputSender, InvokerNotRunning};
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, info_span, trace, warn, warn_span};

pub(crate) enum ActuatorMessageCollector<'a, I, N> {
    Leader {
        follower_state: FollowerState<I, N>,
        leader_state: LeaderState<'a>,
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

impl<'a, I, N> ActuatorMessageCollector<'a, I, N>
where
    I: InvokerInputSender,
    N: restate_network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
{
    pub(crate) async fn send(
        self,
    ) -> Result<LeadershipState<'a, I, N>, ActuatorMessageCollectorError> {
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
        mut timer_service: Pin<&mut TimerService<'a>>,
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

impl<'a, I, N> MessageCollector for ActuatorMessageCollector<'a, I, N> {
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

pub(crate) enum ActuatorStream {
    Follower,
    Leader {
        invoker_stream: ReceiverStream<restate_invoker::Effect>,
        shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
    },
}

impl ActuatorStream {
    pub(crate) fn leader(
        invoker_rx: mpsc::Receiver<restate_invoker::Effect>,
        shuffle_rx: mpsc::Receiver<shuffle::OutboxTruncation>,
    ) -> Self {
        ActuatorStream::Leader {
            invoker_stream: ReceiverStream::new(invoker_rx),
            shuffle_stream: ReceiverStream::new(shuffle_rx),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ActuatorOutput {
    Invoker(restate_invoker::Effect),
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
