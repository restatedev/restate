// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{FollowerState, LeaderState, LeadershipState, TimerService};

use crate::partition::services::non_deterministic;
use crate::partition::services::non_deterministic::ServiceInvoker;
use crate::partition::state_machine::{Action, ActionCollector};
use crate::partition::{
    shuffle, StateMachineAckCommand, StateMachineAckResponse, StateMachineCommand, TimerValue,
};
use crate::util::IdentitySender;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use prost::Message;
use restate_errors::NotRunningError;
use restate_invoker_api::ServiceHandle;
use restate_types::identifiers::{FullInvocationId, InvocationUuid, PartitionLeaderEpoch};
use restate_types::invocation::{ServiceInvocation, SpanRelation};
use restate_types::journal::CompletionResult;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tracing::trace;

pub(crate) enum LeaderAwareActionCollector<'a, I, N> {
    Leader {
        follower_state: FollowerState<I, N>,
        leader_state: LeaderState<'a>,
    },
    Follower(FollowerState<I, N>),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LeaderAwareActionCollectorError {
    #[error(transparent)]
    Invoker(#[from] NotRunningError),
    #[error("failed to send ack response: {0}")]
    Ack(#[from] mpsc::error::SendError<StateMachineAckResponse>),
}

impl<'a, I, N> LeaderAwareActionCollector<'a, I, N>
where
    I: ServiceHandle,
    N: restate_network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
{
    pub(crate) async fn send(
        self,
    ) -> Result<LeadershipState<'a, I, N>, LeaderAwareActionCollectorError> {
        match self {
            LeaderAwareActionCollector::Leader {
                mut follower_state,
                mut leader_state,
            } => {
                for message in leader_state.actions_buffer.drain(..) {
                    trace!(?message, "Handle action");
                    Self::handle_action(
                        message,
                        (follower_state.partition_id, leader_state.leader_epoch),
                        &mut follower_state.invoker_tx,
                        &mut leader_state.shuffle_hint_tx,
                        leader_state.timer_service.as_mut(),
                        &leader_state.non_deterministic_service_invoker,
                        &follower_state.ack_tx,
                        &mut follower_state.self_proposal_tx,
                    )
                    .await?;
                }

                Ok(LeadershipState::Leader {
                    follower_state,
                    leader_state,
                })
            }
            LeaderAwareActionCollector::Follower(follower_state) => {
                Ok(LeadershipState::Follower(follower_state))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_action(
        action: Action,
        partition_leader_epoch: PartitionLeaderEpoch,
        invoker_tx: &mut I,
        shuffle_hint_tx: &mut mpsc::Sender<shuffle::NewOutboxMessage>,
        mut timer_service: Pin<&mut TimerService<'a>>,
        non_deterministic_service_invoker: &ServiceInvoker<'a>,
        ack_tx: &restate_network::PartitionProcessorSender<StateMachineAckResponse>,
        self_proposal_tx: &mut IdentitySender<StateMachineAckCommand>,
    ) -> Result<(), LeaderAwareActionCollectorError> {
        match action {
            Action::Invoke {
                full_invocation_id,
                invoke_input_journal,
            } => {
                invoker_tx
                    .invoke(
                        partition_leader_epoch,
                        full_invocation_id,
                        invoke_input_journal,
                    )
                    .await?
            }
            Action::NewOutboxMessage {
                seq_number,
                message,
            } => {
                // it is ok if this message is not sent since it is only a hint
                let _ =
                    shuffle_hint_tx.try_send(shuffle::NewOutboxMessage::new(seq_number, message));
            }
            Action::RegisterTimer { timer_value } => timer_service.as_mut().add_timer(timer_value),
            Action::AckStoredEntry {
                full_invocation_id,
                entry_index,
            } => {
                invoker_tx
                    .notify_stored_entry_ack(
                        partition_leader_epoch,
                        full_invocation_id,
                        entry_index,
                    )
                    .await?;
            }
            Action::ForwardCompletion {
                full_invocation_id,
                completion,
            } => {
                invoker_tx
                    .notify_completion(partition_leader_epoch, full_invocation_id, completion)
                    .await?
            }
            Action::SendAckResponse(ack_response) => ack_tx.send(ack_response).await?,
            Action::AbortInvocation(full_invocation_id) => {
                invoker_tx
                    .abort_invocation(partition_leader_epoch, full_invocation_id)
                    .await?
            }
            Action::InvokeBuiltInService {
                full_invocation_id,
                span_context,
                response_sink,
                method,
                argument,
            } => {
                non_deterministic_service_invoker
                    .invoke(
                        full_invocation_id,
                        method.deref(),
                        span_context,
                        response_sink,
                        argument,
                    )
                    .await;
            }
            Action::NotifyVirtualJournalCompletion {
                target_service,
                method_name,
                invocation_uuid,
                completion,
            } => {
                debug_assert_ne!(
                    completion.result,
                    CompletionResult::Ack,
                    "Virtual Journal completions doesn't support acks"
                );

                // We need this to agree on the invocation uuid, which is randomly generated
                // We could get rid of it if invocation uuids are deterministically generated.
                let _ = self_proposal_tx.send(StateMachineAckCommand::no_ack(StateMachineCommand::Invocation(ServiceInvocation::new(
                    FullInvocationId::with_service_id(target_service, InvocationUuid::now_v7()),
                    method_name,
                    restate_pb::restate::internal::JournalCompletionNotificationRequest {
                        entry_index: completion.entry_index,
                        invocation_uuid: Bytes::copy_from_slice(invocation_uuid.as_bytes()),
                        result: Some(match completion.result {
                            CompletionResult::Empty =>
                                restate_pb::restate::internal::journal_completion_notification_request::Result::Empty(()),
                            CompletionResult::Success(s) =>
                                restate_pb::restate::internal::journal_completion_notification_request::Result::Success(s),
                            CompletionResult::Failure(code, msg) =>               restate_pb::restate::internal::journal_completion_notification_request::Result::Failure(
                                restate_pb::restate::internal::InvocationFailure {
                                    code: code.into(),
                                    message: msg.to_string(),
                                }
                            ),
                            CompletionResult::Ack => { unreachable!() }
                        }),
                    }.encode_to_vec(),
                    None,
                    SpanRelation::None
                )))).await;
            }
            Action::NotifyVirtualJournalKill {
                target_service,
                method_name,
                invocation_uuid,
            } => {
                // We need this to agree on the invocation uuid, which is randomly generated
                // We could get rid of it if invocation uuids are deterministically generated.
                let _ = self_proposal_tx
                    .send(StateMachineAckCommand::no_ack(
                        StateMachineCommand::Invocation(ServiceInvocation::new(
                            FullInvocationId::with_service_id(
                                target_service,
                                InvocationUuid::now_v7(),
                            ),
                            method_name,
                            restate_pb::restate::internal::KillNotificationRequest {
                                invocation_uuid: Bytes::copy_from_slice(invocation_uuid.as_bytes()),
                            }
                            .encode_to_vec(),
                            None,
                            SpanRelation::None,
                        )),
                    ))
                    .await;
            }
        }

        Ok(())
    }
}

impl<'a, I, N> ActionCollector for LeaderAwareActionCollector<'a, I, N> {
    fn collect(&mut self, message: Action) {
        match self {
            LeaderAwareActionCollector::Leader {
                leader_state: LeaderState { actions_buffer, .. },
                ..
            } => {
                actions_buffer.push(message);
            }
            LeaderAwareActionCollector::Follower(..) => {}
        }
    }
}

pub(crate) enum ActionEffectStream {
    Follower,
    Leader {
        invoker_stream: ReceiverStream<restate_invoker_api::Effect>,
        shuffle_stream: ReceiverStream<shuffle::OutboxTruncation>,
        non_deterministic_service_invoker_stream:
            UnboundedReceiverStream<non_deterministic::Effects>,
    },
}

impl ActionEffectStream {
    pub(crate) fn leader(
        invoker_rx: mpsc::Receiver<restate_invoker_api::Effect>,
        shuffle_rx: mpsc::Receiver<shuffle::OutboxTruncation>,
        non_deterministic_service_invoker_rx: non_deterministic::EffectsReceiver,
    ) -> Self {
        ActionEffectStream::Leader {
            invoker_stream: ReceiverStream::new(invoker_rx),
            shuffle_stream: ReceiverStream::new(shuffle_rx),
            non_deterministic_service_invoker_stream: UnboundedReceiverStream::new(
                non_deterministic_service_invoker_rx,
            ),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ActionEffect {
    Invoker(restate_invoker_api::Effect),
    Shuffle(shuffle::OutboxTruncation),
    Timer(TimerValue),
    BuiltInInvoker(non_deterministic::Effects),
}

impl Stream for ActionEffectStream {
    type Item = ActionEffect;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut() {
            ActionEffectStream::Follower => Poll::Pending,
            ActionEffectStream::Leader {
                invoker_stream,
                shuffle_stream,
                non_deterministic_service_invoker_stream,
            } => {
                let invoker_stream = invoker_stream.map(ActionEffect::Invoker);
                let shuffle_stream = shuffle_stream.map(ActionEffect::Shuffle);
                let non_deterministic_service_invoker_stream =
                    non_deterministic_service_invoker_stream.map(ActionEffect::BuiltInInvoker);

                let mut all_streams = futures::stream_select!(
                    invoker_stream,
                    shuffle_stream,
                    non_deterministic_service_invoker_stream
                );
                Pin::new(&mut all_streams).poll_next(cx)
            }
        }
    }
}
