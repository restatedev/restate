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
use crate::partition::shuffle::HintSender;
use crate::partition::state_machine::{Action, ActionCollector};
use crate::partition::types::AckResponse;
use crate::partition::{shuffle, ConsensusWriter};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use prost::Message;
use restate_core::metadata;
use restate_errors::NotRunningError;
use restate_ingress_dispatcher::{IngressDispatcherInput, IngressDispatcherInputSender};
use restate_invoker_api::ServiceHandle;
use restate_types::identifiers::{FullInvocationId, PartitionLeaderEpoch, WithPartitionKey};
use restate_types::invocation::{ServiceInvocation, Source, SpanRelation};
use restate_types::journal::CompletionResult;
use restate_wal_protocol::effects::BuiltinServiceEffects;
use restate_wal_protocol::timer::TimerValue;
use restate_wal_protocol::{AckMode, Command, Destination, Envelope, Header};
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
    Ack(#[from] mpsc::error::SendError<AckResponse>),
}

impl<'a, I, N> LeaderAwareActionCollector<'a, I, N>
where
    I: ServiceHandle,
    N: restate_network::NetworkHandle<shuffle::ShuffleInput, Envelope>,
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
                        &leader_state.shuffle_hint_tx,
                        leader_state.timer_service.as_mut(),
                        &mut leader_state.non_deterministic_service_invoker,
                        &follower_state.ack_tx,
                        &mut follower_state.consensus_writer,
                        &follower_state.ingress_tx,
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
        shuffle_hint_tx: &HintSender,
        mut timer_service: Pin<&mut TimerService>,
        non_deterministic_service_invoker: &mut ServiceInvoker<'a>,
        ack_tx: &restate_network::PartitionProcessorSender<AckResponse>,
        consensus_writer: &mut ConsensusWriter,
        ingress_tx: &IngressDispatcherInputSender,
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
            } => shuffle_hint_tx.send(shuffle::NewOutboxMessage::new(seq_number, message)),
            Action::RegisterTimer { timer_value } => timer_service.as_mut().add_timer(timer_value),
            Action::DeleteTimer { timer_key } => {
                timer_service.as_mut().remove_timer(timer_key.into())
            }
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
                invocation_id,
                completion,
            } => {
                let journal_notification_request = Bytes::from(restate_pb::restate::internal::JournalCompletionNotificationRequest {
                    entry_index: completion.entry_index,
                    invocation_uuid: invocation_id.invocation_uuid().into(),
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
                    }),
                }.encode_to_vec());

                // We need this to agree on the invocation uuid, which is randomly generated
                // We could get rid of it if invocation uuids are deterministically generated.
                let service_invocation = ServiceInvocation::new(
                    FullInvocationId::generate(target_service),
                    method_name,
                    journal_notification_request,
                    Source::Internal,
                    None,
                    SpanRelation::None,
                );

                let envelope = Self::wrap_service_invocation_in_envelope(
                    partition_leader_epoch,
                    service_invocation,
                );

                let _ = consensus_writer.send(envelope).await;
            }
            Action::NotifyVirtualJournalKill {
                target_service,
                method_name,
                invocation_id,
            } => {
                // We need this to agree on the invocation uuid, which is randomly generated
                // We could get rid of it if invocation uuids are deterministically generated.
                let service_invocation = ServiceInvocation::new(
                    FullInvocationId::generate(target_service),
                    method_name,
                    restate_pb::restate::internal::KillNotificationRequest {
                        invocation_uuid: invocation_id.invocation_uuid().into(),
                    }
                    .encode_to_vec(),
                    Source::Internal,
                    None,
                    SpanRelation::None,
                );

                let envelope = Self::wrap_service_invocation_in_envelope(
                    partition_leader_epoch,
                    service_invocation,
                );

                let _ = consensus_writer.send(envelope).await;
            }
            Action::IngressResponse(ingress_response) => {
                // ingress should only be unavailable when shutting down
                let _ = ingress_tx
                    .send(IngressDispatcherInput::Response(ingress_response))
                    .await;
            }
        }

        Ok(())
    }

    fn wrap_service_invocation_in_envelope(
        partition_leader_epoch: PartitionLeaderEpoch,
        service_invocation: ServiceInvocation,
    ) -> Envelope {
        let header = Header {
            ack_mode: AckMode::None,
            dest: Destination::Processor {
                partition_key: service_invocation.fid.partition_key(),
            },
            source: restate_wal_protocol::Source::Processor {
                partition_key: Some(service_invocation.fid.partition_key()),
                partition_id: partition_leader_epoch.0,
                leader_epoch: partition_leader_epoch.1,
                // todo: Add support for deduplicating self proposals
                sequence_number: None,
                node_id: metadata().my_node_id().as_plain(),
            },
        };

        Envelope::new(header, Command::Invoke(service_invocation))
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
        non_deterministic_service_invoker_stream: UnboundedReceiverStream<BuiltinServiceEffects>,
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
    BuiltInInvoker(BuiltinServiceEffects),
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
