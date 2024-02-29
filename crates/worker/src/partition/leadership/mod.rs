// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::shuffle::{HintSender, Shuffle, ShuffleMetadata};
use crate::partition::{shuffle, storage, ConsensusWriter};
use assert2::let_assert;
use futures::{future, StreamExt};
use restate_core::metadata;
use restate_invoker_api::InvokeInputJournal;
use restate_timer::TokioClock;
use std::fmt::Debug;
use std::ops::{Deref, RangeInclusive};

use bytes::Bytes;
use prost::Message;
use std::panic;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinError;
use tracing::trace;

mod action_collector;

use crate::partition::action_effect_handler::ActionEffectHandler;
use crate::partition::services::non_deterministic;
use crate::partition::services::non_deterministic::ServiceInvoker;
use crate::partition::state_machine::Action;
pub(crate) use action_collector::{ActionEffect, ActionEffectStream};
use restate_errors::NotRunningError;
use restate_ingress_dispatcher::{IngressDispatcherInput, IngressDispatcherInputSender};
use restate_schema_impl::Schemas;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::dedup::EpochSequenceNumber;
use restate_types::identifiers::{FullInvocationId, InvocationId, PartitionKey};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionLeaderEpoch, PeerId};
use restate_types::invocation::{ServiceInvocation, Source, SpanRelation};
use restate_types::journal::{CompletionResult, EntryType};
use restate_wal_protocol::timer::TimerValue;

type PartitionStorage = storage::PartitionStorage<RocksDBStorage>;
type TimerService = restate_timer::TimerService<TimerValue, TokioClock, PartitionStorage>;

pub(crate) struct LeaderState {
    leader_epoch: LeaderEpoch,
    shutdown_signal: drain::Signal,
    shuffle_hint_tx: HintSender,
    shuffle_handle: task::JoinHandle<Result<(), anyhow::Error>>,
    timer_service: Pin<Box<TimerService>>,
    non_deterministic_service_invoker: non_deterministic::ServiceInvoker,
    action_effect_handler: ActionEffectHandler,
}

pub(crate) struct FollowerState<I> {
    peer_id: PeerId,
    partition_id: PartitionId,
    timer_service_options: restate_timer::Options,
    channel_size: usize,
    invoker_tx: I,
    consensus_writer: ConsensusWriter,
    ingress_tx: IngressDispatcherInputSender,
    partition_key_range: RangeInclusive<PartitionKey>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invoker is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Invoker(NotRunningError),
    #[error("shuffle failed. This indicates a bug or the system is shutting down: {0}")]
    FailedShuffleTask(#[from] anyhow::Error),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

pub(crate) enum LeadershipState<InvokerInputSender> {
    Follower(FollowerState<InvokerInputSender>),

    Leader {
        follower_state: FollowerState<InvokerInputSender>,
        leader_state: LeaderState,
    },
}

impl<InvokerInputSender> LeadershipState<InvokerInputSender>
where
    InvokerInputSender: restate_invoker_api::ServiceHandle,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn follower(
        peer_id: PeerId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        timer_service_options: restate_timer::Options,
        channel_size: usize,
        invoker_tx: InvokerInputSender,
        consensus_writer: ConsensusWriter,
        ingress_tx: IngressDispatcherInputSender,
    ) -> (Self, ActionEffectStream) {
        (
            Self::Follower(FollowerState {
                peer_id,
                partition_id,
                partition_key_range,
                timer_service_options,
                channel_size,
                invoker_tx,
                consensus_writer,
                ingress_tx,
            }),
            ActionEffectStream::Follower,
        )
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self, LeadershipState::Leader { .. })
    }

    pub(crate) async fn become_leader(
        self,
        epoch_sequence_number: EpochSequenceNumber,
        partition_storage: &mut PartitionStorage,
        schemas: Schemas,
    ) -> Result<(Self, ActionEffectStream), Error> {
        if let LeadershipState::Follower { .. } = self {
            self.unchecked_become_leader(epoch_sequence_number, partition_storage, schemas)
                .await
        } else {
            let (follower_state, _) = self.become_follower().await?;

            follower_state
                .unchecked_become_leader(epoch_sequence_number, partition_storage, schemas)
                .await
        }
    }

    async fn unchecked_become_leader(
        self,
        epoch_sequence_number: EpochSequenceNumber,
        partition_storage: &mut PartitionStorage,
        schemas: Schemas,
    ) -> Result<(Self, ActionEffectStream), Error> {
        if let LeadershipState::Follower(mut follower_state) = self {
            let (mut service_invoker, service_invoker_output_rx) =
                non_deterministic::ServiceInvoker::new(partition_storage.clone(), schemas);

            let leader_epoch = epoch_sequence_number.leader_epoch;

            let invoker_rx = Self::resume_invoked_invocations(
                &mut follower_state.invoker_tx,
                &mut service_invoker,
                (follower_state.partition_id, leader_epoch),
                follower_state.partition_key_range.clone(),
                partition_storage,
                follower_state.channel_size,
            )
            .await?;

            let timer_service = Box::pin(
                follower_state
                    .timer_service_options
                    .build(partition_storage.clone(), TokioClock),
            );

            let (shuffle_tx, shuffle_rx) = mpsc::channel(follower_state.channel_size);

            let shuffle = Shuffle::new(
                ShuffleMetadata::new(
                    follower_state.peer_id,
                    follower_state.partition_id,
                    leader_epoch,
                    metadata().my_node_id().into(),
                ),
                partition_storage.clone(),
                shuffle_tx,
                follower_state.channel_size,
            );

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let (shutdown_signal, shutdown_watch) = drain::channel();

            let shuffle_handle = tokio::spawn(shuffle.run(shutdown_watch));

            let action_effect_handler = ActionEffectHandler::new(
                follower_state.partition_id,
                epoch_sequence_number,
                follower_state.partition_key_range.clone(),
                follower_state.consensus_writer.clone(),
            );

            Ok((
                LeadershipState::Leader {
                    follower_state,
                    leader_state: LeaderState {
                        leader_epoch,
                        shutdown_signal,
                        shuffle_hint_tx,
                        shuffle_handle,
                        timer_service,
                        action_effect_handler,
                        non_deterministic_service_invoker: service_invoker,
                    },
                },
                ActionEffectStream::leader(invoker_rx, shuffle_rx, service_invoker_output_rx),
            ))
        } else {
            unreachable!("This method should only be called if I am a follower!");
        }
    }

    async fn resume_invoked_invocations(
        invoker_handle: &mut InvokerInputSender,
        built_in_service_invoker: &mut non_deterministic::ServiceInvoker,
        partition_leader_epoch: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        partition_storage: &mut PartitionStorage,
        channel_size: usize,
    ) -> Result<mpsc::Receiver<restate_invoker_api::Effect>, Error> {
        let (invoker_tx, invoker_rx) = mpsc::channel(channel_size);

        invoker_handle
            .register_partition(partition_leader_epoch, partition_key_range, invoker_tx)
            .await
            .map_err(Error::Invoker)?;

        let mut built_in_invoked_services = Vec::new();

        {
            let invoked_invocations = partition_storage.scan_invoked_invocations();
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
                        .await
                        .map_err(Error::Invoker)?;
                } else {
                    built_in_invoked_services.push(full_invocation_id);
                }
            }
        }

        for full_invocation_id in built_in_invoked_services {
            let input_entry = partition_storage
                .load_journal_entry(&InvocationId::from(&full_invocation_id), 0)
                .await?
                .expect("first journal entry must be present; if not, this indicates a bug.");

            assert_eq!(
                input_entry.header().as_entry_type(),
                EntryType::Custom,
                "first journal entry must be a '{}' for built in services",
                EntryType::Custom
            );

            let status = partition_storage
                .get_invocation_status(&InvocationId::from(&full_invocation_id))
                .await?;

            let_assert!(InvocationStatus::Invoked(metadata) = status);

            let method = metadata.method;
            let response_sink = metadata.response_sink;
            let argument = input_entry.serialized_entry().clone();
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

        Ok(invoker_rx)
    }

    pub(crate) async fn become_follower(self) -> Result<(Self, ActionEffectStream), Error> {
        if let LeadershipState::Leader {
            follower_state:
                FollowerState {
                    peer_id,
                    partition_id,
                    partition_key_range,
                    channel_size,
                    timer_service_options: num_in_memory_timers,
                    mut invoker_tx,
                    consensus_writer: self_proposal_tx,
                    ingress_tx,
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

            let (shuffle_result, abort_result) = tokio::join!(
                shuffle_handle,
                invoker_tx.abort_all_partition((partition_id, leader_epoch)),
            );

            abort_result.map_err(Error::Invoker)?;

            Self::unwrap_task_result(shuffle_result)?;

            Ok(Self::follower(
                peer_id,
                partition_id,
                partition_key_range,
                num_in_memory_timers,
                channel_size,
                invoker_tx,
                self_proposal_tx,
                ingress_tx,
            ))
        } else {
            Ok((self, ActionEffectStream::Follower))
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

    pub async fn handle_actions(
        &mut self,
        actions: impl Iterator<Item = Action>,
    ) -> Result<(), Error> {
        match self {
            LeadershipState::Follower(_) => {
                // nothing to do :-)
            }
            LeadershipState::Leader {
                follower_state,
                leader_state,
            } => {
                for action in actions {
                    trace!(?action, "Apply action");
                    Self::handle_action(
                        action,
                        (follower_state.partition_id, leader_state.leader_epoch),
                        &mut follower_state.invoker_tx,
                        &leader_state.shuffle_hint_tx,
                        leader_state.timer_service.as_mut(),
                        &mut leader_state.non_deterministic_service_invoker,
                        &mut leader_state.action_effect_handler,
                        &follower_state.ingress_tx,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_action(
        action: Action,
        partition_leader_epoch: PartitionLeaderEpoch,
        invoker_tx: &mut InvokerInputSender,
        shuffle_hint_tx: &HintSender,
        mut timer_service: Pin<&mut TimerService>,
        non_deterministic_service_invoker: &mut ServiceInvoker,
        action_effect_handler: &mut ActionEffectHandler,
        ingress_tx: &IngressDispatcherInputSender,
    ) -> Result<(), Error> {
        match action {
            Action::Invoke {
                full_invocation_id,
                invoke_input_journal,
            } => invoker_tx
                .invoke(
                    partition_leader_epoch,
                    full_invocation_id,
                    invoke_input_journal,
                )
                .await
                .map_err(Error::Invoker)?,
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
                    .await
                    .map_err(Error::Invoker)?;
            }
            Action::ForwardCompletion {
                full_invocation_id,
                completion,
            } => invoker_tx
                .notify_completion(partition_leader_epoch, full_invocation_id, completion)
                .await
                .map_err(Error::Invoker)?,
            Action::AbortInvocation(full_invocation_id) => invoker_tx
                .abort_invocation(partition_leader_epoch, full_invocation_id)
                .await
                .map_err(Error::Invoker)?,
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

                action_effect_handler
                    .handle(ActionEffect::Invocation(service_invocation))
                    .await;
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

                action_effect_handler
                    .handle(ActionEffect::Invocation(service_invocation))
                    .await;
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

    pub async fn handle_action_effect(&mut self, action_effect: ActionEffect) {
        match self {
            LeadershipState::Follower(_) => {
                // nothing to do :-)
            }
            LeadershipState::Leader { leader_state, .. } => {
                leader_state
                    .action_effect_handler
                    .handle(action_effect)
                    .await
            }
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
