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
use crate::partition::{shuffle, storage};
use assert2::let_assert;
use futures::future::OptionFuture;
use futures::{future, StreamExt};
use restate_core::network::NetworkSender;
use restate_core::{
    current_task_partition_id, metadata, task_center, ShutdownError, TaskId, TaskKind,
};
use restate_invoker_api::InvokeInputJournal;
use restate_network::Networking;
use restate_node_protocol::ingress;
use restate_timer::TokioClock;
use std::fmt::Debug;
use std::ops::{Deref, RangeInclusive};
use std::panic;
use std::pin::Pin;
use tokio::sync::mpsc;
use tracing::{trace, warn};

mod action_collector;

use crate::partition::action_effect_handler::ActionEffectHandler;
use crate::partition::services::non_deterministic;
use crate::partition::services::non_deterministic::ServiceInvoker;
use crate::partition::state_machine::Action;
pub(crate) use action_collector::{ActionEffect, ActionEffectStream};
use restate_bifrost::Bifrost;
use restate_errors::NotRunningError;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::dedup::EpochSequenceNumber;
use restate_types::identifiers::{InvocationId, PartitionKey};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionLeaderEpoch};
use restate_types::journal::EntryType;
use restate_wal_protocol::timer::TimerValue;

type PartitionStorage = storage::PartitionStorage<RocksDBStorage>;
type TimerService = restate_timer::TimerService<TimerValue, TokioClock, PartitionStorage>;

pub(crate) struct LeaderState {
    leader_epoch: LeaderEpoch,
    shuffle_hint_tx: HintSender,
    shuffle_task_id: TaskId,
    timer_service: Pin<Box<TimerService>>,
    non_deterministic_service_invoker: non_deterministic::ServiceInvoker,
    action_effect_handler: ActionEffectHandler,
}

pub(crate) struct FollowerState<I> {
    partition_id: PartitionId,
    timer_service_options: restate_timer::Options,
    channel_size: usize,
    invoker_tx: I,
    networking: Networking,
    partition_key_range: RangeInclusive<PartitionKey>,
    bifrost: Bifrost,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invoker is unreachable. This indicates a bug or the system is shutting down: {0}")]
    Invoker(NotRunningError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
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
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        timer_service_options: restate_timer::Options,
        channel_size: usize,
        invoker_tx: InvokerInputSender,
        bifrost: Bifrost,
        networking: Networking,
    ) -> (Self, ActionEffectStream) {
        (
            Self::Follower(FollowerState {
                partition_id,
                partition_key_range,
                timer_service_options,
                channel_size,
                invoker_tx,
                bifrost,
                networking,
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
    ) -> Result<(Self, ActionEffectStream), Error> {
        if let LeadershipState::Follower { .. } = self {
            self.unchecked_become_leader(epoch_sequence_number, partition_storage)
                .await
        } else {
            let (follower_state, _) = self.become_follower().await?;

            follower_state
                .unchecked_become_leader(epoch_sequence_number, partition_storage)
                .await
        }
    }

    async fn unchecked_become_leader(
        self,
        epoch_sequence_number: EpochSequenceNumber,
        partition_storage: &mut PartitionStorage,
    ) -> Result<(Self, ActionEffectStream), Error> {
        if let LeadershipState::Follower(mut follower_state) = self {
            let (mut service_invoker, service_invoker_output_rx) =
                non_deterministic::ServiceInvoker::new(partition_storage.clone());

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
                    follower_state.partition_id,
                    leader_epoch,
                    metadata().my_node_id().into(),
                ),
                partition_storage.clone(),
                shuffle_tx,
                follower_state.channel_size,
                follower_state.bifrost.clone(),
            );

            let shuffle_hint_tx = shuffle.create_hint_sender();

            let shuffle_task_id = task_center().spawn_child(
                TaskKind::Shuffle,
                "shuffle",
                Some(follower_state.partition_id),
                shuffle.run(),
            )?;

            let action_effect_handler = ActionEffectHandler::new(
                follower_state.partition_id,
                epoch_sequence_number,
                follower_state.partition_key_range.clone(),
                follower_state.bifrost.clone(),
            );

            Ok((
                LeadershipState::Leader {
                    follower_state,
                    leader_state: LeaderState {
                        leader_epoch,
                        shuffle_task_id,
                        shuffle_hint_tx,
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
            // Built-in services support only one response_sink
            debug_assert!(
                metadata.response_sinks.len() <= 1,
                "Built-in services support only one response_sink"
            );
            let response_sink = metadata.response_sinks.into_iter().next();
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
                    partition_id,
                    partition_key_range,
                    channel_size,
                    timer_service_options: num_in_memory_timers,
                    mut invoker_tx,
                    bifrost,
                    networking,
                },
            leader_state:
                LeaderState {
                    leader_epoch,
                    shuffle_task_id,
                    ..
                },
        } = self
        {
            let shuffle_handle = OptionFuture::from(task_center().cancel_task(shuffle_task_id));

            let (shuffle_result, abort_result) = tokio::join!(
                shuffle_handle,
                invoker_tx.abort_all_partition((partition_id, leader_epoch)),
            );

            abort_result.map_err(Error::Invoker)?;

            if let Some(shuffle_result) = shuffle_result {
                shuffle_result.expect("graceful termination of shuffle task");
            }

            Ok(Self::follower(
                partition_id,
                partition_key_range,
                num_in_memory_timers,
                channel_size,
                invoker_tx,
                bifrost,
                networking,
            ))
        } else {
            Ok((self, ActionEffectStream::Follower))
        }
    }

    pub(crate) async fn run_timer(&mut self) -> TimerValue {
        match self {
            LeadershipState::Follower { .. } => future::pending().await,
            LeadershipState::Leader {
                leader_state: LeaderState { timer_service, .. },
                ..
            } => timer_service.as_mut().next_timer().await,
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
                        &follower_state.networking,
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
        networking: &Networking,
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
            Action::IngressResponse(ingress_response) => {
                let invocation_id: InvocationId = ingress_response.invocation_id;
                // NOTE: We dispatch the response in a non-blocking task-center task to avoid
                // blocking partition processor. This comes with the risk of overwhelming the
                // runtime. This should be a temporary solution until we have a better way to
                // handle this case. Options are split into two categories:
                //
                // Category A) Do not block PP's loop if ingress is slow/unavailable
                //   - Add timeout to the disposable task to drop old/stale responses in congestion
                //   scenarios.
                //   - Limit the number of inflight ingress responses (per ingress node) by
                //   mapping node_id -> Vec<TaskId>
                // Category B) Enforce Back-pressure on PP if ingress is slow
                //   - Either directly or through a channel/buffer, block this loop if ingress node
                //   cannot keep up with the responses.
                //
                //  todo: Decide.
                let maybe_task = task_center().spawn_child(
                    TaskKind::Disposable,
                    "respond-to-ingress",
                    current_task_partition_id(),
                    {
                        let networking = networking.clone();
                        let invocation_id = invocation_id.clone();
                        async move {
                            if let Err(e) = networking
                                .send(
                                    ingress_response.target_node.into(),
                                    &ingress::IngressMessage::InvocationResponse(
                                        ingress::InvocationResponse {
                                            id: invocation_id.clone(),
                                            response: ingress_response.response,
                                        },
                                    ),
                                )
                                .await
                            {
                                warn!(
                                    ?e,
                                    ingress_node_id = ?ingress_response.target_node,
                                    invocation.id = %invocation_id,
                                    "Failed to send ingress response for invocation, will drop \
                                        the response on the floor"
                                );
                            }
                            Ok(())
                        }
                    },
                );

                if maybe_task.is_err() {
                    trace!(
                        restate.invocation.id = %invocation_id,
                        "Partition processor is shutting down, we are not sending response of {} to ingress",
                        invocation_id
                    );
                }
            }
        }

        Ok(())
    }

    pub async fn handle_action_effect(
        &mut self,
        action_effect: ActionEffect,
    ) -> anyhow::Result<()> {
        match self {
            LeadershipState::Follower(_) => {
                // nothing to do :-)
            }
            LeadershipState::Leader { leader_state, .. } => {
                leader_state
                    .action_effect_handler
                    .handle(action_effect)
                    .await?
            }
        };

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TaskError {
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}
