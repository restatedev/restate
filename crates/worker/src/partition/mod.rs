// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::metric_definitions::{PARTITION_ACTUATOR_HANDLED, PARTITION_TIMER_DUE_HANDLED};
use crate::partition::action_effect_handler::ActionEffectHandler;
use crate::partition::leadership::{ActionEffect, LeadershipState, TaskResult};
use crate::partition::state_machine::{
    AckCommand, ActionCollector, DeduplicatingStateMachine, Effects, InterpretationResult,
};
use crate::partition::storage::{PartitionStorage, Transaction};
use crate::util::IdentitySender;
use futures::StreamExt;
use metrics::counter;
use restate_schema_impl::Schemas;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{PartitionId, PartitionKey, PeerId};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;
use tracing::{debug, info, instrument};

mod action_effect_handler;
mod leadership;
mod services;
pub mod shuffle;
mod state_machine;
pub mod storage;
mod types;

pub use state_machine::{
    AckCommand as StateMachineAckCommand, AckResponse as StateMachineAckResponse,
    AckTarget as StateMachineAckTarget, Command as StateMachineCommand,
    DeduplicationSource as StateMachineDeduplicationSource,
    IngressAckResponse as StateMachineIngressAckResponse,
    ShuffleDeduplicationResponse as StateMachineShuffleDeduplicationResponse,
};
pub(super) use types::TimerValue;

#[derive(Debug)]
pub(super) struct PartitionProcessor<RawEntryCodec, InvokerInputSender, NetworkHandle> {
    peer_id: PeerId,
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,

    timer_service_options: restate_timer::Options,
    channel_size: usize,

    command_rx: mpsc::Receiver<restate_consensus::Command<StateMachineAckCommand>>,
    proposal_tx: IdentitySender<StateMachineAckCommand>,

    invoker_tx: InvokerInputSender,

    network_handle: NetworkHandle,

    ack_tx: restate_network::PartitionProcessorSender<StateMachineAckResponse>,

    rocksdb_storage: RocksDBStorage,

    schemas: Schemas,

    _entry_codec: PhantomData<RawEntryCodec>,
}

impl<RawEntryCodec, InvokerInputSender, NetworkHandle>
    PartitionProcessor<RawEntryCodec, InvokerInputSender, NetworkHandle>
where
    RawEntryCodec: restate_types::journal::raw::RawEntryCodec + Default + Debug,
    InvokerInputSender: restate_invoker_api::ServiceHandle + Clone,
    NetworkHandle: restate_network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer_id: PeerId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        timer_service_options: restate_timer::Options,
        channel_size: usize,
        command_stream: mpsc::Receiver<restate_consensus::Command<StateMachineAckCommand>>,
        proposal_sender: IdentitySender<StateMachineAckCommand>,
        invoker_tx: InvokerInputSender,
        network_handle: NetworkHandle,
        ack_tx: restate_network::PartitionProcessorSender<StateMachineAckResponse>,
        rocksdb_storage: RocksDBStorage,
        schemas: Schemas,
    ) -> Self {
        Self {
            peer_id,
            partition_id,
            partition_key_range,
            timer_service_options,
            channel_size,
            command_rx: command_stream,
            proposal_tx: proposal_sender,
            invoker_tx,
            network_handle,
            ack_tx,
            _entry_codec: Default::default(),
            rocksdb_storage,
            schemas,
        }
    }

    #[instrument(level = "trace", skip_all, fields(peer_id = %self.peer_id, partition_id = %self.partition_id))]
    pub(super) async fn run(self) -> anyhow::Result<()> {
        let PartitionProcessor {
            peer_id,
            partition_id,
            partition_key_range,
            timer_service_options,
            channel_size,
            mut command_rx,
            invoker_tx,
            network_handle,
            proposal_tx,
            ack_tx,
            rocksdb_storage,
            schemas,
            ..
        } = self;

        // The max number of effects should be 2 atm (e.g. RegisterTimer and AppendJournalEntry)
        let mut effects = Effects::with_capacity(2);

        let mut partition_storage =
            PartitionStorage::new(partition_id, partition_key_range.clone(), rocksdb_storage);

        let (mut actuator_stream, mut leadership_state) = LeadershipState::follower(
            peer_id,
            partition_id,
            timer_service_options,
            channel_size,
            invoker_tx,
            network_handle,
            ack_tx,
            proposal_tx.clone(),
        );

        let mut state_machine =
            Self::create_state_machine::<RawEntryCodec>(&mut partition_storage).await?;

        let actuator_output_handler = ActionEffectHandler::new(proposal_tx);

        loop {
            tokio::select! {
                mut next_command = command_rx.recv() => {
                    if next_command.is_none() {
                        break;
                    }

                    while let Some(command) = next_command.take() {
                        match command {
                            restate_consensus::Command::Apply(command) => {
                                // Clear the effects to reuse the vector
                                effects.clear();

                                // Prepare transaction
                                let transaction = partition_storage.create_transaction();

                                // Prepare message collector
                                let is_leader = leadership_state.is_leader();
                                let message_collector = leadership_state.into_message_collector();

                                let (application_result, command) = Self::apply_command(
                                    &mut state_machine,
                                    command,
                                    &mut effects,
                                    transaction,
                                    message_collector,
                                    is_leader,
                                    &mut command_rx)
                                .await?;

                                next_command = command;

                                // Commit actuator messages
                                let message_collector = application_result.commit().await?;
                                leadership_state = message_collector.send().await?;
                            }
                            restate_consensus::Command::BecomeLeader(leader_epoch) => {
                                debug!(restate.partition.peer = %peer_id, restate.partition.id = %partition_id, restate.partition.leader_epoch = %leader_epoch, "Become leader");

                                (actuator_stream, leadership_state) = leadership_state.become_leader(
                                    leader_epoch,
                                    partition_key_range.clone(),
                                    &mut partition_storage,
                                    &schemas,

                                )
                                .await?;
                            }
                            restate_consensus::Command::BecomeFollower => {
                                info!(restate.partition.peer = %peer_id, restate.partition.id = %partition_id, "Become follower");
                                (actuator_stream, leadership_state) = leadership_state.become_follower().await?;
                            },
                            restate_consensus::Command::ApplySnapshot => {
                                unimplemented!("Not supported yet.");
                            }
                            restate_consensus::Command::CreateSnapshot => {
                                unimplemented!("Not supported yet.");
                            }
                        }
                    }
                },
                actuator_output = actuator_stream.next() => {
                    counter!(PARTITION_ACTUATOR_HANDLED).increment(1);
                    let actuator_output = actuator_output.ok_or(anyhow::anyhow!("actuator stream is closed"))?;
                    actuator_output_handler.handle(actuator_output).await;
                },
                task_result = leadership_state.run_tasks() => {
                    match task_result {
                        TaskResult::Timer(timer) => {
                            counter!(PARTITION_TIMER_DUE_HANDLED).increment(1);
                            actuator_output_handler.handle(ActionEffect::Timer(timer)).await;
                        },
                        TaskResult::TerminatedTask(result) => {
                            Err(result)?
                        }
                    }
                },
            }
        }
        debug!(%peer_id, %partition_id, "Shutting partition processor down.");
        let _ = leadership_state.become_follower().await;

        Ok(())
    }

    async fn create_state_machine<Codec>(
        partition_storage: &mut PartitionStorage<RocksDBStorage>,
    ) -> Result<DeduplicatingStateMachine<Codec>, restate_storage_api::StorageError>
    where
        Codec: restate_types::journal::raw::RawEntryCodec + Default + Debug,
    {
        let inbox_seq_number = partition_storage.load_inbox_seq_number().await?;
        let outbox_seq_number = partition_storage.load_outbox_seq_number().await?;

        let state_machine = DeduplicatingStateMachine::new(inbox_seq_number, outbox_seq_number);

        Ok(state_machine)
    }

    pub async fn apply_command<
        TransactionType: restate_storage_api::Transaction + Send,
        Collector: ActionCollector,
    >(
        state_machine: &mut DeduplicatingStateMachine<RawEntryCodec>,
        command: AckCommand,
        effects: &mut Effects,
        transaction: Transaction<TransactionType>,
        message_collector: Collector,
        is_leader: bool,
        command_rx: &mut mpsc::Receiver<restate_consensus::Command<AckCommand>>,
    ) -> Result<
        (
            InterpretationResult<Transaction<TransactionType>, Collector>,
            Option<restate_consensus::Command<AckCommand>>,
        ),
        state_machine::Error,
    > {
        // Apply state machine
        let mut application_result = state_machine
            .apply(command, effects, transaction, message_collector, is_leader)
            .await?;

        while let Ok(command) = command_rx.try_recv() {
            if let restate_consensus::Command::Apply(command) = command {
                let (transaction, message_collector) = application_result.into_inner();
                application_result = state_machine
                    .apply(command, effects, transaction, message_collector, is_leader)
                    .await?;
            } else {
                return Ok((application_result, Some(command)));
            }
        }

        Ok((application_result, None))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("failed committing results: {source:?}")]
pub struct CommitError {
    source: Option<anyhow::Error>,
}

impl CommitError {
    pub fn with_source(source: impl Into<anyhow::Error>) -> Self {
        CommitError {
            source: Some(source.into()),
        }
    }
}

pub trait Committable {
    fn commit(self) -> impl Future<Output = Result<(), CommitError>> + Send;
}
