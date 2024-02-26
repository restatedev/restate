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
use crate::partition::leadership::{ActionEffect, ActionEffectStream, LeadershipState, TaskResult};
use crate::partition::state_machine::{DeduplicatingStateMachine, Effects};
use crate::partition::storage::{PartitionStorage, Transaction};
use crate::util::IdentitySender;
use futures::StreamExt;
use metrics::counter;
use restate_network::Networking;
use restate_schema_impl::Schemas;
use restate_storage_rocksdb::{RocksDBStorage, RocksDBTransaction};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, PeerId};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, instrument};

mod action_effect_handler;
mod leadership;
mod options;
mod services;
pub mod shuffle;
mod state_machine;
pub mod storage;
pub mod types;

use crate::partition::types::AckResponse;
pub use options::Options;
use restate_wal_protocol::Envelope;

type ConsensusReader = mpsc::Receiver<restate_consensus::Command<Envelope>>;
type ConsensusWriter = IdentitySender<Envelope>;
use restate_ingress_dispatcher::IngressDispatcherInputSender;
use restate_network::PartitionProcessorSender;

#[derive(Debug)]
pub(super) struct PartitionProcessor<RawEntryCodec, InvokerInputSender, NetworkHandle> {
    peer_id: PeerId,
    pub partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,

    timer_service_options: restate_timer::Options,
    channel_size: usize,

    consensus_reader: ConsensusReader,
    consensus_writer: ConsensusWriter,

    invoker_tx: InvokerInputSender,

    network_handle: NetworkHandle,

    ack_tx: restate_network::PartitionProcessorSender<AckResponse>,

    rocksdb_storage: RocksDBStorage,

    schemas: Schemas,

    options: Options,

    ingress_tx: IngressDispatcherInputSender,

    _entry_codec: PhantomData<RawEntryCodec>,
}

impl<RawEntryCodec, InvokerInputSender, NetworkHandle>
    PartitionProcessor<RawEntryCodec, InvokerInputSender, NetworkHandle>
where
    RawEntryCodec: restate_types::journal::raw::RawEntryCodec + Default + Debug,
    InvokerInputSender: restate_invoker_api::ServiceHandle + Clone,
    NetworkHandle: restate_network::NetworkHandle<shuffle::ShuffleInput, Envelope>,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer_id: PeerId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        timer_service_options: restate_timer::Options,
        channel_size: usize,
        consensus_reader: ConsensusReader,
        consensus_writer: ConsensusWriter,
        invoker_tx: InvokerInputSender,
        network_handle: NetworkHandle,
        ack_tx: restate_network::PartitionProcessorSender<AckResponse>,
        rocksdb_storage: RocksDBStorage,
        schemas: Schemas,
        options: Options,
        ingress_tx: IngressDispatcherInputSender,
    ) -> Self {
        Self {
            peer_id,
            partition_id,
            partition_key_range,
            timer_service_options,
            channel_size,
            consensus_reader,
            consensus_writer,
            invoker_tx,
            network_handle,
            ack_tx,
            _entry_codec: Default::default(),
            rocksdb_storage,
            schemas,
            options,
            ingress_tx,
        }
    }

    #[instrument(level = "trace", skip_all, fields(peer_id = %self.peer_id, partition_id = %self.partition_id))]
    pub(super) async fn run(self, _networking: Networking) -> anyhow::Result<()> {
        let PartitionProcessor {
            peer_id,
            partition_id,
            partition_key_range,
            timer_service_options,
            channel_size,
            mut consensus_reader,
            invoker_tx,
            network_handle,
            consensus_writer,
            ack_tx,
            rocksdb_storage,
            schemas,
            options,
            ingress_tx,
            ..
        } = self;

        let mut partition_storage =
            PartitionStorage::new(partition_id, partition_key_range.clone(), rocksdb_storage);

        let state_machine =
            Self::create_state_machine::<RawEntryCodec>(&mut partition_storage).await?;

        let mut inner = Inner::new_as_follower(
            state_machine,
            peer_id,
            partition_id,
            partition_key_range,
            schemas,
            timer_service_options,
            channel_size,
            invoker_tx,
            network_handle,
            ack_tx,
            consensus_writer,
            ingress_tx,
            options.max_batch_duration.map(Into::into),
        );

        loop {
            tokio::select! {
                mut next_command = consensus_reader.recv() => {
                    if next_command.is_none() {
                        break;
                    }

                    while let Some(command) = next_command.take() {
                        match command {
                            restate_consensus::Command::Apply(command) => {
                                next_command = inner.apply_command(command, &mut consensus_reader, partition_storage.create_transaction()).await?;
                            }
                            restate_consensus::Command::BecomeLeader(leader_epoch) => {
                                inner.become_leader(leader_epoch, &mut partition_storage).await?;
                            }
                            restate_consensus::Command::BecomeFollower => {
                                inner.become_follower().await?
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
                result = inner.poll_actions() => {
                    result?;
                }
            }
        }

        debug!(%peer_id, %partition_id, "Shutting partition processor down.");
        let _ = inner.become_follower().await;

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
}

struct Inner<C, I, N> {
    peer_id: PeerId,
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
    schemas: Schemas,
    state_machine: DeduplicatingStateMachine<C>,
    leadership_state: Option<LeadershipState<I, N>>,
    action_effect_stream: ActionEffectStream,
    action_effect_handler: Option<ActionEffectHandler>,
    effects_buffer: Effects,
    consensus_writer: ConsensusWriter,
    max_batch_duration: Option<Duration>,
}

impl<C, I, N> Inner<C, I, N>
where
    C: restate_types::journal::raw::RawEntryCodec + Default + Debug,
    I: restate_invoker_api::ServiceHandle + Clone,
    N: restate_network::NetworkHandle<shuffle::ShuffleInput, Envelope>,
{
    #[allow(clippy::too_many_arguments)]
    fn new_as_follower(
        state_machine: DeduplicatingStateMachine<C>,
        peer_id: PeerId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        schemas: Schemas,
        timer_service_options: restate_timer::Options,
        channel_size: usize,
        invoker_tx: I,
        network_handle: N,
        ack_tx: PartitionProcessorSender<AckResponse>,
        consensus_writer: ConsensusWriter,
        ingress_tx: IngressDispatcherInputSender,
        max_batch_duration: Option<Duration>,
    ) -> Self {
        let (action_effect_stream, leadership_state) = LeadershipState::follower(
            peer_id,
            partition_id,
            timer_service_options,
            channel_size,
            invoker_tx,
            network_handle,
            ack_tx,
            consensus_writer.clone(),
            ingress_tx,
        );

        Inner {
            peer_id,
            partition_id,
            partition_key_range,
            schemas,
            state_machine,
            leadership_state: Some(leadership_state),
            action_effect_stream,
            consensus_writer,
            action_effect_handler: None,
            effects_buffer: Effects::with_capacity(128),
            max_batch_duration,
        }
    }

    async fn become_leader(
        &mut self,
        leader_epoch: LeaderEpoch,
        partition_storage: &mut PartitionStorage<RocksDBStorage>,
    ) -> anyhow::Result<()> {
        debug!(restate.partition.peer = %self.peer_id, restate.partition.id = %self.partition_id, restate.partition.leader_epoch = %leader_epoch, "Become leader");

        let (action_effect_stream, leadership_state) = self
            .leadership_state
            .take()
            .expect("leadership state must be set")
            .become_leader(
                leader_epoch,
                self.partition_key_range.clone(),
                partition_storage,
                self.schemas.clone(),
            )
            .await?;

        self.action_effect_stream = action_effect_stream;
        self.leadership_state = Some(leadership_state);

        self.action_effect_handler = Some(ActionEffectHandler::new(
            self.partition_id,
            leader_epoch,
            self.partition_key_range.clone(),
            self.consensus_writer.clone(),
        ));

        Ok(())
    }

    async fn become_follower(&mut self) -> anyhow::Result<()> {
        info!(restate.partition.peer = %self.peer_id, restate.partition.id = %self.partition_id, "Become follower");
        let (action_effect_stream, leadership_state) = self
            .leadership_state
            .take()
            .expect("leadership state must be set")
            .become_follower()
            .await?;

        self.action_effect_stream = action_effect_stream;
        self.leadership_state = Some(leadership_state);
        self.action_effect_handler = None;

        Ok(())
    }

    async fn apply_command(
        &mut self,
        command: Envelope,
        consensus_reader: &mut ConsensusReader,
        transaction: Transaction<RocksDBTransaction<'_>>,
    ) -> anyhow::Result<Option<restate_consensus::Command<Envelope>>> {
        self.effects_buffer.clear();

        let leadership_state = self
            .leadership_state
            .take()
            .expect("leadership state must be present");
        let is_leader = leadership_state.is_leader();
        let message_collector = leadership_state.into_message_collector();

        let max_batch_duration_start = self
            .max_batch_duration
            .map(|duration| (duration, Instant::now()));

        // Apply state machine
        let mut application_result = self
            .state_machine
            .apply(
                command,
                &mut self.effects_buffer,
                transaction,
                message_collector,
                is_leader,
            )
            .await?;

        let mut next_command = None;

        while max_batch_duration_start
            .map(|(max_duration, start)| start.elapsed() < max_duration)
            .unwrap_or(true)
        {
            if let Ok(command) = consensus_reader.try_recv() {
                if let restate_consensus::Command::Apply(envelope) = command {
                    let (transaction, message_collector) = application_result.into_inner();
                    application_result = self
                        .state_machine
                        .apply(
                            envelope,
                            &mut self.effects_buffer,
                            transaction,
                            message_collector,
                            is_leader,
                        )
                        .await?;
                } else {
                    next_command = Some(command);
                    break;
                }
            } else {
                break;
            }
        }

        let message_collector = application_result.commit().await?;
        self.leadership_state = Some(message_collector.send().await?);

        Ok(next_command)
    }

    async fn poll_actions(&mut self) -> anyhow::Result<()> {
        tokio::select! {
            actuator_output = self.action_effect_stream.next() => {
                    counter!(PARTITION_ACTUATOR_HANDLED).increment(1);
                    let actuator_output = actuator_output.ok_or_else(|| anyhow::anyhow!("actuator stream is closed"))?;
                    self.action_effect_handler.as_ref().expect("actuator output handler must be present when being leader").handle(actuator_output).await;
                },
            task_result = self.leadership_state.as_mut().expect("leadership state must be set").run_tasks() => {
                match task_result {
                    TaskResult::Timer(timer) => {
                        counter!(PARTITION_TIMER_DUE_HANDLED).increment(1);
                        self.action_effect_handler.as_ref().expect("actuator output handler must be present when being leader").handle(ActionEffect::Timer(timer)).await;
                    },
                    TaskResult::TerminatedTask(result) => {
                        Err(result)?
                    }
                }
            },
        }

        Ok(())
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
