use futures::StreamExt;
use restate_common::types::{PartitionId, PartitionKey, PeerId};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use tokio::sync::mpsc;
use tracing::{debug, info, instrument};

pub mod ack;
mod actuator_output_handler;
mod effects;
mod leadership;
pub mod shuffle;
mod state_machine;
pub mod storage;
mod types;

pub(super) use crate::partition::ack::{
    AckCommand, AckResponse, AckTarget, DeduplicationSource, IngressAckResponse,
    ShuffleDeduplicationResponse,
};
use crate::partition::actuator_output_handler::ActuatorOutputHandler;
use crate::partition::effects::{Effects, Interpreter};
use crate::partition::leadership::{ActuatorOutput, LeadershipState, TaskResult};
use crate::partition::storage::PartitionStorage;
use crate::util::IdentitySender;
use restate_storage_rocksdb::RocksDBStorage;
pub(crate) use state_machine::Command;
use state_machine::DeduplicatingStateMachine;
pub(super) use types::TimerValue;

#[derive(Debug)]
pub(super) struct PartitionProcessor<RawEntryCodec, InvokerInputSender, NetworkHandle> {
    peer_id: PeerId,
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,

    timer_service_options: restate_timer::Options,
    channel_size: usize,

    command_rx: mpsc::Receiver<restate_consensus::Command<AckCommand>>,
    proposal_tx: IdentitySender<AckCommand>,

    invoker_tx: InvokerInputSender,

    network_handle: NetworkHandle,

    ack_tx: restate_network::PartitionProcessorSender<AckResponse>,

    rocksdb_storage: RocksDBStorage,

    _entry_codec: PhantomData<RawEntryCodec>,
}

impl<RawEntryCodec, InvokerInputSender, NetworkHandle>
    PartitionProcessor<RawEntryCodec, InvokerInputSender, NetworkHandle>
where
    RawEntryCodec: restate_journal::raw::RawEntryCodec + Default + Debug,
    InvokerInputSender: restate_invoker::ServiceHandle + Clone,
    NetworkHandle: restate_network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer_id: PeerId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        timer_service_options: restate_timer::Options,
        channel_size: usize,
        command_stream: mpsc::Receiver<restate_consensus::Command<AckCommand>>,
        proposal_sender: IdentitySender<AckCommand>,
        invoker_tx: InvokerInputSender,
        network_handle: NetworkHandle,
        ack_tx: restate_network::PartitionProcessorSender<AckResponse>,
        rocksdb_storage: RocksDBStorage,
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
            ..
        } = self;

        // The max number of effects should be 2 atm (e.g. RegisterTimer and AppendJournalEntry)
        let mut effects = Effects::with_capacity(2);

        let partition_storage =
            PartitionStorage::new(partition_id, partition_key_range, rocksdb_storage);

        let (mut actuator_stream, mut leadership_state) = LeadershipState::follower(
            peer_id,
            partition_id,
            timer_service_options,
            channel_size,
            invoker_tx,
            network_handle,
            ack_tx,
        );

        let mut state_machine =
            Self::create_state_machine::<RawEntryCodec, _>(&partition_storage).await?;

        let actuator_output_handler = ActuatorOutputHandler::new(proposal_tx);

        loop {
            tokio::select! {
                command = command_rx.recv() => {
                    if let Some(command) = command {
                        match command {
                            restate_consensus::Command::Apply(ackable_command) => {
                                // Clear the effects to reuse the vector
                                effects.clear();

                                // Prepare transaction
                                let mut transaction = partition_storage.create_transaction();

                                // Handle the command, returns the span_relation to use to log effects
                                let (sid, span_relation) = state_machine.on_apply(ackable_command, &mut effects, &mut transaction).await?;

                                let is_leader = leadership_state.is_leader();

                                // Log the effects
                                effects.log(is_leader, sid, span_relation);

                                // Prepare message collector
                                let message_collector = leadership_state.into_message_collector();

                                // Interpret effects
                                let result = Interpreter::<RawEntryCodec>::interpret_effects(&mut effects, transaction, message_collector).await?;

                                // Commit actuator messages
                                let message_collector = result.commit().await?;
                                leadership_state = message_collector.send().await?;
                            }
                            restate_consensus::Command::BecomeLeader(leader_epoch) => {
                                info!(restate.partition.peer = %peer_id, restate.partition.id = %partition_id, restate.partition.leader_epoch = %leader_epoch, "Become leader");

                                (actuator_stream, leadership_state) = leadership_state.become_leader(
                                    leader_epoch,
                                    &partition_storage)
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
                    } else {
                        break;
                    }
                },
                actuator_output = actuator_stream.next() => {
                    let actuator_output = actuator_output.ok_or(anyhow::anyhow!("actuator stream is closed"))?;
                    actuator_output_handler.handle(actuator_output).await;
                },
                task_result = leadership_state.run_tasks() => {
                    match task_result {
                        TaskResult::Timer(timer) => {
                            actuator_output_handler.handle(ActuatorOutput::Timer(timer)).await;
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

    async fn create_state_machine<Codec, Storage>(
        partition_storage: &PartitionStorage<Storage>,
    ) -> Result<DeduplicatingStateMachine<Codec>, restate_storage_api::StorageError>
    where
        Codec: restate_journal::raw::RawEntryCodec + Default + Debug,
        Storage: restate_storage_api::Storage,
    {
        let mut transaction = partition_storage.create_transaction();
        let inbox_seq_number = transaction.load_inbox_seq_number().await?;
        let outbox_seq_number = transaction.load_outbox_seq_number().await?;
        transaction.commit().await?;

        let state_machine =
            state_machine::create_deduplicating_state_machine(inbox_seq_number, outbox_seq_number);

        Ok(state_machine)
    }
}
