use common::types::{EntryIndex, InvocationId, PartitionId, PeerId, ServiceInvocationId};
use futures::{stream, StreamExt};
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tracing::{debug, info};

pub mod ack;
mod actuator_output_handler;
mod effects;
mod leadership;
pub mod shuffle;
mod state_machine;
mod storage;
mod types;

pub(super) use crate::partition::ack::{
    AckResponse, AckTarget, AckableCommand, IngressAckResponse, ShuffleAckResponse,
};
use crate::partition::actuator_output_handler::ActuatorOutputHandler;
use crate::partition::effects::{Effects, Interpreter};
use crate::partition::leadership::LeadershipState;
use crate::util::IdentitySender;
pub(crate) use state_machine::Command;

#[derive(Debug)]
pub(super) struct PartitionProcessor<RawEntryCodec, InvokerInputSender, NetworkHandle, KeyExtractor>
{
    peer_id: PeerId,
    partition_id: PartitionId,

    command_rx: mpsc::Receiver<consensus::Command<AckableCommand>>,
    proposal_tx: IdentitySender<AckableCommand>,

    invoker_tx: InvokerInputSender,

    state_machine: state_machine::StateMachine<RawEntryCodec>,

    network_handle: NetworkHandle,

    ack_tx: network::PartitionProcessorSender<AckResponse>,

    key_extractor: KeyExtractor,

    _entry_codec: PhantomData<RawEntryCodec>,
}

#[derive(Debug, Clone)]
pub(super) struct RocksDBJournalReader;

impl invoker::JournalReader for RocksDBJournalReader {
    type JournalStream = stream::Empty<journal::raw::PlainRawEntry>;
    type Error = Infallible;
    type Future = futures::future::Pending<
        Result<(invoker::JournalMetadata, Self::JournalStream), Self::Error>,
    >;

    fn read_journal(&self, _sid: &ServiceInvocationId) -> Self::Future {
        // TODO implement this
        unimplemented!("Implement JournalReader")
    }
}

impl<RawEntryCodec, InvokerInputSender, NetworkHandle, KeyExtractor>
    PartitionProcessor<RawEntryCodec, InvokerInputSender, NetworkHandle, KeyExtractor>
where
    RawEntryCodec: journal::raw::RawEntryCodec + Default + Debug,
    InvokerInputSender: invoker::InvokerInputSender + Clone,
    NetworkHandle: network::NetworkHandle<shuffle::ShuffleInput, shuffle::ShuffleOutput>,
    KeyExtractor: service_key_extractor::KeyExtractor,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        peer_id: PeerId,
        partition_id: PartitionId,
        command_stream: mpsc::Receiver<consensus::Command<AckableCommand>>,
        proposal_sender: IdentitySender<AckableCommand>,
        invoker_tx: InvokerInputSender,
        network_handle: NetworkHandle,
        ack_tx: network::PartitionProcessorSender<AckResponse>,
        key_extractor: KeyExtractor,
    ) -> Self {
        Self {
            peer_id,
            partition_id,
            command_rx: command_stream,
            proposal_tx: proposal_sender,
            invoker_tx,
            state_machine: Default::default(),
            network_handle,
            ack_tx,
            key_extractor,
            _entry_codec: Default::default(),
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let PartitionProcessor {
            peer_id,
            partition_id,
            mut command_rx,
            mut state_machine,
            invoker_tx,
            network_handle,
            proposal_tx,
            ack_tx,
            key_extractor,
            ..
        } = self;

        // The max number of effects should be 2 atm (e.g. RegisterTimer and AppendJournalEntry)
        let mut effects = Effects::with_capacity(2);

        let (mut actuator_stream, mut leadership_state) =
            LeadershipState::follower(peer_id, partition_id, invoker_tx, network_handle);

        let mut partition_storage = storage::InMemoryPartitionStorage::new();
        let actuator_output_handler =
            ActuatorOutputHandler::<_, RawEntryCodec>::new(proposal_tx, key_extractor);

        loop {
            tokio::select! {
                command = command_rx.recv() => {
                    if let Some(command) = command {
                        match command {
                            consensus::Command::Apply(ackable_command) => {
                                let (fsm_command, ack_target) = ackable_command.into_inner();

                                effects.clear();
                                state_machine.on_apply(fsm_command, &mut effects, &partition_storage).await?;

                                let message_collector = leadership_state.into_message_collector();

                                let transaction = partition_storage.create_transaction();
                                let result = Interpreter::<RawEntryCodec>::interpret_effects(&mut effects, transaction, message_collector).await?;

                                let message_collector = result.commit().await?;
                                leadership_state = message_collector.send().await?;

                                if let Some(ack_target) = ack_target {
                                    ack_tx.send(ack_target.acknowledge()).await?;
                                }
                            }
                            consensus::Command::BecomeLeader(leader_epoch) => {
                                info!(%peer_id, %partition_id, %leader_epoch, "Become leader.");
                                (actuator_stream, leadership_state) = leadership_state.become_leader(leader_epoch, partition_storage.clone()).await?;
                            }
                            consensus::Command::BecomeFollower => {
                                info!(%peer_id, %partition_id, "Become follower.");
                                (actuator_stream, leadership_state) = leadership_state.become_follower().await?;
                            },
                            consensus::Command::ApplySnapshot => {
                                unimplemented!("Not supported yet.");
                            }
                            consensus::Command::CreateSnapshot => {
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
                    Err(task_result)?
                }
            }
        }

        debug!(%peer_id, %partition_id, "Shutting partition processor down.");
        let _ = leadership_state.become_follower().await;

        Ok(())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum InvocationStatus {
    Invoked(InvocationId),
    Suspended {
        invocation_id: InvocationId,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    Free,
}
