use common::types::{EntryIndex, InvocationId, PartitionId, PeerId, ServiceInvocationId};
use futures::{stream, Sink, SinkExt, Stream, StreamExt};
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use tracing::{debug, info};

mod effects;
mod leadership;
pub mod shuffle;
mod state_machine;
mod storage;

use crate::partition::effects::{Effects, Interpreter};
use crate::partition::leadership::{ActuatorOutput, LeadershipState};
use crate::partition::storage::PartitionStorage;
pub(crate) use state_machine::Command;

#[derive(Debug)]
pub(super) struct PartitionProcessor<
    CmdStream,
    ProposalSink,
    RawEntryCodec,
    InvokerInputSender,
    NetworkHandle,
    Storage,
> {
    peer_id: PeerId,
    partition_id: PartitionId,

    storage: Storage,

    command_stream: CmdStream,
    proposal_sink: ProposalSink,

    invoker_tx: InvokerInputSender,

    state_machine: state_machine::StateMachine<RawEntryCodec>,

    network_handle: NetworkHandle,

    _entry_codec: PhantomData<RawEntryCodec>,
}

#[derive(Debug, Clone)]
pub(super) struct RocksDBJournalReader;

impl invoker::JournalReader for RocksDBJournalReader {
    type JournalStream = stream::Empty<journal::raw::RawEntry>;
    type Error = Infallible;
    type Future = futures::future::Pending<
        Result<(invoker::JournalMetadata, Self::JournalStream), Self::Error>,
    >;

    fn read_journal(&self, _sid: &ServiceInvocationId) -> Self::Future {
        // TODO implement this
        unimplemented!("Implement JournalReader")
    }
}

impl<CmdStream, ProposalSink, RawEntryCodec, InvokerInputSender, NetworkHandle, Storage>
    PartitionProcessor<
        CmdStream,
        ProposalSink,
        RawEntryCodec,
        InvokerInputSender,
        NetworkHandle,
        Storage,
    >
where
    CmdStream: Stream<Item = consensus::Command<Command>>,
    ProposalSink: Sink<Command>,
    RawEntryCodec: journal::raw::RawEntryCodec + Default + Debug,
    InvokerInputSender: invoker::InvokerInputSender + Clone,
    NetworkHandle: network::NetworkHandle<shuffle::NetworkInput, shuffle::NetworkOutput>,
    Storage: storage_api::Storage + Clone + Send + 'static,
{
    pub(super) fn new(
        peer_id: PeerId,
        partition_id: PartitionId,
        command_stream: CmdStream,
        proposal_sink: ProposalSink,
        invoker_tx: InvokerInputSender,
        storage: Storage,
        network_handle: NetworkHandle,
    ) -> Self {
        Self {
            peer_id,
            partition_id,
            command_stream,
            proposal_sink,
            invoker_tx,
            state_machine: Default::default(),
            storage,
            network_handle,
            _entry_codec: Default::default(),
        }
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        let PartitionProcessor {
            peer_id,
            partition_id,
            command_stream,
            mut state_machine,
            invoker_tx,
            network_handle,
            storage,
            proposal_sink,
            ..
        } = self;
        tokio::pin!(command_stream);
        tokio::pin!(proposal_sink);

        // The max number of effects should be 2 atm (e.g. RegisterTimer and AppendJournalEntry)
        let mut effects = Effects::with_capacity(2);

        let mut leadership_state =
            LeadershipState::follower(peer_id, partition_id, invoker_tx, network_handle);

        let mut partition_storage = PartitionStorage::new(partition_id, storage);

        loop {
            let mut actuator_stream = leadership_state.actuator_stream();

            tokio::select! {
                command = command_stream.next() => {
                    if let Some(command) = command {
                        match command {
                            consensus::Command::Apply(fsm_command) => {
                                effects.clear();
                                state_machine.on_apply(fsm_command, &mut effects, &partition_storage).await?;

                                let message_collector = leadership_state.into_message_collector();

                                let transaction = partition_storage.create_transaction();
                                let result = Interpreter::<RawEntryCodec>::interpret_effects(&mut effects, transaction, message_collector).await?;

                                let message_collector = result.commit().await?;
                                leadership_state = message_collector.send().await?;
                            }
                            consensus::Command::BecomeLeader(leader_epoch) => {
                                info!(%peer_id, %partition_id, %leader_epoch, "Become leader.");
                                leadership_state = leadership_state.become_leader(leader_epoch, partition_storage.clone()).await?;
                            }
                            consensus::Command::BecomeFollower => {
                                info!(%peer_id, %partition_id, "Become follower.");
                                leadership_state = leadership_state.become_follower().await?;
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
                actuator_message = actuator_stream.next() => {
                    let actuator_message = actuator_message.ok_or(anyhow::anyhow!("actuator stream is closed"))?;
                    Self::handle_actuator_message(actuator_message, &mut proposal_sink).await?;
                }
            }
        }

        debug!(%peer_id, %partition_id, "Shutting partition processor down.");
        let _ = leadership_state.become_follower().await;

        Ok(())
    }

    async fn handle_actuator_message(
        actuator_message: ActuatorOutput,
        proposal_sink: &mut Pin<&mut ProposalSink>,
    ) -> anyhow::Result<()> {
        match actuator_message {
            ActuatorOutput::Invoker(invoker_output) => {
                // Err only if the consensus module is shutting down
                let _ = proposal_sink.send(Command::Invoker(invoker_output)).await;
            }
            ActuatorOutput::Shuffle(outbox_truncation) => {
                // Err only if the consensus module is shutting down
                let _ = proposal_sink
                    .send(Command::OutboxTruncation(outbox_truncation.index()))
                    .await;
            }
            ActuatorOutput::ShuffleTaskTermination { error } => {
                let error =
                    error.unwrap_or(anyhow::anyhow!("shuffle task terminated unexpectedly"));

                return Err(error);
            }
        };

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum InvocationStatus {
    Invoked(InvocationId),
    Suspended {
        invocation_id: InvocationId,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    Free,
}
