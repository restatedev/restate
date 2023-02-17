use common::types::{EntryIndex, InvocationId, PartitionId, PeerId, ServiceInvocationId};
use futures::{stream, Sink, SinkExt, Stream, StreamExt};
use service_protocol::codec::ProtobufRawEntryCodec;
use std::collections::HashSet;
use std::convert::Infallible;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use tracing::{debug, info};

mod effects;
mod leadership;
mod state_machine;
mod storage;

use crate::partition::effects::{Effects, Interpreter};
use crate::partition::leadership::LeadershipState;
use crate::partition::storage::PartitionStorage;
pub(crate) use state_machine::Command;

type StateMachine = state_machine::StateMachine<ProtobufRawEntryCodec>;

#[derive(Debug)]
pub(super) struct PartitionProcessor<
    CmdStream,
    ProposalSink,
    RawEntryCodec,
    InvokerInputSender,
    Storage,
> {
    peer_id: PeerId,
    partition_id: PartitionId,

    storage: Storage,

    command_stream: CmdStream,
    proposal_sink: ProposalSink,

    invoker_tx: InvokerInputSender,

    state_machine: StateMachine,

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

impl<CmdStream, ProposalSink, RawEntryCodec, InvokerInputSender, Storage>
    PartitionProcessor<CmdStream, ProposalSink, RawEntryCodec, InvokerInputSender, Storage>
where
    CmdStream: Stream<Item = consensus::Command<Command>>,
    ProposalSink: Sink<Command>,
    RawEntryCodec: journal::raw::RawEntryCodec + Debug,
    RawEntryCodec::Error: Debug,
    InvokerInputSender: invoker::InvokerInputSender + Clone,
    InvokerInputSender::Error: Debug,
    Storage: storage_api::Storage,
{
    pub(super) fn new(
        peer_id: PeerId,
        partition_id: PartitionId,
        command_stream: CmdStream,
        proposal_sink: ProposalSink,
        invoker_tx: InvokerInputSender,
        storage: Storage,
    ) -> Self {
        Self {
            peer_id,
            partition_id,
            command_stream,
            proposal_sink,
            invoker_tx,
            state_machine: StateMachine::default(),
            storage,
            _entry_codec: Default::default(),
        }
    }

    pub(super) async fn run(self) {
        let PartitionProcessor {
            peer_id,
            partition_id,
            command_stream,
            mut state_machine,
            invoker_tx,
            storage,
            proposal_sink,
            ..
        } = self;
        tokio::pin!(command_stream);
        tokio::pin!(proposal_sink);

        // The max number of effects should be 2 atm (e.g. RegisterTimer and AppendJournalEntry)
        let mut effects = Effects::with_capacity(2);

        let mut leadership_state = LeadershipState::follower(peer_id, partition_id, invoker_tx);

        let mut partition_storage = PartitionStorage::new(partition_id, storage);

        loop {
            let mut actuator_stream = leadership_state.actuator_stream();

            tokio::select! {
                command = command_stream.next() => {
                    if let Some(command) = command {
                        match command {
                            consensus::Command::Apply(fsm_command) => {
                                effects.clear();
                                state_machine.on_apply(fsm_command, &mut effects, &partition_storage).await.expect("State machine application must not fail");

                                let message_collector = leadership_state.into_message_collector();

                                let transaction = partition_storage.create_transaction();
                                let result = Interpreter::<RawEntryCodec>::interpret_effects(&mut effects, transaction, message_collector).await.expect("Effect interpreter must not fail");

                                let message_collector = result.commit().await.expect("Persisting state machine changes must not fail");
                                leadership_state = message_collector.send().await.expect("Actuator message sending must not fail");
                            }
                            consensus::Command::BecomeLeader(leader_epoch) => {
                                info!(%peer_id, %partition_id, %leader_epoch, "Become leader.");
                                leadership_state = leadership_state.become_leader(leader_epoch, &partition_storage).await;
                            }
                            consensus::Command::BecomeFollower => {
                                info!(%peer_id, %partition_id, "Become follower.");
                                leadership_state = leadership_state.become_follower().await;
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
                    let actuator_message = actuator_message.expect("Actuator stream must be open");
                    Self::propose_actuator_message(actuator_message, &mut proposal_sink).await;
                }
            }
        }

        debug!(%peer_id, %partition_id, "Shutting partition processor down.");
        leadership_state.become_follower().await;
    }

    async fn propose_actuator_message(
        actuator_message: invoker::OutputEffect,
        proposal_sink: &mut Pin<&mut ProposalSink>,
    ) {
        // Err only if the consensus module is shutting down
        let _ = proposal_sink.send(Command::Invoker(actuator_message)).await;
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
