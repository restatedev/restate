use crate::partition::state_machine::{Effects, StateMachine};
use common::types::{InvocationId, LeaderEpoch, PartitionId, PeerId};
use futures::{stream, Sink, SinkExt, Stream, StreamExt};
use std::fmt::Debug;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tracing::{debug, info};

mod state_machine;

pub(crate) use state_machine::Command;

#[derive(Debug)]
pub(super) struct PartitionProcessor<CmdStream, ProposalSink, InvokerSink, Storage> {
    peer_id: PeerId,
    partition_id: PartitionId,

    storage: Storage,

    command_stream: CmdStream,
    proposal_sink: ProposalSink,
    invoker_sink: InvokerSink,

    state_machine: StateMachine,
}

impl<CmdStream, ProposalSink, InvokerSink, Storage>
    PartitionProcessor<CmdStream, ProposalSink, InvokerSink, Storage>
where
    CmdStream: Stream<Item = consensus::Command<Command>>,
    ProposalSink: Sink<Command>,
    InvokerSink: Sink<invoker::Input>,
    InvokerSink::Error: Debug,
    Storage: storage_api::Storage,
{
    pub(super) fn new(
        peer_id: PeerId,
        partition_id: PartitionId,
        command_stream: CmdStream,
        proposal_sink: ProposalSink,
        invoker_sink: InvokerSink,
        storage: Storage,
    ) -> Self {
        Self {
            peer_id,
            partition_id,
            command_stream,
            proposal_sink,
            invoker_sink,
            state_machine: StateMachine::default(),
            storage,
        }
    }

    pub(super) async fn run(self) {
        let PartitionProcessor {
            peer_id,
            partition_id,
            command_stream,
            state_machine,
            invoker_sink,
            storage,
            proposal_sink,
        } = self;
        tokio::pin!(command_stream);
        tokio::pin!(invoker_sink);
        tokio::pin!(proposal_sink);

        let mut effects = Effects::default();

        let mut leadership_state = LeadershipState::follower(partition_id, invoker_sink);

        loop {
            let mut actuator_stream = leadership_state.actuator_stream();

            tokio::select! {
                command = command_stream.next() => {
                    if let Some(command) = command {
                        match command {
                            consensus::Command::Apply(fsm_command) => {
                                effects.clear();
                                state_machine.on_apply(fsm_command, &mut effects, &storage);
                                Self::apply_effects(&effects, &storage);
                            }
                            consensus::Command::BecomeLeader(leader_epoch) => {
                                info!(%peer_id, %partition_id, %leader_epoch, "Become leader.");
                                leadership_state = leadership_state.become_leader(leader_epoch, &storage).await;
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
        actuator_message: invoker::Output,
        proposal_sink: &mut Pin<&mut ProposalSink>,
    ) {
        // Err only if the consensus module is shutting down
        let _ = proposal_sink.send(Command::Invoker(actuator_message)).await;
    }

    fn apply_effects(_effects: &Effects, _storage: &Storage) {
        // here is a simple example:
        //
        // let mut txn = storage.transaction();
        // txn.put(TableKind::State, "hello", "world");
        // txn.commit();
    }
}

enum LeadershipState<InvokerSink> {
    Follower {
        partition_id: PartitionId,
        invoker_sink: InvokerSink,
    },

    Leader {
        partition_id: PartitionId,
        leader_epoch: LeaderEpoch,
        invoker_rx: mpsc::Receiver<invoker::Output>,
        invoker_sink: InvokerSink,
    },
}

impl<InvokerSink> LeadershipState<InvokerSink>
where
    InvokerSink: Sink<invoker::Input> + Unpin,
    InvokerSink::Error: Debug,
{
    fn follower(partition_id: PartitionId, invoker_sink: InvokerSink) -> Self {
        Self::Follower {
            partition_id,
            invoker_sink,
        }
    }

    async fn become_leader<S: storage_api::Storage>(
        self,
        leader_epoch: LeaderEpoch,
        storage: &S,
    ) -> Self {
        if let LeadershipState::Follower { .. } = self {
            self.unchecked_become_leader(leader_epoch, storage).await
        } else {
            self.become_follower()
                .await
                .unchecked_become_leader(leader_epoch, storage)
                .await
        }
    }

    async fn unchecked_become_leader<S: storage_api::Storage>(
        self,
        leader_epoch: LeaderEpoch,
        storage: &S,
    ) -> Self {
        if let LeadershipState::Follower {
            partition_id,
            mut invoker_sink,
        } = self
        {
            let (tx, rx) = mpsc::channel(1);

            invoker_sink
                .send(invoker::Input::Register((partition_id, leader_epoch), tx))
                .await
                .expect("Invoker should be running");

            let mut invoked_invocations = Self::invoked_invocations(storage);

            while let Some(invocation_id) = invoked_invocations.next().await {
                invoker_sink
                    .send(invoker::Input::Invoke(invocation_id))
                    .await
                    .expect("Invoker should be running");
            }

            LeadershipState::Leader {
                partition_id,
                leader_epoch,
                invoker_rx: rx,
                invoker_sink,
            }
        } else {
            unreachable!("This method should only be called if I am a follower!");
        }
    }

    async fn become_follower(self) -> Self {
        if let LeadershipState::Leader {
            partition_id,
            leader_epoch,
            mut invoker_sink,
            ..
        } = self
        {
            invoker_sink
                .send(invoker::Input::Abort((partition_id, leader_epoch)))
                .await
                .expect("Invoker should be running");
            Self::follower(partition_id, invoker_sink)
        } else {
            self
        }
    }

    fn actuator_stream(&mut self) -> ActuatorStream<'_, InvokerSink> {
        ActuatorStream { inner: self }
    }

    fn invoked_invocations<S>(_storage: &S) -> impl Stream<Item = InvocationId> {
        stream::empty()
    }
}

struct ActuatorStream<'a, I> {
    inner: &'a mut LeadershipState<I>,
}

impl<'a, I> Stream for ActuatorStream<'a, I> {
    type Item = invoker::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.deref_mut().inner {
            LeadershipState::Leader { invoker_rx, .. } => invoker_rx.poll_recv(cx),
            LeadershipState::Follower { .. } => Poll::Pending,
        }
    }
}
