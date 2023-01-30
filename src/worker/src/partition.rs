use crate::partition::state_machine::{Effects, StateMachine};
use common::types::PeerId;
use futures::{Sink, Stream, StreamExt};
use storage_api::Storage;
use tracing::{debug, info};

mod state_machine;

pub(crate) use state_machine::Command;

#[derive(Debug)]
pub(super) struct PartitionProcessor<C, P, S> {
    id: PeerId,
    command_stream: C,
    _proposal_sink: P,
    storage: S,
    state_machine: StateMachine,
}

impl<C, P, S> PartitionProcessor<C, P, S>
where
    C: Stream<Item = consensus::Command<Command>>,
    P: Sink<Command>,
    S: Storage,
{
    pub(super) fn new(id: PeerId, command_stream: C, proposal_sink: P, storage: S) -> Self {
        Self {
            id,
            command_stream,
            _proposal_sink: proposal_sink,
            state_machine: StateMachine::default(),
            storage,
        }
    }

    pub(super) async fn run(self) {
        let PartitionProcessor {
            id,
            command_stream,
            state_machine,
            storage,
            ..
        } = self;
        tokio::pin!(command_stream);

        let mut effects = Effects::default();

        loop {
            tokio::select! {
                command = command_stream.next() => {
                    if let Some(command) = command {
                        match command {
                            consensus::Command::Apply(fsm_command) => {
                                effects.clear();
                                state_machine.on_apply(fsm_command, &mut effects, &storage);
                                Self::apply_effects(&effects, &storage);
                            }
                            consensus::Command::BecomeLeader => {
                                info!(%id, "Become leader.");
                            }
                            consensus::Command::BecomeFollower => {
                                info!(%id, "Become follower.");
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
                }
            }
        }

        debug!(%id, "Shutting partition processor down.");
    }

    fn apply_effects(_effects: &Effects, _storage: &S) {
        // here is a simple example:
        //
        // let mut txn = storage.transaction();
        // txn.put(TableKind::State, "hello", "world");
        // txn.commit();
    }
}
