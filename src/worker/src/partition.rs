use crate::fsm;
use crate::fsm::{Effects, Fsm};
use common::types::PeerId;
use futures::{Sink, Stream, StreamExt};
use tracing::{debug, info};

#[derive(Debug)]
pub(super) struct PartitionProcessor<C, P> {
    id: PeerId,
    command_stream: C,
    _proposal_sink: P,
    fsm: Fsm,
}

impl<C, P> PartitionProcessor<C, P>
where
    C: Stream<Item = consensus::Command<fsm::Command>>,
    P: Sink<fsm::Command>,
{
    pub(super) fn new(id: PeerId, command_stream: C, proposal_sink: P) -> Self {
        Self {
            id,
            command_stream,
            _proposal_sink: proposal_sink,
            fsm: Fsm::default(),
        }
    }

    pub(super) async fn run(self) {
        let PartitionProcessor {
            id,
            command_stream,
            fsm,
            ..
        } = self;
        tokio::pin!(command_stream);

        let mut effects = Effects::default();

        loop {
            tokio::select! {
                command = command_stream.next() => {
                    if let Some(command) = command {
                        match command {
                            consensus::Command::Commit(fsm_command) => {
                                effects.clear();
                                state_machine.on_apply(fsm_command, &mut effects);
                                Self::apply_effects(&effects);
                            }
                            consensus::Command::Leader => {
                                info!(%id, "Become leader.");
                            }
                            consensus::Command::Follower => {
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

    fn apply_effects(_effects: &Effects) {}
}
