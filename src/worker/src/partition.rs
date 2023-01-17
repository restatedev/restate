use crate::fsm;
use crate::fsm::{Effects, Fsm};
use futures::{Stream, StreamExt};
use tracing::{debug, info};

#[derive(Debug)]
pub(super) struct PartitionProcessor<C> {
    command_stream: C,
    fsm: Fsm,
}

impl<C> PartitionProcessor<C>
where
    C: Stream<Item = consensus::Command<fsm::Command>>,
{
    pub(super) fn build(command_stream: C) -> Self {
        Self {
            command_stream,
            fsm: Fsm::default(),
        }
    }

    pub(super) async fn run(self) {
        let Self {
            command_stream,
            fsm,
        } = self;
        tokio::pin!(command_stream);

        loop {
            tokio::select! {
                command = command_stream.next() => {
                    if let Some(command) = command {
                        match command {
                            consensus::Command::Commit(fsm_command) => {
                                let effects = fsm.on_apply(fsm_command);
                                Self::apply_effects(effects);
                            }
                            consensus::Command::Leader => {
                                info!("Become leader.");
                            }
                            consensus::Command::Follower => {
                                info!("Become follower.");
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

        debug!("Shutting partition processor down.");
    }

    fn apply_effects(_effects: Effects) {}
}
