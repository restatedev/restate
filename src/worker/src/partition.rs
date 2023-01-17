use crate::fsm;
use crate::fsm::{Effects, Fsm};
use futures::{Stream, StreamExt};
use tracing::debug;

#[derive(Debug)]
pub(super) struct PartitionProcessor<C> {
    command_stream: C,
    fsm: Fsm,
}

impl<C> PartitionProcessor<C>
where
    C: Stream<Item = fsm::Command>,
{
    pub(super) fn build(command_stream: C) -> Self {
        Self {
            command_stream,
            fsm: Fsm::default(),
        }
    }

    pub(super) async fn run(self, drain: drain::Watch) {
        let Self {
            command_stream,
            fsm,
        } = self;
        let shutdown = drain.signaled();

        tokio::pin!(shutdown);
        tokio::pin!(command_stream);

        loop {
            tokio::select! {
                command = command_stream.next() => {
                    if let Some(command) = command {
                        let effects = fsm.on_apply(command);
                        Self::apply_effects(effects);
                    } else {
                        break;
                    }
                }
                _ = &mut shutdown => {
                    break;
                }
            }
        }

        debug!("Shutting partition processor down.");
    }

    fn apply_effects(_effects: Effects) {}
}
