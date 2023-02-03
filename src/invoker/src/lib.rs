use common::types::{InvocationId, PartitionLeaderEpoch};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;
use tracing::debug;

#[derive(Debug)]
pub enum Input {
    Invoke(InvocationId),
    Resume,
    Completion,
    Abort(PartitionLeaderEpoch),
    StorageAck,

    // needed for dynamic registration at Invoker
    Register(PartitionLeaderEpoch, mpsc::Sender<Output>),
}

#[derive(Debug)]
pub enum Output {
    JournalEntry,
    Suspended,
    End,
    Failed,
}

pub type InvokerSender = PollSender<Input>;

#[derive(Debug)]
pub struct Invoker {
    input: mpsc::Receiver<Input>,
    partition_processors: HashMap<PartitionLeaderEpoch, mpsc::Sender<Output>>,

    // used for constructing the invoker sender
    input_tx: mpsc::Sender<Input>,
}

impl Invoker {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(32);

        Invoker {
            input: rx,
            partition_processors: HashMap::new(),
            input_tx: tx,
        }
    }

    pub fn create_sender(&self) -> InvokerSender {
        PollSender::new(self.input_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) {
        let Invoker {
            mut input,
            mut partition_processors,
            ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                input_message = input.recv() => {
                    let input_message = input_message.expect("Input is never closed");

                    match input_message {
                        Input::Register(partition_leader_epoch, sender) => {
                            partition_processors.insert(partition_leader_epoch, sender);
                        }
                        Input::Abort(partition_leader_epoch) => {
                            partition_processors.remove(&partition_leader_epoch);
                        },
                        Input::Invoke(..) => {
                            unimplemented!("Not yet implemented");
                        },
                        Input::Resume => {
                            unimplemented!("Not yet implemented");
                        },
                        Input::Completion => {
                            unimplemented!("Not yet implemented");
                        },
                        Input::StorageAck => {
                            unimplemented!("Not yet implemented");
                        }
                    }
                },
                _ = &mut shutdown => {
                    debug!("Shutting down the invoker");
                    break;
                }
            }
        }
    }
}
