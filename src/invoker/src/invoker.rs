use super::*;
use common::types::PartitionLeaderEpoch;
use std::collections::HashMap;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tracing::debug;

pub struct InvokerSender {
    invoke_input: mpsc::Sender<Input<InvokeInputCommand>>,
    resume_input: mpsc::Sender<Input<InvokeInputCommand>>,
    other_input: mpsc::Sender<Input<OtherInputCommand>>,
}

impl InvokerSender {
    pub fn invoke_tx(&self) -> mpsc::Sender<Input<InvokeInputCommand>> {
        self.invoke_input.clone()
    }

    pub fn resume_tx(&self) -> mpsc::Sender<Input<InvokeInputCommand>> {
        self.resume_input.clone()
    }

    pub fn other_tx(&self) -> mpsc::Sender<Input<OtherInputCommand>> {
        self.other_input.clone()
    }
}

#[derive(Debug)]
pub struct Invoker<Codec: ?Sized, JournalReader> {
    invoke_input_rx: mpsc::Receiver<Input<InvokeInputCommand>>,
    resume_input_rx: mpsc::Receiver<Input<InvokeInputCommand>>,
    other_input_rx: mpsc::Receiver<Input<OtherInputCommand>>,
    partition_processors: HashMap<PartitionLeaderEpoch, mpsc::Sender<Output>>,

    _journal_reader: JournalReader,

    // used for constructing the invoker sender
    invoke_input_tx: mpsc::Sender<Input<InvokeInputCommand>>,
    resume_input_tx: mpsc::Sender<Input<InvokeInputCommand>>,
    other_input_tx: mpsc::Sender<Input<OtherInputCommand>>,

    _codec: PhantomData<Codec>,
}

impl<C: ?Sized, JR: JournalReader> Invoker<C, JR> {
    pub fn new(journal_reader: JR) -> Self {
        let (invoke_input_tx, invoke_input_rx) = mpsc::channel(32);
        let (resume_input_tx, resume_input_rx) = mpsc::channel(32);
        let (other_input_tx, other_input_rx) = mpsc::channel(32);

        Invoker {
            invoke_input_rx,
            resume_input_rx,
            other_input_rx,
            partition_processors: HashMap::new(),
            _journal_reader: journal_reader,
            invoke_input_tx,
            resume_input_tx,
            other_input_tx,
            _codec: Default::default(),
        }
    }

    pub fn create_sender(&self) -> InvokerSender {
        InvokerSender {
            invoke_input: self.invoke_input_tx.clone(),
            resume_input: self.resume_input_tx.clone(),
            other_input: self.other_input_tx.clone(),
        }
    }

    pub async fn run(self, drain: drain::Watch) {
        let Invoker {
            mut invoke_input_rx,
            mut resume_input_rx,
            mut other_input_rx,
            mut partition_processors,
            ..
        } = self;

        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                // TODO implement input stream to select biased between resume and invoke
                invoke_input_message = invoke_input_rx.recv() => {
                    let _invoke_input_message = invoke_input_message.expect("Input is never closed");
                    unimplemented!("Not yet implemented");
                },
                resume_input_message = resume_input_rx.recv() => {
                    let _resume_input_message = resume_input_message.expect("Input is never closed");
                    unimplemented!("Not yet implemented");
                },
                other_input_message = other_input_rx.recv() => {
                    let other_input_message = other_input_message.expect("Input is never closed");

                    let partition = other_input_message.partition;
                    match other_input_message.inner {
                        OtherInputCommand::RegisterPartition(sender) => {
                            partition_processors.insert(partition, sender);
                        }
                        OtherInputCommand::AbortAllPartition => {
                            partition_processors.remove(&partition);
                            // TODO teardown of streams
                        },
                        OtherInputCommand::AbortInvocation { .. } => {
                            unimplemented!("Not yet implemented");
                           // TODO teardown of streams
                        },
                        OtherInputCommand::Completion { .. } => {
                            unimplemented!("Not yet implemented");
                        },
                        OtherInputCommand::StoredEntryAck { .. } => {
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
