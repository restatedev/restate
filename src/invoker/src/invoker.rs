use super::*;
use common::types::PartitionLeaderEpoch;
use futures::stream;
use futures::stream::{PollNext, StreamExt};
use std::collections::HashMap;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct UnboundedInvokerInputSender {
    invoke_input: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    resume_input: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    other_input: mpsc::UnboundedSender<Input<OtherInputCommand>>,
}

impl InvokerInputSender for UnboundedInvokerInputSender {
    type Error = ();
    type Future = futures::future::Ready<Result<(), ()>>;

    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        self.invoke_input
            .send(Input {
                partition,
                inner: InvokeInputCommand {
                    service_invocation_id,
                    journal,
                },
            })
            .expect("Invoker should be running");
        futures::future::ok(())
    }

    fn resume(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        self.resume_input
            .send(Input {
                partition,
                inner: InvokeInputCommand {
                    service_invocation_id,
                    journal,
                },
            })
            .expect("Invoker should be running");
        futures::future::ok(())
    }

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal_revision: JournalRevision,
        completion: Completion,
    ) -> Self::Future {
        self.other_input
            .send(Input {
                partition,
                inner: OtherInputCommand::Completion {
                    service_invocation_id,
                    completion,
                    journal_revision,
                },
            })
            .expect("Invoker should be running");
        futures::future::ok(())
    }

    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal_revision: JournalRevision,
        entry_index: EntryIndex,
    ) -> Self::Future {
        self.other_input
            .send(Input {
                partition,
                inner: OtherInputCommand::StoredEntryAck {
                    service_invocation_id,
                    entry_index,
                    journal_revision,
                },
            })
            .expect("Invoker should be running");
        futures::future::ok(())
    }

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future {
        self.other_input
            .send(Input {
                partition,
                inner: OtherInputCommand::AbortAllPartition,
            })
            .expect("Invoker should be running");
        futures::future::ok(())
    }

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<OutputEffect>,
    ) -> Self::Future {
        self.other_input
            .send(Input {
                partition,
                inner: OtherInputCommand::RegisterPartition(sender),
            })
            .expect("Invoker should be running");
        futures::future::ok(())
    }
}

#[derive(Debug)]
struct Input<I> {
    partition: PartitionLeaderEpoch,
    inner: I,
}

#[allow(dead_code)]
#[derive(Debug)]
struct InvokeInputCommand {
    service_invocation_id: ServiceInvocationId,
    journal: InvokeInputJournal,
}

#[allow(dead_code)]
#[derive(Debug)]
enum OtherInputCommand {
    Completion {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
        journal_revision: JournalRevision,
    },
    StoredEntryAck {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        journal_revision: JournalRevision,
    },

    /// Command used to clean up internal state when a partition leader is going away
    AbortAllPartition,

    // needed for dynamic registration at Invoker
    RegisterPartition(mpsc::Sender<OutputEffect>),
}

#[derive(Debug)]
pub struct Invoker<Codec: ?Sized, JournalReader> {
    invoke_input_rx: mpsc::UnboundedReceiver<Input<InvokeInputCommand>>,
    resume_input_rx: mpsc::UnboundedReceiver<Input<InvokeInputCommand>>,
    other_input_rx: mpsc::UnboundedReceiver<Input<OtherInputCommand>>,
    partition_processors: HashMap<PartitionLeaderEpoch, mpsc::Sender<OutputEffect>>,

    _journal_reader: JournalReader,

    // used for constructing the invoker sender
    invoke_input_tx: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    resume_input_tx: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    other_input_tx: mpsc::UnboundedSender<Input<OtherInputCommand>>,

    _codec: PhantomData<Codec>,
}

impl<C: ?Sized, JR: JournalReader> Invoker<C, JR> {
    pub fn new(journal_reader: JR) -> Self {
        let (invoke_input_tx, invoke_input_rx) = mpsc::unbounded_channel();
        let (resume_input_tx, resume_input_rx) = mpsc::unbounded_channel();
        let (other_input_tx, other_input_rx) = mpsc::unbounded_channel();

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

    pub fn create_sender(&self) -> UnboundedInvokerInputSender {
        UnboundedInvokerInputSender {
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

        // Merge the two invoke and resume streams into a single stream
        let invoke_input_stream = stream::poll_fn(move |cx| invoke_input_rx.poll_recv(cx));
        let resume_input_stream = stream::poll_fn(move |cx| resume_input_rx.poll_recv(cx));
        let mut invoke_stream =
            stream::select_with_strategy(invoke_input_stream, resume_input_stream, |_: &mut ()| {
                PollNext::Right
            });

        loop {
            tokio::select! {
                invoke_input_message = invoke_stream.next() => {
                    let _invoke_input_message = invoke_input_message.expect("Input is never closed");
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
