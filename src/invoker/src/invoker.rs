use super::*;
use crate::invocation_task::{InvocationEntry, InvocationTaskResult};
use common::types::PartitionLeaderEpoch;
use common::types::{EntryIndex, PartitionLeaderEpoch};
use futures::stream;
use futures::stream::{PollNext, StreamExt};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::panic;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
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
        completion: Completion,
    ) -> Self::Future {
        self.other_input
            .send(Input {
                partition,
                inner: OtherInputCommand::Completion {
                    service_invocation_id,
                    completion,
                },
            })
            .expect("Invoker should be running");
        futures::future::ok(())
    }

    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) -> Self::Future {
        self.other_input
            .send(Input {
                partition,
                inner: OtherInputCommand::StoredEntryAck {
                    service_invocation_id,
                    entry_index,
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
    },
    StoredEntryAck {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    },

    /// Command used to clean up internal state when a partition leader is going away
    AbortAllPartition,

    // needed for dynamic registration at Invoker
    RegisterPartition(mpsc::Sender<OutputEffect>),
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Invoker<Codec, JournalReader> {
    invoke_input_rx: mpsc::UnboundedReceiver<Input<InvokeInputCommand>>,
    resume_input_rx: mpsc::UnboundedReceiver<Input<InvokeInputCommand>>,
    other_input_rx: mpsc::UnboundedReceiver<Input<OtherInputCommand>>,

    state_machine_coordinator: state_machine_coordinator::InvocationStateMachineCoordinator,

    // used for constructing the invoker sender
    invoke_input_tx: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    resume_input_tx: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    other_input_tx: mpsc::UnboundedSender<Input<OtherInputCommand>>,

    // Service endpoints registry
    endpoints: HashMap<String, (ProtocolType, Uri)>,

    // Channels to communicate between internal channels
    streams_entry_tx: mpsc::UnboundedSender<InvocationEntry>,
    streams_entry_rx: mpsc::UnboundedReceiver<InvocationEntry>,

    // Set of stream coroutines
    invocation_tasks: JoinSet<InvocationTaskResult>,

    journal_reader: JournalReader,

    _codec: PhantomData<Codec>,
}

impl<C, JR: JournalReader> Invoker<C, JR> {
    pub fn new(journal_reader: JR) -> Self {
        let (invoke_input_tx, invoke_input_rx) = mpsc::unbounded_channel();
        let (resume_input_tx, resume_input_rx) = mpsc::unbounded_channel();
        let (other_input_tx, other_input_rx) = mpsc::unbounded_channel();

        let (streams_entry_tx, streams_entry_rx) = mpsc::unbounded_channel();

        Invoker {
            invoke_input_rx,
            resume_input_rx,
            other_input_rx,
            state_machine_coordinator: Default::default(),
            journal_reader,
            invoke_input_tx,
            resume_input_tx,
            other_input_tx,
            _codec: Default::default(),
            endpoints: Default::default(),
            streams_entry_tx,
            streams_entry_rx,
            invocation_tasks: Default::default(),
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
            mut state_machine_coordinator,
            endpoints,
            streams_entry_tx,
            mut streams_entry_rx,
            mut invocation_tasks,
            journal_reader,
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
                Some(invoke_input_message) = invoke_stream.next() => {
                    state_machine_coordinator
                        .resolve_partition(invoke_input_message.partition)
                        .expect("An Invoke was sent with an unregistered partition. \
                                This is not supposed to happen, and it's probably a bug.")
                        .handle_invoke(invoke_input_message.inner);
                },
                Some(other_input_message) = other_input_rx.recv() => {
                    match other_input_message {
                        Input { partition, inner: OtherInputCommand::RegisterPartition(sender) } => {
                            state_machine_coordinator.register_partition(partition, sender);
                        },
                        Input { partition, inner: OtherInputCommand::AbortAllPartition } => {
                            if let Some(partition_state_machine) = state_machine_coordinator.resolve_partition(partition) {
                                partition_state_machine.handle_abort_all_partition();
                            } else {
                                // This is safe to ignore
                            }
                        }
                        Input { partition, inner: OtherInputCommand::Completion { service_invocation_id, journal_revision, completion } } => {
                            state_machine_coordinator
                                .resolve_partition(partition)
                                .expect("A Completion was sent with an unregistered partition. \
                                        This is not supposed to happen, and it's probably a bug.")
                                .handle_completion(service_invocation_id, journal_revision, completion);
                        },
                        Input { partition, inner: OtherInputCommand::StoredEntryAck { service_invocation_id, journal_revision, entry_index } } => {
                            state_machine_coordinator
                                .resolve_partition(partition)
                                .expect("A StoredEntryAck was sent with an unregistered partition. \
                                        This is not supposed to happen, and it's probably a bug.")
                                .handle_stored_entry_ack(service_invocation_id, journal_revision, entry_index);
                        }
                    }
                },
                Some(new_entry) = streams_entry_rx.recv() => {
                    state_machine_coordinator
                        .resolve_partition(new_entry.partition)
                        .expect("A new entry was sent with an unregistered partition. \
                                This is not supposed to happen, and it's probably a bug.")
                        .handle_new_entry(new_entry);
                },
                Some(invocation_task_result) = invocation_tasks.join_next() => {
                    match invocation_task_result {
                        Ok(result) => {
                            state_machine_coordinator
                                .resolve_partition(result.partition)
                                .expect("An invocation task result was sent with an unregistered partition. \
                                        This is not supposed to happen, and it's probably a bug.")
                                .handle_invocation_task_result(result);
                        }
                        Err(err) => {
                            // Propagate panics coming from invocation tasks.
                            if err.is_panic() {
                                panic::resume_unwind(err.into_panic());
                            }
                        }
                    }
                }
                _ = &mut shutdown => {
                    debug!("Shutting down the invoker");
                    break;
                }
            }
        }
    }
}

mod state_machine_coordinator {
    use super::*;

    #[derive(Debug, Default)]
    pub(super) struct InvocationStateMachineCoordinator {
        partitions: HashMap<PartitionLeaderEpoch, PartitionInvocationStateMachineCoordinator>,
    }

    impl InvocationStateMachineCoordinator {
        #[inline]
        pub(super) fn resolve_partition(
            &mut self,
            partition: PartitionLeaderEpoch,
        ) -> Option<&mut PartitionInvocationStateMachineCoordinator> {
            self.partitions.get_mut(&partition)
        }

        #[inline]
        pub(super) fn register_partition(
            &mut self,
            partition: PartitionLeaderEpoch,
            sender: mpsc::Sender<OutputEffect>,
        ) {
            self.partitions.insert(
                partition,
                PartitionInvocationStateMachineCoordinator::new(partition, sender),
            );
        }
    }

    #[derive(Debug)]
    pub(super) struct PartitionInvocationStateMachineCoordinator {
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<OutputEffect>,
        invocation_state_machines:
            HashMap<ServiceInvocationId, invocation_state_machine::InvocationStateMachine>,
    }

    impl PartitionInvocationStateMachineCoordinator {
        fn new(partition: PartitionLeaderEpoch, sender: mpsc::Sender<OutputEffect>) -> Self {
            Self {
                partition,
                sender,
                invocation_state_machines: Default::default(),
            }
        }

        pub(super) fn handle_invoke(&mut self, invoke_input_cmd: InvokeInputCommand) {
            unimplemented!()
        }

        pub(super) fn handle_abort_all_partition(&mut self) {
            unimplemented!()
        }

        pub(super) fn handle_completion(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            journal_revision: JournalRevision,
            completion: Completion,
        ) {
            unimplemented!()
        }

        pub(super) fn handle_stored_entry_ack(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            journal_revision: JournalRevision,
            entry_index: EntryIndex,
        ) {
            unimplemented!()
        }

        pub(super) fn handle_new_entry(&mut self, new_entry: InvocationEntry) {
            unimplemented!()
        }

        pub(super) fn handle_invocation_task_result(
            &mut self,
            invocation_task_result: InvocationTaskResult,
        ) {
            unimplemented!()
        }
    }
}

mod invocation_state_machine {
    use super::*;

    use hyper::Uri;

    use journal::Completion;
    use tokio::sync::mpsc;
    use tokio::task::AbortHandle;

    /// Component encapsulating the business logic of the invocation state machine
    #[derive(Debug)]
    pub(super) struct InvocationStateMachine {
        // Last index seen from the Service Endpoint
        last_seen_index: EntryIndex,
        // Last revision received from the PP
        last_journal_revision: JournalRevision,

        state: InnerState,
    }

    #[derive(Debug)]
    enum InnerState {
        // If there is no completion channel, then the stream is open in request/response mode
        InFlight {
            completions_tx: Option<mpsc::UnboundedSender<Completion>>,
            task_handle: AbortHandle,
        },

        // We remain in this state until the JoinHandle of the tokio's task is completed.
        WaitingClose,
    }
}
