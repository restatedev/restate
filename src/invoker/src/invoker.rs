use super::*;
use crate::invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
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
pub struct Invoker<Codec, JournalReader, ServiceEndpointRegistry> {
    invoke_input_rx: mpsc::UnboundedReceiver<Input<InvokeInputCommand>>,
    resume_input_rx: mpsc::UnboundedReceiver<Input<InvokeInputCommand>>,
    other_input_rx: mpsc::UnboundedReceiver<Input<OtherInputCommand>>,

    state_machine_coordinator: state_machine_coordinator::InvocationStateMachineCoordinator,

    // Used for constructing the invoker sender
    invoke_input_tx: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    resume_input_tx: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    other_input_tx: mpsc::UnboundedSender<Input<OtherInputCommand>>,

    // Service endpoints registry
    service_endpoint_registry: ServiceEndpointRegistry,

    // Channel to communicate with invocation tasks
    invocation_tasks_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invocation_tasks_rx: mpsc::UnboundedReceiver<InvocationTaskOutput>,

    // Set of stream coroutines
    invocation_tasks: JoinSet<()>,

    journal_reader: JournalReader,

    _codec: PhantomData<Codec>,
}

impl<C, JR, JS, SER> Invoker<C, JR, SER>
where
    JR: JournalReader<JournalStream = JS> + Clone + Send + 'static,
    JS: Stream<Item = RawEntry> + Unpin + Send + 'static,
    SER: ServiceEndpointRegistry,
{
    pub fn new(journal_reader: JR, service_endpoint_registry: SER) -> Self {
        let (invoke_input_tx, invoke_input_rx) = mpsc::unbounded_channel();
        let (resume_input_tx, resume_input_rx) = mpsc::unbounded_channel();
        let (other_input_tx, other_input_rx) = mpsc::unbounded_channel();

        let (invocation_tasks_tx, invocation_tasks_rx) = mpsc::unbounded_channel();

        Invoker {
            invoke_input_rx,
            resume_input_rx,
            other_input_rx,
            state_machine_coordinator: Default::default(),
            invoke_input_tx,
            resume_input_tx,
            other_input_tx,
            service_endpoint_registry,
            invocation_tasks_tx,
            invocation_tasks_rx,
            invocation_tasks: Default::default(),
            journal_reader,
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
            mut state_machine_coordinator,
            service_endpoint_registry,
            invocation_tasks_tx,
            mut invocation_tasks_rx,
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
                        .must_resolve_partition(invoke_input_message.partition)
                        .handle_invoke(
                            invoke_input_message.inner,
                            &journal_reader,
                            &service_endpoint_registry,
                            &mut invocation_tasks,
                            &invocation_tasks_tx
                        );
                },
                Some(other_input_message) = other_input_rx.recv() => {
                    match other_input_message {
                        Input { partition, inner: OtherInputCommand::RegisterPartition(sender) } => {
                            state_machine_coordinator.register_partition(partition, sender);
                        },
                        Input { partition, inner: OtherInputCommand::AbortAllPartition } => {
                            if let Some(partition_state_machine) = state_machine_coordinator.remove_partition(partition) {
                                partition_state_machine.handle_abort_all_partition();
                            } else {
                                // This is safe to ignore
                            }
                        }
                        Input { partition, inner: OtherInputCommand::Completion { service_invocation_id, journal_revision, completion } } => {
                            state_machine_coordinator
                                .must_resolve_partition(partition)
                                .handle_completion(service_invocation_id, journal_revision, completion);
                        },
                        Input { partition, inner: OtherInputCommand::StoredEntryAck { service_invocation_id, entry_index, .. } } => {
                            state_machine_coordinator
                                .must_resolve_partition(partition)
                                .handle_stored_entry_ack(service_invocation_id, entry_index);
                        }
                    }
                },
                Some(invocation_task_msg) = invocation_tasks_rx.recv() => {
                    let partition_state_machine =
                        if let Some(psm) = state_machine_coordinator.resolve_partition(invocation_task_msg.partition) {
                            psm
                        } else {
                            // We can skip it as it means the invocation was aborted
                            continue
                        };

                    match invocation_task_msg.inner {
                        InvocationTaskOutputInner::NewEntry {entry_index, raw_entry} => {
                            partition_state_machine.handle_new_entry(
                                invocation_task_msg.service_invocation_id,
                                entry_index,
                                raw_entry
                            )
                        },
                        InvocationTaskOutputInner::Result {last_journal_revision, last_journal_index, kind} => {
                            partition_state_machine.handle_invocation_task_result(
                                invocation_task_msg.service_invocation_id,
                                last_journal_index,
                                last_journal_revision,
                                kind
                            )
                        }
                    };
                },
                Some(invocation_task_result) = invocation_tasks.join_next() => {
                    match invocation_task_result {
                        Err(err) => {
                            // Propagate panics coming from invocation tasks.
                            if err.is_panic() {
                                panic::resume_unwind(err.into_panic());
                            }
                            // Other errors are cancellations caused by us (e.g. after AbortAllPartition),
                            // hence we can ignore them.
                        },
                        _ => {}
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
    use crate::invocation_task::{InvocationTask, InvocationTaskResultKind};
    use crate::invoker::invocation_state_machine::InvocationStateMachine;
    use tracing::warn;

    #[derive(Debug, thiserror::Error)]
    #[error("Cannot find service {0} in the service endpoint registry")]
    pub struct CannotResolveEndpoint(String);

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
        pub(super) fn must_resolve_partition(
            &mut self,
            partition: PartitionLeaderEpoch,
        ) -> &mut PartitionInvocationStateMachineCoordinator {
            self.partitions.get_mut(&partition).expect(
                "An event has been triggered for an unknown partition. \
                This is not supposed to happen, and it's probably a bug.",
            )
        }

        #[inline]
        pub(super) fn remove_partition(
            &mut self,
            partition: PartitionLeaderEpoch,
        ) -> Option<PartitionInvocationStateMachineCoordinator> {
            self.partitions.remove(&partition)
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
        output_tx: mpsc::Sender<OutputEffect>,
        invocation_state_machines: HashMap<ServiceInvocationId, InvocationStateMachine>,
    }

    impl PartitionInvocationStateMachineCoordinator {
        fn new(partition: PartitionLeaderEpoch, sender: mpsc::Sender<OutputEffect>) -> Self {
            Self {
                partition,
                output_tx: sender,
                invocation_state_machines: Default::default(),
            }
        }

        pub(super) fn handle_invoke<JR, JS, SER>(
            &mut self,
            invoke_input_cmd: InvokeInputCommand,
            journal_reader: &JR,
            service_endpoint_registry: &SER,
            invocation_tasks: &mut JoinSet<()>,
            invocation_tasks_tx: &mpsc::UnboundedSender<InvocationTaskOutput>,
        ) where
            JR: JournalReader<JournalStream = JS> + Clone + Send + 'static,
            JS: Stream<Item = RawEntry> + Unpin + Send + 'static,
            SER: ServiceEndpointRegistry,
        {
            let service_invocation_id = invoke_input_cmd.service_invocation_id;
            debug_assert!(!self
                .invocation_state_machines
                .contains_key(&service_invocation_id));

            // Resolve metadata
            let metadata = service_endpoint_registry
                .resolve_endpoint(&service_invocation_id.service_id.service_name);
            if metadata.is_none() {
                let error = Box::new(CannotResolveEndpoint(
                    service_invocation_id.service_id.service_name.to_string(),
                ));
                // No endpoint metadata can be resolved, we just fail it.
                let _ = self.output_tx.send(OutputEffect::Failed {
                    service_invocation_id,
                    error,
                });
                return;
            }

            // Start the InvocationTask
            let (completions_tx, completions_rx) = mpsc::unbounded_channel();
            let abort_handle = invocation_tasks.spawn(
                InvocationTask::new(
                    self.partition,
                    service_invocation_id.clone(),
                    0,
                    metadata.unwrap(),
                    journal_reader.clone(),
                    invocation_tasks_tx.clone(),
                    Some(completions_rx),
                )
                .run(),
            );

            // Register the state machine
            self.invocation_state_machines.insert(
                service_invocation_id,
                InvocationStateMachine::start(abort_handle, Some(completions_tx)),
            );
        }

        pub(super) fn handle_abort_all_partition(&self) {
            for sm in self.invocation_state_machines.values() {
                sm.abort()
            }
        }

        pub(super) fn handle_completion(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            journal_revision: JournalRevision,
            completion: Completion,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .get_mut(&service_invocation_id)
            {
                sm.notify_completion(journal_revision, completion);
            }
            // If no state machine is registered, the PP will send a new invoke
        }

        pub(super) fn handle_stored_entry_ack(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            entry_index: EntryIndex,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .get_mut(&service_invocation_id)
            {
                sm.notify_stored_ack(entry_index);
            }
            // If no state machine is registered, the PP will send a new invoke
        }

        pub(super) fn handle_new_entry(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            entry_index: EntryIndex,
            entry: RawEntry,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .get_mut(&service_invocation_id)
            {
                sm.notify_new_entry(&entry);
                let _ = self.output_tx.send(OutputEffect::JournalEntry {
                    service_invocation_id,
                    entry_index,
                    entry,
                });
            }
            // If no state machine, this might be an entry for an aborted invocation.
        }

        pub(super) fn handle_invocation_task_result(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            last_journal_index: EntryIndex,
            last_journal_revision: JournalRevision,
            kind: InvocationTaskResultKind,
        ) {
            if let Some(mut sm) = self
                .invocation_state_machines
                .remove(&service_invocation_id)
            {
                match kind {
                    InvocationTaskResultKind::Ok => {
                        let output_effect = if sm.ending() {
                            OutputEffect::End {
                                service_invocation_id,
                            }
                        } else {
                            OutputEffect::Suspended {
                                service_invocation_id,
                                journal_revision: last_journal_revision,
                            }
                        };

                        let _ = self.output_tx.send(output_effect);
                    }
                    error_kind => {
                        warn!(
                            restate.sid = %service_invocation_id,
                            "Error when executing the invocation: {}",
                            error_kind
                        );

                        if let Some(_next_retry_timer_duration) =
                            sm.handle_task_error(last_journal_index)
                        {
                            self.invocation_state_machines
                                .insert(service_invocation_id, sm);
                            unimplemented!("Implement timer")
                        } else {
                            let _ = self.output_tx.send(OutputEffect::Failed {
                                service_invocation_id,
                                error: Box::new(error_kind),
                            });
                        }
                    }
                }
            }
            // If no state machine, this might be a result for an aborted invocation.
        }
    }
}

mod invocation_state_machine {
    use super::*;
    use std::time::Duration;

    use journal::{Completion, EntryType};
    use tokio::sync::mpsc;
    use tokio::task::AbortHandle;

    /// Component encapsulating the business logic of the invocation state machine
    #[derive(Debug)]
    pub(super) struct InvocationStateMachine(TaskState, InvocationState);

    #[derive(Debug)]
    enum TaskState {
        Running(AbortHandle),
        NotRunning,
    }

    #[derive(Debug)]
    enum InvocationState {
        // If there is no completion channel, then the stream is open in request/response mode
        InFlight {
            // This can be none if the invocation task is request/response
            completions_tx: Option<mpsc::UnboundedSender<(JournalRevision, Completion)>>,
        },

        // We remain in this state until we get the task result.
        // We enter this state as soon as we see an OutpuStreamEntry.
        WaitingClose,

        WaitingRetry {
            // TODO implement timer
            index_waiting_on_ack: EntryIndex,
        },
    }

    impl InvocationStateMachine {
        pub(super) fn start(
            abort_handle: AbortHandle,
            completions_tx: Option<mpsc::UnboundedSender<(JournalRevision, Completion)>>,
        ) -> Self {
            Self(
                TaskState::Running(abort_handle),
                InvocationState::InFlight { completions_tx },
            )
        }

        pub(super) fn abort(&self) {
            match &self.0 {
                TaskState::Running(task_handle) => task_handle.abort(),
                _ => {}
            }
        }

        pub(super) fn notify_new_entry(&mut self, raw_entry: &RawEntry) {
            if raw_entry.entry_type() == EntryType::OutputStream {
                debug_assert!(matches!(&self.0, TaskState::Running(_)));

                self.1 = InvocationState::WaitingClose
            }
        }

        pub(super) fn notify_stored_ack(&mut self, entry_index: EntryIndex) {
            if let InvocationState::WaitingRetry {
                index_waiting_on_ack,
            } = &mut self.1
            {
                *index_waiting_on_ack = entry_index
            }
        }

        pub(super) fn notify_completion(
            &mut self,
            journal_revision: JournalRevision,
            completion: Completion,
        ) {
            if let InvocationState::InFlight {
                completions_tx: Some(sender),
            } = &mut self.1
            {
                let _ = sender.send((journal_revision, completion));
            }
        }

        /// Returns Some() with the timer for the next retry, otherwise None if retry limit exhausted
        pub(super) fn handle_task_error(&mut self, entry_index: EntryIndex) -> Option<Duration> {
            debug_assert!(matches!(&self.0, TaskState::Running(_)));

            self.0 = TaskState::NotRunning;
            self.1 = InvocationState::WaitingRetry {
                index_waiting_on_ack: entry_index,
            };
            // TODO implement retry policy
            return None;
        }

        pub(super) fn ending(&self) -> bool {
            matches!(self.1, InvocationState::WaitingClose)
        }
    }
}
