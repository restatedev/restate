use super::*;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::{cmp, panic};

use common::types::PartitionLeaderEpoch;
use futures::stream;
use futures::stream::{PollNext, StreamExt};
use journal::raw::RawEntryCodec;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::debug;

use crate::invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};

#[derive(Debug, Clone)]
pub struct UnboundedInvokerInputSender {
    invoke_input: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    resume_input: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    other_input: mpsc::UnboundedSender<Input<OtherInputCommand>>,
}

#[derive(Debug, thiserror::Error)]
#[error("invoker is not running")]
pub struct InvokerNotRunning;

impl InvokerInputSender for UnboundedInvokerInputSender {
    type Error = InvokerNotRunning;
    type Future = futures::future::Ready<Result<(), Self::Error>>;

    fn invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        futures::future::ready(
            self.invoke_input
                .send(Input {
                    partition,
                    inner: InvokeInputCommand {
                        service_invocation_id,
                        journal,
                    },
                })
                .map_err(|_| InvokerNotRunning),
        )
    }

    fn resume(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) -> Self::Future {
        futures::future::ready(
            self.resume_input
                .send(Input {
                    partition,
                    inner: InvokeInputCommand {
                        service_invocation_id,
                        journal,
                    },
                })
                .map_err(|_| InvokerNotRunning),
        )
    }

    fn notify_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) -> Self::Future {
        futures::future::ready(
            self.other_input
                .send(Input {
                    partition,
                    inner: OtherInputCommand::Completion {
                        service_invocation_id,
                        completion,
                    },
                })
                .map_err(|_| InvokerNotRunning),
        )
    }

    fn notify_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) -> Self::Future {
        futures::future::ready(
            self.other_input
                .send(Input {
                    partition,
                    inner: OtherInputCommand::StoredEntryAck {
                        service_invocation_id,
                        entry_index,
                    },
                })
                .map_err(|_| InvokerNotRunning),
        )
    }

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future {
        futures::future::ready(
            self.other_input
                .send(Input {
                    partition,
                    inner: OtherInputCommand::AbortAllPartition,
                })
                .map_err(|_| InvokerNotRunning),
        )
    }

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<OutputEffect>,
    ) -> Self::Future {
        futures::future::ready(
            self.other_input
                .send(Input {
                    partition,
                    inner: OtherInputCommand::RegisterPartition(sender),
                })
                .map_err(|_| InvokerNotRunning),
        )
    }
}

#[derive(Debug)]
struct Input<I> {
    partition: PartitionLeaderEpoch,
    inner: I,
}

#[derive(Debug)]
enum InputCommand {
    Invoke(InvokeInputCommand),
    Other(OtherInputCommand),
}

#[derive(Debug)]
struct InvokeInputCommand {
    service_invocation_id: ServiceInvocationId,
    journal: InvokeInputJournal,
}

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
    C: RawEntryCodec,
    JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
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
            _codec: PhantomData::<C>::default(),
        }
    }
}

impl<C, JR, JS, SER> Invoker<C, JR, SER>
where
    JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
    JS: Stream<Item = RawEntry> + Unpin + Send + 'static,
    SER: ServiceEndpointRegistry,
{
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
        let invoke_stream =
            stream::select_with_strategy(invoke_input_stream, resume_input_stream, |_: &mut ()| {
                PollNext::Right
            })
            .map(|i| Input {
                partition: i.partition,
                inner: InputCommand::Invoke(i.inner),
            });

        // Merge the invoker and other streams
        let other_input_stream =
            stream::poll_fn(move |cx| other_input_rx.poll_recv(cx)).map(|i| Input {
                partition: i.partition,
                inner: InputCommand::Other(i.inner),
            });
        let mut input_stream =
            stream::select_with_strategy(invoke_stream, other_input_stream, |_: &mut ()| {
                PollNext::Right
            });

        loop {
            tokio::select! {
                Some(input_message) = input_stream.next() => {
                    match input_message {
                        Input { partition, inner: InputCommand::Invoke(invoke_input_command) } => {
                            state_machine_coordinator
                                .must_resolve_partition(partition)
                                .handle_invoke(
                                    invoke_input_command,
                                    &journal_reader,
                                    &service_endpoint_registry,
                                    &mut invocation_tasks,
                                    &invocation_tasks_tx
                                ).await;
                        },
                        Input { partition, inner: InputCommand::Other(OtherInputCommand::RegisterPartition(sender)) } => {
                            state_machine_coordinator.register_partition(partition, sender);
                        },
                        Input { partition, inner: InputCommand::Other(OtherInputCommand::AbortAllPartition) } => {
                            if let Some(mut partition_state_machine) = state_machine_coordinator.remove_partition(partition) {
                                partition_state_machine.abort();
                            } else {
                                // This is safe to ignore
                            }
                        }
                        Input { partition, inner: InputCommand::Other(OtherInputCommand::Completion { service_invocation_id, completion }) } => {
                            state_machine_coordinator
                                .must_resolve_partition(partition)
                                .handle_completion(service_invocation_id, completion);
                        },
                        Input { partition, inner: InputCommand::Other(OtherInputCommand::StoredEntryAck { service_invocation_id, entry_index, .. }) } => {
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
                            ).await
                        },
                        InvocationTaskOutputInner::Result {result} => {
                            partition_state_machine.handle_invocation_task_result(
                                invocation_task_msg.service_invocation_id,
                                result
                            ).await
                        }
                    };
                },
                Some(invocation_task_result) = invocation_tasks.join_next() => {
                    if let Err(err) = invocation_task_result {
                        // Propagate panics coming from invocation tasks.
                        if err.is_panic() {
                            panic::resume_unwind(err.into_panic());
                        }
                    }
                    // Other errors are cancellations caused by us (e.g. after AbortAllPartition),
                    // hence we can ignore them.
                }
                _ = &mut shutdown => {
                    debug!("Shutting down the invoker");
                    state_machine_coordinator.abort_all();
                    break;
                }
            }
        }

        // Wait for all the tasks to shutdown
        invocation_tasks.shutdown().await;
    }
}

mod state_machine_coordinator {
    use super::*;
    use crate::invocation_task::{InvocationTask, InvocationTaskError};
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
                This is not supposed to happen, and is probably a bug.",
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

        pub(super) fn abort_all(&mut self) {
            for partition in self.partitions.values_mut() {
                partition.abort();
            }
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

        pub(super) async fn handle_invoke<JR, JS, SER>(
            &mut self,
            invoke_input_cmd: InvokeInputCommand,
            journal_reader: &JR,
            service_endpoint_registry: &SER,
            invocation_tasks: &mut JoinSet<()>,
            invocation_tasks_tx: &mpsc::UnboundedSender<InvocationTaskOutput>,
        ) where
            JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
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
                let _ = self
                    .output_tx
                    .send(OutputEffect {
                        service_invocation_id,
                        kind: Kind::Failed {
                            error_code: tonic::Code::Internal.into(),
                            error,
                        },
                    })
                    .await;
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
                .run(invoke_input_cmd.journal),
            );

            // Register the state machine
            self.invocation_state_machines.insert(
                service_invocation_id,
                InvocationStateMachine::start(abort_handle, Some(completions_tx)),
            );
        }

        pub(super) fn abort(&mut self) {
            for sm in self.invocation_state_machines.values_mut() {
                sm.abort()
            }
        }

        pub(super) fn handle_completion(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            completion: Completion,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .get_mut(&service_invocation_id)
            {
                sm.notify_completion(completion);
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

        pub(super) async fn handle_new_entry(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            entry_index: EntryIndex,
            entry: RawEntry,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .get_mut(&service_invocation_id)
            {
                sm.notify_new_entry(entry_index, &entry);
                let _ = self
                    .output_tx
                    .send(OutputEffect {
                        service_invocation_id,
                        kind: Kind::JournalEntry { entry_index, entry },
                    })
                    .await;
            }
            // If no state machine, this might be an entry for an aborted invocation.
        }

        pub(super) async fn handle_invocation_task_result(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            result: Result<(), InvocationTaskError>,
        ) {
            if let Some(mut sm) = self
                .invocation_state_machines
                .remove(&service_invocation_id)
            {
                match result {
                    Ok(_) => {
                        let output_effect = if sm.is_ending() {
                            OutputEffect {
                                service_invocation_id,
                                kind: Kind::End,
                            }
                        } else {
                            OutputEffect {
                                service_invocation_id,
                                kind: Kind::Suspended {
                                    // TODO https://github.com/restatedev/restate/issues/97
                                    waiting_for_completed_entries: Default::default(),
                                },
                            }
                        };

                        let _ = self.output_tx.send(output_effect).await;
                    }
                    Err(error) => {
                        warn!(
                            restate.sid = %service_invocation_id,
                            "Error when executing the invocation: {}",
                            error
                        );

                        if let Some(_next_retry_timer_duration) = sm.handle_task_error() {
                            self.invocation_state_machines
                                .insert(service_invocation_id, sm);
                            unimplemented!("Implement timer")
                        } else {
                            let _ = self
                                .output_tx
                                .send(OutputEffect {
                                    service_invocation_id,
                                    kind: Kind::Failed {
                                        error_code: tonic::Code::Internal.into(),
                                        error: Box::new(error),
                                    },
                                })
                                .await;
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
    use std::mem;
    use std::time::Duration;

    use journal::raw::RawEntryHeader;
    use journal::Completion;
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

    /// This struct tracks which entries the invocation task generates,
    /// and which ones have been already stored and acked by the partition processor.
    /// This information is used to decide when it's safe to retry.
    ///
    /// Every time the invocation task generates a new entry, the index is notified to this struct with
    /// [`JournalTracker::notify_entry_sent_to_partition_processor`], and every time the invoker receives
    /// [`OtherInputCommand::StoredEntryAck`], the index is notified to this struct with [`JournalTracker::notify_acked_entry_from_partition_processor`].
    ///
    /// After the retry timer is fired, we can check whether we can retry immediately or not with [`JournalTracker::can_retry`].
    #[derive(Default, Debug, Copy, Clone)]
    struct JournalTracker {
        last_acked_entry_from_partition_processor: Option<EntryIndex>,
        last_entry_sent_to_partition_processor: Option<EntryIndex>,
    }

    impl JournalTracker {
        fn notify_acked_entry_from_partition_processor(&mut self, idx: EntryIndex) {
            self.last_entry_sent_to_partition_processor =
                cmp::max(Some(idx), self.last_acked_entry_from_partition_processor)
        }

        fn notify_entry_sent_to_partition_processor(&mut self, idx: EntryIndex) {
            self.last_entry_sent_to_partition_processor =
                cmp::max(Some(idx), self.last_entry_sent_to_partition_processor)
        }

        // TODO remove once we implement retries. https://github.com/restatedev/restate/issues/84
        #[allow(dead_code)]
        fn can_retry(&self) -> bool {
            match (
                self.last_acked_entry_from_partition_processor,
                self.last_entry_sent_to_partition_processor,
            ) {
                (_, None) => {
                    // The invocation task didn't generated new entries.
                    // We're always good to retry in this case.
                    true
                }
                (Some(last_acked), Some(last_sent)) => {
                    // Last acked must be higher than last sent,
                    // otherwise we'll end up retrying when not all the entries have been stored.
                    last_acked >= last_sent
                }
                _ => false,
            }
        }
    }

    #[derive(Debug)]
    enum InvocationState {
        // If there is no completion channel, then the stream is open in request/response mode
        InFlight {
            // This can be none if the invocation task is request/response
            completions_tx: Option<mpsc::UnboundedSender<Completion>>,
            journal_tracker: JournalTracker,
        },

        // We remain in this state until we get the task result.
        // We enter this state as soon as we see an OutpuStreamEntry.
        WaitingClose,

        WaitingRetry {
            // TODO implement timer
            //  https://github.com/restatedev/restate/issues/84
            journal_tracker: JournalTracker,
        },
    }

    impl InvocationStateMachine {
        pub(super) fn start(
            abort_handle: AbortHandle,
            completions_tx: Option<mpsc::UnboundedSender<Completion>>,
        ) -> Self {
            Self(
                TaskState::Running(abort_handle),
                InvocationState::InFlight {
                    completions_tx,
                    journal_tracker: Default::default(),
                },
            )
        }

        pub(super) fn abort(&mut self) {
            if let TaskState::Running(task_handle) =
                mem::replace(&mut self.0, TaskState::NotRunning)
            {
                task_handle.abort();
            }
        }

        pub(super) fn notify_new_entry(&mut self, entry_index: EntryIndex, raw_entry: &RawEntry) {
            debug_assert!(matches!(&self.0, TaskState::Running(_)));
            debug_assert!(matches!(&self.1, InvocationState::InFlight { .. }));

            if raw_entry.header == RawEntryHeader::OutputStream {
                self.1 = InvocationState::WaitingClose;
                return;
            }

            if let InvocationState::InFlight {
                journal_tracker, ..
            } = &mut self.1
            {
                journal_tracker.notify_entry_sent_to_partition_processor(entry_index);
            }
        }

        pub(super) fn notify_stored_ack(&mut self, entry_index: EntryIndex) {
            match &mut self.1 {
                InvocationState::InFlight {
                    journal_tracker, ..
                } => {
                    journal_tracker.notify_acked_entry_from_partition_processor(entry_index);
                }
                InvocationState::WaitingRetry { journal_tracker } => {
                    journal_tracker.notify_acked_entry_from_partition_processor(entry_index);
                    // TODO retry if retry timer fired already and journal_stored_guard.can_retry()
                }
                _ => {}
            }
        }

        pub(super) fn notify_completion(&mut self, completion: Completion) {
            if let InvocationState::InFlight {
                completions_tx: Some(sender),
                ..
            } = &mut self.1
            {
                let _ = sender.send(completion);
            }
        }

        /// Returns Some() with the timer for the next retry, otherwise None if retry limit exhausted
        pub(super) fn handle_task_error(&mut self) -> Option<Duration> {
            debug_assert!(matches!(&self.0, TaskState::Running(_)));
            debug_assert!(matches!(&self.1, InvocationState::InFlight { .. }));

            self.0 = TaskState::NotRunning;
            self.1 = match &self.1 {
                InvocationState::InFlight {
                    journal_tracker, ..
                } => InvocationState::WaitingRetry {
                    journal_tracker: *journal_tracker,
                },
                _ => unreachable!(),
            };
            // TODO implement retry policy
            //  https://github.com/restatedev/restate/issues/84
            // TODO define criteria for recoverable and unrecoverable errors
            None
        }

        pub(super) fn is_ending(&self) -> bool {
            matches!(self.1, InvocationState::WaitingClose)
        }
    }
}
