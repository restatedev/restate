use super::*;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;
use std::{cmp, panic};

use futures::stream;
use futures::stream::{PollNext, StreamExt};
use restate_common::types::PartitionLeaderEpoch;
use restate_journal::raw::{PlainRawEntry, RawEntryCodec};
use restate_service_metadata::ServiceEndpointRegistry;
use restate_timer_queue::TimerQueue;
use serde_with::serde_as;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{debug, trace};

use crate::invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
use crate::invoker::state_machine_coordinator::StartInvocationTaskArguments;

#[derive(Debug, Clone)]
pub struct UnboundedInvokerInputSender {
    invoke_input: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    resume_input: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    other_input: mpsc::UnboundedSender<Input<OtherInputCommand>>,
}

impl InvokerInputSender for UnboundedInvokerInputSender {
    type Future = futures::future::Ready<Result<(), InvokerNotRunning>>;

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

type HttpsClient =
    hyper::Client<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>, hyper::Body>;

/// # Invoker options
#[serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "InvokerOptions"))]
#[serde(rename_all = "camelCase")]
pub struct Options {
    /// # Retry policy
    ///
    /// Retry policy to use for all the invocations handled by this invoker.
    #[cfg_attr(feature = "options_schema", schemars(default))]
    retry_policy: RetryPolicy,

    /// # Suspension timeout
    ///
    /// This timer is used to gracefully shutdown a bidirectional stream
    /// after inactivity on both request and response streams.
    ///
    /// When this timer is fired, the invocation will be closed gracefully
    /// by closing the request stream, triggering a suspension on the sdk,
    /// in case the sdk is waiting on a completion. This won't affect the response stream.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Options::default_suspension_timeout")
    )]
    suspension_timeout: humantime::Duration,

    /// # Response abort timeout
    ///
    /// This timer is used to forcefully shutdown an invocation when only the response stream is open.
    ///
    /// When protocol mode is `restate_service_metadata::ProtocolType::RequestResponse`,
    /// this timer will start as soon as the replay of the journal is completed.
    /// When protocol mode is `restate_service_metadata::ProtocolType::BidiStream`,
    /// this timer will start after the request stream has been closed.
    /// Check `suspensionTimeout` to configure a timer on the request stream.
    ///
    /// When this timer is fired, the response stream will be aborted,
    /// potentially **interrupting** user code! If the user code needs longer to complete,
    /// then this value needs to be set accordingly.
    ///
    /// Can be configured using the [`humantime`](https://docs.rs/humantime/latest/humantime/fn.parse_duration.html) format.
    #[serde_as(as = "serde_with::DisplayFromStr")]
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Options::default_response_abort_timeout")
    )]
    response_abort_timeout: humantime::Duration,

    /// # Message size warning
    ///
    /// Threshold to log a warning in case protocol messages coming from service endpoint are larger than the specified amount.
    #[cfg_attr(
        feature = "options_schema",
        schemars(with = "String", default = "Options::default_message_size_warning")
    )]
    message_size_warning: usize,

    /// # Message size limit
    ///
    /// Threshold to fail the invocation in case protocol messages coming from service endpoint are larger than the specified amount.
    message_size_limit: Option<usize>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            retry_policy: Default::default(),
            suspension_timeout: Options::default_suspension_timeout(),
            response_abort_timeout: Options::default_response_abort_timeout(),
            message_size_warning: Options::default_message_size_warning(),
            message_size_limit: None,
        }
    }
}

impl Options {
    fn default_suspension_timeout() -> humantime::Duration {
        Duration::from_secs(60).into()
    }

    fn default_response_abort_timeout() -> humantime::Duration {
        (Duration::from_secs(60) * 60).into()
    }

    fn default_message_size_warning() -> usize {
        1024 * 1024 * 10 // 10mb
    }

    pub fn build<C, JR, JS, SER>(
        self,
        journal_reader: JR,
        service_endpoint_registry: SER,
    ) -> Invoker<C, JR, SER>
    where
        C: RawEntryCodec,
        JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
        JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
        SER: ServiceEndpointRegistry,
    {
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
            retry_timers: Default::default(),
            retry_policy: self.retry_policy,
            suspension_timeout: self.suspension_timeout.into(),
            response_abort_timeout: self.response_abort_timeout.into(),
            message_size_warning: self.message_size_warning,
            message_size_limit: self.message_size_limit,
            journal_reader,
            _codec: PhantomData::<C>::default(),
        }
    }
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

    // Retry timers
    retry_timers: TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,

    // Connection/protocol options
    retry_policy: RetryPolicy,
    suspension_timeout: Duration,
    response_abort_timeout: Duration,
    message_size_warning: usize,
    message_size_limit: Option<usize>,

    journal_reader: JournalReader,

    _codec: PhantomData<Codec>,
}

impl<C, JR, JS, SER> Invoker<C, JR, SER>
where
    JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
    JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
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
            mut retry_timers,
            journal_reader,
            retry_policy,
            suspension_timeout,
            response_abort_timeout,
            message_size_warning,
            message_size_limit,
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

        // Create the shared HTTP Client to use
        let client = Self::get_client();

        loop {
            tokio::select! {
                Some(input_message) = input_stream.next() => {
                    match input_message {
                        Input { partition, inner: InputCommand::Invoke(invoke_input_command) } => {
                            state_machine_coordinator
                                .must_resolve_partition(partition)
                                .handle_invoke(
                                    invoke_input_command,
                                    StartInvocationTaskArguments::new(
                                        &client,
                                        &journal_reader,
                                        &service_endpoint_registry,
                                        &retry_policy,
                                        suspension_timeout,
                                        response_abort_timeout,
                                        message_size_warning,
                                        message_size_limit,
                                        &mut invocation_tasks,
                                        &invocation_tasks_tx
                                    )
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
                                .handle_stored_entry_ack(
                                    service_invocation_id,
                                    StartInvocationTaskArguments::new(
                                        &client,
                                        &journal_reader,
                                        &service_endpoint_registry,
                                        &retry_policy,
                                        suspension_timeout,
                                        response_abort_timeout,
                                        message_size_warning,
                                        message_size_limit,
                                        &mut invocation_tasks,
                                        &invocation_tasks_tx
                                    ),
                                    entry_index
                                )
                                .await;
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
                        InvocationTaskOutputInner::NewEntry {entry_index, entry, parent_span_context} => {
                            partition_state_machine.handle_new_entry(
                                invocation_task_msg.service_invocation_id,
                                entry_index,
                                entry,
                                parent_span_context
                            ).await
                        },
                        InvocationTaskOutputInner::Closed => {
                            partition_state_machine.handle_invocation_task_closed(
                                invocation_task_msg.service_invocation_id
                            ).await
                        },
                        InvocationTaskOutputInner::Failed(e) => {
                            partition_state_machine.handle_invocation_task_failed(
                                invocation_task_msg.service_invocation_id,
                                e,
                                &mut retry_timers
                            ).await
                        },
                        InvocationTaskOutputInner::Suspended(indexes) => {
                            partition_state_machine.handle_invocation_task_suspended(
                                invocation_task_msg.service_invocation_id,
                                indexes
                            ).await
                        }
                    };
                },
                timer = retry_timers.await_timer() => {
                    let (partition, sid) = timer.into_inner();

                    if let Some(partition_state_machine) = state_machine_coordinator.resolve_partition(partition) {
                        partition_state_machine.handle_retry_timer_fired(
                            sid,
                            StartInvocationTaskArguments::new(
                                &client,
                                &journal_reader,
                                &service_endpoint_registry,
                                &retry_policy,
                                suspension_timeout,
                                response_abort_timeout,
                                message_size_warning,
                                message_size_limit,
                                &mut invocation_tasks,
                                &invocation_tasks_tx
                            )
                        ).await;
                    }
                    // We can skip it as it means the invocation was aborted
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

    // TODO a single client uses the pooling provided by hyper, but this is not enough.
    //  See https://github.com/restatedev/restate/issues/76 for more background on the topic.
    fn get_client() -> HttpsClient {
        hyper::Client::builder()
            .http2_only(true)
            .build::<_, hyper::Body>(
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http2()
                    .build(),
            )
    }
}

mod state_machine_coordinator {
    use super::invocation_state_machine::InvocationStateMachine;
    use super::*;
    use std::time::SystemTime;

    use crate::invocation_task::{InvocationTask, InvocationTaskError};

    use restate_errors::warn_it;
    use restate_journal::raw::Header;
    use restate_service_metadata::{ProtocolType, ServiceEndpointRegistry};
    use tonic::Code;
    use tracing::{instrument, warn};

    #[derive(Debug, thiserror::Error)]
    #[error("Cannot find service {0} in the service endpoint registry")]
    pub struct CannotResolveEndpoint(String);

    #[derive(Debug, thiserror::Error)]
    #[error("Unexpected end of invocation stream. This is probably a symptom of an SDK bug, please contact the developers.")]
    pub struct UnexpectedEndOfInvocationStream;

    /// This struct groups the arguments to start an InvocationTask.
    ///
    /// Because the methods receiving this struct might not start
    /// the InvocationTask (e.g. if the request cannot be retried now),
    /// we pass only borrows so we create clones only if we need them.
    pub(super) struct StartInvocationTaskArguments<'a, JR, SER> {
        client: &'a HttpsClient,
        journal_reader: &'a JR,
        service_endpoint_registry: &'a SER,
        default_retry_policy: &'a RetryPolicy,
        suspension_timeout: Duration,
        response_abort_timeout: Duration,
        message_size_warning: usize,
        message_size_limit: Option<usize>,
        invocation_tasks: &'a mut JoinSet<()>,
        invocation_tasks_tx: &'a mpsc::UnboundedSender<InvocationTaskOutput>,
    }

    impl<'a, JR, SER> StartInvocationTaskArguments<'a, JR, SER> {
        #[allow(clippy::too_many_arguments)]
        pub(super) fn new(
            client: &'a HttpsClient,
            journal_reader: &'a JR,
            service_endpoint_registry: &'a SER,
            default_retry_policy: &'a RetryPolicy,
            suspension_timeout: Duration,
            response_abort_timeout: Duration,
            message_size_warning: usize,
            message_size_limit: Option<usize>,
            invocation_tasks: &'a mut JoinSet<()>,
            invocation_tasks_tx: &'a mpsc::UnboundedSender<InvocationTaskOutput>,
        ) -> Self {
            Self {
                client,
                journal_reader,
                service_endpoint_registry,
                default_retry_policy,
                suspension_timeout,
                response_abort_timeout,
                message_size_warning,
                message_size_limit,
                invocation_tasks,
                invocation_tasks_tx,
            }
        }
    }

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

        // --- Event handlers

        #[instrument(
            level = "trace",
            skip_all,
            fields(
                rpc.service = %invoke_input_cmd.service_invocation_id.service_id.service_name,
                restate.invocation.key = ?invoke_input_cmd.service_invocation_id.service_id.key,
                restate.invocation.id = %invoke_input_cmd.service_invocation_id.invocation_id,
                restate.invoker.partition_leader_epoch = ?self.partition,
            )
        )]
        pub(super) async fn handle_invoke<JR, JS, SER>(
            &mut self,
            invoke_input_cmd: InvokeInputCommand,
            start_arguments: StartInvocationTaskArguments<'_, JR, SER>,
        ) where
            JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
            JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
            SER: ServiceEndpointRegistry,
        {
            let service_invocation_id = invoke_input_cmd.service_invocation_id;
            debug_assert!(!self
                .invocation_state_machines
                .contains_key(&service_invocation_id));

            self.start_invocation_task(
                service_invocation_id.clone(),
                invoke_input_cmd.journal,
                start_arguments,
                InvocationStateMachine::create,
            )
            .await
        }

        #[instrument(
            level = "trace",
            skip_all,
            fields(
                restate.invoker.partition_leader_epoch = ?self.partition,
            )
        )]
        pub(super) fn abort(&mut self) {
            for (service_invocation_id, sm) in self.invocation_state_machines.iter_mut() {
                trace!(
                    rpc.service = %service_invocation_id.service_id.service_name,
                    restate.invocation.key = ?service_invocation_id.service_id.key,
                    restate.invocation.id = %service_invocation_id.invocation_id,
                    "Aborting invocation"
                );
                sm.abort()
            }
        }

        #[instrument(
            level = "trace",
            skip_all,
            fields(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.invoker.partition_leader_epoch = ?self.partition,
            )
        )]
        pub(super) fn handle_completion(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            completion: Completion,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .get_mut(&service_invocation_id)
            {
                trace!(
                    restate.journal.index = completion.entry_index,
                    "Notifying completion"
                );
                sm.notify_completion(completion);
            } else {
                // If no state machine is registered, the PP will send a new invoke
                trace!("No state machine found for given completion");
            }
        }

        #[instrument(
            level = "trace",
            skip_all,
            fields(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.invoker.partition_leader_epoch = ?self.partition,
            )
        )]
        pub(super) async fn handle_retry_timer_fired<JR, JS, SER>(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            start_arguments: StartInvocationTaskArguments<'_, JR, SER>,
        ) where
            JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
            JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
            SER: ServiceEndpointRegistry,
        {
            trace!("Retry timeout fired");
            self.handle_retry_event(service_invocation_id, start_arguments, |sm| {
                sm.notify_retry_timer_fired()
            })
            .await;
        }

        #[instrument(
            level = "trace",
            skip_all,
            fields(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.invoker.partition_leader_epoch = ?self.partition,
                restate.journal.index = entry_index,
            )
        )]
        pub(super) async fn handle_stored_entry_ack<JR, JS, SER>(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            start_arguments: StartInvocationTaskArguments<'_, JR, SER>,
            entry_index: EntryIndex,
        ) where
            JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
            JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
            SER: ServiceEndpointRegistry,
        {
            trace!("Received a new stored journal entry acknowledgement");
            self.handle_retry_event(service_invocation_id, start_arguments, |sm| {
                sm.notify_stored_ack(entry_index)
            })
            .await;
        }

        #[instrument(
            level = "trace",
            skip_all,
            fields(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.invoker.partition_leader_epoch = ?self.partition,
                restate.journal.index = entry_index,
                restate.journal.entry_type = ?entry.header.to_entry_type(),
            )
        )]
        pub(super) async fn handle_new_entry(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            entry_index: EntryIndex,
            entry: PlainRawEntry,
            parent_span_context: Arc<SpanContext>,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .get_mut(&service_invocation_id)
            {
                sm.notify_new_entry(entry_index, &entry);
                trace!(
                    "Received a new entry. Invocation state: {:?}",
                    sm.invocation_state_debug()
                );
                let _ = self
                    .output_tx
                    .send(OutputEffect {
                        service_invocation_id,
                        kind: Kind::JournalEntry {
                            entry_index,
                            entry,
                            parent_span_context,
                        },
                    })
                    .await;
            } else {
                // If no state machine, this might be an entry for an aborted invocation.
                trace!("No state machine found for given entry");
            }
        }

        #[instrument(
            level = "warn",
            skip_all,
            fields(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.invoker.partition_leader_epoch = ?self.partition,
            )
        )]
        pub(super) async fn handle_invocation_task_closed(
            &mut self,
            service_invocation_id: ServiceInvocationId,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .remove(&service_invocation_id)
            {
                if sm.is_ending() {
                    trace!("Invocation task closed correctly");
                    self.send_end(service_invocation_id).await;
                } else {
                    // Protocol violation.
                    // We haven't received any output stream entry, but the invocation task was closed
                    // Because handle_invocation_task_closed is the terminal message coming from the invocation task,
                    // we need to return a message to the partition processor.
                    warn!(
                        "Protocol violation when executing the invocation. \
                        The invocation task was closed without a SuspensionMessage, nor an OutputStreamEntry"
                    );
                    self.send_error(
                        service_invocation_id,
                        Code::Internal,
                        UnexpectedEndOfInvocationStream,
                    )
                    .await;
                }
            } else {
                // If no state machine, this might be a result for an aborted invocation.
                trace!("No state machine found for invocation task closed signal");
            }
        }

        #[instrument(
            level = "warn",
            skip_all,
            fields(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.invoker.partition_leader_epoch = ?self.partition,
            )
        )]
        pub(super) async fn handle_invocation_task_failed(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            error: InvocationTaskError,
            retry_timers: &mut TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
        ) {
            if let Some(mut sm) = self
                .invocation_state_machines
                .remove(&service_invocation_id)
            {
                warn_it!(error, "Error when executing the invocation",);

                if sm.is_ending() {
                    // Soft protocol violation.
                    // This can happen in case we get an error after receiving an OutputStreamEntry.
                    // Because handle_invocation_task_failed is the terminal message coming from the invocation task,
                    // we need to return a message to the partition processor.
                    warn!(
                        "Protocol violation when executing the invocation. \
                        The invocation task sent an OutputStreamEntry and an error afterwards"
                    );
                    self.send_end(service_invocation_id).await;
                    return;
                }

                match sm.handle_task_error() {
                    Some(next_retry_timer_duration) if error.is_transient() => {
                        trace!(
                            "Starting the retry timer {}. Invocation state: {:?}",
                            humantime::format_duration(next_retry_timer_duration),
                            sm.invocation_state_debug()
                        );
                        self.invocation_state_machines
                            .insert(service_invocation_id.clone(), sm);
                        retry_timers.sleep_until(
                            SystemTime::now() + next_retry_timer_duration,
                            (self.partition, service_invocation_id),
                        );
                    }
                    _ => {
                        trace!("Not going to retry the error");
                        self.send_error(service_invocation_id, Code::Internal, error)
                            .await;
                    }
                }
            } else {
                // If no state machine, this might be a result for an aborted invocation.
                trace!("No state machine found for invocation task error signal");
            }
        }

        #[instrument(
            level = "trace",
            skip_all,
            fields(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.invoker.partition_leader_epoch = ?self.partition,
            )
        )]
        pub(super) async fn handle_invocation_task_suspended(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            entry_indexes: HashSet<EntryIndex>,
        ) {
            if let Some(sm) = self
                .invocation_state_machines
                .remove(&service_invocation_id)
            {
                if sm.is_ending() {
                    // Soft protocol violation.
                    // We got both an output stream entry and the suspension message
                    // Because handle_invocation_task_suspended is the terminal message coming from the invocation task,
                    // we need to return a message to the partition processor.
                    warn!(
                        rpc.service = %service_invocation_id.service_id.service_name,
                        restate.invocation.key = ?service_invocation_id.service_id.key,
                        restate.invocation.id = %service_invocation_id.invocation_id,
                        "Protocol violation when executing the invocation. \
                        The invocation task sent an OutputStreamEntry and was closed with a SuspensionMessage"
                    );
                    self.send_end(service_invocation_id).await;
                    return;
                }
                trace!("Suspending invocation");
                let _ = self
                    .output_tx
                    .send(OutputEffect {
                        service_invocation_id,
                        kind: Kind::Suspended {
                            waiting_for_completed_entries: entry_indexes,
                        },
                    })
                    .await;
            } else {
                // If no state machine, this might be a result for an aborted invocation.
                trace!("No state machine found for invocation task suspended signal");
            }
        }

        // --- Helpers

        async fn start_invocation_task<JR, JS, SER>(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            journal: InvokeInputJournal,
            start_arguments: StartInvocationTaskArguments<'_, JR, SER>,
            state_machine_factory: impl FnOnce(RetryPolicy) -> InvocationStateMachine,
        ) where
            JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
            JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
            SER: ServiceEndpointRegistry,
        {
            // Resolve metadata
            let metadata = match start_arguments
                .service_endpoint_registry
                .resolve_endpoint(&service_invocation_id.service_id.service_name)
            {
                Some(m) => m,
                None => {
                    // No endpoint metadata can be resolved, we just fail it.
                    let err = CannotResolveEndpoint(
                        service_invocation_id.service_id.service_name.to_string(),
                    );
                    self.send_error(service_invocation_id, Code::Internal, err)
                        .await;
                    return;
                }
            };

            let retry_policy = metadata
                .retry_policy()
                .unwrap_or(start_arguments.default_retry_policy)
                .clone();

            let mut invocation_state_machine = state_machine_factory(retry_policy);

            // Start the InvocationTask
            let (completions_tx, completions_rx) = match metadata.protocol_type() {
                ProtocolType::RequestResponse => (None, None),
                ProtocolType::BidiStream => {
                    let (tx, rx) = mpsc::unbounded_channel();
                    (Some(tx), Some(rx))
                }
            };
            let abort_handle = start_arguments.invocation_tasks.spawn(
                InvocationTask::new(
                    start_arguments.client.clone(),
                    self.partition,
                    service_invocation_id.clone(),
                    0,
                    metadata,
                    start_arguments.suspension_timeout,
                    start_arguments.response_abort_timeout,
                    start_arguments.message_size_warning,
                    start_arguments.message_size_limit,
                    start_arguments.journal_reader.clone(),
                    start_arguments.invocation_tasks_tx.clone(),
                    completions_rx,
                )
                .run(journal),
            );

            // Transition the state machine, and store it
            invocation_state_machine.start(abort_handle, completions_tx);
            trace!(
                "Invocation task started, state: {:?}",
                invocation_state_machine.invocation_state_debug()
            );
            self.invocation_state_machines
                .insert(service_invocation_id, invocation_state_machine);
        }

        async fn handle_retry_event<JR, JS, SER, FN>(
            &mut self,
            service_invocation_id: ServiceInvocationId,
            start_arguments: StartInvocationTaskArguments<'_, JR, SER>,
            f: FN,
        ) where
            JR: JournalReader<JournalStream = JS> + Clone + Send + Sync + 'static,
            JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
            SER: ServiceEndpointRegistry,
            FN: FnOnce(&mut InvocationStateMachine),
        {
            if let Some(mut sm) = self
                .invocation_state_machines
                .remove(&service_invocation_id)
            {
                f(&mut sm);
                if sm.is_ready_to_retry() {
                    trace!("Going to retry now");
                    self.start_invocation_task(
                        service_invocation_id,
                        InvokeInputJournal::NoCachedJournal,
                        start_arguments,
                        // In case we're retrying, we don't modify the retry policy
                        |_| sm,
                    )
                    .await;
                } else {
                    trace!(
                        "Not going to retry. Invocation state: {:?}",
                        sm.invocation_state_debug()
                    );
                    // Not ready for retrying yet
                    self.invocation_state_machines
                        .insert(service_invocation_id, sm);
                }
            } else {
                // If no state machine is registered, the PP will send a new invoke
                trace!("No state machine found for given retry event");
            }
        }

        async fn send_end(&self, service_invocation_id: ServiceInvocationId) {
            let _ = self
                .output_tx
                .send(OutputEffect {
                    service_invocation_id,
                    kind: Kind::End,
                })
                .await;
        }

        async fn send_error(
            &self,
            service_invocation_id: ServiceInvocationId,
            code: Code,
            err: impl std::error::Error + Send + Sync + 'static,
        ) {
            let _ = self
                .output_tx
                .send(OutputEffect {
                    service_invocation_id,
                    kind: Kind::Failed {
                        error_code: code.into(),
                        error: Box::new(err),
                    },
                })
                .await;
        }
    }
}

mod invocation_state_machine {
    use super::*;
    use std::time::Duration;
    use std::{fmt, mem};

    use restate_common::retry_policy;
    use restate_journal::raw::RawEntryHeader;
    use restate_journal::Completion;
    use tokio::sync::mpsc;
    use tokio::task::AbortHandle;

    /// Component encapsulating the business logic of the invocation state machine
    #[derive(Debug)]
    pub(super) struct InvocationStateMachine {
        invocation_state: InvocationState,
        retry_iter: retry_policy::Iter,
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
            self.last_acked_entry_from_partition_processor =
                cmp::max(Some(idx), self.last_acked_entry_from_partition_processor)
        }

        fn notify_entry_sent_to_partition_processor(&mut self, idx: EntryIndex) {
            self.last_entry_sent_to_partition_processor =
                cmp::max(Some(idx), self.last_entry_sent_to_partition_processor)
        }

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

    enum InvocationState {
        New,

        // If there is no completion channel, then the stream is open in request/response mode
        InFlight {
            // This can be none if the invocation task is request/response
            completions_tx: Option<mpsc::UnboundedSender<Completion>>,
            journal_tracker: JournalTracker,
            abort_handle: AbortHandle,
        },

        // We remain in this state until we get the task result.
        // We enter this state as soon as we see an OutpuStreamEntry.
        WaitingClose,

        WaitingRetry {
            timer_fired: bool,
            journal_tracker: JournalTracker,
        },
    }

    impl fmt::Debug for InvocationState {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                InvocationState::New => f.write_str("New"),
                InvocationState::InFlight {
                    journal_tracker,
                    abort_handle,
                    completions_tx,
                } => f
                    .debug_struct("InFlight")
                    .field("journal_tracker", journal_tracker)
                    .field("abort_handle", abort_handle)
                    .field(
                        "completions_tx_open",
                        &completions_tx
                            .as_ref()
                            .map(|s| !s.is_closed())
                            .unwrap_or(false),
                    )
                    .finish(),
                InvocationState::WaitingClose => f.write_str("WaitingClose"),
                InvocationState::WaitingRetry {
                    journal_tracker,
                    timer_fired,
                } => f
                    .debug_struct("WaitingRetry")
                    .field("journal_tracker", journal_tracker)
                    .field("timer_fired", timer_fired)
                    .finish(),
            }
        }
    }

    impl InvocationStateMachine {
        pub(super) fn create(retry_policy: RetryPolicy) -> InvocationStateMachine {
            Self {
                invocation_state: InvocationState::New,
                retry_iter: retry_policy.into_iter(),
            }
        }

        pub(super) fn start(
            &mut self,
            abort_handle: AbortHandle,
            completions_tx: Option<mpsc::UnboundedSender<Completion>>,
        ) {
            debug_assert!(matches!(
                &self.invocation_state,
                InvocationState::New | InvocationState::WaitingRetry { .. }
            ));

            self.invocation_state = InvocationState::InFlight {
                completions_tx,
                journal_tracker: Default::default(),
                abort_handle,
            };
        }

        pub(super) fn abort(&mut self) {
            if let InvocationState::InFlight { abort_handle, .. } =
                mem::replace(&mut self.invocation_state, InvocationState::WaitingClose)
            {
                abort_handle.abort();
            }
        }

        pub(super) fn notify_new_entry(&mut self, entry_index: EntryIndex, entry: &PlainRawEntry) {
            debug_assert!(matches!(
                &self.invocation_state,
                InvocationState::InFlight { .. }
            ));

            if entry.header == RawEntryHeader::OutputStream {
                self.invocation_state = InvocationState::WaitingClose;
                return;
            }

            if let InvocationState::InFlight {
                journal_tracker, ..
            } = &mut self.invocation_state
            {
                journal_tracker.notify_entry_sent_to_partition_processor(entry_index);
            }
        }

        pub(super) fn notify_stored_ack(&mut self, entry_index: EntryIndex) {
            match &mut self.invocation_state {
                InvocationState::InFlight {
                    journal_tracker, ..
                } => {
                    journal_tracker.notify_acked_entry_from_partition_processor(entry_index);
                }
                InvocationState::WaitingRetry {
                    journal_tracker, ..
                } => {
                    journal_tracker.notify_acked_entry_from_partition_processor(entry_index);
                }
                _ => {}
            }
        }

        pub(super) fn notify_completion(&mut self, completion: Completion) {
            if let InvocationState::InFlight {
                completions_tx: Some(sender),
                ..
            } = &mut self.invocation_state
            {
                let _ = sender.send(completion);
            }
        }

        pub(super) fn notify_retry_timer_fired(&mut self) {
            debug_assert!(matches!(
                &self.invocation_state,
                InvocationState::WaitingRetry { .. }
            ));

            if let InvocationState::WaitingRetry { timer_fired, .. } = &mut self.invocation_state {
                *timer_fired = true;
            }
        }

        /// Returns Some() with the timer for the next retry, otherwise None if retry limit exhausted
        pub(super) fn handle_task_error(&mut self) -> Option<Duration> {
            debug_assert!(matches!(
                &self.invocation_state,
                InvocationState::InFlight { .. }
            ));

            let (next_timer, journal_tracker) = match &self.invocation_state {
                InvocationState::InFlight {
                    journal_tracker, ..
                } => (self.retry_iter.next(), *journal_tracker),
                _ => unreachable!(),
            };

            if next_timer.is_some() {
                self.invocation_state = InvocationState::WaitingRetry {
                    timer_fired: false,
                    journal_tracker,
                };
                next_timer
            } else {
                None
            }
        }

        pub(super) fn is_ending(&self) -> bool {
            matches!(self.invocation_state, InvocationState::WaitingClose)
        }

        pub(super) fn is_ready_to_retry(&self) -> bool {
            match self.invocation_state {
                InvocationState::WaitingRetry {
                    timer_fired,
                    journal_tracker,
                } => timer_fired && journal_tracker.can_retry(),
                _ => false,
            }
        }

        #[inline]
        pub(super) fn invocation_state_debug(&self) -> impl fmt::Debug + '_ {
            &self.invocation_state
        }
    }
}
