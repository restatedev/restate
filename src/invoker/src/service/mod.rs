use super::*;

use crate::status_handle::StatusHandle;
use codederror::CodedError;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::{PollNext, StreamExt};
use futures::FutureExt;
use invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
use restate_common::errors::{InvocationError, InvocationErrorCode, UserErrorCode};
use restate_common::retry_policy::RetryPolicy;
use restate_common::types::{EntryIndex, PartitionLeaderEpoch, ServiceInvocationId};
use restate_hyper_util::proxy_connector::{Proxy, ProxyConnector};
use restate_journal::{Completion, EntryEnricher};
use restate_queue::SegmentQueue;
use restate_service_metadata::ServiceEndpointRegistry;
use restate_timer_queue::TimerQueue;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::time::Duration;
use std::{cmp, panic};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{debug, trace};

mod invocation_state_machine;
mod invocation_task;
mod state_machine_coordinator;
mod status_store;

// -- Handles implementations

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct Input<I> {
    pub(crate) partition: PartitionLeaderEpoch,
    pub(crate) inner: I,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct InvokeInputCommand {
    service_invocation_id: ServiceInvocationId,
    #[serde(skip)]
    journal: InvokeInputJournal,
}

#[derive(Debug)]
pub(crate) enum OtherInputCommand {
    Completion {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    },
    StoredEntryAck {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    },

    /// Abort specific invocation id
    Abort(ServiceInvocationId),

    /// Command used to clean up internal state when a partition leader is going away
    AbortAllPartition,

    // needed for dynamic registration at Invoker
    RegisterPartition(mpsc::Sender<Effect>),

    // Read status
    ReadStatus(restate_futures_util::command::Command<(), Vec<InvocationStatusReport>>),
}

#[derive(Debug, Clone)]
pub struct ChannelServiceHandle {
    invoke_input: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    resume_input: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    other_input: mpsc::UnboundedSender<Input<OtherInputCommand>>,
}

impl ServiceHandle for ChannelServiceHandle {
    type Future = futures::future::Ready<Result<(), ServiceNotRunning>>;

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
                .map_err(|_| ServiceNotRunning),
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
                .map_err(|_| ServiceNotRunning),
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
                .map_err(|_| ServiceNotRunning),
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
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future {
        futures::future::ready(
            self.other_input
                .send(Input {
                    partition,
                    inner: OtherInputCommand::AbortAllPartition,
                })
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn abort_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) -> Self::Future {
        futures::future::ready(
            self.other_input
                .send(Input {
                    partition,
                    inner: OtherInputCommand::Abort(service_invocation_id),
                })
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<Effect>,
    ) -> Self::Future {
        futures::future::ready(
            self.other_input
                .send(Input {
                    partition,
                    inner: OtherInputCommand::RegisterPartition(sender),
                })
                .map_err(|_| ServiceNotRunning),
        )
    }
}

pub struct ChannelStatusReader(pub(crate) mpsc::UnboundedSender<Input<OtherInputCommand>>);

impl StatusHandle for ChannelStatusReader {
    type Iterator = itertools::Either<
        std::iter::Empty<InvocationStatusReport>,
        std::vec::IntoIter<InvocationStatusReport>,
    >;
    type Future = BoxFuture<'static, Self::Iterator>;

    fn read_status(&self) -> Self::Future {
        let (cmd, rx) = restate_futures_util::command::Command::prepare(());
        if self
            .0
            .send(Input {
                partition: (0, 0),
                inner: OtherInputCommand::ReadStatus(cmd),
            })
            .is_err()
        {
            return std::future::ready(itertools::Either::Left(std::iter::empty::<
                InvocationStatusReport,
            >()))
            .boxed();
        }
        async move {
            if let Ok(status_vec) = rx.await {
                itertools::Either::Right(status_vec.into_iter())
            } else {
                itertools::Either::Left(std::iter::empty::<InvocationStatusReport>())
            }
        }
        .boxed()
    }
}

pub(crate) type HttpsClient = hyper::Client<
    ProxyConnector<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>,
    hyper::Body,
>;

// -- Service implementation

#[derive(Debug)]
pub struct Service<Codec, JournalReader, StateReader, EntryEnricher, ServiceEndpointRegistry> {
    invoke_input_rx: mpsc::UnboundedReceiver<Input<InvokeInputCommand>>,
    resume_input_rx: mpsc::UnboundedReceiver<Input<InvokeInputCommand>>,
    other_input_rx: mpsc::UnboundedReceiver<Input<OtherInputCommand>>,

    // Used for constructing the invoker sender
    invoke_input_tx: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    resume_input_tx: mpsc::UnboundedSender<Input<InvokeInputCommand>>,
    other_input_tx: mpsc::UnboundedSender<Input<OtherInputCommand>>,

    // Service endpoints registry
    service_endpoint_registry: ServiceEndpointRegistry,

    // Channel to communicate with invocation tasks
    invocation_tasks_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invocation_tasks_rx: mpsc::UnboundedReceiver<InvocationTaskOutput>,

    // Connection/protocol options
    retry_policy: RetryPolicy,
    suspension_timeout: Duration,
    response_abort_timeout: Duration,
    disable_eager_state: bool,
    message_size_warning: usize,
    message_size_limit: Option<usize>,
    proxy: Option<Proxy>,

    // Invoker options
    tmp_dir: PathBuf,

    journal_reader: JournalReader,
    state_reader: StateReader,
    entry_enricher: EntryEnricher,

    _codec: PhantomData<Codec>,
}

impl<C, JR, SR, EE, SER> Service<C, JR, SR, EE, SER> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        service_endpoint_registry: SER,
        retry_policy: RetryPolicy,
        suspension_timeout: Duration,
        response_abort_timeout: Duration,
        disable_eager_state: bool,
        message_size_warning: usize,
        message_size_limit: Option<usize>,
        proxy: Option<Proxy>,
        tmp_dir: PathBuf,
        journal_reader: JR,
        state_reader: SR,
        entry_enricher: EE,
    ) -> Service<C, JR, SR, EE, SER> {
        let (invoke_input_tx, invoke_input_rx) = mpsc::unbounded_channel();
        let (resume_input_tx, resume_input_rx) = mpsc::unbounded_channel();
        let (other_input_tx, other_input_rx) = mpsc::unbounded_channel();

        let (invocation_tasks_tx, invocation_tasks_rx) = mpsc::unbounded_channel();

        Self {
            invoke_input_rx,
            resume_input_rx,
            other_input_rx,
            invoke_input_tx,
            resume_input_tx,
            other_input_tx,
            service_endpoint_registry,
            invocation_tasks_tx,
            invocation_tasks_rx,
            retry_policy,
            suspension_timeout,
            response_abort_timeout,
            disable_eager_state,
            message_size_warning,
            message_size_limit,
            proxy,
            tmp_dir,
            journal_reader,
            state_reader,
            entry_enricher,
            _codec: PhantomData::<C>::default(),
        }
    }
}

impl<C, JR, SR, EE, SER> Service<C, JR, SR, EE, SER>
where
    JR: JournalReader + Clone + Send + Sync + 'static,
    <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
    SR: StateReader + Clone + Send + Sync + 'static,
    <SR as StateReader>::StateIter: Send,
    EE: EntryEnricher + Clone + Send + 'static,
    SER: ServiceEndpointRegistry,
{
    pub fn handle(&self) -> ChannelServiceHandle {
        ChannelServiceHandle {
            invoke_input: self.invoke_input_tx.clone(),
            resume_input: self.resume_input_tx.clone(),
            other_input: self.other_input_tx.clone(),
        }
    }

    pub fn status_reader(&self) -> ChannelStatusReader {
        ChannelStatusReader(self.other_input_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) {
        // Create the shared HTTP Client to use
        let client = self.get_client();

        let Service {
            mut invoke_input_rx,
            mut resume_input_rx,
            mut other_input_rx,
            service_endpoint_registry,
            invocation_tasks_tx,
            mut invocation_tasks_rx,
            journal_reader,
            state_reader,
            entry_enricher,
            retry_policy,
            suspension_timeout,
            response_abort_timeout,
            disable_eager_state,
            message_size_warning,
            message_size_limit,
            tmp_dir,
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

        // Prepare the segmented queue
        let mut segmented_input_queue = SegmentQueue::init(tmp_dir, 1_056_784)
            .await
            .expect("Cannot initialize input spillable queue");

        let mut service_state = ServiceState {
            client,
            journal_reader,
            state_reader,
            entry_enricher,
            service_endpoint_registry,
            suspension_timeout,
            response_abort_timeout,
            disable_eager_state,
            message_size_warning,
            message_size_limit,
            invocation_tasks: Default::default(),
            invocation_tasks_tx,
            default_retry_policy: retry_policy,
            retry_timers: Default::default(),
        };
        let mut state_machine_coordinator =
            state_machine_coordinator::InvocationStateMachineCoordinator::default();
        let mut status_store = status_store::InvocationStatusStore::default();

        loop {
            tokio::select! {
                // --- Spillable queue loading/offloading
                Some(invoke_input_command) = invoke_stream.next() => {
                    segmented_input_queue.enqueue(invoke_input_command).await
                },
                Some(invoke_input_command) = segmented_input_queue.dequeue(), if !segmented_input_queue.is_empty() => {
                    if let Some(psm) = state_machine_coordinator.resolve_partition(invoke_input_command.partition) {
                        psm.handle_invoke(
                            &mut service_state,
                            &mut status_store,
                            invoke_input_command.inner,
                        ).await;
                    } else {
                        trace!(
                            restate.invoker.partition_leader_epoch = ?invoke_input_command.partition,
                            "Ignoring Invoke command because there is no matching partition"
                        );
                    }
                },

                // --- Other commands (they don't go through the spillable queue)
                Some(input_message) = other_input_rx.recv() => {
                    match input_message {
                        Input { partition, inner: OtherInputCommand::RegisterPartition(sender) } => {
                            state_machine_coordinator.register_partition(partition, sender);
                        },
                        Input { partition, inner: OtherInputCommand::Abort(service_invocation_id) } => {
                            if let Some(psm) = state_machine_coordinator.resolve_partition(partition) {
                                psm.abort(service_invocation_id)
                            } else {
                                trace!(
                                    restate.invoker.partition_leader_epoch = ?partition,
                                    "Ignoring Abort command because there is no matching partition"
                                );
                            }
                        }
                        Input { partition, inner: OtherInputCommand::AbortAllPartition } => {
                            if let Some(mut partition_state_machine) = state_machine_coordinator.remove_partition(partition) {
                                partition_state_machine.abort_all();
                            } else {
                                trace!(
                                    restate.invoker.partition_leader_epoch = ?partition,
                                    "Ignoring AbortAll command because there is no matching partition"
                                );
                            }
                        }
                        Input { partition, inner: OtherInputCommand::Completion { service_invocation_id, completion }} => {
                            if let Some(psm) = state_machine_coordinator.resolve_partition(partition) {
                                psm.handle_completion(service_invocation_id, completion);
                            } else {
                                trace!(
                                    restate.invoker.partition_leader_epoch = ?partition,
                                    "Ignoring Completion command because there is no matching partition"
                                );
                            }
                        },
                        Input { partition, inner: OtherInputCommand::StoredEntryAck { service_invocation_id, entry_index, .. } } => {
                            if let Some(psm) = state_machine_coordinator.resolve_partition(partition) {
                                psm.handle_stored_entry_ack(
                                    &mut service_state,
                                    &mut status_store,
                                    service_invocation_id,
                                    entry_index,
                                )
                                .await;
                            } else {
                                trace!(
                                    restate.invoker.partition_leader_epoch = ?partition,
                                    "Ignoring StoredEntryAck command because there is no matching partition"
                                );
                            }
                        },
                        Input { inner: OtherInputCommand::ReadStatus(cmd), .. } => {
                            let _ = cmd.reply(status_store.iter().collect());
                        }
                    }
                },
                Some(invocation_task_msg) = invocation_tasks_rx.recv() => {
                    let partition_state_machine =
                        if let Some(psm) = state_machine_coordinator.resolve_partition(invocation_task_msg.partition) {
                            psm
                        } else {
                            trace!(
                                restate.invoker.partition_leader_epoch = ?invocation_task_msg.partition,
                                "Ignoring InvocationTaskOutput command because there is no matching partition"
                            );
                            // We can skip it as it means the invocation was aborted
                            continue
                        };

                    match invocation_task_msg.inner {
                        InvocationTaskOutputInner::NewEntry {entry_index, entry} => {
                            partition_state_machine.handle_new_entry(
                                invocation_task_msg.service_invocation_id,
                                entry_index,
                                entry,
                            ).await
                        },
                        InvocationTaskOutputInner::Closed => {
                            partition_state_machine.handle_invocation_task_closed(
                                &mut status_store,
                                invocation_task_msg.service_invocation_id,
                            ).await
                        },
                        InvocationTaskOutputInner::Failed(e) => {
                            partition_state_machine.handle_invocation_task_failed(
                                &mut service_state.retry_timers,
                                &mut status_store,
                                invocation_task_msg.service_invocation_id,
                                e,
                            ).await
                        },
                        InvocationTaskOutputInner::Suspended(indexes) => {
                            partition_state_machine.handle_invocation_task_suspended(
                                &mut status_store,
                                invocation_task_msg.service_invocation_id,
                                indexes
                            ).await
                        }
                    };
                },
                timer = service_state.retry_timers.await_timer() => {
                    let (partition, sid) = timer.into_inner();

                    if let Some(partition_state_machine) = state_machine_coordinator.resolve_partition(partition) {
                        partition_state_machine.handle_retry_timer_fired(
                            &mut service_state,
                            &mut status_store,
                            sid
                        ).await;
                    } else {
                        trace!(
                            restate.invoker.partition_leader_epoch = ?partition,
                            "Ignoring timer fired command because there is no matching partition"
                        );
                    }
                },
                Some(invocation_task_result) = service_state.invocation_tasks.join_next() => {
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
        service_state.invocation_tasks.shutdown().await;
    }

    // TODO a single client uses the pooling provided by hyper, but this is not enough.
    //  See https://github.com/restatedev/restate/issues/76 for more background on the topic.
    fn get_client(&self) -> HttpsClient {
        hyper::Client::builder()
            .http2_only(true)
            .build::<_, hyper::Body>(ProxyConnector::new(
                self.proxy.clone(),
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http2()
                    .build(),
            ))
    }
}

/// This is a subset of Service {} to contain the state we need to carry around in the state machines.
struct ServiceState<JR, SR, EE, SER> {
    client: HttpsClient,
    journal_reader: JR,
    state_reader: SR,
    entry_enricher: EE,
    service_endpoint_registry: SER,
    default_retry_policy: RetryPolicy,
    suspension_timeout: Duration,
    response_abort_timeout: Duration,
    disable_eager_state: bool,
    message_size_warning: usize,
    message_size_limit: Option<usize>,
    invocation_tasks: JoinSet<()>,
    invocation_tasks_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    retry_timers: TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
}

/// Internal error trait for the invoker errors
trait InvokerError: std::error::Error {
    fn is_transient(&self) -> bool;
    fn to_invocation_error(&self) -> InvocationError;

    fn as_invocation_error_code(&self) -> InvocationErrorCode {
        UserErrorCode::Internal.into()
    }
}

impl<InvokerCodedError: InvokerError + CodedError> From<&InvokerCodedError>
    for InvocationErrorReport
{
    fn from(value: &InvokerCodedError) -> Self {
        InvocationErrorReport {
            err: value.to_invocation_error(),
            doc_error_code: value.code(),
        }
    }
}
