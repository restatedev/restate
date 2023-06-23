use super::*;

use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Weak;
use std::time::Duration;
use std::{cmp, panic};

use futures::stream;
use futures::stream::{PollNext, StreamExt};
use restate_common::types::PartitionLeaderEpoch;
use restate_hyper_util::proxy_connector::{Proxy, ProxyConnector};
use restate_journal::EntryEnricher;
use restate_queue::SegmentQueue;
use restate_service_metadata::ServiceEndpointRegistry;
use restate_timer_queue::TimerQueue;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::{debug, trace};

use crate::status::InvocationStatusReportInner;
use invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
use state_machine_coordinator::StartInvocationTaskArguments;

mod invocation_state_machine;
mod invocation_task;
mod state_machine_coordinator;
mod status_store;

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
                .map_err(|_| InvokerNotRunning),
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
                .map_err(|_| InvokerNotRunning),
        )
    }
}

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
    ReadStatus(
        restate_futures_util::command::Command<
            (),
            Vec<(
                ServiceInvocationId,
                PartitionLeaderEpoch,
                Weak<InvocationStatusReportInner>,
            )>,
        >,
    ),
}

pub(crate) type HttpsClient = hyper::Client<
    ProxyConnector<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>,
    hyper::Body,
>;

#[derive(Debug)]
pub struct Invoker<Codec, JournalReader, StateReader, EntryEnricher, ServiceEndpointRegistry> {
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

impl<C, JR, SR, EE, SER> Invoker<C, JR, SR, EE, SER> {
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
    ) -> Invoker<C, JR, SR, EE, SER> {
        let (invoke_input_tx, invoke_input_rx) = mpsc::unbounded_channel();
        let (resume_input_tx, resume_input_rx) = mpsc::unbounded_channel();
        let (other_input_tx, other_input_rx) = mpsc::unbounded_channel();

        let (invocation_tasks_tx, invocation_tasks_rx) = mpsc::unbounded_channel();

        Self {
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

impl<C, JR, SR, EE, SER> Invoker<C, JR, SR, EE, SER>
where
    JR: JournalReader + Clone + Send + Sync + 'static,
    <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
    SR: StateReader + Clone + Send + Sync + 'static,
    <SR as StateReader>::StateIter: Send,
    EE: EntryEnricher + Clone + Send + 'static,
    SER: ServiceEndpointRegistry,
{
    pub fn create_sender(&self) -> UnboundedInvokerInputSender {
        UnboundedInvokerInputSender {
            invoke_input: self.invoke_input_tx.clone(),
            resume_input: self.resume_input_tx.clone(),
            other_input: self.other_input_tx.clone(),
        }
    }

    pub fn create_status_reader(&self) -> InvokerStatusReader {
        InvokerStatusReader(self.other_input_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) {
        // Create the shared HTTP Client to use
        let client = self.get_client();

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
                            invoke_input_command.inner,
                            StartInvocationTaskArguments::new(
                                &client,
                                &journal_reader,
                                &state_reader,
                                &entry_enricher,
                                &service_endpoint_registry,
                                &retry_policy,
                                suspension_timeout,
                                response_abort_timeout,
                                disable_eager_state,
                                message_size_warning,
                                message_size_limit,
                                &mut invocation_tasks,
                                &invocation_tasks_tx,
                                &mut retry_timers,
                            ),
                            &mut status_store
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
                                    service_invocation_id,
                                    StartInvocationTaskArguments::new(
                                        &client,
                                        &journal_reader,
                                        &state_reader,
                                        &entry_enricher,
                                        &service_endpoint_registry,
                                        &retry_policy,
                                        suspension_timeout,
                                        response_abort_timeout,
                                        disable_eager_state,
                                        message_size_warning,
                                        message_size_limit,
                                        &mut invocation_tasks,
                                        &invocation_tasks_tx,
                                        &mut retry_timers,
                                    ),
                                    entry_index,
                                    &mut status_store
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
                            let _ = cmd.reply(status_store.weak_iter().collect());
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
                                invocation_task_msg.service_invocation_id,
                                &mut status_store
                            ).await
                        },
                        InvocationTaskOutputInner::Failed(e) => {
                            partition_state_machine.handle_invocation_task_failed(
                                invocation_task_msg.service_invocation_id,
                                e,
                                &mut retry_timers,
                                &mut status_store
                            ).await
                        },
                        InvocationTaskOutputInner::Suspended(indexes) => {
                            partition_state_machine.handle_invocation_task_suspended(
                                invocation_task_msg.service_invocation_id,
                                indexes,
                                &mut status_store
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
                                &state_reader,
                                &entry_enricher,
                                &service_endpoint_registry,
                                &retry_policy,
                                suspension_timeout,
                                response_abort_timeout,
                                disable_eager_state,
                                message_size_warning,
                                message_size_limit,
                                &mut invocation_tasks,
                                &invocation_tasks_tx,
                                &mut retry_timers,
                            ),
                            &mut status_store
                        ).await;
                    } else {
                        trace!(
                            restate.invoker.partition_leader_epoch = ?partition,
                            "Ignoring timer fired command because there is no matching partition"
                        );
                    }
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
