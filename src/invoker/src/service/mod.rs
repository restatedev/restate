use super::*;

use crate::status_handle::StatusHandle;
use codederror::CodedError;
use futures::future::BoxFuture;
use futures::FutureExt;
use invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
use restate_common::errors::{InvocationError, InvocationErrorCode, UserErrorCode};
use restate_common::journal::{Completion, EntryEnricher};
use restate_common::retry_policy::RetryPolicy;
use restate_common::types::{EntryIndex, PartitionLeaderEpoch, ServiceInvocationId};
use restate_hyper_util::proxy_connector::{Proxy, ProxyConnector};
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

impl<I> Input<I> {
    fn new(partition_leader_epoch: PartitionLeaderEpoch, inner: I) -> Self {
        Self {
            partition: partition_leader_epoch,
            inner,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct InvokeCommand {
    service_invocation_id: ServiceInvocationId,
    #[serde(skip)]
    journal: InvokeInputJournal,
}

#[derive(Debug)]
pub(crate) enum Command {
    Invoke(InvokeCommand),
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
    input: mpsc::UnboundedSender<Input<Command>>,
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
            self.input
                .send(Input {
                    partition,
                    inner: Command::Invoke(InvokeCommand {
                        service_invocation_id,
                        journal,
                    }),
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
            self.input
                .send(Input {
                    partition,
                    inner: Command::Invoke(InvokeCommand {
                        service_invocation_id,
                        journal,
                    }),
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
            self.input
                .send(Input {
                    partition,
                    inner: Command::Completion {
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
            self.input
                .send(Input {
                    partition,
                    inner: Command::StoredEntryAck {
                        service_invocation_id,
                        entry_index,
                    },
                })
                .map_err(|_| ServiceNotRunning),
        )
    }

    fn abort_all_partition(&mut self, partition: PartitionLeaderEpoch) -> Self::Future {
        futures::future::ready(
            self.input
                .send(Input {
                    partition,
                    inner: Command::AbortAllPartition,
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
            self.input
                .send(Input {
                    partition,
                    inner: Command::Abort(service_invocation_id),
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
            self.input
                .send(Input {
                    partition,
                    inner: Command::RegisterPartition(sender),
                })
                .map_err(|_| ServiceNotRunning),
        )
    }
}

pub struct ChannelStatusReader(pub(crate) mpsc::UnboundedSender<Input<Command>>);

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
                inner: Command::ReadStatus(cmd),
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
    input_rx: mpsc::UnboundedReceiver<Input<Command>>,

    // Used for constructing the invoker sender
    input_tx: mpsc::UnboundedSender<Input<Command>>,

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
        let (input_tx, input_rx) = mpsc::unbounded_channel();

        let (invocation_tasks_tx, invocation_tasks_rx) = mpsc::unbounded_channel();

        Self {
            input_rx,
            input_tx,
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
            input: self.input_tx.clone(),
        }
    }

    pub fn status_reader(&self) -> ChannelStatusReader {
        ChannelStatusReader(self.input_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) {
        // Create the shared HTTP Client to use
        let client = self.get_client();

        let Service {
            mut input_rx,
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
                Some(input_message) = input_rx.recv() => {
                    match input_message {
                        // --- Spillable queue loading/offloading
                        Input { partition, inner: Command::Invoke(invoke_command) } => {
                            segmented_input_queue.enqueue(Input::new(
                                partition,
                                invoke_command,
                            )).await;
                        },
                        // --- Other commands (they don't go through the spillable queue)
                        Input { partition, inner: Command::RegisterPartition(sender) } => {
                            state_machine_coordinator.register_partition(partition, sender);
                        },
                        Input { partition, inner: Command::Abort(service_invocation_id) } => {
                            if let Some(psm) = state_machine_coordinator.resolve_partition(partition) {
                                psm.abort(service_invocation_id)
                            } else {
                                trace!(
                                    restate.invoker.partition_leader_epoch = ?partition,
                                    "Ignoring Abort command because there is no matching partition"
                                );
                            }
                        }
                        Input { partition, inner: Command::AbortAllPartition } => {
                            if let Some(mut partition_state_machine) = state_machine_coordinator.remove_partition(partition) {
                                partition_state_machine.abort_all();
                            } else {
                                trace!(
                                    restate.invoker.partition_leader_epoch = ?partition,
                                    "Ignoring AbortAll command because there is no matching partition"
                                );
                            }
                        }
                        Input { partition, inner: Command::Completion { service_invocation_id, completion }} => {
                            if let Some(psm) = state_machine_coordinator.resolve_partition(partition) {
                                psm.handle_completion(service_invocation_id, completion);
                            } else {
                                trace!(
                                    restate.invoker.partition_leader_epoch = ?partition,
                                    "Ignoring Completion command because there is no matching partition"
                                );
                            }
                        },
                        Input { partition, inner: Command::StoredEntryAck { service_invocation_id, entry_index, .. } } => {
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
                        Input { inner: Command::ReadStatus(cmd), .. } => {
                            let _ = cmd.reply(status_store.iter().collect());
                        }
                    }
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

/// This is a subset of Service to contain the state we need to carry around in the state machines.
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

#[cfg(test)]
mod tests {
    use crate::{journal_reader, state_reader, InvokeInputJournal, Service, ServiceHandle};
    use bytes::Bytes;
    use restate_common::retry_policy::RetryPolicy;
    use restate_common::types::{InvocationId, ServiceInvocationId};
    use restate_service_metadata::InMemoryServiceEndpointRegistry;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_test_util::{check, test};
    use std::time::Duration;
    use tokio::sync::mpsc;

    #[test(tokio::test)]
    async fn input_order_is_maintained() {
        let tempdir = tempfile::tempdir().unwrap();
        let service: Service<ProtobufRawEntryCodec, _, _, _, _> = Service::new(
            // all invocations are unknown leading to immediate retries
            InMemoryServiceEndpointRegistry::default(),
            // fixed amount of retries so that an invocation eventually completes with a failure
            RetryPolicy::fixed_delay(Duration::ZERO, 1),
            Duration::ZERO,
            Duration::ZERO,
            false,
            1024,
            None,
            None,
            tempdir.into_path(),
            journal_reader::mocks::EmptyJournalReader,
            state_reader::mocks::EmptyStateReader,
            restate_journal::mocks::MockEntryEnricher::default(),
        );

        let (signal, watch) = drain::channel();

        let mut handle = service.handle();

        let invoker_join_handle = tokio::spawn(service.run(watch));

        let partition_leader_epoch = (0, 0);
        let sid = ServiceInvocationId::new("TestService", Bytes::new(), InvocationId::now_v7());

        let (output_tx, mut output_rx) = mpsc::channel(1);

        handle
            .register_partition(partition_leader_epoch, output_tx)
            .await
            .unwrap();
        handle
            .invoke(
                partition_leader_epoch,
                sid,
                InvokeInputJournal::NoCachedJournal,
            )
            .await
            .unwrap();

        // If input order between 'register partition' and 'invoke' is not maintained, then it can happen
        // that 'invoke' arrives before 'register partition'. In this case, the invoker service will drop
        // the invocation and we won't see a result for the invocation (failure because the service endpoint
        // cannot be resolved).
        check!(let Some(_) = output_rx.recv().await);

        signal.drain().await;
        invoker_join_handle.await.unwrap();
    }
}
