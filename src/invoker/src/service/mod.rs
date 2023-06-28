use super::*;

use crate::service::status_store::InvocationStatusStore;
use codederror::CodedError;
use drain::ReleaseShutdown;
use invocation_state_machine::InvocationStateMachine;
use invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
use restate_common::errors::{InvocationError, InvocationErrorCode, UserErrorCode};
use restate_common::journal::Completion;
use restate_common::retry_policy::RetryPolicy;
use restate_common::types::{
    EnrichedRawEntry, EntryIndex, PartitionLeaderEpoch, ServiceInvocationId,
};
use restate_errors::warn_it;
use restate_hyper_util::proxy_connector::{Proxy, ProxyConnector};
use restate_queue::SegmentQueue;
use restate_service_metadata::{ProtocolType, ServiceEndpointRegistry};
use restate_timer_queue::TimerQueue;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use std::{cmp, panic};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tracing::instrument;
use tracing::{debug, trace};

mod input_command;
mod invocation_state_machine;
mod invocation_task;
mod quota;
mod state_machine_tree;
mod status_store;

pub use input_command::ChannelServiceHandle;
pub use input_command::ChannelStatusReader;
use input_command::{InputCommand, InvokeCommand};

// -- Errors

#[derive(Debug, Clone, thiserror::Error, codederror::CodedError)]
#[error("Cannot find service {0} in the service endpoint registry")]
#[code(unknown)]
pub struct CannotResolveEndpoint(String);

impl InvokerError for CannotResolveEndpoint {
    fn is_transient(&self) -> bool {
        true
    }

    fn to_invocation_error(&self) -> InvocationError {
        InvocationError::new(UserErrorCode::Internal, self.to_string())
    }
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

type HttpsClient = hyper::Client<
    ProxyConnector<hyper_rustls::HttpsConnector<hyper::client::HttpConnector>>,
    hyper::Body,
>;

// -- Service implementation

#[derive(Debug)]
pub struct Service<Codec, JournalReader, StateReader, EntryEnricher, ServiceEndpointRegistry> {
    input_rx: mpsc::UnboundedReceiver<InputCommand>,

    // Used for constructing the invoker sender
    input_tx: mpsc::UnboundedSender<InputCommand>,

    // Service endpoints registry
    service_endpoint_registry: ServiceEndpointRegistry,

    // Channel to communicate with invocation tasks
    invocation_tasks_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invocation_tasks_rx: mpsc::UnboundedReceiver<InvocationTaskOutput>,

    // Connection/protocol options
    client: HttpsClient,
    retry_policy: RetryPolicy,
    suspension_timeout: Duration,
    response_abort_timeout: Duration,
    disable_eager_state: bool,
    message_size_warning: usize,
    message_size_limit: Option<usize>,

    // Invoker options
    tmp_dir: PathBuf,

    journal_reader: JournalReader,
    state_reader: StateReader,
    entry_enricher: EntryEnricher,

    // Invoker state machine
    invocation_tasks: JoinSet<()>,
    retry_timers: TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
    quota: quota::InvokerConcurrencyQuota,
    status_store: InvocationStatusStore,
    invocation_state_machines_tree: state_machine_tree::InvocationStateMachineTree,

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
        concurrency_limit: Option<usize>,
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
            client: Self::create_client(proxy),
            retry_policy,
            suspension_timeout,
            response_abort_timeout,
            disable_eager_state,
            message_size_warning,
            message_size_limit,
            tmp_dir,
            journal_reader,
            state_reader,
            entry_enricher,
            invocation_tasks: Default::default(),
            retry_timers: Default::default(),
            quota: quota::InvokerConcurrencyQuota::new(concurrency_limit),
            status_store: Default::default(),
            invocation_state_machines_tree: Default::default(),
            _codec: PhantomData::<C>::default(),
        }
    }

    // TODO a single client uses the pooling provided by hyper, but this is not enough.
    //  See https://github.com/restatedev/restate/issues/76 for more background on the topic.
    fn create_client(proxy: Option<Proxy>) -> HttpsClient {
        hyper::Client::builder()
            .http2_only(true)
            .build::<_, hyper::Body>(ProxyConnector::new(
                proxy,
                hyper_rustls::HttpsConnectorBuilder::new()
                    .with_native_roots()
                    .https_or_http()
                    .enable_http2()
                    .build(),
            ))
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

    pub async fn run(mut self, drain: drain::Watch) {
        let shutdown = drain.signaled();
        tokio::pin!(shutdown);

        // Prepare the segmented queue
        let mut segmented_input_queue = SegmentQueue::init(self.tmp_dir.clone(), 1_056_784)
            .await
            .expect("Cannot initialize input spillable queue");

        loop {
            if !self
                .step(&mut segmented_input_queue, shutdown.as_mut())
                .await
            {
                break;
            }
        }

        // Wait for all the tasks to shutdown
        self.invocation_tasks.shutdown().await;
    }

    // Returns true if we should execute another step, false if we should stop executing steps
    async fn step<F>(
        &mut self,
        segmented_input_queue: &mut SegmentQueue<InvokeCommand>,
        mut shutdown: Pin<&mut F>,
    ) -> bool
    where
        F: Future<Output = ReleaseShutdown>,
    {
        tokio::select! {
            Some(input_message) = self.input_rx.recv() => {
                match input_message {
                    // --- Spillable queue loading/offloading
                    InputCommand::Invoke(invoke_command) => {
                        segmented_input_queue.enqueue(invoke_command).await;
                    },
                    // --- Other commands (they don't go through the segment queue)
                    InputCommand::RegisterPartition { partition, sender } => {
                        self.invocation_state_machines_tree.register_partition(partition, sender);
                    },
                    InputCommand::Abort { partition, service_invocation_id } => {
                        self.handle_abort_invocation(partition, service_invocation_id);
                    }
                    InputCommand::AbortAllPartition { partition } => {
                        self.handle_abort_partition(partition);
                    }
                    InputCommand::Completion { partition, service_invocation_id, completion } => {
                        self.handle_completion(partition, service_invocation_id, completion);
                    },
                    InputCommand::StoredEntryAck { partition, service_invocation_id, entry_index } => {
                        self.handle_stored_entry_ack(partition, service_invocation_id, entry_index).await;
                    },
                    InputCommand::ReadStatus(cmd) => {
                        let _ = cmd.reply(self.status_store.iter().collect());
                    }
                }
            },

            Some(invoke_input_command) = segmented_input_queue.dequeue(), if !segmented_input_queue.is_empty() && self.quota.is_slot_available() => {
                self.handle_invoke(invoke_input_command.partition, invoke_input_command.service_invocation_id, invoke_input_command.journal).await;
            },

            Some(invocation_task_msg) = self.invocation_tasks_rx.recv() => {
                let InvocationTaskOutput {
                    service_invocation_id,
                    partition,
                    inner
                } = invocation_task_msg;
                match inner {
                    InvocationTaskOutputInner::NewEntry {entry_index, entry} => {
                        self.handle_new_entry(
                            partition,
                            service_invocation_id,
                            entry_index,
                            entry,
                        ).await
                    },
                    InvocationTaskOutputInner::Closed => {
                        self.handle_invocation_task_closed(partition, service_invocation_id).await
                    },
                    InvocationTaskOutputInner::Failed(e) => {
                        self.handle_invocation_task_failed(partition, service_invocation_id, e).await
                    },
                    InvocationTaskOutputInner::Suspended(indexes) => {
                        self.handle_invocation_task_suspended(partition, service_invocation_id, indexes).await
                    }
                };
            },
            timer = self.retry_timers.await_timer() => {
                let (partition, sid) = timer.into_inner();
                self.handle_retry_timer_fired(partition, sid).await;
            },
            Some(invocation_task_result) = self.invocation_tasks.join_next() => {
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
                self.handle_shutdown();
                return false;
            }
        }
        // Execute next loop
        true
    }

    // --- Event handlers

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            rpc.service = %service_invocation_id.service_id.service_name,
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invoke(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
    ) {
        debug_assert!(self.invocation_state_machines_tree.has_partition(partition));
        debug_assert!(self
            .invocation_state_machines_tree
            .resolve_invocation(partition, &service_invocation_id)
            .is_none());

        self.quota.reserve_slot();
        self.start_invocation_task(
            partition,
            service_invocation_id,
            journal,
            InvocationStateMachine::create,
        )
        .await
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            rpc.service = %service_invocation_id.service_id.service_name,
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_retry_timer_fired(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) {
        trace!("Retry timeout fired");
        self.handle_retry_event(partition, service_invocation_id, |sm| {
            sm.notify_retry_timer_fired()
        })
        .await;
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            rpc.service = %service_invocation_id.service_id.service_name,
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.index = entry_index,
        )
    )]
    async fn handle_stored_entry_ack(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) {
        trace!("Received a new stored journal entry acknowledgement");
        self.handle_retry_event(partition, service_invocation_id, |sm| {
            sm.notify_stored_ack(entry_index)
        })
        .await;
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            rpc.service = %service_invocation_id.service_id.service_name,
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.index = entry_index,
            restate.journal.entry_type = ?entry.header.to_entry_type(),
        )
    )]
    async fn handle_new_entry(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
    ) {
        if let Some((output_tx, ism)) = self
            .invocation_state_machines_tree
            .resolve_invocation(partition, &service_invocation_id)
        {
            ism.notify_new_entry(entry_index);
            trace!(
                "Received a new entry. Invocation state: {:?}",
                ism.invocation_state_debug()
            );
            let _ = output_tx
                .send(Effect {
                    service_invocation_id,
                    kind: EffectKind::JournalEntry { entry_index, entry },
                })
                .await;
        } else {
            // If no state machine, this might be an entry for an aborted invocation.
            trace!("No state machine found for given entry");
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            rpc.service = %service_invocation_id.service_id.service_name,
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) {
        if let Some((_, ism)) = self
            .invocation_state_machines_tree
            .resolve_invocation(partition, &service_invocation_id)
        {
            trace!(
                restate.journal.index = completion.entry_index,
                "Notifying completion"
            );
            ism.notify_completion(completion);
        } else {
            // If no state machine is registered, the PP will send a new invoke
            trace!("No state machine found for given completion");
        }
    }

    #[instrument(
        level = "warn",
        skip_all,
        fields(
            rpc.service = %service_invocation_id.service_id.service_name,
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_closed(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) {
        if let Some((sender, _)) = self
            .invocation_state_machines_tree
            .remove_invocation(partition, &service_invocation_id)
        {
            trace!("Invocation task closed correctly");
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &service_invocation_id);
            let _ = sender
                .send(Effect {
                    service_invocation_id,
                    kind: EffectKind::End,
                })
                .await;
        } else {
            // If no state machine, this might be a result for an aborted invocation.
            trace!("No state machine found for invocation task closed signal");
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            rpc.service = %service_invocation_id.service_id.service_name,
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_suspended(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_indexes: HashSet<EntryIndex>,
    ) {
        if let Some((sender, _)) = self
            .invocation_state_machines_tree
            .remove_invocation(partition, &service_invocation_id)
        {
            trace!("Suspending invocation");
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &service_invocation_id);
            let _ = sender
                .send(Effect {
                    service_invocation_id,
                    kind: EffectKind::Suspended {
                        waiting_for_completed_entries: entry_indexes,
                    },
                })
                .await;
        } else {
            // If no state machine, this might be a result for an aborted invocation.
            trace!("No state machine found for invocation task suspended signal");
        }
    }

    #[instrument(
        level = "warn",
        skip_all,
        fields(
            rpc.service = %service_invocation_id.service_id.service_name,
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_failed(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        error: impl InvokerError + CodedError + Send + Sync + 'static,
    ) {
        if let Some((_, ism)) = self
            .invocation_state_machines_tree
            .remove_invocation(partition, &service_invocation_id)
        {
            self.handle_error_event(partition, service_invocation_id, error, ism)
                .await;
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
            restate.invocation.sid = %service_invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_abort_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
    ) {
        if let Some((_, mut ism)) = self
            .invocation_state_machines_tree
            .remove_invocation(partition, &service_invocation_id)
        {
            trace!(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.sid = %service_invocation_id,
                "Aborting invocation"
            );
            ism.abort();
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &service_invocation_id);
        } else {
            trace!(
                restate.invoker.partition_leader_epoch = ?partition,
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.sid = %service_invocation_id,
                "Ignoring Abort command because there is no matching partition/invocation"
            );
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_abort_partition(&mut self, partition: PartitionLeaderEpoch) {
        if let Some(invocation_state_machines) = self
            .invocation_state_machines_tree
            .remove_partition(partition)
        {
            for (sid, mut ism) in invocation_state_machines.into_iter() {
                trace!(
                    rpc.service = %sid.service_id.service_name,
                    restate.invocation.sid = %sid,
                    "Aborting invocation"
                );
                ism.abort();
                self.quota.unreserve_slot();
                self.status_store.on_end(&partition, &sid);
            }
        } else {
            trace!(
                restate.invoker.partition_leader_epoch = ?partition,
                "Ignoring AbortAll command because there is no matching partition"
            );
        }
    }

    #[instrument(level = "trace", skip_all)]
    fn handle_shutdown(&mut self) {
        let partitions = self.invocation_state_machines_tree.registered_partitions();
        for partition in partitions {
            self.handle_abort_partition(partition);
        }
    }

    // --- Helpers

    async fn handle_error_event<E: InvokerError + CodedError + Send + Sync + 'static>(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        error: E,
        mut ism: InvocationStateMachine,
    ) {
        warn_it!(error, "Error when executing the invocation");

        match ism.handle_task_error() {
            Some(next_retry_timer_duration) if error.is_transient() => {
                trace!(
                    "Starting the retry timer {}. Invocation state: {:?}",
                    humantime::format_duration(next_retry_timer_duration),
                    ism.invocation_state_debug()
                );
                self.status_store
                    .on_failure(partition, service_invocation_id.clone(), &error);
                self.invocation_state_machines_tree.register_invocation(
                    partition,
                    service_invocation_id.clone(),
                    ism,
                );
                self.retry_timers.sleep_until(
                    SystemTime::now() + next_retry_timer_duration,
                    (partition, service_invocation_id),
                );
            }
            _ => {
                trace!("Not going to retry the error");
                self.quota.unreserve_slot();
                self.status_store.on_end(&partition, &service_invocation_id);
                let _ = self
                    .invocation_state_machines_tree
                    .resolve_partition_sender(partition)
                    .expect("Partition should be registered")
                    .send(Effect {
                        service_invocation_id,
                        kind: EffectKind::Failed(error.to_invocation_error()),
                    })
                    .await;
            }
        }
    }

    async fn start_invocation_task(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
        state_machine_factory: impl FnOnce(RetryPolicy) -> InvocationStateMachine,
    ) {
        // Resolve metadata
        let endpoint_metadata = match self
            .service_endpoint_registry
            .resolve_endpoint(&service_invocation_id.service_id.service_name)
        {
            Some(m) => m,
            None => {
                // No endpoint metadata can be resolved, we just fail it.
                let err = CannotResolveEndpoint(
                    service_invocation_id.service_id.service_name.to_string(),
                );

                // This method needs a state machine
                self.handle_error_event(
                    partition,
                    service_invocation_id,
                    err,
                    state_machine_factory(self.retry_policy.clone()),
                )
                .await;
                return;
            }
        };

        let retry_policy = endpoint_metadata
            .retry_policy()
            .unwrap_or(&self.retry_policy)
            .clone();

        let mut ism = state_machine_factory(retry_policy);

        // Start the InvocationTask
        let (completions_tx, completions_rx) = match endpoint_metadata.protocol_type() {
            ProtocolType::RequestResponse => (None, None),
            ProtocolType::BidiStream => {
                let (tx, rx) = mpsc::unbounded_channel();
                (Some(tx), Some(rx))
            }
        };
        let abort_handle = self.invocation_tasks.spawn(
            invocation_task::InvocationTask::new(
                self.client.clone(),
                partition,
                service_invocation_id.clone(),
                0,
                endpoint_metadata,
                self.suspension_timeout,
                self.response_abort_timeout,
                self.disable_eager_state,
                self.message_size_warning,
                self.message_size_limit,
                self.journal_reader.clone(),
                self.state_reader.clone(),
                self.entry_enricher.clone(),
                self.invocation_tasks_tx.clone(),
                completions_rx,
            )
            .run(journal),
        );

        // Transition the state machine, and store it
        self.status_store
            .on_start(partition, service_invocation_id.clone());
        ism.start(abort_handle, completions_tx);
        trace!(
            "Invocation task started state. Invocation state: {:?}",
            ism.invocation_state_debug()
        );
        self.invocation_state_machines_tree.register_invocation(
            partition,
            service_invocation_id,
            ism,
        );
    }

    async fn handle_retry_event<FN>(
        &mut self,
        partition: PartitionLeaderEpoch,
        service_invocation_id: ServiceInvocationId,
        f: FN,
    ) where
        FN: FnOnce(&mut InvocationStateMachine),
    {
        if let Some((_, mut ism)) = self
            .invocation_state_machines_tree
            .remove_invocation(partition, &service_invocation_id)
        {
            f(&mut ism);
            if ism.is_ready_to_retry() {
                trace!("Going to retry now");
                self.start_invocation_task(
                    partition,
                    service_invocation_id,
                    InvokeInputJournal::NoCachedJournal,
                    // In case we're retrying, we don't modify the retry policy
                    |_| ism,
                )
                .await;
            } else {
                trace!(
                    "Not going to retry. Invocation state: {:?}",
                    ism.invocation_state_debug()
                );
                // Not ready for retrying yet
                self.invocation_state_machines_tree.register_invocation(
                    partition,
                    service_invocation_id,
                    ism,
                );
            }
        } else {
            // If no state machine is registered, the PP will send a new invoke
            trace!("No state machine found for given retry event");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        entry_enricher, journal_reader, state_reader, InvokeInputJournal, Service, ServiceHandle,
    };
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
            None,
            journal_reader::mocks::EmptyJournalReader,
            state_reader::mocks::EmptyStateReader,
            entry_enricher::mocks::MockEntryEnricher::default(),
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
