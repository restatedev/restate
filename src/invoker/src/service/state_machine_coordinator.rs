use crate::service::invocation_state_machine::InvocationStateMachine;
use crate::service::*;
use std::collections::HashSet;

use codederror::CodedError;
use restate_common::errors::UserErrorCode::Internal;
use restate_common::types::EnrichedRawEntry;
use restate_errors::warn_it;
use restate_journal::raw::Header;
use restate_journal::EntryEnricher;
use restate_service_metadata::{ProtocolType, ServiceEndpointRegistry};
use std::time::SystemTime;
use tracing::{instrument, warn};

#[derive(Debug, Clone, thiserror::Error, codederror::CodedError)]
#[error("Cannot find service {0} in the service endpoint registry")]
#[code(unknown)]
pub struct CannotResolveEndpoint(String);

impl InvokerError for CannotResolveEndpoint {
    fn is_transient(&self) -> bool {
        true
    }

    fn to_invocation_error(&self) -> InvocationError {
        InvocationError::new(Internal, self.to_string())
    }
}

/// This struct groups the arguments to start an InvocationTask.
///
/// Because the methods receiving this struct might not start
/// the InvocationTask (e.g. if the request cannot be retried now),
/// we pass only borrows so we create clones only if we need them.
pub(in crate::service) struct StartInvocationTaskArguments<'a, JR, SR, EE, SER> {
    client: &'a HttpsClient,
    journal_reader: &'a JR,
    state_reader: &'a SR,
    entry_enricher: &'a EE,
    service_endpoint_registry: &'a SER,
    default_retry_policy: &'a RetryPolicy,
    suspension_timeout: Duration,
    response_abort_timeout: Duration,
    disable_eager_state: bool,
    message_size_warning: usize,
    message_size_limit: Option<usize>,
    invocation_tasks: &'a mut JoinSet<()>,
    invocation_tasks_tx: &'a mpsc::UnboundedSender<InvocationTaskOutput>,
    retry_timers: &'a mut TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
}

impl<'a, JR, SR, EE, SER> StartInvocationTaskArguments<'a, JR, SR, EE, SER> {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::service) fn new(
        client: &'a HttpsClient,
        journal_reader: &'a JR,
        state_reader: &'a SR,
        entry_enricher: &'a EE,
        service_endpoint_registry: &'a SER,
        default_retry_policy: &'a RetryPolicy,
        suspension_timeout: Duration,
        response_abort_timeout: Duration,
        disable_eager_state: bool,
        message_size_warning: usize,
        message_size_limit: Option<usize>,
        invocation_tasks: &'a mut JoinSet<()>,
        invocation_tasks_tx: &'a mpsc::UnboundedSender<InvocationTaskOutput>,
        retry_timers: &'a mut TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
    ) -> Self {
        Self {
            client,
            journal_reader,
            state_reader,
            entry_enricher,
            service_endpoint_registry,
            default_retry_policy,
            suspension_timeout,
            response_abort_timeout,
            disable_eager_state,
            message_size_warning,
            message_size_limit,
            invocation_tasks,
            invocation_tasks_tx,
            retry_timers,
        }
    }
}

#[derive(Debug, Default)]
pub(in crate::service) struct InvocationStateMachineCoordinator {
    partitions: HashMap<PartitionLeaderEpoch, PartitionInvocationStateMachineCoordinator>,
}

impl InvocationStateMachineCoordinator {
    #[inline]
    pub(in crate::service) fn resolve_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
    ) -> Option<&mut PartitionInvocationStateMachineCoordinator> {
        self.partitions.get_mut(&partition)
    }

    #[inline]
    pub(in crate::service) fn remove_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
    ) -> Option<PartitionInvocationStateMachineCoordinator> {
        self.partitions.remove(&partition)
    }

    #[inline]
    pub(in crate::service) fn register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        sender: mpsc::Sender<Effect>,
    ) {
        self.partitions.insert(
            partition,
            PartitionInvocationStateMachineCoordinator::new(partition, sender),
        );
    }

    pub(in crate::service) fn abort_all(&mut self) {
        for partition in self.partitions.values_mut() {
            partition.abort_all();
        }
    }
}

#[derive(Debug)]
pub(in crate::service) struct PartitionInvocationStateMachineCoordinator {
    partition: PartitionLeaderEpoch,
    output_tx: mpsc::Sender<Effect>,
    invocation_state_machines: HashMap<ServiceInvocationId, InvocationStateMachine>,
}

impl PartitionInvocationStateMachineCoordinator {
    fn new(partition: PartitionLeaderEpoch, sender: mpsc::Sender<Effect>) -> Self {
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
        restate.invocation.sid = %invoke_input_cmd.service_invocation_id,
        restate.invoker.partition_leader_epoch = ?self.partition,
        )
        )]
    pub(in crate::service) async fn handle_invoke<JR, SR, EE, SER>(
        &mut self,
        invoke_input_cmd: InvokeInputCommand,
        start_arguments: StartInvocationTaskArguments<'_, JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
    ) where
        JR: JournalReader + Clone + Send + Sync + 'static,
        <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
        SR: StateReader + Clone + Send + Sync + 'static,
        <SR as StateReader>::StateIter: Send,
        EE: EntryEnricher + Clone + Send + 'static,
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
            status,
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
    pub(in crate::service) fn abort_all(&mut self) {
        for (service_invocation_id, sm) in self.invocation_state_machines.iter_mut() {
            trace!(
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.sid = %service_invocation_id,
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
        restate.invocation.sid = %service_invocation_id,
        restate.invoker.partition_leader_epoch = ?self.partition,
        )
        )]
    pub(in crate::service) fn abort(&mut self, service_invocation_id: ServiceInvocationId) {
        if let Some(mut sm) = self
            .invocation_state_machines
            .remove(&service_invocation_id)
        {
            sm.abort()
        }
        // If no state machine, it might have been already aborted.
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
        rpc.service = %service_invocation_id.service_id.service_name,
        restate.invocation.sid = %service_invocation_id,
        restate.invoker.partition_leader_epoch = ?self.partition,
        )
        )]
    pub(in crate::service) fn handle_completion(
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
        restate.invocation.sid = %service_invocation_id,
        restate.invoker.partition_leader_epoch = ?self.partition,
        )
        )]
    pub(in crate::service) async fn handle_retry_timer_fired<JR, SR, EE, SER>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        start_arguments: StartInvocationTaskArguments<'_, JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
    ) where
        JR: JournalReader + Clone + Send + Sync + 'static,
        <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
        SR: StateReader + Clone + Send + Sync + 'static,
        <SR as StateReader>::StateIter: Send,
        EE: EntryEnricher + Clone + Send + 'static,
        SER: ServiceEndpointRegistry,
    {
        trace!("Retry timeout fired");
        self.handle_retry_event(service_invocation_id, start_arguments, status, |sm| {
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
        restate.invoker.partition_leader_epoch = ?self.partition,
        restate.journal.index = entry_index,
        )
        )]
    pub(in crate::service) async fn handle_stored_entry_ack<JR, SR, EE, SER>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        start_arguments: StartInvocationTaskArguments<'_, JR, SR, EE, SER>,
        entry_index: EntryIndex,
        status: &mut status_store::InvocationStatusStore,
    ) where
        JR: JournalReader + Clone + Send + Sync + 'static,
        <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
        SR: StateReader + Clone + Send + Sync + 'static,
        <SR as StateReader>::StateIter: Send,
        EE: EntryEnricher + Clone + Send + 'static,
        SER: ServiceEndpointRegistry,
    {
        trace!("Received a new stored journal entry acknowledgement");
        self.handle_retry_event(service_invocation_id, start_arguments, status, |sm| {
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
        restate.invoker.partition_leader_epoch = ?self.partition,
        restate.journal.index = entry_index,
        restate.journal.entry_type = ?entry.header.to_entry_type(),
        )
        )]
    pub(in crate::service) async fn handle_new_entry(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
    ) {
        if let Some(sm) = self
            .invocation_state_machines
            .get_mut(&service_invocation_id)
        {
            sm.notify_new_entry(entry_index);
            trace!(
                "Received a new entry. Invocation state: {:?}",
                sm.invocation_state_debug()
            );
            let _ = self
                .output_tx
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
        level = "warn",
        skip_all,
        fields(
        rpc.service = %service_invocation_id.service_id.service_name,
        restate.invocation.sid = %service_invocation_id,
        restate.invoker.partition_leader_epoch = ?self.partition,
        )
        )]
    pub(in crate::service) async fn handle_invocation_task_closed(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        status: &mut status_store::InvocationStatusStore,
    ) {
        if self
            .invocation_state_machines
            .remove(&service_invocation_id)
            .is_some()
        {
            trace!("Invocation task closed correctly");
            status.on_end(&self.partition, &service_invocation_id);
            self.send_end(service_invocation_id).await;
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
        restate.invocation.sid = %service_invocation_id,
        restate.invoker.partition_leader_epoch = ?self.partition,
        )
        )]
    pub(in crate::service) async fn handle_invocation_task_failed(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        error: impl InvokerError + CodedError + Send + Sync + 'static,
        retry_timers: &mut TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
        status: &mut status_store::InvocationStatusStore,
    ) {
        if let Some(sm) = self
            .invocation_state_machines
            .remove(&service_invocation_id)
        {
            self.handle_error_event(service_invocation_id, error, retry_timers, sm, status)
                .await;
        } else {
            // If no state machine, this might be a result for an aborted invocation.
            trace!("No state machine found for invocation task error signal");
        }
    }

    async fn handle_error_event<E: InvokerError + CodedError + Send + Sync + 'static>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        error: E,
        retry_timers: &mut TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
        mut sm: InvocationStateMachine,
        status: &mut status_store::InvocationStatusStore,
    ) {
        warn_it!(error, "Error when executing the invocation");

        match sm.handle_task_error() {
            Some(next_retry_timer_duration) if error.is_transient() => {
                trace!(
                    "Starting the retry timer {}. Invocation state: {:?}",
                    humantime::format_duration(next_retry_timer_duration),
                    sm.invocation_state_debug()
                );
                status.on_failure(self.partition, service_invocation_id.clone(), &error);
                self.invocation_state_machines
                    .insert(service_invocation_id.clone(), sm);
                retry_timers.sleep_until(
                    SystemTime::now() + next_retry_timer_duration,
                    (self.partition, service_invocation_id),
                );
            }
            _ => {
                trace!("Not going to retry the error");
                status.on_end(&self.partition, &service_invocation_id);
                self.send_error(service_invocation_id, error.to_invocation_error())
                    .await;
            }
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
        rpc.service = %service_invocation_id.service_id.service_name,
        restate.invocation.sid = %service_invocation_id,
        restate.invoker.partition_leader_epoch = ?self.partition,
        )
        )]
    pub(in crate::service) async fn handle_invocation_task_suspended(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        entry_indexes: HashSet<EntryIndex>,
        status: &mut status_store::InvocationStatusStore,
    ) {
        if self
            .invocation_state_machines
            .remove(&service_invocation_id)
            .is_some()
        {
            trace!("Suspending invocation");
            status.on_end(&self.partition, &service_invocation_id);
            let _ = self
                .output_tx
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

    // --- Helpers

    async fn start_invocation_task<JR, SR, EE, SER>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
        start_arguments: StartInvocationTaskArguments<'_, JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
        state_machine_factory: impl FnOnce(RetryPolicy) -> InvocationStateMachine,
    ) where
        JR: JournalReader + Clone + Send + Sync + 'static,
        <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
        SR: StateReader + Clone + Send + Sync + 'static,
        <SR as StateReader>::StateIter: Send,
        EE: EntryEnricher + Clone + Send + 'static,
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

                // This method needs a state machine
                self.handle_error_event(
                    service_invocation_id,
                    err,
                    start_arguments.retry_timers,
                    state_machine_factory(start_arguments.default_retry_policy.clone()),
                    status,
                )
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
            invocation_task::InvocationTask::new(
                start_arguments.client.clone(),
                self.partition,
                service_invocation_id.clone(),
                0,
                metadata,
                start_arguments.suspension_timeout,
                start_arguments.response_abort_timeout,
                start_arguments.disable_eager_state,
                start_arguments.message_size_warning,
                start_arguments.message_size_limit,
                start_arguments.journal_reader.clone(),
                start_arguments.state_reader.clone(),
                start_arguments.entry_enricher.clone(),
                start_arguments.invocation_tasks_tx.clone(),
                completions_rx,
            )
            .run(journal),
        );

        // Transition the state machine, and store it
        status.on_start(self.partition, service_invocation_id.clone());
        invocation_state_machine.start(abort_handle, completions_tx);
        trace!(
            "Invocation task started state. Invocation state: {:?}",
            invocation_state_machine.invocation_state_debug()
        );
        self.invocation_state_machines
            .insert(service_invocation_id, invocation_state_machine);
    }

    async fn handle_retry_event<JR, SR, EE, SER, FN>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        start_arguments: StartInvocationTaskArguments<'_, JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
        f: FN,
    ) where
        JR: JournalReader + Clone + Send + Sync + 'static,
        <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
        SR: StateReader + Clone + Send + Sync + 'static,
        <SR as StateReader>::StateIter: Send,
        EE: EntryEnricher + Clone + Send + 'static,
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
                    status,
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
            .send(Effect {
                service_invocation_id,
                kind: EffectKind::End,
            })
            .await;
    }

    async fn send_error(&self, service_invocation_id: ServiceInvocationId, err: InvocationError) {
        let _ = self
            .output_tx
            .send(Effect {
                service_invocation_id,
                kind: EffectKind::Failed(err),
            })
            .await;
    }
}
