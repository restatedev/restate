use crate::service::invocation_state_machine::InvocationStateMachine;
use crate::service::*;
use crate::EntryEnricher;
use codederror::CodedError;
use restate_common::errors::UserErrorCode::Internal;
use restate_common::journal::raw::Header;
use restate_common::types::EnrichedRawEntry;
use restate_errors::warn_it;
use restate_service_metadata::{ProtocolType, ServiceEndpointRegistry};
use std::collections::HashSet;
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
        service_state: &mut ServiceState<JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
        invoke_input_cmd: InvokeCommand,
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
            service_state,
            status,
            service_invocation_id.clone(),
            invoke_input_cmd.journal,
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
        service_state: &mut ServiceState<JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
        service_invocation_id: ServiceInvocationId,
    ) where
        JR: JournalReader + Clone + Send + Sync + 'static,
        <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
        SR: StateReader + Clone + Send + Sync + 'static,
        <SR as StateReader>::StateIter: Send,
        EE: EntryEnricher + Clone + Send + 'static,
        SER: ServiceEndpointRegistry,
    {
        trace!("Retry timeout fired");
        self.handle_retry_event(service_state, status, service_invocation_id, |sm| {
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
        service_state: &mut ServiceState<JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) where
        JR: JournalReader + Clone + Send + Sync + 'static,
        <JR as JournalReader>::JournalStream: Unpin + Send + 'static,
        SR: StateReader + Clone + Send + Sync + 'static,
        <SR as StateReader>::StateIter: Send,
        EE: EntryEnricher + Clone + Send + 'static,
        SER: ServiceEndpointRegistry,
    {
        trace!("Received a new stored journal entry acknowledgement");
        self.handle_retry_event(service_state, status, service_invocation_id, |sm| {
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
        status: &mut status_store::InvocationStatusStore,
        service_invocation_id: ServiceInvocationId,
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
        retry_timers: &mut TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
        status: &mut status_store::InvocationStatusStore,
        service_invocation_id: ServiceInvocationId,
        error: impl InvokerError + CodedError + Send + Sync + 'static,
    ) {
        if let Some(sm) = self
            .invocation_state_machines
            .remove(&service_invocation_id)
        {
            self.handle_error_event(retry_timers, status, service_invocation_id, error, sm)
                .await;
        } else {
            // If no state machine, this might be a result for an aborted invocation.
            trace!("No state machine found for invocation task error signal");
        }
    }

    async fn handle_error_event<E: InvokerError + CodedError + Send + Sync + 'static>(
        &mut self,
        retry_timers: &mut TimerQueue<(PartitionLeaderEpoch, ServiceInvocationId)>,
        status: &mut status_store::InvocationStatusStore,
        service_invocation_id: ServiceInvocationId,
        error: E,
        mut sm: InvocationStateMachine,
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
        status: &mut status_store::InvocationStatusStore,
        service_invocation_id: ServiceInvocationId,
        entry_indexes: HashSet<EntryIndex>,
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
        service_state: &mut ServiceState<JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
        service_invocation_id: ServiceInvocationId,
        journal: InvokeInputJournal,
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
        let metadata = match service_state
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
                    &mut service_state.retry_timers,
                    status,
                    service_invocation_id,
                    err,
                    state_machine_factory(service_state.default_retry_policy.clone()),
                )
                .await;
                return;
            }
        };

        let retry_policy = metadata
            .retry_policy()
            .unwrap_or(&service_state.default_retry_policy)
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
        let abort_handle = service_state.invocation_tasks.spawn(
            invocation_task::InvocationTask::new(
                service_state.client.clone(),
                self.partition,
                service_invocation_id.clone(),
                0,
                metadata,
                service_state.suspension_timeout,
                service_state.response_abort_timeout,
                service_state.disable_eager_state,
                service_state.message_size_warning,
                service_state.message_size_limit,
                service_state.journal_reader.clone(),
                service_state.state_reader.clone(),
                service_state.entry_enricher.clone(),
                service_state.invocation_tasks_tx.clone(),
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
        service_state: &mut ServiceState<JR, SR, EE, SER>,
        status: &mut status_store::InvocationStatusStore,
        service_invocation_id: ServiceInvocationId,
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
                    service_state,
                    status,
                    service_invocation_id,
                    InvokeInputJournal::NoCachedJournal,
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
