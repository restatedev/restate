// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod input_command;
mod invocation_state_machine;
mod invocation_task;
mod metric_definitions;
mod quota;
mod state_machine_manager;
mod status_store;

use futures::Stream;
use input_command::{InputCommand, InvokeCommand};
use invocation_state_machine::InvocationStateMachine;
use invocation_task::InvocationTask;
use invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
use metrics::counter;
use restate_core::cancellation_watcher;
use restate_errors::warn_it;
use restate_invoker_api::{
    Effect, EffectKind, EntryEnricher, InvocationErrorReport, InvocationStatusReport,
    InvokeInputJournal, JournalReader, StateReader,
};
use restate_queue::SegmentQueue;
use restate_schema_api::deployment::DeploymentResolver;
use restate_timer_queue::TimerQueue;
use restate_types::arc_util::Updateable;
use restate_types::config::{InvokerOptions, ServiceClientOptions};
use restate_types::identifiers::{DeploymentId, InvocationId, PartitionKey, WithPartitionKey};
use restate_types::identifiers::{EntryIndex, PartitionLeaderEpoch};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::Completion;
use restate_types::retries::RetryPolicy;
use status_store::InvocationStatusStore;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::SystemTime;
use std::{cmp, panic};
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};
use tracing::instrument;
use tracing::{debug, trace};

use crate::invocation_task::InvocationTaskError;
pub use input_command::ChannelStatusReader;
pub use input_command::InvokerHandle;
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_service_protocol::RESTATE_SERVICE_PROTOCOL_VERSION;
use restate_types::invocation::InvocationTarget;

use crate::metric_definitions::{
    INVOKER_ENQUEUE, INVOKER_INVOCATION_TASK, TASK_OP_COMPLETED, TASK_OP_FAILED, TASK_OP_STARTED,
    TASK_OP_SUSPENDED,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Notification {
    Completion(Completion),
    Ack(EntryIndex),
}

// -- InvocationTask factory: we use this to mock the state machine in tests

trait InvocationTaskRunner<SR> {
    #[allow(clippy::too_many_arguments)]
    fn start_invocation_task(
        &self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        storage_reader: SR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
        input_journal: InvokeInputJournal,
        task_pool: &mut JoinSet<()>,
    ) -> AbortHandle;
}

#[derive(Debug)]
struct DefaultInvocationTaskRunner<EE, DMR> {
    client: ServiceClient,
    entry_enricher: EE,
    deployment_metadata_resolver: DMR,
}

impl<SR, EE, DMR> InvocationTaskRunner<SR> for DefaultInvocationTaskRunner<EE, DMR>
where
    SR: JournalReader + StateReader + Clone + Send + Sync + 'static,
    <SR as JournalReader>::JournalStream: Unpin + Send + 'static,
    <SR as StateReader>::StateIter: Send,
    EE: EntryEnricher + Clone + Send + 'static,
    DMR: DeploymentResolver + Clone + Send + 'static,
{
    fn start_invocation_task(
        &self,
        opts: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        storage_reader: SR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
        input_journal: InvokeInputJournal,
        task_pool: &mut JoinSet<()>,
    ) -> AbortHandle {
        task_pool.spawn(
            InvocationTask::new(
                self.client.clone(),
                partition,
                invocation_id,
                invocation_target,
                RESTATE_SERVICE_PROTOCOL_VERSION,
                opts.inactivity_timeout.into(),
                opts.abort_timeout.into(),
                opts.disable_eager_state,
                opts.message_size_warning.get(),
                opts.message_size_limit(),
                storage_reader.clone(),
                storage_reader,
                self.entry_enricher.clone(),
                self.deployment_metadata_resolver.clone(),
                invoker_tx,
                invoker_rx,
            )
            .run(input_journal),
        )
    }
}

// -- Service implementation

#[derive(Debug)]
pub struct Service<SR, EntryEnricher, DeploymentRegistry> {
    // Used for constructing the invoker sender and status reader
    input_tx: mpsc::UnboundedSender<InputCommand<SR>>,
    status_tx: mpsc::UnboundedSender<
        restate_futures_util::command::Command<
            RangeInclusive<PartitionKey>,
            Vec<InvocationStatusReport>,
        >,
    >,
    // For the segment queue
    tmp_dir: PathBuf,
    // We have this level of indirection to hide the InvocationTaskRunner,
    // which is a rather internal thing we have only for mocking.
    inner: ServiceInner<DefaultInvocationTaskRunner<EntryEnricher, DeploymentRegistry>, SR>,
}

impl<SR, EE, DMR> Service<SR, EE, DMR> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<JS>(
        options: &InvokerOptions,
        deployment_metadata_resolver: DMR,
        client: ServiceClient,
        entry_enricher: EE,
    ) -> Service<SR, EE, DMR>
    where
        SR: JournalReader<JournalStream = JS> + StateReader + Clone + Send + Sync + 'static,
        JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
        EE: EntryEnricher,
        DMR: DeploymentResolver,
    {
        let (input_tx, input_rx) = mpsc::unbounded_channel();
        let (status_tx, status_rx) = mpsc::unbounded_channel();
        let (invocation_tasks_tx, invocation_tasks_rx) = mpsc::unbounded_channel();

        Self {
            input_tx,
            status_tx,
            tmp_dir: options.gen_tmp_dir(),
            inner: ServiceInner {
                input_rx,
                status_rx,
                invocation_tasks_tx,
                invocation_tasks_rx,
                invocation_task_runner: DefaultInvocationTaskRunner {
                    client,
                    entry_enricher,
                    deployment_metadata_resolver,
                },
                invocation_tasks: Default::default(),
                retry_timers: Default::default(),
                quota: quota::InvokerConcurrencyQuota::new(options.concurrent_invocations_limit()),
                status_store: Default::default(),
                invocation_state_machine_manager: Default::default(),
            },
        }
    }

    pub fn from_options<JS>(
        service_client_options: &ServiceClientOptions,
        invoker_options: &InvokerOptions,
        entry_enricher: EE,
        deployment_registry: DMR,
    ) -> Result<Service<SR, EE, DMR>, BuildError>
    where
        SR: JournalReader<JournalStream = JS> + StateReader + Clone + Send + Sync + 'static,
        JS: Stream<Item = PlainRawEntry> + Unpin + Send + 'static,
        EE: EntryEnricher,
        DMR: DeploymentResolver,
    {
        metric_definitions::describe_metrics();
        let client =
            ServiceClient::from_options(service_client_options, AssumeRoleCacheMode::Unbounded)?;

        Ok(Service::new(
            invoker_options,
            deployment_registry,
            client,
            entry_enricher,
        ))
    }
}

#[derive(Debug, thiserror::Error)]
#[error("failed building the invoker service: {0}")]
pub enum BuildError {
    ServiceClient(#[from] restate_service_client::BuildError),
}

impl<SR, EE, EMR> Service<SR, EE, EMR>
where
    SR: JournalReader + StateReader + Clone + Send + Sync + 'static,
    <SR as JournalReader>::JournalStream: Unpin + Send + 'static,
    <SR as StateReader>::StateIter: Send,
    EE: EntryEnricher + Clone + Send + 'static,
    EMR: DeploymentResolver + Clone + Send + 'static,
{
    pub fn handle(&self) -> InvokerHandle<SR> {
        InvokerHandle {
            input: self.input_tx.clone(),
        }
    }

    pub fn status_reader(&self) -> ChannelStatusReader {
        ChannelStatusReader(self.status_tx.clone())
    }

    pub async fn run(
        self,
        mut updateable_options: impl Updateable<InvokerOptions> + Send + 'static,
    ) -> anyhow::Result<()> {
        let Service {
            tmp_dir,
            inner: mut service,
            ..
        } = self;

        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        // Prepare the segmented queue
        let mut segmented_input_queue = SegmentQueue::init(tmp_dir, 1_056_784)
            .await
            .expect("Cannot initialize input spillable queue");

        loop {
            let options = updateable_options.load();
            if !service
                .step(options, &mut segmented_input_queue, shutdown.as_mut())
                .await
            {
                break;
            }
        }

        // Wait for all the tasks to shutdown
        service.invocation_tasks.shutdown().await;
        Ok(())
    }
}

#[derive(Debug)]
struct ServiceInner<InvocationTaskRunner, SR> {
    input_rx: mpsc::UnboundedReceiver<InputCommand<SR>>,
    status_rx: mpsc::UnboundedReceiver<
        restate_futures_util::command::Command<
            RangeInclusive<PartitionKey>,
            Vec<InvocationStatusReport>,
        >,
    >,

    // Channel to communicate with invocation tasks
    invocation_tasks_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
    invocation_tasks_rx: mpsc::UnboundedReceiver<InvocationTaskOutput>,

    // Invocation task factory
    invocation_task_runner: InvocationTaskRunner,

    // Invoker state machine
    invocation_tasks: JoinSet<()>,
    retry_timers: TimerQueue<(PartitionLeaderEpoch, InvocationId)>,
    quota: quota::InvokerConcurrencyQuota,
    status_store: InvocationStatusStore,
    invocation_state_machine_manager: state_machine_manager::InvocationStateMachineManager<SR>,
}

impl<ITR, SR> ServiceInner<ITR, SR>
where
    ITR: InvocationTaskRunner<SR>,
    SR: JournalReader + StateReader + Clone + Send + Sync + 'static,
    <SR as JournalReader>::JournalStream: Unpin + Send + 'static,
    <SR as StateReader>::StateIter: Send,
{
    // Returns true if we should execute another step, false if we should stop executing steps
    async fn step<F>(
        &mut self,
        options: &InvokerOptions,
        segmented_input_queue: &mut SegmentQueue<InvokeCommand>,
        mut shutdown: Pin<&mut F>,
    ) -> bool
    where
        F: Future<Output = ()>,
    {
        tokio::select! {
            Some(cmd) = self.status_rx.recv() => {
                let keys = cmd.payload();
                let statuses = self
                    .invocation_state_machine_manager
                    .registered_partitions_with_keys(keys.clone())
                    .flat_map(|partition| self.status_store.status_for_partition(partition))
                    .filter(|status| keys.contains(&status.invocation_id().partition_key()))
                    .collect();
                let _ = cmd.reply(statuses);
            },

            Some(input_message) = self.input_rx.recv() => {
                match input_message {
                    // --- Spillable queue loading/offloading
                    InputCommand::Invoke(invoke_command) => {
                        counter!(INVOKER_ENQUEUE).increment(1);
                        segmented_input_queue.enqueue(invoke_command).await;
                    },
                    // --- Other commands (they don't go through the segment queue)
                    InputCommand::RegisterPartition { partition, partition_key_range, storage_reader, sender, } => {
                        self.handle_register_partition(partition, partition_key_range,
                                storage_reader, sender);
                    },
                    InputCommand::Abort { partition, invocation_id } => {
                        self.handle_abort_invocation(partition, invocation_id);
                    }
                    InputCommand::AbortAllPartition { partition } => {
                        self.handle_abort_partition(partition);
                    }
                    InputCommand::Completion { partition, invocation_id, completion } => {
                        self.handle_completion(partition, invocation_id, completion);
                    },
                    InputCommand::StoredEntryAck { partition, invocation_id, entry_index } => {
                        self.handle_stored_entry_ack(options, partition, invocation_id, entry_index).await;
                    }
                }
            },

            Some(invoke_input_command) = segmented_input_queue.dequeue(), if !segmented_input_queue.is_empty() && self.quota.is_slot_available() => {
                self.handle_invoke(options, invoke_input_command.partition, invoke_input_command.invocation_id, invoke_input_command.invocation_target, invoke_input_command.journal).await;
            },

            Some(invocation_task_msg) = self.invocation_tasks_rx.recv() => {
                let InvocationTaskOutput {
                    invocation_id,
                    partition,
                    inner
                } = invocation_task_msg;
                match inner {
                    InvocationTaskOutputInner::SelectedDeployment(deployment_id, has_changed) => {
                        self.handle_selected_deployment(
                            partition,
                            invocation_id,
                            deployment_id,
                            has_changed,
                        ).await
                    }
                    InvocationTaskOutputInner::ServerHeaderReceived(x_restate_server_header) => {
                        self.handle_server_header_received(
                            partition,
                            invocation_id,
                            x_restate_server_header
                        ).await
                    }
                    InvocationTaskOutputInner::NewEntry {entry_index, entry, requires_ack} => {
                        self.handle_new_entry(
                            partition,
                            invocation_id,
                            entry_index,
                            entry,
                            requires_ack
                        ).await
                    },
                    InvocationTaskOutputInner::Closed => {
                        self.handle_invocation_task_closed(partition, invocation_id).await
                    },
                    InvocationTaskOutputInner::Failed(e) => {
                        self.handle_invocation_task_failed(partition, invocation_id, e).await
                    },
                    InvocationTaskOutputInner::Suspended(indexes) => {
                        self.handle_invocation_task_suspended(partition, invocation_id, indexes).await
                    }
                };
            },
            timer = self.retry_timers.await_timer() => {
                let (partition, fid) = timer.into_inner();
                self.handle_retry_timer_fired(options, partition, fid).await;
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
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_register_partition(
        &mut self,
        partition: PartitionLeaderEpoch,
        partition_key_range: RangeInclusive<PartitionKey>,
        storage_reader: SR,
        sender: mpsc::Sender<Effect>,
    ) {
        self.invocation_state_machine_manager.register_partition(
            partition,
            partition_key_range,
            storage_reader,
            sender,
        );
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            rpc.service = %invocation_target.service_name(),
            rpc.method = %invocation_target.handler_name(),
            restate.invocation.id = %invocation_id,
            restate.invocation.target = %invocation_target,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invoke(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        journal: InvokeInputJournal,
    ) {
        debug_assert!(self
            .invocation_state_machine_manager
            .has_partition(partition));
        debug_assert!(self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
            .is_none());

        let storage_reader = self
            .invocation_state_machine_manager
            .partition_storage_reader(partition)
            .expect("partition is registered");
        self.quota.reserve_slot();
        self.start_invocation_task(
            options,
            partition,
            storage_reader.clone(),
            invocation_id,
            journal,
            InvocationStateMachine::create(invocation_target, options.retry_policy.clone()),
        )
        .await
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_retry_timer_fired(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) {
        trace!("Retry timeout fired");
        self.handle_retry_event(options, partition, invocation_id, |sm| {
            sm.notify_retry_timer_fired()
        })
        .await;
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.index = entry_index,
        )
    )]
    async fn handle_stored_entry_ack(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) {
        trace!("Received a new stored journal entry acknowledgement");
        self.handle_retry_event(options, partition, invocation_id, |sm| {
            sm.notify_stored_ack(entry_index)
        })
        .await;
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.deployment.id = %deployment_id,
        )
    )]
    async fn handle_selected_deployment(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        deployment_id: DeploymentId,
        has_changed: bool,
    ) {
        if let Some((_, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
        {
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Chosen deployment {}. Invocation state: {:?}",
                deployment_id,
                ism.invocation_state_debug()
            );

            self.status_store
                .on_deployment_chosen(&partition, &invocation_id, deployment_id);
            // If we think this selected deployment has been freshly picked, otherwise
            // we assume that we have stored it previously.
            if has_changed {
                ism.notify_chosen_deployment(deployment_id);
            }
        } else {
            // If no state machine, this might be an event for an aborted invocation.
            trace!("No state machine found for selected deployment id");
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_server_header_received(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        x_restate_server_header: String,
    ) {
        if let Some((_, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
        {
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "x-restate-server header {}. Invocation state: {:?}",
                x_restate_server_header,
                ism.invocation_state_debug()
            );

            self.status_store.on_server_header_receiver(
                &partition,
                &invocation_id,
                x_restate_server_header,
            );
        } else {
            // If no state machine, this might be an event for an aborted invocation.
            trace!("No state machine found for selected server header");
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.index = entry_index,
            restate.journal.entry_type = ?entry.ty(),
        )
    )]
    async fn handle_new_entry(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
        requires_ack: bool,
    ) {
        if let Some((output_tx, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
        {
            ism.notify_new_entry(entry_index, requires_ack);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Received a new entry. Invocation state: {:?}",
                ism.invocation_state_debug()
            );
            if let Some(deployment_id) = ism.chosen_deployment_to_notify() {
                let _ = output_tx
                    .send(Effect {
                        invocation_id,
                        kind: EffectKind::SelectedDeployment(deployment_id),
                    })
                    .await;
            }
            let _ = output_tx
                .send(Effect {
                    invocation_id,
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
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_completion(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        completion: Completion,
    ) {
        if let Some((_, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
        {
            trace!(
                restate.invocation.target = %ism.invocation_target,
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
        level = "debug",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_closed(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) {
        if let Some((sender, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            counter!(INVOKER_INVOCATION_TASK, "status" => TASK_OP_COMPLETED).increment(1);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Invocation task closed correctly");
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);
            let _ = sender
                .send(Effect {
                    invocation_id,
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
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_suspended(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_indexes: HashSet<EntryIndex>,
    ) {
        if let Some((sender, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            counter!(INVOKER_INVOCATION_TASK, "status" => TASK_OP_SUSPENDED).increment(1);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Suspending invocation");
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);
            let _ = sender
                .send(Effect {
                    invocation_id,
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
        level = "debug",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_failed(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        error: InvocationTaskError,
    ) {
        if let Some((_, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            self.handle_error_event(partition, invocation_id, error, ism)
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
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_abort_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) {
        if let Some((_, _, mut ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Aborting invocation");
            ism.abort();
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);
        } else {
            trace!("Ignoring Abort command because there is no matching partition/invocation");
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
            .invocation_state_machine_manager
            .remove_partition(partition)
        {
            for (fid, mut ism) in invocation_state_machines.into_iter() {
                trace!(
                    restate.invocation.id = %fid,
                    restate.invocation.target = %ism.invocation_target,
                    "Aborting invocation"
                );
                ism.abort();
                self.quota.unreserve_slot();
                self.status_store.on_end(&partition, &fid);
            }
        } else {
            trace!("Ignoring AbortAll command because there is no matching partition");
        }
    }

    #[instrument(level = "trace", skip_all)]
    fn handle_shutdown(&mut self) {
        let partitions = self
            .invocation_state_machine_manager
            .registered_partitions();
        for partition in partitions {
            self.handle_abort_partition(partition);
        }
    }

    // --- Helpers

    async fn handle_error_event(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        error: InvocationTaskError,
        mut ism: InvocationStateMachine,
    ) {
        match ism.handle_task_error() {
            Some(next_retry_timer_duration) if error.is_transient() => {
                counter!(INVOKER_INVOCATION_TASK,
                    "status" => TASK_OP_FAILED,
                    "transient" => "true"
                )
                .increment(1);
                warn_it!(
                    error,
                    restate.invocation.id = %invocation_id,
                    restate.invocation.target = %ism.invocation_target,
                    "Error when executing the invocation, retrying in {}.",
                    humantime::format_duration(next_retry_timer_duration));
                trace!("Invocation state: {:?}.", ism.invocation_state_debug());
                let next_retry_at = SystemTime::now() + next_retry_timer_duration;

                self.status_store.on_failure(
                    partition,
                    invocation_id,
                    error.into_invocation_error_report(),
                    Some(next_retry_at),
                );
                self.invocation_state_machine_manager.register_invocation(
                    partition,
                    invocation_id,
                    ism,
                );
                self.retry_timers
                    .sleep_until(next_retry_at, (partition, invocation_id));
            }
            _ => {
                counter!(INVOKER_INVOCATION_TASK,
                    "status" => TASK_OP_FAILED,
                    "transient" => "false"
                )
                .increment(1);
                warn_it!(
                    error,
                    restate.invocation.id = %invocation_id,
                    restate.invocation.target = %ism.invocation_target,
                    "Error when executing the invocation, not going to retry.");
                self.quota.unreserve_slot();
                self.status_store.on_end(&partition, &invocation_id);

                let _ = self
                    .invocation_state_machine_manager
                    .resolve_partition_sender(partition)
                    .expect("Partition should be registered")
                    .send(Effect {
                        invocation_id,
                        kind: EffectKind::Failed(error.into_invocation_error()),
                    })
                    .await;
            }
        }
    }

    async fn start_invocation_task(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        storage_reader: SR,
        invocation_id: InvocationId,
        journal: InvokeInputJournal,
        mut ism: InvocationStateMachine,
    ) {
        // Start the InvocationTask
        let (completions_tx, completions_rx) = mpsc::unbounded_channel();
        let abort_handle = self.invocation_task_runner.start_invocation_task(
            options,
            partition,
            invocation_id,
            ism.invocation_target.clone(),
            storage_reader,
            self.invocation_tasks_tx.clone(),
            completions_rx,
            journal,
            &mut self.invocation_tasks,
        );

        // Transition the state machine, and store it
        self.status_store.on_start(partition, invocation_id);
        ism.start(abort_handle, completions_tx);
        trace!(
            restate.invocation.target = %ism.invocation_target,
            "Invocation task started state. Invocation state: {:?}",
            ism.invocation_state_debug()
        );
        counter!(INVOKER_INVOCATION_TASK, "status" => TASK_OP_STARTED).increment(1);
        self.invocation_state_machine_manager
            .register_invocation(partition, invocation_id, ism);
    }

    async fn handle_retry_event<FN>(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        f: FN,
    ) where
        FN: FnOnce(&mut InvocationStateMachine),
    {
        if let Some((_, storage_reader, mut ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            f(&mut ism);
            if ism.is_ready_to_retry() {
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    "Going to retry now");
                let storage_reader = storage_reader.clone();
                self.start_invocation_task(
                    options,
                    partition,
                    storage_reader,
                    invocation_id,
                    InvokeInputJournal::NoCachedJournal,
                    ism,
                )
                .await;
            } else {
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    "Not going to retry. Invocation state: {:?}",
                    ism.invocation_state_debug()
                );
                // Not ready for retrying yet
                self.invocation_state_machine_manager.register_invocation(
                    partition,
                    invocation_id,
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
    use super::*;

    use std::future::{pending, ready};
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use bytes::Bytes;
    use restate_core::TaskKind;
    use restate_core::TestCoreEnv;
    use restate_invoker_api::mocks::EmptyStorageReader;
    use restate_types::arc_util::Constant;
    use restate_types::config::InvokerOptionsBuilder;
    use tempfile::tempdir;
    use test_log::test;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use restate_invoker_api::{entry_enricher, ServiceHandle};
    use restate_schema_api::deployment::mocks::MockDeploymentMetadataRegistry;
    use restate_test_util::{check, let_assert};
    use restate_types::identifiers::LeaderEpoch;
    use restate_types::journal::enriched::EnrichedEntryHeader;
    use restate_types::journal::raw::RawEntry;
    use restate_types::retries::RetryPolicy;

    use crate::invocation_task::InvocationTaskError;
    use crate::quota::InvokerConcurrencyQuota;

    // -- Mocks

    const MOCK_PARTITION: PartitionLeaderEpoch = (0, LeaderEpoch::INITIAL);

    impl<ITR, SR> ServiceInner<ITR, SR>
    where
        SR: JournalReader + StateReader + Clone + Send + Sync + 'static,
        <SR as JournalReader>::JournalStream: Unpin + Send + 'static,
        <SR as StateReader>::StateIter: Send,
    {
        #[allow(clippy::type_complexity)]
        fn mock(
            invocation_task_runner: ITR,
            concurrency_limit: Option<usize>,
        ) -> (
            mpsc::UnboundedSender<InputCommand<SR>>,
            mpsc::UnboundedSender<
                restate_futures_util::command::Command<
                    RangeInclusive<PartitionKey>,
                    Vec<InvocationStatusReport>,
                >,
            >,
            Self,
        ) {
            let (input_tx, input_rx) = mpsc::unbounded_channel();
            let (status_tx, status_rx) = mpsc::unbounded_channel();
            let (invocation_tasks_tx, invocation_tasks_rx) = mpsc::unbounded_channel();

            let service_inner = Self {
                input_rx,
                status_rx,
                invocation_tasks_tx,
                invocation_tasks_rx,
                invocation_task_runner,
                invocation_tasks: Default::default(),
                retry_timers: Default::default(),
                quota: InvokerConcurrencyQuota::new(concurrency_limit),
                status_store: Default::default(),
                invocation_state_machine_manager: Default::default(),
            };
            (input_tx, status_tx, service_inner)
        }

        fn register_mock_partition(&mut self, storage_reader: SR) -> mpsc::Receiver<Effect>
        where
            ITR: InvocationTaskRunner<SR>,
        {
            let (partition_tx, partition_rx) = mpsc::channel(1024);
            self.handle_register_partition(
                MOCK_PARTITION,
                RangeInclusive::new(0, 0),
                storage_reader,
                partition_tx,
            );
            partition_rx
        }
    }

    impl<SR, F, Fut> InvocationTaskRunner<SR> for F
    where
        F: Fn(
            PartitionLeaderEpoch,
            InvocationId,
            InvocationTarget,
            SR,
            mpsc::UnboundedSender<InvocationTaskOutput>,
            mpsc::UnboundedReceiver<Notification>,
            InvokeInputJournal,
        ) -> Fut,
        SR: JournalReader + StateReader + Clone + Send + Sync + 'static,
        <SR as JournalReader>::JournalStream: Unpin + Send + 'static,
        <SR as StateReader>::StateIter: Send,
        Fut: Future<Output = ()> + Send + 'static,
    {
        fn start_invocation_task(
            &self,
            _options: &InvokerOptions,
            partition: PartitionLeaderEpoch,
            invocation_id: InvocationId,
            invocation_target: InvocationTarget,
            storage_reader: SR,
            invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
            invoker_rx: mpsc::UnboundedReceiver<Notification>,
            input_journal: InvokeInputJournal,
            task_pool: &mut JoinSet<()>,
        ) -> AbortHandle {
            task_pool.spawn((*self)(
                partition,
                invocation_id,
                invocation_target,
                storage_reader,
                invoker_tx,
                invoker_rx,
                input_journal,
            ))
        }
    }

    #[test(tokio::test)]
    async fn input_order_is_maintained() {
        let node_env = TestCoreEnv::create_with_mock_nodes_config(1, 1).await;
        let tc = node_env.tc;
        let invoker_options = InvokerOptionsBuilder::default()
            // fixed amount of retries so that an invocation eventually completes with a failure
            .retry_policy(RetryPolicy::fixed_delay(Duration::ZERO, Some(1)))
            .inactivity_timeout(Duration::ZERO.into())
            .abort_timeout(Duration::ZERO.into())
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap())
            .message_size_limit(None)
            .build()
            .unwrap();
        let service = Service::new(
            &invoker_options,
            // all invocations are unknown leading to immediate retries
            MockDeploymentMetadataRegistry::default(),
            ServiceClient::from_options(
                &ServiceClientOptions::default(),
                restate_service_client::AssumeRoleCacheMode::None,
            )
            .unwrap(),
            entry_enricher::mocks::MockEntryEnricher,
        );

        let mut handle = service.handle();

        let invoker_task_id = tc
            .spawn(
                TaskKind::SystemService,
                "invoker",
                None,
                service.run(Constant::new(invoker_options)),
            )
            .unwrap();

        let partition_leader_epoch = (0, LeaderEpoch::INITIAL);
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::generate(&invocation_target);

        let (output_tx, mut output_rx) = mpsc::channel(1);

        handle
            .register_partition(
                partition_leader_epoch,
                RangeInclusive::new(0, 0),
                EmptyStorageReader,
                output_tx,
            )
            .await
            .unwrap();
        handle
            .invoke(
                partition_leader_epoch,
                invocation_id,
                invocation_target,
                InvokeInputJournal::NoCachedJournal,
            )
            .await
            .unwrap();

        // If input order between 'register partition' and 'invoke' is not maintained, then it can happen
        // that 'invoke' arrives before 'register partition'. In this case, the invoker service will drop
        // the invocation and we won't see a result for the invocation (failure because the deployment cannot be resolved).
        check!(let Some(_) = output_rx.recv().await);

        tc.cancel_task(invoker_task_id).unwrap().await.unwrap();
    }

    #[test(tokio::test)]
    async fn quota_allows_one_concurrent_invocation() {
        let invoker_options = InvokerOptionsBuilder::default()
            // fixed amount of retries so that an invocation eventually completes with a failure
            .retry_policy(RetryPolicy::fixed_delay(Duration::ZERO, Some(1)))
            .inactivity_timeout(Duration::ZERO.into())
            .abort_timeout(Duration::ZERO.into())
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap())
            .message_size_limit(None)
            .build()
            .unwrap();

        let mut segment_queue = SegmentQueue::new(tempdir().unwrap().into_path(), 1024);
        let cancel_token = CancellationToken::new();
        let shutdown = cancel_token.cancelled();
        tokio::pin!(shutdown);

        let invocation_id_1 = InvocationId::mock_random();
        let invocation_id_2 = InvocationId::mock_random();

        let (_invoker_tx, _status_tx, mut service_inner) =
            ServiceInner::mock(|_, _, _, _, _, _, _| ready(()), Some(1));
        let _ = service_inner.register_mock_partition(EmptyStorageReader);

        // Enqueue sid_1 and sid_2
        segment_queue
            .enqueue(InvokeCommand {
                partition: MOCK_PARTITION,
                invocation_id: invocation_id_1,
                invocation_target: InvocationTarget::mock_virtual_object(),
                journal: InvokeInputJournal::NoCachedJournal,
            })
            .await;
        segment_queue
            .enqueue(InvokeCommand {
                partition: MOCK_PARTITION,
                invocation_id: invocation_id_2,
                invocation_target: InvocationTarget::mock_virtual_object(),
                journal: InvokeInputJournal::NoCachedJournal,
            })
            .await;

        // Now step the state machine to start the invocation
        assert!(
            service_inner
                .step(&invoker_options, &mut segment_queue, shutdown.as_mut())
                .await
        );

        // Check status and quota
        assert!(service_inner
            .status_store
            .resolve_invocation(MOCK_PARTITION, &invocation_id_1)
            .unwrap()
            .in_flight());
        assert!(!service_inner.quota.is_slot_available());

        // Step again to remove sid_1 from task queue. This should not invoke sid_2!
        assert!(
            service_inner
                .step(&invoker_options, &mut segment_queue, shutdown.as_mut())
                .await
        );
        assert!(service_inner
            .status_store
            .resolve_invocation(MOCK_PARTITION, &invocation_id_2)
            .is_none());
        assert!(!service_inner.quota.is_slot_available());

        // Send the close signal
        service_inner
            .handle_invocation_task_closed(MOCK_PARTITION, invocation_id_1)
            .await;

        // Slot should be available again
        assert!(service_inner.quota.is_slot_available());

        // Step now should invoke sid_2
        assert!(
            service_inner
                .step(&invoker_options, &mut segment_queue, shutdown.as_mut())
                .await
        );
        assert!(service_inner
            .status_store
            .resolve_invocation(MOCK_PARTITION, &invocation_id_1)
            .is_none());
        assert!(service_inner
            .status_store
            .resolve_invocation(MOCK_PARTITION, &invocation_id_2)
            .unwrap()
            .in_flight());
        assert!(!service_inner.quota.is_slot_available());
    }

    #[test(tokio::test)]
    async fn reclaim_quota_after_abort() {
        let invoker_options = InvokerOptionsBuilder::default()
            // fixed amount of retries so that an invocation eventually completes with a failure
            .retry_policy(RetryPolicy::fixed_delay(Duration::ZERO, Some(1)))
            .inactivity_timeout(Duration::ZERO.into())
            .abort_timeout(Duration::ZERO.into())
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap())
            .message_size_limit(None)
            .build()
            .unwrap();
        let invocation_id = InvocationId::mock_random();

        let (_, _status_tx, mut service_inner) = ServiceInner::mock(
            |partition,
             invocation_id,
             _service_id,
             _storage_reader,
             invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
             _,
             _| {
                let _ = invoker_tx.send(InvocationTaskOutput {
                    partition,
                    invocation_id,
                    inner: InvocationTaskOutputInner::NewEntry {
                        entry_index: 1,
                        entry: RawEntry::new(EnrichedEntryHeader::SetState {}, Bytes::default()),
                        requires_ack: false,
                    },
                });
                pending() // Never ends
            },
            Some(2),
        );
        let _ = service_inner.register_mock_partition(EmptyStorageReader);

        // Invoke the service
        service_inner
            .handle_invoke(
                &invoker_options,
                MOCK_PARTITION,
                invocation_id,
                InvocationTarget::mock_virtual_object(),
                InvokeInputJournal::NoCachedJournal,
            )
            .await;

        // We should receive the new entry here
        let invoker_effect = service_inner.invocation_tasks_rx.recv().await.unwrap();
        assert_eq!(invoker_effect.invocation_id, invocation_id);
        check!(let InvocationTaskOutputInner::NewEntry { .. } = invoker_effect.inner);

        // Check the quota
        let_assert!(InvokerConcurrencyQuota::Limited { available_slots } = &service_inner.quota);
        assert_eq!(*available_slots, 1);

        // Abort the invocation
        service_inner.handle_abort_invocation(MOCK_PARTITION, invocation_id);

        // Check the quota
        let_assert!(InvokerConcurrencyQuota::Limited { available_slots } = &service_inner.quota);
        assert_eq!(*available_slots, 2);

        // Handle error coming after the abort (this should be noop)
        service_inner
            .handle_invocation_task_failed(
                MOCK_PARTITION,
                invocation_id,
                InvocationTaskError::EmptySuspensionMessage, /* any error is fine */
            )
            .await;

        // Check the quota, should not be changed
        let_assert!(InvokerConcurrencyQuota::Limited { available_slots } = &service_inner.quota);
        assert_eq!(*available_slots, 2);
    }
}
