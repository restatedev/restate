// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod error;
mod input_command;
mod invocation_state_machine;
mod invocation_task;
mod metric_definitions;
mod quota;
mod state_machine_manager;
mod status_store;

use input_command::{InputCommand, InvokeCommand};
use invocation_state_machine::InvocationStateMachine;
use invocation_task::InvocationTask;
use invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
use metrics::counter;
use restate_core::cancellation_watcher;
use restate_errors::warn_it;
use restate_invoker_api::{
    Effect, EffectKind, EntryEnricher, InvocationErrorReport, InvocationStatusReport,
    InvokeInputJournal,
};
use restate_queue::SegmentQueue;
use restate_timer_queue::TimerQueue;
use restate_types::config::{InvokerOptions, ServiceClientOptions};
use restate_types::identifiers::PartitionLeaderEpoch;
use restate_types::identifiers::{DeploymentId, InvocationId, PartitionKey, WithPartitionKey};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{Completion, EntryIndex};
use restate_types::live::{Live, LiveLoad};
use restate_types::retries::RetryPolicy;
use restate_types::schema::deployment::DeploymentResolver;
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
use tracing::{debug, trace};
use tracing::{error, instrument};

use crate::error::SdkInvocationErrorV2;
use crate::metric_definitions::{
    INVOKER_ENQUEUE, INVOKER_INVOCATION_TASKS, TASK_OP_COMPLETED, TASK_OP_FAILED, TASK_OP_STARTED,
    TASK_OP_SUSPENDED,
};
use error::InvokerError;
pub use input_command::ChannelStatusReader;
pub use input_command::InvokerHandle;
use restate_invoker_api::invocation_reader::InvocationReader;
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_types::deployment::PinnedDeployment;
use restate_types::invocation::{InvocationEpoch, InvocationTarget};
use restate_types::journal_v2;
use restate_types::journal_v2::raw::{
    RawCommand, RawEntry, RawEntryHeader, RawEvent, RawNotification,
};
use restate_types::journal_v2::{
    CommandIndex, EntryMetadata, Event, NotificationId, TransientErrorEvent,
};
use restate_types::schema::invocation_target::InvocationTargetResolver;
use restate_types::schema::service::ServiceMetadataResolver;
use restate_types::service_protocol::ServiceProtocolVersion;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Notification {
    Completion(Completion),
    Entry(RawEntry),
    Ack(CommandIndex),
}

// -- InvocationTask factory: we use this to mock the state machine in tests

trait InvocationTaskRunner<SR> {
    #[allow(clippy::too_many_arguments)]
    fn start_invocation_task(
        &self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        invocation_target: InvocationTarget,
        retry_count_since_last_stored_entry: u32,
        storage_reader: SR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
        input_journal: InvokeInputJournal,
        task_pool: &mut JoinSet<()>,
    ) -> AbortHandle;
}

struct DefaultInvocationTaskRunner<EE, Schemas> {
    client: ServiceClient,
    entry_enricher: EE,
    schemas: Live<Schemas>,
}

impl<IR, EE, Schemas> InvocationTaskRunner<IR> for DefaultInvocationTaskRunner<EE, Schemas>
where
    IR: InvocationReader + Clone + Send + Sync + 'static,
    EE: EntryEnricher + Clone + Send + Sync + 'static,
    Schemas: DeploymentResolver
        + ServiceMetadataResolver
        + InvocationTargetResolver
        + Clone
        + Send
        + Sync
        + 'static,
{
    fn start_invocation_task(
        &self,
        opts: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        invocation_target: InvocationTarget,
        retry_count_since_last_stored_entry: u32,
        storage_reader: IR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
        input_journal: InvokeInputJournal,
        task_pool: &mut JoinSet<()>,
    ) -> AbortHandle {
        task_pool
            .build_task()
            .name("invocation-task")
            .spawn(
                InvocationTask::new(
                    self.client.clone(),
                    partition,
                    invocation_id,
                    invocation_epoch,
                    invocation_target,
                    opts.inactivity_timeout.into(),
                    opts.abort_timeout.into(),
                    opts.disable_eager_state,
                    opts.message_size_warning.get(),
                    opts.message_size_limit(),
                    retry_count_since_last_stored_entry,
                    storage_reader,
                    self.entry_enricher.clone(),
                    self.schemas.clone(),
                    invoker_tx,
                    invoker_rx,
                )
                .run(input_journal),
            )
            .expect("to spawn invocation task")
    }
}

// -- Service implementation
pub struct Service<IR, EntryEnricher, DeploymentRegistry> {
    // Used for constructing the invoker sender and status reader
    input_tx: mpsc::UnboundedSender<InputCommand<IR>>,
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
    inner: ServiceInner<DefaultInvocationTaskRunner<EntryEnricher, DeploymentRegistry>, IR>,
}

impl<IR, EE, Schemas> Service<IR, EE, Schemas> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        options: &InvokerOptions,
        deployment_metadata_resolver: Live<Schemas>,
        client: ServiceClient,
        entry_enricher: EE,
    ) -> Service<IR, EE, Schemas>
    where
        IR: InvocationReader + Clone + Send + Sync + 'static,
        EE: EntryEnricher,
        Schemas: DeploymentResolver + ServiceMetadataResolver,
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
                    schemas: deployment_metadata_resolver,
                },
                invocation_tasks: Default::default(),
                retry_timers: Default::default(),
                quota: quota::InvokerConcurrencyQuota::new(options.concurrent_invocations_limit()),
                status_store: Default::default(),
                invocation_state_machine_manager: Default::default(),
            },
        }
    }

    pub fn from_options(
        service_client_options: &ServiceClientOptions,
        invoker_options: &InvokerOptions,
        entry_enricher: EE,
        schemas: Live<Schemas>,
    ) -> Result<Service<IR, EE, Schemas>, BuildError>
    where
        IR: InvocationReader + Clone + Send + Sync + 'static,
        EE: EntryEnricher,
        Schemas: DeploymentResolver + ServiceMetadataResolver,
    {
        metric_definitions::describe_metrics();
        let client =
            ServiceClient::from_options(service_client_options, AssumeRoleCacheMode::Unbounded)?;

        Ok(Service::new(
            invoker_options,
            schemas,
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

impl<IR, EE, Schemas> Service<IR, EE, Schemas>
where
    IR: InvocationReader + Clone + Send + Sync + 'static,
    EE: EntryEnricher + Clone + Send + Sync + 'static,
    Schemas: DeploymentResolver
        + ServiceMetadataResolver
        + InvocationTargetResolver
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub fn handle(&self) -> InvokerHandle<IR> {
        InvokerHandle {
            input: self.input_tx.clone(),
        }
    }

    pub fn status_reader(&self) -> ChannelStatusReader {
        ChannelStatusReader(self.status_tx.clone())
    }

    pub async fn run(
        self,
        mut updateable_options: impl LiveLoad<Live = InvokerOptions>,
    ) -> anyhow::Result<()> {
        debug!("Starting the invoker");
        let Service {
            tmp_dir,
            inner: mut service,
            ..
        } = self;

        let shutdown = cancellation_watcher();
        tokio::pin!(shutdown);

        let in_memory_limit = updateable_options
            .live_load()
            .in_memory_queue_length_limit();
        // Prepare the segmented queue
        let mut segmented_input_queue = SegmentQueue::init(tmp_dir, in_memory_limit)
            .await
            .expect("Cannot initialize input spillable queue");

        loop {
            let options = updateable_options.live_load();
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
    retry_timers: TimerQueue<(PartitionLeaderEpoch, InvocationId, InvocationEpoch)>,
    quota: quota::InvokerConcurrencyQuota,
    status_store: InvocationStatusStore,
    invocation_state_machine_manager: state_machine_manager::InvocationStateMachineManager<SR>,
}

impl<ITR, IR> ServiceInner<ITR, IR>
where
    ITR: InvocationTaskRunner<IR>,
    IR: InvocationReader + Clone + Send + Sync + 'static,
{
    // Returns true if we should execute another step, false if we should stop executing steps
    async fn step<F>(
        &mut self,
        options: &InvokerOptions,
        segmented_input_queue: &mut SegmentQueue<Box<InvokeCommand>>,
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
                    InputCommand::Abort { partition, invocation_id, invocation_epoch } => {
                        self.handle_abort_invocation(partition, invocation_id,invocation_epoch);
                    }
                    InputCommand::AbortAllPartition { partition } => {
                        self.handle_abort_partition(partition);
                    }
                    InputCommand::Completion { partition, invocation_id, completion } => {
                        self.handle_completion(partition, invocation_id, completion);
                    },
                    InputCommand::Notification { partition, invocation_id, invocation_epoch, notification } => {
                        self.handle_notification(options, partition, invocation_id,invocation_epoch, notification);
                    },
                    InputCommand::StoredCommandAck { partition, invocation_id, invocation_epoch, command_index } => {
                        self.handle_stored_command_ack(options, partition, invocation_id,invocation_epoch, command_index);
                    }
                }
            },

            Some(invoke_input_command) = segmented_input_queue.dequeue(), if !segmented_input_queue.is_empty() && self.quota.is_slot_available() => {
                self.handle_invoke(options, invoke_input_command.partition, invoke_input_command.invocation_id, invoke_input_command.invocation_epoch, invoke_input_command.invocation_target, invoke_input_command.journal);
            },

            Some(invocation_task_msg) = self.invocation_tasks_rx.recv() => {
                let InvocationTaskOutput {
                    invocation_id,
                    partition,
                    invocation_epoch,
                    inner
                } = invocation_task_msg;
                match inner {
                    InvocationTaskOutputInner::PinnedDeployment(deployment_metadata, has_changed) => {
                        self.handle_pinned_deployment(
                            partition,
                            invocation_id,
                            invocation_epoch,
                            deployment_metadata,
                            has_changed,
                        )
                    }
                    InvocationTaskOutputInner::ServerHeaderReceived(x_restate_server_header) => {
                        self.handle_server_header_received(
                            partition,
                            invocation_id,
                            invocation_epoch,
                            x_restate_server_header
                        )
                    }
                    InvocationTaskOutputInner::NewEntry {entry_index, entry, requires_ack} => {
                        self.handle_new_entry(
                            partition,
                            invocation_id,
                            invocation_epoch,
                            entry_index,
                            entry,
                            requires_ack
                        ).await
                    },
                    InvocationTaskOutputInner::NewNotificationProposal { notification } => {
                        self.handle_new_notification_proposal(
                            partition,
                            invocation_id,
                            invocation_epoch,
                            notification
                        ).await
                    },
                    InvocationTaskOutputInner::Closed => {
                        self.handle_invocation_task_closed(partition, invocation_id, invocation_epoch).await
                    },
                    InvocationTaskOutputInner::Failed(e) => {
                        self.handle_invocation_task_failed(options, partition, invocation_id, invocation_epoch, e).await
                    },
                    InvocationTaskOutputInner::Suspended(indexes) => {
                        self.handle_invocation_task_suspended(partition, invocation_id, invocation_epoch, indexes).await
                    }
                    InvocationTaskOutputInner::NewCommand { command, command_index, requires_ack } => {
                        self.handle_new_command(
                            partition,
                            invocation_id,
                            invocation_epoch,
                            command_index,
                            command,
                            requires_ack
                        ).await
                    }
                    InvocationTaskOutputInner::SuspendedV2(notification_ids) => {
                        self.handle_invocation_task_suspended_v2(partition, invocation_id, invocation_epoch, notification_ids).await
                    }
                };
            },
            timer = self.retry_timers.await_timer() => {
                let (partition, fid, invocation_epoch) = timer.into_inner();
                self.handle_retry_timer_fired(options, partition, fid, invocation_epoch);
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
        storage_reader: IR,
        sender: mpsc::Sender<Box<Effect>>,
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
            restate.invocation.epoch = %invocation_epoch,
            restate.invocation.target = %invocation_target,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_invoke(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        invocation_target: InvocationTarget,
        journal: InvokeInputJournal,
    ) {
        if self
            .invocation_state_machine_manager
            .has_partition(partition)
        {
            if let Some((_, ism)) = self
                .invocation_state_machine_manager
                .resolve_invocation(partition, &invocation_id)
            {
                if invocation_epoch > ism.invocation_epoch {
                    // Important constraints to keep in mind here:
                    // * For a given epoch, an Abort command can overtake the Invoke, but not viceversa
                    // * Messages of an epoch can overtake messages of another epoch
                    //
                    // The following message ordering is possible:
                    //
                    // Abort(epoch 0) -> this aborts InvocationStateMachine epoch 0
                    // Abort(epoch 1) -> this gets ignored
                    // Invoke(epoch 1) -> this starts InvocationStateMachine epoch 1
                    // Invoke(epoch 2) -> THIS BRANCH
                    trace!(
                        "Got an invoke for an invocation that already exists, with a lower invocation epoch. \
                        Existing invocation epoch {} < new invocation epoch {}",
                        ism.invocation_epoch, invocation_epoch
                    );
                    let this_invocation_epoch = ism.invocation_epoch;
                    self.handle_abort_invocation(partition, invocation_id, this_invocation_epoch)
                } else {
                    panic!(
                        "Got an Invoke command with InvocationEpoch {} <= Invoker state InvocationEpoch {}, this is an unexpected logical/sync issue between PP and invoker!",
                        invocation_epoch, ism.invocation_epoch
                    );
                }
            }

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
                InvocationStateMachine::create(
                    invocation_target,
                    invocation_epoch,
                    options.retry_policy.clone(),
                ),
            )
        } else {
            trace!(
                "No registered partition {partition:?} was found for the invocation {invocation_id}"
            );
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_retry_timer_fired(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
    ) {
        trace!("Retry timeout fired");
        self.handle_retry_event(options, partition, invocation_id, invocation_epoch, |sm| {
            sm.notify_retry_timer_fired()
        });
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.command.index = command_index,
        )
    )]
    fn handle_stored_command_ack(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        command_index: CommandIndex,
    ) {
        trace!("Received a new stored command entry acknowledgement");
        self.handle_retry_event(options, partition, invocation_id, invocation_epoch, |sm| {
            sm.notify_stored_ack(command_index)
        });
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.deployment.id = %pinned_deployment.deployment_id,
        )
    )]
    fn handle_pinned_deployment(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        pinned_deployment: PinnedDeployment,
        has_changed: bool,
    ) {
        self.invocation_state_machine_manager.handle_for_invocation(
            partition,
            &invocation_id,
            invocation_epoch,
            |_, ism| {
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    "Pinned deployment '{:?}'. Invocation state: {:?}",
                    pinned_deployment,
                    ism.invocation_state_debug()
                );

                self.status_store.on_deployment_chosen(
                    &partition,
                    &invocation_id,
                    pinned_deployment.deployment_id,
                    pinned_deployment.service_protocol_version,
                );
                // If we think this selected deployment has been freshly picked, otherwise
                // we assume that we have stored it previously.
                if has_changed {
                    ism.notify_pinned_deployment(pinned_deployment);
                } else {
                    // The service protocol is selected only if this was the stored version already,
                    // or if we send the pinned deployment through (see handle_new_command)
                    ism.notify_selected_service_protocol(
                        pinned_deployment.service_protocol_version,
                    );
                }
            },
        );
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_server_header_received(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        x_restate_server_header: String,
    ) {
        self.invocation_state_machine_manager.handle_for_invocation(
            partition,
            &invocation_id,
            invocation_epoch,
            |_, ism| {
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
            },
        );
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.index = entry_index,
            restate.journal.entry_type = ?entry.ty(),
        )
    )]
    async fn handle_new_entry(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
        requires_ack: bool,
    ) {
        if let Some((output_tx, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
        {
            ism.notify_new_command(entry_index, requires_ack);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Received a new entry. Invocation state: {:?}",
                ism.invocation_state_debug()
            );
            if let Some(pinned_deployment) = ism.pinned_deployment_to_notify() {
                let _ = output_tx
                    .send(Box::new(Effect {
                        invocation_id,
                        invocation_epoch: ism.invocation_epoch,
                        kind: EffectKind::PinnedDeployment(pinned_deployment),
                    }))
                    .await;
            }
            let _ = output_tx
                .send(Box::new(Effect {
                    invocation_id,
                    invocation_epoch: ism.invocation_epoch,
                    kind: EffectKind::JournalEntry { entry_index, entry },
                }))
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
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.entry.ty = %notification.ty(),
            restate.journal.notification.id = ?notification.id(),
        )
    )]
    async fn handle_new_notification_proposal(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        notification: RawNotification,
    ) {
        if let Some((output_tx, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
        {
            ism.notify_new_notification_proposal(notification.id());
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Received a new notification. Invocation state: {:?}",
                ism.invocation_state_debug()
            );
            if let Some(pinned_deployment) = ism.pinned_deployment_to_notify() {
                let _ = output_tx
                    .send(Box::new(Effect {
                        invocation_id,
                        invocation_epoch: ism.invocation_epoch,
                        kind: EffectKind::PinnedDeployment(pinned_deployment),
                    }))
                    .await;
            }
            let _ = output_tx
                .send(Box::new(Effect {
                    invocation_id,
                    invocation_epoch: ism.invocation_epoch,
                    kind: EffectKind::JournalEntryV2 {
                        command_index_to_ack: None,
                        entry: RawEntry::new(RawEntryHeader::new(), notification),
                    },
                }))
                .await;
        } else {
            // If no state machine, this might be an entry for an aborted invocation.
            trace!("No state machine found for given notification");
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.command.index = command_index,
            restate.journal.entry.ty = %command.ty(),
        )
    )]
    async fn handle_new_command(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        command_index: CommandIndex,
        command: RawCommand,
        requires_ack: bool,
    ) {
        if let Some((output_tx, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
        {
            ism.notify_new_command(command_index, requires_ack);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Received a new command. Invocation state: {:?}",
                ism.invocation_state_debug()
            );
            if let Some(pinned_deployment) = ism.pinned_deployment_to_notify() {
                let _ = output_tx
                    .send(Box::new(Effect {
                        invocation_id,
                        invocation_epoch: ism.invocation_epoch,
                        kind: EffectKind::PinnedDeployment(pinned_deployment),
                    }))
                    .await;
            }
            let _ = output_tx
                .send(Box::new(Effect {
                    invocation_id,
                    invocation_epoch: ism.invocation_epoch,
                    kind: EffectKind::JournalEntryV2 {
                        command_index_to_ack: Some(command_index),
                        entry: RawEntry::new(RawEntryHeader::new(), command),
                    },
                }))
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
        self.invocation_state_machine_manager.handle_for_invocation(
            partition,
            &invocation_id,
            0,
            |_, ism| {
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    restate.journal.index = completion.entry_index,
                    "Notifying completion"
                );
                ism.notify_completion(completion);
            },
        );
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_notification(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        notification: RawNotification,
    ) {
        self.handle_retry_event(options, partition, invocation_id, invocation_epoch, |ism| {
            trace!(
                restate.invocation.target = %ism.invocation_target,
                restate.journal.ty = %notification.ty(),
                "Sending entry"
            );

            ism.notify_entry(RawEntry::new(RawEntryHeader::default(), notification));
        });
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_closed(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
    ) {
        if let Some((sender, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
        {
            debug_assert_eq!(invocation_epoch, ism.invocation_epoch);
            counter!(INVOKER_INVOCATION_TASKS, "status" => TASK_OP_COMPLETED).increment(1);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Invocation task closed correctly");
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);
            let _ = sender
                .send(Box::new(Effect {
                    invocation_id,
                    invocation_epoch,
                    kind: EffectKind::End,
                }))
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
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_suspended(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        entry_indexes: HashSet<EntryIndex>,
    ) {
        if let Some((sender, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
        {
            debug_assert_eq!(invocation_epoch, ism.invocation_epoch);
            counter!(INVOKER_INVOCATION_TASKS, "status" => TASK_OP_SUSPENDED).increment(1);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Suspending invocation");
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);
            let _ = sender
                .send(Box::new(Effect {
                    invocation_id,
                    invocation_epoch,
                    kind: EffectKind::Suspended {
                        waiting_for_completed_entries: entry_indexes,
                    },
                }))
                .await;
        } else {
            // If no state machine, this might be a result for an aborted invocation.
            trace!("No state machine found for invocation task suspended signal");
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_suspended_v2(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        waiting_for_notifications: HashSet<NotificationId>,
    ) {
        if let Some((sender, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
        {
            debug_assert_eq!(invocation_epoch, ism.invocation_epoch);
            counter!(INVOKER_INVOCATION_TASKS, "status" => TASK_OP_SUSPENDED).increment(1);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Suspending invocation"
            );
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);
            let _ = sender
                .send(Box::new(Effect {
                    invocation_id,
                    invocation_epoch: ism.invocation_epoch,
                    kind: EffectKind::SuspendedV2 {
                        waiting_for_notifications,
                    },
                }))
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
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    async fn handle_invocation_task_failed(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        error: InvokerError,
    ) {
        if let Some((_, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
        {
            debug_assert_eq!(invocation_epoch, ism.invocation_epoch);
            self.handle_error_event(options, partition, invocation_id, error, ism)
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
            restate.invocation.epoch = %invocation_epoch,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_abort_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
    ) {
        if let Some((_, _, mut ism)) = self
            .invocation_state_machine_manager
            .remove_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
        {
            // We abort only if the requested abort invocation epoch is same.
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Aborting invocation"
            );
            ism.abort();
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);
        } else {
            trace!(
                "Ignoring Abort command because there is no matching partition/invocation/invocation epoch"
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
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        error: InvokerError,
        mut ism: InvocationStateMachine,
    ) {
        match ism.handle_task_error(
            error.next_retry_interval_override(),
            error.should_bump_start_message_retry_count_since_last_stored_entry(),
        ) {
            Some(next_retry_timer_duration) if error.is_transient() => {
                counter!(INVOKER_INVOCATION_TASKS,
                    "status" => TASK_OP_FAILED,
                    "transient" => "true"
                )
                .increment(1);
                if let Some(error_stacktrace) = error.error_stacktrace() {
                    // The error details is treated differently from the pretty printer,
                    // makes sure it prints at the end of the log the spammy exception
                    warn_it!(
                        error,
                        restate.invocation.id = %invocation_id,
                        restate.invocation.target = %ism.invocation_target,
                        restate.invocation.error.stacktrace = %error_stacktrace,
                        "Invocation error, retrying in {}.",
                        humantime::format_duration(next_retry_timer_duration));
                } else {
                    warn_it!(
                        error,
                        restate.invocation.id = %invocation_id,
                        restate.invocation.target = %ism.invocation_target,
                        "Invocation error, retrying in {}.",
                        humantime::format_duration(next_retry_timer_duration));
                }
                trace!("Invocation state: {:?}.", ism.invocation_state_debug());
                let next_retry_at = SystemTime::now() + next_retry_timer_duration;

                let journal_v2_related_command_type =
                    if let InvokerError::SdkV2(SdkInvocationErrorV2 {
                        related_command: Some(ref related_entry),
                        ..
                    }) = error
                    {
                        related_entry
                            .related_entry_type
                            .and_then(|e| e.try_as_command_ref().copied())
                    } else {
                        None
                    };
                let invocation_error_report = error.into_invocation_error_report();
                if options.experimental_features_propose_events()
                    && ism
                        .selected_service_protocol()
                        .is_some_and(|sp| sp >= ServiceProtocolVersion::V4)
                {
                    // Only if protocol version >= 4 was selected we can propose the transient error
                    let transient_error_event = TransientErrorEvent {
                        error_code: invocation_error_report.err.code(),
                        error_message: invocation_error_report.err.message().to_owned(),
                        // Note from the review:
                        //  The stacktrace might be very long, but trimming it is not a piece of cake.
                        //  That's because some languages (Python!) have the stacktrace in reverse,
                        //  so it's hard here to decide whether to just drop the suffix or the prefix.
                        error_stacktrace: invocation_error_report
                            .err
                            .stacktrace()
                            .map(|s| s.to_owned()),
                        restate_doc_error_code: invocation_error_report
                            .doc_error_code
                            .map(|c| c.code().to_owned()),
                        related_command_index: invocation_error_report.related_entry_index,
                        related_command_name: invocation_error_report.related_entry_name.clone(),
                        related_command_type: journal_v2_related_command_type,
                    };
                    // Modify or remove this to disable the deduplication logic!
                    let deduplication_hash = transient_error_event.deduplication_hash();

                    let mut raw_event =
                        RawEvent::from(Event::TransientError(transient_error_event));
                    raw_event.set_deduplication_hash(deduplication_hash);

                    let _ = self
                        .invocation_state_machine_manager
                        .resolve_partition_sender(partition)
                        .expect("Partition should be registered")
                        .send(Box::new(Effect {
                            invocation_id,
                            invocation_epoch: ism.invocation_epoch,
                            kind: EffectKind::JournalEntryV2 {
                                entry: RawEntry::new(RawEntryHeader::new(), raw_event),
                                command_index_to_ack: None,
                            },
                        }))
                        .await;
                }

                self.status_store.on_failure(
                    partition,
                    invocation_id,
                    invocation_error_report,
                    Some(next_retry_at),
                );
                let epoch = ism.invocation_epoch;
                self.invocation_state_machine_manager.register_invocation(
                    partition,
                    invocation_id,
                    ism,
                );
                self.retry_timers
                    .sleep_until(next_retry_at, (partition, invocation_id, epoch));
            }
            _ => {
                counter!(INVOKER_INVOCATION_TASKS,
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
                    .send(Box::new(Effect {
                        invocation_id,
                        invocation_epoch: ism.invocation_epoch,
                        kind: EffectKind::Failed(error.into_invocation_error()),
                    }))
                    .await;
            }
        }
    }

    fn start_invocation_task(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        storage_reader: IR,
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
            ism.invocation_epoch,
            ism.invocation_target.clone(),
            ism.start_message_retry_count_since_last_stored_command,
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
        counter!(INVOKER_INVOCATION_TASKS, "status" => TASK_OP_STARTED).increment(1);
        self.invocation_state_machine_manager
            .register_invocation(partition, invocation_id, ism);
    }

    fn handle_retry_event<FN>(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
        f: FN,
    ) where
        FN: FnOnce(&mut InvocationStateMachine),
    {
        if let Some((_, storage_reader, mut ism)) = self
            .invocation_state_machine_manager
            .remove_invocation_with_epoch(partition, &invocation_id, invocation_epoch)
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
                );
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use googletest::prelude::eq;
    use googletest::{assert_that, pat};
    use serde_json::Value;
    use tempfile::tempdir;
    use test_log::test;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use restate_core::{TaskCenter, TaskKind};
    use restate_invoker_api::InvokerHandle;
    use restate_invoker_api::entry_enricher;
    use restate_invoker_api::test_util::EmptyStorageReader;
    use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
    use restate_test_util::{check, let_assert};
    use restate_types::config::InvokerOptionsBuilder;
    use restate_types::identifiers::{LeaderEpoch, PartitionId, ServiceRevision};
    use restate_types::invocation::ServiceType;
    use restate_types::journal::enriched::EnrichedEntryHeader;
    use restate_types::journal::raw::RawEntry;
    use restate_types::journal_v2::{Command, Encoder, Entry, OutputCommand, OutputResult};
    use restate_types::journal_v2::{CompletionType, NotificationType};
    use restate_types::live::Constant;
    use restate_types::retries::RetryPolicy;
    use restate_types::schema::deployment::Deployment;
    use restate_types::schema::invocation_target::InvocationTargetMetadata;
    use restate_types::schema::service::{InvocationAttemptOptions, ServiceMetadata};

    use crate::error::{InvokerError, SdkInvocationErrorV2};
    use crate::quota::InvokerConcurrencyQuota;

    // -- Mocks

    const MOCK_PARTITION: PartitionLeaderEpoch = (PartitionId::MIN, LeaderEpoch::INITIAL);

    impl<ITR, IR> ServiceInner<ITR, IR>
    where
        IR: InvocationReader + Clone + Send + Sync + 'static,
    {
        #[allow(clippy::type_complexity)]
        fn mock(
            invocation_task_runner: ITR,
            concurrency_limit: Option<usize>,
        ) -> (
            mpsc::UnboundedSender<InputCommand<IR>>,
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

        fn register_mock_partition(&mut self, storage_reader: IR) -> mpsc::Receiver<Box<Effect>>
        where
            ITR: InvocationTaskRunner<IR>,
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

    impl<IR, F, Fut> InvocationTaskRunner<IR> for F
    where
        F: Fn(
            PartitionLeaderEpoch,
            InvocationId,
            InvocationTarget,
            IR,
            mpsc::UnboundedSender<InvocationTaskOutput>,
            mpsc::UnboundedReceiver<Notification>,
            InvokeInputJournal,
        ) -> Fut,
        IR: InvocationReader + Clone + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        fn start_invocation_task(
            &self,
            _options: &InvokerOptions,
            partition: PartitionLeaderEpoch,
            invocation_id: InvocationId,
            _invocation_epoch: InvocationEpoch,
            invocation_target: InvocationTarget,
            _retry_count_since_last_stored_entry: u32,
            storage_reader: IR,
            invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
            invoker_rx: mpsc::UnboundedReceiver<Notification>,
            input_journal: InvokeInputJournal,
            task_pool: &mut JoinSet<()>,
        ) -> AbortHandle {
            task_pool
                .build_task()
                .name("invocation-task-fn")
                .spawn((*self)(
                    partition,
                    invocation_id,
                    invocation_target,
                    storage_reader,
                    invoker_tx,
                    invoker_rx,
                    input_journal,
                ))
                .expect("to spawn invocation task")
        }
    }

    // Just pending
    impl<SR> InvocationTaskRunner<SR> for ()
    where
        SR: InvocationReader + Clone + Send + Sync + 'static,
    {
        fn start_invocation_task(
            &self,
            _options: &InvokerOptions,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _invocation_epoch: InvocationEpoch,
            _invocation_target: InvocationTarget,
            _retry_count_since_last_stored_entry: u32,
            _storage_reader: SR,
            _invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
            _invoker_rx: mpsc::UnboundedReceiver<Notification>,
            _input_journal: InvokeInputJournal,
            task_pool: &mut JoinSet<()>,
        ) -> AbortHandle {
            task_pool.spawn(pending())
        }
    }

    impl<SR> InvocationTaskRunner<SR> for Arc<AtomicUsize>
    where
        SR: InvocationReader + Clone + Send + Sync + 'static,
    {
        fn start_invocation_task(
            &self,
            _options: &InvokerOptions,
            _partition: PartitionLeaderEpoch,
            _invocation_id: InvocationId,
            _invocation_epoch: InvocationEpoch,
            _invocation_target: InvocationTarget,
            _retry_count_since_last_stored_entry: u32,
            _storage_reader: SR,
            _invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
            _invoker_rx: mpsc::UnboundedReceiver<Notification>,
            _input_journal: InvokeInputJournal,
            task_pool: &mut JoinSet<()>,
        ) -> AbortHandle {
            self.fetch_add(1, Ordering::SeqCst);
            task_pool.spawn(pending())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockSchemas;

    impl ServiceMetadataResolver for MockSchemas {
        fn resolve_latest_service(&self, _: impl AsRef<str>) -> Option<ServiceMetadata> {
            None
        }

        fn resolve_invocation_attempt_options(
            &self,
            _: &DeploymentId,
            _: impl AsRef<str>,
            _: impl AsRef<str>,
        ) -> Option<InvocationAttemptOptions> {
            None
        }

        fn resolve_latest_service_openapi(&self, _: impl AsRef<str>) -> Option<Value> {
            None
        }

        fn resolve_latest_service_type(&self, _: impl AsRef<str>) -> Option<ServiceType> {
            None
        }

        fn list_services(&self) -> Vec<ServiceMetadata> {
            vec![]
        }
    }

    impl DeploymentResolver for MockSchemas {
        fn resolve_latest_deployment_for_service(&self, _: impl AsRef<str>) -> Option<Deployment> {
            None
        }

        fn get_deployment(&self, _: &DeploymentId) -> Option<Deployment> {
            None
        }

        fn get_deployment_and_services(
            &self,
            _: &DeploymentId,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
            None
        }

        fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
            vec![]
        }
    }

    impl InvocationTargetResolver for MockSchemas {
        fn resolve_latest_invocation_target(
            &self,
            _service_name: impl AsRef<str>,
            _handler_name: impl AsRef<str>,
        ) -> Option<InvocationTargetMetadata> {
            None
        }
    }

    #[test(restate_core::test)]
    async fn input_order_is_maintained() {
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
            Live::from_value(MockSchemas),
            ServiceClient::from_options(
                &ServiceClientOptions::default(),
                AssumeRoleCacheMode::None,
            )
            .unwrap(),
            entry_enricher::test_util::MockEntryEnricher,
        );

        let mut handle = service.handle();

        let invoker_task_id = TaskCenter::spawn(
            TaskKind::SystemService,
            "invoker",
            service.run(Constant::new(invoker_options)),
        )
        .unwrap();

        let partition_leader_epoch = (PartitionId::from(0), LeaderEpoch::INITIAL);
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);

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
                0,
                invocation_target,
                InvokeInputJournal::NoCachedJournal,
            )
            .await
            .unwrap();

        // If input order between 'register partition' and 'invoke' is not maintained, then it can happen
        // that 'invoke' arrives before 'register partition'. In this case, the invoker service will drop
        // the invocation and we won't see a result for the invocation (failure because the deployment cannot be resolved).
        check!(let Some(_) = output_rx.recv().await);

        TaskCenter::cancel_task(invoker_task_id)
            .unwrap()
            .await
            .unwrap();
    }

    #[test(restate_core::test)]
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

        let mut segment_queue = SegmentQueue::new(tempdir().unwrap().keep(), 1024);
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
            .enqueue(Box::new(InvokeCommand {
                partition: MOCK_PARTITION,
                invocation_id: invocation_id_1,
                invocation_epoch: 0,
                invocation_target: InvocationTarget::mock_virtual_object(),
                journal: InvokeInputJournal::NoCachedJournal,
            }))
            .await;
        segment_queue
            .enqueue(Box::new(InvokeCommand {
                partition: MOCK_PARTITION,
                invocation_id: invocation_id_2,
                invocation_epoch: 0,
                invocation_target: InvocationTarget::mock_virtual_object(),
                journal: InvokeInputJournal::NoCachedJournal,
            }))
            .await;

        // Now step the state machine to start the invocation
        assert!(
            service_inner
                .step(&invoker_options, &mut segment_queue, shutdown.as_mut())
                .await
        );

        // Check status and quota
        assert!(
            service_inner
                .status_store
                .resolve_invocation(MOCK_PARTITION, &invocation_id_1)
                .unwrap()
                .in_flight()
        );
        assert!(!service_inner.quota.is_slot_available());

        // Step again to remove sid_1 from task queue. This should not invoke sid_2!
        assert!(
            service_inner
                .step(&invoker_options, &mut segment_queue, shutdown.as_mut())
                .await
        );
        assert!(
            service_inner
                .status_store
                .resolve_invocation(MOCK_PARTITION, &invocation_id_2)
                .is_none()
        );
        assert!(!service_inner.quota.is_slot_available());

        // Send the close signal
        service_inner
            .handle_invocation_task_closed(MOCK_PARTITION, invocation_id_1, 0)
            .await;

        // Slot should be available again
        assert!(service_inner.quota.is_slot_available());

        // Step now should invoke sid_2
        assert!(
            service_inner
                .step(&invoker_options, &mut segment_queue, shutdown.as_mut())
                .await
        );
        assert!(
            service_inner
                .status_store
                .resolve_invocation(MOCK_PARTITION, &invocation_id_1)
                .is_none()
        );
        assert!(
            service_inner
                .status_store
                .resolve_invocation(MOCK_PARTITION, &invocation_id_2)
                .unwrap()
                .in_flight()
        );
        assert!(!service_inner.quota.is_slot_available());
    }

    #[test(restate_core::test)]
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
                    invocation_epoch: 0,
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
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            0,
            InvocationTarget::mock_virtual_object(),
            InvokeInputJournal::NoCachedJournal,
        );

        // We should receive the new entry here
        let invoker_effect = service_inner.invocation_tasks_rx.recv().await.unwrap();
        assert_eq!(invoker_effect.invocation_id, invocation_id);
        check!(let InvocationTaskOutputInner::NewEntry { .. } = invoker_effect.inner);

        // Check the quota
        let_assert!(InvokerConcurrencyQuota::Limited { available_slots } = &service_inner.quota);
        assert_eq!(*available_slots, 1);

        // Abort the invocation
        service_inner.handle_abort_invocation(MOCK_PARTITION, invocation_id, 0);

        // Check the quota
        let_assert!(InvokerConcurrencyQuota::Limited { available_slots } = &service_inner.quota);
        assert_eq!(*available_slots, 2);

        // Handle error coming after the abort (this should be noop)
        service_inner
            .handle_invocation_task_failed(
                &invoker_options,
                MOCK_PARTITION,
                invocation_id,
                0,
                InvokerError::EmptySuspensionMessage, /* any error is fine */
            )
            .await;

        // Check the quota, should not be changed
        let_assert!(InvokerConcurrencyQuota::Limited { available_slots } = &service_inner.quota);
        assert_eq!(*available_slots, 2);
    }

    #[test(restate_core::test)]
    async fn abort_doesnt_get_applied_with_old_epoch() {
        let invocation_id = InvocationId::mock_random();

        let (_, _status_tx, mut service_inner) = ServiceInner::mock((), None);
        let _ = service_inner.register_mock_partition(EmptyStorageReader);

        // Invoke the service with epoch one
        service_inner.handle_invoke(
            &InvokerOptions::default(),
            MOCK_PARTITION,
            invocation_id,
            1,
            InvocationTarget::mock_virtual_object(),
            InvokeInputJournal::NoCachedJournal,
        );
        assert_eq!(
            service_inner
                .invocation_state_machine_manager
                .resolve_invocation(MOCK_PARTITION, &invocation_id)
                .unwrap()
                .1
                .invocation_epoch,
            1
        );

        // Now abort 0, this should have no effect
        service_inner.handle_abort_invocation(MOCK_PARTITION, invocation_id, 0);
        assert_eq!(
            service_inner
                .invocation_state_machine_manager
                .resolve_invocation(MOCK_PARTITION, &invocation_id)
                .unwrap()
                .1
                .invocation_epoch,
            1
        );

        // Also handle error on epoch 0 should have no effect
        service_inner
            .handle_invocation_task_failed(
                &InvokerOptions::default(),
                MOCK_PARTITION,
                invocation_id,
                0,
                InvokerError::SdkV2(SdkInvocationErrorV2::unknown()),
            )
            .await;
        assert_eq!(
            service_inner
                .invocation_state_machine_manager
                .resolve_invocation(MOCK_PARTITION, &invocation_id)
                .unwrap()
                .1
                .invocation_epoch,
            1
        );

        // Now abort 1, this should have effect
        service_inner.handle_abort_invocation(MOCK_PARTITION, invocation_id, 1);
        assert!(
            service_inner
                .invocation_state_machine_manager
                .resolve_invocation(MOCK_PARTITION, &invocation_id)
                .is_none()
        );
    }

    #[test(restate_core::test)]
    async fn invoke_then_new_invoke_then_old_abort() {
        let invocation_id = InvocationId::mock_random();
        let started_tasks_count = Arc::new(AtomicUsize::new(0));

        let (_, _status_tx, mut service_inner) =
            ServiceInner::mock(started_tasks_count.clone(), None);
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Invoke the service with epoch zero
        service_inner.handle_invoke(
            &InvokerOptions::default(),
            MOCK_PARTITION,
            invocation_id,
            0,
            InvocationTarget::mock_virtual_object(),
            InvokeInputJournal::NoCachedJournal,
        );
        assert_eq!(
            service_inner
                .invocation_state_machine_manager
                .resolve_invocation(MOCK_PARTITION, &invocation_id)
                .unwrap()
                .1
                .invocation_epoch,
            0
        );
        assert_eq!(started_tasks_count.load(Ordering::SeqCst), 1);

        // Now invoke the service with epoch one, this replaces the old task
        service_inner.handle_invoke(
            &InvokerOptions::default(),
            MOCK_PARTITION,
            invocation_id,
            1,
            InvocationTarget::mock_virtual_object(),
            InvokeInputJournal::NoCachedJournal,
        );
        assert_eq!(
            service_inner
                .invocation_state_machine_manager
                .resolve_invocation(MOCK_PARTITION, &invocation_id)
                .unwrap()
                .1
                .invocation_epoch,
            1
        );
        assert_eq!(started_tasks_count.load(Ordering::SeqCst), 2);

        // Also ignore stuff related to old invocations
        service_inner
            .handle_new_command(
                MOCK_PARTITION,
                invocation_id,
                0,
                1,
                ServiceProtocolV4Codec::encode_entry(Entry::Command(Command::Output(
                    OutputCommand {
                        result: OutputResult::Success(Bytes::default()),
                        name: Default::default(),
                    },
                )))
                .inner
                .try_as_command()
                .unwrap(),
                false,
            )
            .await;
        assert!(
            effects_rx.try_recv().is_err(),
            "No effect should have been created so far"
        );

        // But commands with epoch 1 should be propagated
        service_inner
            .handle_new_command(
                MOCK_PARTITION,
                invocation_id,
                1,
                1,
                ServiceProtocolV4Codec::encode_entry(Entry::Command(Command::Output(
                    OutputCommand {
                        result: OutputResult::Success(Bytes::default()),
                        name: Default::default(),
                    },
                )))
                .inner
                .try_as_command()
                .unwrap(),
                false,
            )
            .await;
        assert_that!(
            *effects_rx.try_recv().unwrap(),
            pat!(Effect {
                invocation_id: eq(invocation_id),
                invocation_epoch: eq(1),
                kind: pat!(EffectKind::JournalEntryV2 { .. })
            })
        );

        // Now abort 0, this should have no effect
        service_inner.handle_abort_invocation(MOCK_PARTITION, invocation_id, 0);
        assert_eq!(
            service_inner
                .invocation_state_machine_manager
                .resolve_invocation(MOCK_PARTITION, &invocation_id)
                .unwrap()
                .1
                .invocation_epoch,
            1
        );

        // Now abort 1, this should have effect
        service_inner.handle_abort_invocation(MOCK_PARTITION, invocation_id, 1);
        assert!(
            service_inner
                .invocation_state_machine_manager
                .resolve_invocation(MOCK_PARTITION, &invocation_id)
                .is_none()
        );
    }

    #[test(restate_core::test)]
    async fn notification_triggers_retry() {
        let invoker_options = InvokerOptionsBuilder::default()
            .retry_policy(RetryPolicy::fixed_delay(Duration::ZERO, Some(1)))
            .inactivity_timeout(Duration::ZERO.into())
            .abort_timeout(Duration::ZERO.into())
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap())
            .message_size_limit(None)
            .build()
            .unwrap();

        let invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_virtual_object();

        // Create a mock ServiceInner that tracks when an invocation task is started
        let (task_started_tx, mut task_started_rx) = mpsc::channel(1);
        let (_, _status_tx, mut service_inner) = ServiceInner::mock(
            move |partition,
                  invocation_id,
                  invocation_target,
                  _storage_reader,
                  _invoker_tx,
                  _invoker_rx,
                  _input_journal| {
                let task_started_tx = task_started_tx.clone();
                async move {
                    // Signal that the task has started
                    let _ = task_started_tx
                        .send((partition, invocation_id, invocation_target))
                        .await;
                    // Never end
                    pending::<()>().await
                }
            },
            None,
        );

        // Register a mock partition
        let _ = service_inner.register_mock_partition(EmptyStorageReader);

        // Create an invocation state machine
        let mut ism = InvocationStateMachine::create(
            invocation_target.clone(),
            0,
            invoker_options.retry_policy.clone(),
        );
        let (tx, _rx) = mpsc::unbounded_channel();
        ism.start(tokio::spawn(async {}).abort_handle(), tx);

        // Add a notification proposal
        ism.notify_new_notification_proposal(NotificationId::CompletionId(1));

        // Put the state machine in the WaitingRetry state
        ism.handle_task_error(None, true);

        // Register the invocation state machine
        service_inner
            .invocation_state_machine_manager
            .register_invocation(MOCK_PARTITION, invocation_id, ism);

        // Fire the retry timer
        service_inner.handle_retry_timer_fired(&invoker_options, MOCK_PARTITION, invocation_id, 0);

        // Create a notification
        let notification = RawNotification::new(
            NotificationType::Completion(CompletionType::Run),
            NotificationId::CompletionId(1),
            Bytes::default(),
        );

        // Send the notification
        service_inner.handle_notification(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            0,
            notification,
        );

        let (partition, id, target) =
            tokio::time::timeout(Duration::from_millis(100), task_started_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(partition, MOCK_PARTITION);
        assert_eq!(id, invocation_id);
        assert_eq!(target, invocation_target);
    }
}
