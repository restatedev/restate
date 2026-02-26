// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
mod invocation_memory;
mod invocation_state_machine;
mod invocation_task;
mod metric_definitions;
mod quota;
mod state_machine_manager;
mod status_store;

use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime};
use std::{cmp, panic};

use futures::StreamExt;
use gardal::futures::ThrottledStream;
use gardal::{PaddedAtomicSharedStorage, StreamExt as GardalStreamExt, TokioClock};
use metrics::counter;
use restate_futures_util::concurrency::Permit;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};
use tracing::instrument;
use tracing::{debug, trace, warn};

use restate_core::cancellation_token;
use restate_errors::warn_it;
use restate_invoker_api::capacity::{OUTBOUND_SEED_SIZE, SEED_SIZE, TokenBucket};
use restate_invoker_api::invocation_reader::InvocationReader;
use restate_invoker_api::{
    Effect, EffectKind, EntryEnricher, InvocationErrorReport, InvocationStatusReport,
    InvokerEffect, YieldReason,
};
use restate_memory::{InvocationMemory, LocalMemoryLease, MemoryLease, MemoryPool};
use restate_queue::SegmentQueue;
use restate_service_client::{AssumeRoleCacheMode, ServiceClient};
use restate_time_util::DurationExt;
use restate_types::config::{Configuration, InvokerOptions, ServiceClientOptions};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::{DeploymentId, InvocationId, PartitionKey, WithPartitionKey};
use restate_types::identifiers::{PartitionId, PartitionLeaderEpoch};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::EntryIndex;
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{Event, PausedEvent, TransientErrorEvent};
use restate_types::journal_v2::raw::{RawCommand, RawNotification};
use restate_types::journal_v2::{CommandIndex, EntryMetadata, NotificationId};
use restate_types::live::{Live, LiveLoad};
use restate_types::schema::deployment::DeploymentResolver;
use restate_types::schema::invocation_target::InvocationTargetResolver;
use tokio_util::time::DelayQueue;
use tokio_util::time::delay_queue::Key as RetryTimerKey;

use crate::error::InvokerError;
use crate::error::MemoryDirection;
use crate::error::SdkInvocationErrorV2;
use crate::input_command::{InputCommand, InvokeCommand};
use crate::invocation_state_machine::InvocationStateMachine;
use crate::invocation_state_machine::OnTaskError;
use crate::invocation_task::InvocationTask;
use crate::invocation_task::{InvocationTaskOutput, InvocationTaskOutputInner};
use crate::metric_definitions::{
    ID_LOOKUP, INVOKER_ENQUEUE, INVOKER_INVOCATION_TASKS, TASK_OP_COMPLETED, TASK_OP_FAILED,
    TASK_OP_STARTED, TASK_OP_SUSPENDED,
};
use crate::status_store::InvocationStatusStore;

pub use input_command::ChannelStatusReader;
pub use input_command::InvokerHandle;

use self::input_command::VQueueInvokeCommand;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Notification {
    /// V1 completion signal: just the entry index (data read from RocksDB on demand).
    Completion(EntryIndex),
    /// V2 notification signal: entry index.
    Entry(EntryIndex),
    /// V2 command ack: already signal-only.
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
        invocation_target: InvocationTarget,
        retry_count_since_last_stored_entry: u32,
        storage_reader: SR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
        task_pool: &mut JoinSet<()>,
        budget: InvocationMemory,
    ) -> AbortHandle;
}

struct DefaultInvocationTaskRunner<EE, Schemas> {
    client: ServiceClient,
    entry_enricher: EE,
    schemas: Live<Schemas>,
    action_token_bucket: Option<TokenBucket>,
}

impl<IR, EE, Schemas> InvocationTaskRunner<IR> for DefaultInvocationTaskRunner<EE, Schemas>
where
    IR: InvocationReader + Clone + Send + Sync + 'static,
    EE: EntryEnricher + Clone + Send + Sync + 'static,
    Schemas: DeploymentResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    fn start_invocation_task(
        &self,
        opts: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        retry_count_since_last_stored_entry: u32,
        storage_reader: IR,
        invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
        invoker_rx: mpsc::UnboundedReceiver<Notification>,
        task_pool: &mut JoinSet<()>,
        budget: InvocationMemory,
    ) -> AbortHandle {
        task_pool
            .build_task()
            .name("invocation-task")
            .spawn(
                InvocationTask::new(
                    self.client.clone(),
                    partition,
                    invocation_id,
                    invocation_target,
                    opts.inactivity_timeout.into(),
                    opts.abort_timeout.into(),
                    opts.disable_eager_state,
                    opts.message_size_warning.as_non_zero_usize(),
                    opts.message_size_limit(),
                    retry_count_since_last_stored_entry,
                    self.entry_enricher.clone(),
                    self.schemas.clone(),
                    invoker_tx,
                    invoker_rx,
                    self.action_token_bucket.clone(),
                )
                .run(storage_reader, budget),
            )
            .expect("to spawn invocation task")
    }
}

/// Currently the invoker id is mainly used with invoker specific
/// metrics (not related to a particular partition)
///
/// Right now it exactly matches the partition id since
/// we have one invoker per partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvokerId(u16);

impl From<PartitionId> for InvokerId {
    fn from(value: PartitionId) -> Self {
        Self(value.into())
    }
}

impl From<InvokerId> for PartitionId {
    fn from(value: InvokerId) -> PartitionId {
        PartitionId::from(value.0)
    }
}

impl From<u16> for InvokerId {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<InvokerId> for u16 {
    fn from(value: InvokerId) -> Self {
        value.0
    }
}

// -- Service implementation
pub struct Service<StorageReader, EntryEnricher, Schemas> {
    // Used for constructing the invoker sender and status reader
    input_tx: mpsc::UnboundedSender<InputCommand<StorageReader>>,
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
    inner:
        ServiceInner<DefaultInvocationTaskRunner<EntryEnricher, Schemas>, Schemas, StorageReader>,
    invocation_token_bucket: Option<TokenBucket>,
}

impl<StorageReader, TEntryEnricher, Schemas> Service<StorageReader, TEntryEnricher, Schemas> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        invoker_id: impl Into<InvokerId>,
        options: &InvokerOptions,
        schemas: Live<Schemas>,
        client: ServiceClient,
        entry_enricher: TEntryEnricher,
        invocation_token_bucket: Option<TokenBucket>,
        action_token_bucket: Option<TokenBucket>,
        memory_pool: MemoryPool,
    ) -> Service<StorageReader, TEntryEnricher, Schemas>
    where
        StorageReader: InvocationReader + Clone + Send + Sync + 'static,
        TEntryEnricher: EntryEnricher,
        Schemas: DeploymentResolver + InvocationTargetResolver + Clone,
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
                    schemas: Live::clone(&schemas),
                    action_token_bucket,
                },
                schemas,
                invocation_tasks: Default::default(),
                retry_timers: Default::default(),
                last_retry_timer_compact: Instant::now(),
                quota: quota::InvokerConcurrencyQuota::new(
                    invoker_id,
                    options.concurrent_invocations_limit(),
                ),
                status_store: Default::default(),
                invocation_state_machine_manager: Default::default(),
                memory_pool,
                pending_seed_lease: None,
            },
            invocation_token_bucket,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_options(
        invoker_id: impl Into<InvokerId>,
        service_client_options: &ServiceClientOptions,
        invoker_options: &InvokerOptions,
        entry_enricher: TEntryEnricher,
        schemas: Live<Schemas>,
        invocation_token_bucket: Option<TokenBucket>,
        action_token_bucket: Option<TokenBucket>,
        memory_pool: MemoryPool,
    ) -> Result<Service<StorageReader, TEntryEnricher, Schemas>, BuildError>
    where
        StorageReader: InvocationReader + Clone + Send + Sync + 'static,
        TEntryEnricher: EntryEnricher,
        Schemas: DeploymentResolver + InvocationTargetResolver + Clone,
    {
        metric_definitions::describe_metrics();
        let client =
            ServiceClient::from_options(service_client_options, AssumeRoleCacheMode::Unbounded)?;

        Ok(Service::new(
            invoker_id,
            invoker_options,
            schemas,
            client,
            entry_enricher,
            invocation_token_bucket,
            action_token_bucket,
            memory_pool,
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
    Schemas: DeploymentResolver + InvocationTargetResolver + Clone + Send + Sync + 'static,
{
    pub fn handle(&self) -> InvokerHandle<IR> {
        InvokerHandle {
            input: self.input_tx.clone(),
        }
    }

    pub fn status_reader(&self) -> ChannelStatusReader {
        ChannelStatusReader(self.status_tx.clone())
    }

    pub async fn run(self, mut updateable_options: impl LiveLoad<Live = InvokerOptions>) {
        debug!("Starting the invoker");
        let Service {
            tmp_dir,
            inner: mut service,
            invocation_token_bucket,
            ..
        } = self;

        let in_memory_limit = updateable_options
            .live_load()
            .in_memory_queue_length_limit();

        invocation_token_bucket.as_ref().inspect(|bucket| {
            debug!("Invocation throttling limit: {:?}", bucket.limit());
        });

        // Prepare the segmented queue
        let mut segmented_input_queue = match SegmentQueue::init(tmp_dir.clone(), in_memory_limit)
            .await
        {
            Ok(queue) => std::pin::pin!(queue.throttle(invocation_token_bucket)),
            Err(e) if e.kind() == ErrorKind::PermissionDenied => {
                warn!(
                    "Could not initialize the invoker spill queue, permission denied to write the directory '{}'\n\
                Make sure restate-server has permissions to write that directory, or change the spill queue directory with the config option 'worker.invoker.tmp_dir' or the env RESTATE_WORKER__INVOKER__TMP_DIR.\n{e}",
                    tmp_dir.display()
                );
                panic!("Could not initialize invoker spill queue: {e}");
            }
            Err(e) => {
                warn!(
                    "Could not initialize the invoker spill queue, error when trying to write directory '{}'\n\
                If the error persists, change the spill queue directory with the config option 'worker.invoker.tmp_dir' or the env RESTATE_WORKER__INVOKER__TMP_DIR.\n{e}",
                    tmp_dir.display()
                );
                panic!("Could not initialize invoker spill queue: {e}");
            }
        };

        loop {
            let options = updateable_options.live_load();
            if cancellation_token()
                .run_until_cancelled(service.step(options, segmented_input_queue.as_mut()))
                .await
                .is_none()
            {
                debug!("Shutting down the invoker");
                service.handle_shutdown();
                break;
            }
        }

        // Wait for all the tasks to shutdown
        service.invocation_tasks.shutdown().await;
    }
}

struct ServiceInner<InvocationTaskRunner, Schemas, StorageReader> {
    input_rx: mpsc::UnboundedReceiver<InputCommand<StorageReader>>,
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

    schemas: Live<Schemas>,

    // Invoker state machine
    invocation_tasks: JoinSet<()>,
    retry_timers: DelayQueue<(PartitionLeaderEpoch, InvocationId)>,
    last_retry_timer_compact: Instant,
    quota: quota::InvokerConcurrencyQuota,
    status_store: InvocationStatusStore,
    invocation_state_machine_manager:
        state_machine_manager::InvocationStateMachineManager<StorageReader>,

    // Global memory budget shared across all invocations on this node.
    memory_pool: MemoryPool,
    /// Pre-acquired seed lease for the next invocation from the segment queue.
    /// Acquired at the top of `step()` when the queue is non-empty, and consumed
    /// when the segment queue arm fires.
    pending_seed_lease: Option<MemoryLease>,
}

impl<ITR, Schemas, IR> ServiceInner<ITR, Schemas, IR>
where
    ITR: InvocationTaskRunner<IR>,
    IR: InvocationReader + Clone + Send + Sync + 'static,
    Schemas: InvocationTargetResolver,
{
    // Returns true if we should execute another step, false if we should stop executing steps
    async fn step(
        &mut self,
        options: &InvokerOptions,
        mut segmented_input_queue: Pin<
            &mut ThrottledStream<
                SegmentQueue<Box<InvokeCommand>>,
                PaddedAtomicSharedStorage,
                TokioClock,
            >,
        >,
    ) {
        // Pre-acquire a seed lease from the memory pool so the segment queue arm
        // can create a budget without blocking inside select!.
        if self.pending_seed_lease.is_none() && !segmented_input_queue.inner().is_empty() {
            self.pending_seed_lease = self.memory_pool.try_reserve(SEED_SIZE);
        }

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
                        counter!(INVOKER_ENQUEUE, "partition_id" => invoke_command.partition.0.to_string()).increment(1);
                        segmented_input_queue.inner_pin_mut().enqueue(invoke_command).await;
                    },
                    InputCommand::VQInvoke(command) => {
                        counter!(INVOKER_ENQUEUE, "status" => TASK_OP_COMPLETED, "partition_id" => ID_LOOKUP.get(command.partition.0)).increment(1);
                        self.handle_vqueue_invoke(options, *command);
                    },
                    // --- Other commands (they don't go through the segment queue)
                    InputCommand::RegisterPartition { partition, partition_key_range, storage_reader, sender, } => {
                        self.handle_register_partition(partition, partition_key_range,
                                storage_reader, sender);
                    },
                    InputCommand::Abort { partition, invocation_id } => {
                        self.handle_abort_invocation(partition, invocation_id);
                    }
                    InputCommand::RetryNow { partition, invocation_id } => {
                        self.handle_retry_now_invocation(options, partition, invocation_id);
                    }
                    InputCommand::Pause { partition, invocation_id } => {
                        self.handle_pause_invocation(partition, invocation_id);
                    }
                    InputCommand::AbortAllPartition { partition } => {
                        self.handle_abort_partition(partition);
                    }
                    InputCommand::Completion { partition, invocation_id, entry_index } => {
                        self.handle_completion(partition, invocation_id, entry_index);
                    },
                    InputCommand::Notification { partition, invocation_id, entry_index, notification_id } => {
                        self.handle_notification(options, partition, invocation_id, entry_index, notification_id);
                    },
                    InputCommand::StoredCommandAck { partition, invocation_id, command_index } => {
                        self.handle_stored_command_ack(options, partition, invocation_id, command_index);
                    }
                }
            },
            Some(invoke_input_command) = segmented_input_queue.next(), if !segmented_input_queue.inner().is_empty() && self.quota.is_slot_available() && self.pending_seed_lease.is_some() => {
                let mut seed = self.pending_seed_lease.take().unwrap();
                let outbound_seed = seed.split(OUTBOUND_SEED_SIZE);
                let budget = self.create_invocation_memory(options, seed, outbound_seed);
                self.handle_invoke(options, invoke_input_command.partition, invoke_input_command.invocation_id, invoke_input_command.invocation_target, budget);
            },
            memory_lease = self.memory_pool.reserve(SEED_SIZE), if !segmented_input_queue.inner().is_empty() && self.pending_seed_lease.is_none() => {
                self.pending_seed_lease = Some(memory_lease);
            }
            Some(invocation_task_msg) = self.invocation_tasks_rx.recv() => {
                let InvocationTaskOutput {
                    invocation_id,
                    partition,
                    inner,
                } = invocation_task_msg;
                match inner {
                    InvocationTaskOutputInner::PinnedDeployment(deployment_metadata, has_changed) => {
                        self.handle_pinned_deployment(
                            partition,
                            invocation_id,
                            deployment_metadata,
                            has_changed,
                        )
                    }
                    InvocationTaskOutputInner::ServerHeaderReceived(x_restate_server_header) => {
                        self.handle_server_header_received(
                            partition,
                            invocation_id,
                            x_restate_server_header
                        )
                    }
                    InvocationTaskOutputInner::NewEntry {entry_index, entry, requires_ack, inbound_lease } => {
                        self.handle_new_entry(
                            partition,
                            invocation_id,
                            entry_index,
                            *entry,
                            requires_ack,
                            inbound_lease,
                        )
                    },
                    InvocationTaskOutputInner::NewNotificationProposal { notification, inbound_lease } => {
                        self.handle_new_notification_proposal(
                            partition,
                            invocation_id,
                            notification,
                            inbound_lease,
                        )
                    },
                    InvocationTaskOutputInner::Closed => {
                        self.handle_invocation_task_closed(partition, invocation_id)
                    },
                    InvocationTaskOutputInner::Failed(e, returned_budget) => {
                        self.handle_invocation_task_failed(partition, invocation_id, e, returned_budget)
                    },
                    InvocationTaskOutputInner::Suspended(indexes) => {
                        self.handle_invocation_task_suspended(partition, invocation_id, indexes)
                    }
                    InvocationTaskOutputInner::NewCommand { command, command_index, requires_ack, inbound_lease } => {
                        self.handle_new_command(
                            partition,
                            invocation_id,
                            command_index,
                            command,
                            requires_ack,
                            inbound_lease,
                        )
                    }
                    InvocationTaskOutputInner::SuspendedV2(notification_ids) => {
                        self.handle_invocation_task_suspended_v2(partition, invocation_id, notification_ids)
                    }
                    InvocationTaskOutputInner::ShouldYield { inbound_needed, outbound_needed, budget } => {
                        self.handle_invocation_task_should_yield(partition, invocation_id, inbound_needed, outbound_needed, budget)
                    }
                };
            },
            Some(expired) = self.retry_timers.next() => {
                let timer_key = expired.key();
                let (partition, invocation_id) = expired.into_inner();
                self.handle_retry_timer_fired(options, partition, invocation_id, timer_key);
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
        }

        // Periodically compact the retry timers to reclaim memory
        if self.last_retry_timer_compact.elapsed() >= Duration::from_secs(10) {
            self.retry_timers.compact();
            self.last_retry_timer_compact = Instant::now();
        }
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
        sender: mpsc::UnboundedSender<InvokerEffect>,
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
            rpc.service = %command.invocation_target.service_name(),
            rpc.method = %command.invocation_target.handler_name(),
            restate.invocation.id = %command.invocation_id,
            restate.invocation.target = %command.invocation_target,
            restate.invoker.partition_leader_epoch = ?command.partition,
        )
    )]
    fn handle_vqueue_invoke(&mut self, options: &InvokerOptions, command: VQueueInvokeCommand) {
        if self
            .invocation_state_machine_manager
            .has_partition(command.partition)
        {
            let (retry_iter, on_max_attempts) =
                self.schemas.live_load().resolve_invocation_retry_policy(
                    None,
                    command.invocation_target.service_name(),
                    command.invocation_target.handler_name(),
                );

            let storage_reader = self
                .invocation_state_machine_manager
                .partition_storage_reader(command.partition)
                .expect("partition is registered");
            let budget =
                self.create_invocation_memory(options, command.inbound_seed, command.outbound_seed);
            self.start_invocation_task(
                options,
                command.partition,
                storage_reader.clone(),
                command.invocation_id,
                InvocationStateMachine::create(
                    Some(command.qid),
                    command.permit,
                    command.invocation_target,
                    retry_iter,
                    on_max_attempts,
                ),
                budget,
            )
        } else {
            trace!(
                "No registered partition {:?} was found for the invocation {}",
                command.partition, command.invocation_id
            );
        }
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
    fn handle_invoke(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        budget: InvocationMemory,
    ) {
        if self
            .invocation_state_machine_manager
            .has_partition(partition)
        {
            let (retry_iter, on_max_attempts) =
                self.schemas.live_load().resolve_invocation_retry_policy(
                    None,
                    invocation_target.service_name(),
                    invocation_target.handler_name(),
                );

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
                InvocationStateMachine::create(
                    None,
                    Permit::new_empty(),
                    invocation_target,
                    retry_iter,
                    on_max_attempts,
                ),
                budget,
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
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_retry_timer_fired(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        timer_key: RetryTimerKey,
    ) {
        trace!("Retry timeout fired");
        self.handle_retry_event(options, partition, invocation_id, |sm| {
            sm.notify_retry_timer_fired(timer_key)
        });
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.command.index = command_index,
        )
    )]
    fn handle_stored_command_ack(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        command_index: CommandIndex,
    ) {
        trace!("Received a new stored command entry acknowledgement");
        self.handle_retry_event(options, partition, invocation_id, |sm| {
            sm.notify_stored_ack(command_index)
        });
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
            restate.deployment.id = %pinned_deployment.deployment_id,
        )
    )]
    fn handle_pinned_deployment(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        pinned_deployment: PinnedDeployment,
        has_changed: bool,
    ) {
        self.invocation_state_machine_manager.handle_for_invocation(
            partition,
            &invocation_id,
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

                ism.update_retry_policy_if_needed(
                    pinned_deployment.deployment_id,
                    self.schemas.live_load(),
                );

                ism.notify_pinned_deployment(pinned_deployment, has_changed);
            },
        );
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_server_header_received(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        x_restate_server_header: String,
    ) {
        self.invocation_state_machine_manager.handle_for_invocation(
            partition,
            &invocation_id,
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
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.index = entry_index,
            restate.journal.entry_type = ?entry.ty(),
        )
    )]
    fn handle_new_entry(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        entry: EnrichedRawEntry,
        requires_ack: bool,
        inbound_lease: LocalMemoryLease,
    ) {
        if let Some((output_tx, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
        {
            ism.notify_new_command(entry_index, requires_ack);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Received a new entry. Invocation state: {:?}",
                ism.invocation_state_debug()
            );
            self.status_store
                .on_progress_made(&partition, &invocation_id);
            if let Some(pinned_deployment) = ism.pinned_deployment_to_notify() {
                let _ = output_tx.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::PinnedDeployment(pinned_deployment),
                    }),
                    inbound_lease: None,
                });
            }
            let _ = output_tx.send(InvokerEffect {
                effect: Box::new(Effect {
                    invocation_id,
                    kind: EffectKind::JournalEntry { entry_index, entry },
                }),
                inbound_lease: Some(inbound_lease),
            });
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
            restate.journal.entry.ty = %notification.ty(),
            restate.journal.notification.id = ?notification.id(),
        )
    )]
    fn handle_new_notification_proposal(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        notification: RawNotification,
        inbound_lease: LocalMemoryLease,
    ) {
        if let Some((output_tx, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
        {
            ism.notify_new_notification_proposal(notification.id());
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Received a new notification. Invocation state: {:?}",
                ism.invocation_state_debug()
            );
            self.status_store
                .on_progress_made(&partition, &invocation_id);
            if let Some(pinned_deployment) = ism.pinned_deployment_to_notify() {
                let _ = output_tx.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::PinnedDeployment(pinned_deployment),
                    }),
                    inbound_lease: None,
                });
            }
            let _ = output_tx.send(InvokerEffect {
                effect: Box::new(Effect {
                    invocation_id,
                    kind: EffectKind::journal_entry(notification, None),
                }),
                inbound_lease: Some(inbound_lease),
            });
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
            restate.invoker.partition_leader_epoch = ?partition,
            restate.journal.command.index = command_index,
            restate.journal.entry.ty = %command.ty(),
        )
    )]
    fn handle_new_command(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        command_index: CommandIndex,
        command: RawCommand,
        requires_ack: bool,
        inbound_lease: LocalMemoryLease,
    ) {
        if let Some((output_tx, ism)) = self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
        {
            ism.notify_new_command(command_index, requires_ack);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Received a new command. Invocation state: {:?}",
                ism.invocation_state_debug()
            );
            self.status_store
                .on_progress_made(&partition, &invocation_id);
            if let Some(pinned_deployment) = ism.pinned_deployment_to_notify() {
                let _ = output_tx.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::PinnedDeployment(pinned_deployment),
                    }),
                    inbound_lease: None,
                });
            }
            let _ = output_tx.send(InvokerEffect {
                effect: Box::new(Effect {
                    invocation_id,
                    kind: EffectKind::journal_entry(command, Some(command_index)),
                }),
                inbound_lease: Some(inbound_lease),
            });
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
        entry_index: EntryIndex,
    ) {
        self.invocation_state_machine_manager.handle_for_invocation(
            partition,
            &invocation_id,
            |_, ism| {
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    restate.journal.index = entry_index,
                    "Notifying completion"
                );
                ism.notify_completion(entry_index);
            },
        );
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_notification(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        notification_id: NotificationId,
    ) {
        self.handle_retry_event(options, partition, invocation_id, |ism| {
            trace!(
                restate.invocation.target = %ism.invocation_target,
                restate.journal.index = entry_index,
                "Sending notification signal"
            );

            ism.notify_entry(entry_index, notification_id);
        });
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_invocation_task_closed(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) {
        if let Some((sender, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            counter!(INVOKER_INVOCATION_TASKS, "status" => TASK_OP_COMPLETED, "partition_id" => ID_LOOKUP.get(partition.0)).increment(1);
            trace!(
                restate.invocation.target = %ism.invocation_target,
                "Invocation task closed correctly");
            if ism._permit.is_empty() {
                // the permit is empty when we are using a real permit token (vqueues).
                self.quota.unreserve_slot();
            }
            self.status_store.on_end(&partition, &invocation_id);
            let _ = sender.send(InvokerEffect {
                effect: Box::new(Effect {
                    invocation_id,
                    kind: EffectKind::End,
                }),
                inbound_lease: None,
            });
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
    fn handle_invocation_task_suspended(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        entry_indexes: HashSet<EntryIndex>,
    ) {
        if let Some((sender, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            counter!(INVOKER_INVOCATION_TASKS, "status" => TASK_OP_SUSPENDED, "partition_id" => ID_LOOKUP.get(partition.0)).increment(1);
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);

            if ism.requested_pause {
                // We should send pause instead
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    "Pausing invocation after suspension"
                );

                let _ = sender.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::Paused {
                            paused_event: RawEvent::from(Event::Paused(PausedEvent {
                                last_failure: None,
                            })),
                        },
                    }),
                    inbound_lease: None,
                });
            } else {
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    "Suspending invocation"
                );

                let _ = sender.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::Suspended {
                            waiting_for_completed_entries: entry_indexes,
                        },
                    }),
                    inbound_lease: None,
                });
            }
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
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_invocation_task_suspended_v2(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        waiting_for_notifications: HashSet<NotificationId>,
    ) {
        if let Some((sender, _, ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            counter!(INVOKER_INVOCATION_TASKS, "status" => TASK_OP_SUSPENDED, "partition_id" => ID_LOOKUP.get(partition.0))
                .increment(1);
            self.quota.unreserve_slot();
            self.status_store.on_end(&partition, &invocation_id);

            if ism.requested_pause {
                // We should send pause instead
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    "Pausing invocation after suspension"
                );

                let _ = sender.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::Paused {
                            paused_event: RawEvent::from(Event::Paused(PausedEvent {
                                last_failure: None,
                            })),
                        },
                    }),
                    inbound_lease: None,
                });
            } else {
                trace!(
                    restate.invocation.target = %ism.invocation_target,
                    "Suspending invocation"
                );

                let _ = sender.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::SuspendedV2 {
                            waiting_for_notifications,
                        },
                    }),
                    inbound_lease: None,
                });
            }
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
    fn handle_invocation_task_failed(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        error: InvokerError,
        returned_budget: InvocationMemory,
    ) {
        if let Some((_, _, mut ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            // Stash the budget on the ISM so it can be reused if we retry.
            ism.budget = Some(returned_budget);
            self.handle_error_event(partition, invocation_id, error, ism);
        } else {
            // If no state machine, this might be a result for an aborted invocation.
            trace!("No state machine found for invocation task error signal");
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
    fn handle_invocation_task_should_yield(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        inbound_needed: usize,
        outbound_needed: usize,
        budget: InvocationMemory,
    ) {
        if let Some((sender, _, mut ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            if Configuration::pinned()
                .common
                .experimental_enable_invoker_yield
            {
                debug!(
                    restate.invocation.target = %ism.invocation_target,
                    inbound_needed,
                    outbound_needed,
                    "Invocation yielding due to memory budget exhaustion"
                );
                ism.abort();
                if ism._permit.is_empty() {
                    self.quota.unreserve_slot();
                }
                self.status_store.on_end(&partition, &invocation_id);
                let _ = sender.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::Yield(YieldReason::OutOfMemory {
                            inbound_needed_memory: inbound_needed,
                            outbound_needed_memory: outbound_needed,
                        }),
                    }),
                    inbound_lease: None,
                });
            } else {
                // Flag disabled: fall back to retry.
                let error = if inbound_needed > budget.inbound.min_reserved() {
                    InvokerError::OutOfMemory {
                        needed: inbound_needed,
                        direction: MemoryDirection::Inbound,
                    }
                } else {
                    InvokerError::OutOfMemory {
                        needed: outbound_needed,
                        direction: MemoryDirection::Outbound,
                    }
                };
                ism.budget = Some(budget);
                self.handle_error_event(partition, invocation_id, error, ism);
            }
        } else {
            trace!("No state machine found for invocation task yield signal");
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
            // Cancel retry timer if ISM was in WaitingRetry state
            if let Some(timer_key) = ism.take_retry_timer_key() {
                self.retry_timers.try_remove(&timer_key);
            }

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
            restate.invocation.id = %invocation_id,
            restate.invoker.partition_leader_epoch = ?partition,
        )
    )]
    fn handle_retry_now_invocation(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) {
        // Get the timer key from the ISM if it's in WaitingRetry state
        let timer_key = self
            .invocation_state_machine_manager
            .resolve_invocation(partition, &invocation_id)
            .and_then(|(_, ism)| ism.take_retry_timer_key());

        if let Some(timer_key) = timer_key {
            // Cancel the pending timer from the queue
            self.retry_timers.try_remove(&timer_key);

            // Retry now is equivalent to immediately firing the retry timer
            self.handle_retry_timer_fired(options, partition, invocation_id, timer_key);
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
    fn handle_pause_invocation(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
    ) {
        if let Some((sender, _, mut ism)) = self
            .invocation_state_machine_manager
            .remove_invocation(partition, &invocation_id)
        {
            // Cancel retry timer if ISM was in WaitingRetry state
            if let Some(timer_key) = ism.take_retry_timer_key() {
                self.retry_timers.try_remove(&timer_key);
            }

            if ism.notify_pause() {
                // If returns true, we need to pause now
                let _ = sender.send(InvokerEffect {
                    effect: Box::new(Effect {
                        invocation_id,
                        kind: EffectKind::Paused {
                            paused_event: RawEvent::from(Event::Paused(PausedEvent {
                                last_failure: ism.last_transient_error_event,
                            })),
                        },
                    }),
                    inbound_lease: None,
                });
            } else {
                // Invocation still in flight, pause will happen later on
                self.invocation_state_machine_manager.register_invocation(
                    partition,
                    invocation_id,
                    ism,
                );
            }
        } else {
            // If no state machine, this might pause for an aborted invocation.
            trace!("No state machine found for pause");
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
                // Cancel retry timer if ISM was in WaitingRetry state
                if let Some(timer_key) = ism.take_retry_timer_key() {
                    self.retry_timers.try_remove(&timer_key);
                }

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

    fn handle_error_event(
        &mut self,
        partition: PartitionLeaderEpoch,
        invocation_id: InvocationId,
        error: InvokerError,
        mut ism: InvocationStateMachine,
    ) {
        let attempt_deployment_id = ism.attempt_deployment_id();

        // Call handle_task_error with a closure that registers the timer.
        // We need to capture the duration for logging and status updates.
        let result = ism.handle_task_error(
            error.is_transient(),
            error.next_retry_interval_override(),
            error.should_bump_start_message_retry_count_since_last_stored_entry(),
            |duration| {
                self.retry_timers
                    .insert((partition, invocation_id), duration)
            },
        );

        match result {
            OnTaskError::Retrying(next_retry_timer_duration) => {
                counter!(INVOKER_INVOCATION_TASKS,
                    "status" => TASK_OP_FAILED,
                    "transient" => "true",
                    "partition_id" => ID_LOOKUP.get(partition.0)
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
                        restate.deployment.id = %attempt_deployment_id,
                        "Invocation error, retrying in {}.",
                        next_retry_timer_duration.friendly());
                } else {
                    warn_it!(
                        error,
                        restate.invocation.id = %invocation_id,
                        restate.invocation.target = %ism.invocation_target,
                        restate.deployment.id = %attempt_deployment_id,
                        "Invocation error, retrying in {}.",
                        next_retry_timer_duration.friendly());
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
                let event = TransientErrorEvent {
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

                // Some trivial deduplication here: if we already sent this transient error in the previous retry, don't send it again
                if ism.should_emit_transient_error_event(&event) {
                    let _ = self
                        .invocation_state_machine_manager
                        .resolve_partition_sender(partition)
                        .expect("Partition should be registered")
                        .send(InvokerEffect {
                            effect: Box::new(Effect {
                                invocation_id,
                                kind: EffectKind::JournalEvent {
                                    event: RawEvent::from(Event::TransientError(event)),
                                },
                            }),
                            inbound_lease: None,
                        });
                }

                self.status_store.on_failure(
                    partition,
                    invocation_id,
                    invocation_error_report,
                    Some(next_retry_at),
                );

                // Timer was already registered inside handle_task_error via the closure
                self.invocation_state_machine_manager.register_invocation(
                    partition,
                    invocation_id,
                    ism,
                );
            }
            OnTaskError::Pause => {
                counter!(INVOKER_INVOCATION_TASKS,
                    "status" => TASK_OP_FAILED,
                    "transient" => "false",
                    "partition_id" => ID_LOOKUP.get(partition.0)
                )
                .increment(1);
                warn_it!(
                    error,
                    restate.invocation.id = %invocation_id,
                    restate.invocation.target = %ism.invocation_target,
                    restate.deployment.id = %attempt_deployment_id,
                    "Error when executing the invocation, pausing the invocation.");
                self.quota.unreserve_slot();
                self.status_store.on_end(&partition, &invocation_id);

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
                let paused_event = PausedEvent {
                    last_failure: Some(TransientErrorEvent {
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
                    }),
                };

                let _ = self
                    .invocation_state_machine_manager
                    .resolve_partition_sender(partition)
                    .expect("Partition should be registered")
                    .send(InvokerEffect {
                        effect: Box::new(Effect {
                            invocation_id,
                            kind: EffectKind::Paused {
                                paused_event: RawEvent::from(Event::Paused(paused_event)),
                            },
                        }),
                        inbound_lease: None,
                    });
            }
            OnTaskError::Kill => {
                counter!(INVOKER_INVOCATION_TASKS,
                    "status" => TASK_OP_FAILED,
                    "transient" => "false",
                    "partition_id" => ID_LOOKUP.get(partition.0)
                )
                .increment(1);
                warn_it!(
                    error,
                    restate.invocation.id = %invocation_id,
                    restate.invocation.target = %ism.invocation_target,
                    restate.deployment.id = %attempt_deployment_id,
                    "Error when executing the invocation, not going to retry.");
                self.quota.unreserve_slot();
                self.status_store.on_end(&partition, &invocation_id);

                let _ = self
                    .invocation_state_machine_manager
                    .resolve_partition_sender(partition)
                    .expect("Partition should be registered")
                    .send(InvokerEffect {
                        effect: Box::new(Effect {
                            invocation_id,
                            kind: EffectKind::Failed(error.into_invocation_error()),
                        }),
                        inbound_lease: None,
                    });
            }
        }
    }

    /// Create a per-invocation memory budget from the given seed leases.
    ///
    /// The upper bound is set to the pool's total capacity (or `usize::MAX` for
    /// unlimited pools) to prevent a single invocation from monopolizing the pool.
    fn create_invocation_memory(
        &self,
        options: &InvokerOptions,
        inbound_seed: MemoryLease,
        outbound_seed: MemoryLease,
    ) -> InvocationMemory {
        // todo figure out the proper upper bound for the inboud and outbound budgets
        let upper_bound = options.message_size_limit().get();
        InvocationMemory::new(inbound_seed, upper_bound, outbound_seed, upper_bound)
    }

    fn start_invocation_task(
        &mut self,
        options: &InvokerOptions,
        partition: PartitionLeaderEpoch,
        storage_reader: IR,
        invocation_id: InvocationId,
        mut ism: InvocationStateMachine,
        budget: InvocationMemory,
    ) {
        // Start the InvocationTask
        let (completions_tx, completions_rx) = mpsc::unbounded_channel();
        let abort_handle = self.invocation_task_runner.start_invocation_task(
            options,
            partition,
            invocation_id,
            ism.invocation_target.clone(),
            ism.start_message_retry_count_since_last_stored_command,
            storage_reader,
            self.invocation_tasks_tx.clone(),
            completions_rx,
            &mut self.invocation_tasks,
            budget,
        );

        // Transition the state machine, and store it
        self.status_store.on_start(partition, invocation_id);
        ism.start(abort_handle, completions_tx);
        trace!(
            restate.invocation.target = %ism.invocation_target,
            "Invocation task started state. Invocation state: {:?}",
            ism.invocation_state_debug()
        );
        counter!(INVOKER_INVOCATION_TASKS, "status" => TASK_OP_STARTED, "partition_id" => ID_LOOKUP.get(partition.0)).increment(1);
        self.invocation_state_machine_manager
            .register_invocation(partition, invocation_id, ism);
    }

    fn handle_retry_event<FN>(
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
                // Reuse the budget stashed on the ISM from the previous attempt
                let budget = ism
                    .budget
                    .take()
                    .expect("Invocation budget must be present when retrying");
                self.start_invocation_task(
                    options,
                    partition,
                    storage_reader,
                    invocation_id,
                    ism,
                    budget,
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
    use gardal::StreamExt as GardalStreamExt;
    use googletest::prelude::*;
    use restate_core::{TaskCenter, TaskKind};
    use restate_invoker_api::InvokerHandle;
    use restate_invoker_api::entry_enricher;
    use restate_invoker_api::test_util::EmptyStorageReader;
    use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
    use restate_test_util::check;
    use restate_time_util::FriendlyDuration;
    use restate_types::config::InvokerOptionsBuilder;
    use restate_types::deployment::{DeploymentAddress, Headers};
    use restate_types::errors::{InvocationError, codes};
    use restate_types::identifiers::{LeaderEpoch, PartitionId, ServiceRevision};
    use restate_types::invocation::ServiceType;
    use restate_types::journal::enriched::EnrichedEntryHeader;
    use restate_types::journal::raw::RawEntry;
    use restate_types::journal_events::EventType;
    use restate_types::journal_v2::{Command, Encoder, Entry, OutputCommand, OutputResult};
    use restate_types::live::Constant;
    use restate_types::retries::{RetryIter, RetryPolicy};
    use restate_types::schema::deployment::Deployment;
    use restate_types::schema::invocation_target::{
        InvocationAttemptOptions, InvocationTargetMetadata, OnMaxAttempts,
    };
    use restate_types::schema::service::ServiceMetadata;
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_types::vqueue::{VQueueId, VQueueInstance, VQueueParent};
    use tempfile::tempdir;
    use test_log::test;
    use tokio::sync::mpsc;

    use restate_memory::{LocalMemoryLease, LocalMemoryPool};

    use crate::error::{InvokerError, SdkInvocationErrorV2};
    use crate::quota::InvokerConcurrencyQuota;

    /// Create a zero-sized [`LocalMemoryLease`] for tests that construct
    /// payload-carrying [`InvocationTaskOutputInner`] variants directly.
    fn test_empty_inbound_lease() -> LocalMemoryLease {
        let budget = LocalMemoryPool::new(
            MemoryPool::unlimited(),
            MemoryPool::unlimited().empty_lease(),
            0,
            usize::MAX,
        );
        budget.empty_lease()
    }

    // -- Mocks

    const MOCK_PARTITION: PartitionLeaderEpoch = (PartitionId::MIN, LeaderEpoch::INITIAL);

    impl<ITR, Schemas, IR> ServiceInner<ITR, Schemas, IR>
    where
        IR: InvocationReader + Clone + Send + Sync + 'static,
        Schemas: InvocationTargetResolver,
    {
        #[allow(clippy::type_complexity)]
        fn mock(
            invocation_task_runner: ITR,
            schemas: Schemas,
            concurrency_limit: Option<NonZeroUsize>,
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
                schemas: Live::from_value(schemas),
                invocation_tasks: Default::default(),
                retry_timers: Default::default(),
                last_retry_timer_compact: Instant::now(),
                quota: InvokerConcurrencyQuota::new(0, concurrency_limit),
                status_store: Default::default(),
                invocation_state_machine_manager: Default::default(),
                memory_pool: MemoryPool::unlimited(),
                pending_seed_lease: None,
            };
            (input_tx, status_tx, service_inner)
        }

        fn register_mock_partition(
            &mut self,
            storage_reader: IR,
        ) -> mpsc::UnboundedReceiver<InvokerEffect>
        where
            ITR: InvocationTaskRunner<IR>,
        {
            let (partition_tx, partition_rx) = mpsc::unbounded_channel();
            self.handle_register_partition(
                MOCK_PARTITION,
                RangeInclusive::new(0, 0),
                storage_reader,
                partition_tx,
            );
            partition_rx
        }

        /// Helper for tests: Create a budget from the mock's unlimited pool.
        fn test_budget(&self) -> InvocationMemory {
            let upper_bound = match self.memory_pool.capacity().as_usize() {
                0 => usize::MAX,
                cap => cap,
            };
            let inbound_seed = self.memory_pool.empty_lease();
            let outbound_seed = self.memory_pool.empty_lease();
            InvocationMemory::new(inbound_seed, upper_bound, outbound_seed, upper_bound)
        }

        /// Helper for tests: Process the registered retry timers until all timers have fired.
        async fn process_retry_timers(&mut self, options: &InvokerOptions)
        where
            ITR: InvocationTaskRunner<IR>,
        {
            while let Some(expired) = self.retry_timers.next().await {
                let timer_key = expired.key();
                let (partition, invocation_id) = expired.into_inner();
                self.handle_retry_timer_fired(options, partition, invocation_id, timer_key);
            }
        }

        /// Helper for tests: checks if an invocation's ISM is in WaitingRetry state.
        fn is_invocation_waiting_retry(
            &mut self,
            partition: PartitionLeaderEpoch,
            invocation_id: &InvocationId,
        ) -> bool {
            self.invocation_state_machine_manager
                .resolve_invocation(partition, invocation_id)
                .is_some_and(|(_, ism)| ism.take_retry_timer_key().is_some())
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
        ) -> Fut,
        IR: InvocationReader + Clone + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        fn start_invocation_task(
            &self,
            _options: &InvokerOptions,
            partition: PartitionLeaderEpoch,
            invocation_id: InvocationId,
            invocation_target: InvocationTarget,
            _retry_count_since_last_stored_entry: u32,
            storage_reader: IR,
            invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
            invoker_rx: mpsc::UnboundedReceiver<Notification>,
            task_pool: &mut JoinSet<()>,
            _budget: InvocationMemory,
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
            _invocation_target: InvocationTarget,
            _retry_count_since_last_stored_entry: u32,
            _storage_reader: SR,
            _invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
            _invoker_rx: mpsc::UnboundedReceiver<Notification>,
            task_pool: &mut JoinSet<()>,
            _budget: InvocationMemory,
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
            _invocation_target: InvocationTarget,
            _retry_count_since_last_stored_entry: u32,
            _storage_reader: SR,
            _invoker_tx: mpsc::UnboundedSender<InvocationTaskOutput>,
            _invoker_rx: mpsc::UnboundedReceiver<Notification>,
            task_pool: &mut JoinSet<()>,
            _budget: InvocationMemory,
        ) -> AbortHandle {
            self.fetch_add(1, Ordering::SeqCst);
            task_pool.spawn(pending())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct MockSchemas(Option<RetryPolicy>, Option<OnMaxAttempts>);

    impl DeploymentResolver for MockSchemas {
        fn resolve_latest_deployment_for_service(&self, _: impl AsRef<str>) -> Option<Deployment> {
            None
        }

        fn find_deployment(
            &self,
            _: &DeploymentAddress,
            _: &Headers,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
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

        fn resolve_invocation_attempt_options(
            &self,
            _: &DeploymentId,
            _: impl AsRef<str>,
            _: impl AsRef<str>,
        ) -> Option<InvocationAttemptOptions> {
            None
        }

        fn resolve_latest_service_type(&self, _: impl AsRef<str>) -> Option<ServiceType> {
            None
        }

        fn resolve_invocation_retry_policy(
            &self,
            _: Option<&DeploymentId>,
            _: impl AsRef<str>,
            _: impl AsRef<str>,
        ) -> (RetryIter<'static>, OnMaxAttempts) {
            (
                self.0
                    .clone()
                    .unwrap_or_else(|| {
                        RetryPolicy::exponential(Duration::from_millis(100), 2.0, None, None)
                    })
                    .into_iter(),
                self.1.unwrap_or(OnMaxAttempts::Kill),
            )
        }
    }

    #[test(restate_core::test)]
    async fn input_order_is_maintained() {
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap().into())
            .message_size_limit(None)
            .build()
            .unwrap();
        let service = Service::new(
            0,
            &invoker_options,
            // all invocations are unknown leading to immediate retries
            Live::from_value(MockSchemas(
                // fixed amount of retries so that an invocation eventually completes with a failure
                Some(RetryPolicy::fixed_delay(Duration::ZERO, Some(1))),
                Some(OnMaxAttempts::Kill),
            )),
            ServiceClient::from_options(
                &ServiceClientOptions::default(),
                AssumeRoleCacheMode::None,
            )
            .unwrap(),
            entry_enricher::test_util::MockEntryEnricher,
            None,
            None,
            MemoryPool::unlimited(),
        );

        let mut handle = service.handle();

        let invoker_task = TaskCenter::spawn_unmanaged(
            TaskKind::SystemService,
            "invoker",
            service.run(Constant::new(invoker_options)),
        )
        .unwrap()
        .into_guard();

        let partition_leader_epoch = (PartitionId::from(0), LeaderEpoch::INITIAL);
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);

        let (output_tx, mut output_rx) = mpsc::unbounded_channel();

        handle
            .register_partition(
                partition_leader_epoch,
                RangeInclusive::new(0, 0),
                EmptyStorageReader,
                output_tx,
            )
            .unwrap();
        handle
            .invoke(partition_leader_epoch, invocation_id, invocation_target)
            .unwrap();

        // If input order between 'register partition' and 'invoke' is not maintained, then it can happen
        // that 'invoke' arrives before 'register partition'. In this case, the invoker service will drop
        // the invocation and we won't see a result for the invocation (failure because the deployment cannot be resolved).
        check!(let Some(_) = output_rx.recv().await);

        invoker_task.cancel_and_wait().await.unwrap();
    }

    #[test(restate_core::test)]
    async fn quota_allows_one_concurrent_invocation() {
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap().into())
            .message_size_limit(None)
            .build()
            .unwrap();

        let mut segment_queue =
            std::pin::pin!(SegmentQueue::new(tempdir().unwrap().keep(), 1024).throttle(None));

        let invocation_id_1 = InvocationId::mock_random();
        let invocation_id_2 = InvocationId::mock_random();

        let (_invoker_tx, _status_tx, mut service_inner) = ServiceInner::mock(
            |_, _, _, _, _, _| ready(()),
            MockSchemas(
                // fixed amount of retries so that an invocation eventually completes with a failure
                Some(RetryPolicy::fixed_delay(Duration::ZERO, Some(1))),
                Some(OnMaxAttempts::Kill),
            ),
            Some(NonZeroUsize::new(1).unwrap()),
        );
        let _ = service_inner.register_mock_partition(EmptyStorageReader);

        // Enqueue sid_1 and sid_2
        segment_queue
            .as_mut()
            .inner_pin_mut()
            .enqueue(Box::new(InvokeCommand {
                partition: MOCK_PARTITION,
                invocation_id: invocation_id_1,
                invocation_target: InvocationTarget::mock_virtual_object(),
            }))
            .await;
        segment_queue
            .as_mut()
            .inner_pin_mut()
            .enqueue(Box::new(InvokeCommand {
                partition: MOCK_PARTITION,
                invocation_id: invocation_id_2,
                invocation_target: InvocationTarget::mock_virtual_object(),
            }))
            .await;

        // Now step the state machine to start the invocation
        service_inner
            .step(&invoker_options, segment_queue.as_mut())
            .await;

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
        service_inner
            .step(&invoker_options, segment_queue.as_mut())
            .await;

        assert!(
            service_inner
                .status_store
                .resolve_invocation(MOCK_PARTITION, &invocation_id_2)
                .is_none()
        );
        assert!(!service_inner.quota.is_slot_available());

        // Send the close signal
        service_inner.handle_invocation_task_closed(MOCK_PARTITION, invocation_id_1);

        // Slot should be available again
        assert!(service_inner.quota.is_slot_available());

        // Step now should invoke sid_2
        service_inner
            .step(&invoker_options, segment_queue.as_mut())
            .await;

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
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap().into())
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
             _| {
                let _ = invoker_tx.send(InvocationTaskOutput {
                    partition,
                    invocation_id,
                    inner: InvocationTaskOutputInner::NewEntry {
                        entry_index: 1,
                        entry: RawEntry::new(EnrichedEntryHeader::SetState {}, Bytes::default())
                            .into(),
                        requires_ack: false,
                        inbound_lease: test_empty_inbound_lease(),
                    },
                });
                pending() // Never ends
            },
            MockSchemas(
                // fixed amount of retries so that an invocation eventually completes with a failure
                Some(RetryPolicy::fixed_delay(Duration::ZERO, Some(1))),
                Some(OnMaxAttempts::Kill),
            ),
            Some(NonZeroUsize::new(2).unwrap()),
        );
        let _ = service_inner.register_mock_partition(EmptyStorageReader);

        // Invoke the service
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // We should receive the new entry here
        let invoker_effect = service_inner.invocation_tasks_rx.recv().await.unwrap();
        assert_eq!(invoker_effect.invocation_id, invocation_id);
        check!(let InvocationTaskOutputInner::NewEntry { .. } = invoker_effect.inner);

        // Check the quota
        assert_eq!(service_inner.quota.available_slots(), 1);

        // Abort the invocation
        service_inner.handle_abort_invocation(MOCK_PARTITION, invocation_id);

        // Check the quota
        assert_eq!(service_inner.quota.available_slots(), 2);

        // Handle error coming after the abort (this should be noop)
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            InvokerError::EmptySuspensionMessage, /* any error is fine */
            service_inner.test_budget(),
        );

        // Check the quota, should not be changed
        assert_eq!(service_inner.quota.available_slots(), 2);
    }

    #[test(restate_core::test(start_paused = true))]
    async fn notification_triggers_retry() {
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap().into())
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
                  _invoker_rx| {
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
            MockSchemas(
                // fixed amount of retries so that an invocation eventually completes with a failure
                Some(RetryPolicy::fixed_delay(Duration::ZERO, Some(1))),
                Some(OnMaxAttempts::Kill),
            ),
            None,
        );

        // Register a mock partition
        let _ = service_inner.register_mock_partition(EmptyStorageReader);

        // Create an invocation state machine and register it with an in-flight notification proposal
        let mut ism = InvocationStateMachine::create(
            Some(VQueueId::new(
                VQueueParent::default_unlimited(),
                invocation_id.partition_key(),
                VQueueInstance::Default,
            )),
            Permit::new_empty(),
            invocation_target.clone(),
            RetryPolicy::fixed_delay(Duration::from_millis(100), None).into_iter(),
            OnMaxAttempts::Kill,
        );
        let (tx, _rx) = mpsc::unbounded_channel();
        ism.start(tokio::spawn(async {}).abort_handle(), tx);

        // Add a notification proposal
        ism.notify_new_notification_proposal(NotificationId::CompletionId(1));

        // Register the ISM and use handle_invocation_task_failed to put it in WaitingRetry state.
        // This will register the timer in the real DelayQueue.
        service_inner
            .invocation_state_machine_manager
            .register_invocation(MOCK_PARTITION, invocation_id, ism);
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            InvokerError::SdkV2(SdkInvocationErrorV2::unknown()),
            service_inner.test_budget(),
        );

        // Fire the retry timer using the helper that retrieves the key from the ISM
        service_inner.process_retry_timers(&invoker_options).await;

        // Send the notification with completion id 1
        service_inner.handle_notification(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            0, // entry index does not matter as we are not reading this notification
            NotificationId::CompletionId(1),
        );

        let (partition, id, target) = task_started_rx.recv().await.unwrap();
        assert_eq!(partition, MOCK_PARTITION);
        assert_eq!(id, invocation_id);
        assert_eq!(target, invocation_target);
    }

    #[test(restate_core::test(start_paused = true))]
    async fn status_store_clears_last_failure_on_new_command() {
        let invocation_id = InvocationId::mock_random();

        let (_, _status_tx, mut service_inner) =
            ServiceInner::mock((), MockSchemas::default(), None);
        let _effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start an invocation with epoch 0
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &InvokerOptions::default(),
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Simulate a transient failure to populate last_retry_attempt_failure
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            InvokerError::SdkV2(SdkInvocationErrorV2::unknown()),
            service_inner.test_budget(),
        );

        // After failure, the status store should record the last failure
        let report = service_inner
            .status_store
            .resolve_invocation(MOCK_PARTITION, &invocation_id)
            .expect("status report exists after failure");
        assert!(
            report.last_retry_attempt_failure().is_some(),
            "expected last_retry_attempt_failure to be set"
        );

        // Trigger the retry
        service_inner
            .process_retry_timers(&InvokerOptions::default())
            .await;

        // Now a new command proposal should clear the last failure (progress made)
        service_inner.handle_new_command(
            MOCK_PARTITION,
            invocation_id,
            1,
            ServiceProtocolV4Codec::encode_entry(Entry::Command(Command::Output(OutputCommand {
                result: OutputResult::Success(Bytes::default()),
                name: Default::default(),
            })))
            .try_as_command()
            .unwrap(),
            false,
            test_empty_inbound_lease(),
        );

        let report_after = service_inner
            .status_store
            .resolve_invocation(MOCK_PARTITION, &invocation_id)
            .expect("status report exists after new command");
        assert!(
            report_after.last_retry_attempt_failure().is_none(),
            "expected last_retry_attempt_failure to be cleared after new command"
        );
    }

    #[test(restate_core::test(start_paused = true))]
    async fn transient_error_event_deduplication() {
        // Enable proposing events and keep timers short for the test
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .disable_eager_state(false)
            .build()
            .unwrap();

        let invocation_id = InvocationId::mock_random();

        // Mock service and register partition
        let (_, _status_tx, mut service_inner) = ServiceInner::mock(
            (),
            MockSchemas(
                // fixed amount of retries so that an invocation eventually completes with a failure
                Some(RetryPolicy::fixed_delay(Duration::ZERO, Some(3))),
                Some(OnMaxAttempts::Kill),
            ),
            None,
        );
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start invocation epoch 0
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Select protocol V4 to allow proposing events
        service_inner.handle_pinned_deployment(
            MOCK_PARTITION,
            invocation_id,
            PinnedDeployment::new(DeploymentId::new(), ServiceProtocolVersion::V4),
            false, // has_changed = false -> directly selects protocol without emitting effect
        );

        // First transient error (A) -> should propose a TransientError event
        let error_a = InvokerError::SdkV2(SdkInvocationErrorV2 {
            related_command: None,
            next_retry_interval_override: Some(Duration::from_millis(1)),
            error: InvocationError::new(codes::INTERNAL, "boom").into(),
        });
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            error_a,
            service_inner.test_budget(),
        );
        assert_that!(
            *effects_rx
                .try_recv()
                .expect("expected a proposed transient error event")
                .effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::JournalEvent {
                    event: predicate(|e: &RawEvent| e.ty() == EventType::TransientError)
                })
            })
        );

        // Fire the timer to let the invocation go back to in flight
        service_inner.process_retry_timers(&invoker_options).await;

        // Same transient error (A again) -> should NOT propose a new event
        let error_a_same = InvokerError::SdkV2(SdkInvocationErrorV2 {
            related_command: None,
            next_retry_interval_override: Some(Duration::from_millis(1)),
            error: InvocationError::new(codes::INTERNAL, "boom").into(),
        });
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            error_a_same,
            service_inner.test_budget(),
        );
        assert!(
            effects_rx.try_recv().is_err(),
            "duplicate transient error event should not be proposed"
        );

        // Fire the timer to let the invocation go back to in flight
        service_inner.process_retry_timers(&invoker_options).await;

        // Different transient error (B: different message) -> should propose a new event
        let error_b = InvokerError::SdkV2(SdkInvocationErrorV2 {
            related_command: None,
            next_retry_interval_override: Some(Duration::from_millis(1)),
            error: InvocationError::new(codes::INTERNAL, "boom-2").into(),
        });
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            error_b,
            service_inner.test_budget(),
        );
        assert_that!(
            *effects_rx
                .try_recv()
                .expect("expected a newly proposed transient error event for different content")
                .effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::JournalEvent {
                    event: predicate(|e: &RawEvent| e.ty() == EventType::TransientError)
                })
            })
        );
    }

    #[test(restate_core::test)]
    async fn abort_error_counts_towards_retry_policy() {
        // Enable proposing events and keep timers short for the test
        let invocation_id = InvocationId::mock_random();

        // Mock service and register partition
        let (_, _status_tx, mut service_inner) =
            ServiceInner::mock((), MockSchemas::default(), None);
        let _rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start invocation epoch 0
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &InvokerOptions::default(),
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Abort error
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            InvokerError::AbortTimeoutFired(Duration::from_secs(10).into()),
            service_inner.test_budget(),
        );

        // Check the ISM is in WaitingRetry state and retry count is 1
        assert!(service_inner.is_invocation_waiting_retry(MOCK_PARTITION, &invocation_id));
        let (_, ism) = service_inner
            .invocation_state_machine_manager
            .resolve_invocation(MOCK_PARTITION, &invocation_id)
            .unwrap();
        assert_that!(
            ism.start_message_retry_count_since_last_stored_command,
            eq(1)
        );
    }

    #[test(restate_core::test(start_paused = true))]
    async fn pause_effect_emitted_when_pause_on_max_attempts_and_max_attempts_one() {
        // Configure invoker to propose events to flush transient error event (not strictly needed for pause)
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .build()
            .unwrap();

        let invocation_id = InvocationId::mock_random();

        // Mock schemas: max attempts = 1, on max attempts Pause
        let (_, _status_tx, mut service_inner) = ServiceInner::mock(
            (),
            MockSchemas(
                Some(RetryPolicy::fixed_delay(Duration::from_millis(1), Some(1))),
                Some(OnMaxAttempts::Pause),
            ),
            None,
        );
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start invocation
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // First transient error -> schedules retry (because 1 attempt available)
        let error_a = InvokerError::SdkV2(SdkInvocationErrorV2::unknown());
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            error_a,
            service_inner.test_budget(),
        );
        // There might be an extra transient error event proposed; drain if present
        let _ = effects_rx.try_recv();

        // Fire timer to go back in flight
        service_inner.process_retry_timers(&invoker_options).await;

        // Second transient error -> retries exhausted and Pause behavior -> expect Paused effect
        let error_b = InvokerError::SdkV2(SdkInvocationErrorV2::unknown());
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            error_b,
            service_inner.test_budget(),
        );

        let effect = effects_rx
            .try_recv()
            .expect("expected an effect to be emitted after pause");
        assert_that!(
            *effect.effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::Paused {
                    paused_event: predicate(|e: &RawEvent| e.ty() == EventType::Paused)
                })
            })
        );
    }

    #[test(restate_core::test(start_paused = true))]
    async fn retry_iter_updates_on_pinned_deployment_and_not_reset_on_same_dp() {
        use restate_types::service_protocol::ServiceProtocolVersion;

        // Create custom resolver that switches behavior based on presence of deployment id
        #[derive(Clone)]
        struct SwitchingResolver;
        impl InvocationTargetResolver for SwitchingResolver {
            fn resolve_latest_invocation_target(
                &self,
                _service_name: impl AsRef<str>,
                _handler_name: impl AsRef<str>,
            ) -> Option<InvocationTargetMetadata> {
                None
            }
            fn resolve_latest_service_type(
                &self,
                _service_name: impl AsRef<str>,
            ) -> Option<ServiceType> {
                None
            }
            fn resolve_invocation_attempt_options(
                &self,
                _deployment_id: &DeploymentId,
                _service_name: impl AsRef<str>,
                _handler_name: impl AsRef<str>,
            ) -> Option<InvocationAttemptOptions> {
                None
            }
            fn resolve_invocation_retry_policy(
                &self,
                deployment_id: Option<&DeploymentId>,
                _service_name: impl AsRef<str>,
                _handler_name: impl AsRef<str>,
            ) -> (RetryIter<'static>, OnMaxAttempts) {
                // Both cases max attempts = 2, but OnMaxAttempts switches
                let iter = RetryPolicy::fixed_delay(std::time::Duration::from_millis(1), Some(2))
                    .into_iter();
                match deployment_id {
                    None => (iter, OnMaxAttempts::Pause),
                    Some(_) => (iter, OnMaxAttempts::Kill),
                }
            }
        }

        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .build()
            .unwrap();

        let invocation_id = InvocationId::mock_random();
        let (_, _status_tx, mut service_inner) = ServiceInner::mock((), SwitchingResolver, None);
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start invocation
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Pin deployment (switches policy to Kill and resets attempts)
        let dp = PinnedDeployment::new(DeploymentId::new(), ServiceProtocolVersion::V4);
        service_inner.handle_pinned_deployment(MOCK_PARTITION, invocation_id, dp.clone(), true);

        // First transient failure after pin -> schedules retry
        let err1 = InvokerError::SdkV2(SdkInvocationErrorV2::unknown());
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            err1,
            service_inner.test_budget(),
        );
        // Drain any proposed event
        effects_rx.try_recv().unwrap();
        service_inner.process_retry_timers(&invoker_options).await;

        // Second transient failure after pin -> schedules retry (attempts now exhausted)
        let err2 = InvokerError::SdkV2(SdkInvocationErrorV2::unknown());
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            err2,
            service_inner.test_budget(),
        );
        effects_rx.try_recv().unwrap_err();
        service_inner.process_retry_timers(&invoker_options).await;

        // Send the same pinned deployment again -> must NOT reset the retry iterator
        let same_dp = PinnedDeployment::new(dp.deployment_id, ServiceProtocolVersion::V4);
        service_inner.handle_pinned_deployment(MOCK_PARTITION, invocation_id, same_dp, false);

        // Next failure should hit OnMaxAttempts::Kill immediately (no more retries)
        let err3 = InvokerError::SdkV2(SdkInvocationErrorV2::unknown());
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            err3,
            service_inner.test_budget(),
        );

        let effect = effects_rx
            .try_recv()
            .expect("expected an effect to be emitted after kill");
        assert_that!(
            *effect.effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::Failed(_))
            })
        );
    }

    #[test(restate_core::test)]
    async fn manual_pause_while_waiting_retry() {
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .build()
            .unwrap();

        let invocation_id = InvocationId::mock_random();

        // Use OnMaxAttempts::Kill to ensure pause is from manual request
        let (_, _status_tx, mut service_inner) =
            ServiceInner::mock((), MockSchemas(None, Some(OnMaxAttempts::Kill)), None);
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start invocation
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Simulate a transient error to put invocation in WaitingRetry state
        let error = InvokerError::SdkV2(SdkInvocationErrorV2::unknown());
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            error,
            service_inner.test_budget(),
        );
        // Drain any proposed event
        let _ = effects_rx.try_recv();

        // Verify invocation is in WaitingRetry state
        assert!(service_inner.is_invocation_waiting_retry(MOCK_PARTITION, &invocation_id));
        assert!(!service_inner.retry_timers.is_empty());

        // Call manual pause while waiting for retry
        service_inner.handle_pause_invocation(MOCK_PARTITION, invocation_id);

        // Should emit Paused effect immediately with no last_failure
        let effect = effects_rx
            .try_recv()
            .expect("expected Paused effect to be emitted");
        assert_that!(
            *effect.effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::Paused {
                    paused_event: predicate(|e: &RawEvent| e.ty() == EventType::Paused)
                })
            })
        );

        // Verify the retry timer was cancelled from the queue
        assert!(
            service_inner.retry_timers.is_empty(),
            "retry timer should be cancelled after pause"
        );
    }

    #[test(restate_core::test)]
    async fn manual_pause_while_in_flight_then_suspended() {
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .build()
            .unwrap();

        let invocation_id = InvocationId::mock_random();

        // Use OnMaxAttempts::Kill to ensure pause is from manual request
        let (_, _status_tx, mut service_inner) =
            ServiceInner::mock((), MockSchemas(None, Some(OnMaxAttempts::Kill)), None);
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start invocation (goes to InFlight state with pending task)
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Call manual pause while in flight
        service_inner.handle_pause_invocation(MOCK_PARTITION, invocation_id);

        // State machine should still be registered with requested_pause = true
        let (_, ism) = service_inner
            .invocation_state_machine_manager
            .resolve_invocation(MOCK_PARTITION, &invocation_id)
            .unwrap();
        assert!(ism.requested_pause);

        // Simulate the invocation task suspending
        service_inner.handle_invocation_task_suspended(
            MOCK_PARTITION,
            invocation_id,
            HashSet::new(), // No pending entries
        );

        // Should emit Paused effect (not Suspended) with no last_failure
        let effect = effects_rx
            .try_recv()
            .expect("expected Paused effect to be emitted");
        assert_that!(
            *effect.effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::Paused {
                    paused_event: predicate(|e: &RawEvent| e.ty() == EventType::Paused)
                })
            })
        );
    }

    #[test(restate_core::test)]
    async fn manual_pause_while_in_flight_then_error() {
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .build()
            .unwrap();

        let invocation_id = InvocationId::mock_random();

        // Use OnMaxAttempts::Kill to ensure pause is from manual request
        let (_, _status_tx, mut service_inner) =
            ServiceInner::mock((), MockSchemas(None, Some(OnMaxAttempts::Kill)), None);
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start invocation (goes to InFlight state with pending task)
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Call manual pause while in flight
        service_inner.handle_pause_invocation(MOCK_PARTITION, invocation_id);

        // State machine should still be registered with requested_pause = true
        let (_, ism) = service_inner
            .invocation_state_machine_manager
            .resolve_invocation(MOCK_PARTITION, &invocation_id)
            .unwrap();
        assert!(ism.requested_pause);

        // Simulate the invocation task failing with a transient error
        let error = InvokerError::SdkV2(SdkInvocationErrorV2::unknown());
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            error,
            service_inner.test_budget(),
        );

        // Should emit Paused effect (not Kill or ScheduleRetry) with last_failure set
        let effect = effects_rx
            .try_recv()
            .expect("expected Paused effect to be emitted");
        assert_that!(
            *effect.effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::Paused {
                    paused_event: predicate(|e: &RawEvent| {
                        e.ty() == EventType::Paused &&
                        // Verify last_failure is present by checking the event contains error info
                        e.clone().into_event_or_unknown().try_as_paused().unwrap().last_failure.is_some()
                    })
                })
            })
        );
    }

    #[test(restate_core::test)]
    async fn manual_pause_while_waiting_retry_with_previous_failure() {
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .build()
            .unwrap();

        let invocation_id = InvocationId::mock_random();

        // Use OnMaxAttempts::Kill to ensure pause is from manual request
        let (_, _status_tx, mut service_inner) =
            ServiceInner::mock((), MockSchemas(None, Some(OnMaxAttempts::Kill)), None);
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        // Start invocation
        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Simulate a transient error to put invocation in WaitingRetry state
        let error = InvokerError::SdkV2(SdkInvocationErrorV2::unknown());
        service_inner.handle_invocation_task_failed(
            MOCK_PARTITION,
            invocation_id,
            error,
            service_inner.test_budget(),
        );
        // Drain any proposed event
        let _ = effects_rx.try_recv();

        // Verify invocation is in WaitingRetry state
        assert!(service_inner.is_invocation_waiting_retry(MOCK_PARTITION, &invocation_id));

        // Call manual pause while waiting for retry (after error was notified)
        service_inner.handle_pause_invocation(MOCK_PARTITION, invocation_id);

        // Should emit Paused effect immediately with last_failure populated from the error
        let effect = effects_rx
            .try_recv()
            .expect("expected Paused effect to be emitted");
        assert_that!(
            *effect.effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::Paused {
                    paused_event: predicate(|e: &RawEvent| {
                        e.ty() == EventType::Paused &&
                        // Verify last_failure is present since we paused while waiting retry after error
                        e.clone().into_event_or_unknown().try_as_paused().unwrap().last_failure.is_some()
                    })
                })
            })
        );
    }

    /// When the yield flag is disabled (default), budget exhaustion falls back to the
    /// retry path without bumping the retry count.
    #[test(restate_core::test(start_paused = true))]
    async fn yield_flag_disabled_falls_back_to_retry() {
        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap().into())
            .message_size_limit(None)
            .build()
            .unwrap();
        let invocation_id = InvocationId::mock_random();

        let (_, _status_tx, mut service_inner) = ServiceInner::mock(
            (),
            MockSchemas(
                Some(RetryPolicy::fixed_delay(Duration::from_millis(100), None)),
                None,
            ),
            None,
        );
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Simulate yield from invocation task (flag disabled by default)
        service_inner.handle_invocation_task_should_yield(
            MOCK_PARTITION,
            invocation_id,
            65536,
            32768,
            service_inner.test_budget(),
        );

        // Should NOT emit EffectKind::Yield — instead the error goes through retry
        // The ISM should be in WaitingRetry state (error was handled as OutOfMemory)
        assert!(service_inner.is_invocation_waiting_retry(MOCK_PARTITION, &invocation_id));

        // No Yield effect should be emitted, but a transient error event should be
        let effect = effects_rx
            .try_recv()
            .expect("expected a transient error event");
        assert_that!(
            *effect.effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::JournalEvent {
                    event: predicate(|e: &RawEvent| e.ty() == EventType::TransientError)
                })
            })
        );
    }

    /// When the yield flag is enabled, the invoker sends EffectKind::Yield and
    /// releases the invocation slot.
    #[test(restate_core::test(start_paused = true))]
    async fn yield_flag_enabled_sends_yield_effect() {
        // Enable the experimental yield flag
        let mut config = Configuration::default();
        config.common.experimental_enable_invoker_yield = true;
        restate_types::config::set_current_config(config);

        let invoker_options = InvokerOptionsBuilder::default()
            .inactivity_timeout(FriendlyDuration::ZERO)
            .abort_timeout(FriendlyDuration::ZERO)
            .disable_eager_state(false)
            .message_size_warning(NonZeroUsize::new(1024).unwrap().into())
            .message_size_limit(None)
            .build()
            .unwrap();
        let invocation_id = InvocationId::mock_random();

        let (_, _status_tx, mut service_inner) = ServiceInner::mock(
            (),
            MockSchemas(
                Some(RetryPolicy::fixed_delay(Duration::from_millis(100), None)),
                None,
            ),
            None,
        );
        let mut effects_rx = service_inner.register_mock_partition(EmptyStorageReader);

        let budget = service_inner.test_budget();
        service_inner.handle_invoke(
            &invoker_options,
            MOCK_PARTITION,
            invocation_id,
            InvocationTarget::mock_virtual_object(),
            budget,
        );

        // Simulate yield from invocation task
        service_inner.handle_invocation_task_should_yield(
            MOCK_PARTITION,
            invocation_id,
            65536,
            32768,
            service_inner.test_budget(),
        );

        // Should emit EffectKind::Yield
        let effect = effects_rx
            .try_recv()
            .expect("expected Yield effect to be emitted");
        assert_that!(
            *effect.effect,
            pat!(Effect {
                invocation_id: eq(invocation_id),
                kind: pat!(EffectKind::Yield(pat!(YieldReason::OutOfMemory {
                    inbound_needed_memory: eq(65536),
                    outbound_needed_memory: eq(32768),
                })))
            })
        );

        // The invocation should no longer be tracked (slot released)
        assert!(service_inner.quota.is_slot_available());
    }
}
