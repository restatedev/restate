// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use assert2::let_assert;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt as _};
use metrics::{counter, histogram};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, instrument, trace, warn, Instrument, Span};

use restate_bifrost::{Bifrost, FindTailAttributes};
use restate_core::network::{Networking, TransportConnect};
use restate_core::{cancellation_watcher, metadata, TaskHandle, TaskKind};
use restate_partition_store::{PartitionStore, PartitionStoreTransaction};
use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, DeduplicationTable, ProducerId,
    ReadOnlyDeduplicationTable,
};
use restate_storage_api::fsm_table::{FsmTable, ReadOnlyFsmTable};
use restate_storage_api::outbox_table::ReadOnlyOutboxTable;
use restate_storage_api::{invocation_status_table, StorageError, Transaction};
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, ReplayStatus, RunMode};
use restate_types::config::{Configuration, WorkerOptions};
use restate_types::identifiers::{LeaderEpoch, PartitionId, PartitionKey, SnapshotId};
use restate_types::journal::raw::RawEntryCodec;
use restate_types::live::Live;
use restate_types::logs::MatchKeyQuery;
use restate_types::logs::{KeyFilter, LogId, Lsn, SequenceNumber};
use restate_types::retries::RetryPolicy;
use restate_types::time::MillisSinceEpoch;
use restate_types::GenerationalNodeId;
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

use crate::metric_definitions::{
    PARTITION_ACTUATOR_HANDLED, PARTITION_LABEL, PARTITION_LEADER_HANDLE_ACTION_BATCH_DURATION,
    PP_APPLY_COMMAND_BATCH_SIZE, PP_APPLY_COMMAND_DURATION,
};
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::leadership::{LeadershipState, PartitionProcessorMetadata};
use crate::partition::snapshot_producer::{SnapshotProducer, SnapshotSource};
use crate::partition::state_machine::{ActionCollector, StateMachine};

mod action_effect_handler;
mod cleaner;
pub mod invoker_storage_reader;
mod leadership;
pub mod shuffle;
mod snapshot_producer;
mod state_machine;
pub mod types;

/// Control messages from Manager to individual partition processor instances.
pub enum PartitionProcessorControlCommand {
    RunForLeader(LeaderEpoch),
    StepDown,
    CreateSnapshot(Option<oneshot::Sender<anyhow::Result<SnapshotId>>>),
}

#[derive(Debug)]
pub(super) struct PartitionProcessorBuilder<InvokerInputSender> {
    node_id: GenerationalNodeId,
    pub partition_id: PartitionId,
    pub partition_key_range: RangeInclusive<PartitionKey>,

    num_timers_in_memory_limit: Option<usize>,
    enable_new_invocation_status_table: bool,
    cleanup_interval: Duration,
    channel_size: usize,
    max_command_batch_size: usize,

    status: PartitionProcessorStatus,
    invoker_tx: InvokerInputSender,
    control_rx: mpsc::Receiver<PartitionProcessorControlCommand>,
    status_watch_tx: watch::Sender<PartitionProcessorStatus>,
}

impl<InvokerInputSender> PartitionProcessorBuilder<InvokerInputSender>
where
    InvokerInputSender:
        restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>> + Clone,
{
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        node_id: GenerationalNodeId,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        status: PartitionProcessorStatus,
        options: &WorkerOptions,
        control_rx: mpsc::Receiver<PartitionProcessorControlCommand>,
        status_watch_tx: watch::Sender<PartitionProcessorStatus>,
        invoker_tx: InvokerInputSender,
    ) -> Self {
        Self {
            node_id,
            partition_id,
            partition_key_range,
            status,
            num_timers_in_memory_limit: options.num_timers_in_memory_limit(),
            enable_new_invocation_status_table: options
                .experimental_feature_new_invocation_status_table(),
            cleanup_interval: options.cleanup_interval(),
            channel_size: options.internal_queue_length(),
            max_command_batch_size: options.max_command_batch_size(),
            invoker_tx,
            control_rx,
            status_watch_tx,
        }
    }

    pub async fn build<Codec: RawEntryCodec + Default + Debug, T: TransportConnect>(
        self,
        networking: Networking<T>,
        bifrost: Bifrost,
        mut partition_store: PartitionStore,
        configuration: Live<Configuration>,
    ) -> Result<PartitionProcessor<Codec, InvokerInputSender, T>, StorageError> {
        let PartitionProcessorBuilder {
            partition_id,
            partition_key_range,
            num_timers_in_memory_limit,
            cleanup_interval,
            enable_new_invocation_status_table,
            channel_size,
            max_command_batch_size,
            invoker_tx,
            control_rx,
            status_watch_tx,
            status,
            ..
        } = self;

        let state_machine = Self::create_state_machine::<Codec>(
            &mut partition_store,
            partition_key_range.clone(),
            enable_new_invocation_status_table,
        )
        .await?;

        let last_seen_leader_epoch = partition_store
            .get_dedup_sequence_number(&ProducerId::self_producer())
            .await?
            .map(|dedup| {
                let_assert!(
                    DedupSequenceNumber::Esn(esn) = dedup,
                    "self producer must store epoch sequence numbers!"
                );
                esn.leader_epoch
            });

        let leadership_state = LeadershipState::new(
            PartitionProcessorMetadata::new(
                self.node_id,
                partition_id,
                partition_key_range.clone(),
            ),
            num_timers_in_memory_limit,
            cleanup_interval,
            channel_size,
            invoker_tx,
            bifrost.clone(),
            networking,
            last_seen_leader_epoch,
        );

        Ok(PartitionProcessor {
            partition_id,
            partition_key_range,
            leadership_state,
            state_machine,
            max_command_batch_size,
            partition_store,
            bifrost,
            configuration,
            control_rx,
            status_watch_tx,
            status,
            inflight_create_snapshot_task: None,
        })
    }

    async fn create_state_machine<Codec>(
        partition_store: &mut PartitionStore,
        partition_key_range: RangeInclusive<PartitionKey>,
        enable_new_invocation_status_table: bool,
    ) -> Result<StateMachine<Codec>, StorageError>
    where
        Codec: RawEntryCodec + Default + Debug,
    {
        let inbox_seq_number = partition_store.get_inbox_seq_number().await?;
        let outbox_seq_number = partition_store.get_outbox_seq_number().await?;
        let outbox_head_seq_number = partition_store.get_outbox_head_seq_number().await?;

        let state_machine = StateMachine::new(
            inbox_seq_number,
            outbox_seq_number,
            outbox_head_seq_number,
            partition_key_range,
            if enable_new_invocation_status_table {
                invocation_status_table::SourceTable::New
            } else {
                invocation_status_table::SourceTable::Old
            },
        );

        Ok(state_machine)
    }
}

pub struct PartitionProcessor<Codec, InvokerSender, T> {
    partition_id: PartitionId,
    partition_key_range: RangeInclusive<PartitionKey>,
    leadership_state: LeadershipState<InvokerSender, T>,
    state_machine: StateMachine<Codec>,
    bifrost: Bifrost,
    configuration: Live<Configuration>,
    control_rx: mpsc::Receiver<PartitionProcessorControlCommand>,
    status_watch_tx: watch::Sender<PartitionProcessorStatus>,
    status: PartitionProcessorStatus,
    max_command_batch_size: usize,
    partition_store: PartitionStore,
    inflight_create_snapshot_task: Option<TaskHandle<anyhow::Result<()>>>,
}

impl<Codec, InvokerSender, T> PartitionProcessor<Codec, InvokerSender, T>
where
    Codec: RawEntryCodec + Default + Debug,
    InvokerSender: restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>> + Clone,
    T: TransportConnect,
{
    #[instrument(level = "error", skip_all, fields(partition_id = %self.partition_id, is_leader = tracing::field::Empty))]
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut partition_store = self.partition_store.clone();
        let last_applied_lsn = partition_store.get_applied_lsn().await?;
        let last_applied_lsn = last_applied_lsn.unwrap_or(Lsn::INVALID);

        self.status.last_applied_log_lsn = Some(last_applied_lsn);

        // todo make this configurable
        let find_tail_retry_policy = RetryPolicy::exponential(
            Duration::from_millis(100),
            2.0,
            None,
            Some(Duration::from_secs(10)),
        );

        // It can happen that the log is currently unavailable. That's why we need to retry.
        // todo if being stuck expose the state to the controller to allow it to make a control decision
        let current_tail = find_tail_retry_policy
            .retry(|| {
                self.bifrost.find_tail(
                    LogId::from(self.partition_id),
                    FindTailAttributes::default(),
                )
            })
            .await
            .expect("we should be retrying indefinitely");

        debug!(
            last_applied_lsn = %last_applied_lsn,
            current_log_tail = ?current_tail,
            "PartitionProcessor creating log reader",
        );
        if current_tail.offset() == last_applied_lsn.next() {
            if self.status.replay_status != ReplayStatus::Active {
                debug!(
                    ?last_applied_lsn,
                    "Processor has caught up with the log tail."
                );
                self.status.replay_status = ReplayStatus::Active;
            }
        } else {
            // catching up.
            self.status.target_tail_lsn = Some(current_tail.offset());
            self.status.replay_status = ReplayStatus::CatchingUp;
        }

        // If our `last_applied_lsn` is at or beyond the tail, this is a strong indicator
        // that the log has reverted backwards.
        if last_applied_lsn >= current_tail.offset() {
            error!(
                ?last_applied_lsn,
                log_tail_lsn = ?current_tail.offset(),
                "Processor has applied log entries beyond the log tail. This indicates data-loss in the log!"
            );
            // todo: declare unhealthy state to cluster controller, or raise a flare.
        } else if last_applied_lsn.next() != current_tail.offset() {
            debug!(
                "Replaying the log from lsn={}, log tail lsn={}",
                last_applied_lsn.next(),
                current_tail.offset()
            );
        }

        // Start reading after the last applied lsn
        let key_query = KeyFilter::Within(self.partition_key_range.clone());
        let mut log_reader = self
            .bifrost
            .create_reader(
                LogId::from(self.partition_id),
                key_query.clone(),
                last_applied_lsn.next(),
                Lsn::MAX,
            )?
            .map_ok(|entry| {
                trace!(?entry, "Read entry");
                let lsn = entry.sequence_number();
                let Some(envelope) = entry.try_decode_arc::<Envelope>() else {
                    // trim-gap
                    unimplemented!("Handling trim gap is currently not supported")
                };
                anyhow::Ok((lsn, envelope?))
            })
            .try_take_while(|entry| {
                // a catch-all safety net if all lower layers didn't filter this record out. This
                // could happen for old records that didn't store `Keys` in the log store.
                //
                // At some point, we should remove this and trust that stored records have Keys
                // stored correctly.
                std::future::ready(Ok(entry
                    .as_ref()
                    .is_ok_and(|(_, envelope)| envelope.matches_key_query(&key_query))))
            });

        info!("PartitionProcessor starting up.");

        // avoid synchronized timers. We pick a randomised timer between 500 and 1023 millis.
        let mut status_update_timer =
            tokio::time::interval(Duration::from_millis(500 + rand::random::<u64>() % 524));
        status_update_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut cancellation = std::pin::pin!(cancellation_watcher());
        let partition_id_str: &'static str = Box::leak(Box::new(self.partition_id.to_string()));
        // Telemetry setup
        let apply_command_latency =
            histogram!(PP_APPLY_COMMAND_DURATION, PARTITION_LABEL => partition_id_str);
        let record_actions_latency = histogram!(PARTITION_LEADER_HANDLE_ACTION_BATCH_DURATION);
        let command_batch_size =
            histogram!(PP_APPLY_COMMAND_BATCH_SIZE, PARTITION_LABEL => partition_id_str);
        let actuator_effects_handled = counter!(PARTITION_ACTUATOR_HANDLED);

        let mut action_collector = ActionCollector::default();
        let mut command_buffer = Vec::with_capacity(self.max_command_batch_size);

        loop {
            tokio::select! {
                _ = &mut cancellation => break,
                Some(command) = self.control_rx.recv() => {
                    if let Err(err) = self.on_command(command).await {
                        warn!("Failed executing command: {err}");
                    }
                }
                _ = status_update_timer.tick() => {
                    self.status_watch_tx.send_modify(|old| {
                        old.clone_from(&self.status);
                        old.updated_at = MillisSinceEpoch::now();
                    });
                }
                operation = Self::read_commands(&mut log_reader, self.max_command_batch_size, &mut command_buffer) => {
                    // check that reading has succeeded
                    operation?;

                    command_batch_size.record(command_buffer.len() as f64);

                    let mut transaction = partition_store.transaction();

                    // clear buffers used when applying the next record
                    action_collector.clear();

                    for (lsn, envelope) in command_buffer.drain(..) {
                        let command_start = Instant::now();

                        trace!(%lsn, "Processing bifrost record for '{}': {:?}", envelope.command.name(), envelope.header);

                        let leadership_change = self.apply_record(
                            lsn,
                            envelope,
                            &mut transaction,
                            &mut action_collector).await?;

                        apply_command_latency.record(command_start.elapsed());

                        if let Some((header, announce_leader)) = leadership_change {
                            // commit all changes so far, this is important so that the actuators see all changes
                            // when becoming leader.
                            transaction.commit().await?;

                            // We can ignore all actions collected so far because as a new leader we have to instruct the
                            // actuators afresh.
                            action_collector.clear();

                            self.status.last_observed_leader_epoch = Some(announce_leader.leader_epoch);
                            if header.source.is_processor_generational() {
                                let Source::Processor { generational_node_id, .. } = header.source else {
                                    unreachable!("processor source must have generational_node_id");
                                };
                                // all new AnnounceLeader messages should come from a PartitionProcessor
                                self.status.last_observed_leader_node = generational_node_id;
                            } else if announce_leader.node_id.is_some() {
                                // older AnnounceLeader messages have the announce_leader.node_id set
                                self.status.last_observed_leader_node = announce_leader.node_id;
                            }

                            let is_leader = self.leadership_state.on_announce_leader(announce_leader, &mut partition_store).await?;

                            Span::current().record("is_leader", is_leader);

                            if is_leader {
                                self.status.effective_mode = RunMode::Leader;
                            } else {
                                self.status.effective_mode = RunMode::Follower;
                            }

                            transaction = partition_store.transaction();
                        }
                    }

                    // Commit our changes and notify actuators about actions if we are the leader
                    transaction.commit().await?;
                    let actions_start = Instant::now();
                    self.leadership_state.handle_actions(action_collector.drain(..)).await?;
                    record_actions_latency.record(actions_start.elapsed());
                },
                Some(action_effects) = self.leadership_state.next_action_effects() => {
                    actuator_effects_handled.increment(action_effects.len() as u64);
                    self.leadership_state.handle_action_effect(action_effects).await?;
                },
            }
            // Allow other tasks on this thread to run, but only if we have exhausted the coop
            // budget.
            tokio::task::consume_budget().await;
        }

        debug!(restate.node = %metadata().my_node_id(), %self.partition_id, "Shutting partition processor down.");
        // ignore errors that happen during shut down
        let _ = self.leadership_state.step_down().await;

        Ok(())
    }

    async fn on_command(
        &mut self,
        command: PartitionProcessorControlCommand,
    ) -> anyhow::Result<()> {
        match command {
            PartitionProcessorControlCommand::RunForLeader(leader_epoch) => {
                self.status.planned_mode = RunMode::Leader;
                self.leadership_state
                    .run_for_leader(leader_epoch)
                    .await
                    .context("failed handling RunForLeader command")?;
            }
            PartitionProcessorControlCommand::StepDown => {
                self.status.planned_mode = RunMode::Follower;
                self.leadership_state
                    .step_down()
                    .await
                    .context("failed handling StepDown command")?;
            }
            PartitionProcessorControlCommand::CreateSnapshot(maybe_sender) => {
                if self
                    .inflight_create_snapshot_task
                    .as_ref()
                    .is_some_and(|task| !task.is_finished())
                {
                    warn!("Snapshot creation already in progress, rejecting request");
                    maybe_sender
                        .and_then(|tx| tx.send(Err(anyhow!("Snapshot creation in progress"))).ok());
                    return Ok(());
                }

                let config = self.configuration.live_load();
                let snapshot_source = SnapshotSource {
                    cluster_name: config.common.cluster_name().into(),
                    node_name: config.common.node_name().into(),
                };
                let snapshot_base_path = config.worker.snapshots.snapshots_dir(self.partition_id);
                let partition_store = self.partition_store.clone();
                let snapshot_span = tracing::info_span!("create-snapshot");
                let inflight_create_snapshot_task = restate_core::task_center().spawn_unmanaged(
                    TaskKind::PartitionSnapshotProducer,
                    "create-snapshot",
                    Some(self.partition_id),
                    async move {
                        let result = SnapshotProducer::create(
                            snapshot_source,
                            partition_store,
                            snapshot_base_path,
                        )
                        .await;

                        if let Some(tx) = maybe_sender {
                            tx.send(result.map(|metadata| metadata.snapshot_id)).ok();
                        }
                        Ok(())
                    }
                    .instrument(snapshot_span),
                )?;

                self.inflight_create_snapshot_task
                    .replace(inflight_create_snapshot_task);
            }
        }

        Ok(())
    }

    async fn apply_record<'a, 'b: 'a>(
        &mut self,
        lsn: Lsn,
        envelope: Arc<Envelope>,
        transaction: &mut PartitionStoreTransaction<'b>,
        action_collector: &mut ActionCollector,
    ) -> Result<Option<(Header, AnnounceLeader)>, state_machine::Error> {
        transaction.put_applied_lsn(lsn).await;

        // Update replay status
        self.status.last_applied_log_lsn = Some(lsn);
        self.status.last_record_applied_at = Some(MillisSinceEpoch::now());
        match self.status.replay_status {
            ReplayStatus::CatchingUp
                if self
                    .status
                    .target_tail_lsn
                    .is_some_and(|tail| lsn.next() >= tail) =>
            {
                // finished catching up
                self.status.replay_status = ReplayStatus::Active;
            }
            _ => {}
        };

        if let Some(dedup_information) = self.is_targeted_to_me(&envelope.header) {
            // deduplicate if deduplication information has been provided
            if let Some(dedup_information) = dedup_information {
                if Self::is_outdated_or_duplicate(dedup_information, transaction).await? {
                    debug!(
                        "Ignoring outdated or duplicate message: {:?}",
                        envelope.header
                    );
                    return Ok(None);
                }
                transaction
                    .put_dedup_seq_number(
                        dedup_information.producer_id.clone(),
                        &dedup_information.sequence_number,
                    )
                    .await;
            }

            // todo: check whether it's worth passing the arc further down
            let envelope = Arc::unwrap_or_clone(envelope);

            if let Command::AnnounceLeader(announce_leader) = envelope.command {
                // leadership change detected, let's finish our transaction here
                return Ok(Some((envelope.header, announce_leader)));
            } else {
                self.state_machine
                    .apply(
                        envelope.command,
                        transaction,
                        action_collector,
                        self.leadership_state.is_leader(),
                    )
                    .await?;
            }
        } else {
            self.status.num_skipped_records += 1;
            trace!(
                "Ignore message which is not targeted to me: {:?}",
                envelope.header
            );
        }

        Ok(None)
    }

    fn is_targeted_to_me<'a>(&self, header: &'a Header) -> Option<&'a Option<DedupInformation>> {
        match &header.dest {
            Destination::Processor {
                partition_key,
                dedup,
            } if self.partition_key_range.contains(partition_key) => Some(dedup),
            _ => None,
        }
    }

    async fn is_outdated_or_duplicate(
        dedup_information: &DedupInformation,
        dedup_resolver: &mut PartitionStoreTransaction<'_>,
    ) -> Result<bool, StorageError> {
        let last_dsn = dedup_resolver
            .get_dedup_sequence_number(&dedup_information.producer_id)
            .await?;

        // Check whether we have seen this message before
        let is_duplicate = if let Some(last_dsn) = last_dsn {
            match (last_dsn, &dedup_information.sequence_number) {
                (DedupSequenceNumber::Esn(last_esn), DedupSequenceNumber::Esn(esn)) => last_esn >= *esn,
                (DedupSequenceNumber::Sn(last_sn), DedupSequenceNumber::Sn(sn)) => last_sn >= *sn,
                (last_dsn, dsn) => panic!("sequence number types do not match: last sequence number '{:?}', received sequence number '{:?}'", last_dsn, dsn),
            }
        } else {
            false
        };

        Ok(is_duplicate)
    }

    /// Tries to read as many records from the `log_reader` as are immediately available and stops
    /// reading at `max_batching_size`.
    async fn read_commands<S>(
        log_reader: &mut S,
        max_batching_size: usize,
        record_buffer: &mut Vec<(Lsn, Arc<Envelope>)>,
    ) -> anyhow::Result<()>
    where
        S: Stream<Item = Result<anyhow::Result<(Lsn, Arc<Envelope>)>, restate_bifrost::Error>>
            + Unpin,
    {
        // beyond this point we must not await; otherwise we are no longer cancellation safe
        let first_record = log_reader.next().await;

        let Some(first_record) = first_record else {
            // read stream terminated!
            anyhow::bail!("Read stream terminated for partition processor");
        };

        record_buffer.clear();
        record_buffer.push(first_record??);

        while record_buffer.len() < max_batching_size {
            // read more message from the stream but only if they are immediately available
            if let Some(record) = log_reader.next().now_or_never() {
                let Some(record) = record else {
                    // read stream terminated!
                    anyhow::bail!("Read stream terminated for partition processor");
                };

                record_buffer.push(record??);
            } else {
                // no more immediately available records found
                break;
            }
        }

        Ok(())
    }
}
