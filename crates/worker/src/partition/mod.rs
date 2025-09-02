// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod cleaner;
pub mod invoker_storage_reader;
mod leadership;
mod rpc;
pub mod shuffle;
mod state_machine;
pub mod types;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use assert2::let_assert;
use enumset::EnumSet;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt as _};
use metrics::{SharedString, gauge, histogram};
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tracing::{Span, debug, error, info, instrument, trace, warn};

use restate_bifrost::Bifrost;
use restate_bifrost::loglet::FindTailOptions;
use restate_core::network::{Oneshot, Reciprocal, ServiceMessage, Verdict};
use restate_core::{ShutdownError, cancellation_watcher, my_node_id};
use restate_partition_store::{PartitionStore, PartitionStoreTransaction};
use restate_storage_api::deduplication_table::{
    DedupInformation, DedupSequenceNumber, DeduplicationTable, ProducerId,
    ReadOnlyDeduplicationTable,
};
use restate_storage_api::fsm_table::{FsmTable, PartitionDurability, ReadOnlyFsmTable};
use restate_storage_api::outbox_table::ReadOnlyOutboxTable;
use restate_storage_api::{StorageError, Transaction};
use restate_types::cluster::cluster_state::{PartitionProcessorStatus, ReplayStatus, RunMode};
use restate_types::config::Configuration;
use restate_types::identifiers::LeaderEpoch;
use restate_types::logs::MatchKeyQuery;
use restate_types::logs::{KeyFilter, Lsn, SequenceNumber};
use restate_types::net::RpcRequest;
use restate_types::net::partition_processor::{
    PartitionLeaderService, PartitionProcessorRpcError, PartitionProcessorRpcRequest,
    PartitionProcessorRpcResponse,
};
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::retries::{RetryPolicy, with_jitter};
use restate_types::storage::StorageDecodeError;
use restate_types::time::{MillisSinceEpoch, NanosSinceEpoch};
use restate_types::{GenerationalNodeId, SemanticRestateVersion};
use restate_wal_protocol::control::AnnounceLeader;
use restate_wal_protocol::{Command, Destination, Envelope, Header, Source};

use self::leadership::trim_queue::TrimQueue;
use crate::metric_definitions::{
    PARTITION_BLOCKED_FLARE, PARTITION_LABEL, PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS,
};
use crate::partition::invoker_storage_reader::InvokerStorageReader;
use crate::partition::leadership::LeadershipState;
use crate::partition::state_machine::{ActionCollector, StateMachine};

/// Target leader state of the partition processor.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum TargetLeaderState {
    Leader(LeaderEpoch),
    #[default]
    Follower,
}

#[derive(Debug)]
pub(super) struct PartitionProcessorBuilder<InvokerInputSender> {
    status: PartitionProcessorStatus,
    invoker_tx: InvokerInputSender,
    target_leader_state_rx: watch::Receiver<TargetLeaderState>,
    network_svc_rx: mpsc::Receiver<ServiceMessage<PartitionLeaderService>>,
    status_watch_tx: watch::Sender<PartitionProcessorStatus>,
}

impl<InvokerInputSender> PartitionProcessorBuilder<InvokerInputSender>
where
    InvokerInputSender:
        restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>> + Clone,
{
    pub(super) fn new(
        status: PartitionProcessorStatus,
        target_leader_state_rx: watch::Receiver<TargetLeaderState>,
        network_svc_rx: mpsc::Receiver<ServiceMessage<PartitionLeaderService>>,
        status_watch_tx: watch::Sender<PartitionProcessorStatus>,
        invoker_tx: InvokerInputSender,
    ) -> Self {
        Self {
            status,
            invoker_tx,
            target_leader_state_rx,
            network_svc_rx,
            status_watch_tx,
        }
    }

    pub async fn build(
        self,
        bifrost: Bifrost,
        mut partition_store: PartitionStore,
        replica_set_states: PartitionReplicaSetStates,
    ) -> Result<PartitionProcessor<InvokerInputSender>, state_machine::Error> {
        let PartitionProcessorBuilder {
            invoker_tx,
            target_leader_state_rx,
            network_svc_rx: rpc_rx,
            status_watch_tx,
            status,
            ..
        } = self;

        let partition_id_str = SharedString::from(partition_store.partition_id().to_string());
        let state_machine = Self::create_state_machine(&mut partition_store).await?;

        let trim_queue = TrimQueue::default();
        if let Some(ref partition_durability) = partition_store.get_partition_durability().await? {
            trim_queue.push(partition_durability);
        }

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

        if let Some(last_leader_epoch) = last_seen_leader_epoch {
            replica_set_states.note_observed_leader(
                partition_store.partition_id(),
                restate_types::partitions::state::LeadershipState {
                    current_leader_epoch: last_leader_epoch,
                    // we don't know the old leader node-id, another node might update it
                    current_leader: GenerationalNodeId::INVALID,
                },
            );
        }

        let leadership_state = LeadershipState::new(
            Arc::clone(partition_store.partition()),
            invoker_tx,
            bifrost.clone(),
            last_seen_leader_epoch,
            trim_queue.clone(),
        );

        Ok(PartitionProcessor {
            partition_id_str,
            leadership_state,
            state_machine,
            partition_store,
            bifrost,
            target_leader_state_rx,
            network_leader_svc_rx: rpc_rx,
            status_watch_tx,
            status,
            replica_set_states,
            trim_queue,
        })
    }

    async fn create_state_machine(
        partition_store: &mut PartitionStore,
    ) -> Result<StateMachine, state_machine::Error> {
        let inbox_seq_number = partition_store.get_inbox_seq_number().await?;
        let outbox_seq_number = partition_store.get_outbox_seq_number().await?;
        let outbox_head_seq_number = partition_store.get_outbox_head_seq_number().await?;
        let min_restate_version = partition_store.get_min_restate_version().await?;

        if !SemanticRestateVersion::current().is_equal_or_newer_than(&min_restate_version) {
            gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL =>
                partition_store.partition_id().to_string())
            .set(1);
            return Err(state_machine::Error::VersionBarrier {
                required_min_version: min_restate_version,
                barrier_reason: String::new(),
            });
        }

        let state_machine = StateMachine::new(
            inbox_seq_number,
            outbox_seq_number,
            outbox_head_seq_number,
            partition_store.partition_key_range().clone(),
            min_restate_version,
            EnumSet::empty(),
        );

        Ok(state_machine)
    }
}

pub struct PartitionProcessor<InvokerSender> {
    partition_id_str: SharedString,
    leadership_state: LeadershipState<InvokerSender>,
    state_machine: StateMachine,
    bifrost: Bifrost,
    target_leader_state_rx: watch::Receiver<TargetLeaderState>,
    network_leader_svc_rx: mpsc::Receiver<ServiceMessage<PartitionLeaderService>>,
    status_watch_tx: watch::Sender<PartitionProcessorStatus>,
    status: PartitionProcessorStatus,
    replica_set_states: PartitionReplicaSetStates,

    partition_store: PartitionStore,
    trim_queue: TrimQueue,
}

#[derive(Debug, derive_more::Display, thiserror::Error)]
pub enum ProcessorError {
    /// Indicates that the processor encountered a trim gap in the log.
    /// This is a signal to the PartitionProcessorManager to attempt to restart
    /// the processor for this partition. This might occur after the first startup
    /// of a worker that's been down while a log trim occurred, and recoverable
    /// as long as we can find a snapshot with a min LSN of trim_gap_end or later.
    #[display("[{read_pointer}..{trim_gap_end}]")]
    TrimGapEncountered {
        read_pointer: Lsn,
        trim_gap_end: Lsn,
    },
    Storage(#[from] StorageError),
    Decode(#[from] StorageDecodeError),
    Bifrost(#[from] restate_bifrost::Error),
    StoreOpen(#[from] restate_partition_store::OpenError),
    StateMachine(#[from] state_machine::Error),
    ActionEffect(#[from] leadership::Error),
    ShutdownError(#[from] ShutdownError),
    LogReadStreamTerminated,
    Other(#[from] anyhow::Error),
}

struct LsnEnvelope {
    pub lsn: Lsn,
    pub created_at: NanosSinceEpoch,
    pub envelope: Arc<Envelope>,
}

impl<InvokerSender> PartitionProcessor<InvokerSender>
where
    InvokerSender: restate_invoker_api::InvokerHandle<InvokerStorageReader<PartitionStore>> + Clone,
{
    #[instrument(
        level = "error", skip_all,
        fields(partition_id = %self.partition_store.partition_id())
    )]
    pub async fn run(mut self) -> Result<(), ProcessorError> {
        debug!("Starting the partition processor.");

        let res = tokio::select! {
            res = self.run_inner() => {
                match res.as_ref() {
                    // run_inner never returns normally
                    Ok(_) => warn!("Shutting partition processor down because it stopped unexpectedly."),
                    Err(ProcessorError::TrimGapEncountered { trim_gap_end, read_pointer }) =>
                        info!(
                            %read_pointer,
                            %trim_gap_end,
                            "Shutting partition processor down because it encountered a trim gap in the log."
                        ),
                    Err(ProcessorError::StateMachine(state_machine::Error::VersionBarrier { .. })) => {
                        gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL => self.partition_id_str.clone()).set(1);
                    }
                    Err(err) => warn!("Shutting partition processor down because of error: {err}"),
                }
                res
            },
            _ = cancellation_watcher() => {
                debug!("Shutting partition processor down because it was cancelled.");
                Ok(())
            },
        };

        // clean up pending rpcs and stop child tasks
        self.leadership_state.step_down().await;

        // Drain leader network service
        self.network_leader_svc_rx.close();
        while let Some(msg) = self.network_leader_svc_rx.recv().await {
            // signals that we are not the leader anymore
            msg.fail(Verdict::SortCodeNotFound);
        }

        res
    }

    async fn run_inner(&mut self) -> Result<(), ProcessorError> {
        let mut partition_store = self.partition_store.clone();

        // Run migrations
        partition_store.verify_and_run_migrations().await?;

        let last_applied_lsn = partition_store
            .get_applied_lsn()
            .await?
            .unwrap_or(Lsn::INVALID);

        let log_id = self.partition_store.partition().log_id();
        let partition_id = self.partition_store.partition_id();
        let my_node = my_node_id().as_plain();

        self.status.last_applied_log_lsn = Some(last_applied_lsn);
        let mut durable_lsn_watch = self.partition_store.get_durable_lsn().await?;
        let durable_lsn = durable_lsn_watch
            .borrow_and_update()
            .unwrap_or(Lsn::INVALID);
        self.status.last_persisted_log_lsn = Some(durable_lsn);
        self.replica_set_states
            .note_durable_lsn(partition_id, my_node, durable_lsn);

        // If the underlying log is not provisioned, now is the time to provision it.
        // We'll retry a few times before giving back control to PPM
        //
        // The primary reason for retries is the initial cluster provision case where nodes might
        // still be starting up and we don't have enough nodes to form legal nodesets.
        let mut retries = RetryPolicy::exponential(
            Duration::from_secs(1),
            1.5,
            Some(3),
            Some(Duration::from_secs(5)),
        )
        .into_iter();
        while let Err(e) = self.bifrost.admin().ensure_log_exists(log_id).await {
            // We cannot provision the log for this partition
            if let Some(dur) = retries.next() {
                debug!(
                    "Cannot create a bifrost log for partition {}, will retry in {:?}; reason={}",
                    partition_id, dur, e
                );
                tokio::time::sleep(dur).await;
            } else {
                return Err(e.into());
            }
        }

        debug!("Finding tail for partition",);
        // propagate errors and let the PPM handle error retries
        let current_tail = self
            .bifrost
            .find_tail(log_id, FindTailOptions::ConsistentRead)
            .await?;

        debug!(
            last_applied_lsn = %last_applied_lsn,
            current_log_tail = %current_tail,
            "Partition creating log reader",
        );
        if current_tail.offset() == last_applied_lsn.next() {
            if self.status.replay_status != ReplayStatus::Active {
                debug!(
                    %last_applied_lsn,
                    "Processor has caught up with the log tail."
                );
                self.status.target_tail_lsn = None;
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
                %last_applied_lsn,
                log_tail_lsn = %current_tail.offset(),
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

        let mut live_config = Configuration::live();

        // Telemetry setup
        let leader_record_write_to_read_latency =
            histogram!(PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS, "leader" => "1");
        let follower_record_write_to_read_latency =
            histogram!(PARTITION_RECORD_COMMITTED_TO_READ_LATENCY_SECONDS, "leader" => "0");
        // Start reading after the last applied lsn
        let key_query = KeyFilter::Within(self.partition_store.partition_key_range().clone());

        let mut record_stream = self
            .bifrost
            .create_reader(log_id, key_query.clone(), last_applied_lsn.next(), Lsn::MAX)?
            .map(|entry| match entry {
                Ok(entry) => {
                    trace!(?entry, "Read entry");
                    let lsn = entry.sequence_number();
                    if entry.is_data_record() {
                        let record = entry.into_record().unwrap();
                        let created_at = record.created_at();
                        let envelope = record.decode_arc()?;
                        Ok(LsnEnvelope {
                            lsn,
                            created_at,
                            envelope,
                        })
                    } else {
                        Err(ProcessorError::TrimGapEncountered {
                            trim_gap_end: entry
                                .trim_gap_to_sequence_number()
                                .expect("trim gap has to-LSN"),
                            read_pointer: entry.sequence_number(),
                        })
                    }
                }
                Err(err) => Err(ProcessorError::from(err)),
            })
            .try_take_while(|record| {
                // a catch-all safety net if all lower layers didn't filter this record out. This
                // could happen for old records that didn't store `Keys` in the log store.
                //
                // At some point, we should remove this and trust that stored records have Keys
                // stored correctly.
                std::future::ready(Ok(record.envelope.matches_key_query(&key_query)))
            });

        // avoid synchronized timers.
        let mut status_update_timer =
            tokio::time::interval(with_jitter(Duration::from_millis(500), 0.5));
        status_update_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        let mut action_collector = ActionCollector::default();
        let mut command_buffer =
            Vec::with_capacity(live_config.live_load().worker.max_command_batch_size());

        let mut watch_leader_changes = self.replica_set_states.watch_leadership_state(partition_id);
        watch_leader_changes.mark_changed();

        info!("Partition {} started", partition_id);

        loop {
            let config = live_config.live_load();
            tokio::select! {
                _ = self.target_leader_state_rx.changed() => {
                    let target_leader_state = *self.target_leader_state_rx.borrow_and_update();
                    self.on_target_leader_state(target_leader_state).await.context("failed handling target leader state change")?;
                }
                Ok(()) = watch_leader_changes.changed() => {
                    // cloning to avoid holding the underlying RwLock.
                    let new_state = *watch_leader_changes.borrow_and_update();
                    if self.status.last_observed_leader_epoch.is_none_or(|last| last < new_state.current_leader_epoch) {
                        self.status.last_observed_leader_epoch = Some(new_state.current_leader_epoch);
                        if new_state.current_leader.is_valid() {
                            self.status.last_observed_leader_node = Some(new_state.current_leader);
                        }
                    }
                    self.leadership_state.maybe_step_down(new_state.current_leader_epoch, new_state.current_leader).await;
                    self.status.effective_mode = self.leadership_state.effective_mode();
                }
                Some(msg) = self.network_leader_svc_rx.recv() => {
                    match msg {
                        ServiceMessage::Rpc(msg) if msg.msg_type() == PartitionProcessorRpcRequest::TYPE => {
                            let msg = msg.into_typed::<PartitionProcessorRpcRequest>();
                            // note: split() decodes the payload
                            let (response_tx, body) = msg.split();
                            self.on_rpc(response_tx, body, &mut partition_store).await;
                        }
                        msg => { msg.fail(Verdict::MessageUnrecognized); }
                    }
                }
                _ = status_update_timer.tick() => {
                    if durable_lsn_watch.has_changed().map_err(|e| ProcessorError::Other(e.into()))? {
                        let durable_lsn = durable_lsn_watch
                                .borrow_and_update()
                                .unwrap_or(Lsn::INVALID);
                        self.status.last_persisted_log_lsn = Some(durable_lsn);
                        self.replica_set_states.note_durable_lsn(
                            partition_id,
                            my_node,
                            durable_lsn,
                        );
                    }
                    self.status_watch_tx.send_modify(|old| {
                        old.clone_from(&self.status);
                        old.updated_at = MillisSinceEpoch::now();
                    });
                }
                operation = Self::read_commands(&mut record_stream, config.worker.max_command_batch_size(), &mut command_buffer) => {
                    // check that reading has succeeded
                    operation?;

                    let mut transaction = partition_store.transaction();

                    // clear buffers used when applying the next record
                    action_collector.clear();

                    for record in command_buffer.drain(..) {
                        trace!(lsn = %record.lsn, "Processing bifrost record for '{}': {:?}", record.envelope.command.name(), record.envelope.header);

                        if self.leadership_state.is_leader() {
                            leader_record_write_to_read_latency.record(record.created_at.elapsed());
                        } else {
                            follower_record_write_to_read_latency.record(record.created_at.elapsed());
                        }
                        let leadership_change = self.apply_record(
                            record,
                            &mut transaction,
                            &mut action_collector,
                        ).await?;

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
                            self.replica_set_states.note_observed_leader(
                                partition_id,
                                restate_types::partitions::state::LeadershipState {
                                    current_leader_epoch: announce_leader.leader_epoch,
                                    current_leader:
                                    self.status.last_observed_leader_node.unwrap_or(GenerationalNodeId::INVALID),
                                });

                            let is_leader = self.leadership_state.on_announce_leader(&announce_leader, &mut partition_store, &self.replica_set_states, config).await?;

                            Span::current().record("is_leader", is_leader);

                            if is_leader {
                                self.status.effective_mode = RunMode::Leader;
                            } else {
                                // make sure that we set our effective_mode to follower also when
                                // not being explicitly asked by the PPM
                                self.status.effective_mode = RunMode::Follower;
                            }

                            transaction = partition_store.transaction();
                        }
                    }

                    // Commit our changes and notify actuators about actions if we are the leader
                    transaction.commit().await?;
                    self.leadership_state.handle_actions(action_collector.drain(..)).await?;
                },
                result = self.leadership_state.run() => {
                    let action_effects = result?;
                    // We process the action_effects not directly in the run future because it
                    // requires the run future to be cancellation safe. In the future this could be
                    // implemented.
                    self.leadership_state.handle_action_effects(action_effects).await?;
                }
            }
            // Allow other tasks on this thread to run, but only if we have exhausted the coop
            // budget.
            tokio::task::consume_budget().await;
        }
    }

    async fn on_target_leader_state(
        &mut self,
        target_leader_state: TargetLeaderState,
    ) -> anyhow::Result<()> {
        match target_leader_state {
            TargetLeaderState::Leader(leader_epoch) => {
                self.status.planned_mode = RunMode::Leader;
                self.leadership_state
                    .run_for_leader(leader_epoch)
                    .await
                    .context("failed handling RunForLeader command")?;
            }
            TargetLeaderState::Follower => {
                self.status.planned_mode = RunMode::Follower;
                self.leadership_state.step_down().await;
                self.status.effective_mode = RunMode::Follower;
            }
        }

        Ok(())
    }

    async fn on_rpc(
        &mut self,
        response_tx: Reciprocal<
            Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
        body: PartitionProcessorRpcRequest,
        partition_store: &mut PartitionStore,
    ) {
        let _ = rpc::RpcHandler::handle(
            rpc::RpcContext::new(&mut self.leadership_state, partition_store),
            body,
            rpc::Replier::new(response_tx),
        )
        .await;
    }

    // --- Apply new commands/records

    async fn apply_record<'a, 'b: 'a>(
        &mut self,
        record: LsnEnvelope,
        transaction: &mut PartitionStoreTransaction<'b>,
        action_collector: &mut ActionCollector,
    ) -> Result<Option<(Header, Box<AnnounceLeader>)>, state_machine::Error> {
        transaction.put_applied_lsn(record.lsn).await?;

        // Update replay status
        self.status.last_applied_log_lsn = Some(record.lsn);
        self.status.last_record_applied_at = Some(MillisSinceEpoch::now());
        match self.status.replay_status {
            ReplayStatus::CatchingUp
                if self
                    .status
                    .target_tail_lsn
                    .is_some_and(|tail| record.lsn.next() >= tail) =>
            {
                // finished catching up
                self.status.replay_status = ReplayStatus::Active;
                self.status.target_tail_lsn = None;
            }
            _ => {}
        };

        if let Some(dedup_information) = self.is_targeted_to_me(&record.envelope.header) {
            // deduplicate if deduplication information has been provided
            if let Some(dedup_information) = dedup_information {
                if Self::is_outdated_or_duplicate(dedup_information, transaction).await? {
                    debug!(
                        "Ignoring outdated or duplicate message: {:?}",
                        record.envelope.header
                    );
                    return Ok(None);
                }
                transaction
                    .put_dedup_seq_number(
                        dedup_information.producer_id.clone(),
                        &dedup_information.sequence_number,
                    )
                    .await
                    .map_err(state_machine::Error::Storage)?;
            }

            // todo: redesign to pass the arc (or reference) further down
            let record_created_at = record.created_at;
            let record_lsn = record.lsn;
            let envelope = Arc::unwrap_or_clone(record.envelope);

            if let Command::AnnounceLeader(announce_leader) = envelope.command {
                // leadership change detected, let's finish our transaction here
                return Ok(Some((envelope.header, announce_leader)));
            } else if let Command::UpdatePartitionDurability(partition_durability) =
                envelope.command
            {
                if partition_durability.partition_id != self.partition_store.partition_id() {
                    self.status.num_skipped_records += 1;
                    trace!(
                        "Ignore update-partition-durability message which is not targeted to me. Message is for {} but I'm {}",
                        partition_durability.partition_id,
                        self.partition_store.partition_id()
                    );
                    return Ok(None);
                }

                let partition_durability = PartitionDurability {
                    modification_time: partition_durability.modification_time,
                    durable_point: partition_durability.durable_point,
                };
                if self.trim_queue.push(&partition_durability) {
                    transaction
                        .put_partition_durability(&partition_durability)
                        .await?;
                }
            } else {
                self.state_machine
                    .apply(
                        envelope.command,
                        record_created_at.into(),
                        record_lsn,
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
                record.envelope.header
            );
        }

        Ok(None)
    }

    fn is_targeted_to_me<'a>(&self, header: &'a Header) -> Option<&'a Option<DedupInformation>> {
        match &header.dest {
            Destination::Processor {
                partition_key,
                dedup,
            } if self
                .partition_store
                .partition_key_range()
                .contains(partition_key) =>
            {
                Some(dedup)
            }
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
                (DedupSequenceNumber::Esn(last_esn), DedupSequenceNumber::Esn(esn)) => {
                    last_esn >= *esn
                }
                (DedupSequenceNumber::Sn(last_sn), DedupSequenceNumber::Sn(sn)) => last_sn >= *sn,
                (last_dsn, dsn) => panic!(
                    "sequence number types do not match: last sequence number '{last_dsn:?}', received sequence number '{dsn:?}'"
                ),
            }
        } else {
            false
        };

        Ok(is_duplicate)
    }

    /// Tries to read as many records from the `log_reader` as are immediately available and stops
    /// reading at `max_batching_size`. Trim gaps will result in an immediate error.
    async fn read_commands<S>(
        log_reader: &mut S,
        max_batching_size: usize,
        record_buffer: &mut Vec<LsnEnvelope>,
    ) -> Result<(), ProcessorError>
    where
        S: Stream<Item = Result<LsnEnvelope, ProcessorError>> + Unpin,
    {
        // beyond this point we must not await; otherwise we are no longer cancellation safe
        let first_record = log_reader.next().await;

        let Some(first_record) = first_record else {
            return Err(ProcessorError::LogReadStreamTerminated);
        };

        record_buffer.clear();
        record_buffer.push(first_record?);

        while record_buffer.len() < max_batching_size {
            // read more message from the stream but only if they are immediately available
            if let Some(record) = log_reader.next().now_or_never() {
                let Some(record) = record else {
                    return Err(ProcessorError::LogReadStreamTerminated);
                };
                record_buffer.push(record?);
            } else {
                // no more immediately available records found
                break;
            }
        }

        Ok(())
    }
}
