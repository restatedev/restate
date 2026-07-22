// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use metrics::gauge;
use tokio::sync::watch;
use tracing::info;

use restate_partition_store::PartitionStore;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_types::cluster::cluster_state::PartitionProcessorStatus;
use restate_types::identifiers::LeaderEpoch;
use restate_types::logs::{LogId, Lsn};
use restate_types::partitions::{Partition, PersistedFeatures};
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::{SemanticRestateVersion, Version};
use restate_util_time::DurationExt;
use restate_vqueues::context::{HasVQueues, HasVQueuesMut};
use restate_vqueues::{VQueuesMeta, VQueuesMetaCache};
use restate_worker_api::processor::*;

use crate::metric_definitions::{
    FLARE_REASON_VERSION_BARRIER, PARTITION_BLOCKED_FLARE, PARTITION_LABEL, REASON_LABEL,
};
use crate::partition::ProcessorError;
use crate::partition::leadership::trim_queue::{HasTrimQueue, TrimQueue};

use super::dedup::Dedup;
use super::outbox::Outbox;
use super::status::{HasStatus, HasStatusMut, Status};
use super::{DedupAccess, DedupMut, Fsm, HasDedup, HasDedupMut};

// Soft cap for the in-memory vqueue cache; once reached, inactive
// entries are evicted at insert time. The cache will still grow past
// this if compaction frees nothing.
const VQUEUE_CACHE_CAPACITY: usize = 10_000;

pub struct ProcessorRawContext {
    /// Metadata of the partition
    partition: Arc<Partition>,
    /// Live status tracking of this processor
    status: Status,
    /// Caches the latest FSM state of the partition processor
    fsm_cache: Fsm,
    /// Deduplicates incoming commands
    dedup_cache: Dedup,
    /// Tracks the partition outbox (operations crossing the partition-key boundary)
    outbox: Outbox,
    /// The trim queue managed by this processor
    trim_queue: TrimQueue,
    /// The vqueues metadata cache
    vqueues: VQueuesMetaCache,
}

impl ProcessorRawContext {
    pub async fn create(
        current_restate_version: &SemanticRestateVersion,
        partition_store: &mut PartitionStore,
    ) -> Result<Self, ProcessorError> {
        let partition = partition_store.partition().clone();
        let mut fsm_cache = Fsm::create(partition_store).await?;
        // When not verified, we reject starting the processor because it the data
        // on disk requires a higher version than the current server version. At
        // this point, the only piece of data that can be assumed to be correct is
        // the minimum restate version persisted on the partition store.
        if !fsm_cache
            .verify_and_run_migrations(partition_store, current_restate_version, false)
            .await?
        {
            gauge!(PARTITION_BLOCKED_FLARE, PARTITION_LABEL =>
                partition.id().to_string(),
                REASON_LABEL => FLARE_REASON_VERSION_BARRIER
            )
            .set(1);
            return Err(ProcessorError::VersionBarrier {
                required_min_version: fsm_cache.min_restate_version().clone(),
                barrier_reason: String::new(),
                feature_changes: Vec::default(),
            });
        }

        let dedup_cache = Dedup::create(partition_store).await?;

        let outbox = Outbox::create(partition_store).await?;

        // Choosing the biggest epoch we observed through deduplication because it's the signal
        // that complies with the requirements of the gossip protocol. We require that we announce
        // leader epochs that we believe its AnnounceLeader command has been committed to the log.
        // In the case of deduplication table, we know the epoch number but we don't know the leader
        // node-id.
        let mut status = Status::new();
        status.set_last_observed_leader_epoch(dedup_cache.my_dedup_epoch());

        let trim_queue = TrimQueue::default();
        if let Some(partition_durability) = fsm_cache.durable_point() {
            trim_queue.push(partition_durability);
        }

        let vqueues = VQueuesMetaCache::create(
            partition_store.partition_db().clone(),
            VQUEUE_CACHE_CAPACITY,
        )
        .await?;

        Ok(Self {
            partition,
            fsm_cache,
            dedup_cache,
            outbox,
            status,
            trim_queue,
            vqueues,
        })
    }

    /// Builds an in-memory context with empty caches and no backing storage,
    /// for use as a test double. Feature flags are injected directly instead of
    /// being loaded from the partition store.
    #[cfg(test)]
    pub fn new(partition: Arc<Partition>, enabled_features: PersistedFeatures) -> Self {
        Self {
            partition,
            status: Status::new(),
            fsm_cache: Fsm::new(enabled_features),
            dedup_cache: Dedup::new_empty(),
            outbox: Outbox::new_empty(),
            trim_queue: TrimQueue::default(),
            vqueues: VQueuesMetaCache::new_empty(VQUEUE_CACHE_CAPACITY),
        }
    }

    /// Seeds the in-memory outbox head/tail without persisting it. Test-only,
    /// lets partition-command tests start from a partially-truncated outbox.
    #[cfg(test)]
    pub fn seed_outbox_in_memory(
        &mut self,
        tail: restate_types::message::MessageIndex,
        head: Option<restate_types::message::MessageIndex>,
    ) {
        self.outbox = Outbox::seed(tail, head);
    }

    /// Overwrites the in-memory feature set without persisting it. Test-only,
    /// mirrors the old `StateMachine`-embedded feature cache mutation.
    #[cfg(test)]
    pub fn set_enabled_features_in_memory(&mut self, enabled_features: PersistedFeatures) {
        self.fsm_cache
            .set_enabled_features_in_memory(enabled_features);
    }

    pub fn enabled_features(&self) -> &PersistedFeatures {
        self.fsm_cache.enabled_features()
    }

    pub fn release_applied_lsn(&mut self) {
        // Notify all lsn watchers that the lsn has been committed
        self.fsm_cache.release_applied_lsn();
    }

    pub fn set_catchup_lsn(&mut self, catch_up_tail: Lsn) {
        let id = self.partition.id();
        let last_applied_lsn = self.fsm().last_applied_lsn();
        self.status
            .set_catchup_lsn(id, catch_up_tail, last_applied_lsn);
    }

    pub fn merge_with_status(&mut self, other: &mut PartitionProcessorStatus) {
        other.last_applied_log_lsn = Some(self.fsm_cache.last_applied_lsn());
        other.last_applied_schema_version = Some(self.fsm_cache.schema_version());
        let rule_book_version = self.fsm_cache.rule_book_version();
        other.last_applied_rule_book_version =
            (rule_book_version >= Version::MIN).then_some(rule_book_version);
        other.enabled_features = *self.fsm_cache.enabled_features();
        other.planned_mode = self.status.planned_mode();
        other.replay_status = self.status.replay_status();
        other.target_tail_lsn = self.status.target_tail_lsn();
        other.last_record_applied_at = self.status.last_lsn_applied_at();
        other.last_observed_leader_node = self.status.last_observed_leader_node();
        other.last_observed_leader_epoch = if self.status.last_observed_leader_epoch().is_valid() {
            Some(self.status.last_observed_leader_epoch())
        } else {
            None
        };
    }

    pub fn update_last_applied_lsn<S: WriteFsmTable>(
        &mut self,
        txn: &mut S,
        lsn: Lsn,
    ) -> Result<(), ProcessorError> {
        self.fsm_cache.set_last_applied_lsn(txn, lsn);
        if self.status.update_last_applied_lsn(lsn) {
            info!(
                "Partition {} caught up in {}!",
                self.partition_id(),
                self.status.started_at().elapsed().friendly()
            );
        }
        Ok(())
    }

    pub fn subscribe_to_last_applied_lsn(&self) -> watch::Receiver<Lsn> {
        self.fsm_cache.subscribe_to_last_applied_lsn()
    }
}

impl Processor for ProcessorRawContext {
    #[inline]
    fn log_id(&self) -> LogId {
        self.partition.log_id()
    }

    #[inline]
    fn partition_id(&self) -> PartitionId {
        self.partition.id()
    }

    fn current_leader_epoch(&self) -> LeaderEpoch {
        self.dedup_cache.my_dedup_epoch()
    }

    #[inline]
    fn key_range(&self) -> KeyRange {
        self.partition.key_range
    }
}

impl HasDedup for ProcessorRawContext {
    #[inline]
    fn dedup(&self) -> impl DedupAccess {
        &self.dedup_cache
    }
}

impl HasDedupMut for ProcessorRawContext {
    #[inline]
    fn dedup_mut(&mut self) -> impl DedupMut {
        &mut self.dedup_cache
    }
}

impl HasOutbox for ProcessorRawContext {
    #[inline]
    fn outbox(&self) -> impl OutboxAccess {
        &self.outbox
    }
}

impl HasOutboxMut for ProcessorRawContext {
    #[inline]
    fn outbox_mut(&mut self) -> impl OutboxMut {
        &mut self.outbox
    }
}

impl HasFsm for ProcessorRawContext {
    #[inline]
    fn fsm(&self) -> impl FsmAccess {
        &self.fsm_cache
    }
}

impl HasFsmMut for ProcessorRawContext {
    #[inline]
    fn fsm_mut(&mut self) -> impl FsmMut {
        &mut self.fsm_cache
    }
}

impl HasTrimQueue for ProcessorRawContext {
    #[inline]
    fn trim_queue(&self) -> &TrimQueue {
        &self.trim_queue
    }
}

impl HasVQueues for ProcessorRawContext {
    #[inline]
    fn vqueues(&self) -> VQueuesMeta<'_> {
        self.vqueues.view()
    }
}

impl HasVQueuesMut for ProcessorRawContext {
    #[inline]
    fn vqueues_mut(&mut self) -> &mut VQueuesMetaCache {
        &mut self.vqueues
    }
}

impl HasStatus for ProcessorRawContext {
    #[inline]
    fn status(&self) -> &Status {
        &self.status
    }
}

impl HasStatusMut for ProcessorRawContext {
    #[inline]
    fn status_mut(&mut self) -> &mut Status {
        &mut self.status
    }
}
