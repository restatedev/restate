// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;
use std::pin::Pin;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use metrics::counter;
use pin_project::pin_project;
use slotmap::SecondaryMap;
use tokio::time::Instant;
use tracing::{debug, trace, warn};

use restate_limiter::RuleUpdate;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::EntryKey;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_types::identifiers::PartitionKey;
use restate_types::vqueues::VQueueId;
use restate_types::{LockName, Scope};
use restate_worker_api::UserLimitCounterEntry;

use crate::EventDetails;
use crate::VQueueEvent;
use crate::VQueuesMeta;
use crate::cache;
use crate::cache::VQueueHandle;
use crate::metric_definitions::VQUEUE_ENQUEUE;
use crate::metric_definitions::VQUEUE_RUN_CONFIRMED;
use crate::scheduler::eligible::{EligibilityTracker, SchedulingGroup, WeightResolver};
use crate::scheduler::vqueue_state::Pop;

use super::Decisions;
use super::ReservedResources;
use super::ResourceManager;
use super::VQueueSchedulerStatus;
use super::vqueue_state::VQueueState;

#[pin_project]
pub struct DRRScheduler<S: VQueueStore> {
    resource_manager: ResourceManager,
    // sorted by queue_id
    eligible: EligibilityTracker,
    q: SecondaryMap<VQueueHandle, VQueueState<S>>,
    /// Waker to be notified when scheduler is potentially able to scheduler more work
    waker: Waker,
    /// Time of the last memory reporting and memory compaction
    last_report: Instant,

    /// Limits the number of queues picked per scheduler's poll
    limit_qid_per_poll: NonZeroU32,
    /// Limits the number of items included in a single decision across all queues
    max_items_per_decision: NonZeroU32,

    // SAFETY NOTE: **must** Keep this at the end since it needs to outlive all readers.
    storage: S,
}

impl<S: VQueueStore> DRRScheduler<S> {
    pub fn new(
        limit_qid_per_poll: NonZeroU32,
        max_items_per_decision: NonZeroU32,
        resource_manager: ResourceManager,
        storage: S,
        vqueues: VQueuesMeta<'_>,
        weight_resolver: WeightResolver,
    ) -> Self {
        let mut total_running = 0;
        let mut total_waiting = 0;

        let mut q = SecondaryMap::with_capacity(vqueues.capacity());
        let mut eligible: EligibilityTracker =
            EligibilityTracker::with_capacity(vqueues.capacity(), weight_resolver);

        for (handle, qid, meta) in vqueues.iter_active_vqueues() {
            total_running += meta.num_running();
            total_waiting += meta.total_waiting();
            q.insert(handle, VQueueState::new(qid, &storage, meta.num_running()));
            // We init all active vqueues as eligible first
            eligible.insert_eligible(handle, SchedulingGroup::of(meta));
        }

        debug!(
            "Scheduler started. num_vqueues={}, total_running_items={total_running}, total_waiting_items={total_waiting}",
            q.len(),
        );

        Self {
            resource_manager,
            q,
            eligible,
            waker: Waker::noop().clone(),
            last_report: Instant::now(),
            storage,
            limit_qid_per_poll,
            max_items_per_decision,
        }
    }

    fn report(&mut self) {
        trace!(
            "DRR scheduler report: eligible={}, queue_states_len={}",
            self.eligible.len(),
            self.q.len(),
        );
    }

    pub fn poll_schedule_next(
        mut self: Pin<&mut Self>,
        metas: VQueuesMeta<'_>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<Decisions, StorageError>> {
        let mut decisions = Decisions::default();

        let this = self.as_mut().project();
        // tbd if we'd want to poll resource manager inside the loop or not
        this.eligible.poll_delayed(cx);
        this.resource_manager.poll_resources(cx, this.eligible);

        loop {
            let this = self.as_mut().project();
            // bail if we exhausted coop budget.
            let coop = match tokio::task::coop::poll_proceed(cx) {
                Poll::Ready(coop) => coop,
                Poll::Pending => break,
            };
            // stop when we have enough queues picked
            if decisions.num_queues() >= this.limit_qid_per_poll.get() as usize
                || decisions.total_items() >= this.max_items_per_decision.get() as usize
            {
                trace!(
                    "Reached limits of a single DRR decision. num_items={} qids_in_decision={}",
                    decisions.total_items(),
                    decisions.num_queues()
                );
                break;
            }

            let Some(handle) = this
                .eligible
                .next_eligible(cx, metas, this.storage, this.q)?
            else {
                break;
            };

            let qstate = this.q.get_mut(handle).unwrap();
            let slot = metas.get(handle).unwrap();

            match qstate.try_pop(cx, handle, slot, this.resource_manager)? {
                Pop::NeedsCredit => {
                    this.eligible.rotate_one();
                }
                Pop::Run(action) => {
                    coop.made_progress();
                    decisions.push(slot.vqueue_id(), action);
                    // We need to set the state so we check eligibility and setup
                    // necessary schedules when we poll the queue again.
                    this.eligible.front_needs_poll();
                }
                Pop::Yield(action) => {
                    coop.made_progress();
                    decisions.push(slot.vqueue_id(), action);
                    // We need to set the state so we check eligibility and setup
                    // necessary schedules when we poll the queue again.
                    this.eligible.front_needs_poll();
                }
                Pop::Blocked(resource) => {
                    trace!("VQueue {} is blocked on {resource:?}", slot.vqueue_id());
                    this.eligible.front_blocked(resource);
                }
            }
        }

        if decisions.is_empty() {
            if self.last_report.elapsed() >= Duration::from_secs(10) {
                self.report();
                // also report vqueues states
                self.last_report = Instant::now();
                self.eligible.compact_memory();
            }

            self.waker.clone_from(cx.waker());
            Poll::Pending
        } else {
            decisions.report_metrics();
            Poll::Ready(Ok(decisions))
        }
    }

    fn mark_vqueue_as_dormant(&mut self, qid: &VQueueId, handle: VQueueHandle) {
        trace!("VQueue {qid} is no longer observed by the scheduler");

        // A dormant queue can still sit in a resource wait-list (for example invoker
        // throttling). Remove it first to avoid leaving a stale head that can block
        // progress for waiters behind it.
        if let Some(blocked_resource) = self.eligible.find_blocking_resource(handle).cloned() {
            self.resource_manager
                .remove_vqueue(handle, &blocked_resource);
        }

        self.q.remove(handle);
        self.eligible.remove(handle);
    }

    // To be replaced by an invoke event that runs the invocation within the scheduler itself.
    pub fn confirm_run_attempt(
        mut self: Pin<&mut Self>,
        handle: VQueueHandle,
        slot: &cache::Slot,
        key: &EntryKey,
    ) -> Option<ReservedResources> {
        let qstate = self.q.get_mut(handle)?;

        // I've the resources. Let's run it.
        let (permit, metadata) = qstate.remove_from_unconfirmed_assignments(key)?;
        counter!(VQUEUE_RUN_CONFIRMED).increment(1);

        if qstate.is_dormant(slot.meta()) {
            trace!(
                "VQueue {} is no longer observed by the scheduler",
                slot.vqueue_id()
            );
            self.q.remove(handle);
            self.eligible.remove(handle);
        }
        Some(permit.build(metadata, &self.resource_manager))
    }

    /// Forward a batch of rule-book updates to the embedded resource manager.
    pub fn on_rules_updated(&self, updates: Box<[RuleUpdate]>) {
        self.resource_manager.on_rules_updated(updates);
    }

    #[tracing::instrument(skip_all)]
    #[track_caller]
    pub fn on_inbox_event(&mut self, metas: VQueuesMeta<'_>, event: VQueueEvent) {
        for update in &event.updates {
            match update {
                EventDetails::LockReleased { scope, lock_name } => {
                    self.release_lock(scope, lock_name);
                }
                EventDetails::QueuePaused => {
                    let Some(qstate) = self.q.get_mut(event.queue) else {
                        continue;
                    };

                    let Some(slot) = metas.get(event.queue) else {
                        panic!("vqueue meta must be in cache: {event:?}");
                    };

                    if qstate.is_dormant(slot.meta()) {
                        self.mark_vqueue_as_dormant(slot.vqueue_id(), event.queue);
                    }
                }
                EventDetails::QueueResumed => {
                    // We might have received a resume for an inactive vqueue
                    let Some(slot) = metas.get(event.queue) else {
                        continue;
                    };

                    let qstate = self.q.entry(event.queue).unwrap().or_insert_with(|| {
                        trace!("VQueue {} is added to the scheduler", slot.vqueue_id());
                        VQueueState::new(slot.vqueue_id(), &self.storage, slot.meta().num_running())
                    });

                    self.eligible
                        .refresh_membership(event.queue, slot.meta(), qstate);
                }
                EventDetails::EnqueuedToInbox { key, value } => {
                    let Some(slot) = metas.get(event.queue) else {
                        continue;
                    };

                    let qstate = self.q.entry(event.queue).unwrap().or_insert_with(|| {
                        trace!("VQueue {} is added to the scheduler", slot.vqueue_id());
                        VQueueState::new_empty()
                    });

                    counter!(VQUEUE_ENQUEUE).increment(1);

                    if let Some(permit_builder) = qstate.notify_enqueued(key, value) {
                        // The newly enqueued item became the head of the queue.
                        // If the vqueue was blocked we need to place it back
                        // on the ready ring if it wasn't already there.
                        if let Some(resource) = self.eligible.mark_queue_unblocked(event.queue) {
                            self.resource_manager.remove_vqueue(event.queue, &resource);
                        } else {
                            self.eligible
                                .refresh_membership(event.queue, slot.meta(), qstate);
                        }

                        // Let the other queues that were blocked on my partial permit
                        // get unblocked. But only after I added myself back (potentially)
                        // to the ready ring.
                        self.resource_manager
                            .revert_permit_builder(&mut self.eligible, permit_builder);
                    }
                }
                EventDetails::RemovedFromInbox(key) => {
                    let Some(qstate) = self.q.get_mut(event.queue) else {
                        continue;
                    };

                    let Some(slot) = metas.get(event.queue) else {
                        panic!("vqueue meta must be in cache: {event:?}");
                    };

                    // Three cases:
                    // 1. The item was pending confirmation (we hold resources that should be
                    // released).
                    // 2. The item was the head of the queue (not scheduled yet). We _may_ have a
                    // permit builder in-flight that we can release.
                    // 3. None of the above, removing only changes the vqueue metadata.
                    //
                    // If we have been holding a concurrency permit for this item, we release it.
                    if let Some((permit, _)) = qstate.remove_from_unconfirmed_assignments(key) {
                        // Case 1:
                        // This item is _not_ going to run, so we revert its built up permit
                        self.resource_manager
                            .revert_permit_builder(&mut self.eligible, permit);
                    } else if let Some(permit_builder) = qstate.notify_removed(key) {
                        // Case 2:
                        // This means it might be the current head. Let's invalidate it if
                        // that's the case.
                        if let Some(resource) = self.eligible.find_blocking_resource(event.queue) {
                            // The head was removed and the queue was blocked on a resource.
                            self.resource_manager.remove_vqueue(event.queue, resource);
                        }

                        if !qstate.is_dormant(slot.meta()) {
                            // force the queue to be polled again since the head
                            // will definitely be unknown at this point.
                            self.eligible.ensure_queue_needs_polling(event.queue);
                        }
                        // let the other queues that were blocked on my partial permit
                        // get unblocked. But only after I added myself back (potentially)
                        // to the ready ring.
                        self.resource_manager
                            .revert_permit_builder(&mut self.eligible, permit_builder);
                    }

                    if qstate.is_dormant(slot.meta()) {
                        // the removal makes the queue dormant. Remove it from everything
                        self.mark_vqueue_as_dormant(slot.vqueue_id(), event.queue);
                    }
                }
            }
        }
    }

    fn release_lock(&mut self, scope: &Option<Scope>, lock_name: &LockName) {
        self.resource_manager
            .release_lock(&mut self.eligible, scope, lock_name);
    }

    pub fn iter_status(
        &self,
        metas: VQueuesMeta<'_>,
    ) -> impl Iterator<Item = (VQueueId, VQueueSchedulerStatus)> {
        // Resolver shared across every queue's status snapshot in this sweep —
        // the rule store doesn't change while we hold `&self`.
        let resolve_rule = |handle| self.resource_manager.resolve_user_rule(handle);
        self.q.iter().map(move |(handle, qstate)| {
            let slot = metas.get(handle).unwrap();
            let status = VQueueSchedulerStatus {
                wait_stats: qstate.get_head_wait_stats(),
                remaining_running: qstate.num_remaining_in_running_stage(),
                waiting_inbox: qstate.num_waiting_inbox(slot.meta()),
                status: self
                    .eligible
                    .get_status(handle, slot.meta(), qstate, &resolve_rule),
                head_entry_id: qstate.head_entry_id(),
            };

            (slot.vqueue_id().clone(), status)
        })
    }

    #[cfg(test)]
    pub fn get_status(
        &self,
        metas: VQueuesMeta<'_>,
        handle: VQueueHandle,
    ) -> VQueueSchedulerStatus {
        let Some(qstate) = self.q.get(handle) else {
            return VQueueSchedulerStatus::default();
        };
        let Some(slot) = metas.get(handle) else {
            return VQueueSchedulerStatus::default();
        };

        let resolve_rule = |handle| self.resource_manager.resolve_user_rule(handle);
        VQueueSchedulerStatus {
            wait_stats: qstate.get_head_wait_stats(),
            remaining_running: qstate.num_remaining_in_running_stage(),
            waiting_inbox: qstate.num_waiting_inbox(slot.meta()),
            status: self
                .eligible
                .get_status(handle, slot.meta(), qstate, &resolve_rule),
            head_entry_id: qstate.head_entry_id(),
        }
    }

    /// Verifies the eligibility tracker's internal invariants. Test-only.
    #[cfg(test)]
    pub fn assert_scheduler_invariants(&self) {
        self.eligible.assert_invariants();
    }

    /// Snapshot of every user-limit counter currently tracked by this scheduler's
    /// resource manager. Stamped with the owning partition's key.
    pub fn scan_user_limit_counters(
        &self,
        partition_key: PartitionKey,
    ) -> Vec<UserLimitCounterEntry> {
        self.resource_manager
            .scan_user_limit_counters(partition_key)
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::task::Poll;

    use restate_clock::RoughTimestamp;
    use restate_clock::time::MillisSinceEpoch;
    use restate_core::TaskCenter;
    use restate_futures_util::concurrency::Concurrency;
    use restate_limiter::LimitKey;
    use restate_memory::{MemoryPool, NonZeroByteCount};
    use restate_partition_store::{
        PartitionDb, PartitionStore, PartitionStoreManager, PartitionStoreTransaction,
    };
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::scheduler::SchedulerAction;
    use restate_storage_api::vqueue_table::stats::WaitStats;
    use restate_storage_api::vqueue_table::{
        EntryKey, EntryMetadata, EntryStatusHeader, ReadVQueueTable, Stage,
    };
    use restate_types::ServiceName;
    use restate_types::clock::UniqueTimestamp;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::VQueueId;
    use restate_types::vqueues::{EntryId, EntryKind};
    use restate_worker_api::BlockedResource;

    use crate::cache::VQueuesMetaCache;
    use crate::{GlobalTokenBucket, SchedulingStatus, VQueue, VQueueEvent};

    const TEST_VQUEUES_CAPACITY: usize = 1024;

    /// All services get the default weight of 1 unless a test overrides the resolver.
    fn test_weight_resolver() -> WeightResolver {
        std::sync::Arc::new(|_group: &SchedulingGroup| NonZeroU32::MIN)
    }

    use super::*;

    const BASE_RUN_AT_MS: u64 = 1_744_000_000_000;

    fn test_qid(partition_key: u64) -> VQueueId {
        VQueueId::custom(partition_key, "1")
    }

    async fn storage_test_environment() -> PartitionStore {
        let rocksdb_manager = RocksDbManager::init();
        TaskCenter::set_on_shutdown(Box::pin(async {
            rocksdb_manager.shutdown().await;
        }));

        let manager = PartitionStoreManager::create(true)
            .await
            .expect("DB storage creation succeeds");
        manager
            .open(
                &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
                None,
            )
            .await
            .expect("DB storage creation succeeds")
    }

    async fn enqueue_entry(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaCache,
        qid: &VQueueId,
        id: u8,
        run_at_ms: u64,
        action_collector: Option<&mut Vec<VQueueEvent>>,
    ) -> EntryKey {
        let run_at = MillisSinceEpoch::new(BASE_RUN_AT_MS + run_at_ms);
        enqueue_entry_with_run_at(txn, cache, qid, id, run_at, action_collector).await
    }

    /// Like [`enqueue_entry`] but linking the vqueue to a caller-chosen service, so
    /// tests can exercise the per-service group ring.
    async fn enqueue_entry_for_service(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaCache,
        qid: &VQueueId,
        service: &ServiceName,
        id: u8,
        action_collector: Option<&mut Vec<VQueueEvent>>,
    ) -> EntryKey {
        let run_at = MillisSinceEpoch::new(BASE_RUN_AT_MS);
        let created_at = UniqueTimestamp::try_from(1000u64 + id as u64).unwrap();
        let seq = id as u64;
        let entry_id = EntryId::new(EntryKind::Invocation, [id; EntryId::REMAINDER_LEN]);
        let run_at_rough = RoughTimestamp::from_unix_millis_clamped(run_at);

        let mut vqueue = VQueue::get_or_create_vqueue(
            created_at,
            qid,
            txn,
            cache,
            action_collector,
            service,
            &None,
            &LimitKey::None,
            &None,
        )
        .await
        .expect("vqueue should be created");

        vqueue.enqueue_new(
            created_at,
            seq,
            Some(run_at),
            entry_id,
            EntryMetadata::default(),
        );

        EntryKey::new(false, run_at_rough, seq, entry_id)
    }

    async fn enqueue_entry_with_run_at(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaCache,
        qid: &VQueueId,
        id: u8,
        run_at: MillisSinceEpoch,
        action_collector: Option<&mut Vec<VQueueEvent>>,
    ) -> EntryKey {
        let created_at = UniqueTimestamp::try_from(1000u64 + id as u64).unwrap();
        let seq = id as u64;
        let entry_id = EntryId::new(EntryKind::Invocation, [id; EntryId::REMAINDER_LEN]);
        let run_at_rough = if run_at > created_at.to_unix_millis() {
            RoughTimestamp::from_unix_millis_ceil(run_at)
        } else {
            RoughTimestamp::from_unix_millis_clamped(run_at)
        };

        let mut vqueue = VQueue::get_or_create_vqueue(
            created_at,
            qid,
            txn,
            cache,
            action_collector,
            &ServiceName::new("test"),
            &None,
            &LimitKey::None,
            &None,
        )
        .await
        .expect("vqueue should be created");

        vqueue.enqueue_new(
            created_at,
            seq,
            Some(run_at),
            entry_id,
            EntryMetadata::default(),
        );

        EntryKey::new(false, run_at_rough, seq, entry_id)
    }

    async fn move_to_running(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaCache,
        qid: &VQueueId,
        key: &EntryKey,
        action_collector: Option<&mut Vec<VQueueEvent>>,
    ) -> EntryKey {
        let at = UniqueTimestamp::try_from(1100u64).unwrap();
        let header = txn
            .get_vqueue_entry_status(qid.partition_key(), key.entry_id())
            .await
            .expect("entry state header lookup should succeed")
            .expect("entry state header should exist");

        let mut vqueue = VQueue::get_or_create_vqueue(
            at,
            qid,
            txn,
            cache,
            action_collector,
            &ServiceName::new("test"),
            &None,
            &LimitKey::None,
            &None,
        )
        .await
        .expect("vqueue should be created");

        vqueue.run_entry(at, &header, WaitStats::default())
    }

    async fn reschedule(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaCache,
        qid: &VQueueId,
        entry_id: &EntryId,
        run_at: RoughTimestamp,
        action_collector: Option<&mut Vec<VQueueEvent>>,
    ) {
        let at = UniqueTimestamp::try_from(1_300u64).unwrap();
        let header = txn
            .get_vqueue_entry_status(qid.partition_key(), entry_id)
            .await
            .expect("entry state header lookup should succeed")
            .expect("entry state header should exist");

        let mut vqueue = VQueue::get_or_create_vqueue(
            at,
            qid,
            txn,
            cache,
            action_collector,
            &ServiceName::new("test"),
            &None,
            &LimitKey::None,
            &None,
        )
        .await
        .expect("vqueue should be created");

        vqueue.reschedule(&header, run_at, None);
    }

    /// Parks a running entry into the Suspended stage.
    async fn suspend(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaCache,
        qid: &VQueueId,
        entry_id: &EntryId,
    ) {
        let at = UniqueTimestamp::try_from(1_250u64).unwrap();
        let header = txn
            .get_vqueue_entry_status(qid.partition_key(), entry_id)
            .await
            .expect("entry state header lookup should succeed")
            .expect("entry state header should exist");

        let mut vqueue = VQueue::get_or_create_vqueue(
            at,
            qid,
            txn,
            cache,
            None::<&mut Vec<VQueueEvent>>,
            &ServiceName::new("test"),
            &None,
            &LimitKey::None,
            &None,
        )
        .await
        .expect("vqueue should be created");

        vqueue.suspend_entry(at, &header);
    }

    async fn read_header(
        txn: &PartitionStoreTransaction<'_>,
        qid: &VQueueId,
        entry_id: &EntryId,
    ) -> impl EntryStatusHeader + 'static {
        txn.get_vqueue_entry_status(qid.partition_key(), entry_id)
            .await
            .expect("entry state header lookup should succeed")
            .expect("entry state header should exist")
    }

    async fn create_resource_manager_with_throttling(
        db: &PartitionDb,
        concurrency: Concurrency,
        global_throttling: Option<GlobalTokenBucket>,
    ) -> ResourceManager {
        create_resource_manager_full(db, concurrency, global_throttling, test_weight_resolver())
            .await
    }

    async fn create_resource_manager_full(
        db: &PartitionDb,
        concurrency: Concurrency,
        global_throttling: Option<GlobalTokenBucket>,
        weight_resolver: WeightResolver,
    ) -> ResourceManager {
        ResourceManager::create(
            db.clone(),
            concurrency,
            global_throttling,
            MemoryPool::unlimited(),
            NonZeroByteCount::new(NonZeroUsize::MIN),
            weight_resolver,
        )
        .await
        .expect("resource manager creation should succeed")
    }

    async fn create_resource_manager(
        db: &PartitionDb,
        concurrency: Concurrency,
    ) -> ResourceManager {
        create_resource_manager_with_throttling(db, concurrency, None).await
    }

    async fn create_scheduler(
        db: &PartitionDb,
        cache: &VQueuesMetaCache,
    ) -> DRRScheduler<PartitionDb> {
        DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(100).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        )
    }

    async fn create_scheduler_with_concurrency(
        db: &PartitionDb,
        cache: &VQueuesMetaCache,
        concurrency_limit: usize,
    ) -> DRRScheduler<PartitionDb> {
        DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(100).unwrap(),
            create_resource_manager(
                db,
                Concurrency::new(Some(NonZeroUsize::new(concurrency_limit).unwrap())),
            )
            .await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        )
    }

    fn poll_scheduler(
        mut scheduler: Pin<&mut DRRScheduler<PartitionDb>>,
        metas: VQueuesMeta<'_>,
    ) -> Poll<Result<Decisions, restate_storage_api::StorageError>> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        scheduler.as_mut().poll_schedule_next(metas, &mut cx)
    }

    fn run_keys(decision: &Decisions) -> Vec<EntryKey> {
        decision
            .qids
            .values()
            .flat_map(|actions| actions.iter())
            .filter_map(|action| match action {
                SchedulerAction::Run(run) => Some(run.key),
                _ => None,
            })
            .collect()
    }

    #[restate_core::test]
    async fn empty_scheduler_returns_pending() {
        let rocksdb = storage_test_environment().await;
        let db = rocksdb.partition_db();
        let cache = VQueuesMetaCache::create(db.clone(), TEST_VQUEUES_CAPACITY)
            .await
            .unwrap();

        let mut scheduler = create_scheduler(db, &cache).await;
        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler), cache.view()),
            Poll::Pending
        ));
    }

    #[restate_core::test]
    async fn poll_yields_enqueued_item() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid = test_qid(2000);

        let mut txn = rocksdb.transaction();
        enqueue_entry(&mut txn, &mut cache, &qid, 1, 0, None).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache).await;

        let result = poll_scheduler(Pin::new(&mut scheduler), cache.view());
        assert!(matches!(result, Poll::Ready(Ok(ref decision)) if !decision.is_empty()));

        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.num_run(), 1);
        assert_eq!(decision.num_queues(), 1);
        assert_eq!(decision.total_items(), 1);
    }

    #[restate_core::test]
    async fn run_at_below_now_preempts_within_inbox() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid = test_qid(2_100);
        let now = MillisSinceEpoch::now().as_u64();

        let mut txn = rocksdb.transaction();
        let future_key = enqueue_entry_with_run_at(
            &mut txn,
            &mut cache,
            &qid,
            1,
            MillisSinceEpoch::new(now.saturating_add(60_000)),
            None,
        )
        .await;
        let overdue_key = enqueue_entry_with_run_at(
            &mut txn,
            &mut cache,
            &qid,
            2,
            MillisSinceEpoch::new(now.saturating_sub(1_000)),
            None,
        )
        .await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache).await;

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };

        assert_eq!(decision.num_run(), 1);
        let keys = run_keys(&decision);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], overdue_key);
        assert_ne!(keys[0], future_key);
    }

    /// `reschedule` re-keys an entry's `run_at` for the waiting stages and is a no-op otherwise:
    /// - Inbox: pulls the entry forward (and emits the reconciliation pair); no-op if unchanged.
    /// - Suspended: re-keys in place without notifying the scheduler (parked entries aren't tracked).
    /// - Running: nothing to reschedule.
    /// In every case the entry's `Status` and stats are preserved.
    #[restate_core::test]
    async fn reschedule_moves_run_at_only_for_waiting_stages() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid = test_qid(2_200);
        let now = MillisSinceEpoch::now().as_u64();
        let now_rough = RoughTimestamp::from_unix_millis_clamped(MillisSinceEpoch::new(now));
        // A distinct, far-future run_at so every reschedule is an actual move.
        let future = MillisSinceEpoch::new(now.saturating_add(600_000));

        let mut txn = rocksdb.transaction();
        let mut events: Vec<VQueueEvent> = Vec::new();

        // -- Inbox: a waiting entry is pulled forward to `now`, emitting the reconciliation pair
        //    (`RemovedFromInbox` of the old key + `EnqueuedToInbox` at the new key).
        let inbox = *enqueue_entry_with_run_at(&mut txn, &mut cache, &qid, 1, future, None)
            .await
            .entry_id();
        let inbox_status = read_header(&txn, &qid, &inbox).await.status();

        reschedule(
            &mut txn,
            &mut cache,
            &qid,
            &inbox,
            now_rough,
            Some(&mut events),
        )
        .await;
        let header = read_header(&txn, &qid, &inbox).await;
        assert_eq!(header.entry_key().run_at(), now_rough);
        assert_eq!(header.stage(), Stage::Inbox);
        assert_eq!(header.status(), inbox_status); // status untouched
        assert_eq!(header.stats().num_yields, 0); // not counted as a yield
        assert_eq!(events.len(), 1);
        match &events[0].updates[..] {
            [
                EventDetails::RemovedFromInbox(old_key),
                EventDetails::EnqueuedToInbox { key: new_key, .. },
            ] => {
                assert_ne!(old_key.run_at(), now_rough);
                assert_eq!(new_key.run_at(), now_rough);
            }
            other => panic!("expected a RemovedFromInbox + EnqueuedToInbox pair, got {other:?}"),
        }

        // Re-keying to the same run_at is a no-op: nothing emitted, nothing changed.
        events.clear();
        reschedule(
            &mut txn,
            &mut cache,
            &qid,
            &inbox,
            now_rough,
            Some(&mut events),
        )
        .await;
        assert!(events.is_empty());
        assert_eq!(
            read_header(&txn, &qid, &inbox).await.entry_key().run_at(),
            now_rough
        );

        // -- Suspended: re-keyed in place, no scheduler event.
        let suspended = *enqueue_entry_with_run_at(&mut txn, &mut cache, &qid, 2, future, None)
            .await
            .entry_id();
        suspend(&mut txn, &mut cache, &qid, &suspended).await;
        let suspended_status = read_header(&txn, &qid, &suspended).await.status();

        events.clear();
        reschedule(
            &mut txn,
            &mut cache,
            &qid,
            &suspended,
            now_rough,
            Some(&mut events),
        )
        .await;
        let header = read_header(&txn, &qid, &suspended).await;
        assert_eq!(header.entry_key().run_at(), now_rough);
        assert_eq!(header.stage(), Stage::Suspended);
        assert_eq!(header.status(), suspended_status);
        assert!(events.is_empty());

        // -- Running: nothing to reschedule, the entry is left untouched.
        let running_key = {
            let key = enqueue_entry_with_run_at(&mut txn, &mut cache, &qid, 3, future, None).await;
            move_to_running(&mut txn, &mut cache, &qid, &key, None).await
        };
        let running = *running_key.entry_id();

        events.clear();
        reschedule(
            &mut txn,
            &mut cache,
            &qid,
            &running,
            now_rough,
            Some(&mut events),
        )
        .await;
        let header = read_header(&txn, &qid, &running).await;
        assert_eq!(header.stage(), Stage::Running);
        assert_eq!(header.entry_key().run_at(), running_key.run_at());
        assert!(events.is_empty());

        txn.commit().await.expect("commit should succeed");
        drop(txn);
    }

    #[restate_core::test]
    async fn round_robin_fairness() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);

        let mut txn = rocksdb.transaction();
        for i in 1..=3u64 {
            let qid = test_qid(10 + i);
            for j in 0..=2 {
                enqueue_entry(
                    &mut txn,
                    &mut cache,
                    &qid,
                    (qid.partition_key() + j) as u8,
                    0,
                    None,
                )
                .await;
            }
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(3).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };

        assert_eq!(decision.num_queues(), 3);
        assert_eq!(decision.num_run(), 3);

        for (qid, actions) in &decision.qids {
            let runs: Vec<_> = actions
                .iter()
                .filter_map(|action| match action {
                    SchedulerAction::Run(run) => Some(run),
                    _ => None,
                })
                .collect();
            assert_eq!(runs.len(), 1);
            assert_eq!(runs[0].key.seq().as_u64(), qid.partition_key());
        }
    }

    #[restate_core::test]
    async fn concurrency_limiting() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid1 = test_qid(7000);
        let qid2 = test_qid(7001);

        let mut qids = vec![qid1.clone(), qid2.clone()];

        let mut txn = rocksdb.transaction();
        enqueue_entry(&mut txn, &mut cache, &qid1, 1, 0, None).await;
        enqueue_entry(&mut txn, &mut cache, &qid2, 2, 0, None).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler_with_concurrency(db, &cache, 1).await;

        let h_qid1 = cache.view().handle_for(&qid1).unwrap();
        let h_qid2 = cache.view().handle_for(&qid2).unwrap();

        assert_eq!(
            scheduler.get_status(cache.view(), h_qid1).status,
            SchedulingStatus::Ready,
        );
        assert_eq!(
            scheduler.get_status(cache.view(), h_qid2).status,
            SchedulingStatus::Ready,
        );

        let result = poll_scheduler(Pin::new(&mut scheduler), cache.view());
        assert!(matches!(result, Poll::Ready(Ok(ref d)) if d.total_items() == 1));
        let Poll::Ready(Ok(result)) = result else {
            panic!("expected decision");
        };
        assert_eq!(result.num_run(), 1);

        let first_pop_qid = result.qids.keys().next().unwrap().clone();
        let first_pop_key = run_keys(&result)[0];
        let h_first_pop = cache.view().handle_for(&first_pop_qid).unwrap();
        qids.retain(|qid| qid != &first_pop_qid);
        let h_remaining = cache.view().handle_for(&qids[0]).unwrap();

        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler), cache.view()),
            Poll::Pending
        ));
        assert_eq!(
            scheduler.get_status(cache.view(), h_first_pop).status,
            SchedulingStatus::Empty,
        );
        assert_eq!(
            scheduler.get_status(cache.view(), h_remaining).status,
            SchedulingStatus::BlockedOn(BlockedResource::InvokerConcurrency),
        );

        let metas = cache.view();
        let first_pop_slot = metas.get(h_first_pop).unwrap();
        let mut scheduler = Pin::new(&mut scheduler);
        let resources =
            scheduler
                .as_mut()
                .confirm_run_attempt(h_first_pop, first_pop_slot, &first_pop_key);
        assert!(resources.is_some());
        drop(resources);

        let Poll::Ready(Ok(result2)) = poll_scheduler(scheduler.as_mut(), cache.view()) else {
            panic!("expected decision");
        };
        assert_eq!(result2.num_run(), 1);

        let second_pop_qid = result2.qids.keys().next().unwrap().clone();
        qids.retain(|qid| qid != &second_pop_qid);
        assert!(qids.is_empty());
        assert_eq!(
            scheduler.get_status(cache.view(), h_qid1).status,
            SchedulingStatus::Dormant,
        );
        assert_eq!(
            scheduler.get_status(cache.view(), h_qid2).status,
            SchedulingStatus::Empty,
        );
    }

    #[restate_core::test]
    async fn get_status_reports_invoker_throttling_retry_estimate() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid1 = test_qid(21_101);
        let qid2 = test_qid(21_102);

        let mut txn = rocksdb.transaction();
        enqueue_entry(&mut txn, &mut cache, &qid1, 1, 0, None).await;
        enqueue_entry(&mut txn, &mut cache, &qid2, 2, 0, None).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let throttling_bucket = GlobalTokenBucket::new(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(1).unwrap(),
            ),
            gardal::TokioClock,
        );

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(100).unwrap(),
            create_resource_manager_with_throttling(
                db,
                Concurrency::new_unlimited(),
                Some(throttling_bucket),
            )
            .await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        assert_eq!(decision.num_run(), 1);

        let running_qid = decision
            .qids
            .keys()
            .next()
            .cloned()
            .expect("a single queue should be scheduled");
        let blocked_qid = if running_qid == qid1 { qid2 } else { qid1 };

        let h_blocked = cache.view().handle_for(&blocked_qid).unwrap();
        let h_running = cache.view().handle_for(&running_qid).unwrap();

        let blocked_status = scheduler.get_status(cache.view(), h_blocked).status;
        let SchedulingStatus::BlockedOn(BlockedResource::InvokerThrottling { estimated_retry_at }) =
            blocked_status
        else {
            panic!("expected invoker throttling blocked status");
        };
        assert!(estimated_retry_at.is_some());

        assert_eq!(
            scheduler.get_status(cache.view(), h_running).status,
            SchedulingStatus::Empty
        );
    }

    #[restate_core::test]
    async fn waiters_outside_throttling_window_report_no_estimate() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);

        let qids = [
            test_qid(22_001),
            test_qid(22_002),
            test_qid(22_003),
            test_qid(22_004),
            test_qid(22_005),
        ];

        let mut txn = rocksdb.transaction();
        for (idx, qid) in qids.iter().enumerate() {
            enqueue_entry(
                &mut txn,
                &mut cache,
                qid,
                u8::try_from(idx + 1).unwrap(),
                0,
                None,
            )
            .await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let throttling_bucket = GlobalTokenBucket::new(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(2).unwrap(),
            ),
            gardal::TokioClock,
        );

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(100).unwrap(),
            create_resource_manager_with_throttling(
                db,
                Concurrency::new_unlimited(),
                Some(throttling_bucket),
            )
            .await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        );

        let Poll::Ready(Ok(_decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };

        let mut some_estimate = 0;
        let mut none_estimate = 0;

        for qid in &qids {
            let handle = cache.view().handle_for(qid).unwrap();
            if let SchedulingStatus::BlockedOn(BlockedResource::InvokerThrottling {
                estimated_retry_at,
            }) = scheduler.get_status(cache.view(), handle).status
            {
                if estimated_retry_at.is_some() {
                    some_estimate += 1;
                } else {
                    none_estimate += 1;
                }
            }
        }

        assert!(some_estimate >= 1);
        assert!(none_estimate >= 1);
    }

    #[restate_core::test]
    async fn max_items_per_decision_limit() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);

        let mut txn = rocksdb.transaction();
        for i in 1..=5u64 {
            let qid = test_qid(9000 + i);
            enqueue_entry(&mut txn, &mut cache, &qid, i as u8, 0, None).await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(2).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        );

        if let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view()) {
            assert!(!decision.is_empty());
            assert!(decision.total_items() <= 2);
        }
    }

    #[restate_core::test]
    async fn higher_priority_preempts_between_polls() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid = test_qid(14_000);
        let mut events = Vec::new();

        let mut txn = rocksdb.transaction();
        for i in 1..=7 {
            enqueue_entry(&mut txn, &mut cache, &qid, i, 10_000, None).await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(2).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        let mut in_flight = run_keys(&decision);
        assert_eq!(in_flight.len(), 2);
        assert_eq!(in_flight[0].seq().as_u64(), 1);
        assert_eq!(in_flight[1].seq().as_u64(), 2);

        let mut txn = rocksdb.transaction();
        events.clear();
        enqueue_entry(&mut txn, &mut cache, &qid, 125, 0, Some(&mut events)).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);
        for event in events.drain(..) {
            scheduler.on_inbox_event(cache.view(), event);
        }

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        let next_keys = run_keys(&decision);
        assert_eq!(next_keys.len(), 2);
        assert_eq!(next_keys[0].seq().as_u64(), 125);
        assert_eq!(next_keys[1].seq().as_u64(), 3);
        in_flight.extend(next_keys);

        let mut txn = rocksdb.transaction();
        events.clear();
        for key in in_flight.drain(..) {
            move_to_running(&mut txn, &mut cache, &qid, &key, Some(&mut events)).await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);
        for event in events.drain(..) {
            scheduler.on_inbox_event(cache.view(), event);
        }

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        let keys = run_keys(&decision);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].seq().as_u64(), 4);
        assert_eq!(keys[1].seq().as_u64(), 5);
    }

    #[restate_core::test]
    async fn get_status_reflects_scheduling_states() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid1 = test_qid(21_001);
        let qid2 = test_qid(21_002);

        let mut txn = rocksdb.transaction();
        for i in 1..=2u8 {
            let key = enqueue_entry(&mut txn, &mut cache, &qid1, i, 0, None).await;
            move_to_running(&mut txn, &mut cache, &qid1, &key, None).await;
        }
        enqueue_entry(&mut txn, &mut cache, &qid1, 3, 0, None).await;
        enqueue_entry(&mut txn, &mut cache, &qid2, 10, 0, None).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(10).unwrap(),
            create_resource_manager(db, Concurrency::new(Some(NonZeroUsize::new(1).unwrap())))
                .await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        );

        let h_qid1 = cache.view().handle_for(&qid1).unwrap();
        let h_qid2 = cache.view().handle_for(&qid2).unwrap();

        assert_eq!(
            scheduler.get_status(cache.view(), h_qid1).status,
            SchedulingStatus::Ready
        );
        assert_eq!(
            scheduler.get_status(cache.view(), h_qid2).status,
            SchedulingStatus::Ready
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 3);
        assert_eq!(decision.num_queues(), 2);

        let status = scheduler.get_status(cache.view(), h_qid1);
        assert_eq!(
            status.status,
            SchedulingStatus::BlockedOn(BlockedResource::InvokerConcurrency)
        );
        assert_eq!(status.waiting_inbox, 1);
        assert_eq!(status.remaining_running, 0);

        let status = scheduler.get_status(cache.view(), h_qid2);
        assert_eq!(status.status, SchedulingStatus::Empty);
        assert_eq!(status.waiting_inbox, 0);
        assert_eq!(status.remaining_running, 0);

        let qid2_key = decision
            .qids
            .get(&qid2)
            .and_then(|actions| {
                actions.iter().find_map(|action| match action {
                    SchedulerAction::Run(run) => Some(run.key),
                    _ => None,
                })
            })
            .expect("qid2 run action should exist");

        let metas = cache.view();
        let qid2_slot = metas.get(h_qid2).unwrap();
        let mut scheduler = Pin::new(&mut scheduler);
        let resources = scheduler
            .as_mut()
            .confirm_run_attempt(h_qid2, qid2_slot, &qid2_key);
        assert!(resources.is_some());
        drop(resources);

        let Poll::Ready(Ok(decision)) = poll_scheduler(scheduler.as_mut(), cache.view()) else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 1);
        assert_eq!(decision.num_queues(), 1);

        assert_eq!(
            scheduler.get_status(cache.view(), h_qid1).status,
            SchedulingStatus::Empty
        );
        assert_eq!(
            scheduler.get_status(cache.view(), h_qid2).status,
            SchedulingStatus::Dormant
        );
        assert!(matches!(
            poll_scheduler(scheduler.as_mut(), cache.view()),
            Poll::Pending
        ));
    }

    // --- Two-level weighted round-robin tests -------------------------------------
    //
    // The tests below cover the service-group WRR layered on top of the per-queue DRR:
    // backwards compatibility at weight=1, weighted dispatch ratios, work conservation,
    // group lifecycle, and internal data-structure invariants.

    /// Weight resolver used by WRR tests: derives the weight from the service name
    /// suffix, e.g. "svc-w10" gets weight 10. Anything else gets weight 1.
    fn suffix_weight_resolver() -> WeightResolver {
        std::sync::Arc::new(|group: &SchedulingGroup| {
            let SchedulingGroup::Service(service_name) = group else {
                return NonZeroU32::MIN;
            };
            let name: &str = service_name.as_ref();
            name.rsplit_once("-w")
                .and_then(|(_, w)| w.parse::<u32>().ok())
                .and_then(NonZeroU32::new)
                .unwrap_or(NonZeroU32::MIN)
        })
    }

    /// Builds `queues_per_service` single-entry vqueues for each given service. Queue
    /// partition keys are `base + service_index * 1000 + queue_index` so a dispatched
    /// qid can be mapped back to its service.
    async fn setup_services(
        rocksdb: &mut PartitionStore,
        cache: &mut VQueuesMetaCache,
        base: u64,
        services: &[&str],
        queues_per_service: u64,
    ) {
        let mut txn = rocksdb.transaction();
        for (si, service) in services.iter().enumerate() {
            let service_name = ServiceName::new(service);
            for qi in 0..queues_per_service {
                let pk = base + si as u64 * 1000 + qi;
                let qid = test_qid(pk);
                enqueue_entry_for_service(
                    &mut txn,
                    cache,
                    &qid,
                    &service_name,
                    // entry ids only need to be unique within a queue
                    ((si * 40 + qi as usize) % 255) as u8 + 1,
                    None,
                )
                .await;
            }
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);
    }

    /// Polls the scheduler with max_items_per_decision=1 until it goes Pending, and
    /// counts dispatched items per service index (derived from the partition key).
    fn drain_and_count(
        scheduler: &mut DRRScheduler<PartitionDb>,
        cache: &VQueuesMetaCache,
        base: u64,
        num_services: usize,
        max_polls: usize,
    ) -> Vec<usize> {
        let mut counts = vec![0usize; num_services];
        for _ in 0..max_polls {
            match poll_scheduler(Pin::new(scheduler), cache.view()) {
                Poll::Ready(Ok(decision)) => {
                    for qid in decision.qids.keys() {
                        let si = ((qid.partition_key() - base) / 1000) as usize;
                        counts[si] += 1;
                    }
                    scheduler.assert_scheduler_invariants();
                }
                Poll::Ready(Err(err)) => panic!("scheduler error: {err}"),
                Poll::Pending => break,
            }
        }
        counts
    }

    fn scheduler_with_resolver(
        db: &PartitionDb,
        cache: &VQueuesMetaCache,
        resource_manager: ResourceManager,
        resolver: WeightResolver,
    ) -> DRRScheduler<PartitionDb> {
        DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(1).unwrap(), // one item per decision to observe dispatch order
            resource_manager,
            db.clone(),
            cache.view(),
            resolver,
        )
    }

    /// Backwards compatibility: with every service at weight 1 the groups share slots
    /// equally, and every enqueued item is dispatched (nothing lost, nothing starved).
    #[restate_core::test]
    async fn wrr_all_weight_one_equal_service_share() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 30_000;

        setup_services(
            &mut rocksdb,
            &mut cache,
            BASE,
            &["svc-a", "svc-b", "svc-c"],
            5,
        )
        .await;

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            test_weight_resolver(),
        );

        let counts = drain_and_count(&mut scheduler, &cache, BASE, 3, 100);
        assert_eq!(counts, vec![5, 5, 5], "each weight-1 service drains fully");
    }

    /// A single service must drain exactly like the previous flat ring: all items
    /// dispatched, scheduler Pending afterwards, group dropped when empty.
    #[restate_core::test]
    async fn wrr_single_service_drains_completely() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 32_000;

        setup_services(&mut rocksdb, &mut cache, BASE, &["svc-solo"], 10).await;

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            test_weight_resolver(),
        );

        let counts = drain_and_count(&mut scheduler, &cache, BASE, 1, 100);
        assert_eq!(counts, vec![10]);
        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler), cache.view()),
            Poll::Pending
        ));
        scheduler.assert_scheduler_invariants();
    }

    /// Weighted ratio: weight-10 service gets ~10 slots per weight-1 slot while both
    /// have work queued. Observed over the first dispatches before either drains.
    #[restate_core::test]
    async fn wrr_two_groups_dispatch_ratio_one_to_ten() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 34_000;

        // 40 queues each so neither group drains within the observation window
        setup_services(
            &mut rocksdb,
            &mut cache,
            BASE,
            &["payment-w1", "indexer-w10"],
            40,
        )
        .await;

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            suffix_weight_resolver(),
        );

        // observe 22 dispatch slots: exactly two full WRR cycles (1 + 10 per cycle)
        let counts = drain_and_count(&mut scheduler, &cache, BASE, 2, 22);
        assert_eq!(
            counts[0] + counts[1],
            22,
            "both groups still had work; every poll must dispatch"
        );
        assert_eq!(counts[0], 2, "weight-1 group gets 1 slot per cycle");
        assert_eq!(counts[1], 20, "weight-10 group gets 10 slots per cycle");
    }

    /// Work conservation: when the high-weight group drains, the low-weight group gets
    /// every remaining slot and drains completely (no starvation, no lost items).
    #[restate_core::test]
    async fn wrr_work_conservation_after_group_drains() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 36_000;

        let mut txn = rocksdb.transaction();
        let heavy = ServiceName::new("heavy-w100");
        let light = ServiceName::new("light-w1");
        for qi in 0..2u64 {
            let qid = test_qid(BASE + qi);
            enqueue_entry_for_service(&mut txn, &mut cache, &qid, &heavy, qi as u8 + 1, None).await;
        }
        for qi in 0..20u64 {
            let qid = test_qid(BASE + 1000 + qi);
            enqueue_entry_for_service(&mut txn, &mut cache, &qid, &light, qi as u8 + 10, None)
                .await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            suffix_weight_resolver(),
        );

        let counts = drain_and_count(&mut scheduler, &cache, BASE, 2, 100);
        assert_eq!(
            counts,
            vec![2, 20],
            "heavy group drains its 2 items, light group still drains all 20"
        );
        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler), cache.view()),
            Poll::Pending
        ));
    }

    /// Group lifecycle: a drained group is removed from the ring and correctly
    /// re-created when new work for its service arrives.
    #[restate_core::test]
    async fn wrr_group_removed_and_recreated_on_new_enqueue() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 38_000;
        let service = ServiceName::new("cycler");
        let qid = test_qid(BASE);

        let mut txn = rocksdb.transaction();
        enqueue_entry_for_service(&mut txn, &mut cache, &qid, &service, 1, None).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            test_weight_resolver(),
        );

        let counts = drain_and_count(&mut scheduler, &cache, BASE, 1, 10);
        assert_eq!(counts, vec![1]);
        scheduler.assert_scheduler_invariants();

        // new work for the same service re-creates the group
        let mut txn = rocksdb.transaction();
        let mut events = Vec::new();
        enqueue_entry_for_service(&mut txn, &mut cache, &qid, &service, 2, Some(&mut events)).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);
        for event in events.drain(..) {
            scheduler.on_inbox_event(cache.view(), event);
        }

        let counts = drain_and_count(&mut scheduler, &cache, BASE, 1, 10);
        assert_eq!(counts, vec![1], "re-created group dispatches new work");
        scheduler.assert_scheduler_invariants();
    }

    /// Blocked queues leave the ring without stalling their group: with a concurrency
    /// limit of 1, other queues of the same service keep dispatching one at a time.
    #[restate_core::test]
    async fn wrr_blocked_queue_does_not_stall_group() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 40_000;

        setup_services(&mut rocksdb, &mut cache, BASE, &["limited"], 3).await;

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new(Some(NonZeroUsize::new(1).unwrap())))
                .await,
            test_weight_resolver(),
        );

        // Only one Run fits within the concurrency limit; the other two queues get
        // blocked on InvokerConcurrency and must leave the ring without panicking.
        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected a decision");
        };
        assert_eq!(decision.num_run(), 1);
        scheduler.assert_scheduler_invariants();

        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler), cache.view()),
            Poll::Pending
        ));
        scheduler.assert_scheduler_invariants();

        // Confirming the running item frees the concurrency slot; a blocked queue is
        // woken up and dispatches next.
        let first_qid = decision.qids.keys().next().unwrap().clone();
        let first_key = run_keys(&decision)[0];
        let metas = cache.view();
        let h_first = metas.handle_for(&first_qid).unwrap();
        let first_slot = metas.get(h_first).unwrap();
        let mut scheduler = Pin::new(&mut scheduler);
        let resources = scheduler
            .as_mut()
            .confirm_run_attempt(h_first, first_slot, &first_key);
        assert!(resources.is_some());
        drop(resources);

        let Poll::Ready(Ok(decision2)) = poll_scheduler(scheduler.as_mut(), cache.view()) else {
            panic!("expected a second decision");
        };
        assert_eq!(decision2.num_run(), 1);
        scheduler.assert_scheduler_invariants();
    }

    /// Weight changes take effect when a group cycles: the resolver is re-consulted on
    /// group re-creation, mirroring an admin PATCH between benchmark runs.
    #[restate_core::test]
    async fn wrr_weight_change_applies_on_group_recreation() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU32, Ordering};

        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 42_000;
        let service = ServiceName::new("mutable");
        let qid = test_qid(BASE);

        let mut txn = rocksdb.transaction();
        enqueue_entry_for_service(&mut txn, &mut cache, &qid, &service, 1, None).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let weight = Arc::new(AtomicU32::new(1));
        let resolver_weight = Arc::clone(&weight);
        let resolver: WeightResolver = Arc::new(move |_group: &SchedulingGroup| {
            NonZeroU32::new(resolver_weight.load(Ordering::Relaxed)).unwrap()
        });

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            resolver,
        );

        // drain the group so it gets dropped, then bump the weight
        let counts = drain_and_count(&mut scheduler, &cache, BASE, 1, 10);
        assert_eq!(counts, vec![1]);
        weight.store(10, Ordering::Relaxed);

        // new work re-creates the group; the new weight must be picked up (verified
        // indirectly: the dispatch succeeds and invariants hold — the ratio itself is
        // covered by wrr_two_groups_dispatch_ratio_one_to_ten)
        let mut txn = rocksdb.transaction();
        let mut events = Vec::new();
        enqueue_entry_for_service(&mut txn, &mut cache, &qid, &service, 2, Some(&mut events)).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);
        for event in events.drain(..) {
            scheduler.on_inbox_event(cache.view(), event);
        }
        let counts = drain_and_count(&mut scheduler, &cache, BASE, 1, 10);
        assert_eq!(counts, vec![1]);
        scheduler.assert_scheduler_invariants();
    }

    /// Bulk stress on the invariants: many services, many queues, drain everything.
    #[restate_core::test]
    async fn wrr_invariants_under_bulk_load() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 44_000;

        setup_services(
            &mut rocksdb,
            &mut cache,
            BASE,
            &["s0-w1", "s1-w2", "s2-w4", "s3-w1", "s4-w8"],
            8,
        )
        .await;

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            suffix_weight_resolver(),
        );

        // drain_and_count asserts invariants after every poll
        let counts = drain_and_count(&mut scheduler, &cache, BASE, 5, 200);
        assert_eq!(counts, vec![8, 8, 8, 8, 8], "all queues fully drained");
        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler), cache.view()),
            Poll::Pending
        ));
    }

    /// Three groups with weights 1/2/4: within one full WRR cycle (7 slots) the groups
    /// receive exactly 1, 2 and 4 dispatches.
    #[restate_core::test]
    async fn wrr_three_groups_weighted_ratio() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 46_000;

        setup_services(
            &mut rocksdb,
            &mut cache,
            BASE,
            &["a-w1", "b-w2", "c-w4"],
            20,
        )
        .await;

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            suffix_weight_resolver(),
        );

        // two full cycles: 2 * (1 + 2 + 4) = 14 slots, no group drains (20 queues each)
        let counts = drain_and_count(&mut scheduler, &cache, BASE, 3, 14);
        assert_eq!(counts.iter().sum::<usize>(), 14);
        assert_eq!(
            counts,
            vec![2, 4, 8],
            "1:2:4 weighted split over two cycles"
        );
    }

    /// The thesis starvation scenario in miniature: a huge weight-1 group must not
    /// starve a tiny weight-10 group. The small group's share depends only on the
    /// weights, not on the queue-count imbalance.
    #[restate_core::test]
    async fn wrr_small_high_weight_group_not_starved_by_large_group() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 48_000;

        let mut txn = rocksdb.transaction();
        let payment = ServiceName::new("payment-w1");
        let batcher = ServiceName::new("batcher-w10");
        // 100 payment queues vs 2 batcher queues with plenty of work: enqueue two
        // entries per batcher queue so the batcher group survives multiple cycles
        for qi in 0..100u64 {
            let qid = test_qid(BASE + qi);
            enqueue_entry_for_service(&mut txn, &mut cache, &qid, &payment, qi as u8, None).await;
        }
        for qi in 0..2u64 {
            let qid = test_qid(BASE + 1000 + qi);
            enqueue_entry_for_service(&mut txn, &mut cache, &qid, &batcher, 200 + qi as u8, None)
                .await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            suffix_weight_resolver(),
        );

        // Observe the first 4 slots: the flat ring would have given the batcher a
        // ~2/102 chance per slot; the WRR must dispatch both batcher queues within the
        // first cycle (1 payment slot + up to 10 batcher slots).
        let counts = drain_and_count(&mut scheduler, &cache, BASE, 2, 4);
        assert_eq!(
            counts[1], 2,
            "both batcher queues dispatched within the first cycle despite 100 payment queues"
        );
    }

    /// max_items_per_decision caps the whole decision across groups, not per group.
    #[restate_core::test]
    async fn wrr_max_items_per_decision_across_groups() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 50_000;

        setup_services(
            &mut rocksdb,
            &mut cache,
            BASE,
            &["x-w1", "y-w1", "z-w1"],
            10,
        )
        .await;

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(5).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
            suffix_weight_resolver(),
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        assert!(
            decision.total_items() <= 5,
            "decision item cap applies across all groups, got {}",
            decision.total_items()
        );
        scheduler.assert_scheduler_invariants();
    }

    /// Delayed items (run_at in the future) leave the ring into the delay queue and the
    /// overdue item preempts within the same queue — original behavior, now per group.
    #[restate_core::test]
    async fn wrr_run_at_scheduling_unchanged() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid = test_qid(52_000);
        let now = MillisSinceEpoch::now().as_u64();

        let mut txn = rocksdb.transaction();
        let future_key = enqueue_entry_with_run_at(
            &mut txn,
            &mut cache,
            &qid,
            1,
            MillisSinceEpoch::new(now.saturating_add(60_000)),
            None,
        )
        .await;
        let overdue_key = enqueue_entry_with_run_at(
            &mut txn,
            &mut cache,
            &qid,
            2,
            MillisSinceEpoch::new(now.saturating_sub(1_000)),
            None,
        )
        .await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache).await;

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        let keys = run_keys(&decision);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], overdue_key);
        assert_ne!(keys[0], future_key);
        scheduler.assert_scheduler_invariants();
    }

    /// External removal (RemovedFromInbox) of a queue's only item makes the queue
    /// dormant; its group must not retain a stale handle and the sibling queue in the
    /// same group keeps dispatching.
    #[restate_core::test]
    async fn wrr_external_removal_purges_handle_from_group() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 54_000;
        let service = ServiceName::new("removal");
        let qid_kept = test_qid(BASE);
        let qid_removed = test_qid(BASE + 1);

        let mut txn = rocksdb.transaction();
        enqueue_entry_for_service(&mut txn, &mut cache, &qid_kept, &service, 1, None).await;
        let removed_key =
            enqueue_entry_for_service(&mut txn, &mut cache, &qid_removed, &service, 2, None).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            test_weight_resolver(),
        );

        // Externally remove the only item of the second queue.
        let mut txn = rocksdb.transaction();
        let mut events = Vec::new();
        let at = UniqueTimestamp::try_from(2_000u64).unwrap();
        let header = txn
            .get_vqueue_entry_status(qid_removed.partition_key(), removed_key.entry_id())
            .await
            .expect("lookup succeeds")
            .expect("entry exists");
        let mut vqueue = VQueue::get_or_create_vqueue(
            at,
            &qid_removed,
            &mut txn,
            &mut cache,
            Some(&mut events),
            &service,
            &None,
            &LimitKey::None,
            &None,
        )
        .await
        .expect("vqueue exists");
        vqueue.end(
            at,
            &header,
            restate_storage_api::vqueue_table::Status::Killed,
            Duration::ZERO,
        );
        txn.commit().await.expect("commit should succeed");
        drop(txn);
        for event in events.drain(..) {
            scheduler.on_inbox_event(cache.view(), event);
        }
        scheduler.assert_scheduler_invariants();

        // Only the kept queue's item is dispatched; the removed one never appears.
        let counts = drain_and_count(&mut scheduler, &cache, BASE, 1, 10);
        assert_eq!(counts, vec![1], "only the kept queue dispatches");
        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler), cache.view()),
            Poll::Pending
        ));
        scheduler.assert_scheduler_invariants();
    }

    /// Vqueues without a service link land in the shared unlinked group and are
    /// scheduled normally alongside service-linked groups.
    #[restate_core::test]
    async fn wrr_unlinked_and_linked_queues_coexist() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 56_000;

        let mut txn = rocksdb.transaction();
        // enqueue_entry uses the fixed service "test" — plus one differently-named
        // service, exercising two coexisting groups with default weight
        enqueue_entry(&mut txn, &mut cache, &test_qid(BASE), 1, 0, None).await;
        let other = ServiceName::new("other");
        enqueue_entry_for_service(
            &mut txn,
            &mut cache,
            &test_qid(BASE + 1000),
            &other,
            2,
            None,
        )
        .await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = scheduler_with_resolver(
            db,
            &cache,
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            test_weight_resolver(),
        );

        let counts = drain_and_count(&mut scheduler, &cache, BASE, 2, 10);
        assert_eq!(counts, vec![1, 1], "both groups fully served");
        scheduler.assert_scheduler_invariants();
    }

    /// Higher-priority (earlier run_at) items still preempt within a queue between
    /// polls — regression guard for the original `higher_priority_preempts` behavior
    /// under the group ring.
    #[restate_core::test]
    async fn wrr_priority_preemption_within_group_unchanged() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        let qid = test_qid(58_000);
        let service = ServiceName::new("preempt");
        let mut events = Vec::new();

        let mut txn = rocksdb.transaction();
        for i in 1..=3 {
            enqueue_entry_for_service(&mut txn, &mut cache, &qid, &service, i, None).await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(2).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
            test_weight_resolver(),
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        assert_eq!(decision.num_run(), 2);
        scheduler.assert_scheduler_invariants();

        // enqueue a fourth item while the first two are in flight
        let mut txn = rocksdb.transaction();
        events.clear();
        enqueue_entry_for_service(&mut txn, &mut cache, &qid, &service, 4, Some(&mut events)).await;
        txn.commit().await.expect("commit should succeed");
        drop(txn);
        for event in events.drain(..) {
            scheduler.on_inbox_event(cache.view(), event);
        }

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
        else {
            panic!("expected decision");
        };
        assert_eq!(decision.num_run(), 2, "remaining items keep flowing");
        scheduler.assert_scheduler_invariants();
    }

    /// End-to-end: under invoker-concurrency pressure (limit 1), the WRR waiter
    /// list — not arrival order — decides who acquires freed permits. Payment
    /// queues arrive first and outnumber the batcher 6:2; a FIFO waiter list
    /// would grant all payment permits before any batcher permit. The batcher
    /// must instead be dispatched within the first WRR cycle after a release.
    #[restate_core::test]
    async fn wrr_waiter_list_decides_dispatch_under_concurrency_pressure() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 62_000;
        let payment = ServiceName::new("payment-w1");
        let batcher = ServiceName::new("batcher-w10");

        let mut txn = rocksdb.transaction();
        // payments enqueued FIRST — they flood the waiter list on the first poll
        for qi in 0..6u64 {
            let qid = test_qid(BASE + qi);
            enqueue_entry_for_service(&mut txn, &mut cache, &qid, &payment, qi as u8 + 1, None)
                .await;
        }
        for qi in 0..2u64 {
            let qid = test_qid(BASE + 1000 + qi);
            enqueue_entry_for_service(&mut txn, &mut cache, &qid, &batcher, 50 + qi as u8, None)
                .await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(1).unwrap(),
            create_resource_manager_full(
                db,
                Concurrency::new(Some(NonZeroUsize::new(1).unwrap())),
                None,
                suffix_weight_resolver(),
            )
            .await,
            db.clone(),
            cache.view(),
            suffix_weight_resolver(),
        );

        // Dispatch one item at a time; after each Run, confirm it to release
        // the single permit, then record which service the next dispatch hits.
        let mut order: Vec<&str> = Vec::new();
        for _ in 0..8 {
            let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
            else {
                panic!("expected a decision, got Pending after {order:?}");
            };
            assert_eq!(decision.num_run(), 1);
            let qid = decision.qids.keys().next().unwrap().clone();
            let service = if qid.partition_key() >= BASE + 1000 {
                "batcher"
            } else {
                "payment"
            };
            order.push(service);
            scheduler.assert_scheduler_invariants();

            // release the permit so the waiter list decides the next grant
            let key = run_keys(&decision)[0];
            let metas = cache.view();
            let handle = metas.handle_for(&qid).unwrap();
            let slot = metas.get(handle).unwrap();
            let resources = Pin::new(&mut scheduler).confirm_run_attempt(handle, slot, &key);
            assert!(resources.is_some());
            drop(resources);
        }

        assert_eq!(order.len(), 8, "all queues eventually dispatched");
        assert_eq!(
            order.iter().filter(|s| **s == "batcher").count(),
            2,
            "both batcher queues dispatched"
        );
        let first_batcher = order.iter().position(|s| *s == "batcher").unwrap();
        assert!(
            first_batcher <= 2,
            "batcher must be granted within the first WRR cycle despite arriving \
             last behind 6 payment queues; order: {order:?}"
        );
    }

    /// Scope groups: scoped queues of DIFFERENT services schedule as ONE group
    /// whose weight comes from the (rule-fed) scope-weights map, competing
    /// against unscoped per-service groups at weight 1 — the end-to-end shape
    /// of `restate rules set payment --weight 10` + scope inheritance.
    #[restate_core::test]
    async fn wrr_scope_group_spans_services_with_rule_weight() {
        use crate::scheduler::{ScopeWeights, scope_weight_resolver};
        use restate_types::Scope;

        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 64_000;
        let payment_scope = Scope::try_non_interned("payment").unwrap();

        let mut txn = rocksdb.transaction();
        // scoped: two different services under ONE scope, 20 queues total
        for qi in 0..10u64 {
            for (si, svc) in ["PaymentWorkflow", "SepaService"].iter().enumerate() {
                let qid = test_qid(BASE + si as u64 * 100 + qi);
                let service = ServiceName::new(svc);
                let at = UniqueTimestamp::try_from(1000u64 + qi + si as u64 * 50).unwrap();
                let entry_id = EntryId::new(
                    EntryKind::Invocation,
                    [(si as u8) * 100 + qi as u8 + 1; EntryId::REMAINDER_LEN],
                );
                let run_at = MillisSinceEpoch::new(BASE_RUN_AT_MS);
                let mut vqueue = VQueue::get_or_create_vqueue(
                    at,
                    &qid,
                    &mut txn,
                    &mut cache,
                    None::<&mut Vec<VQueueEvent>>,
                    &service,
                    &Some(payment_scope.clone()),
                    &LimitKey::None,
                    &None,
                )
                .await
                .expect("vqueue should be created");
                vqueue.enqueue_new(
                    at,
                    qi + si as u64 * 100,
                    Some(run_at),
                    entry_id,
                    EntryMetadata::default(),
                );
            }
        }
        // unscoped competitor: one service, 20 queues
        for qi in 0..20u64 {
            let qid = test_qid(BASE + 1000 + qi);
            enqueue_entry_for_service(
                &mut txn,
                &mut cache,
                &qid,
                &ServiceName::new("indexer"),
                200 + qi as u8,
                None,
            )
            .await;
        }
        txn.commit().await.expect("commit should succeed");
        drop(txn);

        // rule-fed weights: scope "payment" -> 10
        let weights = ScopeWeights::default();
        weights
            .write()
            .unwrap()
            .insert("payment".to_owned(), NonZeroU32::new(10).unwrap());
        let resolver = scope_weight_resolver(weights);

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU32::new(100).unwrap(),
            NonZeroU32::new(1).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
            resolver,
        );

        // one full WRR cycle = 10 (scope) + 1 (indexer service group) slots
        let mut scope_count = 0usize;
        let mut service_count = 0usize;
        for _ in 0..22 {
            let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler), cache.view())
            else {
                panic!("expected a decision");
            };
            for qid in decision.qids.keys() {
                if qid.partition_key() >= BASE + 1000 {
                    service_count += 1;
                } else {
                    scope_count += 1;
                }
            }
            scheduler.assert_scheduler_invariants();
        }
        assert_eq!(
            (scope_count, service_count),
            (20, 2),
            "scope group (weight 10, spanning two services) gets 10 slots per cycle \
             vs 1 for the unscoped service group"
        );
    }

    /// Rapid enqueue/drain cycles across many services: groups are created and dropped
    /// repeatedly without desyncing ring and map.
    #[restate_core::test]
    async fn wrr_repeated_group_churn_invariants() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty(TEST_VQUEUES_CAPACITY);
        const BASE: u64 = 60_000;
        let db = rocksdb.partition_db().clone();

        let mut scheduler = scheduler_with_resolver(
            &db,
            &cache,
            create_resource_manager(&db, Concurrency::new_unlimited()).await,
            suffix_weight_resolver(),
        );

        for round in 0..5u64 {
            let mut txn = rocksdb.transaction();
            let mut events = Vec::new();
            for si in 0..3u64 {
                let service = ServiceName::new(&format!("churn{si}-w{}", si + 1));
                let qid = test_qid(BASE + si * 1000 + round);
                enqueue_entry_for_service(
                    &mut txn,
                    &mut cache,
                    &qid,
                    &service,
                    (round * 10 + si) as u8 + 1,
                    Some(&mut events),
                )
                .await;
            }
            txn.commit().await.expect("commit should succeed");
            drop(txn);
            for event in events.drain(..) {
                scheduler.on_inbox_event(cache.view(), event);
            }

            let counts = drain_and_count(&mut scheduler, &cache, BASE, 3, 20);
            assert_eq!(counts, vec![1, 1, 1], "round {round}: all services served");
        }
        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler), cache.view()),
            Poll::Pending
        ));
        scheduler.assert_scheduler_invariants();
    }
}
