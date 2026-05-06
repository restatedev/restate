// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;
use std::pin::Pin;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use hashbrown::HashMap;
use metrics::counter;
use pin_project::pin_project;
use slotmap::SlotMap;
use tokio::time::Instant;
use tracing::{info, trace, warn};

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
use crate::metric_definitions::VQUEUE_RUN_CONFIRMED;
use crate::scheduler::MetaLiteUpdate;
use crate::scheduler::eligible::EligibilityTracker;
use crate::scheduler::vqueue_state::Pop;

use super::Decisions;
use super::ReservedResources;
use super::ResourceManager;
use super::VQueueHandle;
use super::VQueueMetaLite;
use super::VQueueSchedulerStatus;
use super::vqueue_state::VQueueState;

#[pin_project]
pub struct DRRScheduler<S: VQueueStore> {
    resource_manager: ResourceManager,
    // sorted by queue_id
    eligible: EligibilityTracker,
    /// Mapping of vqueue_id -> handle for active vqueues
    id_lookup: HashMap<VQueueId, VQueueHandle>,
    q: SlotMap<VQueueHandle, VQueueState<S>>,
    /// Waker to be notified when scheduler is potentially able to scheduler more work
    waker: Waker,
    /// Time of the last memory reporting and memory compaction
    last_report: Instant,

    /// Limits the number of queues picked per scheduler's poll
    limit_qid_per_poll: NonZeroU16,
    /// Limits the number of items included in a single decision across all queues
    max_items_per_decision: NonZeroU16,

    // SAFETY NOTE: **must** Keep this at the end since it needs to outlive all readers.
    storage: S,
}

impl<S: VQueueStore> DRRScheduler<S> {
    pub fn new(
        limit_qid_per_poll: NonZeroU16,
        max_items_per_decision: NonZeroU16,
        resource_manager: ResourceManager,
        storage: S,
        vqueues: VQueuesMeta<'_>,
    ) -> Self {
        let mut total_running = 0;
        let mut total_waiting = 0;

        let num_active = vqueues.num_active();
        let mut q = SlotMap::with_capacity_and_key(num_active);
        let mut id_lookup = HashMap::with_capacity(num_active);
        let mut eligible: EligibilityTracker = EligibilityTracker::with_capacity(num_active);

        for (qid, meta) in vqueues.iter_active_vqueues() {
            total_running += meta.num_running();
            total_waiting += meta.total_waiting();
            let handle = q.insert_with_key(|handle| {
                VQueueState::new(
                    qid.clone(),
                    handle,
                    VQueueMetaLite::new(meta),
                    &storage,
                    meta.num_running(),
                )
            });
            id_lookup.insert(qid.clone(), handle);
            // We init all active vqueues as eligible first
            eligible.insert_eligible(handle);
        }

        info!(
            "Scheduler started. num_vqueues={}, total_running_items={total_running}, total_waiting_items={total_waiting}",
            q.len(),
        );

        Self {
            resource_manager,
            id_lookup,
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

            let Some(handle) = this.eligible.next_eligible(cx, this.storage, this.q)? else {
                break;
            };

            let qstate = this.q.get_mut(handle).unwrap();

            match qstate.try_pop(cx, this.resource_manager)? {
                Pop::NeedsCredit => {
                    this.eligible.rotate_one();
                }
                Pop::Run(action) => {
                    coop.made_progress();
                    decisions.push(&qstate.qid, action);
                    // We need to set the state so we check eligibility and setup
                    // necessary schedules when we poll the queue again.
                    this.eligible.front_needs_poll();
                }
                Pop::Yield(action) => {
                    coop.made_progress();
                    decisions.push(&qstate.qid, action);
                    // We need to set the state so we check eligibility and setup
                    // necessary schedules when we poll the queue again.
                    this.eligible.front_needs_poll();
                }
                Pop::Blocked(resource) => {
                    trace!("VQueue {:?} is blocked on {resource:?}", qstate.qid);
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

        self.id_lookup.remove(qid);
        self.q.remove(handle);
        self.eligible.remove(handle);
    }

    // To be replaced by an invoke event that runs the invocation within the scheduler itself.
    pub fn confirm_run_attempt(
        mut self: Pin<&mut Self>,
        qid: &VQueueId,
        key: &EntryKey,
    ) -> Option<ReservedResources> {
        let handle = self.id_lookup.get(qid).copied()?;
        let qstate = self.q.get_mut(handle)?;

        // I've the resources. Let's run it.
        let permit = qstate.remove_from_unconfirmed_assignments(key)?;
        // To make sure the num_waiting counter is updated.
        qstate.apply_meta_update(&MetaLiteUpdate::RemovedFromInbox(*key));
        counter!(VQUEUE_RUN_CONFIRMED).increment(1);

        if qstate.is_dormant() {
            trace!("VQueue {qid} is no longer observed by the scheduler");
            self.id_lookup.remove(qid);
            self.q.remove(handle);
            self.eligible.remove(handle);
        }
        Some(permit.build(&self.resource_manager))
    }

    #[tracing::instrument(skip_all)]
    #[track_caller]
    pub fn on_inbox_event(&mut self, event: VQueueEvent) {
        let qid = event.qid;
        let mut maybe_handle = self.id_lookup.get(&qid).copied();
        debug_assert!(maybe_handle.map(|h| self.q.contains_key(h)).unwrap_or(true));

        for update in event.updates {
            match update {
                EventDetails::AddVQueue {
                    scope,
                    limit_key,
                    lock_name,
                } => {
                    if maybe_handle.is_none() {
                        // add it.
                        trace!("VQueue {qid} is added to the scheduler");
                        maybe_handle = Some(self.q.insert_with_key(|h| {
                            VQueueState::new_empty(
                                qid.clone(),
                                h,
                                VQueueMetaLite::new_empty(scope, limit_key, lock_name),
                            )
                        }));
                        self.id_lookup.insert(qid.clone(), maybe_handle.unwrap());
                        // Note: We don't make this eligible yet. We'll do that when we see
                        // an actual enqueue because we initialize the vqueue metadata with
                        // length 0.
                    }
                }
                EventDetails::LockReleased { scope, lock_name } => {
                    self.release_lock(&scope, &lock_name);
                }
                EventDetails::InboxUpdate(ref update @ MetaLiteUpdate::QueuePaused) => {
                    let Some(handle) = maybe_handle else {
                        continue;
                    };
                    let qstate = self.q.get_mut(handle).unwrap();
                    qstate.apply_meta_update(update);
                    if qstate.is_dormant() {
                        self.mark_vqueue_as_dormant(&qid, handle);
                    }
                }
                EventDetails::InboxUpdate(ref update @ MetaLiteUpdate::QueueResumed { .. }) => {
                    let Some(handle) = maybe_handle else {
                        continue;
                    };
                    let qstate = self.q.get_mut(handle).unwrap();
                    qstate.apply_meta_update(update);
                    if self.eligible.refresh_membership(qstate) {
                        self.wake_up();
                    }
                }
                EventDetails::InboxUpdate(
                    ref update @ MetaLiteUpdate::EnqueuedToInbox { ref key, ref value },
                ) => {
                    assert!(
                        maybe_handle.is_some(),
                        "scheduler must know about queue {qid}  before an inbox enqueue",
                    );
                    let handle = maybe_handle.unwrap();
                    let qstate = self.q.get_mut(handle).unwrap();

                    // keep inbox size in sync
                    qstate.apply_meta_update(update);

                    if let Some(permit_builder) = qstate.notify_enqueued(key, value) {
                        // The newly enqueued item became the head of the queue.
                        // If the vqueue was blocked we need to place it back
                        // on the ready ring if it wasn't already there.
                        let mut wake_up = false;
                        if let Some(resource) = self.eligible.mark_queue_unblocked(handle) {
                            self.resource_manager.remove_vqueue(handle, &resource);
                            wake_up |= true;
                        } else if self.eligible.refresh_membership(qstate) {
                            wake_up |= true;
                        }

                        // Let the other queues that were blocked on my partial permit
                        // get unblocked. But only after I added myself back (potentially)
                        // to the ready ring.
                        wake_up |= self
                            .resource_manager
                            .revert_permit_builder(&mut self.eligible, permit_builder);

                        if wake_up {
                            self.wake_up();
                        }
                    }
                }
                EventDetails::InboxUpdate(
                    ref update @ MetaLiteUpdate::RemovedFromInbox(ref key),
                ) => {
                    let Some(handle) = maybe_handle else {
                        continue;
                    };
                    let qstate = self.q.get_mut(handle).unwrap();

                    // Three cases:
                    // 1. The item was pending confirmation (we hold resources that should be
                    // released).
                    // 2. The item was the head of the queue (not scheduled yet). We _may_ have a
                    // permit builder in-flight that we can release.
                    // 3. None of the above, removing only changes the vqueue metadata.

                    let mut wake_up = false;
                    // keep inbox size in sync
                    qstate.apply_meta_update(update);

                    // If we have been holding a concurrency permit for this item, we release it.
                    if let Some(permit) = qstate.remove_from_unconfirmed_assignments(key) {
                        // Case 1:
                        // This item is _not_ going to run, so we revert its built up permit
                        wake_up |= self
                            .resource_manager
                            .revert_permit_builder(&mut self.eligible, permit);
                    } else if let Some(permit_builder) = qstate.notify_removed(key) {
                        // Case 2:
                        // This means it might be the current head. Let's invalidate it if
                        // that's the case.
                        if let Some(resource) = self.eligible.find_blocking_resource(handle) {
                            // The head was removed and the queue was blocked on a resource.
                            self.resource_manager.remove_vqueue(handle, resource);
                        }

                        if !qstate.is_dormant() {
                            // force the queue to be polled again since the head
                            // will definitely be unknown at this point.
                            self.eligible.ensure_queue_needs_polling(handle);
                            wake_up |= true;
                        }
                        // let the other queues that were blocked on my partial permit
                        // get unblocked. But only after I added myself back (potentially)
                        // to the ready ring.
                        wake_up |= self
                            .resource_manager
                            .revert_permit_builder(&mut self.eligible, permit_builder);
                    }

                    if qstate.is_dormant() {
                        // the removal makes the queue dormant. Remove it from everything
                        self.mark_vqueue_as_dormant(&qid, handle);
                    }

                    if wake_up {
                        self.waker.wake_by_ref();
                    }
                }
            }
        }
    }

    fn release_lock(&mut self, scope: &Option<Scope>, lock_name: &LockName) {
        if self
            .resource_manager
            .release_lock(&mut self.eligible, scope, lock_name)
        {
            self.wake_up();
        }
    }

    pub fn iter_status(&self) -> impl Iterator<Item = (VQueueId, VQueueSchedulerStatus)> {
        // Resolver shared across every queue's status snapshot in this sweep —
        // the rule store doesn't change while we hold `&self`.
        let resolve_rule = |handle| self.resource_manager.resolve_user_rule(handle);
        self.q.iter().map(move |(_handle, qstate)| {
            let status = VQueueSchedulerStatus {
                wait_stats: qstate.get_head_wait_stats(),
                remaining_running: qstate.num_remaining_in_running_stage(),
                waiting_inbox: qstate.num_waiting_inbox(),
                status: self.eligible.get_status(qstate, &resolve_rule),
                head_entry_id: qstate.head_entry_id(),
            };

            (qstate.qid.clone(), status)
        })
    }

    pub fn get_status(&self, qid: &VQueueId) -> VQueueSchedulerStatus {
        let Some(qstate) = self
            .id_lookup
            .get(qid)
            .and_then(|handle| self.q.get(*handle))
        else {
            return VQueueSchedulerStatus::default();
        };

        let resolve_rule = |handle| self.resource_manager.resolve_user_rule(handle);
        VQueueSchedulerStatus {
            wait_stats: qstate.get_head_wait_stats(),
            remaining_running: qstate.num_remaining_in_running_stage(),
            waiting_inbox: qstate.num_waiting_inbox(),
            status: self.eligible.get_status(qstate, &resolve_rule),
            head_entry_id: qstate.head_entry_id(),
        }
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

    fn wake_up(&mut self) {
        self.waker.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
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
    use restate_storage_api::vqueue_table::{EntryKey, EntryMetadata, ReadVQueueTable};
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

        let manager = PartitionStoreManager::create()
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

        vqueue.run_entry(at, &header, &WaitStats::default())
    }

    async fn create_resource_manager_with_throttling(
        db: &PartitionDb,
        concurrency: Concurrency,
        global_throttling: Option<GlobalTokenBucket>,
    ) -> ResourceManager {
        ResourceManager::create(
            db.clone(),
            concurrency,
            global_throttling,
            MemoryPool::unlimited(),
            NonZeroByteCount::new(NonZeroUsize::MIN),
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
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(100).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
        )
    }

    async fn create_scheduler_with_concurrency(
        db: &PartitionDb,
        cache: &VQueuesMetaCache,
        concurrency_limit: usize,
    ) -> DRRScheduler<PartitionDb> {
        DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(100).unwrap(),
            create_resource_manager(
                db,
                Concurrency::new(Some(NonZeroUsize::new(concurrency_limit).unwrap())),
            )
            .await,
            db.clone(),
            cache.view(),
        )
    }

    fn poll_scheduler(
        mut scheduler: Pin<&mut DRRScheduler<PartitionDb>>,
    ) -> Poll<Result<Decisions, restate_storage_api::StorageError>> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        scheduler.as_mut().poll_schedule_next(&mut cx)
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
    async fn test_empty_scheduler_returns_pending() {
        let rocksdb = storage_test_environment().await;
        let db = rocksdb.partition_db();
        let cache = VQueuesMetaCache::create(db.clone()).await.unwrap();

        let mut scheduler = create_scheduler(db, &cache).await;
        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler)),
            Poll::Pending
        ));
    }

    #[restate_core::test]
    async fn test_poll_yields_enqueued_item() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
        let qid = test_qid(2000);

        let mut txn = rocksdb.transaction();
        enqueue_entry(&mut txn, &mut cache, &qid, 1, 0, None).await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache).await;

        let result = poll_scheduler(Pin::new(&mut scheduler));
        assert!(matches!(result, Poll::Ready(Ok(ref decision)) if !decision.is_empty()));

        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.num_run(), 1);
        assert_eq!(decision.num_queues(), 1);
        assert_eq!(decision.total_items(), 1);
    }

    #[restate_core::test]
    async fn test_run_at_below_now_preempts_within_inbox() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
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

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache).await;

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler)) else {
            panic!("expected decision");
        };

        assert_eq!(decision.num_run(), 1);
        let keys = run_keys(&decision);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], overdue_key);
        assert_ne!(keys[0], future_key);
    }

    #[restate_core::test]
    async fn test_round_robin_fairness() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();

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

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(3).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler)) else {
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
    async fn test_concurrency_limiting() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
        let qid1 = test_qid(7000);
        let qid2 = test_qid(7001);

        let mut qids = vec![qid1.clone(), qid2.clone()];

        let mut txn = rocksdb.transaction();
        enqueue_entry(&mut txn, &mut cache, &qid1, 1, 0, None).await;
        enqueue_entry(&mut txn, &mut cache, &qid2, 2, 0, None).await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler_with_concurrency(db, &cache, 1).await;

        assert_eq!(scheduler.get_status(&qid1).status, SchedulingStatus::Ready,);
        assert_eq!(scheduler.get_status(&qid2).status, SchedulingStatus::Ready,);

        let result = poll_scheduler(Pin::new(&mut scheduler));
        assert!(matches!(result, Poll::Ready(Ok(ref d)) if d.total_items() == 1));
        let Poll::Ready(Ok(result)) = result else {
            panic!("expected decision");
        };
        assert_eq!(result.num_run(), 1);

        let first_pop_qid = result.qids.keys().next().unwrap().clone();
        let first_pop_key = run_keys(&result)[0];
        qids.retain(|qid| qid != &first_pop_qid);

        assert!(matches!(
            poll_scheduler(Pin::new(&mut scheduler)),
            Poll::Pending
        ));
        assert_eq!(
            scheduler.get_status(&first_pop_qid).status,
            SchedulingStatus::Empty,
        );
        assert_eq!(
            scheduler.get_status(&qids[0]).status,
            SchedulingStatus::BlockedOn(BlockedResource::InvokerConcurrency),
        );

        let mut scheduler = Pin::new(&mut scheduler);
        let resources = scheduler
            .as_mut()
            .confirm_run_attempt(&first_pop_qid, &first_pop_key);
        assert!(resources.is_some());
        drop(resources);

        let Poll::Ready(Ok(result2)) = poll_scheduler(scheduler.as_mut()) else {
            panic!("expected decision");
        };
        assert_eq!(result2.num_run(), 1);

        let second_pop_qid = result2.qids.keys().next().unwrap().clone();
        qids.retain(|qid| qid != &second_pop_qid);
        assert!(qids.is_empty());
        assert_eq!(
            scheduler.get_status(&qid1).status,
            SchedulingStatus::Dormant,
        );
        assert_eq!(scheduler.get_status(&qid2).status, SchedulingStatus::Empty,);
    }

    #[restate_core::test]
    async fn test_get_status_reports_invoker_throttling_retry_estimate() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
        let qid1 = test_qid(21_101);
        let qid2 = test_qid(21_102);

        let mut txn = rocksdb.transaction();
        enqueue_entry(&mut txn, &mut cache, &qid1, 1, 0, None).await;
        enqueue_entry(&mut txn, &mut cache, &qid2, 2, 0, None).await;
        txn.commit().await.expect("commit should succeed");

        let throttling_bucket = GlobalTokenBucket::new(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(1).unwrap(),
            ),
            gardal::TokioClock,
        );

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(100).unwrap(),
            create_resource_manager_with_throttling(
                db,
                Concurrency::new_unlimited(),
                Some(throttling_bucket),
            )
            .await,
            db.clone(),
            cache.view(),
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler)) else {
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

        let blocked_status = scheduler.get_status(&blocked_qid).status;
        let SchedulingStatus::BlockedOn(BlockedResource::InvokerThrottling { estimated_retry_at }) =
            blocked_status
        else {
            panic!("expected invoker throttling blocked status");
        };
        assert!(estimated_retry_at.is_some());

        assert_eq!(
            scheduler.get_status(&running_qid).status,
            SchedulingStatus::Empty
        );
    }

    #[restate_core::test]
    async fn test_waiters_outside_throttling_window_report_no_estimate() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();

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

        let throttling_bucket = GlobalTokenBucket::new(
            gardal::Limit::per_second_and_burst(
                NonZeroU32::new(1).unwrap(),
                NonZeroU32::new(2).unwrap(),
            ),
            gardal::TokioClock,
        );

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(100).unwrap(),
            create_resource_manager_with_throttling(
                db,
                Concurrency::new_unlimited(),
                Some(throttling_bucket),
            )
            .await,
            db.clone(),
            cache.view(),
        );

        let Poll::Ready(Ok(_decision)) = poll_scheduler(Pin::new(&mut scheduler)) else {
            panic!("expected decision");
        };

        let mut some_estimate = 0;
        let mut none_estimate = 0;

        for qid in &qids {
            if let SchedulingStatus::BlockedOn(BlockedResource::InvokerThrottling {
                estimated_retry_at,
            }) = scheduler.get_status(qid).status
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
    async fn test_max_items_per_decision_limit() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();

        let mut txn = rocksdb.transaction();
        for i in 1..=5u64 {
            let qid = test_qid(9000 + i);
            enqueue_entry(&mut txn, &mut cache, &qid, i as u8, 0, None).await;
        }
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(2).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
        );

        if let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler)) {
            assert!(!decision.is_empty());
            assert!(decision.total_items() <= 2);
        }
    }

    #[restate_core::test]
    async fn test_higher_priority_preempts_between_polls() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
        let qid = test_qid(14_000);
        let mut events = Vec::new();

        let mut txn = rocksdb.transaction();
        for i in 1..=7 {
            enqueue_entry(&mut txn, &mut cache, &qid, i, 10_000, None).await;
        }
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(2).unwrap(),
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
        );

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler)) else {
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
        for event in events.drain(..) {
            scheduler.on_inbox_event(event);
        }

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler)) else {
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
        for event in events.drain(..) {
            scheduler.on_inbox_event(event);
        }

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler)) else {
            panic!("expected decision");
        };
        let keys = run_keys(&decision);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].seq().as_u64(), 4);
        assert_eq!(keys[1].seq().as_u64(), 5);
    }

    #[restate_core::test]
    async fn test_get_status_reflects_scheduling_states() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
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

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(10).unwrap(),
            create_resource_manager(db, Concurrency::new(Some(NonZeroUsize::new(1).unwrap())))
                .await,
            db.clone(),
            cache.view(),
        );

        assert_eq!(scheduler.get_status(&qid1).status, SchedulingStatus::Ready);
        assert_eq!(scheduler.get_status(&qid2).status, SchedulingStatus::Ready);

        let Poll::Ready(Ok(decision)) = poll_scheduler(Pin::new(&mut scheduler)) else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 3);
        assert_eq!(decision.num_queues(), 2);

        let status = scheduler.get_status(&qid1);
        assert_eq!(
            status.status,
            SchedulingStatus::BlockedOn(BlockedResource::InvokerConcurrency)
        );
        assert_eq!(status.waiting_inbox, 1);
        assert_eq!(status.remaining_running, 0);

        let status = scheduler.get_status(&qid2);
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

        let mut scheduler = Pin::new(&mut scheduler);
        let resources = scheduler.as_mut().confirm_run_attempt(&qid2, &qid2_key);
        assert!(resources.is_some());
        drop(resources);

        let Poll::Ready(Ok(decision)) = poll_scheduler(scheduler.as_mut()) else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 1);
        assert_eq!(decision.num_queues(), 1);

        assert_eq!(scheduler.get_status(&qid1).status, SchedulingStatus::Empty);
        assert_eq!(
            scheduler.get_status(&qid2).status,
            SchedulingStatus::Dormant
        );
        assert!(matches!(poll_scheduler(scheduler.as_mut()), Poll::Pending));
    }
}
