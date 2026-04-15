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
use restate_storage_api::vqueue_table::VQueueEntry;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_types::vqueue::VQueueId;
use restate_types::{LockName, Scope};

use crate::EventDetails;
use crate::VQueueEvent;
use crate::VQueuesMeta;
use crate::metric_definitions::VQUEUE_ENQUEUE;
use crate::metric_definitions::VQUEUE_RUN_CONFIRMED;
use crate::scheduler::eligible::EligibilityTracker;
use crate::scheduler::vqueue_state::Pop;

use super::Decision;
use super::ReservedResources;
use super::ResourceManager;
use super::VQueueHandle;
use super::VQueueSchedulerStatus;
use super::vqueue_state::VQueueState;

#[pin_project]
pub struct DRRScheduler<S: VQueueStore> {
    resource_manager: ResourceManager,
    /// Permits waiting for confirmation, rejection, or removal from the leader.
    pending_resources: HashMap<u64, ReservedResources>,
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
    #[allow(clippy::too_many_arguments)]
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
            let handle =
                q.insert_with_key(|handle| VQueueState::new(qid.clone(), handle, meta.clone()));
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
            pending_resources: Default::default(),
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
    ) -> Poll<Result<Decision<S::Item>, StorageError>> {
        let mut decision = Decision::default();
        let mut items_collected = 0;

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
            if decision.num_queues() >= this.limit_qid_per_poll.get() as usize
                || items_collected >= this.max_items_per_decision.get() as usize
            {
                trace!(
                    "Reached limits of a single DRR decision. num_items={} qids_in_decision={}",
                    decision.total_items(),
                    decision.num_queues()
                );
                break;
            }

            let Some(handle) = this.eligible.next_eligible(this.storage, this.q)? else {
                trace!(
                    "No more eligible vqueues, {:?}, states_len={}, states_capacity={}",
                    this.eligible,
                    this.q.len(),
                    this.q.capacity()
                );
                break;
            };

            let qstate = this.q.get_mut(handle).unwrap();

            match qstate.try_pop(cx, this.storage, this.resource_manager)? {
                Pop::NeedsCredit => {
                    this.eligible.rotate_one();
                }
                Pop::Item {
                    action,
                    resources,
                    entry,
                } => {
                    coop.made_progress();
                    items_collected += 1;
                    if let Some(resources) = resources {
                        this.pending_resources
                            .insert(entry.item.unique_hash(), resources);
                    }
                    decision.push(&qstate.qid, action, entry);
                    // We need to set the state so we check eligibility and setup
                    // necessary schedules when we poll the queue again.
                    this.eligible.front_needs_poll();
                }
                Pop::Throttle { delay, scope } => {
                    this.eligible.front_throttled(delay, scope);
                }
                Pop::Blocked(resource) => {
                    trace!("VQueue {:?} is blocked on {resource:?}", qstate.qid);
                    this.eligible.front_blocked(resource);
                }
            }
        }

        if decision.is_empty() {
            if self.last_report.elapsed() >= Duration::from_secs(10) {
                self.report();
                // also report vqueues states
                self.last_report = Instant::now();
                self.eligible.compact_memory();
            }

            self.waker.clone_from(cx.waker());
            Poll::Pending
        } else {
            decision.report_metrics();
            Poll::Ready(Ok(decision))
        }
    }

    fn add_active_vqueue(&mut self, qid: &VQueueId, meta: VQueueMeta) {
        let handle = self
            .q
            .insert_with_key(|handle| VQueueState::new_empty(qid.clone(), handle, meta));

        assert!(
            self.id_lookup.insert(qid.clone(), handle).is_none(),
            "vqueue id lingering"
        );
    }

    fn mark_vqueue_as_dormant(&mut self, qid: &VQueueId) {
        let Some(handle) = self.id_lookup.remove(qid) else {
            return;
        };
        // retire the vqueue state
        self.q.remove(handle);
        self.eligible.remove(handle);
    }

    pub fn pop_resources(mut self: Pin<&mut Self>, item_hash: u64) -> Option<ReservedResources> {
        self.pending_resources.remove(&item_hash)
    }

    #[tracing::instrument(skip_all)]
    #[track_caller]
    pub fn on_inbox_event(&mut self, event: VQueueEvent<S::Item>) {
        let qid = event.qid;
        for update in event.updates {
            match update {
                EventDetails::MetadataUpdated(update) => {
                    let qstate = self
                        .id_lookup
                        .get(&qid)
                        .and_then(|handle| self.q.get_mut(*handle))
                        .expect("vqueue must be active");
                    qstate.apply_meta_update(&update);
                }
                EventDetails::VQueueBecameActive(meta) => {
                    self.add_active_vqueue(&qid, meta);
                }
                EventDetails::VQueueBecameDormant => {
                    self.mark_vqueue_as_dormant(&qid);
                }
                EventDetails::LockReleased { scope, lock_name } => {
                    self.release_lock(&scope, &lock_name);
                }
                EventDetails::Enqueued(ref item) => {
                    let handle = self
                        .id_lookup
                        .get(&qid)
                        .copied()
                        .expect("vqueue must be active");
                    let qstate = self.q.get_mut(handle).expect("vqueue must be active");

                    counter!(VQUEUE_ENQUEUE).increment(1);
                    if let Some(permit_builder) = qstate.notify_enqueued(item) {
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
                EventDetails::RunAttemptConfirmed { item_hash } => {
                    let Some(handle) = self.id_lookup.get(&qid) else {
                        continue;
                    };
                    let Some(qstate) = self.q.get_mut(*handle) else {
                        continue;
                    };
                    // Note: do not remove the permit here, we depend on the leader to pop it
                    // when it actually runs the item (e.g on VQInvoke action) which doesn't
                    // necessarily need to happen before we see this inbox event.
                    if qstate.remove_from_unconfirmed_assignments(item_hash) {
                        counter!(VQUEUE_RUN_CONFIRMED).increment(1);
                    }
                }
                EventDetails::Removed { item_hash } => {
                    let Some(handle) = self.id_lookup.get(&qid).copied() else {
                        continue;
                    };
                    let Some(qstate) = self.q.get_mut(handle) else {
                        continue;
                    };

                    // If we have been holding a concurrency permit for this item, we release it.
                    self.pending_resources.remove(&item_hash);

                    // This means it might be the current head. Let's invalidate it if
                    // that's the case.
                    if let Some(permit_builder) = qstate.notify_removed(item_hash) {
                        if let Some(resource) = self.eligible.find_blocking_resource(handle) {
                            // The head was removed and the queue was blocked on a resource.
                            self.resource_manager.remove_vqueue(handle, resource);
                        }

                        let mut wake_up = false;
                        if qstate.is_dormant() {
                            // queue is now dormant. Remove it from everything.
                            self.eligible.remove(handle);
                            self.id_lookup.remove(&qid);
                            self.q.remove(handle);
                        } else {
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

                        if wake_up {
                            self.wake_up();
                        }
                    } else if qstate.is_dormant() {
                        // the removal makes the queue dormant. Remove it from everything
                        self.eligible.remove(handle);
                        self.id_lookup.remove(&qid);
                        self.q.remove(handle);
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
        self.q.iter().map(move |(_handle, qstate)| {
            let status = VQueueSchedulerStatus {
                wait_stats: qstate.get_head_wait_stats(),
                remaining_running: qstate.num_remaining_in_running_stage(),
                waiting_inbox: qstate.num_waiting_inbox(),
                status: self.eligible.get_status(qstate),
            };

            (qstate.qid.clone(), status)
        })
    }

    pub fn get_status(&self, qid: &VQueueId) -> Option<VQueueSchedulerStatus> {
        let qstate = self
            .id_lookup
            .get(qid)
            .and_then(|handle| self.q.get(*handle))?;

        Some(VQueueSchedulerStatus {
            wait_stats: qstate.get_head_wait_stats(),
            remaining_running: qstate.num_remaining_in_running_stage(),
            waiting_inbox: qstate.num_waiting_inbox(),
            status: self.eligible.get_status(qstate),
        })
    }

    fn wake_up(&mut self) {
        self.waker.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::num::{NonZeroU16, NonZeroUsize};
    use std::ops::RangeInclusive;
    use std::pin::pin;
    use std::task::Poll;

    use restate_core::TaskCenter;
    use restate_futures_util::concurrency::Concurrency;
    use restate_limiter::LimitKey;
    use restate_memory::{MemoryPool, NonZeroByteCount};
    use restate_partition_store::{
        PartitionDb, PartitionStore, PartitionStoreManager, PartitionStoreTransaction,
    };
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::{EntryCard, EntryId, EntryKind, VisibleAt};
    use restate_types::clock::UniqueTimestamp;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;
    use restate_types::vqueue::{
        EffectivePriority, NewEntryPriority, VQueueId, VQueueInstance, VQueueParent,
    };

    use crate::cache::VQueuesMetaCache;
    use crate::scheduler::Action;
    use crate::scheduler::resource_manager::ResourceKind;
    use crate::{SchedulingStatus, VQueue, VQueueEvent};

    // ==================== Test Helpers ====================

    /// Helper to create a test VQueueId with a unique partition key for test isolation.
    fn test_qid(partition_key: u64) -> VQueueId {
        VQueueId::new(
            VQueueParent::SYSTEM_UNLIMITED,
            PartitionKey::from(partition_key),
            VQueueInstance::from_raw(1),
        )
    }

    /// Creates a test PartitionStore environment.
    async fn storage_test_environment() -> PartitionStore {
        let rocksdb_manager = RocksDbManager::init();
        TaskCenter::set_on_shutdown(Box::pin(async {
            rocksdb_manager.shutdown().await;
        }));

        let manager = PartitionStoreManager::create()
            .await
            .expect("DB storage creation succeeds");
        // A single partition store that spans all keys.
        manager
            .open(
                &Partition::new(
                    PartitionId::MIN,
                    RangeInclusive::new(0, PartitionKey::MAX - 1),
                ),
                None,
            )
            .await
            .expect("DB storage creation succeeds")
    }

    /// Enqueues an entry using the formalized VQueues state transitions.
    /// Returns the emitted VQueueEvent and the EntryCard that was created.
    async fn enqueue_entry(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaCache,
        qid: &VQueueId,
        id: u8,
        priority: NewEntryPriority,
        action_collector: Option<&mut Vec<VQueueEvent<EntryCard>>>,
    ) -> EntryCard {
        let created_at = UniqueTimestamp::try_from(1000u64 + id as u64).unwrap();
        let entry_id = EntryId::new([id; 16]);

        let mut vqueue = VQueue::get_or_create_vqueue(
            qid,
            txn,
            cache,
            action_collector,
            &None,
            &LimitKey::None,
            &None,
        )
        .await
        .expect("vqueue should be created");

        vqueue
            .enqueue_new::<()>(
                created_at,
                VisibleAt::Now,
                priority,
                EntryKind::Invocation,
                entry_id,
                None,
            )
            .expect("enqueue should succeed")
    }

    async fn move_to_running(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaCache,
        qid: &VQueueId,
        card: &EntryCard,
        action_collector: Option<&mut Vec<VQueueEvent<EntryCard>>>,
    ) -> EntryCard {
        let at = UniqueTimestamp::try_from(1100u64).unwrap();
        let mut vqueue = VQueue::get_or_create_vqueue(
            qid,
            txn,
            cache,
            action_collector,
            &None,
            &LimitKey::None,
            &None,
        )
        .await
        .expect("vqueue should be created");

        vqueue
            .attempt_to_run(at, card)
            .expect("attempt_to_run should succeed")
            .expect("attempted to run an item that was not found in waiting inbox")
    }

    /// Creates a scheduler using PartitionDb as the storage backend.
    async fn create_resource_manager(
        db: &PartitionDb,
        concurrency: Concurrency,
    ) -> ResourceManager {
        ResourceManager::create(
            db.clone(),
            concurrency,
            None,
            MemoryPool::unlimited(),
            NonZeroByteCount::new(NonZeroUsize::MIN),
        )
        .await
        .expect("resource manager creation should succeed")
    }

    async fn create_scheduler(
        db: &PartitionDb,
        cache: &VQueuesMetaCache,
    ) -> DRRScheduler<PartitionDb> {
        DRRScheduler::new(
            NonZeroU16::new(100).unwrap(), // limit_qid_per_poll
            NonZeroU16::new(100).unwrap(), // max_items_per_decision
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
    ) -> Poll<Result<Decision<EntryCard>, restate_storage_api::StorageError>> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        scheduler.as_mut().poll_schedule_next(&mut cx)
    }

    // ==================== Tests ====================

    /// Test: A newly created scheduler with no vqueues returns Pending.
    #[restate_core::test]
    async fn test_empty_scheduler_returns_pending() {
        let rocksdb = storage_test_environment().await;
        let db = rocksdb.partition_db();
        let cache = VQueuesMetaCache::create(db.clone()).await.unwrap();

        let mut scheduler = create_scheduler(db, &cache).await;

        let result = poll_scheduler(pin!(&mut scheduler));
        assert!(matches!(result, Poll::Pending));
    }

    /// Test: Polling the scheduler after enqueue yields the item.
    #[restate_core::test]
    async fn test_poll_yields_enqueued_item() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
        let qid = test_qid(2000);

        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid,
            1,
            NewEntryPriority::UserDefault,
            None,
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache).await;

        let result = poll_scheduler(pin!(&mut scheduler));
        assert!(matches!(result, Poll::Ready(Ok(ref decision)) if !decision.is_empty()));

        if let Poll::Ready(Ok(decision)) = result {
            assert_eq!(decision.num_start(), 1);
            assert_eq!(decision.num_queues(), 1);
            assert_eq!(decision.total_items(), 1);
        }
    }

    /// Test: Round-robin fairness - multiple vqueues get scheduled in rotation.
    #[restate_core::test]
    async fn test_round_robin_fairness() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();

        // Create 3 vqueues, each with 3 items
        let mut txn = rocksdb.transaction();
        for i in 1..=3u64 {
            let qid = test_qid(10 + i);
            for j in 0..=2 {
                enqueue_entry(
                    &mut txn,
                    &mut cache,
                    &qid,
                    // items are offsetted by the queue ID
                    (qid.partition_key() + j) as u8,
                    NewEntryPriority::UserDefault,
                    None,
                )
                .await;
            }
        }
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        // Create scheduler with max_items_per_decision = 3
        // In this case, we should see an item per vqueue.
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(3).unwrap(), // max 3 items per decision
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
        );

        // Poll and verify that a decision was made
        let result = poll_scheduler(pin!(&mut scheduler));

        // The decision should include items from all queues since we have high limits
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        // With unlimited concurrency and high limits, all items should be picked
        assert_eq!(decision.num_queues(), 3);
        assert_eq!(decision.num_start(), 3);

        decision.into_iter().for_each(|(qid, assignments)| {
            for (action, mut items) in assignments.into_iter_per_action() {
                assert_eq!(action, Action::MoveToRun);
                // for each vqueue, we should see the head item. The head item is the lowest
                // ID, so we should expect the item ID to the same as the queue ID.
                assert_eq!(items.len(), 1);
                assert_eq!(
                    items.next().unwrap().item.id,
                    EntryId::new([qid.partition_key() as u8; 16])
                );
            }
        });
    }

    /// Test: Concurrency limiting blocks scheduling when capacity is exhausted.
    #[restate_core::test]
    async fn test_concurrency_limiting() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
        let qid1 = test_qid(7000);
        let qid2 = test_qid(7001);

        let mut qids = vec![qid1.clone(), qid2.clone()];

        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid1,
            1,
            NewEntryPriority::UserDefault,
            None,
        )
        .await;
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid2,
            2,
            NewEntryPriority::UserDefault,
            None,
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler_with_concurrency(db, &cache, 1).await;

        assert_eq!(
            scheduler.get_status(&qid1).unwrap().status,
            SchedulingStatus::Ready,
        );
        assert_eq!(
            scheduler.get_status(&qid2).unwrap().status,
            SchedulingStatus::Ready,
        );

        // First poll should succeed with one item (concurrency limit = 1)
        let result = poll_scheduler(pin!(&mut scheduler));
        assert!(matches!(result, Poll::Ready(Ok(ref d)) if d.total_items() == 1));
        let Poll::Ready(Ok(result)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(result.num_start(), 1);

        let first_pop_qid = result.iter_qids().next().unwrap();
        qids.retain(|qid| qid != first_pop_qid);

        // Second poll should return Pending since we're at capacity
        assert!(matches!(
            poll_scheduler(pin!(&mut scheduler)),
            Poll::Pending
        ));
        // Inspect the status of the vqueues, we should see that some vqueues are blocked on
        // capacity
        assert_eq!(
            scheduler.get_status(first_pop_qid).unwrap().status,
            // we drained this vqueue as it has a single item
            SchedulingStatus::Empty,
        );
        assert_eq!(
            scheduler.get_status(&qids[0]).unwrap().status,
            SchedulingStatus::BlockedOn(ResourceKind::InvokerConcurrency),
        );
        // Pop the resources from the scheduler to release the concurrency token.
        // In production, the leader calls pop_resources() when it actually runs the item.
        let item_hash = result
            .into_iter()
            .flat_map(|(_, a)| a.into_iter_per_action())
            .flat_map(|(_, items)| items)
            .next()
            .unwrap()
            .item
            .unique_hash();
        let mut scheduler = pin!(scheduler);
        let resources = scheduler.as_mut().pop_resources(item_hash);
        assert!(resources.is_some());
        drop(resources);
        // Now we should be able to get another item
        let result2 = poll_scheduler(scheduler.as_mut());
        let Poll::Ready(Ok(result2)) = result2 else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(result2.num_start(), 1);

        let second_pop_qid = result2.iter_qids().next().unwrap();
        qids.retain(|qid| qid != second_pop_qid);
        assert!(qids.is_empty());
        // both are empty
        assert_eq!(
            scheduler.get_status(&qid1).unwrap().status,
            SchedulingStatus::Empty,
        );
        assert_eq!(
            scheduler.get_status(&qid2).unwrap().status,
            SchedulingStatus::Empty,
        );
    }

    /// Test: Scheduler respects max_items_per_decision limit.
    #[restate_core::test]
    async fn test_max_items_per_decision_limit() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();

        // Create 5 vqueues with 1 item each
        let mut txn = rocksdb.transaction();
        for i in 1..=5u64 {
            let qid = test_qid(9000 + i);
            enqueue_entry(
                &mut txn,
                &mut cache,
                &qid,
                i as u8,
                NewEntryPriority::UserDefault,
                None,
            )
            .await;
        }
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();

        // Create scheduler with max_items_per_decision = 2
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(2).unwrap(), // max 2 items per decision
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
        );

        // First poll should yield at most 2 items
        let result = poll_scheduler(pin!(&mut scheduler));
        if let Poll::Ready(Ok(decision)) = result {
            assert!(!decision.is_empty());
            assert!(decision.total_items() <= 2);
        }
    }

    /// Test: Higher priority item enqueued between polls preempts lower priority head.
    ///
    /// Scenario:
    /// 1. Start with 7 low priority items already in inbox
    /// 2. Pop a couple of items, not confirmed yet.
    /// 3. Enqueue a new higher priority item
    /// 4. Dequeuing should return the high priority item before continuing with the low priority items
    /// 5. Confirm the assignments (move to running)
    /// 6. Dequeue the rest.
    // Temporarily disabled while VQueueConfig::concurrency() is hardcoded to 1.
    // Re-enable and re-evaluate this test when VQueueConfig is fully removed.
    #[restate_core::test]
    #[ignore = "Re-enable when VQueueConfig is removed"]
    async fn test_higher_priority_preempts_between_polls() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
        let test_qid = test_qid(14_000);
        let mut events = Vec::new();

        // Enqueue low priority item first
        let mut txn = rocksdb.transaction();
        // 7 low priority items
        for i in 1..=7 {
            enqueue_entry(
                &mut txn,
                &mut cache,
                &test_qid,
                i, // low priority item
                NewEntryPriority::UserDefault,
                None,
            )
            .await;
        }
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(), // limit_qid_per_poll
            // Important so we can slow down how the scheduler advances in each poll
            NonZeroU16::new(2).unwrap(), // max_items_per_decision
            create_resource_manager(db, Concurrency::new_unlimited()).await,
            db.clone(),
            cache.view(),
        );

        let mut in_flight = Vec::new();

        // Poll - should yield 2 low priority item first
        let result = poll_scheduler(pin!(&mut scheduler));
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };

        assert_eq!(decision.num_start(), 2);

        for (qid, assignments) in decision {
            assert_eq!(qid, test_qid);

            // we expect the items to return in their natural ID order because the
            // creation timestamp is identical (for the purpose of the test)
            for (action, entries) in assignments.iter() {
                assert_eq!(entries.len(), 2);
                assert_eq!(action, Action::MoveToRun);
                assert_eq!(entries[0].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[0].item.id, EntryId::new([1; 16]));
                in_flight.push(entries[0].item.clone());

                assert_eq!(entries[1].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[1].item.id, EntryId::new([2; 16]));
                in_flight.push(entries[1].item.clone());
            }
        }

        let status = scheduler.get_status(&test_qid).unwrap();
        assert_eq!(status.status, SchedulingStatus::Ready);
        // 5 left in inbox from scheduler's perspective
        assert_eq!(status.waiting_inbox, 5);
        assert_eq!(status.remaining_running, 0);

        // Now enqueue high priority item
        let mut txn = rocksdb.transaction();
        events.clear();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &test_qid,
            125, // high priority item but with high ID
            NewEntryPriority::UserHigh,
            Some(&mut events),
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        // Notify about the new items
        for event in events.drain(..) {
            scheduler.on_inbox_event(event);
        }

        // Poll - should yield high priority item first
        let result = poll_scheduler(pin!(&mut scheduler));
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_start(), 2);

        for (qid, assignments) in decision {
            assert_eq!(qid, test_qid);

            for (action, entries) in assignments.iter() {
                assert_eq!(entries.len(), 2);
                assert_eq!(action, Action::MoveToRun);
                // Verify first item is high priority
                assert_eq!(entries[0].item.priority, EffectivePriority::UserHigh);
                assert_eq!(entries[0].item.id, EntryId::new([125; 16]));
                in_flight.push(entries[0].item.clone());

                // The second item is the default priority again
                assert_eq!(entries[1].item.priority, EffectivePriority::UserDefault);
                // the scheduler must skip (1, 2) by internal tracking of the unconfirmed
                // assignments.
                assert_eq!(entries[1].item.id, EntryId::new([3; 16]));
                in_flight.push(entries[1].item.clone());
            }
        }

        let status = scheduler.get_status(&test_qid).unwrap();
        assert_eq!(status.status, SchedulingStatus::Ready);
        // we added one, and took 2 (5 + 1 - 2 = 4)
        assert_eq!(status.waiting_inbox, 4);
        assert_eq!(status.remaining_running, 0);
        // let's confirm all the items
        events.clear();
        let mut txn = rocksdb.transaction();
        for item in in_flight.drain(..) {
            let new_card =
                move_to_running(&mut txn, &mut cache, &test_qid, &item, Some(&mut events)).await;
            assert_eq!(new_card.priority, EffectivePriority::TokenHeld);
        }
        txn.commit().await.expect("commit should succeed");

        assert_eq!(events.len(), 4);
        for event in events.drain(..) {
            assert!(
                event
                    .updates
                    .iter()
                    .any(|update| matches!(update, EventDetails::RunAttemptConfirmed { .. }))
            );
            scheduler.on_inbox_event(event);
        }

        // polling the rest of the items
        let result = poll_scheduler(pin!(&mut scheduler));
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_start(), 2);

        for (_, assignments) in decision {
            for (action, entries) in assignments.iter() {
                assert_eq!(entries.len(), 2);
                assert_eq!(action, Action::MoveToRun);
                // Verify first item is high priority
                assert_eq!(entries[0].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[0].item.id, EntryId::new([4; 16]));
                in_flight.push(entries[0].item.clone());

                assert_eq!(entries[1].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[1].item.id, EntryId::new([5; 16]));
                in_flight.push(entries[1].item.clone());
            }
        }

        let result = poll_scheduler(pin!(&mut scheduler));
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_start(), 2);

        for (_, assignments) in decision {
            for (action, entries) in assignments.iter() {
                assert_eq!(entries.len(), 2);
                assert_eq!(action, Action::MoveToRun);
                // Verify first item is high priority
                assert_eq!(entries[0].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[0].item.id, EntryId::new([6; 16]));
                in_flight.push(entries[0].item.clone());

                assert_eq!(entries[1].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[1].item.id, EntryId::new([7; 16]));
                in_flight.push(entries[1].item.clone());
            }
        }

        // No more items
        let result = poll_scheduler(pin!(&mut scheduler));
        assert!(matches!(result, Poll::Pending));
        // check status
        let status: Vec<_> = scheduler.iter_status().collect();
        // Why? because the schedule will hold on to this vqueue until we confirm all their
        // pending assignments.
        assert_eq!(status.len(), 1);
        for (_, s) in status {
            assert_eq!(s.status, SchedulingStatus::Empty);
        }

        // let's confirm everything
        events.clear();
        let mut txn = rocksdb.transaction();
        for item in in_flight.drain(..) {
            let new_card =
                move_to_running(&mut txn, &mut cache, &test_qid, &item, Some(&mut events)).await;
            assert_eq!(new_card.priority, EffectivePriority::TokenHeld);
        }
        txn.commit().await.expect("commit should succeed");

        assert_eq!(events.len(), 4);
        for event in events.drain(..) {
            assert!(
                event
                    .updates
                    .iter()
                    .any(|update| matches!(update, EventDetails::RunAttemptConfirmed { .. }))
            );
            scheduler.on_inbox_event(event);
        }

        // The scheduler should forget about those vqueues now.
        let status: Vec<_> = scheduler.iter_status().collect();
        assert_eq!(status.len(), 0);
    }

    /// Test: get_status returns different statuses based on queue state.
    // Temporarily disabled while VQueueConfig::concurrency() is hardcoded to 1.
    // Re-enable and re-evaluate this test when VQueueConfig is fully removed.
    #[restate_core::test]
    #[ignore = "Re-enable when VQueueConfig is removed"]
    async fn test_get_status_reflects_scheduling_states() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaCache::new_empty();
        let qid1 = test_qid(21_001);
        let qid2 = test_qid(21_002);

        // Enqueue items in two queues
        let mut txn = rocksdb.transaction();
        // QID1
        // - 2 items in running stage
        // - 1 items in inbox stage
        for i in 1..=2u64 {
            let card = enqueue_entry(
                &mut txn,
                &mut cache,
                &qid1,
                i as u8,
                NewEntryPriority::UserDefault,
                None,
            )
            .await;
            move_to_running(&mut txn, &mut cache, &qid1, &card, None).await;
        }
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid1,
            3u8,
            NewEntryPriority::UserDefault,
            None,
        )
        .await;
        // QID2
        // - 1 item in inbox
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid2,
            10,
            NewEntryPriority::UserDefault,
            None,
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        // Create scheduler with concurrency limit of 1
        let mut scheduler = DRRScheduler::new(
            NonZeroU16::new(100).unwrap(), // limit_qid_per_poll
            NonZeroU16::new(10).unwrap(),  // max_items_per_decision
            // only one concurrency token
            create_resource_manager(db, Concurrency::new(Some(NonZeroUsize::new(1).unwrap())))
                .await,
            db.clone(),
            cache.view(),
        );

        // Both queues should be Ready initially
        assert_eq!(
            scheduler.get_status(&qid1).unwrap().status,
            SchedulingStatus::Ready
        );
        assert_eq!(
            scheduler.get_status(&qid2).unwrap().status,
            SchedulingStatus::Ready
        );

        // Poll
        let result = poll_scheduler(pin!(&mut scheduler));
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        // we expect 3 items because:
        // - 2 are at the running stage does not require global concurrency token
        // - only 1 item can be acquired from waiting inbox before we are blocked on capacity
        // and we expect to see both queues. Due to load balancing, we expect the single waiting
        // item of qid2 to be the first non-running item in the returned decision.
        assert_eq!(decision.total_items(), 3);
        assert_eq!(decision.num_queues(), 2);

        // qid1 has 1 item left and it's blocked on capacity
        let status = scheduler.get_status(&qid1).unwrap();
        assert_eq!(
            status.status,
            SchedulingStatus::BlockedOn(ResourceKind::InvokerConcurrency)
        );
        assert_eq!(status.waiting_inbox, 1);
        assert_eq!(status.remaining_running, 0);

        // qid2 is exhausted
        let status = scheduler.get_status(&qid2).unwrap();
        assert_eq!(status.status, SchedulingStatus::Empty);
        assert_eq!(status.waiting_inbox, 0);
        assert_eq!(status.remaining_running, 0);

        // Pop the permit from the scheduler to release the concurrency token.
        // Only inbox items acquire permits, running items yield without permits.
        // The inbox item from qid2 is the only one that acquired a permit.
        let inbox_item_hash = decision
            .into_iter()
            .flat_map(|(_, a)| a.into_iter_per_action())
            .flat_map(|(_, items)| items)
            .find(|e| e.item.priority.is_new())
            .unwrap()
            .item
            .unique_hash();
        let mut scheduler = pin!(scheduler);
        let resources = scheduler.as_mut().pop_resources(inbox_item_hash);
        assert!(resources.is_some());
        drop(resources);

        let result = poll_scheduler(scheduler.as_mut());
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 1);
        assert_eq!(decision.num_queues(), 1);

        assert_eq!(
            scheduler.get_status(&qid1).unwrap().status,
            SchedulingStatus::Empty
        );
        assert_eq!(
            scheduler.get_status(&qid2).unwrap().status,
            SchedulingStatus::Empty
        );
        assert!(matches!(poll_scheduler(scheduler.as_mut()), Poll::Pending));
    }
}
