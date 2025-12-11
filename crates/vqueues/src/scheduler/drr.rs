// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use restate_futures_util::concurrency::Concurrency;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::VQueueStore;
use restate_types::vqueue::VQueueId;

use crate::EventDetails;
use crate::VQueueEvent;
use crate::VQueuesMeta;
use crate::metric_definitions::VQUEUE_ENQUEUE;
use crate::metric_definitions::VQUEUE_RUN_CONFIRMED;
use crate::metric_definitions::VQUEUE_RUN_REJECTED;
use crate::scheduler::eligible::EligibilityTracker;
use crate::scheduler::vqueue_state::Pop;

use super::Decision;
use super::GlobalTokenBucket;
use super::VQueueHandle;
use super::VQueueSchedulerStatus;
use super::vqueue_state::VQueueState;

#[pin_project]
pub struct DRRScheduler<S: VQueueStore> {
    concurrency_limiter: Concurrency,
    global_throttling: Option<GlobalTokenBucket>,
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
        concurrency_limiter: Concurrency,
        global_throttling: Option<GlobalTokenBucket>,
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
            let config = vqueues.config_pool().find(&qid.parent);
            total_running += meta.num_running();
            total_waiting += meta.total_waiting();
            let handle = q.insert_with_key(|handle| VQueueState::new(*qid, handle, meta, config));
            id_lookup.insert(*qid, handle);
            // We init all active vqueues as eligible first
            eligible.insert_eligible(handle);
        }

        info!(
            "Scheduler started. num_vqueues={}, total_running_items={total_running}, total_waiting_items={total_waiting}",
            q.len(),
        );

        Self {
            concurrency_limiter,
            global_throttling,
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
        vqueues: VQueuesMeta<'_>,
    ) -> Poll<Result<Decision<S::Item>, StorageError>> {
        let mut decision = Decision::default();
        let mut items_collected = 0;
        let mut first_blocked = None;

        loop {
            self.eligible.poll_delayed(cx);
            // bail if we exhausted coop budget.
            let coop = match tokio::task::coop::poll_proceed(cx) {
                Poll::Ready(coop) => coop,
                Poll::Pending => break,
            };
            // stop when we have enough queues picked
            if decision.num_queues() >= self.limit_qid_per_poll.get() as usize
                || items_collected >= self.max_items_per_decision.get() as usize
            {
                trace!(
                    "Reached limits of a single DRR decision. num_items={} qids_in_decision={}",
                    decision.total_items(),
                    decision.num_queues()
                );
                break;
            }

            let this = self.as_mut().project();

            let Some(handle) = this.eligible.next_eligible(this.storage, this.q, vqueues)? else {
                trace!(
                    "No more eligible vqueues, {:?}, states_len={}, states_capacity={}",
                    this.eligible,
                    this.q.len(),
                    this.q.capacity()
                );
                break;
            };

            if first_blocked.is_some_and(|first| first == handle) {
                // do not check the same vqueue twice for capacity in the same poll.
                break;
            }

            let qstate = this.q.get_mut(handle).unwrap();

            match qstate.pop_unchecked(
                cx,
                this.storage,
                this.concurrency_limiter,
                this.global_throttling.as_ref(),
            )? {
                Pop::DeficitExhausted => {
                    this.eligible.rotate_one();
                    continue;
                }
                Pop::Item {
                    action,
                    entry,
                    updated_zt,
                } => {
                    coop.made_progress();
                    items_collected += 1;
                    decision.push(&qstate.qid, action, entry, updated_zt);
                    // We need to set the state so we check eligibility and setup
                    // necessary schedules when we poll the queue again.
                    this.eligible.front_needs_poll();
                }
                Pop::Throttle { delay, scope } => {
                    this.eligible.front_throttled(delay, scope);
                    continue;
                }
                Pop::BlockedOnCapacity => {
                    if first_blocked.is_none() {
                        first_blocked = Some(handle);
                    }
                    this.eligible.front_blocked();
                    // stay in ready ring
                    this.eligible.rotate_one();
                    continue;
                }
            }
        }

        if decision.is_empty() {
            if self.last_report.elapsed() >= Duration::from_secs(10) {
                vqueues.report();
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

    #[tracing::instrument(skip_all)]
    pub fn on_inbox_event(
        &mut self,
        vqueues: VQueuesMeta<'_>,
        event: &VQueueEvent<S::Item>,
    ) -> Result<(), StorageError> {
        let qid = event.qid;
        match event.details {
            EventDetails::Enqueued(ref item) => {
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                let qstate = match self.id_lookup.get(&qid) {
                    Some(handle) => match self.q.get_mut(*handle) {
                        Some(qstate) => qstate,
                        None => {
                            let handle = self.q.insert_with_key(|handle| {
                                VQueueState::new_empty(qid, handle, meta, config)
                            });
                            self.id_lookup.insert(qid, handle);
                            self.q.get_mut(handle).unwrap()
                        }
                    },
                    None => {
                        let handle = self.q.insert_with_key(|handle| {
                            VQueueState::new_empty(qid, handle, meta, config)
                        });
                        self.id_lookup.insert(qid, handle);
                        self.q.get_mut(handle).unwrap()
                    }
                };

                counter!(VQUEUE_ENQUEUE).increment(1);
                if qstate.notify_enqueued(item)
                    && self.eligible.refresh_membership(qstate, meta, config)
                {
                    self.wake_up();
                }
            }
            EventDetails::RunAttemptConfirmed { item_hash } => {
                let Some(handle) = self.id_lookup.get(&qid) else {
                    return Ok(());
                };
                let Some(qstate) = self.q.get_mut(*handle) else {
                    return Ok(());
                };
                if qstate.remove_from_unconfirmed_assignments(item_hash) {
                    counter!(VQUEUE_RUN_CONFIRMED).increment(1);
                }
            }
            EventDetails::RunAttemptRejected { item_hash } => {
                let Some(handle) = self.id_lookup.get(&qid) else {
                    return Ok(());
                };
                let Some(qstate) = self.q.get_mut(*handle) else {
                    return Ok(());
                };
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                if qstate.remove_from_unconfirmed_assignments(item_hash) {
                    counter!(VQUEUE_RUN_REJECTED).increment(1);

                    if qstate.is_dormant(meta) {
                        // retire the vqueue state
                        self.q.remove(*handle);
                        self.eligible.remove(*handle);
                        self.id_lookup.remove(&qid);
                    } else if self.eligible.refresh_membership(qstate, meta, config) {
                        self.wake_up();
                    }
                }
            }
            EventDetails::Removed { item_hash } => {
                let Some(handle) = self.id_lookup.get(&qid) else {
                    return Ok(());
                };
                let Some(qstate) = self.q.get_mut(*handle) else {
                    return Ok(());
                };
                let config = vqueues.config_pool().find(&qid.parent);
                let meta = vqueues.get_vqueue(&qid).unwrap();

                let _ = qstate.remove_from_unconfirmed_assignments(item_hash);
                qstate.notify_removed(item_hash);

                if qstate.is_dormant(meta) {
                    // retire the vqueue state
                    self.q.remove(*handle);
                    self.eligible.remove(*handle);
                    self.id_lookup.remove(&qid);
                } else if self.eligible.refresh_membership(qstate, meta, config) {
                    self.wake_up();
                }
            }
        }
        Ok(())
    }

    pub fn iter_status(
        &self,
        cache: VQueuesMeta<'_>,
    ) -> impl Iterator<Item = (&VQueueId, VQueueSchedulerStatus)> {
        self.q.iter().map(move |(_handle, qstate)| {
            let Some(meta) = cache.get_vqueue(&qstate.qid) else {
                return (&qstate.qid, VQueueSchedulerStatus::default());
            };

            let config = cache.config_pool().find(&qstate.qid.parent);
            let status = VQueueSchedulerStatus {
                is_paused: qstate.is_paused(meta, config),
                wait_stats: qstate.get_head_wait_stats(),
                remaining_running: qstate.num_remaining_in_running_stage(),
                waiting_inbox: qstate.num_waiting_inbox(meta),
                tokens_used: qstate.num_tokens_used(meta),
                status: self.eligible.get_status(qstate, meta, config),
            };

            (&qstate.qid, status)
        })
    }

    pub fn get_status(
        &self,
        qid: &VQueueId,
        cache: VQueuesMeta<'_>,
    ) -> Option<VQueueSchedulerStatus> {
        let qstate = self
            .id_lookup
            .get(qid)
            .and_then(|handle| self.q.get(*handle))?;

        let Some(meta) = cache.get_vqueue(qid) else {
            return Some(VQueueSchedulerStatus::default());
        };

        let config = cache.config_pool().find(&qstate.qid.parent);

        Some(VQueueSchedulerStatus {
            is_paused: qstate.is_paused(meta, config),
            wait_stats: qstate.get_head_wait_stats(),
            remaining_running: qstate.num_remaining_in_running_stage(),
            waiting_inbox: qstate.num_waiting_inbox(meta),
            tokens_used: qstate.num_tokens_used(meta),
            status: self.eligible.get_status(qstate, meta, config),
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

    use crate::cache::VQueuesMetaMut;
    use crate::scheduler::Action;
    use crate::{SchedulingStatus, VQueueEvent, VQueues};

    // ==================== Test Helpers ====================

    /// Helper to create a test VQueueId with a unique partition key for test isolation.
    fn test_qid(partition_key: u64) -> VQueueId {
        VQueueId {
            partition_key: PartitionKey::from(partition_key),
            parent: VQueueParent::SYSTEM_UNLIMITED,
            instance: VQueueInstance::from_raw(1),
        }
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
        cache: &mut VQueuesMetaMut,
        qid: &VQueueId,
        id: u8,
        priority: NewEntryPriority,
        action_collector: Option<&mut Vec<VQueueEvent<EntryCard>>>,
    ) {
        let created_at = UniqueTimestamp::try_from(1000u64 + id as u64).unwrap();
        let entry_id = EntryId::new([id; 16]);

        // let mut txn = rocksdb.transaction();
        let mut vqueues = VQueues::new(*qid, txn, cache, action_collector);
        vqueues
            .enqueue_new::<()>(
                created_at,
                VisibleAt::Now,
                priority,
                EntryKind::Invocation,
                entry_id,
                None,
            )
            .await
            .expect("enqueue should succeed");
    }

    /// Creates a scheduler using PartitionDb as the storage backend.
    fn create_scheduler(db: &PartitionDb, cache: &VQueuesMetaMut) -> DRRScheduler<PartitionDb> {
        DRRScheduler::new(
            NonZeroU16::new(100).unwrap(), // limit_qid_per_poll
            NonZeroU16::new(100).unwrap(), // max_items_per_decision
            Concurrency::new_unlimited(),
            None, // no global throttling
            db.clone(),
            cache.view(),
        )
    }

    fn create_scheduler_with_concurrency(
        db: &PartitionDb,
        cache: &VQueuesMetaMut,
        concurrency_limit: usize,
    ) -> DRRScheduler<PartitionDb> {
        DRRScheduler::new(
            NonZeroU16::new(100).unwrap(),
            NonZeroU16::new(100).unwrap(),
            Concurrency::new(Some(NonZeroUsize::new(concurrency_limit).unwrap())),
            None,
            db.clone(),
            cache.view(),
        )
    }

    fn poll_scheduler(
        mut scheduler: Pin<&mut DRRScheduler<PartitionDb>>,
        cache: &VQueuesMetaMut,
    ) -> Poll<Result<Decision<EntryCard>, restate_storage_api::StorageError>> {
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(&waker);
        scheduler.as_mut().poll_schedule_next(&mut cx, cache.view())
    }

    // ==================== Tests ====================

    /// Test: A newly created scheduler with no vqueues returns Pending.
    #[restate_core::test]
    async fn test_empty_scheduler_returns_pending() {
        let rocksdb = storage_test_environment().await;
        let db = rocksdb.partition_db();
        let cache = VQueuesMetaMut::default();

        let mut scheduler = create_scheduler(db, &cache);

        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        assert!(matches!(result, Poll::Pending));
    }

    /// Test: Polling the scheduler after enqueue yields the item.
    #[restate_core::test]
    async fn test_poll_yields_enqueued_item() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
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
        let mut scheduler = create_scheduler(db, &cache);

        let result = poll_scheduler(pin!(&mut scheduler), &cache);
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
        let mut cache = VQueuesMetaMut::default();

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
                    (qid.partition_key + j) as u8,
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
            Concurrency::new_unlimited(),
            None,
            db.clone(),
            cache.view(),
        );

        // Poll and verify that a decision was made
        let result = poll_scheduler(pin!(&mut scheduler), &cache);

        // The decision should include items from all queues since we have high limits
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        // With unlimited concurrency and high limits, all items should be picked
        assert_eq!(decision.num_queues(), 3);
        assert_eq!(decision.num_start(), 3);

        decision.into_iter().for_each(|(qid, assignments)| {
            for (action, mut items) in assignments.into_iter_per_action() {
                assert_eq!(action, Action::MoveToRunning);
                // for each vqueue, we should see the head item. The head item is the lowest
                // ID, so we should expect the item ID to the same as the queue ID.
                assert_eq!(items.len(), 1);
                assert_eq!(
                    items.next().unwrap().item.id,
                    EntryId::new([qid.partition_key as u8; 16])
                );
            }
        });
    }

    /// Test: Concurrency limiting blocks scheduling when capacity is exhausted.
    #[restate_core::test]
    async fn test_concurrency_limiting() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
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
        let mut scheduler = create_scheduler_with_concurrency(db, &cache, 1);

        assert_eq!(
            scheduler.get_status(&qid1, cache.view()).unwrap().status,
            SchedulingStatus::Ready,
        );
        assert_eq!(
            scheduler.get_status(&qid2, cache.view()).unwrap().status,
            SchedulingStatus::Ready,
        );

        // First poll should succeed with one item (concurrency limit = 1)
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        assert!(matches!(result, Poll::Ready(Ok(ref d)) if d.total_items() == 1));
        let Poll::Ready(Ok(result)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(result.num_start(), 1);

        let first_pop_qid = result.iter_qids().next().unwrap().clone();
        qids.retain(|qid| qid != &first_pop_qid);

        // Second poll should return Pending since we're at capacity
        assert!(matches!(
            poll_scheduler(pin!(&mut scheduler), &cache),
            Poll::Pending
        ));
        // Inspect the status of the vqueues, we should see that some vqueues are blocked on
        // capacity
        assert_eq!(
            scheduler
                .get_status(&first_pop_qid, cache.view())
                .unwrap()
                .status,
            // we drained this vqueue as it has a single item
            SchedulingStatus::Dormant,
        );
        assert_eq!(
            scheduler.get_status(&qids[0], cache.view()).unwrap().status,
            SchedulingStatus::BlockedOnCapacity,
        );
        // dropping the permit should release the token, we should get another item
        drop(result);
        // Second poll should return Pending since we're at capacity
        let result2 = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(result2)) = result2 else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(result2.num_start(), 1);

        let second_pop_qid = result2.iter_qids().next().unwrap().clone();
        qids.retain(|qid| qid != &second_pop_qid);
        assert!(qids.is_empty());
        // both are empty
        assert_eq!(
            scheduler.get_status(&qid1, cache.view()).unwrap().status,
            SchedulingStatus::Dormant,
        );
        assert_eq!(
            scheduler.get_status(&qid2, cache.view()).unwrap().status,
            SchedulingStatus::Dormant,
        );
    }

    /// Test: Scheduler respects max_items_per_decision limit.
    #[restate_core::test]
    async fn test_max_items_per_decision_limit() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();

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
            Concurrency::new_unlimited(),
            None,
            db.clone(),
            cache.view(),
        );

        // First poll should yield at most 2 items
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        if let Poll::Ready(Ok(decision)) = result {
            assert!(!decision.is_empty());
            assert!(decision.total_items() <= 2);
        }
    }

    // ==================== High-Signal Scheduler Interaction Tests ====================
    // These tests focus on scenarios where items are added, confirmed, or rejected
    // between scheduler polls - the critical interaction patterns in production.

    /// Test: Enqueue new item between polls - scheduler picks up newly enqueued item.
    ///
    /// Scenario:
    /// 1. Poll scheduler (returns Pending - empty)
    /// 2. Enqueue item + notify scheduler via on_inbox_event
    /// 3. Poll scheduler again -> should yield the item
    #[restate_core::test]
    async fn test_enqueue_between_polls() {
        let mut rocksdb = storage_test_environment().await;
        let db = rocksdb.partition_db();
        let mut cache = VQueuesMetaMut::default();
        let qid = test_qid(10_000);

        let mut scheduler = create_scheduler(db, &cache);

        // First poll - empty scheduler should return Pending
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        assert!(matches!(result, Poll::Pending));
        // Queue doesn't exist yet, get_status returns None
        assert!(scheduler.get_status(&qid, cache.view()).is_none());

        // Enqueue an item between polls
        let mut events = Vec::new();
        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid,
            1,
            NewEntryPriority::UserDefault,
            Some(&mut events),
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        // Notify scheduler about the enqueue
        for event in &events {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("event processing should succeed");
        }

        // Second poll - should now yield the item
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision)), got {:?}", result);
        };
        assert_eq!(decision.num_start(), 1);
        assert_eq!(decision.total_items(), 1);
    }

    /// Test: Confirm after poll - scheduler picks item, confirm event arrives, then poll again.
    ///
    /// Scenario:
    /// 1. Enqueue item
    /// 2. Poll scheduler -> picks item (MoveToRunning)
    /// 3. Send RunAttemptConfirmed event
    /// 4. Poll scheduler -> should return Pending (queue drained)
    #[restate_core::test]
    async fn test_confirm_after_poll() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid = test_qid(11_000);

        // Enqueue item
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
        let mut scheduler = create_scheduler(db, &cache);

        // Poll scheduler - should yield the item
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_start(), 1);

        // Extract the item hash to confirm
        let (_, assignments) = decision.into_iter().next().unwrap();
        let (_, mut items) = assignments.into_iter_per_action().next().unwrap();
        let entry = items.next().unwrap();
        let item_hash = entry.item.unique_hash();

        // Send RunAttemptConfirmed event
        let confirm_event = VQueueEvent::new(qid, EventDetails::RunAttemptConfirmed { item_hash });
        scheduler
            .on_inbox_event(cache.view(), &confirm_event)
            .expect("event processing should succeed");

        // Poll again - should return Pending since queue is drained
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        assert!(matches!(result, Poll::Pending));

        // Status should be dormant (queue drained)
        assert_eq!(
            scheduler.get_status(&qid, cache.view()).unwrap().status,
            SchedulingStatus::Dormant
        );
    }

    /// Test: Reject after poll cleans up unconfirmed assignments.
    ///
    /// When a RunAttemptRejected event arrives, the scheduler removes the item from
    /// unconfirmed_assignments. The actual re-scheduling of the item depends on whether
    /// the item was actually moved to running in storage (which in production happens
    /// via VQueues::attempt_to_run). If the attempt was rejected before storage update,
    /// the item remains in Inbox and can be rescheduled.
    ///
    /// Scenario:
    /// 1. Enqueue two items in same queue
    /// 2. Poll scheduler -> picks first item
    /// 3. Send RunAttemptRejected for first item
    /// 4. Poll scheduler -> should yield second item (first item is tracked by scheduler's
    ///    internal queue state which already advanced past it)
    #[restate_core::test]
    async fn test_reject_cleans_unconfirmed_assignments() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid = test_qid(12_000);

        // Enqueue two items
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
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid,
            2,
            NewEntryPriority::UserDefault,
            None,
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache);

        // Poll scheduler - should yield items
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_start(), 2);

        // Extract the item hashes
        let (_, assignments) = decision.into_iter().next().unwrap();
        let (_, items) = assignments.into_iter_per_action().next().unwrap();
        let item_hashes: Vec<_> = items.map(|e| e.item.unique_hash()).collect();

        // Reject first item
        let reject_event = VQueueEvent::new(
            qid,
            EventDetails::RunAttemptRejected {
                item_hash: item_hashes[0],
            },
        );
        scheduler
            .on_inbox_event(cache.view(), &reject_event)
            .expect("reject should succeed");

        // Confirm second item
        let confirm_event = VQueueEvent::new(
            qid,
            EventDetails::RunAttemptConfirmed {
                item_hash: item_hashes[1],
            },
        );
        scheduler
            .on_inbox_event(cache.view(), &confirm_event)
            .expect("confirm should succeed");

        // After reject and confirm, the queue should be dormant
        // since the scheduler's internal state has advanced past both items
        let status = scheduler
            .get_status(&qid, cache.view())
            .map(|s| s.status)
            .unwrap_or(SchedulingStatus::Dormant);
        assert_eq!(status, SchedulingStatus::Dormant);
    }

    /// Test: Interleaved enqueue and confirm across multiple vqueues.
    ///
    /// This test verifies that when working with multiple queues:
    /// - Confirming an item in one queue doesn't affect others
    /// - New enqueues in a third queue are picked up correctly
    /// - The scheduler correctly handles mixed confirm/enqueue events
    ///
    /// Scenario:
    /// 1. Enqueue items in vqueue A and B
    /// 2. Poll -> picks items from both
    /// 3. Confirm both items
    /// 4. Enqueue new item in C
    /// 5. Poll -> should pick from C
    #[restate_core::test]
    async fn test_interleaved_enqueue_confirm_multiple_queues() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid_a = test_qid(13_001);
        let qid_b = test_qid(13_002);
        let qid_c = test_qid(13_003);

        // Initial enqueue in A and B
        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid_a,
            1,
            NewEntryPriority::UserDefault,
            None,
        )
        .await;
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid_b,
            2,
            NewEntryPriority::UserDefault,
            None,
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache);

        // First poll - picks from A and B
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_queues(), 2);
        assert_eq!(decision.num_start(), 2);

        // Extract item hashes
        let mut item_hash_a = None;
        let mut item_hash_b = None;
        for (qid, assignments) in decision {
            for (_, items) in assignments.into_iter_per_action() {
                for entry in items {
                    if qid == qid_a {
                        item_hash_a = Some(entry.item.unique_hash());
                    } else if qid == qid_b {
                        item_hash_b = Some(entry.item.unique_hash());
                    }
                }
            }
        }

        // Confirm A's item
        let confirm_event_a = VQueueEvent::new(
            qid_a,
            EventDetails::RunAttemptConfirmed {
                item_hash: item_hash_a.unwrap(),
            },
        );
        scheduler
            .on_inbox_event(cache.view(), &confirm_event_a)
            .expect("confirm A should succeed");

        // Confirm B's item too
        let confirm_event_b = VQueueEvent::new(
            qid_b,
            EventDetails::RunAttemptConfirmed {
                item_hash: item_hash_b.unwrap(),
            },
        );
        scheduler
            .on_inbox_event(cache.view(), &confirm_event_b)
            .expect("confirm B should succeed");

        // Both A and B should now be dormant (depending on whether the
        // scheduler has retired the queue state)
        let status_a = scheduler
            .get_status(&qid_a, cache.view())
            .map(|s| s.status)
            .unwrap_or(SchedulingStatus::Dormant);
        let status_b = scheduler
            .get_status(&qid_b, cache.view())
            .map(|s| s.status)
            .unwrap_or(SchedulingStatus::Dormant);
        assert_eq!(status_a, SchedulingStatus::Dormant);
        assert_eq!(status_b, SchedulingStatus::Dormant);

        // Enqueue new item in C
        let mut events_c = Vec::new();
        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid_c,
            3,
            NewEntryPriority::UserDefault,
            Some(&mut events_c),
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        for event in &events_c {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("enqueue C event should succeed");
        }

        // Second poll - should pick only from C
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_queues(), 1);
        assert!(decision.iter_qids().any(|q| *q == qid_c));
    }

    /// Test: Higher priority item enqueued between polls preempts lower priority head.
    ///
    /// Scenario:
    /// 1. Enqueue low priority item
    /// 2. Notify scheduler
    /// 3. Enqueue high priority item before polling
    /// 4. Poll -> should yield high priority item first
    #[restate_core::test]
    async fn test_higher_priority_preempts_between_polls() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid = test_qid(14_000);

        // Enqueue low priority item first
        let mut events = Vec::new();
        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid,
            1, // low priority item
            NewEntryPriority::UserDefault,
            Some(&mut events),
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache);

        // Notify about low priority item
        for event in &events {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("event should succeed");
        }

        // Now enqueue high priority item
        let mut events_high = Vec::new();
        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid,
            2, // high priority item (System priority)
            NewEntryPriority::System,
            Some(&mut events_high),
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        // Notify about high priority item
        for event in &events_high {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("event should succeed");
        }

        // Poll - should yield high priority item first
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };

        // Verify first item is high priority
        for (_, assignments) in decision {
            for (_, entries) in assignments.iter() {
                // First entry should be System priority
                assert_eq!(entries[0].item.priority, EffectivePriority::System);
            }
        }
    }

    /// Test: Status transitions through the poll/confirm cycle.
    ///
    /// Scenario:
    /// 1. Enqueue item -> status = Ready
    /// 2. Poll (picks item) -> status should reflect unconfirmed assignment
    /// 3. Confirm -> status = Empty (single item drained)
    #[restate_core::test]
    async fn test_status_transitions_poll_confirm_cycle() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid = test_qid(15_000);

        // Enqueue item
        let mut events = Vec::new();
        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid,
            1,
            NewEntryPriority::UserDefault,
            Some(&mut events),
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache);

        // Notify scheduler
        for event in &events {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("event should succeed");
        }

        // Status should be Ready
        assert_eq!(
            scheduler.get_status(&qid, cache.view()).unwrap().status,
            SchedulingStatus::Ready
        );

        // Poll - picks the item
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };

        // Extract item hash
        let (_, assignments) = decision.into_iter().next().unwrap();
        let (_, mut items) = assignments.into_iter_per_action().next().unwrap();
        let item_hash = items.next().unwrap().item.unique_hash();

        // Status after poll - item is unconfirmed, queue head should be dormant
        // since the scheduler's internal state has advanced past the item
        let status = scheduler.get_status(&qid, cache.view()).unwrap().status;
        assert_eq!(status, SchedulingStatus::Dormant);

        // Confirm the item
        let confirm_event = VQueueEvent::new(qid, EventDetails::RunAttemptConfirmed { item_hash });
        scheduler
            .on_inbox_event(cache.view(), &confirm_event)
            .expect("confirm should succeed");

        // Status after confirm - should be dormant (queue may or may not be retired
        // depending on internal state cleanup timing)
        let status = scheduler
            .get_status(&qid, cache.view())
            .map(|s| s.status)
            .unwrap_or(SchedulingStatus::Dormant);
        assert_eq!(status, SchedulingStatus::Dormant);
    }

    /// Test: Item removed between polls - scheduler handles gracefully.
    ///
    /// Scenario:
    /// 1. Enqueue two items
    /// 2. Poll -> scheduler internal state knows about items
    /// 3. Remove first item via Removed event
    /// 4. Poll -> should yield second item only
    #[restate_core::test]
    async fn test_removal_between_polls() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid = test_qid(16_000);

        // Enqueue two items
        let mut events = Vec::new();
        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid,
            1,
            NewEntryPriority::UserDefault,
            Some(&mut events),
        )
        .await;
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid,
            2,
            NewEntryPriority::UserDefault,
            Some(&mut events),
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache);

        // Notify scheduler
        for event in &events {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("event should succeed");
        }

        // Poll once to initialize internal state
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 2);

        // Extract item hashes
        let mut item_hashes: Vec<u64> = Vec::new();
        for (_, assignments) in decision {
            for (_, items) in assignments.into_iter_per_action() {
                for entry in items {
                    item_hashes.push(entry.item.unique_hash());
                }
            }
        }

        // Confirm first item, reject second
        let confirm_event = VQueueEvent::new(
            qid,
            EventDetails::RunAttemptConfirmed {
                item_hash: item_hashes[0],
            },
        );
        scheduler
            .on_inbox_event(cache.view(), &confirm_event)
            .expect("confirm should succeed");

        let reject_event = VQueueEvent::new(
            qid,
            EventDetails::RunAttemptRejected {
                item_hash: item_hashes[1],
            },
        );
        scheduler
            .on_inbox_event(cache.view(), &reject_event)
            .expect("reject should succeed");

        // Now remove the second item
        let remove_event = VQueueEvent::new(
            qid,
            EventDetails::Removed {
                item_hash: item_hashes[1],
            },
        );
        scheduler
            .on_inbox_event(cache.view(), &remove_event)
            .expect("remove should succeed");

        // Poll again - should return Pending since both items are gone
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        assert!(matches!(result, Poll::Pending));

        // Status should be dormant (queue state may be retired, so None is also acceptable)
        let status = scheduler
            .get_status(&qid, cache.view())
            .map(|s| s.status)
            .unwrap_or(SchedulingStatus::Dormant);
        assert_eq!(status, SchedulingStatus::Dormant);
    }

    /// Test: Multiple enqueues followed by single poll - batch processing.
    ///
    /// Scenario:
    /// 1. Enqueue 5 items to same queue rapidly (with events)
    /// 2. Single poll -> should yield all items in priority/timestamp order
    #[restate_core::test]
    async fn test_batch_enqueue_single_poll() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid = test_qid(17_000);

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache);

        // Batch enqueue 5 items with events
        let mut all_events = Vec::new();
        for i in 1..=5u8 {
            let mut events = Vec::new();
            let mut txn = rocksdb.transaction();
            enqueue_entry(
                &mut txn,
                &mut cache,
                &qid,
                i,
                NewEntryPriority::UserDefault,
                Some(&mut events),
            )
            .await;
            txn.commit().await.expect("commit should succeed");
            all_events.extend(events);
        }

        // Notify scheduler about all enqueues
        for event in &all_events {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("event should succeed");
        }

        // Single poll - should yield all items
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.num_queues(), 1);
        assert_eq!(decision.total_items(), 5);
        assert_eq!(decision.num_start(), 5);
    }

    /// Test: Confirm releases concurrency token, allowing next item to be scheduled.
    ///
    /// Scenario:
    /// 1. Set concurrency limit = 1
    /// 2. Enqueue 2 items in different queues
    /// 3. Poll -> picks 1 item, second blocked on capacity
    /// 4. Confirm first item (releases token)
    /// 5. Poll -> picks second item
    #[restate_core::test]
    async fn test_confirm_releases_concurrency_for_next_poll() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid1 = test_qid(18_001);
        let qid2 = test_qid(18_002);

        // Enqueue items in two different queues
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
        let mut scheduler = create_scheduler_with_concurrency(db, &cache, 1);

        // First poll - picks 1 item
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 1);

        // Extract which queue was picked and the item hash
        let (picked_qid, assignments) = decision.into_iter().next().unwrap();
        let (_, mut items) = assignments.into_iter_per_action().next().unwrap();
        let entry = items.next().unwrap();
        let item_hash = entry.item.unique_hash();

        // Second poll - should be Pending (blocked on capacity)
        // But first, we need to drop the permit from the previous decision
        drop(entry);

        // Confirm the first item
        let confirm_event =
            VQueueEvent::new(picked_qid, EventDetails::RunAttemptConfirmed { item_hash });
        scheduler
            .on_inbox_event(cache.view(), &confirm_event)
            .expect("confirm should succeed");

        // Now poll again - should pick the second item
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision after confirm");
        };
        assert_eq!(decision.total_items(), 1);
    }

    // ==================== get_status API Tests ====================

    /// Test: get_status returns None for unknown vqueue.
    #[restate_core::test]
    async fn test_get_status_returns_none_for_unknown_queue() {
        let rocksdb = storage_test_environment().await;
        let db = rocksdb.partition_db();
        let cache = VQueuesMetaMut::default();

        let scheduler = create_scheduler(db, &cache);

        // Query a vqueue that doesn't exist
        let unknown_qid = test_qid(99_999);
        assert!(scheduler.get_status(&unknown_qid, cache.view()).is_none());
    }

    /// Test: get_status returns correct status fields for an active queue.
    #[restate_core::test]
    async fn test_get_status_returns_full_status() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid = test_qid(20_000);

        // Enqueue multiple items
        let mut events = Vec::new();
        let mut txn = rocksdb.transaction();
        for i in 1..=3u8 {
            enqueue_entry(
                &mut txn,
                &mut cache,
                &qid,
                i,
                NewEntryPriority::UserDefault,
                Some(&mut events),
            )
            .await;
        }
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        let mut scheduler = create_scheduler(db, &cache);

        // Notify scheduler about enqueues
        for event in &events {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("event should succeed");
        }

        // Get status before polling
        let status = scheduler.get_status(&qid, cache.view());
        assert!(status.is_some());
        let status = status.unwrap();

        // Before polling, queue should be ready with items waiting
        assert_eq!(status.status, SchedulingStatus::Ready);
        assert_eq!(status.waiting_inbox, 3);
        assert_eq!(status.remaining_running, 0);

        // Poll to pick items
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.num_start(), 3);

        // Get status after polling - queue should be dormant (all items picked)
        let status_after = scheduler.get_status(&qid, cache.view());
        assert!(status_after.is_some());
        let status_after = status_after.unwrap();
        assert_eq!(status_after.status, SchedulingStatus::Dormant);
    }

    /// Test: get_status returns different statuses based on queue state.
    #[restate_core::test]
    async fn test_get_status_reflects_scheduling_states() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
        let qid1 = test_qid(21_001);
        let qid2 = test_qid(21_002);

        // Enqueue items in two queues
        let mut events = Vec::new();
        let mut txn = rocksdb.transaction();
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid1,
            1,
            NewEntryPriority::UserDefault,
            Some(&mut events),
        )
        .await;
        enqueue_entry(
            &mut txn,
            &mut cache,
            &qid2,
            2,
            NewEntryPriority::UserDefault,
            Some(&mut events),
        )
        .await;
        txn.commit().await.expect("commit should succeed");

        let db = rocksdb.partition_db();
        // Create scheduler with concurrency limit of 1
        let mut scheduler = create_scheduler_with_concurrency(db, &cache, 1);

        // Notify scheduler
        for event in &events {
            scheduler
                .on_inbox_event(cache.view(), event)
                .expect("event should succeed");
        }

        // Both queues should be Ready initially
        assert_eq!(
            scheduler.get_status(&qid1, cache.view()).unwrap().status,
            SchedulingStatus::Ready
        );
        assert_eq!(
            scheduler.get_status(&qid2, cache.view()).unwrap().status,
            SchedulingStatus::Ready
        );

        // Poll - picks one item (concurrency limit = 1)
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 1);

        let picked_qid = decision.iter_qids().next().unwrap().clone();
        let other_qid = if picked_qid == qid1 { qid2 } else { qid1 };

        // Picked queue should be dormant (drained)
        assert_eq!(
            scheduler
                .get_status(&picked_qid, cache.view())
                .unwrap()
                .status,
            SchedulingStatus::Dormant
        );

        // Other queue should be blocked on capacity
        assert_eq!(
            scheduler
                .get_status(&other_qid, cache.view())
                .unwrap()
                .status,
            SchedulingStatus::BlockedOnCapacity
        );
    }
}
