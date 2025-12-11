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

                    let meta = vqueues.get_vqueue(&qid).unwrap();
                    let config = vqueues.config_pool().find(&qid.parent);
                    if qstate.is_dormant(meta, config) {
                        // retire the vqueue state
                        self.q.remove(*handle);
                        self.eligible.remove(*handle);
                        self.id_lookup.remove(&qid);
                    }
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

                    if qstate.is_dormant(meta, config) {
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

                if qstate.is_dormant(meta, config) {
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
    ) -> impl Iterator<Item = (VQueueId, VQueueSchedulerStatus)> {
        self.q.iter().map(move |(_handle, qstate)| {
            let Some(meta) = cache.get_vqueue(&qstate.qid) else {
                return (qstate.qid, VQueueSchedulerStatus::default());
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

            (qstate.qid, status)
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
    use crate::scheduler::{Action, IsPaused};
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
    ) -> EntryCard {
        let created_at = UniqueTimestamp::try_from(1000u64 + id as u64).unwrap();
        let entry_id = EntryId::new([id; 16]);

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
            .expect("enqueue should succeed")
    }

    async fn move_to_running(
        txn: &mut PartitionStoreTransaction<'_>,
        cache: &mut VQueuesMetaMut,
        qid: &VQueueId,
        card: &EntryCard,
        action_collector: Option<&mut Vec<VQueueEvent<EntryCard>>>,
    ) -> EntryCard {
        let at = UniqueTimestamp::try_from(1100u64 as u64).unwrap();
        let mut vqueues = VQueues::new(*qid, txn, cache, action_collector);

        vqueues
            .attempt_to_run(at, card, None)
            .await
            .expect("attempt_to_run should succeed")
            .expect("attempted to run an item that was not found in waiting inbox")
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
            SchedulingStatus::Empty,
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
            SchedulingStatus::Empty,
        );
        assert_eq!(
            scheduler.get_status(&qid2, cache.view()).unwrap().status,
            SchedulingStatus::Empty,
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

    /// Test: Higher priority item enqueued between polls preempts lower priority head.
    ///
    /// Scenario:
    /// 1. Start with 7 low priority items already in inbox
    /// 2. Pop a couple of items, not confirmed yet.
    /// 3. Enqueue a new higher priority item
    /// 4. Dequeuing should return the high priority item before continuing with the low priority items
    /// 5. Confirm the assignments (move to running)
    /// 6. Dequeue the rest.
    #[restate_core::test]
    async fn test_higher_priority_preempts_between_polls() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
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
            Concurrency::new_unlimited(),
            None, // no global throttling
            db.clone(),
            cache.view(),
        );

        let mut in_flight = Vec::new();

        // Poll - should yield 2 low priority item first
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };

        assert_eq!(decision.num_start(), 2);

        // Verify first item is high priority
        for (qid, assignments) in decision {
            assert_eq!(qid, test_qid);

            // we expect the items to return in their natural ID order because the
            // creation timestamp is identical (for the purpose of the test)
            for (action, entries) in assignments.iter() {
                assert_eq!(entries.len(), 2);
                assert_eq!(action, Action::MoveToRunning);
                assert_eq!(entries[0].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[0].item.id, EntryId::new([1; 16]));
                in_flight.push(entries[0].item.clone());

                assert_eq!(entries[1].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[1].item.id, EntryId::new([2; 16]));
                in_flight.push(entries[1].item.clone());
            }
        }

        let status = scheduler.get_status(&test_qid, cache.view()).unwrap();
        assert_eq!(status.status, SchedulingStatus::Ready);
        // 5 left in inbox from scheduler's perspective
        assert_eq!(status.waiting_inbox, 5);
        assert_eq!(status.remaining_running, 0);
        assert!(matches!(status.is_paused, IsPaused::No));
        assert_eq!(status.tokens_used, 2);

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
            scheduler
                .on_inbox_event(cache.view(), &event)
                .expect("event should succeed");
        }

        // Poll - should yield high priority item first
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_start(), 2);

        for (qid, assignments) in decision {
            assert_eq!(qid, test_qid);

            for (action, entries) in assignments.iter() {
                assert_eq!(entries.len(), 2);
                assert_eq!(action, Action::MoveToRunning);
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

        let status = scheduler.get_status(&test_qid, cache.view()).unwrap();
        assert_eq!(status.status, SchedulingStatus::Ready);
        // we added one, and took 2 (5 + 1 - 2 = 4)
        assert_eq!(status.waiting_inbox, 4);
        assert_eq!(status.remaining_running, 0);
        assert!(matches!(status.is_paused, IsPaused::No));
        assert_eq!(status.tokens_used, 4);
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
            assert!(matches!(
                event.details,
                EventDetails::RunAttemptConfirmed { .. }
            ));
            scheduler
                .on_inbox_event(cache.view(), &event)
                .expect("event should succeed");
        }

        // polling the rest of the items
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_start(), 2);

        for (_, assignments) in decision {
            for (action, entries) in assignments.iter() {
                assert_eq!(entries.len(), 2);
                assert_eq!(action, Action::MoveToRunning);
                // Verify first item is high priority
                assert_eq!(entries[0].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[0].item.id, EntryId::new([4; 16]));
                in_flight.push(entries[0].item.clone());

                assert_eq!(entries[1].item.priority, EffectivePriority::UserDefault);
                assert_eq!(entries[1].item.id, EntryId::new([5; 16]));
                in_flight.push(entries[1].item.clone());
            }
        }

        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected Poll::Ready(Ok(decision))");
        };
        assert_eq!(decision.num_start(), 2);

        for (_, assignments) in decision {
            for (action, entries) in assignments.iter() {
                assert_eq!(entries.len(), 2);
                assert_eq!(action, Action::MoveToRunning);
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
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        assert!(matches!(result, Poll::Pending));
        // check status
        let status: Vec<_> = scheduler.iter_status(cache.view()).collect();
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
            assert!(matches!(
                event.details,
                EventDetails::RunAttemptConfirmed { .. }
            ));
            scheduler
                .on_inbox_event(cache.view(), &event)
                .expect("event should succeed");
        }

        // The scheduler should forget about those vqueues now.
        let status: Vec<_> = scheduler.iter_status(cache.view()).collect();
        assert_eq!(status.len(), 0);
    }

    /// Test: get_status returns different statuses based on queue state.
    #[restate_core::test]
    async fn test_get_status_reflects_scheduling_states() {
        let mut rocksdb = storage_test_environment().await;
        let mut cache = VQueuesMetaMut::default();
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
            Concurrency::new(Some(NonZeroUsize::new(1).unwrap())),
            None,
            db.clone(),
            cache.view(),
        );

        // Both queues should be Ready initially
        assert_eq!(
            scheduler.get_status(&qid1, cache.view()).unwrap().status,
            SchedulingStatus::Ready
        );
        assert_eq!(
            scheduler.get_status(&qid2, cache.view()).unwrap().status,
            SchedulingStatus::Ready
        );

        // Poll
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
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
        let status = scheduler.get_status(&qid1, cache.view()).unwrap();
        assert_eq!(status.status, SchedulingStatus::BlockedOnCapacity);
        assert_eq!(status.waiting_inbox, 1);
        assert_eq!(status.remaining_running, 0);
        assert!(matches!(status.is_paused, IsPaused::No));
        assert_eq!(status.tokens_used, 2);

        // qid is exhausted
        let status = scheduler.get_status(&qid2, cache.view()).unwrap();
        assert_eq!(status.status, SchedulingStatus::Empty);
        assert_eq!(status.waiting_inbox, 0);
        assert_eq!(status.remaining_running, 0);
        assert!(matches!(status.is_paused, IsPaused::No));
        assert_eq!(status.tokens_used, 1);

        drop(decision);
        let result = poll_scheduler(pin!(&mut scheduler), &cache);
        let Poll::Ready(Ok(decision)) = result else {
            panic!("expected decision");
        };
        assert_eq!(decision.total_items(), 1);
        assert_eq!(decision.num_queues(), 1);

        assert_eq!(
            scheduler.get_status(&qid1, cache.view()).unwrap().status,
            SchedulingStatus::Empty
        );
        assert_eq!(
            scheduler.get_status(&qid2, cache.view()).unwrap().status,
            SchedulingStatus::Empty
        );
        assert!(matches!(
            poll_scheduler(pin!(&mut scheduler), &cache),
            Poll::Pending
        ));
    }
}
