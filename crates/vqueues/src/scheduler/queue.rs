// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hashbrown::HashSet;
use tracing::error;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{VQueueCursor, VQueueEntry, VQueueStore};
use restate_types::vqueue::VQueueId;

#[derive(Debug)]
enum Head<Item> {
    /// We need a seek+read to know the head.
    Unknown,
    /// The current cursor's head
    Known(Item),
    /// We know that we've reached the end of the vqueue
    Empty,
}

#[derive(Debug)]
pub enum QueueItem<Item> {
    Inbox(Item),
    Running(Item),
    None,
}

#[derive(derive_more::Debug)]
pub(crate) enum Reader<S: VQueueStore> {
    /// Reader was never opened and might need to scan running items
    New { already_running: u32 },
    #[debug("Running")]
    Running {
        remaining: u32,
        reader: S::RunningReader,
    },
    #[debug("Inbox")]
    Inbox(S::InboxReader),
    // We can transition back to Reader::Inbox if new items have been added to the inbox
    // but we should never return to `Running`.
    #[debug("Closed")]
    Closed,
}

pub(crate) struct Queue<S: VQueueStore> {
    head: Head<S::Item>,
    reader: Reader<S>,
}

impl<S: VQueueStore> Queue<S> {
    /// Creates a new queue that must first go through the given number of running items
    /// before it switches to reading the waiting inbox.
    pub fn new(num_running: u32) -> Self {
        Self {
            head: Head::Unknown,
            reader: Reader::New {
                already_running: num_running,
            },
        }
    }

    /// Creates an empty queue
    pub fn new_closed() -> Self {
        Self {
            head: Head::Empty,
            reader: Reader::Closed,
        }
    }

    /// If the queue is known to be empty (no more items to dequeue)
    pub fn is_empty(&self) -> bool {
        matches!(self.head, Head::Empty)
    }

    pub fn remove(&mut self, item_hash: u64) -> bool {
        // Can this be the known head?
        // Yes. Perhaps it expired/ended externally.
        // We do not do anything if the reader is still at the running stage,
        //
        // This means that the scheduler might still yield the "running" item after
        // the state machine has declared it as completed/removed. The state machine
        // must be able to handle this case and "ignore" the yield command of this item.
        if matches!(self.reader, Reader::Closed | Reader::Inbox(..))
            && let Head::Known(ref item) = self.head
            && item.unique_hash() == item_hash
        {
            self.head = Head::Unknown;
            // Ensure that next advance would re-seek to the newly added item
            self.reader = Reader::Closed;
            true
        } else {
            false
        }
    }

    /// Returns true if the head was changed
    pub fn enqueue(&mut self, item: &S::Item) -> bool {
        match (&self.head, &self.reader) {
            // we are only unknown if we are new and didn't read the running list yet,
            // we might also be in a limbo state if advance() failed.
            (_, Reader::New { .. } | Reader::Running { .. }) => { /* do nothing */ }
            (Head::Unknown, _) => { /* do nothing */ }
            (Head::Empty, _) => {
                self.reader = Reader::Closed;
                self.head = Head::Known(item.clone());
                return true;
            }
            (Head::Known(current), Reader::Inbox(_) | Reader::Closed) => {
                if item < current {
                    self.head = Head::Known(item.clone());
                    // Ensure that next advance would re-seek to the newly added item
                    self.reader = Reader::Closed;
                    return true;
                }
            }
        }
        false
    }

    /// Returns the head if known, or None if the queue needs advancing
    pub fn head(&self) -> Option<QueueItem<&S::Item>> {
        match (&self.head, &self.reader) {
            (Head::Unknown, _) => None,
            (_, Reader::New { .. }) => None,
            (Head::Known(item), Reader::Running { .. }) => Some(QueueItem::Running(item)),
            (Head::Known(item), Reader::Inbox(_) | Reader::Closed) => Some(QueueItem::Inbox(item)),
            (Head::Empty, _) => Some(QueueItem::None),
        }
    }

    pub fn advance_if_needed(
        &mut self,
        storage: &S,
        skip: &HashSet<u64>,
        qid: &VQueueId,
    ) -> Result<QueueItem<&S::Item>, StorageError> {
        // Keep advancing until the head is known
        while matches!(self.head, Head::Unknown) {
            self.advance(storage, skip, qid)?;
        }

        match (&self.head, &self.reader) {
            (Head::Unknown, _) => unreachable!("head must be known"),
            (_, Reader::New { .. }) => unreachable!("reader cannot be new after poll"),
            (Head::Known(item), Reader::Running { .. }) => Ok(QueueItem::Running(item)),
            (Head::Known(item), Reader::Inbox(_) | Reader::Closed) => Ok(QueueItem::Inbox(item)),
            (Head::Empty, _) => Ok(QueueItem::None),
        }
    }

    pub fn advance(
        &mut self,
        storage: &S,
        skip: &HashSet<u64>,
        qid: &VQueueId,
    ) -> Result<(), StorageError> {
        loop {
            match self.reader {
                Reader::New { already_running } if already_running > 0 => {
                    let mut reader = storage.new_run_reader(qid);
                    reader.seek_to_first();
                    let item = reader.peek()?;
                    if let Some(item) = item {
                        self.head = Head::Known(item);
                        self.reader = Reader::Running {
                            remaining: already_running,
                            reader,
                        };
                        break;
                    } else {
                        error!(
                            "vqueue {:?} has no running items but its metadata says that it has {already_running} running items",
                            qid
                        );
                        debug_assert!(already_running > 0);
                        // move to inbox reading
                        self.head = Head::Unknown;
                        self.reader = Reader::Closed;
                    }
                }
                Reader::New { .. } => {
                    // create new inbox reader
                    self.reader = Reader::Closed;
                }
                Reader::Running {
                    ref mut reader,
                    ref mut remaining,
                } => {
                    reader.advance();
                    *remaining = remaining.saturating_sub(1);
                    let item = reader.peek()?;
                    if let Some(item) = item {
                        debug_assert!(*remaining > 0);
                        self.head = Head::Known(item);
                        break;
                    } else {
                        debug_assert_eq!(0, *remaining);
                        // move to inbox reading
                        self.head = Head::Unknown;
                        self.reader = Reader::Closed;
                    }
                }
                Reader::Inbox(ref mut reader) => {
                    reader.advance();
                    let item = reader.peek()?;
                    if let Some(item) = item {
                        if skip.contains(&item.unique_hash()) {
                            continue;
                        }
                        self.head = Head::Known(item);
                        break;
                    } else {
                        // we are done reading inbox
                        self.head = Head::Empty;
                        self.reader = Reader::Closed;
                        break;
                    }
                }
                Reader::Closed => {
                    match self.head {
                        Head::Unknown => {
                            let mut reader = storage.new_inbox_reader(qid);
                            reader.seek_to_first();
                            let item = reader.peek()?;
                            self.reader = Reader::Inbox(reader);
                            if let Some(item) = item {
                                if skip.contains(&item.unique_hash()) {
                                    continue;
                                }
                                self.head = Head::Known(item);
                                break;
                            } else {
                                self.head = Head::Empty;
                                self.reader = Reader::Closed;
                            }
                        }
                        Head::Known(ref item) => {
                            // seek to known head first, then advance.
                            let mut reader = storage.new_inbox_reader(qid);
                            reader.seek_after(qid, item);
                            let item = reader.peek()?;
                            self.reader = Reader::Inbox(reader);
                            if let Some(item) = item {
                                if skip.contains(&item.unique_hash()) {
                                    continue;
                                }
                                self.head = Head::Known(item);
                                break;
                            } else {
                                self.head = Head::Empty;
                                self.reader = Reader::Closed;
                            }
                        }
                        Head::Empty => {
                            // do nothing.
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::RangeInclusive;

    use restate_core::{TaskCenter, TestCoreEnv};
    use restate_partition_store::{PartitionDb, PartitionStore, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::{
        EntryCard, EntryId, EntryKind, Stage, VisibleAt, WriteVQueueTable,
    };
    use restate_types::clock::UniqueTimestamp;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;
    use restate_types::vqueue::{EffectivePriority, VQueueId, VQueueInstance, VQueueParent};

    /// Helper to create an EntryCard for testing.
    fn entry_card(id: u8) -> EntryCard {
        entry_card_with_priority(id, EffectivePriority::UserDefault)
    }

    /// Helper to create an EntryCard with a specific priority.
    fn entry_card_with_priority(id: u8, priority: EffectivePriority) -> EntryCard {
        EntryCard {
            priority,
            visible_at: VisibleAt::Now,
            created_at: UniqueTimestamp::try_from(1000u64 + id as u64).unwrap(),
            kind: EntryKind::Invocation,
            id: EntryId::new([id; 16]),
        }
    }

    /// Helper to create a test VQueueId with a unique partition key for test isolation.
    fn test_qid(partition_key: u64) -> VQueueId {
        VQueueId {
            partition_key: PartitionKey::from(partition_key),
            parent: VQueueParent::from_raw(1),
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

    /// Helper to insert entries into the inbox or run stage.
    async fn insert_entries(
        rocksdb: &mut PartitionStore,
        qid: &VQueueId,
        stage: Stage,
        entries: &[EntryCard],
    ) {
        let mut txn = rocksdb.transaction();
        for entry in entries {
            txn.put_inbox_entry(qid, stage, entry);
        }
        txn.commit().await.expect("commit should succeed");
    }

    /// Tests the core state machine: Running -> Inbox -> Empty transitions
    #[restate_core::test]
    async fn test_queue_running_to_inbox_to_empty() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(1000);
        let running_card = entry_card(1);
        let inbox_card = entry_card(2);

        // Insert running entry
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Run,
            std::slice::from_ref(&running_card),
        )
        .await;
        // Insert inbox entry
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&inbox_card),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(1);
        let mut skip = HashSet::new();

        // not empty, because we need to poll the head
        assert!(!queue.is_empty());

        // Phase 1: Running items come first
        let head = queue.advance_if_needed(db, &skip, &qid).unwrap();
        assert!(matches!(head, QueueItem::Running(item) if *item == running_card));
        let QueueItem::Running(head) = head else {
            panic!("expected QueueItem::Running");
        };
        skip.insert(head.unique_hash());

        assert!(matches!(queue.head(), Some(QueueItem::Running(item)) if *item == running_card));
        let Some(QueueItem::Running(head)) = queue.head() else {
            panic!("expected Some(QueueItem::Running)");
        };
        skip.insert(head.unique_hash());

        // Phase 2: Transitions to inbox
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox(item)) if *item == inbox_card));
        let Some(QueueItem::Inbox(head)) = queue.head() else {
            panic!("expected inbox head");
        };
        skip.insert(head.unique_hash());
        assert!(!queue.is_empty());

        // Phase 3: Empty
        queue.advance(db, &skip, &qid).unwrap();
        assert!(queue.is_empty());

        // Enqueueing an item brings the queue back to life.
        let higher = entry_card_with_priority(0, EffectivePriority::UserDefault);
        assert!(queue.enqueue(&higher));
        let head = queue.advance_if_needed(db, &skip, &qid).unwrap();
        assert!(matches!(head, QueueItem::Inbox(item) if *item == higher));
    }

    /// Tests that items are dequeued in priority order (smaller priority value = higher priority)
    #[restate_core::test]
    async fn test_priority_ordering() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(2000);
        let low_prio = entry_card_with_priority(1, EffectivePriority::UserDefault);
        let high_prio = entry_card_with_priority(2, EffectivePriority::System);
        let highest_prio = entry_card_with_priority(3, EffectivePriority::TokenHeld);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[low_prio.clone(), high_prio.clone(), highest_prio.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = HashSet::new();

        // Should dequeue in priority order: TokenHeld -> System -> UserDefault
        queue.advance(db, &skip, &qid).unwrap();
        assert!(
            matches!(queue.head(), Some(QueueItem::Inbox(item)) if item.priority == EffectivePriority::TokenHeld)
        );

        queue.advance(db, &skip, &qid).unwrap();
        assert!(
            matches!(queue.head(), Some(QueueItem::Inbox(item)) if item.priority == EffectivePriority::System)
        );

        queue.advance(db, &skip, &qid).unwrap();
        assert!(
            matches!(queue.head(), Some(QueueItem::Inbox(item)) if item.priority == EffectivePriority::UserDefault)
        );
    }

    /// Tests that enqueue updates the head when a higher-priority item arrives
    #[restate_core::test]
    async fn test_enqueue_replaces_head_on_higher_priority() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(3000);
        let initial = entry_card_with_priority(1, EffectivePriority::UserDefault);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&initial),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = HashSet::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox(item)) if *item == initial));

        // Enqueue higher priority - should replace head
        let higher = entry_card_with_priority(2, EffectivePriority::System);
        assert!(queue.enqueue(&higher));
        assert!(matches!(queue.head(), Some(QueueItem::Inbox(item)) if *item == higher));
        // also if we advance_if_needed, the head would remain the same
        assert!(matches!(queue.advance_if_needed(db, &skip, &qid).unwrap(),
                QueueItem::Inbox(item) if *item == higher));

        // Enqueue lower priority - should NOT replace head
        let lower = entry_card_with_priority(3, EffectivePriority::UserDefault);
        assert!(!queue.enqueue(&lower));
        assert!(matches!(queue.head(), Some(QueueItem::Inbox(item)) if *item == higher));
    }

    /// Tests that remove() returns true only for current head
    #[restate_core::test]
    async fn test_remove() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(4000);
        let card = entry_card(1);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&card),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let mut skip = HashSet::new();

        queue.advance(db, &skip, &qid).unwrap();
        let hash = match queue.head() {
            Some(QueueItem::Inbox(item)) => item.unique_hash(),
            _ => panic!("Expected head"),
        };

        assert!(!queue.remove(999)); // Non-matching hash returns false
        assert!(queue.remove(hash)); // Matching hash returns true
        assert!(queue.head().is_none()); // Head becomes unknown after remove
        // it would still appear if we reseek because it's not in the skip set.
        assert!(matches!(queue.advance_if_needed(db, &skip, &qid).unwrap(),
                QueueItem::Inbox(item) if *item == card));
        // let's try to remove it again, but this time we'd add it to the skip set.
        skip.insert(card.unique_hash());
        assert!(queue.remove(hash)); // Matching hash returns true
        assert!(queue.head().is_none()); // Head becomes unknown after remove
        // we land on None because the queue is _logically_ empty.
        assert!(matches!(
            queue.advance_if_needed(db, &skip, &qid).unwrap(),
            QueueItem::None
        ));
    }

    /// Tests that skip set causes items to be skipped during advance
    #[restate_core::test]
    async fn test_skip_set() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(5000);
        let card1 = entry_card(1);
        let card2 = entry_card(2);
        let card3 = entry_card(3);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[card1.clone(), card2.clone(), card3.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let mut skip = HashSet::new();
        skip.insert(card1.unique_hash());
        skip.insert(card2.unique_hash());

        queue.advance(db, &skip, &qid).unwrap();
        // Should skip card1 and card2, land on card3 even though they are still in partition-db.
        assert!(matches!(queue.head(), Some(QueueItem::Inbox(item)) if *item == card3));
    }

    /// Tests that running items are always processed before inbox, regardless of priority
    #[restate_core::test]
    async fn test_running_before_inbox_regardless_of_priority() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(6000);
        let running_low_prio = entry_card_with_priority(1, EffectivePriority::UserDefault);
        let inbox_high_prio = entry_card_with_priority(2, EffectivePriority::TokenHeld);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Run,
            std::slice::from_ref(&running_low_prio),
        )
        .await;
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&inbox_high_prio),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(1);
        let skip = HashSet::new();

        // Running comes first even though inbox has higher priority
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Running(_))));

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox(_))));
    }

    /// Tests that enqueue and remove are no-ops while the reader is in the Running stage.
    ///
    /// When processing running items:
    /// - enqueue operations (which add to inbox) should not affect the current head
    /// - remove is a no-op, even for the current head, because the scheduler must
    ///   continue processing all running items before moving to the inbox
    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_enqueue_and_remove_ignored_during_running_stage() {
        let _env = TestCoreEnv::create_with_single_node(1, 1).await;
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(7000);
        let running1 = entry_card_with_priority(1, EffectivePriority::UserDefault);
        let running2 = entry_card_with_priority(2, EffectivePriority::UserDefault);
        let high_prio_inbox = entry_card_with_priority(10, EffectivePriority::TokenHeld);

        // Insert two running entries and one inbox entry upfront
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Run,
            &[running1.clone(), running2.clone()],
        )
        .await;
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&high_prio_inbox),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(2);
        let skip = HashSet::new();

        // Advance to get the first running item
        queue.advance(db, &skip, &qid).unwrap();
        let head = queue.head();
        assert!(matches!(head, Some(QueueItem::Running(item)) if *item == running1));

        // Try to enqueue a higher-priority inbox item - should have NO effect on head
        // because we're still in the Running stage
        let even_higher_prio = entry_card_with_priority(11, EffectivePriority::TokenHeld);
        let enqueue_result = queue.enqueue(&even_higher_prio);
        assert!(
            !enqueue_result,
            "enqueue should return false during Running stage"
        );
        // Head should still be the same running item
        assert!(matches!(queue.head(), Some(QueueItem::Running(item)) if *item == running1));

        // Try to remove a non-head item - should return false (no-op)
        let remove_result = queue.remove(running2.unique_hash());
        assert!(
            !remove_result,
            "remove of non-head item should return false"
        );
        // Head should still be the same
        assert!(matches!(queue.head(), Some(QueueItem::Running(item)) if *item == running1));

        // Try to remove the current head - should ALSO return false during Running stage
        // because remove is a no-op while processing running items
        let remove_result = queue.remove(running1.unique_hash());
        assert!(
            !remove_result,
            "remove of head should return false during Running stage"
        );
        // Head should still be the same running item
        assert!(matches!(queue.head(), Some(QueueItem::Running(item)) if *item == running1));

        // Advance to the second running item
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Running(item)) if *item == running2));

        // Advance again - should transition to inbox stage
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox(item)) if *item == high_prio_inbox));

        // Now that we're in the Inbox stage, remove SHOULD work
        let remove_result = queue.remove(high_prio_inbox.unique_hash());
        assert!(
            remove_result,
            "remove of head should return true during Inbox stage"
        );
        assert!(
            queue.head().is_none(),
            "head should be None (Unknown) after removing current head in Inbox stage"
        );
    }
}
