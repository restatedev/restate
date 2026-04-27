// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, VQueueCursor, VQueueStore};
use restate_types::vqueues::VQueueId;

use super::UnconfirmedAssignments;

#[derive(Debug)]
enum Head {
    /// We need a seek+read to know the head.
    Unknown,
    /// The current cursor's head
    Known { key: EntryKey, value: EntryValue },
    /// We know that we've reached the end of the vqueue
    Empty,
}

#[derive(Debug)]
pub enum QueueItem<'a> {
    Inbox {
        key: &'a EntryKey,
        value: &'a EntryValue,
    },
    Running {
        key: &'a EntryKey,
        value: &'a EntryValue,
    },
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
    head: Head,
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

    pub fn remove(&mut self, key_to_remove: &EntryKey) -> bool {
        // Can this be the known head?
        // Yes. Perhaps it expired/ended externally.
        // We do not do anything if the reader is still at the running stage,
        //
        // This means that the scheduler might still yield the "running" item after
        // the state machine has declared it as completed/removed. The state machine
        // must be able to handle this case and "ignore" the yield command of this item.
        if matches!(self.reader, Reader::Closed | Reader::Inbox(..))
            && let Head::Known { ref key, .. } = self.head
            && key == key_to_remove
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
    pub fn enqueue(&mut self, key: &EntryKey, value: &EntryValue) -> bool {
        match (&self.head, &self.reader) {
            // we are only unknown if we are new and didn't read the running list yet,
            // we might also be in a limbo state if advance() failed.
            (_, Reader::New { .. } | Reader::Running { .. }) => { /* do nothing */ }
            (Head::Unknown, _) => { /* do nothing */ }
            (Head::Empty, _) => {
                self.reader = Reader::Closed;
                self.head = Head::Known {
                    key: *key,
                    value: value.clone(),
                };
                return true;
            }
            (
                Head::Known {
                    key: current_key, ..
                },
                Reader::Inbox(_) | Reader::Closed,
            ) => {
                if key < current_key {
                    self.head = Head::Known {
                        key: *key,
                        value: value.clone(),
                    };
                    // Ensure that next advance would re-seek to the newly added item
                    self.reader = Reader::Closed;
                    return true;
                } else {
                    // This is a temporary fix to ensure that we perform a re-seek
                    // to fix the issue where the iterator wouldn't see the newly added
                    // items if the memtable was flushed prior the seek.
                    self.reader = Reader::Closed;
                }
            }
        }
        false
    }

    /// Returns the head if known, or None if the queue needs advancing
    pub fn head(&self) -> Option<QueueItem<'_>> {
        match (&self.head, &self.reader) {
            (Head::Unknown, _) => None,
            (_, Reader::New { .. }) => None,
            (Head::Known { key, value }, Reader::Running { .. }) => {
                Some(QueueItem::Running { key, value })
            }
            (Head::Known { key, value }, Reader::Inbox(_) | Reader::Closed) => {
                Some(QueueItem::Inbox { key, value })
            }
            (Head::Empty, _) => Some(QueueItem::None),
        }
    }

    pub fn advance_if_needed(
        &mut self,
        storage: &S,
        skip: &UnconfirmedAssignments,
        qid: &VQueueId,
    ) -> Result<QueueItem<'_>, StorageError> {
        // Keep advancing until the head is known
        while matches!(self.head, Head::Unknown) {
            self.advance(storage, skip, qid)?;
        }

        match (&self.head, &self.reader) {
            (Head::Unknown, _) => unreachable!("head must be known"),
            (_, Reader::New { .. }) => unreachable!("reader cannot be new after poll"),
            (Head::Known { key, value }, Reader::Running { .. }) => {
                Ok(QueueItem::Running { key, value })
            }
            (Head::Known { key, value }, Reader::Inbox(_) | Reader::Closed) => {
                Ok(QueueItem::Inbox { key, value })
            }
            (Head::Empty, _) => Ok(QueueItem::None),
        }
    }

    /// Advances the queue to the next item.
    ///
    /// The queue reader will skip over items in `skip` when reading the inbox stage. When reading
    /// the running stage, the `skip` set is ignored.
    pub fn advance(
        &mut self,
        storage: &S,
        skip: &UnconfirmedAssignments,
        qid: &VQueueId,
    ) -> Result<(), StorageError> {
        loop {
            match self.reader {
                Reader::New { already_running } if already_running > 0 => {
                    let mut reader = storage.new_run_reader(qid);
                    reader.seek_to_first();
                    let item = reader.peek()?;
                    if let Some((key, value)) = item {
                        self.head = Head::Known { key, value };
                        self.reader = Reader::Running {
                            remaining: already_running,
                            reader,
                        };
                        break;
                    } else {
                        assert!(
                            already_running > 0,
                            "vqueue {qid:?} has no running items but its metadata says that it has {already_running} running items",
                        );
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
                    if let Some((key, value)) = item {
                        debug_assert!(*remaining > 0);
                        self.head = Head::Known { key, value };
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
                    let key = reader.current_key()?;
                    if let Some(key) = key {
                        if skip.contains_key(&key) {
                            continue;
                        }
                        self.head = Head::Known {
                            key,
                            value: reader.current_value()?.unwrap(),
                        };
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
                            let key = reader.current_key()?;
                            if let Some(key) = key {
                                if skip.contains_key(&key) {
                                    self.reader = Reader::Inbox(reader);
                                    continue;
                                }
                                self.head = Head::Known {
                                    key,
                                    value: reader.current_value()?.unwrap(),
                                };
                                self.reader = Reader::Inbox(reader);
                                break;
                            } else {
                                self.head = Head::Empty;
                                self.reader = Reader::Closed;
                            }
                        }
                        Head::Known { ref key, .. } => {
                            // seek to known head first, then advance.
                            let mut reader = storage.new_inbox_reader(qid);
                            reader.seek_after(qid, key);
                            let next_key = reader.current_key()?;
                            if let Some(next_key) = next_key {
                                if skip.contains_key(&next_key) {
                                    self.reader = Reader::Inbox(reader);
                                    continue;
                                }
                                self.head = Head::Known {
                                    key: next_key,
                                    value: reader.current_value()?.unwrap(),
                                };
                                self.reader = Reader::Inbox(reader);
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

    pub(crate) fn remaining_in_running_stage(&self) -> u32 {
        match self.reader {
            Reader::New { already_running } => already_running,
            Reader::Running { remaining, .. } => remaining,
            Reader::Inbox(..) => 0,
            Reader::Closed => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_clock::RoughTimestamp;
    use restate_clock::time::MillisSinceEpoch;
    use restate_core::TaskCenter;
    use restate_partition_store::{PartitionDb, PartitionStore, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::{
        EntryId, EntryKey, EntryKind, EntryMetadata, EntryValue, Stage, Status, WriteVQueueTable,
        stats::EntryStatistics,
    };
    use restate_types::clock::UniqueTimestamp;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::VQueueId;

    const BASE_RUN_AT_MS: u64 = 1_744_000_000_000;

    fn test_entry_at(id: u8, has_lock: bool, run_at: MillisSinceEpoch) -> (EntryKey, EntryValue) {
        let run_at = RoughTimestamp::from_unix_millis_clamped(run_at);
        let created_at = UniqueTimestamp::try_from(1_000u64 + id as u64).unwrap();
        let entry_id = EntryId::new(EntryKind::Invocation, [id; EntryId::REMAINDER_LEN]);
        let key = EntryKey::new(has_lock, run_at, id as u64, entry_id);
        let stats = EntryStatistics::new(created_at, run_at);
        let value = EntryValue {
            status: if stats.first_runnable_at > created_at.to_unix_millis() {
                Status::Scheduled
            } else {
                Status::New
            },
            metadata: EntryMetadata::default(),
            stats,
        };

        (key, value)
    }

    fn test_entry(id: u8, has_lock: bool, run_at_ms: u64) -> (EntryKey, EntryValue) {
        test_entry_at(
            id,
            has_lock,
            MillisSinceEpoch::new(BASE_RUN_AT_MS + run_at_ms),
        )
    }

    fn default_entry(id: u8) -> (EntryKey, EntryValue) {
        test_entry(id, false, 0)
    }

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

    async fn insert_entries(
        rocksdb: &mut PartitionStore,
        qid: &VQueueId,
        stage: Stage,
        entries: &[(EntryKey, EntryValue)],
    ) {
        let mut txn = rocksdb.transaction();
        for (key, value) in entries {
            txn.put_vqueue_inbox(qid, stage, key, value);
        }
        txn.commit().await.expect("commit should succeed");
    }

    #[restate_core::test]
    async fn test_queue_running_to_inbox_to_empty() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(1000);
        let running_entry = default_entry(1);
        let inbox_entry = default_entry(2);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Running,
            std::slice::from_ref(&running_entry),
        )
        .await;
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&inbox_entry),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(1);
        let mut skip = UnconfirmedAssignments::new();

        assert!(!queue.is_empty());

        let head = queue.advance_if_needed(db, &skip, &qid).unwrap();
        assert!(matches!(head, QueueItem::Running { key, .. } if *key == running_entry.0));
        assert!(
            matches!(queue.head(), Some(QueueItem::Running { key, .. }) if *key == running_entry.0)
        );

        queue.advance(db, &skip, &qid).unwrap();
        assert!(
            matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == inbox_entry.0)
        );
        let Some(QueueItem::Inbox { key, .. }) = queue.head() else {
            panic!("expected inbox head");
        };
        skip.insert(*key, Default::default());
        assert!(!queue.is_empty());

        queue.advance(db, &skip, &qid).unwrap();
        assert!(queue.is_empty());

        let higher = default_entry(0);
        assert!(queue.enqueue(&higher.0, &higher.1));
        let head = queue.advance_if_needed(db, &skip, &qid).unwrap();
        assert!(matches!(head, QueueItem::Inbox { key, .. } if *key == higher.0));
    }

    #[restate_core::test]
    async fn test_entry_key_ordering() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(2000);
        let low = test_entry(1, false, 3_000);
        let high = test_entry(2, false, 2_000);
        let highest = test_entry(3, true, 9_000);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[low.clone(), high.clone(), highest.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == highest.0));

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == high.0));

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == low.0));
    }

    #[restate_core::test]
    async fn test_run_at_below_now_bumps_entry_higher_in_inbox() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(2_500);
        let now = MillisSinceEpoch::now().as_u64();
        let future_entry =
            test_entry_at(1, false, MillisSinceEpoch::new(now.saturating_add(60_000)));
        let overdue_entry =
            test_entry_at(2, false, MillisSinceEpoch::new(now.saturating_sub(1_000)));

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[future_entry.clone(), overdue_entry.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(
            matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == overdue_entry.0)
        );

        queue.advance(db, &skip, &qid).unwrap();
        assert!(
            matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == future_entry.0)
        );
    }

    #[restate_core::test]
    async fn test_enqueue_replaces_head_on_smaller_key() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(3000);
        let initial = test_entry(1, false, 3_000);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&initial),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == initial.0));

        let higher = test_entry(2, false, 2_000);
        assert!(queue.enqueue(&higher.0, &higher.1));
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == higher.0));
        assert!(
            matches!(queue.advance_if_needed(db, &skip, &qid).unwrap(), QueueItem::Inbox { key, .. } if *key == higher.0)
        );

        let lower = test_entry(3, false, 4_000);
        assert!(!queue.enqueue(&lower.0, &lower.1));
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == higher.0));
    }

    #[restate_core::test]
    async fn test_remove() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(4000);
        let entry = default_entry(1);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&entry),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let mut skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        let head_key = match queue.head() {
            Some(QueueItem::Inbox { key, .. }) => *key,
            _ => panic!("expected inbox head"),
        };

        assert!(!queue.remove(&default_entry(99).0));
        assert!(queue.remove(&head_key));
        assert!(queue.head().is_none());
        assert!(
            matches!(queue.advance_if_needed(db, &skip, &qid).unwrap(), QueueItem::Inbox { key, .. } if *key == entry.0)
        );

        skip.insert(entry.0, Default::default());
        assert!(queue.remove(&head_key));
        assert!(queue.head().is_none());
        assert!(matches!(
            queue.advance_if_needed(db, &skip, &qid).unwrap(),
            QueueItem::None
        ));
    }

    #[restate_core::test]
    async fn test_skip_set() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(5000);
        let entry1 = default_entry(1);
        let entry2 = default_entry(2);
        let entry3 = default_entry(3);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[entry1.clone(), entry2.clone(), entry3.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let mut skip = UnconfirmedAssignments::new();
        skip.insert(entry1.0, Default::default());
        skip.insert(entry2.0, Default::default());

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == entry3.0));
    }

    #[restate_core::test]
    async fn test_running_before_inbox_regardless_of_key() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(6000);
        let running_entry = default_entry(1);
        let inbox_entry = test_entry(2, true, 0);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Running,
            std::slice::from_ref(&running_entry),
        )
        .await;
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&inbox_entry),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(1);
        let skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Running { .. })));

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { .. })));
    }

    #[restate_core::test]
    async fn test_enqueue_and_remove_ignored_during_running_stage() {
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(7000);
        let running1 = default_entry(1);
        let running2 = default_entry(2);
        let inbox_entry = test_entry(10, true, 0);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Running,
            &[running1.clone(), running2.clone()],
        )
        .await;
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&inbox_entry),
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(2);
        let skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Running { key, .. }) if *key == running1.0));

        let even_higher = test_entry(11, true, 0);
        assert!(!queue.enqueue(&even_higher.0, &even_higher.1));
        assert!(matches!(queue.head(), Some(QueueItem::Running { key, .. }) if *key == running1.0));

        assert!(!queue.remove(&running2.0));
        assert!(matches!(queue.head(), Some(QueueItem::Running { key, .. }) if *key == running1.0));

        assert!(!queue.remove(&running1.0));
        assert!(matches!(queue.head(), Some(QueueItem::Running { key, .. }) if *key == running1.0));

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Running { key, .. }) if *key == running2.0));

        queue.advance(db, &skip, &qid).unwrap();
        assert!(
            matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == inbox_entry.0)
        );

        assert!(queue.remove(&inbox_entry.0));
        assert!(queue.head().is_none());
    }
}
