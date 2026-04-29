// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::{EntryKey, EntryValue, VQueueCursor, VQueueStore};
use restate_types::vqueues::VQueueId;

use super::UnconfirmedAssignments;

/// The number of entries we are willing to keep in cache
const INBOX_CACHE_CAPACITY: usize = 24;

#[derive(Debug)]
struct InboxCache {
    /// Sorted ascending by EntryKey. Front = current head.
    items: VecDeque<(EntryKey, EntryValue)>,
    /// `seek_after` anchor for the next refill, and the upper bound used to
    /// decide whether to accept a `notify_enqueued`. Items currently in the
    /// cache, items previously consumed from the cache, and items in the
    /// caller's skip set together account for every inbox key
    /// `<= refill_anchor`. Keys `> refill_anchor` are undiscovered and will
    /// appear on the next refill via `seek_after(refill_anchor)`.
    refill_anchor: Option<EntryKey>,
}

impl Default for InboxCache {
    fn default() -> Self {
        Self {
            items: VecDeque::with_capacity(INBOX_CACHE_CAPACITY),
            refill_anchor: None,
        }
    }
}

impl InboxCache {
    /// Inserts `(key, value)` into the sorted cache.
    ///
    /// Returns `true` iff the item became the new front (head). Returns
    /// `false` if the item was rejected because its key falls strictly above
    /// the cache's coverage zone (`> refill_anchor`); in that case the item
    /// will be discovered on the next refill.
    fn enqueue(&mut self, key: &EntryKey, value: &EntryValue) -> bool {
        // Priority-queue rule: ignore items strictly above our coverage zone.
        // The bound is `refill_anchor`, NOT `cache.back`. After consuming the
        // back of the cache, `cache.back` can be lower than `refill_anchor`,
        // and items in `(cache.back, refill_anchor]` would be lost (the next
        // refill seek_after(refill_anchor) only returns keys strictly greater
        // than the anchor).
        if let Some(ref anchor) = self.refill_anchor
            && key > anchor
        {
            return false;
        }
        let pos = match self.items.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(_) => {
                debug_assert!(false, "duplicate enqueue for key {key:?}");
                return false;
            }
            Err(pos) => pos,
        };

        // At-cap fast path: avoid the VecDeque grow/shrink that would happen
        // if we inserted first and evicted afterwards.
        if self.items.len() >= INBOX_CACHE_CAPACITY {
            if pos == self.items.len() {
                // The new key would land at the back and be evicted on the
                // very next step (matches the post-insert `pop_back` from
                // the original implementation). Skip the insert, but still
                // lower the anchor to the current back so the next refill
                // can re-discover `key` via `seek_after(anchor)`.
                self.refill_anchor = self.items.back().map(|(k, _)| *k);
                return false;
            }
            // Make room first; `pos` is unaffected because pos < old_len in
            // this branch (we shift elements right of `pos` regardless).
            debug_assert!(pos < self.items.len());
            self.items.pop_back();
            self.items.insert(pos, (*key, value.clone()));
            self.refill_anchor = self.items.back().map(|(k, _)| *k);
            return pos == 0;
        }

        // Normal path: cache below capacity.
        self.items.insert(pos, (*key, value.clone()));
        // Anchor maintenance: if it was None (very first insert), set it to
        // this key. Otherwise the precondition above guarantees
        // `key <= anchor`, so the anchor stays put.
        if self.refill_anchor.is_none() {
            self.refill_anchor = Some(*key);
        }
        pos == 0
    }

    /// Returns `true` iff the removed key was the current front (head).
    fn remove(&mut self, key: &EntryKey) -> bool {
        let Ok(pos) = self.items.binary_search_by(|(k, _)| k.cmp(key)) else {
            return false;
        };
        self.items.remove(pos);
        // The anchor stays put — even if we removed the back, the entry was
        // also removed from storage, so `seek_after(anchor)` will skip it
        // naturally on the next refill.
        pos == 0
    }

    fn front(&self) -> Option<&(EntryKey, EntryValue)> {
        self.items.front()
    }

    fn pop_front(&mut self) {
        self.items.pop_front();
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
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
enum Stage<S: VQueueStore> {
    /// Brand-new queue; running items still need to be drained first.
    New { already_running: u32 },
    /// In running stage. Single-item head, single-shot reader.
    #[debug("Running")]
    Running {
        head: (EntryKey, EntryValue),
        reader: S::RunningReader,
        remaining: u32,
    },
    /// In inbox stage. The queue's `inbox_cache` is the source of truth
    /// between refills.
    Inbox,
    /// Inbox is fully drained.
    Empty,
}

pub(crate) struct Queue<S: VQueueStore> {
    stage: Stage<S>,
    /// Backing cache for the inbox stage.
    /// Meaningful only when `stage` is `Inbox` or `Empty`; for
    /// other stages it must be empty (invariant maintained by `advance` /
    /// `remove` / `enqueue`).
    inbox_cache: InboxCache,
}

impl<S: VQueueStore> Queue<S> {
    /// Creates a new queue that must first go through the given number of running items
    /// before it switches to reading the waiting inbox.
    pub fn new(num_running: u32) -> Self {
        Self {
            stage: Stage::New {
                already_running: num_running,
            },
            inbox_cache: InboxCache::default(),
        }
    }

    /// Creates an empty queue
    pub fn new_closed() -> Self {
        Self {
            stage: Stage::Empty,
            inbox_cache: InboxCache::default(),
        }
    }

    /// If the queue is known to be empty (no more items to dequeue)
    pub fn is_empty(&self) -> bool {
        matches!(self.stage, Stage::Empty)
    }

    /// Returns `true` iff the removed key was the head of the queue.
    ///
    /// While the queue is still in the running stage, this is a no-op: the
    /// scheduler may still yield a running item after the state machine has
    /// declared it removed, and the state machine must ignore that yield.
    pub fn remove(&mut self, key_to_remove: &EntryKey) -> bool {
        match self.stage {
            Stage::New { .. } | Stage::Running { .. } | Stage::Empty => false,
            Stage::Inbox => self.inbox_cache.remove(key_to_remove),
        }
    }

    /// Returns `true` iff the new item became the head of the queue.
    pub fn enqueue(&mut self, key: &EntryKey, value: &EntryValue) -> bool {
        match self.stage {
            Stage::New { .. } | Stage::Running { .. } => false,
            Stage::Empty => {
                // The cache is already allocated and empty (kept around
                // across the previous `Inbox -> Empty` transition). Just
                // re-seed it and flip the marker.
                debug_assert!(self.inbox_cache.items.is_empty());
                self.inbox_cache.items.push_back((*key, value.clone()));
                self.inbox_cache.refill_anchor = Some(*key);
                self.stage = Stage::Inbox;
                true
            }
            Stage::Inbox => self.inbox_cache.enqueue(key, value),
        }
    }

    /// Returns the head if known, or `None` if the queue needs advancing.
    pub fn head(&self) -> Option<QueueItem<'_>> {
        match &self.stage {
            Stage::New { .. } => None,
            Stage::Running { head: (k, v), .. } => Some(QueueItem::Running { key: k, value: v }),
            Stage::Inbox => self
                .inbox_cache
                .front()
                .map(|(k, v)| QueueItem::Inbox { key: k, value: v }),
            Stage::Empty => Some(QueueItem::None),
        }
    }

    pub fn advance_if_needed(
        &mut self,
        storage: &S,
        skip: &UnconfirmedAssignments,
        qid: &VQueueId,
    ) -> Result<QueueItem<'_>, StorageError> {
        loop {
            let needs_advance = match self.stage {
                Stage::New { .. } => true,
                Stage::Inbox => self.inbox_cache.is_empty(),
                _ => false,
            };
            if !needs_advance {
                break;
            }
            self.advance(storage, skip, qid)?;
        }

        Ok(match &self.stage {
            Stage::New { .. } => unreachable!("head must be resolved after advance_if_needed"),
            Stage::Running { head: (k, v), .. } => QueueItem::Running { key: k, value: v },
            Stage::Inbox => {
                let (k, v) = self
                    .inbox_cache
                    .front()
                    .expect("inbox cache must have a head after advance_if_needed");
                QueueItem::Inbox { key: k, value: v }
            }
            Stage::Empty => QueueItem::None,
        })
    }

    /// Advances the queue to the next item.
    ///
    /// In the inbox stage this consumes the current head (if any) and exposes
    /// the next cached item; the cache is refilled from storage in batches of
    /// up to [`INBOX_CACHE_CAPACITY`] when it empties. The `skip` set is
    /// consulted only when reading the inbox stage; it is ignored when
    /// reading the running stage.
    pub fn advance(
        &mut self,
        storage: &S,
        skip: &UnconfirmedAssignments,
        qid: &VQueueId,
    ) -> Result<(), StorageError> {
        // Split into disjoint borrows so the `Stage::Inbox` arm below can
        // mutate both `stage` and `inbox_cache` without fighting the
        // borrow checker.
        let Self { stage, inbox_cache } = self;
        loop {
            match stage {
                Stage::New { already_running } if *already_running > 0 => {
                    let already_running = *already_running;
                    let mut reader = storage.new_run_reader(qid);
                    reader.seek_to_first();
                    if let Some((key, value)) = reader.peek()? {
                        *stage = Stage::Running {
                            head: (key, value),
                            reader,
                            remaining: already_running,
                        };
                        return Ok(());
                    }
                    debug_assert!(
                        false,
                        "vqueue {qid:?} has no running items but its metadata says it has {already_running}",
                    );
                    *stage = Stage::Inbox;
                }
                Stage::New { .. } => {
                    *stage = Stage::Inbox;
                }
                Stage::Running {
                    reader,
                    remaining,
                    head,
                } => {
                    reader.advance();
                    *remaining = remaining.saturating_sub(1);
                    match reader.peek()? {
                        Some(next) => {
                            debug_assert!(*remaining > 0);
                            *head = next;
                            return Ok(());
                        }
                        None => {
                            debug_assert_eq!(0, *remaining);
                            *stage = Stage::Inbox;
                        }
                    }
                }
                Stage::Inbox => {
                    inbox_cache.pop_front();
                    if inbox_cache.is_empty() && !refill_inbox(storage, qid, skip, inbox_cache)? {
                        *stage = Stage::Empty;
                    }
                    return Ok(());
                }
                Stage::Empty => return Ok(()),
            }
        }
    }

    pub(crate) fn remaining_in_running_stage(&self) -> u32 {
        match &self.stage {
            Stage::New { already_running } => *already_running,
            Stage::Running { remaining, .. } => *remaining,
            Stage::Inbox | Stage::Empty => 0,
        }
    }
}

/// Refills `cache` with up to [`INBOX_CACHE_CAPACITY`] items from storage,
/// starting at `cache.refill_anchor` (or the very first key if no anchor is
/// set yet). Returns true if items were added. Items in the `skip` set are
/// not added to the cache, but the anchor advances past them so they are not
/// reconsidered on the next refill.
fn refill_inbox<S: VQueueStore>(
    storage: &S,
    qid: &VQueueId,
    skip: &UnconfirmedAssignments,
    cache: &mut InboxCache,
) -> Result<bool, StorageError> {
    let mut reader = storage.new_inbox_reader(qid);
    match cache.refill_anchor.as_ref() {
        Some(anchor) => reader.seek_after(qid, anchor),
        None => reader.seek_to_first(),
    }
    let mut loaded = false;
    while cache.items.len() < INBOX_CACHE_CAPACITY {
        let Some(key) = reader.current_key()? else {
            break;
        };
        if !skip.contains_key(&key) {
            // we can safely unwrap the option here because we know that the key
            // exists.
            cache
                .items
                .push_back((key, reader.current_value()?.unwrap()));
            loaded = true;
        }
        cache.refill_anchor = Some(cache.refill_anchor.map_or(key, |a| a.max(key)));
        reader.advance();
    }
    Ok(loaded)
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
        // Verifies that `remove` of the front key drops it from the cache
        // and that the next item (also already in cache) becomes the head.
        // `remove` is paired with a storage delete in production
        // (`notify_removed`), so the cache must not re-fetch the removed
        // entry on subsequent advances.
        let mut rocksdb = storage_test_environment().await;

        let qid = test_qid(4000);
        let entry1 = default_entry(1);
        let entry2 = default_entry(2);

        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[entry1.clone(), entry2.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        // Load both entries into the cache via the first advance.
        queue.advance(db, &skip, &qid).unwrap();
        let head_key = match queue.head() {
            Some(QueueItem::Inbox { key, .. }) => *key,
            _ => panic!("expected inbox head"),
        };

        // Removing a key that is not in the cache is a no-op.
        assert!(!queue.remove(&default_entry(99).0));

        // Removing the front key returns true and exposes the next cached
        // item as the new head — no storage round-trip required.
        assert!(queue.remove(&head_key));
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == entry2.0));

        // Removing the new front empties the cache.
        assert!(queue.remove(&entry2.0));
        assert!(queue.head().is_none());

        // The next advance refills via `seek_after(refill_anchor)`. The
        // anchor sits at `entry2.0`, so storage has nothing further to
        // return and the queue transitions to Empty.
        assert!(matches!(
            queue.advance_if_needed(db, &skip, &qid).unwrap(),
            QueueItem::None
        ));
        assert!(queue.is_empty());
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

    #[restate_core::test]
    async fn test_inbox_refill_reads_in_batches() {
        // Stage > INBOX_CACHE_CAPACITY items in storage, then drive the queue to completion.
        // The cache should be loaded in two batches (INBOX_CACHE_CAPACITY then the remainder),
        // not item-by-item.
        const TOTAL: u8 = 100;
        let mut rocksdb = storage_test_environment().await;
        let qid = test_qid(8000);

        let entries: Vec<_> = (0..TOTAL).map(default_entry).collect();
        insert_entries(&mut rocksdb, &qid, Stage::Inbox, &entries).await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        // First advance loads the first INBOX_CACHE_CAPACITY items. The cache is sorted by
        // EntryKey; with `default_entry(id)` the ordering matches `id` (no
        // locks, identical run_at, distinct seq).
        queue.advance(db, &skip, &qid).unwrap();
        let mut produced = 0u32;
        while !queue.is_empty() {
            assert!(queue.head().is_some());
            queue.advance(db, &skip, &qid).unwrap();
            produced += 1;
        }
        assert_eq!(produced, TOTAL as u32);
    }

    #[restate_core::test]
    async fn test_enqueue_within_range_inserts_in_sorted_position() {
        // The cache is sorted; an enqueue with a key between front and back
        // must land at the right position and be served on subsequent
        // advances without any storage round-trip.
        let mut rocksdb = storage_test_environment().await;
        let qid = test_qid(8100);

        let low = default_entry(1);
        let high = default_entry(5);
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[low.clone(), high.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == low.0));

        // Enqueue a key that sorts between `low` and `high`.
        let middle = default_entry(3);
        assert!(!queue.enqueue(&middle.0, &middle.1));
        // Head unchanged.
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == low.0));

        // Advance through the cache: low, middle, high.
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == middle.0));
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == high.0));
    }

    #[restate_core::test]
    async fn test_enqueue_above_anchor_is_ignored_then_picked_up_on_refill() {
        // Items > refill_anchor are dropped at enqueue time. They must still
        // be discovered by the next refill via seek_after(anchor).
        let mut rocksdb = storage_test_environment().await;
        let qid = test_qid(8200);

        let entry1 = default_entry(1);
        let entry2 = default_entry(2);
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&entry1),
        )
        .await;

        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        // First advance loads entry1; the anchor is now `entry1.0`.
        {
            let db = rocksdb.partition_db();
            queue.advance(db, &skip, &qid).unwrap();
            assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == entry1.0));
            // entry2 sorts after entry1 (lower priority) and is therefore
            // above the current anchor. notify_enqueued must reject it.
            assert!(!queue.enqueue(&entry2.0, &entry2.1));
            assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == entry1.0));
        }

        // Persist entry2 to storage now that the immutable borrow has ended.
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            std::slice::from_ref(&entry2),
        )
        .await;

        // Drain entry1 from the cache; the refill picks up entry2 via
        // seek_after(entry1.0).
        let db = rocksdb.partition_db();
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == entry2.0));
    }

    #[restate_core::test]
    async fn test_enqueue_below_cache_front_replaces_head() {
        let mut rocksdb = storage_test_environment().await;
        let qid = test_qid(8300);

        let mid = default_entry(5);
        let high = default_entry(8);
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[mid.clone(), high.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == mid.0));

        // Higher-priority key arrives via notify_enqueued.
        let new_head = default_entry(2);
        assert!(queue.enqueue(&new_head.0, &new_head.1));
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == new_head.0));

        // The other two items remain in the cache and are served in order
        // without further storage reads.
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == mid.0));
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == high.0));
    }

    #[restate_core::test]
    async fn test_remove_middle_does_not_force_reseek() {
        let mut rocksdb = storage_test_environment().await;
        let qid = test_qid(8400);

        let a = default_entry(1);
        let b = default_entry(2);
        let c = default_entry(3);
        insert_entries(
            &mut rocksdb,
            &qid,
            Stage::Inbox,
            &[a.clone(), b.clone(), c.clone()],
        )
        .await;

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == a.0));

        // Removing the middle item leaves the head unchanged.
        assert!(!queue.remove(&b.0));
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == a.0));

        // After advancing past `a`, the next head is `c` (b was removed).
        queue.advance(db, &skip, &qid).unwrap();
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == c.0));
    }

    #[restate_core::test]
    async fn test_empty_queue_resumes_on_enqueue() {
        let rocksdb = storage_test_environment().await;
        let qid = test_qid(8500);

        let db = rocksdb.partition_db();
        let mut queue: Queue<PartitionDb> = Queue::new(0);
        let skip = UnconfirmedAssignments::new();

        // Empty storage → first advance lands in Empty.
        queue.advance(db, &skip, &qid).unwrap();
        assert!(queue.is_empty());

        let entry = default_entry(1);
        assert!(queue.enqueue(&entry.0, &entry.1));
        assert!(!queue.is_empty());
        assert!(matches!(queue.head(), Some(QueueItem::Inbox { key, .. }) if *key == entry.0));
    }
}
