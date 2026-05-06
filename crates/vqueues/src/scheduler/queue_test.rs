// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test harness for the vqueue refill machinery.
//!
//! These tests use a fake [`VQueueStore`] (`GatedStore`) that lets the
//! caller freeze the snapshot a refill task sees and interleave
//! `notify_enqueued` / `notify_removed` events deterministically against
//! the in-flight refill thread. That's what makes overlay and cancellation
//! behaviour testable without a real RocksDB.
//!
//! Each [`GatedReader`] is created with `Options::allow_blocking_io` set
//! by the queue itself:
//! - The first attempt (`try_refill`) passes `allow_blocking_io: false`;
//!   the fake responds with [`CursorError::WouldBlock`] which drives the
//!   queue down the async refill path.
//! - The async path then constructs a second reader with
//!   `allow_blocking_io: true`; that reader parks on its first `peek`
//!   (signalling the test that the snapshot is frozen) and resumes once
//!   the test releases the latch.

use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};

use restate_clock::RoughTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    CursorError, EntryId, EntryKey, EntryKind, EntryMetadata, EntryValue, Options, Status,
    VQueueCursor, VQueueRunningCursor, VQueueStore,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::vqueues::VQueueId;

use super::*;

// ---------- helpers ----------

const BASE_RUN_AT_MS: u64 = 1_744_000_000_000;

/// Builds an entry whose sort position is determined entirely by `seq`
/// (other `EntryKey` components are fixed).
fn entry_at_seq(seq: u64) -> (EntryKey, EntryValue) {
    let run_at = RoughTimestamp::from_unix_millis_clamped(MillisSinceEpoch::new(BASE_RUN_AT_MS));
    let created_at = UniqueTimestamp::try_from(1_000u64 + seq).unwrap();
    let entry_id = EntryId::new(EntryKind::Invocation, [0u8; EntryId::REMAINDER_LEN]);
    let key = EntryKey::new(false, run_at, seq, entry_id);
    let stats = EntryStatistics::new(created_at, run_at);
    let value = EntryValue {
        status: Status::New,
        metadata: EntryMetadata::default(),
        stats,
    };
    (key, value)
}

fn test_qid(partition_key: u64) -> VQueueId {
    VQueueId::custom(partition_key, "1")
}

/// Polls the queue once and asserts it returned `Pending`. Used to kick
/// off an in-flight refill task so the test can interleave events while
/// the refill thread is parked at its gate.
fn poll_once_expect_pending<S: VQueueStore>(
    queue: &mut Queue<S>,
    storage: &S,
    skip: &UnconfirmedAssignments,
    qid: &VQueueId,
) {
    let mut cx = Context::from_waker(Waker::noop());
    match queue.poll_advance_if_needed(&mut cx, storage, skip, qid, false, false) {
        Poll::Pending => {}
        Poll::Ready(Ok(item)) => panic!("expected Pending, got Ready({item:?})"),
        Poll::Ready(Err(e)) => panic!("expected Pending, got error: {e:?}"),
    }
}

/// Drives `poll_advance_if_needed` until it returns `Ready`. Yields between
/// Pending iterations so the `spawn_blocking` refill task can make progress.
async fn drive_until_ready<S: VQueueStore>(
    queue: &mut Queue<S>,
    storage: &S,
    skip: &UnconfirmedAssignments,
    qid: &VQueueId,
) {
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        match queue.poll_advance_if_needed(&mut cx, storage, skip, qid, false, false) {
            Poll::Ready(Ok(_)) => return,
            Poll::Ready(Err(e)) => panic!("queue error: {e:?}"),
            Poll::Pending => tokio::time::sleep(std::time::Duration::from_millis(1)).await,
        }
    }
}

/// Pops everything currently in the cache (no refill, no I/O) and returns
/// the keys in head-to-tail order.
fn drain_cache<S: VQueueStore>(queue: &mut Queue<S>) -> Vec<EntryKey> {
    let mut keys = vec![];
    while let Some(QueueItem::Inbox { key, .. }) = queue.head() {
        keys.push(*key);
        queue.try_advance().unwrap();
    }
    keys
}

// ---------- fake VQueueStore ----------

/// In-memory storage with hooks to gate the refill thread at a precise
/// moment, so tests can deterministically interleave queue events against
/// an in-flight refill.
struct GatedStore {
    /// Source of truth for what `new_inbox_reader` will snapshot. Sorted
    /// ascending by `EntryKey`.
    entries: Mutex<Vec<(EntryKey, EntryValue)>>,
    /// Latch released by the test when it wants the parked refill thread
    /// to proceed past its first `peek`.
    release: Arc<(Mutex<bool>, Condvar)>,
    /// Signalled by the reader the moment it parks. The test waits on
    /// this to know the snapshot is frozen and queue events can now be
    /// safely interleaved.
    parked: Arc<(Mutex<bool>, Condvar)>,
}

impl GatedStore {
    fn new(entries: Vec<(EntryKey, EntryValue)>) -> Self {
        Self {
            entries: Mutex::new(entries),
            release: Arc::new((Mutex::new(false), Condvar::new())),
            parked: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn release_refill_thread(&self) {
        let (lock, cv) = &*self.release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }

    fn wait_until_parked(&self) {
        let (lock, cv) = &*self.parked;
        let mut parked = lock.lock().unwrap();
        while !*parked {
            parked = cv.wait(parked).unwrap();
        }
    }
}

struct GatedReader {
    /// Frozen at construction time (matches RocksDB snapshot semantics).
    /// Sorted ascending by `EntryKey`.
    snapshot: Vec<(EntryKey, EntryValue)>,
    cursor: usize,
    /// `true` for blocking readers (those constructed via the async refill
    /// path with `allow_blocking_io: true`); they park at the gate on the
    /// first `peek` so the test can pin the snapshot moment. Non-blocking
    /// readers always return `WouldBlock` instead, driving the queue down
    /// the async refill path.
    blocking: bool,
    release: Arc<(Mutex<bool>, Condvar)>,
    parked: Arc<(Mutex<bool>, Condvar)>,
    /// Whether this reader has parked yet. Each reader parks at most
    /// once, on its first peek/key/value call.
    has_parked: bool,
}

impl GatedReader {
    fn block_until_released(&mut self) {
        if self.has_parked {
            return;
        }
        self.has_parked = true;
        // Tell the test we're parked.
        let (lock, cv) = &*self.parked;
        *lock.lock().unwrap() = true;
        cv.notify_all();
        // Wait for the test to release us.
        let (lock, cv) = &*self.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = cv.wait(released).unwrap();
        }
    }
}

impl VQueueCursor for GatedReader {
    fn seek_to_first(&mut self) {
        self.cursor = 0;
    }

    fn seek_after(&mut self, anchor: &EntryKey) {
        self.cursor = self
            .snapshot
            .iter()
            .position(|(k, _)| k > anchor)
            .unwrap_or(self.snapshot.len());
    }

    fn advance(&mut self) {
        if self.cursor < self.snapshot.len() {
            self.cursor += 1;
        }
    }

    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>, CursorError> {
        if !self.blocking {
            return Err(CursorError::WouldBlock);
        }
        self.block_until_released();
        Ok(self.snapshot.get(self.cursor).cloned())
    }
}

/// Stub for the running stage. Tests construct queues with
/// `num_running = 0`, so this is never actually exercised; it only exists
/// to satisfy the [`VQueueStore`] trait bound.
struct StubRunningReader;

impl VQueueRunningCursor for StubRunningReader {
    fn seek_to_first(&mut self) {}
    fn peek(&mut self) -> Result<Option<(EntryKey, EntryValue)>, StorageError> {
        Ok(None)
    }
    fn advance(&mut self) {}
}

impl VQueueStore for GatedStore {
    type RunningReader = StubRunningReader;
    type InboxReader = GatedReader;

    fn new_run_reader(&self, _qid: &VQueueId) -> Self::RunningReader {
        StubRunningReader
    }

    fn new_inbox_reader(&self, _qid: &VQueueId, opts: Options) -> Self::InboxReader {
        // Snapshot freezes here.
        let snapshot = self.entries.lock().unwrap().clone();
        GatedReader {
            snapshot,
            cursor: 0,
            blocking: opts.allow_blocking_io,
            release: self.release.clone(),
            parked: self.parked.clone(),
            has_parked: false,
        }
    }
}

// ---------- tests ----------

/// Sanity: a single in-flight refill with no concurrent events surfaces
/// every storage row in the cache, in `EntryKey` order.
#[restate_core::test]
async fn refill_without_overlay_activity() {
    let entries: Vec<_> = (1..=5).map(entry_at_seq).collect();
    let storage = GatedStore::new(entries.clone());
    let qid = test_qid(1);
    let mut queue: Queue<GatedStore> = Queue::new(0, &storage, &qid);
    let skip = UnconfirmedAssignments::new();

    poll_once_expect_pending(&mut queue, &storage, &skip, &qid);
    storage.wait_until_parked();
    storage.release_refill_thread();
    drive_until_ready(&mut queue, &storage, &skip, &qid).await;

    let drained = drain_cache(&mut queue);
    assert_eq!(
        drained,
        entries.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
        "cache should contain every storage row in order",
    );
}

/// Tombstones in the overlay correctly suppress the matching storage row
/// during merge — no overflow happens here, so the overlay's information
/// is fully preserved. This is the "happy path" for the
/// commit-before-notify invariant.
#[restate_core::test]
async fn tombstone_in_overlay_suppresses_storage_row() {
    let r_target = entry_at_seq(100);
    let storage = GatedStore::new(vec![r_target.clone()]);
    let qid = test_qid(2);
    let mut queue: Queue<GatedStore> = Queue::new(0, &storage, &qid);
    let skip = UnconfirmedAssignments::new();

    poll_once_expect_pending(&mut queue, &storage, &skip, &qid);
    storage.wait_until_parked();
    // Storage commit (modelled here as the snapshot being frozen with
    // r_target visible) lands first; the notify_removed lands second.
    queue.remove(&r_target.0);
    storage.release_refill_thread();
    drive_until_ready(&mut queue, &storage, &skip, &qid).await;

    let drained = drain_cache(&mut queue);
    assert!(
        !drained.contains(&r_target.0),
        "deleted row should be suppressed by overlay tombstone, got: {drained:?}",
    );
}

/// **Bug demonstration.** When the overlay is at capacity and the back
/// entry is a tombstone, `push_added_item`'s `pop_back` silently drops
/// that tombstone. The merge then lets the (already-deleted) row into
/// the cache.
///
/// Layout at the moment of overflow:
///
/// ```text
/// overlay[0]                     = Tombstone(seq=50)        // pre-tombstone
/// overlay[1]                     = Tombstone(seq=100)       // pre-tombstone
/// overlay[2..CAPACITY-1]         = Add(seq=150..)           // CAPACITY-3 adds
/// overlay[CAPACITY-1]            = Tombstone(seq=500)       // *r_target*
/// ```
///
/// The trigger add (seq=400) sorts at `pos = CAPACITY-1`, which is
/// `< overlay.len() == CAPACITY`, so `push_added_item` evicts the back
/// (`Tombstone(500)`) and inserts the trigger. After this, the overlay
/// holds only `[T(50), T(100), Add(150..), Add(400)]` — the tombstone
/// for `r_target` is gone.
///
/// Storage is `[r_target=seq500]` (a single row that's been deleted but
/// is still in the in-flight task's snapshot, faithful to the race the
/// invariant doc on `RefillTask` describes). The merge produces:
///
/// ```text
/// [T(50), T(100), Add(150..), Add(400), Left(seq500)]
/// ```
///
/// With the tombstone evicted there's nothing to suppress `Left(seq500)`,
/// so the merge inserts it into the cache. The drain then sees `seq500`,
/// which is the bug.
#[restate_core::test]
async fn tombstone_evicted_on_overlay_overflow_leaks_deleted_row() {
    // Overlay layout requires CAPACITY >= 4 (2 front tombstones + at
    // least one add + 1 back tombstone). Bail loudly if someone shrinks
    // the cache below that.
    const { assert!(INBOX_CACHE_CAPACITY >= 4) };

    // Front-anchor tombstones: low seqs so they sort to the front of the
    // overlay and don't influence the eviction we want to trigger.
    let pre_tombstone_a = entry_at_seq(50);
    let pre_tombstone_b = entry_at_seq(100);

    // The deleted row that the in-flight task's snapshot still sees.
    let r_target = entry_at_seq(500);

    // Adds that fit between the front tombstones and r_target's
    // tombstone. Together with the three tombstones the overlay reaches
    // exactly CAPACITY. We use a contiguous seq range starting at 150.
    let n_adds = INBOX_CACHE_CAPACITY - 3;
    let pre_adds: Vec<_> = (150..150 + n_adds as u64).map(entry_at_seq).collect();

    // The trigger: an add that sorts BEFORE r_target's tombstone but
    // after every other overlay entry. With `pos < overlay.len()` and
    // the overlay at cap, `push_added_item` will `pop_back` — evicting
    // `Tombstone(r_target)`.
    let trigger_add = entry_at_seq(400);

    let storage = GatedStore::new(vec![r_target.clone()]);
    let qid = test_qid(3);
    let mut queue: Queue<GatedStore> = Queue::new(0, &storage, &qid);
    let skip = UnconfirmedAssignments::new();

    // Kick off the in-flight refill so subsequent enqueue/remove events
    // route through the overlay rather than the cache.
    poll_once_expect_pending(&mut queue, &storage, &skip, &qid);
    storage.wait_until_parked();

    // Three tombstones; only Tombstone(r_target) matters for the bug.
    queue.remove(&pre_tombstone_a.0);
    queue.remove(&pre_tombstone_b.0);
    queue.remove(&r_target.0);

    // Fill the overlay up to CAPACITY with the front-of-r_target adds.
    for (k, v) in &pre_adds {
        queue.enqueue(k, v);
    }

    // Trigger the eviction. After this, Tombstone(r_target) is gone.
    queue.enqueue(&trigger_add.0, &trigger_add.1);

    // Release the refill thread; it returns the snapshot ([r_target]).
    storage.release_refill_thread();
    drive_until_ready(&mut queue, &storage, &skip, &qid).await;

    let drained = drain_cache(&mut queue);

    // Bug: r_target shows up in the cache even though we issued
    // `notify_removed` for it before the merge ran. The matching
    // tombstone was dropped on overlay overflow.
    assert!(
        !drained.contains(&r_target.0),
        "deleted row leaked into cache: {drained:?}",
    );
}
