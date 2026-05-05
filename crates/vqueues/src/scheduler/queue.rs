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
use std::num::NonZeroU32;
use std::pin::Pin;
use std::task::{Poll, ready};

use itertools::{EitherOrBoth, Itertools as _};
use tokio::time::Instant;
use tracing::trace;

use restate_storage_api::vqueue_table::CursorError;
use restate_storage_api::vqueue_table::{
    EntryKey, EntryValue, VQueueCursor, VQueueRunningCursor, VQueueStore,
};
use restate_storage_api::{StorageError, vqueue_table};
use restate_time_util::DurationExt;
use restate_types::vqueues::VQueueId;

use super::UnconfirmedAssignments;

/// The number of entries we are willing to keep in cache
const INBOX_CACHE_CAPACITY: usize = 24;

enum Overlay {
    Add(EntryValue),
    Tombstone,
}

/// In-flight async refill, plus an overlay of `notify_enqueued` /
/// `notify_removed` events that arrived while the task was running.
///
/// # Storage-write-before-notify invariant
///
/// The partition processor commits inbox writes/deletes to RocksDB *before*
/// dispatching `EnqueuedToInbox` / `RemovedFromInbox` events to the
/// scheduler. This ordering rule underpins the following overlay reasoning:
///
/// 1. **Why tombstones are needed at all.** The refill task's RocksDB
///    snapshot is fixed at task-start time. A `notify_removed` that arrives
///    while the task is in flight may correspond to a delete that committed
///    *before* the task started its scan (snapshot still sees the row) or
///    *after* (snapshot doesn't). We can't tell from the notification
///    alone, so we record a tombstone and let `merge_join_by` suppress the
///    row if it shows up in the result.
///
/// 2. **Why `push_added_item` panics on a key it already tombstoned.** A
///    later add of a key we previously tombstoned would imply the
///    notifications arrived out-of-order with respect to storage commits,
///    which the invariant rules out.
///
/// 3. **Why cancelling an in-flight task discards the overlay safely** (see
///    [`RefillState::update_anchor`]). When cancellation kicks in, every
///    pending notification has already had its storage write committed.
///    The next fresh refill takes a new snapshot and sees post-commit
///    state directly, so the overlay would be redundant.
struct RefillTask {
    started_at: Instant,
    refill_anchor: Option<EntryKey>,
    /// Items that are known to be added or removed while the refill was in-flight
    /// Sorted ascending by EntryKey.
    overlay: VecDeque<(EntryKey, Overlay)>,
    handle: tokio::task::JoinHandle<Result<Vec<(EntryKey, EntryValue)>, StorageError>>,
    /// Exclusive upper bound on the overlay's coverage. `None` until the
    /// first at-capacity event; once set, only shrinks.
    ///
    /// On overflow we set this (exclusive upper bound) instead of
    /// evicting from the overlay. Overlay events and merge-time storage rows
    /// with `key >= horizon` are then dropped; the next refill rediscovers
    /// them via `seek_after(cache.back)` (always below `horizon`). Avoids
    /// the silent-drop hazard of back-eviction — a popped tombstone could
    /// re-admit a deleted row — at the cost of re-fetching some
    /// already-read storage rows.
    horizon: Option<EntryKey>,
}
impl RefillTask {
    /// Capacity/horizon handling shared by the `push_*` methods. Caller resolves
    /// duplicates first; `pos` is insertion position.
    ///
    /// Returns `Some(pos)` to insert there, or `None` if the event was dropped.
    fn prepare_overlay_insert(&mut self, key: EntryKey, pos: usize) -> Option<usize> {
        if self.horizon.is_some_and(|h| key >= h) {
            return None;
        }
        if self.overlay.len() < INBOX_CACHE_CAPACITY {
            return Some(pos);
        }
        // At capacity: shrink the horizon to seal the affected range.
        if pos == self.overlay.len() {
            // New key sorts past every overlay entry — it becomes the
            // horizon itself and is not tracked.
            self.horizon = Some(self.horizon.map_or(key, |h| h.min(key)));
            None
        } else {
            // New key displaces the back; the back's key becomes the
            // horizon (anything at-or-above it is now uncertain).
            let back_key = self
                .overlay
                .back()
                .expect("overlay at capacity must have a back")
                .0;
            self.horizon = Some(self.horizon.map_or(back_key, |h| h.min(back_key)));
            self.overlay.pop_back();
            Some(pos)
        }
    }

    fn push_tombstone(&mut self, key_to_remove: &EntryKey) {
        let pos = match self
            .overlay
            .binary_search_by_key(key_to_remove, |&(k, _)| k)
        {
            Ok(pos) => {
                // Existing overlay entry → upgrade to Tombstone.
                self.overlay.get_mut(pos).unwrap().1 = Overlay::Tombstone;
                return;
            }
            Err(pos) => pos,
        };
        if let Some(pos) = self.prepare_overlay_insert(*key_to_remove, pos) {
            self.overlay
                .insert(pos, (*key_to_remove, Overlay::Tombstone));
        }
    }

    fn push_added_item(&mut self, key_to_add: &EntryKey, value: &EntryValue) {
        let pos = match self.overlay.binary_search_by_key(key_to_add, |&(k, _)| k) {
            Ok(_) => panic!(
                "Something went wrong here. We should not see duplicate enqueues or an enqueue after a removal of the same key: {key_to_add:?}"
            ),
            Err(pos) => pos,
        };
        if let Some(pos) = self.prepare_overlay_insert(*key_to_add, pos) {
            self.overlay
                .insert(pos, (*key_to_add, Overlay::Add(value.clone())));
        }
    }
}

#[derive(derive_more::Debug)]
enum RefillState {
    #[debug("standby")]
    Standby {
        /// `seek_after` anchor for the next refill, and the upper bound used to
        /// decide whether to accept a `notify_enqueued`. Items currently in the
        /// cache, items previously consumed from the cache, and items in the
        /// caller's skip set together account for every inbox key
        /// `<= refill_anchor`. Keys `> refill_anchor` are undiscovered and will
        /// appear on the next refill via `seek_after(refill_anchor)`.
        refill_anchor: Option<EntryKey>,
    },
    #[debug("in-flight (age: {})", _0.started_at.elapsed().friendly())]
    InFlight(Box<RefillTask>),
}

impl Default for RefillState {
    fn default() -> Self {
        Self::Standby {
            refill_anchor: None,
        }
    }
}

impl RefillState {
    /// Updates the standby anchor, or cancels an in-flight task and resets
    /// to standby with the given anchor.
    ///
    /// Dropping an in-flight `RefillTask` here also drops its overlay
    /// (pending `Add` / `Tombstone` records). This is safe per the
    /// storage-write-before-notify invariant documented on [`RefillTask`]:
    /// every notification we have observed (and thus every overlay entry)
    /// has its storage write already committed, so the next fresh refill's
    /// snapshot reflects them directly without needing the overlay.
    fn update_anchor(&mut self, new_anchor: Option<EntryKey>) {
        match self {
            RefillState::Standby { refill_anchor } => {
                *refill_anchor = new_anchor;
            }
            RefillState::InFlight(_) => {
                trace!("Refill task was in-flight but no longer needed, cancelling it");
                // Drop the task; the receiver going away signals the worker
                // thread to bail. See `RefillTask` doc for why discarding
                // the overlay is safe here.
                *self = RefillState::Standby {
                    refill_anchor: new_anchor,
                }
            }
        }
    }

    fn is_in_flight(&self) -> bool {
        matches!(self, Self::InFlight(_))
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
    New {
        already_running: NonZeroU32,
        reader: Option<S::RunningReader>,
    },
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
    ///
    /// Sorted ascending by EntryKey. Front = current head.
    items: VecDeque<(EntryKey, EntryValue)>,
    refill_state: RefillState,
}

impl<S: VQueueStore> Queue<S> {
    /// Creates a new queue that must first go through the given number of running items
    /// before it switches to reading the waiting inbox.
    pub fn new(num_running: u32, storage: &S, qid: &VQueueId) -> Self {
        let stage = if num_running > 0 {
            Stage::New {
                already_running: NonZeroU32::new(num_running).unwrap(),
                reader: Some(storage.new_run_reader(qid)),
            }
        } else {
            Stage::Inbox
        };

        Self {
            stage,
            items: VecDeque::with_capacity(INBOX_CACHE_CAPACITY),
            refill_state: Default::default(),
        }
    }

    /// Creates an empty queue
    pub fn new_closed() -> Self {
        Self {
            stage: Stage::Empty,
            items: VecDeque::with_capacity(INBOX_CACHE_CAPACITY),
            refill_state: Default::default(),
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
        if matches!(
            self.stage,
            Stage::New { .. } | Stage::Running { .. } | Stage::Empty
        ) {
            return false;
        }

        // We are in Inbox/Waiting stage.
        let Ok(pos) = self.items.binary_search_by_key(key_to_remove, |&(k, _)| k) else {
            // This branch handles when we don't have the item in cache.
            //
            // The removed item can be:
            // - Cached
            // - Unconfirmed (not in cache) || < refill anchor
            // - > refill anchor
            //   - We have in-flight refill
            //   - No in-inflight refills
            //
            // Scenarios:
            // - We have empty cache (or does it matter?), maybe what matters is if we have it in cache or not.
            //   - We have waiting inbox (inbox > 0)
            //   - We have waiting inbox but with unconfirmed items in flights, we even out at zero.
            //     which means that we should not see any removals outside what we have already
            //     decided to start. We are effectively (empty).
            //   - No inflight refill yet. Ignore.
            //   - In flight refill exists. --> Maybe tombstone it.
            // - We have cached, no in-flight refill.
            //   - Remove if in cache, otherwise, ignore.
            // - We have cached items, removal is < the refill anchor, we don't have it in cache. Ignore.
            // - We have cached items, but the removal is > the refill anchor --> Maybe tombstone it.
            //
            // Removed item is beyond the refill anchor
            match self.refill_state {
                RefillState::Standby { .. } => {}
                RefillState::InFlight(ref mut task)
                    if task
                        .refill_anchor
                        .is_none_or(|ref refill| key_to_remove > refill) =>
                {
                    // The refill task is in flight. We cannot determine if the key will impact
                    // its result or not, we'll keep at most INBOX_CACHE_CAPACITY tombstones to
                    // use as overlay when the refill is complete.
                    task.push_tombstone(key_to_remove);
                }
                RefillState::InFlight(_) => {
                    // Ignore it. This item has either been shipped (unconfirmed assignment)
                    // or was never in the inbox to start with.
                }
            }

            return false;
        };
        self.items.remove(pos);
        // The anchor stays put — even if we removed the back, the entry was
        // also removed from storage, so `seek_after(anchor)` will skip it
        // naturally on the next refill.
        pos == 0
    }

    /// Returns `true` iff the new item became the head of the queue.
    pub fn enqueue(&mut self, key: &EntryKey, value: &EntryValue) -> bool {
        match self.stage {
            Stage::New { .. } | Stage::Running { .. } => return false,
            Stage::Empty => {
                // The cache is already allocated and empty (kept around
                // across the previous `Inbox -> Empty` transition). Just
                // re-seed it and flip the marker.
                assert!(self.items.is_empty());
                assert!(!self.refill_state.is_in_flight());
                self.items.push_back((*key, value.clone()));
                self.refill_state.update_anchor(Some(*key));
                self.stage = Stage::Inbox;
                return true;
            }
            Stage::Inbox => { /* fall-through */ }
        }

        // Insert `(key, value)` into the sorted cache.
        //
        // Returns `true` iff the item became the new front (head). Returns
        // `false` if the item was rejected because its key falls strictly above
        // the cache's coverage zone (`> refill_anchor`); in that case the item
        // will be discovered on the next refill.

        // Priority-queue rule: ignore items strictly above our coverage zone.
        // The bound is `refill_anchor`, NOT `cache.back`. After consuming the
        // back of the cache, `cache.back` can be lower than `refill_anchor`,
        // and items in `(cache.back, refill_anchor]` would be lost (the next
        // refill seek_after(refill_anchor) only returns keys strictly greater
        // than the anchor).
        match self.refill_state {
            RefillState::Standby { refill_anchor, .. }
                if refill_anchor.is_none_or(|ref refill| key > refill) =>
            {
                // We cannot accept items beyond our coverage zone.
                return false;
            }
            RefillState::Standby { .. } => { /* in coverage zone */ }
            // This enqueue may or may not appear in the in-flight operation and
            // there is no way to determine that. So, we stage it on the in-flight
            // task overlay until we receive the result.
            RefillState::InFlight(ref mut task)
                if task.refill_anchor.is_none_or(|ref refill| key > refill) =>
            {
                task.push_added_item(key, value);
                return false;
            }
            RefillState::InFlight(ref task) => {
                // the new item is < than what the refill task is interested in. Therefore,
                // we add it right now.
                assert!(task.refill_anchor.is_some_and(|ref refill| key <= refill));
            }
        }

        let pos = match self.items.binary_search_by_key(key, |&(k, _)| k) {
            Ok(_) => {
                // We already know about this key which means that the head has not changed
                // as a result of this enqueue.
                return false;
            }
            Err(pos) => pos,
        };

        // We need to be careful about moving the refill anchor if there is an
        // in-flight refill task.
        //
        // let's say we have a refill task in flight, and we are enqueueing smaller
        // than it's anchor point, we are confident that the task will never return
        // this item. If as a result of the enqueue we exceeded the cache capacity.
        //
        // If there was an in-flight refill, we must cancel it and reset our refill
        // anchor to point to the back of the cache so that the next refill can
        // re-discover it. This is an acceptable trade-off because we must be here
        // because we have sufficiently populated the cache with recently enqueued
        // items and we wouldn't need the refill immediately anyway.

        // At-cap fast path: avoid the VecDeque grow/shrink that would happen
        // if we inserted first and evicted afterwards.
        if self.items.len() >= INBOX_CACHE_CAPACITY {
            if pos == self.items.len() {
                // The new key would land at the back and be evicted on the
                // very next step. Skip the insert, but still lower the anchor
                // to the current back so the next refill can re-discover `key`
                // via `seek_after(anchor)`.
                //
                // Cancels the in-flight task if exists
                self.refill_state
                    .update_anchor(self.items.back().map(|(k, _)| *k));
                return false;
            }
            // Make room first; `pos` is unaffected because pos < old_len in
            // this branch (we shift elements right of `pos` regardless).
            debug_assert!(pos < self.items.len());
            self.items.pop_back();
            self.items.insert(pos, (*key, value.clone()));

            // Cancels the in-flight task if exists
            self.refill_state
                .update_anchor(self.items.back().map(|(k, _)| *k));
            return pos == 0;
        }

        // Normal path: cache below capacity.
        self.items.insert(pos, (*key, value.clone()));
        // Anchor maintenance: if it was None (very first insert), set it to
        // this key. Otherwise the precondition above guarantees
        // `key <= anchor`, so the anchor stays put.
        if let RefillState::Standby { refill_anchor } = &mut self.refill_state
            && refill_anchor.is_none()
        {
            *refill_anchor = Some(*key);
        }
        pos == 0
    }

    /// Returns the head if known, or `None` if the queue needs advancing.
    pub fn head(&self) -> Option<QueueItem<'_>> {
        match &self.stage {
            Stage::New { .. } => None,
            Stage::Running { head: (k, v), .. } => Some(QueueItem::Running { key: k, value: v }),
            Stage::Inbox => self
                .items
                .front()
                .map(|(k, v)| QueueItem::Inbox { key: k, value: v }),
            Stage::Empty => Some(QueueItem::None),
        }
    }

    pub fn poll_advance_if_needed(
        &mut self,
        cx: &mut std::task::Context<'_>,
        storage: &S,
        skip: &UnconfirmedAssignments,
        qid: &VQueueId,
        effectively_empty: bool,
        allow_blocking_io: bool,
    ) -> Poll<Result<QueueItem<'_>, StorageError>> {
        loop {
            let needs_advance = match self.stage {
                Stage::New { .. } => true,
                Stage::Inbox => self.items.is_empty(),
                _ => false,
            };
            if !needs_advance {
                break;
            }
            if !self.try_advance()? {
                // We cannot advance without a refill
                if self.refill_state.is_in_flight() {
                    ready!(self.poll_refill_task(cx, skip));
                    // If we ended up also being empty and we can't determine if we are
                    // empty or not, we should start another refill task and return Pending.
                    // This happens automatically because in that case we'll "continue"
                } else {
                    // Do we need to refill?
                    // We don't need to refill if we are at Inbox stage and we know no more
                    // inbox entries are available.
                    if effectively_empty && matches!(self.stage, Stage::Inbox) {
                        self.stage = Stage::Empty;
                        break;
                    }
                    // A) try refill immediate refill if allowed
                    match self.try_refill(storage, qid, skip, allow_blocking_io) {
                        Ok(_) => {}
                        Err(CursorError::WouldBlock) => {
                            // B) start an async refill task
                            self.start_refill_task(storage, qid)?;
                        }
                        Err(CursorError::Other(e)) => {
                            // C) fail miserably
                            return Poll::Ready(Err(e));
                        }
                    }
                }
            }
        }

        Poll::Ready(Ok(match &self.stage {
            Stage::New { .. } => unreachable!("head must be resolved after advance_if_needed"),
            Stage::Running { head: (k, v), .. } => QueueItem::Running { key: k, value: v },
            Stage::Inbox => {
                let (k, v) = self
                    .items
                    .front()
                    .expect("inbox cache must have a head after advance_if_needed");
                QueueItem::Inbox { key: k, value: v }
            }
            Stage::Empty => QueueItem::None,
        }))
    }

    /// Advances the queue to the next item.
    ///
    /// In the inbox stage this consumes the current head (if any) and exposes
    /// the next cached item; the cache is refilled from storage in batches of
    /// up to [`INBOX_CACHE_CAPACITY`] when it empties. The `skip` set is
    /// consulted only when reading the inbox stage; it is ignored when
    /// reading the running stage.
    ///
    ///
    /// Returns false if advance was not possible and we need to perform a refill.
    pub fn try_advance(&mut self) -> Result<bool, StorageError> {
        // Split into disjoint borrows so the `Stage::Inbox` arm below can
        // mutate both `stage` and `inbox_cache` without fighting the
        // borrow checker.
        let Self { stage, items, .. } = self;
        loop {
            match stage {
                Stage::New {
                    already_running,
                    reader,
                } => {
                    let mut reader = reader.take().unwrap();
                    reader.seek_to_first();
                    if let Some((key, value)) = reader.peek()? {
                        *stage = Stage::Running {
                            head: (key, value),
                            reader,
                            remaining: already_running.get(),
                        };
                        return Ok(true);
                    }
                    debug_assert!(
                        false,
                        "vqueue has no running items but its metadata says it has {already_running}",
                    );
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
                            return Ok(true);
                        }
                        None => {
                            debug_assert_eq!(0, *remaining);
                            *stage = Stage::Inbox;
                        }
                    }
                }
                Stage::Inbox => return Ok(items.pop_front().is_some()),
                Stage::Empty => return Ok(true),
            }
        }
    }

    /// Refills `cache` with up to [`INBOX_CACHE_CAPACITY`] items from storage if
    /// it's possible to do so without blocking the thread.
    ///
    /// Starting at `cache.refill_anchor` (or the very first key if no anchor is
    /// set yet). Returns true if items were added. Items in the `skip` set are
    /// not added to the cache, but the anchor advances past them so they are not
    /// reconsidered on the next refill.
    fn try_refill(
        &mut self,
        storage: &S,
        qid: &VQueueId,
        skip: &UnconfirmedAssignments,
        allow_blocking_io: bool,
    ) -> Result<(), CursorError> {
        let start = Instant::now();
        let mut reader = storage.new_inbox_reader(qid, vqueue_table::Options { allow_blocking_io });
        let RefillState::Standby { refill_anchor } = &mut self.refill_state else {
            panic!("refill state must be standby");
        };

        match refill_anchor {
            Some(anchor) => reader.seek_after(anchor),
            None => reader.seek_to_first(),
        }

        while self.items.len() < INBOX_CACHE_CAPACITY {
            let Some((key, value)) = reader.peek()? else {
                if self.items.is_empty() {
                    self.stage = Stage::Empty;
                }
                break;
            };

            if !skip.contains_key(&key) {
                // we can safely unwrap the option here because we know that the key
                // exists.
                self.items.push_back((key, value));
            }
            *refill_anchor = Some(refill_anchor.map_or(key, |a| a.max(key)));
            reader.advance();
        }
        tracing::debug!(
            "non-blocking refill finished in {}, cache has {} items",
            start.elapsed().friendly(),
            self.items.len()
        );
        Ok(())
    }

    fn start_refill_task(&mut self, storage: &S, qid: &VQueueId) -> Result<(), StorageError> {
        assert!(!self.refill_state.is_in_flight());
        let RefillState::Standby { refill_anchor } = self.refill_state else {
            panic!("refill state must be standby");
        };

        let mut reader = storage.new_inbox_reader(
            qid,
            vqueue_table::Options {
                allow_blocking_io: true,
            },
        );

        let handle = tokio::task::spawn_blocking(move || {
            // collect and send the results at the end
            let mut results = Vec::with_capacity(INBOX_CACHE_CAPACITY);

            match refill_anchor {
                Some(anchor) => reader.seek_after(&anchor),
                None => reader.seek_to_first(),
            }

            while results.len() < INBOX_CACHE_CAPACITY {
                // In this mode, we don't expect to see WouldBlock
                match reader.peek() {
                    Ok(Some((key, value))) => {
                        results.push((key, value));
                        reader.advance();
                    }
                    Ok(None) => {
                        // no more items
                        break;
                    }
                    Err(CursorError::WouldBlock) => {
                        unreachable!("refill task should never see WouldBlock");
                    }
                    Err(CursorError::Other(e)) => {
                        tracing::error!("refill thread failed: {e}");
                        return Err(e);
                    }
                }
            }
            // Ship the results
            tracing::info!("Refill thread completed");
            Ok(results)
        });

        let task = Box::new(RefillTask {
            started_at: Instant::now(),
            refill_anchor,
            horizon: None,
            overlay: VecDeque::with_capacity(INBOX_CACHE_CAPACITY),
            handle,
        });

        self.refill_state = RefillState::InFlight(task);
        Ok(())
    }

    fn poll_refill_task(
        &mut self,
        cx: &mut std::task::Context<'_>,
        skip: &UnconfirmedAssignments,
    ) -> Poll<()> {
        let (items, overlay, mut refill_anchor, horizon) = match self.refill_state {
            RefillState::Standby { .. } => return Poll::Ready(()),
            RefillState::InFlight(ref mut task) => {
                match ready!(Pin::new(&mut task.handle).poll(cx)) {
                    Err(join_err) => {
                        tracing::error!("refill task panicked: {join_err}");
                        self.refill_state = RefillState::Standby {
                            refill_anchor: task.refill_anchor.take(),
                        };
                        return Poll::Ready(());
                    }
                    Ok(Err(err)) => {
                        tracing::error!("refill task failed: {err}");
                        self.refill_state = RefillState::Standby {
                            refill_anchor: task.refill_anchor.take(),
                        };
                        return Poll::Ready(());
                    }
                    Ok(Ok(result)) => {
                        tracing::debug!(
                            "refill task finished with {} items in  {}",
                            result.len(),
                            task.started_at.elapsed().friendly()
                        );
                        (
                            result,
                            std::mem::take(&mut task.overlay),
                            task.refill_anchor.take(),
                            task.horizon.take(),
                        )
                    }
                }
            }
        };
        // If we got less items than what we asked, then this must have been the end of the queue
        // at the time of the refill.
        let end_of_queue = items.len() < INBOX_CACHE_CAPACITY;

        // We now need to merge the items we received, apply the overlays, and deduplicate
        // with existing entries in cache.
        // At the end, we figure out what's the next anchor to use for the next refill.
        //
        //
        // The strategy here is more complex that the blocking/inline version because
        // concurrency is hard, who knew!
        // We need to do all that while keeping the cache capacity in check. We don't
        // want to re-allocate/resize the cache.
        //
        // We navigate both the overlays and the items in together in semi-lockstep.
        // Technically, this is a LSM-style read/compaction algorithm.
        for either in items
            .into_iter()
            .merge_join_by(overlay, |(key, _), (overlay_key, _)| key.cmp(overlay_key))
        {
            // First key at-or-above the horizon ends the merge: overlay
            // entries are all below it by construction, and the merge
            // walks ascending, so the rest is storage that the next
            // refill will rediscover via `seek_after`.
            let key = match &either {
                EitherOrBoth::Left((k, _))
                | EitherOrBoth::Right((k, _))
                | EitherOrBoth::Both((k, _), _) => *k,
            };
            if horizon.is_some_and(|h| key >= h) {
                break;
            }
            // Left is the item from db, right is the overlay
            match either {
                // Somewhat similar to a normal enqueue
                EitherOrBoth::Left((key, value))
                | EitherOrBoth::Right((key, Overlay::Add(value)))
                    // overlay always wins.
                | EitherOrBoth::Both((key, _), (_, Overlay::Add(value))) => {
                    if skip.contains_key(&key) {
                            // The key was already dispatched, skip it.
                        refill_anchor = Some(refill_anchor.map_or(key, |a| a.max(key)));
                        continue;
                    }
                    // Insert sorted in cache and ignore it if we already have it.
                    // If this item pushes us over the cache capcity, then we ignore it and reset
                    // the refill anchor to it.
                    let pos = match self.items.binary_search_by_key(&key, |&(k, _)| k) {
                        Ok(_) => continue,
                        Err(pos) => pos,
                    };
                    if self.items.len() >= INBOX_CACHE_CAPACITY {
                        if pos == self.items.len() {
                            // beyond capacity, ignore the item. Reset the anchor.
                            refill_anchor = self.items.back().map(|(k, _)| *k);
                            break;
                        }
                        // Make room first;
                        self.items.pop_back();
                        self.items.insert(pos, (key, value));
                        refill_anchor = self.items.back().map(|(k, _)| *k);
                        break;
                    }

                    // Normal path: cache below capacity.
                    self.items.insert(pos, (key, value));
                    refill_anchor = Some(key);
                }
                EitherOrBoth::Right((key, Overlay::Tombstone))
                    // overlay always wins.
                | EitherOrBoth::Both((key, _), (_, Overlay::Tombstone)) => {
                    // In theory, we should never see a tombstone that impacts the existing
                    // cache.
                    debug_assert!(self.items.binary_search_by_key(&key, |&(k, _)| k).is_err());
                    // we should push the anchor to this item.
                    refill_anchor = Some(key);
                }
            }
        }

        // It's very important is that we must reset the task to standby
        self.refill_state = RefillState::Standby {
            refill_anchor: refill_anchor.or(self.items.back().map(|(k, _)| *k)),
        };

        // at the end, if the cache is empty and end_of_queue is true, then we must
        // have exhausted the inbox.
        if self.items.is_empty() && end_of_queue {
            self.stage = Stage::Empty;
        }

        Poll::Ready(())
    }

    // pub fn refill(
    //     &mut self,
    //     generation: u16,
    //     items: Vec<(EntryKey, EntryValue)>,
    //     skip: &UnconfirmedAssignments,
    // ) -> bool {
    //     let mut pushed = false;
    //     if generation != self.inbox_cache.refill_generation {
    //         // ignore old refills
    //         return pushed;
    //     }
    //
    //     // are we at the end?
    //     let at_end = items.len() < INBOX_CACHE_CAPACITY;
    //
    //     for (key, value) in items {
    //         if !self.has_cache_capacity() {
    //             // There are items that we won't attempt, so we can't say that
    //             // the queue is empty.
    //             self.inbox_cache.refill_generation =
    //                 self.inbox_cache.refill_generation.wrapping_add(1);
    //             return pushed;
    //         }
    //         pushed = self.inbox_cache.push_back(skip, key, value);
    //     }
    //
    //     self.inbox_cache.refill_generation = self.inbox_cache.refill_generation.wrapping_add(1);
    //     // todo: we have stuff so, we should switch into ready to poll.
    //     if self.inbox_cache.is_empty() && at_end {
    //         self.stage = Stage::Empty;
    //     }
    //     pushed
    // }

    pub(crate) fn remaining_in_running_stage(&self) -> u32 {
        match &self.stage {
            Stage::New {
                already_running, ..
            } => already_running.get(),
            Stage::Running { remaining, .. } => *remaining,
            Stage::Inbox | Stage::Empty => 0,
        }
    }
}

#[cfg(test)]
#[path = "queue_test.rs"]
mod queue_test;
