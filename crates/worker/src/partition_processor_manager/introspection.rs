// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Per-partition leader-side introspection registry.
//!
//! Partition processors self-register handles here when they become leader —
//! one entry per query kind, each guarded by an RAII token whose lifetime is
//! tied to the owning sub-task (invoker task, future scheduler task, …).
//! DataFusion reads this registry to route `sys_invocation_state` and
//! `sys_scheduler` queries directly to the owning task, bypassing the
//! partition processor's main `select!` loop.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::debug;

use restate_invoker_api::{InvocationStatusReport, StatusHandle};
use restate_invoker_impl::ChannelStatusReader;
use restate_platform::sync::Mutex;
use restate_storage_query_datafusion::context::PartitionLeaderStatusHandle;
use restate_types::sharding::KeyRange;
use restate_worker_api::{
    LeaderQueryRequest, LeaderQueryResponse, LeaderQuerySender, SchedulerStatusEntry,
};

/// Monotonically increasing counter stamping each registration.
///
/// On guard drop, the stored epoch is compared with the current entry's epoch —
/// if a newer registration has overwritten the entry, the old guard's drop is a
/// no-op. Prevents a stale RAII guard from clobbering a fresh registration
/// during leadership bounces.
static EPOCH: AtomicU64 = AtomicU64::new(1);

type Epoch = u64;

#[inline]
fn next_epoch() -> Epoch {
    EPOCH.fetch_add(1, Ordering::Relaxed)
}

/// Upper-bound `KeyRange` for a `BTreeMap::range(..=upper)` lookup of entries
/// whose `start <= keys.end()`. `KeyRange` orders by `(start, end)` lex, so
/// `KeyRange::new(keys.end(), u64::MAX)` is the greatest key whose start does
/// not exceed `keys.end()` — any entry with `start > keys.end()` sorts strictly
/// after it and is skipped.
#[inline]
const fn upper_bound_for(keys: KeyRange) -> KeyRange {
    KeyRange::new(keys.end(), u64::MAX)
}

/// Collect the keys of all entries whose range overlaps `keys`. Uses the
/// BTreeMap ordering to stop iterating past the point where no further entry
/// can overlap.
fn overlapping_keys<V>(map: &BTreeMap<KeyRange, V>, keys: KeyRange) -> Vec<KeyRange> {
    map.range(..=upper_bound_for(keys))
        .filter(|(range, _)| range.end() >= keys.start())
        .map(|(range, _)| *range)
        .collect()
}

/// Registry of per-partition leader-side handles, keyed by `KeyRange`.
///
/// Each query kind has an independent per-partition entry registered by its owning
/// task (invoker task, scheduler task in the future, …). Independent entries mean
/// each task owns its own RAII guard and the entries come and go on their own
/// lifecycles — teardown of one does not drop the others.
///
/// Cheap to clone — all clones share the same underlying maps.
#[derive(Debug, Clone, Default)]
pub struct PartitionLeaderHandlesRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    /// Direct invoker status channels. Read-through bypasses the partition
    /// processor's main `select!`.
    invoker_status: BTreeMap<KeyRange, (Epoch, ChannelStatusReader)>,
    /// Command channels whose receivers are drained by the partition processor's
    /// `select!` loop. Used for scheduler status (until the scheduler is taskified)
    /// and future user-limit-counter queries.
    ///
    /// When the scheduler becomes its own task, add a sibling `scheduler_status`
    /// map populated directly by the scheduler task, and retire this one (or
    /// narrow it to whatever kinds still need partition-select! routing).
    leader_query_tx: BTreeMap<KeyRange, (Epoch, LeaderQuerySender)>,
}

impl PartitionLeaderHandlesRegistry {
    /// Register the invoker-status handle for a partition and return an RAII guard.
    ///
    /// The guard is meant to be moved into the invoker task's future so the
    /// registry entry's lifetime is bound to the invoker task — cancel or panic
    /// drops the future, drops the guard, removes the entry.
    pub fn register_invoker_status(
        &self,
        key_range: KeyRange,
        reader: ChannelStatusReader,
    ) -> InvokerStatusGuard {
        let epoch = next_epoch();
        let mut inner = self.inner.lock();
        if let Some((old_epoch, _)) = inner.invoker_status.insert(key_range, (epoch, reader)) {
            debug!(
                ?key_range,
                old_epoch,
                new_epoch = epoch,
                "overwriting existing invoker-status entry"
            );
        }
        InvokerStatusGuard {
            registry: self.clone(),
            key_range,
            epoch,
        }
    }

    fn unregister_invoker_status(&self, key_range: KeyRange, epoch: Epoch) {
        let mut inner = self.inner.lock();
        if let Some((current_epoch, _)) = inner.invoker_status.get(&key_range)
            && *current_epoch == epoch
        {
            inner.invoker_status.remove(&key_range);
        }
    }

    /// Register the leader-query channel for a partition and return an RAII guard.
    ///
    /// Holder today is whoever's lifecycle matches "while we're leader" — the
    /// `LeaderState`. When scheduler and user-limit queries each move to their own
    /// tasks, this registration goes away in favor of per-task registrations.
    pub fn register_leader_query(
        &self,
        key_range: KeyRange,
        tx: LeaderQuerySender,
    ) -> LeaderQueryGuard {
        let epoch = next_epoch();
        let mut inner = self.inner.lock();
        if let Some((old_epoch, _)) = inner.leader_query_tx.insert(key_range, (epoch, tx)) {
            debug!(
                ?key_range,
                old_epoch,
                new_epoch = epoch,
                "overwriting existing leader-query entry"
            );
        }
        LeaderQueryGuard {
            registry: self.clone(),
            key_range,
            epoch,
        }
    }

    fn unregister_leader_query(&self, key_range: KeyRange, epoch: Epoch) {
        let mut inner = self.inner.lock();
        if let Some((current_epoch, _)) = inner.leader_query_tx.get(&key_range)
            && *current_epoch == epoch
        {
            inner.leader_query_tx.remove(&key_range);
        }
    }

    /// Force-remove every entry whose key range intersects the given one, across
    /// all kinds and regardless of epoch. Used as a defensive backstop by the
    /// partition-processor manager on abnormal exit; the normal teardown path
    /// removes entries via the per-kind RAII guards. Using intersect (rather
    /// than exact-match) also catches stale entries from pre-reshard ranges.
    pub fn unregister_all(&self, key_range: KeyRange) {
        let mut inner = self.inner.lock();
        // Range-bounded collect, then remove — avoids scanning the tail of the
        // map where `entry.start > key_range.end()` and so cannot overlap.
        let invoker_keys = overlapping_keys(&inner.invoker_status, key_range);
        for k in invoker_keys {
            inner.invoker_status.remove(&k);
        }
        let query_keys = overlapping_keys(&inner.leader_query_tx, key_range);
        for k in query_keys {
            inner.leader_query_tx.remove(&k);
        }
    }

    fn overlapping_invoker_readers(&self, keys: KeyRange) -> Vec<ChannelStatusReader> {
        // clone while holding the lock, release before awaiting —
        // parking_lot guards must not be held across .await points
        let inner = self.inner.lock();
        let matching: Vec<ChannelStatusReader> = inner
            .invoker_status
            .range(..=upper_bound_for(keys))
            .filter(|(range, _)| range.end() >= keys.start())
            .map(|(_, (_, reader))| reader.clone())
            .collect();
        drop(inner);
        matching
    }

    fn overlapping_leader_query_senders(&self, keys: KeyRange) -> Vec<LeaderQuerySender> {
        let inner = self.inner.lock();
        let matching: Vec<LeaderQuerySender> = inner
            .leader_query_tx
            .range(..=upper_bound_for(keys))
            .filter(|(range, _)| range.end() >= keys.start())
            .map(|(_, (_, tx))| tx.clone())
            .collect();
        drop(inner);
        matching
    }

    async fn collect_scheduler_status(
        &self,
        keys: KeyRange,
    ) -> std::vec::IntoIter<SchedulerStatusEntry> {
        let senders = self.overlapping_leader_query_senders(keys);

        let mut result = Vec::new();
        for sender in senders {
            let (command, response_rx) = restate_futures_util::command::Command::prepare(
                LeaderQueryRequest::SchedulerStatus { keys },
            );
            if sender.send(command).is_err() {
                continue;
            }

            if let Ok(LeaderQueryResponse::SchedulerStatus(mut statuses)) = response_rx.await {
                result.append(&mut statuses);
            }
        }

        result.sort_by(|(qid_a, _), (qid_b, _)| qid_a.cmp(qid_b));
        result.into_iter()
    }
}

impl StatusHandle for PartitionLeaderHandlesRegistry {
    type Iterator = std::vec::IntoIter<InvocationStatusReport>;

    async fn read_status(&self, keys: KeyRange) -> Self::Iterator {
        let readers = self.overlapping_invoker_readers(keys);

        let mut result = Vec::new();
        for reader in readers {
            // direct to the invoker task — does NOT hop through the
            // partition processor's main select!
            result.extend(reader.read_status(keys).await);
        }

        result.sort_by(|a, b| a.invocation_id().cmp(b.invocation_id()));
        result.into_iter()
    }
}

impl PartitionLeaderStatusHandle for PartitionLeaderHandlesRegistry {
    type SchedulerStatus = SchedulerStatusEntry;
    type SchedulerStatusIterator = std::vec::IntoIter<Self::SchedulerStatus>;
    type UserLimitCounter = ();
    type UserLimitCounterIterator = std::iter::Empty<Self::UserLimitCounter>;

    async fn read_scheduler_status(&self, keys: KeyRange) -> Self::SchedulerStatusIterator {
        self.collect_scheduler_status(keys).await
    }

    async fn read_user_limit_counters(&self, _keys: KeyRange) -> Self::UserLimitCounterIterator {
        std::iter::empty()
    }
}

/// RAII guard that unregisters the invoker-status entry for a partition on drop.
///
/// Epoch-protected: if a newer registration has overwritten the entry, this
/// guard's drop is a no-op — it cannot clobber a fresh registration.
///
/// Move into the invoker task's future so the entry's lifetime is bound to the
/// invoker task.
#[must_use = "dropping the guard unregisters the invoker-status entry"]
#[derive(Debug)]
pub struct InvokerStatusGuard {
    registry: PartitionLeaderHandlesRegistry,
    key_range: KeyRange,
    epoch: Epoch,
}

impl Drop for InvokerStatusGuard {
    fn drop(&mut self) {
        self.registry
            .unregister_invoker_status(self.key_range, self.epoch);
    }
}

/// RAII guard that unregisters the leader-query channel entry for a partition on drop.
///
/// Epoch-protected: if a newer registration has overwritten the entry, this
/// guard's drop is a no-op.
///
/// Held by whoever owns the "while we're leader" scope today (e.g. `LeaderState`).
/// Will be replaced by per-task guards (scheduler, user-limit) as those become tasks.
#[must_use = "dropping the guard unregisters the leader-query entry"]
#[derive(Debug)]
pub struct LeaderQueryGuard {
    registry: PartitionLeaderHandlesRegistry,
    key_range: KeyRange,
    epoch: Epoch,
}

impl Drop for LeaderQueryGuard {
    fn drop(&mut self) {
        self.registry
            .unregister_leader_query(self.key_range, self.epoch);
    }
}

#[cfg(test)]
mod tests {
    use restate_types::sharding::KeyRange;
    use restate_worker_api::channel;

    use super::PartitionLeaderHandlesRegistry;

    /// Covers the core guarantees of the handles registry:
    /// 1. Guard drop removes the entry (RAII happy path).
    /// 2. Re-registering for the same `KeyRange` bumps the epoch; the old guard's
    ///    drop is a no-op and does NOT clobber the newer entry (bounce safety).
    /// 3. `unregister_all` force-clears entries regardless of epoch.
    #[test]
    fn epoch_protects_stale_drops() {
        let registry = PartitionLeaderHandlesRegistry::default();
        let key_range = KeyRange::new(0, 1000);

        // 1. register + RAII drop
        let (tx1, _rx1) = channel();
        let guard1 = registry.register_leader_query(key_range, tx1);
        assert_eq!(
            registry.inner.lock().leader_query_tx.len(),
            1,
            "entry should be registered"
        );
        drop(guard1);
        assert!(
            registry.inner.lock().leader_query_tx.is_empty(),
            "guard drop should have unregistered"
        );

        // 2. epoch protection: stale guard drop is a no-op
        let (tx_old, _rx_old) = channel();
        let stale_guard = registry.register_leader_query(key_range, tx_old);
        let (tx_new, _rx_new) = channel();
        let fresh_guard = registry.register_leader_query(key_range, tx_new); // overwrites
        let fresh_epoch = fresh_guard.epoch;
        drop(stale_guard); // must NOT remove fresh entry
        {
            let inner = registry.inner.lock();
            let entry = inner
                .leader_query_tx
                .get(&key_range)
                .expect("fresh entry must survive stale drop");
            assert_eq!(entry.0, fresh_epoch, "fresh registration must be intact");
        }

        // 3. force-clear via unregister_all — also clears intersecting ranges
        let neighbor = KeyRange::new(500, 2000); // overlaps [0, 1000]
        let (tx_neighbor, _rx_neighbor) = channel();
        let _neighbor_guard = registry.register_leader_query(neighbor, tx_neighbor);
        registry.unregister_all(key_range);
        assert!(
            registry.inner.lock().leader_query_tx.is_empty(),
            "unregister_all must clear the entry and every overlapping one"
        );
        drop(fresh_guard); // idempotent; nothing to do
    }
}
