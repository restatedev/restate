// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use slotmap::SlotMap;
use tokio::task::JoinHandle;
use tracing::{debug, trace};

use restate_platform::hash::HashMap;
use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::VQueueMeta;
use restate_storage_api::vqueue_table::{ReadVQueueTable, ScanVQueueTable};
use restate_types::sharding::PartitionKey;
use restate_types::vqueues::VQueueId;

type Result<T> = std::result::Result<T, StorageError>;

slotmap::new_key_type! { pub struct VQueueHandle; }

// A read-only view over the in-memory stash of vqueues.
#[derive(Copy, Clone)]
pub struct VQueuesMeta<'a> {
    inner: &'a VQueuesMetaCache,
}

impl<'a> VQueuesMeta<'a> {
    #[inline]
    fn new(cache: &'a VQueuesMetaCache) -> VQueuesMeta<'a> {
        Self { inner: cache }
    }

    pub fn get(&self, key: VQueueHandle) -> Option<&Slot> {
        self.inner.slab.get(key)
    }

    /// Lookup the cache handle of the vqueue by its id.
    pub fn handle_for(&self, qid: &VQueueId) -> Option<VQueueHandle> {
        self.inner.queues.get(qid).copied()
    }

    pub fn get_vqueue(&'a self, qid: &VQueueId) -> Option<&'a VQueueMeta> {
        self.inner
            .queues
            .get(qid)
            .and_then(|key| self.get(*key))
            .map(|slot| &slot.meta)
    }

    pub fn iter_active_vqueues(
        &'a self,
    ) -> impl Iterator<Item = (VQueueHandle, &'a VQueueId, &'a VQueueMeta)> {
        self.inner
            .slab
            .iter()
            .filter_map(|(key, Slot { qid, meta, .. })| {
                meta.is_active().then_some((key, qid, meta))
            })
    }

    pub fn num_active(&self) -> usize {
        self.inner
            .slab
            .values()
            .filter(|slot| slot.meta.is_active())
            .count()
    }

    pub fn report(&self) {
        self.inner.report();
    }

    pub fn capacity(&self) -> usize {
        self.inner.slab.capacity()
    }
}

#[derive(Clone)]
pub struct Slot {
    qid: VQueueId,
    meta: VQueueMeta,
    /// `Some` iff this slot belongs to the inactive list. A singleton has both
    /// links set to `None`, so the outer option records membership explicitly.
    inactive: Option<InactiveLinks>,
}

/// Intrusive linked-list of inactive slots.
#[derive(Clone, Copy)]
struct InactiveLinks {
    prev: Option<VQueueHandle>,
    next: Option<VQueueHandle>,
}

impl Slot {
    #[inline(always)]
    pub fn vqueue_id(&self) -> &VQueueId {
        &self.qid
    }

    #[inline(always)]
    pub fn partition_key(&self) -> PartitionKey {
        self.qid.partition_key()
    }

    #[inline(always)]
    pub fn meta(&self) -> &VQueueMeta {
        &self.meta
    }
}

#[derive(Clone)]
pub struct VQueuesMetaCache {
    queues: HashMap<VQueueId, VQueueHandle>,
    slab: SlotMap<VQueueHandle, Slot>,
    /// Purged slots detached from `queues` but retained until the scheduler has
    /// handled all events from the committed WAL batch.
    pending_purges: Vec<VQueueHandle>,
    /// Soft cap; active queues remain cached even above this target.
    target_capacity: usize,
    /// Intrusive FIFO of inactive slots, excluding pending purges. Membership
    /// changes never detach ID lookup or invalidate handles unless compaction is
    /// triggered.
    inactive_head: Option<VQueueHandle>,
    inactive_tail: Option<VQueueHandle>,
}

impl VQueuesMetaCache {
    pub fn view(&self) -> VQueuesMeta<'_> {
        VQueuesMeta::new(self)
    }

    pub fn get(&self, key: VQueueHandle) -> Option<&Slot> {
        self.slab.get(key)
    }

    /// Mutates metadata and keeps eviction eligibility in sync. Mutable metadata
    /// must not escape this closure: slots remain cached until the batch boundary.
    pub(super) fn update_meta<R>(
        &mut self,
        handle: VQueueHandle,
        update: impl FnOnce(&VQueueId, &mut VQueueMeta) -> R,
    ) -> R {
        let slot = self.slab.get_mut(handle).expect("cached vqueue has a slot");
        let was_active = slot.meta.is_active();
        let result = update(&slot.qid, &mut slot.meta);
        let is_active = slot.meta.is_active();
        if was_active != is_active {
            if is_active {
                self.unlink_inactive(handle);
            } else {
                self.link_inactive(handle);
            }
        }
        result
    }

    fn link_inactive(&mut self, handle: VQueueHandle) {
        let slot = self.slab.get_mut(handle).expect("cached vqueue has a slot");
        debug_assert!(!slot.meta.is_active());
        debug_assert!(slot.inactive.is_none());
        slot.inactive = Some(InactiveLinks {
            prev: self.inactive_tail,
            next: None,
        });
        if let Some(tail) = self.inactive_tail {
            self.slab[tail].inactive.as_mut().unwrap().next = Some(handle);
        } else {
            self.inactive_head = Some(handle);
        }
        self.inactive_tail = Some(handle);
    }

    fn unlink_inactive(&mut self, handle: VQueueHandle) {
        let Some(links) = self.slab[handle].inactive.take() else {
            return;
        };
        if let Some(prev) = links.prev {
            self.slab[prev].inactive.as_mut().unwrap().next = links.next;
        } else {
            self.inactive_head = links.next;
        }
        if let Some(next) = links.next {
            self.slab[next].inactive.as_mut().unwrap().prev = links.prev;
        } else {
            self.inactive_tail = links.prev;
        }
    }

    pub fn len(&self) -> usize {
        self.slab.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slab.is_empty()
    }

    pub(super) fn defer_purge(&mut self, handle: VQueueHandle) {
        self.unlink_inactive(handle);
        let slot = self.slab.get(handle).expect("cached vqueue has a slot");
        let removed = self.queues.remove(slot.vqueue_id());
        debug_assert_eq!(removed, Some(handle));
        self.pending_purges.push(handle);
    }

    /// Evicts pending purges, then the oldest inactive queues until occupancy
    /// reaches target capacity or no inactive queues remain. Never scans active
    /// queues, even when they exceed the target.
    /// Must be called only after the scheduler has handled all events emitted by
    /// the batch because those events refer to cache handles. Returns the total
    /// number of evicted entries.
    pub fn try_compact(&mut self) -> usize {
        let mut evicted = 0;
        for handle in self.pending_purges.drain(..) {
            evicted += usize::from(self.slab.remove(handle).is_some());
        }

        let excess = self.slab.len().saturating_sub(self.target_capacity);
        for _ in 0..excess {
            let Some(handle) = self.inactive_head else {
                break;
            };
            self.unlink_inactive(handle);
            let slot = self
                .slab
                .remove(handle)
                .expect("inactive vqueue has a slot");
            debug_assert!(!slot.meta.is_active());
            let removed = self.queues.remove(&slot.qid);
            debug_assert_eq!(removed, Some(handle));
            evicted += 1;
        }
        if evicted > 0 {
            trace!("vqueue cache compaction freed {evicted} entries");
        }

        evicted
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_empty(target_capacity: usize) -> Self {
        Self {
            slab: SlotMap::with_capacity_and_key(target_capacity),
            queues: HashMap::with_capacity(target_capacity),
            pending_purges: Vec::new(),
            target_capacity,
            inactive_head: None,
            inactive_tail: None,
        }
    }

    /// Initializes the vqueue cache by loading all active vqueues into the cache.
    ///
    /// `target_capacity` is the soft cap that drives compaction; the cache will
    /// still grow past it if compaction frees nothing.
    ///
    /// Subsequent metadata mutations maintain inactive-list membership through
    /// [`Self::update_meta`].
    pub async fn create<S: ScanVQueueTable + Send + Sync + 'static>(
        storage: S,
        target_capacity: usize,
    ) -> Result<Self> {
        let handle: JoinHandle<Result<_>> = tokio::task::spawn_blocking({
            move || {
                // Allocation doesn't bump the RSS until we write to the allocated pages.
                let mut slab = SlotMap::with_capacity_and_key(target_capacity);
                let mut queues = HashMap::with_capacity(target_capacity);
                // find and load all active vqueues.
                storage.scan_active_vqueues(|qid, meta| {
                    let key = slab.insert(Slot {
                        qid: qid.clone(),
                        meta,
                        inactive: None,
                    });
                    // SAFETY: at batch load time we are guaranteed to observe every vqueue id only once.
                    unsafe { queues.insert_unique_unchecked(qid, key) };
                })?;
                Ok((slab, queues))
            }
        });

        let (slab, queues) = handle
            .await
            .map_err(|e| StorageError::Generic(e.into()))??;

        Ok(Self {
            slab,
            queues,
            pending_purges: Vec::new(),
            target_capacity,
            inactive_head: None,
            inactive_tail: None,
        })
    }

    pub async fn load<S: ReadVQueueTable>(
        &mut self,
        storage: &mut S,
        qid: &VQueueId,
    ) -> Result<Option<VQueueHandle>> {
        if let Some(handle) = self.queues.get(qid) {
            return Ok(Some(*handle));
        }

        // Not in cache; consult storage.
        match storage.get_vqueue(qid).await? {
            None => Ok(None),
            Some(meta) => Ok(Some(self.insert(qid.clone(), meta))),
        }
    }

    /// Inserts metadata and records eviction eligibility without evicting anything.
    /// Compaction only runs via [`Self::try_compact`] at the batch boundary.
    pub(super) fn insert(&mut self, qid: VQueueId, meta: VQueueMeta) -> VQueueHandle {
        let is_active = meta.is_active();
        let key = self.slab.insert(Slot {
            qid: qid.clone(),
            meta,
            inactive: None,
        });
        self.queues.insert(qid, key);
        if !is_active {
            self.link_inactive(key);
        }
        key
    }

    pub fn report(&self) {
        debug!(
            "VQueues Cache Report: vqueues_cached={}, cached_mem={}bytes",
            self.queues.len(),
            self.queues.allocation_size(),
        );
        for (qid, meta) in self.queues.iter() {
            trace!("[{qid:?}]: {meta:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use restate_clock::time::MillisSinceEpoch;
    use restate_limiter::LimitKey;
    use restate_storage_api::vqueue_table::Stage;
    use restate_storage_api::vqueue_table::metadata::{Action, MoveMetrics, Update, VQueueLink};
    use restate_types::clock::UniqueTimestamp;

    use super::*;

    fn ts(unix_ms: u64) -> UniqueTimestamp {
        UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(unix_ms))
    }

    fn empty_meta(at: UniqueTimestamp) -> VQueueMeta {
        VQueueMeta::new(at, None, LimitKey::None, VQueueLink::None)
    }

    /// Bumps inbox count so `is_active()` returns true.
    fn enqueue_to_inbox(meta: &mut VQueueMeta, at: UniqueTimestamp) {
        let metrics = MoveMetrics {
            last_transition_at: at,
            has_started: false,
            first_runnable_at: at.to_unix_millis(),
            scheduler_wait_stats: None,
        };
        meta.apply_update(&Update::new(
            at,
            Action::Move {
                prev_stage: None,
                next_stage: Stage::Inbox,
                metrics,
            },
        ));
    }

    #[test]
    fn compact_evicts_inactive_and_keeps_active() {
        let now = ts(1_744_000_000_000);
        let mut cache = VQueuesMetaCache::new_empty(1);

        let qid_active = VQueueId::custom(1, "alive");
        let qid_inactive = VQueueId::custom(2, "dormant");
        let qid_empty_paused = VQueueId::custom(3, "paused");

        // Active: has an inbox entry.
        let mut active_meta = empty_meta(now);
        enqueue_to_inbox(&mut active_meta, now);
        let h_active = cache.insert(qid_active.clone(), active_meta);

        // Inactive: brand-new meta has no entries.
        let h_inactive = cache.insert(qid_inactive.clone(), empty_meta(now));

        // Paused queue with empty inbox is also !is_active.
        let mut paused_meta = empty_meta(now);
        paused_meta.apply_update(&Update::new(now, Action::PauseVQueue {}));
        let h_paused = cache.insert(qid_empty_paused.clone(), paused_meta);

        assert_eq!(cache.len(), 3);

        let evicted = cache.try_compact();

        assert_eq!(evicted, 2);
        assert_eq!(cache.len(), 1);

        // Active stayed; inactive ones gone from both maps.
        assert!(cache.get(h_active).is_some());
        assert!(cache.get(h_inactive).is_none());
        assert!(cache.get(h_paused).is_none());
        assert_eq!(cache.view().handle_for(&qid_active), Some(h_active));
        assert_eq!(cache.view().handle_for(&qid_inactive), None);
        assert_eq!(cache.view().handle_for(&qid_empty_paused), None);
    }

    #[test]
    fn compact_on_empty_cache_is_noop() {
        let mut cache = VQueuesMetaCache::new_empty(1024);
        assert_eq!(cache.try_compact(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn compact_on_all_active_evicts_nothing() {
        let now = ts(1_744_000_000_000);
        let mut cache = VQueuesMetaCache::new_empty(2);
        for i in 0..5 {
            let qid = VQueueId::custom(i, "q");
            let mut meta = empty_meta(now);
            enqueue_to_inbox(&mut meta, now);
            cache.insert(qid, meta);
        }
        assert_eq!(cache.try_compact(), 0);
        assert_eq!(cache.len(), 5);
        assert!(cache.inactive_head.is_none());
        assert!(cache.inactive_tail.is_none());

        // When active queues alone exceed the target, remove every inactive
        // entry in this call, including more than the former per-batch cap.
        for i in 0..512 {
            cache.insert(VQueueId::custom(i, "dormant"), empty_meta(now));
        }
        assert_eq!(cache.try_compact(), 512);
        assert_eq!(cache.len(), 5);
        assert_inactive_list(&cache);

        // Becoming inactive above target is sufficient to trigger eviction;
        // no subsequent insertion or full sweep is necessary.
        let qid = VQueueId::custom(0, "q");
        let handle = cache.view().handle_for(&qid).unwrap();
        cache.update_meta(handle, |_, meta| {
            meta.apply_update(&Update::new(now, Action::PauseVQueue {}));
        });
        assert_eq!(cache.try_compact(), 1);
        assert_eq!(cache.len(), 4);
        assert!(cache.get(handle).is_none());
        assert!(cache.view().handle_for(&qid).is_none());
        assert_inactive_list(&cache);
    }

    #[test]
    fn eviction_removes_oldest_inactive_until_target() {
        let now = ts(1_744_000_000_000);
        let mut cache = VQueuesMetaCache::new_empty(3);

        // Fill with 3 inactive entries.
        for i in 0..3 {
            cache.insert(VQueueId::custom(i, "stale"), empty_meta(now));
        }
        assert_eq!(cache.len(), 3);

        // Inactive entries remain cached at the target.
        assert_eq!(cache.try_compact(), 0);
        assert_eq!(cache.len(), 3);

        let mut active_meta = empty_meta(now);
        enqueue_to_inbox(&mut active_meta, now);
        cache.insert(VQueueId::custom(99, "fresh"), active_meta);

        assert_eq!(cache.try_compact(), 1);
        assert_eq!(cache.len(), 3);
        assert!(
            cache
                .view()
                .handle_for(&VQueueId::custom(0, "stale"))
                .is_none()
        );
        for i in 1..3 {
            assert!(
                cache
                    .view()
                    .handle_for(&VQueueId::custom(i, "stale"))
                    .is_some()
            );
        }
        assert!(
            cache
                .view()
                .handle_for(&VQueueId::custom(99, "fresh"))
                .is_some()
        );

        let handles: Vec<_> = (0..512)
            .map(|i| cache.insert(VQueueId::custom(i + 100, "stale"), empty_meta(now)))
            .collect();
        assert_eq!(cache.try_compact(), 512);
        assert_eq!(cache.len(), 3);
        // Keep the active queue and only the two newest inactive entries.
        for (i, handle) in handles.into_iter().enumerate() {
            assert_eq!(cache.get(handle).is_some(), i >= 510);
        }
        assert!(
            cache
                .view()
                .handle_for(&VQueueId::custom(99, "fresh"))
                .is_some()
        );
        assert_eq!(cache.try_compact(), 0);
        assert_inactive_list(&cache);
    }

    #[test]
    fn pending_purges_preserve_recreated_queue_and_target() {
        let now = ts(1_744_000_000_000);
        let mut cache = VQueuesMetaCache::new_empty(2);

        let purged_qid = VQueueId::custom(1, "purged");
        let purged = cache.insert(purged_qid.clone(), empty_meta(now));
        let retained = cache.insert(VQueueId::custom(2, "retained"), empty_meta(now));
        let mut active_meta = empty_meta(now);
        enqueue_to_inbox(&mut active_meta, now);
        let active = cache.insert(VQueueId::custom(3, "active"), active_meta);
        cache.defer_purge(purged);
        assert_eq!(cache.try_compact(), 1);

        assert_eq!(cache.len(), 2);
        assert!(cache.get(purged).is_none());
        assert!(cache.view().handle_for(&purged_qid).is_none());
        assert!(cache.get(retained).is_some());
        assert!(cache.get(active).is_some());
        assert_inactive_list(&cache);

        // Purging and recreating an ID in the same batch must not let eviction
        // of the old handle remove the new mapping.
        cache.defer_purge(retained);
        let replacement_qid = VQueueId::custom(2, "retained");
        let mut meta = empty_meta(now);
        enqueue_to_inbox(&mut meta, now);
        let replacement = cache.insert(replacement_qid.clone(), meta);
        assert_ne!(replacement, retained);
        assert!(cache.get(retained).is_some());
        assert_eq!(cache.try_compact(), 1);
        assert!(cache.get(retained).is_none());
        assert_eq!(cache.view().handle_for(&replacement_qid), Some(replacement));
        assert!(cache.get(replacement).is_some());
        assert_inactive_list(&cache);
    }

    #[test]
    fn insert_grows_past_capacity_when_nothing_to_evict() {
        let now = ts(1_744_000_000_000);
        let mut cache = VQueuesMetaCache::new_empty(2);

        // Two active entries fill the cap.
        for i in 0..2 {
            let mut meta = empty_meta(now);
            enqueue_to_inbox(&mut meta, now);
            cache.insert(VQueueId::custom(i, "active"), meta);
        }
        assert_eq!(cache.len(), 2);

        // Third insert: compact attempts but frees nothing; cache grows.
        let mut meta = empty_meta(now);
        enqueue_to_inbox(&mut meta, now);
        cache.insert(VQueueId::custom(2, "active"), meta);

        assert_eq!(cache.len(), 3);
        assert_eq!(cache.try_compact(), 0);
    }

    fn assert_inactive_list(cache: &VQueuesMetaCache) {
        let mut cursor = cache.inactive_head;
        let mut prev = None;
        let mut visited = Vec::new();
        while let Some(handle) = cursor {
            assert!(!visited.contains(&handle), "inactive list contains a cycle");
            let slot = &cache.slab[handle];
            assert!(!slot.meta.is_active());
            assert_eq!(cache.queues.get(&slot.qid), Some(&handle));
            let links = slot.inactive.unwrap();
            assert_eq!(links.prev, prev);
            visited.push(handle);
            prev = Some(handle);
            cursor = links.next;
        }
        assert_eq!(prev, cache.inactive_tail);
        for (handle, slot) in &cache.slab {
            let pending_purge = cache.pending_purges.contains(&handle);
            assert_eq!(
                slot.inactive.is_some(),
                !slot.meta.is_active() && !pending_purge
            );
            assert_eq!(slot.inactive.is_some(), visited.contains(&handle));
        }
    }

    #[test]
    fn activity_transitions_unlink_and_relink_without_invalidating_handles() {
        let now = ts(1_744_000_000_000);
        let mut cache = VQueuesMetaCache::new_empty(0);
        let mut handles = Vec::new();
        for i in 0..4 {
            handles.push(cache.insert(VQueueId::custom(i, "q"), empty_meta(now)));
        }
        assert_inactive_list(&cache);

        // Remove middle, head, tail, then singleton by reactivating metadata.
        for i in [1, 0, 3, 2] {
            let handle = handles[i];
            cache.update_meta(handle, |_, meta| enqueue_to_inbox(meta, now));
            assert_inactive_list(&cache);
            assert_eq!(
                cache.view().handle_for(cache.slab[handle].vqueue_id()),
                Some(handle)
            );
        }
        assert_eq!(cache.try_compact(), 0);

        // Pause and resume repeatedly within a batch. Neither operation should
        // evict the slot, and unchanged activity must not duplicate membership.
        for handle in &handles {
            for action in [
                Action::PauseVQueue {},
                Action::PauseVQueue {},
                Action::ResumeVQueue {},
                Action::PauseVQueue {},
                Action::ResumeVQueue {},
            ] {
                cache.update_meta(*handle, |_, meta| {
                    meta.apply_update(&Update::new(now, action));
                });
                assert!(cache.get(*handle).is_some());
                assert_inactive_list(&cache);
            }
        }
        assert_eq!(cache.try_compact(), 0);

        // Exercise unlinking pending purges at every list position too.
        for handle in &handles {
            cache.update_meta(*handle, |_, meta| {
                meta.apply_update(&Update::new(now, Action::PauseVQueue {}));
            });
        }
        for i in [1, 0, 3, 2] {
            cache.defer_purge(handles[i]);
            assert_inactive_list(&cache);
        }
        assert_eq!(cache.try_compact(), 4);
        assert!(cache.is_empty());
        assert_inactive_list(&cache);

        // Slot reuse must start with fresh links despite the old generation.
        let new_handle = cache.insert(VQueueId::custom(0, "q"), empty_meta(now));
        assert!(handles.iter().all(|handle| cache.get(*handle).is_none()));
        assert_inactive_list(&cache);
        assert_eq!(cache.try_compact(), 1);
        assert!(cache.get(new_handle).is_none());
        assert_inactive_list(&cache);
    }
}
