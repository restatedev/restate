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
use restate_storage_api::vqueue_table::metadata::{self, VQueueMeta};
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
            .filter_map(|(key, Slot { qid, meta })| meta.is_active().then_some((key, qid, meta)))
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

    /// Returns is_active of the vqueue before and after all the updates
    /// in the form of a tuple (before, after).
    pub fn apply_update(&mut self, update: &metadata::Update) -> (bool, bool) {
        let before = self.meta.is_active();
        self.meta.apply_update(update);
        let after = self.meta.is_active();

        (before, after)
    }
}

// Needs rewriting after the workload pattern becomes more clear.
#[derive(Clone)]
pub struct VQueuesMetaCache {
    queues: HashMap<VQueueId, VQueueHandle>,
    slab: SlotMap<VQueueHandle, Slot>,
    /// Soft cap; partition processor triggers `compact()` once `len()` reaches
    /// this number. The slab/hashmap will still grow past this if compaction
    /// frees nothing.
    target_capacity: usize,
}

impl VQueuesMetaCache {
    pub fn view(&self) -> VQueuesMeta<'_> {
        VQueuesMeta::new(self)
    }

    pub fn get(&self, key: VQueueHandle) -> Option<&Slot> {
        self.slab.get(key)
    }

    pub fn get_mut(&mut self, key: VQueueHandle) -> Option<&mut Slot> {
        self.slab.get_mut(key)
    }

    pub fn len(&self) -> usize {
        self.slab.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slab.is_empty()
    }

    /// Sweeps the cache and evicts entries whose vqueues are no longer needed.
    /// A slot is safe to evict when its meta reports `!is_active()`. The scheduler
    /// drops its handle eagerly on the events that flip the meta inactive
    /// (`RemovedFromInbox`, `QueuePaused`), so meta inactivity implies the
    /// scheduler has already released the handle. Returns the number of evicted
    /// entries.
    ///
    /// Triggered automatically by `insert` once occupancy reaches
    /// `target_capacity`, so steady-state operations never pay for compaction.
    fn compact(&mut self) -> usize {
        let mut evicted = 0;
        self.slab.retain(|_handle, slot| {
            if slot.meta.is_active() {
                true
            } else {
                self.queues.remove(&slot.qid);
                evicted += 1;
                false
            }
        });
        evicted
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_empty(target_capacity: usize) -> Self {
        Self {
            slab: SlotMap::with_capacity_and_key(target_capacity),
            queues: HashMap::with_capacity(target_capacity),
            target_capacity,
        }
    }

    /// Initializes the vqueue cache by loading all active vqueues into the cache.
    ///
    /// `target_capacity` is the soft cap that drives compaction; the cache will
    /// still grow past it if compaction frees nothing.
    ///
    /// From this point on, the cache remains in-sync with the storage state by
    /// using the "apply_updates" method.
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
            target_capacity,
        })
    }

    pub async fn load<S: ReadVQueueTable>(
        &mut self,
        storage: &mut S,
        qid: &VQueueId,
    ) -> Result<Option<VQueueHandle>> {
        if self.queues.contains_key(qid) {
            return Ok(self.queues.get(qid).copied());
        }
        // Not in cache; consult storage.
        match storage.get_vqueue(qid).await? {
            None => Ok(None),
            Some(meta) => Ok(Some(self.insert(qid.clone(), meta))),
        }
    }

    /// Inserts a vqueue metadata unconditionally to the cache. The single point
    /// of entry into the cache; runs compaction when occupancy reaches the
    /// configured `target_capacity`.
    pub(super) fn insert(&mut self, qid: VQueueId, meta: VQueueMeta) -> VQueueHandle {
        if self.slab.len() >= self.target_capacity {
            let evicted = self.compact();
            if evicted == 0 {
                tracing::info!(
                    "vqueue cache at {} entries with no inactive queues to evict; cache will grow past target_capacity={}",
                    self.slab.len(),
                    self.target_capacity,
                );
            } else {
                trace!("vqueue cache compaction freed {evicted} entries");
            }
        }
        let key = self.slab.insert(Slot {
            qid: qid.clone(),
            meta,
        });
        self.queues.insert(qid, key);
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
            blocked_on_concurrency_rules_ms: 0,
            blocked_on_invoker_throttling_ms: 0,
            first_runnable_at: at.to_unix_millis(),
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
        // Use a high cap so insert() does not auto-compact during setup.
        let mut cache = VQueuesMetaCache::new_empty(1024);

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

        let evicted = cache.compact();

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
        assert_eq!(cache.compact(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn compact_on_all_active_evicts_nothing() {
        let now = ts(1_744_000_000_000);
        let mut cache = VQueuesMetaCache::new_empty(1024);
        for i in 0..5 {
            let qid = VQueueId::custom(i, "q");
            let mut meta = empty_meta(now);
            enqueue_to_inbox(&mut meta, now);
            cache.insert(qid, meta);
        }
        assert_eq!(cache.compact(), 0);
        assert_eq!(cache.len(), 5);
    }

    #[test]
    fn insert_auto_compacts_when_capacity_hit() {
        let now = ts(1_744_000_000_000);
        // Tiny cap so we can drive the auto-compact path in two steps.
        let mut cache = VQueuesMetaCache::new_empty(3);

        // Fill with 3 inactive entries.
        for i in 0..3 {
            cache.insert(VQueueId::custom(i, "stale"), empty_meta(now));
        }
        assert_eq!(cache.len(), 3);

        // The next insert should observe `len >= target_capacity` and compact
        // the inactive ones before adding the new entry.
        let mut active_meta = empty_meta(now);
        enqueue_to_inbox(&mut active_meta, now);
        cache.insert(VQueueId::custom(99, "fresh"), active_meta);

        assert_eq!(cache.len(), 1);
        assert!(
            cache
                .view()
                .handle_for(&VQueueId::custom(99, "fresh"))
                .is_some()
        );
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
    }
}
