// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hashbrown::{HashMap, hash_map};
use slotmap::SlotMap;
use tokio::task::JoinHandle;
use tracing::{debug, trace};

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{self, VQueueMeta};
use restate_storage_api::vqueue_table::{ReadVQueueTable, ScanVQueueTable};
use restate_types::vqueues::VQueueId;

type Result<T> = std::result::Result<T, StorageError>;

slotmap::new_key_type! { pub struct VQueueCacheKey; }

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

    pub fn get(&self, key: VQueueCacheKey) -> Option<&Slot> {
        self.inner.slab.get(key)
    }

    pub fn get_vqueue(&'a self, qid: &VQueueId) -> Option<&'a VQueueMeta> {
        self.inner
            .queues
            .get(qid)
            .and_then(|key| self.get(*key))
            .map(|slot| &slot.meta)
    }

    pub fn iter_active_vqueues(&'a self) -> impl Iterator<Item = (&'a VQueueId, &'a VQueueMeta)> {
        self.inner
            .slab
            .values()
            .filter_map(|Slot { qid, meta }| meta.is_active().then_some((qid, meta)))
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
    queues: HashMap<VQueueId, VQueueCacheKey>,
    slab: SlotMap<VQueueCacheKey, Slot>,
}

impl VQueuesMetaCache {
    pub fn view(&self) -> VQueuesMeta<'_> {
        VQueuesMeta::new(self)
    }

    pub fn get_mut(&mut self, key: VQueueCacheKey) -> Option<&mut Slot> {
        self.slab.get_mut(key)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn new_empty() -> Self {
        Self {
            slab: Default::default(),
            queues: Default::default(),
        }
    }

    /// Initializes the vqueue cache by loading all active vqueues into the cache.
    ///
    /// From this point on, the cache remains in-sync with the storage state by
    /// using the "apply_updates" method.
    pub async fn create<S: ScanVQueueTable + Send + Sync + 'static>(storage: S) -> Result<Self> {
        let handle: JoinHandle<Result<_>> = tokio::task::spawn_blocking({
            move || {
                let mut slab = SlotMap::default();
                let mut queues = HashMap::default();
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

        Ok(Self { slab, queues })
    }

    pub async fn load<S: ReadVQueueTable>(
        &mut self,
        storage: &mut S,
        qid: &VQueueId,
    ) -> Result<Option<VQueueCacheKey>> {
        Ok(match self.queues.entry_ref(qid) {
            hash_map::EntryRef::Occupied(entry) => Some(*entry.get()),
            hash_map::EntryRef::Vacant(entry) => {
                // We don't have it in cache, we must check storage.
                match storage.get_vqueue(qid).await? {
                    None => None,
                    Some(meta) => {
                        let key = self.slab.insert(Slot {
                            qid: qid.clone(),
                            meta,
                        });
                        entry.insert(key);
                        Some(key)
                    }
                }
            }
        })
    }

    /// Inserts a vqueue metadata unconditionally to the cache.
    pub(super) fn insert(&mut self, qid: VQueueId, meta: VQueueMeta) -> VQueueCacheKey {
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
