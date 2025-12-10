// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use hashbrown::{HashMap, hash_map};
use tracing::{debug, trace};

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{VQueueMeta, VQueueMetaUpdates};
use restate_storage_api::vqueue_table::{ReadVQueueTable, ScanVQueueTable};
use restate_types::vqueue::VQueueId;

use crate::vqueue_config::ConfigPool;

type Result<T> = std::result::Result<T, StorageError>;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Slot {
    Active,
    EmptyCached,
    // reserved for when a vqueue is known to be empty and we removed it from the cache
    #[allow(unused)]
    EmptyNotCached,
}

// A read-only view over the in-memory stash of vqueues.
#[derive(Copy, Clone)]
pub struct VQueuesMeta<'a> {
    inner: &'a VQueuesMetaMut,
}

impl<'a> VQueuesMeta<'a> {
    #[inline]
    fn new(cache: &'a VQueuesMetaMut) -> VQueuesMeta<'a> {
        Self { inner: cache }
    }

    pub(crate) fn config_pool(&'a self) -> &'a ConfigPool {
        &self.inner.config
    }

    pub fn get_vqueue(&'a self, qid: &VQueueId) -> Option<&'a VQueueMeta> {
        self.inner.queues.get(qid)
    }

    pub fn iter_active_vqueues(&'a self) -> impl Iterator<Item = (&'a VQueueId, &'a VQueueMeta)> {
        self.inner
            .queues
            .iter()
            .filter(|(_, meta)| meta.is_active())
    }

    pub fn num_active(&self) -> usize {
        self.inner
            .queues
            .values()
            .filter(|meta| meta.is_active())
            .count()
    }

    pub fn report(&self) {
        self.inner.report();
    }
}

// Needs rewriting after the workload pattern becomes more clear.
#[derive(Default)]
pub struct VQueuesMetaMut {
    config: ConfigPool,
    queues: HashMap<VQueueId, VQueueMeta>,
    known: HashMap<VQueueId, Slot>,
}

impl VQueuesMetaMut {
    pub fn view(&self) -> VQueuesMeta<'_> {
        VQueuesMeta::new(self)
    }

    pub async fn load_all_active_vqueues<S: ScanVQueueTable>(&mut self, storage: &S) -> Result<()> {
        // find and load all active vqueues.
        storage.scan_active_vqueues(|vqueue_id, meta| {
            self.insert(vqueue_id, meta);
        })?;

        Ok(())
    }

    fn insert(&mut self, qid: VQueueId, meta: VQueueMeta) {
        match self.known.get(&qid).copied() {
            Some(Slot::Active | Slot::EmptyCached) => {
                /* do nothing, it's already cached */
                debug_assert!(self.queues.contains_key(&qid));
            }
            None | Some(Slot::EmptyNotCached) => {
                // cache it.
                self.known.insert(
                    qid,
                    if meta.is_empty() {
                        Slot::EmptyCached
                    } else {
                        Slot::Active
                    },
                );
                self.queues.insert(qid, meta);
            }
        }
    }

    async fn load<'a, S: ReadVQueueTable>(
        &'a mut self,
        storage: &mut S,
        qid: &VQueueId,
    ) -> Result<&'a mut VQueueMeta> {
        match self.queues.entry_ref(qid) {
            hash_map::EntryRef::Occupied(entry) => Ok(entry.into_mut()),
            hash_map::EntryRef::Vacant(entry) => {
                // We don't have it in cache, we must check storage.
                match storage.get_vqueue(qid).await? {
                    None => {
                        self.known.insert(*qid, Slot::EmptyCached);
                        Ok(entry.insert(Default::default()))
                    }
                    Some(meta) => {
                        self.known.insert(
                            *qid,
                            if meta.is_empty() {
                                Slot::EmptyCached
                            } else {
                                Slot::Active
                            },
                        );
                        Ok(entry.insert(meta))
                    }
                }
            }
        }
    }

    /// Returns is_active of the vqueue before and after all the updates
    /// in the form of a tuple (before, after).
    pub(crate) async fn apply_updates<S: ReadVQueueTable>(
        &mut self,
        storage: &mut S,
        qid: &VQueueId,
        updates: &VQueueMetaUpdates,
    ) -> Result<(bool, bool)> {
        let vqueue = self.load(storage, qid).await?;
        let before = vqueue.is_active();
        vqueue.apply_updates(updates)?;
        let after = vqueue.is_active();

        // todo(asoli): Add compaction logic to remove empty vqueues that has been empty for a while
        // only if the cache hits a certain threshold.

        Ok((before, after))
    }

    pub fn report(&self) {
        let empty_cached = self
            .known
            .iter()
            .filter(|(_, slot)| **slot == Slot::EmptyCached)
            .count();

        let empty_not_cached = self
            .known
            .iter()
            .filter(|(_, slot)| **slot == Slot::EmptyNotCached)
            .count();

        debug!(
            "VQueues Cache Report: vqueues_cached={}, cached_mem={}bytes, empty_cached={empty_cached}, empty_not_cached={empty_not_cached}",
            self.queues.len(),
            self.queues.allocation_size(),
        );
        for (qid, meta) in self.queues.iter() {
            trace!("[{qid:?}]: {meta:?}");
        }
    }
}
