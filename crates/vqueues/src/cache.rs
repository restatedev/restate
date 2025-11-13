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
use tracing::info;

use restate_storage_api::StorageError;
use restate_storage_api::vqueue_table::metadata::{VQueueMeta, VQueueMetaUpdates, VQueueStatus};
use restate_storage_api::vqueue_table::{ReadVQueueTable, ScanVQueueTable};
use restate_types::vqueue::VQueueId;

use crate::vqueue_config::ConfigPool;

type Result<T> = std::result::Result<T, StorageError>;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Slot {
    Active,
    EmptyCached,
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

    pub fn num_active_vqueues(&self) -> usize {
        self.inner.queues.len()
    }

    pub(crate) fn config_pool(&'a self) -> &'a ConfigPool {
        &self.inner.config
    }

    pub fn get_vqueue(&'a self, qid: &VQueueId) -> Option<&'a VQueueMeta> {
        self.inner.queues.get(qid)
    }

    pub fn iter_vqueues(&'a self) -> impl Iterator<Item = (&'a VQueueId, &'a VQueueMeta)> {
        self.inner.queues.iter()
    }
}

// Needs rewriting after the workload pattern becomes more clear.
#[derive(Default)]
pub struct VQueuesMetaMut {
    config: ConfigPool,
    queues: HashMap<VQueueId, VQueueMeta>,
    // queues we _know_ are empty
    known: HashMap<VQueueId, Slot>,
}

impl VQueuesMetaMut {
    pub fn view(&self) -> VQueuesMeta<'_> {
        VQueuesMeta::new(self)
    }

    pub async fn load_all_active_vqueues<S: ScanVQueueTable>(&mut self, storage: &S) -> Result<()> {
        // find and load all active vqueues.
        storage.scan_active_vqueues(|vqueue_id, meta| {
            self.insert_active_vqueue(vqueue_id, meta);
        })?;

        Ok(())
    }

    fn insert_active_vqueue(&mut self, qid: VQueueId, meta: VQueueMeta) {
        info!("Found active vqueue {qid:?}");
        // is this a vqueue we know about?

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

    async fn load_vqueue<'a, S: ReadVQueueTable>(
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

    /// Returns the status of the vqueue before and after all the updates
    /// in the form of a tuple (before, after).
    pub(crate) async fn apply_updates<S: ReadVQueueTable>(
        &mut self,
        storage: &mut S,
        qid: &VQueueId,
        updates: &VQueueMetaUpdates,
    ) -> Result<(VQueueStatus, VQueueStatus)> {
        // do we have the vqueue?
        let vqueue = self.load_vqueue(storage, qid).await?;

        let before = vqueue.status();
        vqueue.apply_updates(updates)?;
        let after = vqueue.status();

        // todo(asoli): Add compaction logic to remove empty vqueues that has been empty for a while
        // only if the cache hits a certain threshold.

        Ok((before, after))
    }
}
