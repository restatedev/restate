// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use moka::{
    ops::compute::Op,
    policy::EvictionPolicy,
    sync::{Cache, CacheBuilder},
};

use crate::storage::PolyBytes;

use super::{LogletId, LogletOffset, Record, SequenceNumber};

/// Unique record key across different loglets.
type RecordKey = (LogletId, LogletOffset);

/// A a simple LRU-based record cache.
///
/// This can be safely shared between all ReplicatedLoglet(s) and the LocalSequencers or the
/// RemoteSequencers
#[derive(Clone)]
pub struct RecordCache {
    inner: Option<Cache<RecordKey, Record, ahash::RandomState>>,
}

impl RecordCache {
    /// Creates a new instance of RecordCache. If memory budget is 0
    /// cache will be disabled
    pub fn new(memory_budget_bytes: usize) -> Self {
        let inner = if memory_budget_bytes > 0 {
            Some(
                CacheBuilder::default()
                    .name("ReplicatedLogRecordCache")
                    .weigher(|_, record: &Record| {
                        record
                            .estimated_encode_size()
                            .try_into()
                            .unwrap_or(u32::MAX)
                    })
                    .max_capacity(memory_budget_bytes.try_into().unwrap_or(u64::MAX))
                    .eviction_policy(EvictionPolicy::lru())
                    .build_with_hasher(ahash::RandomState::default()),
            )
        } else {
            None
        };

        Self { inner }
    }

    fn insert(&self, loglet_id: LogletId, offset: LogletOffset, record: &Record) {
        let Some(ref inner) = self.inner else {
            return;
        };

        inner
            .entry((loglet_id, offset))
            .and_compute_with(|existing| {
                let Some(existing) = existing else {
                    return Op::Put(record.clone());
                };
                match (existing.value().body(), record.body()) {
                    (PolyBytes::Bytes(_), PolyBytes::Bytes(_)) => Op::Nop,
                    (PolyBytes::Bytes(_), PolyBytes::Typed(_)) => Op::Put(record.clone()),
                    (PolyBytes::Bytes(_), PolyBytes::Both(typed, _)) => {
                        // we only need to cache the typed value, let's repackage it.
                        Op::Put(Record::from_parts(
                            record.created_at(),
                            record.keys().clone(),
                            PolyBytes::Typed(Arc::clone(typed)),
                        ))
                    }
                    // Shouldn't happen (we only cache Typed or Bytes), but let's handle it anyway.
                    (PolyBytes::Both(typed, _), _) =>
                    // repackge the existing value into Typed only
                    {
                        Op::Put(Record::from_parts(
                            existing.value().created_at(),
                            existing.value().keys().clone(),
                            PolyBytes::Typed(Arc::clone(typed)),
                        ))
                    }
                    (PolyBytes::Typed(_), _) => Op::Nop,
                }
            });
    }

    /// Writes a record to cache externally
    pub fn add(&self, loglet_id: LogletId, offset: LogletOffset, record: &Record) {
        self.insert(loglet_id, offset, record);
    }

    /// Removes the record from cache if it exists
    pub fn invalidate_record(&self, loglet_id: LogletId, offset: LogletOffset) {
        let Some(ref inner) = self.inner else {
            return;
        };
        inner.invalidate(&(loglet_id, offset));
    }

    /// Extend cache with records
    pub fn extend<I: AsRef<[Record]>>(
        &self,
        loglet_id: LogletId,
        mut first_offset: LogletOffset,
        records: I,
    ) {
        if self.inner.is_none() {
            return;
        };

        for record in records.as_ref() {
            self.insert(loglet_id, first_offset, record);
            first_offset = first_offset.next();
        }
    }

    /// Get a for given loglet id and offset.
    pub fn get(&self, loglet_id: LogletId, offset: LogletOffset) -> Option<Record> {
        let inner = self.inner.as_ref()?;

        inner.get(&(loglet_id, offset))
    }
}
