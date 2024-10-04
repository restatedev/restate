// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use moka::{
    policy::EvictionPolicy,
    sync::{Cache, CacheBuilder},
};
use xxhash_rust::xxh3::Xxh3Builder;

use restate_types::{
    logs::{LogletOffset, Record, SequenceNumber},
    replicated_loglet::ReplicatedLogletId,
};

/// Unique record key across different loglets.
type RecordKey = (ReplicatedLogletId, LogletOffset);

/// A a simple LRU-based record cache.
///
/// This can be safely shared between all ReplicatedLoglet(s) and the LocalSequencers or the
/// RemoteSequencers
#[derive(Clone)]
pub struct RecordCache {
    inner: Cache<RecordKey, Record, Xxh3Builder>,
}

impl RecordCache {
    pub fn new(memory_budget_bytes: usize) -> Self {
        let inner: Cache<RecordKey, Record, _> = CacheBuilder::default()
            .name("ReplicatedLogRecordCache")
            .weigher(|_, record: &Record| {
                (size_of::<RecordKey>() + record.estimated_encode_size())
                    .try_into()
                    .unwrap_or(u32::MAX)
            })
            .max_capacity(memory_budget_bytes.try_into().unwrap_or(u64::MAX))
            .eviction_policy(EvictionPolicy::lru())
            .build_with_hasher(Xxh3Builder::default());

        Self { inner }
    }

    /// Writes a record to cache externally
    pub fn add(&self, loglet_id: ReplicatedLogletId, offset: LogletOffset, record: Record) {
        // self.inner.insert((loglet_id, offset), record);
    }

    /// Extend cache with records
    pub fn extend<I: AsRef<[Record]>>(
        &self,
        loglet_id: ReplicatedLogletId,
        mut first_offset: LogletOffset,
        records: I,
    ) {
        // for record in records.as_ref() {
        //     self.inner.insert((loglet_id, first_offset), record.clone());
        //     first_offset = first_offset.next();
        // }
    }

    /// Get a for given loglet id and offset.
    pub fn get(&self, loglet_id: ReplicatedLogletId, offset: LogletOffset) -> Option<Record> {
        self.inner.get(&(loglet_id, offset))
    }
}
