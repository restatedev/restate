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

use bytes::Bytes;
use moka::{
    ops::compute::Op,
    policy::EvictionPolicy,
    sync::{Cache, CacheBuilder},
};

use crate::storage::PolyBytes;

use super::{LogletId, LogletOffset, Record, SequenceNumber};

/// Unique record key across different loglets.
type RecordKey = (LogletId, LogletOffset);

#[derive(Clone)]
struct RecordCacheEntry {
    record: Record,
    weight: u32,
}

/// A a simple LRU-based record cache.
///
/// This can be safely shared between all ReplicatedLoglet(s) and the LocalSequencers or the
/// RemoteSequencers
#[derive(Clone)]
pub struct RecordCache {
    inner: Option<Cache<RecordKey, RecordCacheEntry, ahash::RandomState>>,
}

impl RecordCache {
    /// Creates a new instance of RecordCache. If memory budget is 0
    /// cache will be disabled
    pub fn new(memory_budget_bytes: usize) -> Self {
        let inner = if memory_budget_bytes > 0 {
            Some(
                CacheBuilder::default()
                    .name("ReplicatedLogRecordCache")
                    .weigher(|_, entry: &RecordCacheEntry| entry.weight)
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
                    if !Self::fits(inner, record) {
                        return Op::Nop;
                    }

                    // ensure the cache entry does not retain a much larger buffer
                    return Op::Put(Self::cache_owned(record));
                };

                match (existing.value().record.body(), record.body()) {
                    (PolyBytes::Bytes(_), PolyBytes::Bytes(_)) => Op::Nop,
                    (PolyBytes::Bytes(_), PolyBytes::Typed(_)) => {
                        Op::Put(Self::cache_owned(record))
                    }
                    (PolyBytes::Bytes(_), PolyBytes::Both(_, _)) => {
                        // we only need to cache the typed value, let's repackage it.
                        Op::Put(Self::cache_owned(record))
                    }
                    // Shouldn't happen (we only cache Typed or Bytes), but let's handle it anyway.
                    (PolyBytes::Both(typed, _), _) =>
                    // repackage the existing value into Typed only
                    {
                        Op::Put(RecordCacheEntry {
                            record: Record::from_parts(
                                existing.value().record.created_at(),
                                existing.value().record.keys().clone(),
                                PolyBytes::Typed(Arc::clone(typed)),
                            ),
                            weight: existing.value().weight,
                        })
                    }
                    (PolyBytes::Typed(_), _) => Op::Nop,
                }
            });
    }

    fn weight(record: &Record) -> u32 {
        record
            .estimated_encode_size()
            .try_into()
            .unwrap_or(u32::MAX)
    }

    fn fits(
        inner: &Cache<RecordKey, RecordCacheEntry, ahash::RandomState>,
        record: &Record,
    ) -> bool {
        inner
            .policy()
            .max_capacity()
            .is_none_or(|max_capacity| u64::from(Self::weight(record)) <= max_capacity)
    }

    /// Creates the representation retained by the cache.
    ///
    /// A [`Bytes`] value can be a small slice of a much larger allocation. Copy raw bodies when
    /// admitting a record so the cache capacity accounts for the memory it actually retains.
    /// Typed values are already reference counted, and any `Both` values will be reduced to only
    /// the typed representation. The weight is retained from the original record because a
    /// `Typed` value cannot estimate its encoded size.
    fn cache_owned(record: &Record) -> RecordCacheEntry {
        let body = match record.body() {
            PolyBytes::Bytes(bytes) => PolyBytes::Bytes(Bytes::copy_from_slice(bytes)),
            PolyBytes::Typed(typed) => PolyBytes::Typed(Arc::clone(typed)),
            // Shouldn't happen (we only cache Typed or Bytes), but handle it anyway:
            PolyBytes::Both(typed, _) => PolyBytes::Typed(Arc::clone(typed)),
        };

        RecordCacheEntry {
            record: Record::from_parts(record.created_at(), record.keys().clone(), body),
            weight: Self::weight(record),
        }
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

        inner.get(&(loglet_id, offset)).map(|entry| entry.record)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use bytes::Bytes;

    use super::{LogletId, LogletOffset, PolyBytes, Record, RecordCache};
    use crate::{logs::Keys, time::NanosSinceEpoch};

    const LOGLET_ID: LogletId = LogletId::new_unchecked(1);
    const OFFSET: LogletOffset = LogletOffset::new(1);

    struct TrackingBacking {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for TrackingBacking {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for TrackingBacking {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn cache_miss_copies_raw_body_without_retaining_its_backing_allocation() {
        let dropped = Arc::new(AtomicBool::new(false));
        let backing = Bytes::from_owner(TrackingBacking {
            bytes: vec![42; 128 * 1024],
            dropped: Arc::clone(&dropped),
        });
        let body = backing.slice(64 * 1024..65 * 1024);
        let record = Record::from_parts(
            NanosSinceEpoch::now(),
            Keys::None,
            PolyBytes::Bytes(body.clone()),
        );
        let cache = RecordCache::new(2 * 1024);

        cache.add(LOGLET_ID, OFFSET, &record);

        drop(record);
        drop(body);
        drop(backing);

        assert!(dropped.load(Ordering::Relaxed));

        let cached = cache.get(LOGLET_ID, OFFSET).unwrap();
        let PolyBytes::Bytes(cached_body) = cached.body() else {
            panic!("raw record should remain raw in the cache");
        };

        assert_eq!(cached_body.as_ref(), &[42; 1024]);
    }

    #[test]
    fn cache_miss_retains_typed_values_without_raw_bytes() {
        let typed = Arc::new("typed body".to_owned());
        let typed_record = Record::from_parts(
            NanosSinceEpoch::now(),
            Keys::None,
            PolyBytes::Typed(typed.clone()),
        );
        let typed_cache = RecordCache::new(4 * 1024);

        typed_cache.add(LOGLET_ID, OFFSET, &typed_record);

        assert!(Arc::ptr_eq(
            &typed_cache
                .get(LOGLET_ID, OFFSET)
                .unwrap()
                .decode_arc::<String>()
                .unwrap(),
            &typed,
        ));

        let both_cache = RecordCache::new(16 * 1024);
        both_cache.add(
            LOGLET_ID,
            OFFSET,
            &Record::from_parts(
                NanosSinceEpoch::now(),
                Keys::None,
                PolyBytes::Both(typed.clone(), Bytes::from(vec![42; 8 * 1024])),
            ),
        );

        let cached = both_cache.get(LOGLET_ID, OFFSET).unwrap();
        assert_matches!(cached.body(), PolyBytes::Typed(_));
        assert!(Arc::ptr_eq(&cached.decode_arc::<String>().unwrap(), &typed,));
    }

    #[test]
    fn cache_miss_detaches_both_body_and_preserves_its_weight() {
        let dropped = Arc::new(AtomicBool::new(false));
        let backing = Bytes::from_owner(TrackingBacking {
            bytes: vec![42; 128 * 1024],
            dropped: Arc::clone(&dropped),
        });
        let body = backing.slice(64 * 1024..128 * 1024);
        let typed = Arc::new("typed body".to_owned());
        let record = Record::from_parts(
            NanosSinceEpoch::now(),
            Keys::None,
            PolyBytes::Both(typed.clone(), body.clone()),
        );
        let expected_weight = u64::from(RecordCache::weight(&record));
        let cache = RecordCache::new(128 * 1024);

        cache.add(LOGLET_ID, OFFSET, &record);

        drop(record);
        drop(body);
        drop(backing);

        assert!(dropped.load(Ordering::Relaxed));

        let cached = cache.get(LOGLET_ID, OFFSET).unwrap();
        assert!(matches!(cached.body(), PolyBytes::Typed(_)));
        assert!(Arc::ptr_eq(&cached.decode_arc::<String>().unwrap(), &typed));

        let inner = cache.inner.as_ref().unwrap();
        inner.run_pending_tasks();
        assert_eq!(inner.weighted_size(), expected_weight);
    }

    #[test]
    fn raw_entry_replaced_with_both_preserves_its_weight() {
        let cache = RecordCache::new(128 * 1024);
        cache.add(
            LOGLET_ID,
            OFFSET,
            &Record::from_parts(
                NanosSinceEpoch::now(),
                Keys::None,
                PolyBytes::Bytes(Bytes::from_static(b"raw body")),
            ),
        );

        let dropped = Arc::new(AtomicBool::new(false));
        let backing = Bytes::from_owner(TrackingBacking {
            bytes: vec![42; 128 * 1024],
            dropped: Arc::clone(&dropped),
        });
        let body = backing.slice(64 * 1024..128 * 1024);
        let typed = Arc::new("typed body".to_owned());
        let record = Record::from_parts(
            NanosSinceEpoch::now(),
            Keys::None,
            PolyBytes::Both(typed.clone(), body.clone()),
        );
        let expected_weight = u64::from(RecordCache::weight(&record));

        cache.add(LOGLET_ID, OFFSET, &record);

        drop(record);
        drop(body);
        drop(backing);

        assert!(dropped.load(Ordering::Relaxed));

        let cached = cache.get(LOGLET_ID, OFFSET).unwrap();
        assert_matches!(cached.body(), PolyBytes::Typed(_));
        assert!(Arc::ptr_eq(&cached.decode_arc::<String>().unwrap(), &typed));

        let inner = cache.inner.as_ref().unwrap();
        inner.run_pending_tasks();
        assert_eq!(inner.weighted_size(), expected_weight);
    }

    #[test]
    fn duplicate_raw_record_does_not_replace_cached_record() {
        let cache = RecordCache::new(4 * 1024);
        let first = Record::from_parts(
            NanosSinceEpoch::now(),
            Keys::None,
            PolyBytes::Bytes(Bytes::from_static(b"first")),
        );
        let duplicate = Record::from_parts(
            NanosSinceEpoch::now(),
            Keys::None,
            PolyBytes::Bytes(Bytes::from_static(b"duplicate")),
        );

        cache.add(LOGLET_ID, OFFSET, &first);
        let first_cached = cache.get(LOGLET_ID, OFFSET).unwrap();
        cache.add(LOGLET_ID, OFFSET, &duplicate);
        let cached = cache.get(LOGLET_ID, OFFSET).unwrap();

        assert_matches!(cached.body(), PolyBytes::Bytes(bytes) if bytes.as_ref() == b"first");
        assert_matches!(
            (first_cached.body(), cached.body()),
            (PolyBytes::Bytes(first), PolyBytes::Bytes(cached)) if first.as_ptr() == cached.as_ptr()
        );
    }

    #[test]
    fn oversized_raw_record_is_not_cached() {
        let cache = RecordCache::new(1024);
        let record = Record::from_parts(
            NanosSinceEpoch::now(),
            Keys::None,
            PolyBytes::Bytes(Bytes::from(vec![42; 1024])),
        );

        cache.add(LOGLET_ID, OFFSET, &record);

        assert!(cache.get(LOGLET_ID, OFFSET).is_none());
    }
}
