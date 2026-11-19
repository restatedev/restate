// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::compaction_filter::CompactionFilter;
use rocksdb::compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory};

use crate::keys::KeyKind;

pub struct PartitionCompactionFactory;

#[derive(Default)]
pub struct Compactor;

impl CompactionFilterFactory for PartitionCompactionFactory {
    type Filter = Compactor;

    fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
        Compactor
    }

    fn name(&self) -> &std::ffi::CStr {
        c"PartitionCompactionFactory"
    }
}

impl CompactionFilter for Compactor {
    fn filter(&mut self, level: u32, key: &[u8], value: &[u8]) -> rocksdb::CompactionDecision {
        // Every table's cleanup is dispatched on the key kind, next to its merge
        // operator, so this factory stays the single one installed per partition.
        KeyKind::compaction_filter(level, key, value)
    }

    fn name(&self) -> &std::ffi::CStr {
        c"PartitionCompactor"
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes};
    use rocksdb::CompactionDecision;
    use rocksdb::{BottommostLevelCompaction, CompactOptions, DB, Options};
    use zerocopy::IntoBytes;

    use restate_clock::WallClock;
    use restate_storage_api::vqueue_table::{Stage, Status};
    use restate_types::blob::{BlobId, BlobKind};
    use restate_types::clock::UniqueTimestamp;
    use restate_types::sharding::PartitionId;
    use restate_util_string::ReString;

    use crate::blob_table::{BlobHeader, BlobKey};
    use crate::job::{MigrationJob, MigrationJobId, MigrationJobStatus};
    use crate::keys::FixedLengthTableKey;
    use crate::registry::Registry;
    use crate::registry::records::RecordKey;
    use crate::stats::{AggregateKeyPrefix, StatKind};
    use crate::walk::{Source, WalkKind, Walks};

    use super::*;

    #[test]
    fn stats_filter_removes_zeroes_and_keeps_malformed_data() {
        let mut compactor = Compactor;
        let mut value = vec![Stage::Inbox as u8, Status::New as u8];
        value.put_u64(1);

        // The aggregation kind follows the identity and selects the value codec.
        let mut key = AggregateKeyPrefix::new(PartitionId::from(8), 70_000)
            .as_bytes()
            .to_vec();
        key.push(StatKind::StageStatusBucketedGauge as u8);
        assert!(matches!(
            compactor.filter(0, &key, &[]),
            CompactionDecision::Remove
        ));
        assert!(matches!(
            compactor.filter(0, &key, &value),
            CompactionDecision::Keep
        ));
        assert!(matches!(
            compactor.filter(0, &key, &[0]),
            CompactionDecision::Keep
        ));
        key[AggregateKeyPrefix::SERIALIZED_LENGTH] = u8::MAX;
        assert!(matches!(
            compactor.filter(0, &key, &[]),
            CompactionDecision::Keep
        ));
    }

    #[test]
    fn rocksdb_merges_stats_and_reclaims_materialized_zeroes() {
        let directory = restate_test_util::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("stats", KeyKind::full_merge, KeyKind::partial_merge);
        options.set_compaction_filter_factory(PartitionCompactionFactory);
        let db = DB::open(&options, directory.path()).unwrap();
        let mut compact = CompactOptions::default();
        compact.set_bottommost_level_compaction(BottommostLevelCompaction::Force);

        // Blob cleanup and aggregate cleanup must share the installed factory.
        let blob_keys = [0, 1, 2].map(|seq| {
            BlobKey {
                blob_id: BlobId::new(UniqueTimestamp::MIN, seq, 8, [0; 16]),
            }
            .to_bytes()
        });
        let mut live_blob = Vec::new();
        BlobHeader::new(BlobKind::Journal, 1).encode(&mut live_blob);
        live_blob.extend_from_slice(b"payload");
        let mut dead_blob = Vec::new();
        BlobHeader::new(BlobKind::Journal, 0).encode(&mut dead_blob);
        db.put(blob_keys[0], dead_blob.as_slice()).unwrap();
        db.put(blob_keys[1], live_blob.as_slice()).unwrap();
        db.put(blob_keys[2], [0]).unwrap();

        for (kind, bucket) in [
            (StatKind::AggregatedGauge, &b""[..]),
            (StatKind::StageBucketedGauge, &b"i"[..]),
            (StatKind::StageStatusBucketedGauge, &b"i\x01"[..]),
        ] {
            let mut key = AggregateKeyPrefix::new(PartitionId::from(8), 70_000)
                .as_bytes()
                .to_vec();
            key.push(kind as u8);
            let operand = |delta: i64| {
                let mut bytes = bucket.to_vec();
                bytes.put_i64(delta);
                bytes
            };
            db.merge(key.as_slice(), operand(3)).unwrap();
            db.flush().unwrap();
            db.merge(key.as_slice(), operand(-1)).unwrap();
            db.merge(key.as_slice(), operand(1)).unwrap();
            db.flush().unwrap();
            db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact);

            assert_eq!(db.get(key.as_slice()).unwrap().unwrap(), operand(3));
            db.merge(key.as_slice(), operand(-3)).unwrap();
            db.flush().unwrap();
            // One pass may materialize the merged zero; a later pass filters it.
            for _ in 0..2 {
                db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact);
            }
            assert!(db.get(key.as_slice()).unwrap().is_none());

            // A reclaimed zero must behave like the default base for new updates.
            db.merge(key.as_slice(), operand(1)).unwrap();
            assert_eq!(db.get(key.as_slice()).unwrap().unwrap(), operand(1));
        }

        // A retired statistics key with pending operands must not fail compaction:
        // its merge keeps whatever is stored, nothing here, and the key stays until
        // a later commit's open-time delete removes it.
        #[allow(deprecated)]
        let mut retired = KeyKind::LegacyStats.as_bytes().to_vec();
        retired.extend_from_slice(&[0, 0, 0, 8, 0, 12, 3, 0]);
        retired.extend_from_slice(b"payload");
        db.merge(&retired, [0, 0, 0, 0, 0, 0, 0, 3]).unwrap();
        db.flush().unwrap();
        db.merge(&retired, [0, 0, 0, 0, 0, 0, 0, 1]).unwrap();
        db.flush().unwrap();
        db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact);
        assert_eq!(
            db.get(&retired).unwrap().unwrap(),
            b"",
            "retired_operands_merge_to_nothing_and_survive_compaction"
        );
        assert!(db.get(blob_keys[0]).unwrap().is_none());
        assert_eq!(db.get(blob_keys[1]).unwrap().unwrap(), live_blob);
        assert_eq!(db.get(blob_keys[2]).unwrap().unwrap(), [0]);
    }

    /// An expired finished job is removed by compaction with every older
    /// version of its record, here an unfinished one in the bottommost level that
    /// the removing compaction doesn't reach: RocksDB writes the removal as a
    /// tombstone. Unfinished and unexpired jobs, the registry and walks stay.
    #[test]
    fn rocksdb_removes_expired_jobs_with_their_older_versions() {
        let directory = restate_test_util::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compaction_filter_factory(PartitionCompactionFactory);
        // Static level sizes: a range compaction then moves level 0 down one
        // level at a time, so the expired job is filtered before its older
        // version is in the same compaction.
        options.set_level_compaction_dynamic_level_bytes(false);
        options.set_disable_auto_compactions(true);
        let db = DB::open(&options, directory.path()).unwrap();
        let level = |level: usize| {
            db.property_int_value(&format!("rocksdb.num-files-at-level{level}"))
                .unwrap()
                .unwrap()
        };

        let partition = PartitionId::from(1);
        let now = WallClock::now_ms().as_u64();
        let job = |id| {
            MigrationJob::index_backfill(id, ReString::from_static("stat/1"), None, None, None)
        };
        let finished = |id, expires_at| {
            let mut job = job(id);
            job.finish(MigrationJobStatus::Completed, Some(now), expires_at);
            job
        };
        let key = |id| RecordKey::job(partition, MigrationJobId::for_entry(id));

        // Job 66's unfinished record, compacted into the bottommost level.
        db.put(key(66), job(66).encode()).unwrap();
        db.flush().unwrap();
        let mut to_bottom = CompactOptions::default();
        to_bottom.set_change_level(true);
        to_bottom.set_target_level(6);
        db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &to_bottom);
        assert_eq!(level(6), 1);

        // Then: 66 finished and expired, 65 finished and not expired yet, 64
        // unfinished, and the registry with a walk.
        let mut registry = Registry::default();
        let mut walks = Walks::default();
        let walk = walks
            .start(
                &mut registry,
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::new(),
            )
            .unwrap();
        let kept = [
            (RecordKey::registry(partition), registry.encode()),
            (
                RecordKey::walk(partition, walk),
                walks.get(walk).unwrap().encode(),
            ),
            (key(64), job(64).encode()),
            (key(65), finished(65, now + 3_600_000).encode()),
        ];
        for (key, value) in &kept {
            db.put(key, value).unwrap();
        }
        db.put(key(66), finished(66, now - 1).encode()).unwrap();
        db.flush().unwrap();
        let mut compact = CompactOptions::default();
        compact.set_bottommost_level_compaction(BottommostLevelCompaction::Skip);
        db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact);
        assert_eq!(level(0), 0);

        assert_eq!(db.get(key(66)).unwrap(), None, "no version of 66 is left");
        for (key, value) in &kept {
            assert_eq!(db.get(key).unwrap().as_ref(), Some(value), "{key:?}");
        }
    }
}
