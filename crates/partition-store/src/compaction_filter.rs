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
    use bytes::BufMut;
    use rocksdb::CompactionDecision;
    use rocksdb::{BottommostLevelCompaction, CompactOptions, DB, Options};
    use zerocopy::IntoBytes;

    use restate_storage_api::vqueue_table::{Stage, Status};
    use restate_types::blob::{BlobId, BlobKind};
    use restate_types::clock::UniqueTimestamp;
    use restate_types::sharding::PartitionId;

    use crate::blob_table::{BlobHeader, BlobKey};
    use crate::keys::FixedLengthTableKey;
    use crate::stats::{AggregateKeyPrefix, StatKind};

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
        BlobHeader::new(BlobKind::Output, 1).encode(&mut live_blob);
        live_blob.extend_from_slice(b"payload");
        let mut dead_blob = Vec::new();
        BlobHeader::new(BlobKind::Output, 0).encode(&mut dead_blob);
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
            db.merge(&key, operand(3)).unwrap();
            db.flush().unwrap();
            db.merge(&key, operand(-1)).unwrap();
            db.merge(&key, operand(1)).unwrap();
            db.flush().unwrap();
            db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact);

            assert_eq!(db.get(&key).unwrap().unwrap(), operand(3));
            db.merge(&key, operand(-3)).unwrap();
            db.flush().unwrap();
            // One pass may materialize the merged zero; a later pass filters it.
            for _ in 0..2 {
                db.compact_range_opt(None::<&[u8]>, None::<&[u8]>, &compact);
            }
            assert!(db.get(&key).unwrap().is_none());

            // A reclaimed zero must behave like the default base for new updates.
            db.merge(&key, operand(1)).unwrap();
            assert_eq!(db.get(&key).unwrap().unwrap(), operand(1));
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
}
