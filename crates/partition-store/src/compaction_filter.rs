// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::CompactionDecision;
use rocksdb::compaction_filter::CompactionFilter;
use rocksdb::compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory};
use tracing::warn;

use crate::keys::KeyKind;
use crate::stats::{RegisteredStatKeyPrefix, StatKeyPrefix};

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
    fn filter(&mut self, _level: u32, key: &[u8], value: &[u8]) -> rocksdb::CompactionDecision {
        if key.starts_with(KeyKind::Stats.as_bytes()) {
            // Reclaim materialized zero aggregates. This callback handles stored
            // values; RocksDB keeps unresolved merge operands separately.
            let Ok((prefix, _)) = StatKeyPrefix::decode_prefix(key) else {
                warn!(
                    key = %format_args!("{key:02x?}"),
                    "Failed to decode aggregated statistic key during compaction"
                );
                return CompactionDecision::Keep;
            };

            if prefix.stat_kind().should_filter_value(value) {
                return CompactionDecision::Remove;
            }
        } else if key.starts_with(KeyKind::RegisteredStats.as_bytes()) {
            let Ok((_, kind, _)) = RegisteredStatKeyPrefix::decode(key) else {
                warn!(
                    key = %format_args!("{key:02x?}"),
                    "Failed to decode registered statistic key during compaction"
                );
                return CompactionDecision::Keep;
            };
            if kind.should_filter_value(value) {
                return CompactionDecision::Remove;
            }
        }
        CompactionDecision::Keep
    }

    fn name(&self) -> &std::ffi::CStr {
        c"PartitionCompactor"
    }
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;
    use rocksdb::{BottommostLevelCompaction, CompactOptions, DB, Options};
    use zerocopy::IntoBytes;

    use restate_storage_api::vqueue_table::{Stage, Status};
    use restate_types::sharding::PartitionId;

    use crate::stats::{RegisteredStatKeyPrefix, StatKeyPrefix, StatKind};

    use super::*;

    #[test]
    fn stats_filter_removes_zeroes_and_keeps_malformed_data() {
        let mut key = StatKeyPrefix::new(
            PartitionId::from(8),
            12,
            crate::stats::StatKind::StageStatusBucketedGauge,
        )
        .as_bytes()
        .to_vec();
        let mut compactor = Compactor;
        assert!(matches!(
            compactor.filter(0, &key, &[]),
            CompactionDecision::Remove
        ));

        let mut value = vec![Stage::Inbox as u8, Status::New as u8];
        value.put_u64(1);
        assert!(matches!(
            compactor.filter(0, &key, &value),
            CompactionDecision::Keep
        ));
        assert!(matches!(
            compactor.filter(0, &key, &[0]),
            CompactionDecision::Keep
        ));

        key[8] = u8::MAX;
        assert!(matches!(
            compactor.filter(0, &key, &[]),
            CompactionDecision::Keep
        ));

        // Registered statistics carry the aggregation kind after their identity.
        let mut key = RegisteredStatKeyPrefix::new(PartitionId::from(8), 70_000)
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
        key[RegisteredStatKeyPrefix::SERIALIZED_LENGTH] = u8::MAX;
        assert!(matches!(
            compactor.filter(0, &key, &[]),
            CompactionDecision::Keep
        ));
    }

    #[test]
    fn rocksdb_merges_stats_and_reclaims_materialized_zeroes() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_merge_operator("stats", KeyKind::full_merge, KeyKind::partial_merge);
        options.set_compaction_filter_factory(PartitionCompactionFactory);
        let db = DB::open(&options, directory.path()).unwrap();
        let mut compact = CompactOptions::default();
        compact.set_bottommost_level_compaction(BottommostLevelCompaction::Force);

        for (registered, kind, bucket) in [false, true].into_iter().flat_map(|registered| {
            [
                (registered, StatKind::AggregatedGauge, &b""[..]),
                (registered, StatKind::StageBucketedGauge, &b"i"[..]),
                (
                    registered,
                    StatKind::StageStatusBucketedGauge,
                    &b"i\x01"[..],
                ),
            ]
        }) {
            let key = if registered {
                let mut key = RegisteredStatKeyPrefix::new(PartitionId::from(8), 70_000)
                    .as_bytes()
                    .to_vec();
                key.push(kind as u8);
                key
            } else {
                StatKeyPrefix::new(PartitionId::from(8), 12, kind)
                    .as_bytes()
                    .to_vec()
            };
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
    }
}
