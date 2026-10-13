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
use crate::stats::StatKeyPrefix;

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
            // Filter zeros for aggregated Gauge statistics
            let Ok((prefix, _)) = StatKeyPrefix::decode_prefix(key) else {
                warn!("Failed to decode aggregated statistic key during compaction");
                return CompactionDecision::Keep;
            };

            if prefix.stat_kind().should_filter_value(value) {
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
    use zerocopy::IntoBytes;

    use restate_storage_api::stats::service_load::ServiceLoad;
    use restate_storage_api::vqueue_table::{Stage, Status};
    use restate_types::sharding::PartitionId;

    use crate::stats::StatKeyPrefix;

    use super::*;

    #[test]
    fn stats_filter_removes_zeroes_and_keeps_malformed_data() {
        let mut key = StatKeyPrefix::of::<ServiceLoad>(PartitionId::from(8))
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
    }
}
