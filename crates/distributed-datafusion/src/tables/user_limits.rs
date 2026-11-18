// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::catalog::TableProvider;

use restate_limiter::Level;
use restate_storage_query_api::LogicalSchema;
use restate_storage_query_api::tables::user_limits::{
    UserLimits, UserLimitsBatchBuilder, UserLimitsColumn,
};
use restate_types::identifiers::{PartitionKey, partitioner::HashPartitioner};
use restate_worker_api::{PartitionCaptureRequirements, UserLimitCounterEntry};

use crate::context::SelectPartitions;
use crate::live_scanners::LivePartitionScanner;
use crate::source_registry::SourceRegistry;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

crate::tables::table_adapter!(pub(crate) SysUserLimitsTable, UserLimits);

impl crate::sharding::PartitionSource for SysUserLimitsTable {
    // Within a storage partition, like V1's and the other partition tables': the
    // processor captures the counters sorted by partition key, which is each
    // counter's scope's key (the key this table shows).
    const ORDERING: &'static [UserLimitsColumn] = &[UserLimitsColumn::PartitionKey];
}

impl SysUserLimitsTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(
            PartitionedTableProvider::declared::<Self>(partitions, sources).with_statistics(
                TableStatisticsBuilder::<UserLimits>::new()
                    .with_num_rows_estimate(RowEstimate::Small)
                    .build(),
            ),
        )
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        LivePartitionScanner::<UserLimits, _>::new(
            PartitionCaptureRequirements {
                user_limits: true,
                ..Default::default()
            },
            UserLimits::candidate_shard_keys,
            |captured| captured.user_limits.as_ref(),
            partition_key,
            append_row,
        )
    }
}

fn partition_key(entry: &UserLimitCounterEntry) -> PartitionKey {
    HashPartitioner::compute_partition_key(&entry.scope)
}

fn append_row(builder: &mut UserLimitsBatchBuilder, entry: &UserLimitCounterEntry) {
    let mut row = builder.row();
    row.partition_key(partition_key(entry));
    row.scope(&entry.scope);
    if let Some(key) = &entry.l1 {
        row.l1(key);
    }
    if let Some(key) = &entry.l2 {
        row.l2(key);
    }
    row.level(match entry.level {
        Level::Scope => "Scope",
        Level::Level1 => "Level1",
        Level::Level2 => "Level2",
    });
    row.usage(entry.usage);
    if let Some(limit) = entry.concurrency_limit {
        row.concurrency_limit(limit);
        row.available(limit.saturating_sub(entry.usage));
    }
    if let Some(pattern) = &entry.rule_pattern {
        row.rule_pattern(pattern);
    }
    row.num_waiters(entry.num_waiters);
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::common::DFSchema;
    use datafusion::prelude::{SessionContext, col, lit};
    use futures::TryStreamExt;

    use restate_types::Scope;
    use restate_types::identifiers::{PartitionId, WithPartitionKey};
    use restate_worker_api::{CapturedPartitionRead, CapturedPartitionState};

    use crate::access::PrimaryRead;
    use crate::table_providers::{PartitionReadContext, ReadMetrics};

    use super::*;

    #[restate_core::test]
    async fn captured_counters_route_and_materialize_by_scope() {
        let scope = Scope::try_non_interned("tenant-a").unwrap();
        let key = scope.partition_key();
        let entries: Vec<_> = [Level::Scope, Level::Level1, Level::Level2]
            .into_iter()
            .map(|level| UserLimitCounterEntry {
                partition_key: key.wrapping_add(1), // Legacy synthetic key must not affect V2.
                scope: scope.as_str().to_owned(),
                l1: (level != Level::Scope).then(|| "one".to_owned()),
                l2: (level == Level::Level2).then(|| "two".to_owned()),
                level,
                usage: 3,
                concurrency_limit: (level != Level::Scope).then_some(2),
                rule_pattern: None,
                num_waiters: 4,
            })
            .collect();
        let context = PartitionReadContext::from_capture(CapturedPartitionRead {
            partition: PartitionId::MIN,
            storage: None,
            live: CapturedPartitionState {
                user_limits: Some(entries.into()),
                ..Default::default()
            },
            timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
        });
        let scanner = Arc::new(SysUserLimitsTable::create_local_scanner());
        let schema = UserLimits::arrow_schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let session = SessionContext::new();
        for (expr, count) in [
            (col("scope").eq(lit(scope.as_str())), 3),
            (col("partition_key").eq(lit(key)), 3),
            (col("partition_key").eq(lit(key.wrapping_add(1))), 0),
        ] {
            let predicate = session.create_physical_expr(expr, &df_schema).unwrap();
            let prepared = scanner
                .clone()
                .prepare_read(PrimaryRead::Scan, schema.clone(), Some(predicate), None)
                .unwrap();
            assert_eq!(
                prepared.capture_requirements(),
                PartitionCaptureRequirements {
                    user_limits: true,
                    ..Default::default()
                }
            );
            let batches = prepared
                .execute(context.clone(), 1, ReadMetrics::default())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                count
            );
            for (index, batch) in batches.iter().enumerate() {
                assert_eq!(
                    array_value_to_string(batch.column(0), 0).unwrap(),
                    key.to_string()
                );
                if index == 0 {
                    assert_eq!(batch.column(8).null_count(), 1);
                } else {
                    assert_eq!(array_value_to_string(batch.column(8), 0).unwrap(), "0");
                }
            }
        }
    }
}
