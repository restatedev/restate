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

use restate_storage_query_api::LogicalSchema;
use restate_storage_query_api::tables::hot_keys::{HotKeys, HotKeysBatchBuilder};
use restate_worker_api::{HotKeyEntry, PartitionCaptureRequirements};

use crate::context::SelectPartitions;
use crate::live_scanners::LivePartitionScanner;
use crate::source_registry::SourceRegistry;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

crate::tables::table_adapter!(pub(crate) SysHotKeysTable, HotKeys);

impl crate::sharding::PartitionSource for SysHotKeysTable {
    // The leader keeps the hot keys of the last window sorted by partition key.
    const ORDERS: &'static [restate_storage_query_api::ordering::NativeOrder] =
        &[&[restate_storage_query_api::ordering::OrderTerm::Column(
            "partition_key",
        )]];
}

impl SysHotKeysTable {
    pub(crate) fn create_provider(
        partitions: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        Arc::new(
            PartitionedTableProvider::declared::<Self>(partitions, sources).with_statistics(
                TableStatisticsBuilder::<HotKeys>::new()
                    .with_num_rows_estimate(RowEstimate::Small)
                    .build(),
            ),
        )
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        LivePartitionScanner::<HotKeys, _>::new(
            PartitionCaptureRequirements {
                hot_keys: true,
                ..Default::default()
            },
            HotKeys::candidate_shard_keys,
            |captured| captured.hot_keys.as_ref(),
            |entry: &HotKeyEntry| entry.partition_key,
            append_row,
        )
    }
}

fn append_row(builder: &mut HotKeysBatchBuilder, entry: &HotKeyEntry) {
    let mut row = builder.row();
    row.partition_key(entry.partition_key);
    row.actions(entry.actions);
    row.window_start(entry.window_start.as_u64() as i64);
    row.window_end(entry.window_end.as_u64() as i64);
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::common::DFSchema;
    use datafusion::prelude::{SessionContext, col, lit};
    use futures::TryStreamExt;

    use restate_types::identifiers::PartitionId;
    use restate_types::time::MillisSinceEpoch;
    use restate_worker_api::{CapturedPartitionRead, CapturedPartitionState};

    use crate::access::PrimaryRead;
    use crate::table_providers::{PartitionReadContext, ReadMetrics};

    use super::*;

    #[restate_core::test]
    async fn captured_hot_keys_route_and_materialize_by_partition_key() {
        let entries: Vec<_> = [7, 42]
            .into_iter()
            .map(|partition_key| HotKeyEntry {
                partition_key,
                actions: 600,
                window_start: MillisSinceEpoch::new(1_000),
                window_end: MillisSinceEpoch::new(61_000),
            })
            .collect();
        let context = PartitionReadContext::from_capture(CapturedPartitionRead {
            partition: PartitionId::MIN,
            storage: None,
            live: CapturedPartitionState {
                hot_keys: Some(entries.into()),
                ..Default::default()
            },
            timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
        });
        let scanner = Arc::new(SysHotKeysTable::create_local_scanner());
        let schema = HotKeys::arrow_schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
        let session = SessionContext::new();
        for (expr, expected) in [
            (col("actions").eq(lit(600u64)), vec!["7", "42"]),
            (col("partition_key").eq(lit(42u64)), vec!["42"]),
            (col("partition_key").eq(lit(43u64)), vec![]),
        ] {
            let predicate = session.create_physical_expr(expr, &df_schema).unwrap();
            let prepared = scanner
                .clone()
                .prepare_read(PrimaryRead::Scan, schema.clone(), Some(predicate), None)
                .unwrap();
            assert_eq!(
                prepared.capture_requirements(),
                PartitionCaptureRequirements {
                    hot_keys: true,
                    ..Default::default()
                }
            );
            let batches = prepared
                .execute(context.clone(), 1, ReadMetrics::default())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let mut partition_keys = Vec::new();
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    partition_keys.push(array_value_to_string(batch.column(0), row).unwrap());
                    assert_eq!(
                        array_value_to_string(batch.column(3), row).unwrap(),
                        "1970-01-01T00:01:01Z"
                    );
                }
            }
            assert_eq!(partition_keys, expected);
        }
    }
}
