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

use datafusion::catalog::TableProvider;
use strum::IntoDiscriminant;

use restate_storage_query_api::LogicalSchema;
use restate_storage_query_api::tables::scheduler::{Scheduler, SchedulerColumn};
use restate_worker_api::{PartitionCaptureRequirements, SchedulerStatusEntry, SchedulingStatus};

use super::schema::{SysSchedulerBuilder, SysSchedulerTable};
use crate::context::SelectPartitions;
use crate::live_scanners::LivePartitionScanner;
use crate::source_registry::SourceRegistry;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysSchedulerTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        sources: &SourceRegistry,
    ) -> Arc<dyn TableProvider> {
        let statistics = TableStatisticsBuilder::<Scheduler>::new()
            .with_num_rows_estimate(RowEstimate::Small)
            .with_partition_key(SchedulerColumn::PartitionKey)
            .with_primary_key(SchedulerColumn::Id);
        let table = PartitionedTableProvider::declared::<Self>(partition_selector, sources)
            .with_statistics(statistics.build());
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner() -> impl ScanPartition {
        LivePartitionScanner::<Scheduler, _>::new(
            PartitionCaptureRequirements {
                scheduler: true,
                ..Default::default()
            },
            Scheduler::candidate_shard_keys,
            |captured| captured.scheduler.as_ref(),
            |(qid, _)| qid.partition_key(),
            append_scheduler_row,
        )
    }
}

#[inline]
fn append_scheduler_row(builder: &mut SysSchedulerBuilder, row_data: &SchedulerStatusEntry) {
    let (qid, status) = row_data;
    let mut row = builder.row();
    if row.is_partition_key_defined() {
        row.partition_key(qid.partition_key());
    }
    if row.is_id_defined() {
        row.fmt_id(qid);
    }
    if row.is_num_inbox_defined() {
        row.num_inbox(status.waiting_inbox);
    }
    if row.is_status_defined() {
        row.status(status.status.name());
    }
    if row.is_head_entry_id_defined()
        && let Some(entry_id) = status.head_entry_id.as_ref()
    {
        row.fmt_head_entry_id(entry_id.display(qid.partition_key()));
    }
    if row.is_scheduled_at_defined()
        && let SchedulingStatus::Scheduled { at } = status.status
    {
        row.scheduled_at(at.as_unix_millis().as_u64() as i64);
    }
    if row.is_blocked_on_json_defined()
        && let SchedulingStatus::BlockedOn(ref blocked_on) = status.status
    {
        let json =
            serde_json::to_string(blocked_on).expect("blocked_on_json serde should be infallible");
        row.blocked_on_json(json);
    }
    if row.is_blocked_on_defined()
        && let SchedulingStatus::BlockedOn(ref blocked_on) = status.status
    {
        row.fmt_blocked_on(blocked_on.discriminant());
    }
    if row.is_invoker_concurrency_block_duration_defined() {
        row.invoker_concurrency_block_duration(
            status.wait_stats.blocked_on_invoker_concurrency_ms as i64,
        );
    }
    if row.is_throttling_rules_block_duration_defined() {
        row.throttling_rules_block_duration(
            status.wait_stats.blocked_on_throttling_rules_ms as i64,
        );
    }
    if row.is_invoker_throttling_block_duration_defined() {
        row.invoker_throttling_block_duration(
            status.wait_stats.blocked_on_invoker_throttling_ms as i64,
        );
    }
    if row.is_invoker_memory_block_duration_defined() {
        row.invoker_memory_block_duration(status.wait_stats.blocked_on_invoker_memory_ms as i64);
    }
    if row.is_concurrency_rules_block_duration_defined() {
        row.concurrency_rules_block_duration(
            status.wait_stats.blocked_on_concurrency_rules_ms as i64,
        );
    }
    if row.is_lock_block_duration_defined() {
        row.lock_block_duration(status.wait_stats.blocked_on_lock_ms as i64);
    }
    if row.is_deployment_concurrency_block_duration_defined() {
        row.deployment_concurrency_block_duration(
            status.wait_stats.blocked_on_deployment_concurrency_ms as i64,
        );
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::util::display::array_value_to_string;
    use futures::TryStreamExt;

    use restate_types::identifiers::PartitionId;
    use restate_types::vqueues::VQueueId;
    use restate_worker_api::{
        CapturedPartitionRead, CapturedPartitionState, VQueueSchedulerStatus,
    };

    use crate::access::PrimaryRead;
    use crate::table_providers::{PartitionReadContext, ReadMetrics};

    use super::*;

    #[restate_core::test]
    async fn scheduler_reuses_capture_with_fresh_bounded_cursors() {
        let scanner = Arc::new(SysSchedulerTable::create_local_scanner());
        let prepared = scanner
            .prepare_read(
                PrimaryRead::Scan,
                SysSchedulerBuilder::schema(),
                None,
                Some(2),
            )
            .unwrap();
        assert_eq!(
            prepared.capture_requirements(),
            PartitionCaptureRequirements {
                scheduler: true,
                ..Default::default()
            }
        );
        let mut live: Vec<_> = (0..3)
            .map(|key| {
                (
                    VQueueId::custom(key, "capture"),
                    VQueueSchedulerStatus {
                        waiting_inbox: key + 10,
                        ..Default::default()
                    },
                )
            })
            .collect();
        let captured = PartitionReadContext::from_capture(CapturedPartitionRead {
            partition: PartitionId::MIN,
            storage: None,
            live: CapturedPartitionState {
                scheduler: Some(live.clone().into()),
                ..Default::default()
            },
            timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
        });
        live[0].1.waiting_inbox = 99;
        for _ in 0..2 {
            let batches = prepared
                .execute(captured.clone(), 1, ReadMetrics::default())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches.len(), 2);
            let values: Vec<_> = batches
                .iter()
                .map(|batch| {
                    assert_eq!(batch.num_rows(), 1);
                    array_value_to_string(batch.column_by_name("num_inbox").unwrap(), 0).unwrap()
                })
                .collect();
            assert_eq!(values, ["10", "11"]);
        }
        let newer = PartitionReadContext::from_capture(CapturedPartitionRead {
            partition: PartitionId::MIN,
            storage: None,
            live: CapturedPartitionState {
                scheduler: Some(live.into()),
                ..Default::default()
            },
            timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
        });
        let batches = prepared
            .execute(newer, 1, ReadMetrics::default())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            array_value_to_string(batches[0].column_by_name("num_inbox").unwrap(), 0).unwrap(),
            "99"
        );
        assert!(
            prepared
                .execute(
                    PartitionReadContext::live(PartitionId::MIN),
                    1,
                    Default::default()
                )
                .is_err()
        );
        let empty = PartitionReadContext::from_capture(CapturedPartitionRead {
            partition: PartitionId::MIN,
            storage: None,
            live: CapturedPartitionState::empty(prepared.capture_requirements()),
            timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
        });
        assert!(
            prepared
                .execute(empty, 1, ReadMetrics::default())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    }
}
