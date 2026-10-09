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

use restate_types::vqueues::VQueueId;
use restate_worker_api::{PartitionQueryAccess, SchedulerStatusEntry, SchedulingStatus};

use crate::context::SelectPartitions;
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::live_scanners::LivePartitionScanner;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::scheduler_status::schema::{
    SysSchedulerBuilder, SysSchedulerTable, sys_scheduler_sort_order,
};
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};

impl SysSchedulerTable {
    pub(crate) fn create_provider(
        partition_selector: impl SelectPartitions,
        remote_scanner_manager: &RemoteScannerManager,
    ) -> Arc<dyn TableProvider> {
        let schema = SysSchedulerBuilder::schema();
        let statistics = TableStatisticsBuilder::new(schema.clone())
            .with_num_rows_estimate(RowEstimate::Small)
            .with_partition_key()
            .with_primary_key("id");
        let table = PartitionedTableProvider::new(
            partition_selector,
            schema,
            sys_scheduler_sort_order(),
            remote_scanner_manager.create_distributed_scanner::<Self>(),
            FirstMatchingPartitionKeyExtractor::default()
                .with_partitioned_resource_id::<VQueueId>("id")
                .with_vqueue_entry_id("head_entry_id"),
        )
        .with_statistics(statistics.build());
        Arc::new(table)
    }

    pub(crate) fn create_local_scanner(
        access: Arc<dyn PartitionQueryAccess>,
    ) -> impl ScanPartition {
        LivePartitionScanner::new(
            access,
            |access, partition, keys| access.scan_scheduler_status(partition, keys),
            append_scheduler_row,
        )
    }
}

#[inline]
fn append_scheduler_row(builder: &mut SysSchedulerBuilder, row_data: SchedulerStatusEntry) {
    let (qid, status) = row_data;
    let mut row = builder.row();
    if row.is_partition_key_defined() {
        row.partition_key(qid.partition_key());
    }
    if row.is_id_defined() {
        row.id(qid.to_string());
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
        && let SchedulingStatus::BlockedOn(blocked_on) = status.status
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
