// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::sync::Arc;

use anyhow::anyhow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::sync::mpsc::Sender;

use restate_partition_store::PartitionStoreManager;
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::VQueueId;
use restate_worker_api::{SchedulerStatusEntry, SchedulingStatus};

use crate::context::{PartitionLeaderStatusHandle, QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::scheduler_status::schema::{SysSchedulerBuilder, sys_scheduler_sort_order};
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::table_util::Builder;

const NAME: &str = "sys_scheduler";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    status: Option<impl PartitionLeaderStatusHandle<SchedulerStatus = SchedulerStatusEntry>>,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_partition_scanner = match status {
        Some(status_handle) => {
            let scanner = Arc::new(SchedulerStatusScanner {
                status_handle,
                partition_store_manager,
            }) as Arc<dyn ScanPartition>;
            Some(scanner)
        }
        None => None,
    };

    let schema = SysSchedulerBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Small)
        .with_partition_key()
        .with_primary_key("id");

    let scheduler_table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        sys_scheduler_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_partition_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_partitioned_resource_id::<VQueueId>("id")
            .with_invocation_id("head_entry_id"),
    )
    .with_statistics(statistics.build());

    ctx.register_partitioned_table(NAME, Arc::new(scheduler_table))
}

async fn partition_key_range(
    partition_store_manager: &PartitionStoreManager,
    partition_id: PartitionId,
) -> datafusion::common::Result<KeyRange> {
    partition_store_manager
        .get_partition_store(partition_id)
        .await
        .ok_or_else(|| {
            let err = anyhow!("expecting a partition store");
            DataFusionError::External(err.into())
        })
        .map(|store| store.partition_key_range())
}

#[derive(derive_more::Debug, Clone)]
struct SchedulerStatusScanner<S> {
    status_handle: S,
    #[debug(skip)]
    partition_store_manager: Arc<PartitionStoreManager>,
}

impl<S> ScanPartition for SchedulerStatusScanner<S>
where
    S: PartitionLeaderStatusHandle<SchedulerStatus = SchedulerStatusEntry>
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        _range: KeyRange,
        projection: SchemaRef,
        _predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        _elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let status = self.status_handle.clone();
        let partition_store_manager = self.partition_store_manager.clone();
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 1);
        let tx = stream_builder.tx();

        let background_task = async move {
            let range = partition_key_range(&partition_store_manager, partition_id).await?;
            match limit {
                Some(limit) => {
                    for_each_scheduler_status(
                        schema,
                        tx,
                        status.read_scheduler_status(range).await.take(limit),
                        batch_size,
                    )
                    .await
                }
                None => {
                    for_each_scheduler_status(
                        schema,
                        tx,
                        status.read_scheduler_status(range).await,
                        batch_size,
                    )
                    .await
                }
            }

            Ok(())
        };

        stream_builder.spawn(background_task);
        Ok(stream_builder.build())
    }
}

async fn for_each_scheduler_status<'a, I>(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: I,
    batch_size: usize,
) where
    I: Iterator<Item = SchedulerStatusEntry> + 'a,
{
    let mut builder = SysSchedulerBuilder::new(schema.clone());
    for row_data in rows {
        append_scheduler_row(&mut builder, row_data);
        if builder.num_rows() >= batch_size {
            let batch = builder.finish_and_new();
            if tx.send(batch).await.is_err() {
                return;
            }
        }
    }

    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
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
    if row.is_blocked_on_defined()
        && let SchedulingStatus::BlockedOn(blocked_on) = status.status
    {
        row.fmt_blocked_on(blocked_on);
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
