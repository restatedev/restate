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
use restate_worker_api::UserLimitCounterEntry;

use crate::context::{PartitionLeaderStatusHandle, QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{RowEstimate, TableStatisticsBuilder};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::table_util::Builder;
use crate::user_limits::row::append_user_limit_row;
use crate::user_limits::schema::{SysUserLimitsBuilder, sys_user_limits_sort_order};

const NAME: &str = "sys_user_limits";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    status: Option<impl PartitionLeaderStatusHandle<UserLimitCounter = UserLimitCounterEntry>>,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_partition_scanner = match status {
        Some(status_handle) => {
            let scanner = Arc::new(UserLimitsScanner {
                status_handle,
                partition_store_manager,
            }) as Arc<dyn ScanPartition>;
            Some(scanner)
        }
        None => None,
    };

    let schema = SysUserLimitsBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Small)
        .with_partition_key();

    let user_limits_table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        sys_user_limits_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_partition_scanner),
        FirstMatchingPartitionKeyExtractor::default(),
    )
    .with_statistics(statistics.build());

    ctx.register_partitioned_table(NAME, Arc::new(user_limits_table))
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
struct UserLimitsScanner<S> {
    status_handle: S,
    #[debug(skip)]
    partition_store_manager: Arc<PartitionStoreManager>,
}

impl<S> ScanPartition for UserLimitsScanner<S>
where
    S: PartitionLeaderStatusHandle<UserLimitCounter = UserLimitCounterEntry>
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
                    for_each_user_limit(
                        schema,
                        tx,
                        status.read_user_limit_counters(range).await.take(limit),
                        batch_size,
                    )
                    .await
                }
                None => {
                    for_each_user_limit(
                        schema,
                        tx,
                        status.read_user_limit_counters(range).await,
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

async fn for_each_user_limit<I>(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: I,
    batch_size: usize,
) where
    I: Iterator<Item = UserLimitCounterEntry>,
{
    let mut builder = SysUserLimitsBuilder::new(schema.clone());
    for entry in rows {
        append_user_limit_row(&mut builder, &entry);
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
