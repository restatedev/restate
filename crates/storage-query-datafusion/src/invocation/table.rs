// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use restate_invoker_api::{InvocationStatusReport, StatusHandle};
use restate_partition_store::PartitionStoreManager;
use restate_storage_api::invocation_status_table::ScanInvocationStatusTable;
use restate_types::identifiers::{InvocationId, PartitionId, PartitionKey};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::{ControlFlow, RangeInclusive};
use std::sync::Arc;

use crate::context::{QueryContext, SelectPartitions};
use crate::filter::FirstMatchingPartitionKeyExtractor;
use crate::filter::InvocationIdFilter;
use crate::invocation::row::append_invocation_row;
use crate::invocation::schema::{SysInvocationBuilder, sys_invocation_sort_order};
use crate::partition_store_scanner::ScanLocalPartitionFilter;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::statistics::{
    DEPLOYMENT_ROW_ESTIMATE, RowEstimate, SERVICE_ROW_ESTIMATE, TableStatisticsBuilder,
};
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::table_util::BatchSender;

const NAME: &str = "sys_invocation";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    status_handle: impl StatusHandle + Send + Sync + Debug + Clone + 'static,
    partition_store_manager: Arc<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_scanner = Arc::new(LocalInvocationStatusPartitionScanner {
        partition_store_manager,
        status_handle,
    }) as Arc<dyn ScanPartition>;

    let schema = SysInvocationBuilder::schema();
    let statistics = TableStatisticsBuilder::new(schema.clone())
        .with_num_rows_estimate(RowEstimate::Large)
        .with_partition_key()
        .with_primary_key("id")
        .with_foreign_key("pinned_deployment_id", DEPLOYMENT_ROW_ESTIMATE)
        .with_foreign_key("target_service_name", SERVICE_ROW_ESTIMATE);

    let table = PartitionedTableProvider::new(
        partition_selector,
        schema,
        sys_invocation_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_scanner),
        FirstMatchingPartitionKeyExtractor::default()
            .with_service_key("target_service_key")
            .with_invocation_id("id"),
    )
    .with_statistics(statistics.build());
    ctx.register_partitioned_table(NAME, Arc::new(table))
}

#[derive(Clone, derive_more::Debug)]
struct LocalInvocationStatusPartitionScanner<S> {
    #[debug(skip)]
    partition_store_manager: Arc<PartitionStoreManager>,
    #[debug(skip)]
    status_handle: S,
}

impl<S> LocalInvocationStatusPartitionScanner<S>
where
    S: StatusHandle + Send + Sync + Debug + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let partition_store_manager = self.partition_store_manager.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 1);
        let tx = stream_builder.tx();
        let status_handle = self.status_handle.clone();

        let background_task = async move {
            let partition_store = partition_store_manager.get_partition_store(partition_id).await.ok_or_else(|| {
                // make sure that the consumer of this stream to learn about the fact that this node does not have
                // that partition anymore, so that it can decide how to react to this.
                // for example, they can retry or fail the query with a useful message.
                let err = anyhow!("partition {} doesn't exist on this node, this is benign if the partition is being transferred out of/into this node.", partition_id);
                DataFusionError::External(err.into())
            })?;

            // timer starts on first row, stops on scanner drop
            let mut elapsed_compute =
                crate::partition_store_scanner::ElapsedCompute::new(elapsed_compute);

            let mut batch_sender =
                BatchSender::new(projection, tx, predicate.clone(), batch_size, limit);

            // Collect invocation status report
            let invocation_status_reports: HashMap<InvocationId, InvocationStatusReport> =
                status_handle
                    .read_status(range.clone())
                    .await
                    .map(|isr| (*isr.invocation_id(), isr))
                    .collect();

            partition_store
                .for_each_invocation_status_lazy(
                    InvocationIdFilter::new(range.clone(), predicate).into(),
                    move |(invocation_id, invocation_status)| {
                        elapsed_compute.start();
                        match append_invocation_row(
                            batch_sender.builder_mut(),
                            invocation_id,
                            invocation_status,
                            invocation_status_reports.get(&invocation_id),
                        ) {
                            Ok(()) => {}
                            err => return ControlFlow::Break(err),
                        }
                        batch_sender.send_if_needed().map_break(Ok)
                    },
                )
                .map_err(|err| DataFusionError::External(err.into()))?
                .await
                .map_err(|err| DataFusionError::External(err.into()))?;

            Ok(())
        };
        stream_builder.spawn(background_task);
        Ok(stream_builder.build())
    }
}

impl<S> ScanPartition for LocalInvocationStatusPartitionScanner<S>
where
    S: StatusHandle + Send + Sync + Debug + Clone + 'static,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.scan_partition(
            partition_id,
            range,
            projection,
            predicate,
            batch_size,
            limit,
            elapsed_compute,
        )
    }
}
