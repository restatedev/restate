// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use restate_invoker_api::{InvocationStatusReport, StatusHandle};
use restate_partition_store::PartitionStoreManager;
use restate_types::identifiers::{PartitionId, PartitionKey};

use crate::context::{QueryContext, SelectPartitions};
use crate::invocation_state::row::append_invocation_state_row;
use crate::invocation_state::schema::{SysInvocationStateBuilder, sys_invocation_state_sort_order};
use crate::partition_filter::FirstMatchingPartitionKeyExtractor;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::table_providers::{PartitionedTableProvider, ScanPartition};
use crate::table_util::Builder;

const NAME: &str = "sys_invocation_state";

pub(crate) fn register_self(
    ctx: &QueryContext,
    partition_selector: impl SelectPartitions,
    status: Option<impl StatusHandle + Send + Sync + Debug + Clone + 'static>,
    local_partition_store_manager: Option<PartitionStoreManager>,
    remote_scanner_manager: &RemoteScannerManager,
) -> datafusion::common::Result<()> {
    let local_partition_scanner = match (status, local_partition_store_manager) {
        (Some(status_handle), Some(partition_store_manager)) => {
            let status_scanner = Arc::new(StatusScanner {
                status_handle,
                partition_store_manager,
            }) as Arc<dyn ScanPartition>;
            Some(status_scanner)
        }
        (None, None) => None,
        _ => {
            let err = anyhow!(
                "Was expecting either both a status scanner and a local partition scanner OR none of them"
            );
            return Err(DataFusionError::External(err.into()));
        }
    };
    let status_table = PartitionedTableProvider::new(
        partition_selector,
        SysInvocationStateBuilder::schema(),
        sys_invocation_state_sort_order(),
        remote_scanner_manager.create_distributed_scanner(NAME, local_partition_scanner),
        FirstMatchingPartitionKeyExtractor::default().with_invocation_id("id"),
    );
    ctx.register_partitioned_table(NAME, Arc::new(status_table))
}

async fn partition_key_range(
    partition_store_manager: PartitionStoreManager,
    partition_id: PartitionId,
) -> datafusion::common::Result<RangeInclusive<PartitionKey>> {
    partition_store_manager
        .get_partition_store(partition_id)
        .await
        .ok_or_else(|| {
            let err = anyhow!("expecting a partition store");
            DataFusionError::External(err.into())
        })
        .map(|store| store.partition_key_range().clone())
}

#[derive(derive_more::Debug, Clone)]
struct StatusScanner<S> {
    status_handle: S,
    #[debug(skip)]
    partition_store_manager: PartitionStoreManager,
}

impl<S: StatusHandle + Send + Sync + Debug + Clone + 'static> ScanPartition for StatusScanner<S> {
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        _range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        batch_size: usize,
        limit: Option<usize>,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let status = self.status_handle.clone();
        let partition_store_manager = self.partition_store_manager.clone();
        let schema = projection.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection, 1);
        let tx = stream_builder.tx();

        let background_task = async move {
            let range = partition_key_range(partition_store_manager, partition_id).await?;
            match limit {
                Some(limit) => {
                    for_each_state(
                        schema,
                        tx,
                        status.read_status(range).await.take(limit),
                        batch_size,
                    )
                    .await
                }
                None => {
                    for_each_state(schema, tx, status.read_status(range).await, batch_size).await
                }
            }
            Ok(())
        };

        stream_builder.spawn(background_task);
        Ok(stream_builder.build())
    }
}

async fn for_each_state<'a, I>(
    schema: SchemaRef,
    tx: Sender<datafusion::common::Result<RecordBatch>>,
    rows: I,
    batch_size: usize,
) where
    I: Iterator<Item = InvocationStatusReport> + 'a,
{
    let mut builder = SysInvocationStateBuilder::new(schema.clone());
    for row in rows {
        append_invocation_state_row(&mut builder, row);
        if builder.num_rows() >= batch_size {
            let batch = builder.finish();
            if tx.send(batch).await.is_err() {
                // not sure what to do here?
                // the other side has hung up on us.
                // we probably don't want to panic, is it will cause the entire process to exit
                return;
            }
            builder = SysInvocationStateBuilder::new(schema.clone());
        }
    }
    if !builder.empty() {
        let result = builder.finish();
        let _ = tx.send(result).await;
    }
}
