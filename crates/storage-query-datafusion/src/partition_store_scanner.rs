// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::sync::mpsc::Sender;
use tracing::warn;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_types::identifiers::{PartitionId, PartitionKey};

use crate::table_providers::ScanPartition;

pub trait ScanLocalPartition: Send + Sync + std::fmt::Debug + 'static {
    fn scan_partition_store(
        partition_store: PartitionStore,
        tx: Sender<Result<RecordBatch, datafusion::error::DataFusionError>>,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> impl std::future::Future<Output = ()> + Send;
}

#[derive(Clone, Debug)]
pub struct LocalPartitionsScanner<S> {
    partition_store_manager: PartitionStoreManager,
    _marker: std::marker::PhantomData<S>,
}

impl<S> LocalPartitionsScanner<S>
where
    S: ScanLocalPartition,
{
    pub fn new(partition_store_manager: PartitionStoreManager, _scanner: S) -> Self {
        Self {
            partition_store_manager,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S> ScanPartition for LocalPartitionsScanner<S>
where
    S: ScanLocalPartition,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
    ) -> SendableRecordBatchStream {
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 16);
        let tx = stream_builder.tx();
        let partition_store_manager = self.partition_store_manager.clone();
        let background_task = async move {
            let Some(partition_store) = partition_store_manager
                .get_partition_store(partition_id)
                .await
            else {
                warn!("partition {} doesn't exist, this is benign if the partition is being transferred out of this node", partition_id);
                return Ok(());
            };
            S::scan_partition_store(partition_store, tx, range, projection).await;

            Ok(())
        };
        stream_builder.spawn(background_task);
        stream_builder.build()
    }
}
