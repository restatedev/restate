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
use std::time::Instant;
use std::{fmt::Debug, ops::ControlFlow};

use anyhow::anyhow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_storage_api::scan::RowConsumer;
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::table_providers::ScanPartition;
use crate::table_util::BatchSender;

pub trait ScanLocalPartitionFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self;
}

impl ScanLocalPartitionFilter for KeyRange {
    fn new(range: KeyRange, _predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        range
    }
}

pub trait ScanLocalPartition: Send + Sync + Debug + 'static {
    type Builder: crate::table_util::Builder + Send;
    type Item<'a>: Send;
    type ConversionError: Into<anyhow::Error>;
    type Filter: ScanLocalPartitionFilter + Send + Sync + 'static;

    fn for_each_row<F: for<'a> RowConsumer<Item<'a> = Self::Item<'a>>>(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<F>> + Send, StorageError>;

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError>;
}

#[derive(Clone, derive_more::Debug)]
pub struct LocalPartitionsScanner<S> {
    #[debug(skip)]
    partition_store_manager: Arc<PartitionStoreManager>,
    _marker: std::marker::PhantomData<S>,
}

impl<S> LocalPartitionsScanner<S>
where
    S: ScanLocalPartition,
{
    pub fn new(partition_store_manager: Arc<PartitionStoreManager>, _scanner: S) -> Self {
        Self {
            partition_store_manager,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<S, RB> LocalPartitionsScanner<S>
where
    S: ScanLocalPartition<Builder = RB>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let partition_store_manager = self.partition_store_manager.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 1);
        let tx = stream_builder.tx();

        let background_task = async move {
            let partition_store = partition_store_manager.get_partition_store(partition_id).await.ok_or_else(|| {
                // make sure that the consumer of this stream to learn about the fact that this node does not have
                // that partition anymore, so that it can decide how to react to this.
                // for example, they can retry or fail the query with a useful message.
                let err = anyhow!("partition {} doesn't exist on this node, this is benign if the partition is being transferred out of/into this node.", partition_id);
                DataFusionError::External(err.into())
            })?;

            let consumer = BatchConsumer::<S> {
                batch_sender: BatchSender::new(
                    projection,
                    tx,
                    predicate.clone(),
                    batch_size,
                    limit,
                ),
                elapsed_compute: ElapsedCompute::new(elapsed_compute),
            };
            let mut consumer =
                S::for_each_row(&partition_store, S::Filter::new(range, predicate), consumer)
                    .map_err(|err| DataFusionError::External(err.into()))?
                    .await
                    .map_err(|err| DataFusionError::External(err.into()))?;
            // The scan is over: send what's left.
            consumer.batch_sender.finish().await
        };
        stream_builder.spawn(background_task);
        Ok(stream_builder.build())
    }
}

impl<S, RB> ScanPartition for LocalPartitionsScanner<S>
where
    S: ScanLocalPartition<Builder = RB>,
    RB: crate::table_util::Builder + Send + Sync + 'static,
{
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
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

struct BatchConsumer<S: ScanLocalPartition> {
    batch_sender: BatchSender<S::Builder>,
    elapsed_compute: ElapsedCompute,
}

impl<S: ScanLocalPartition> RowConsumer for BatchConsumer<S> {
    type Item<'a> = S::Item<'a>;

    async fn consume(
        &mut self,
        row: Self::Item<'_>,
    ) -> restate_storage_api::Result<ControlFlow<()>> {
        self.elapsed_compute.start();
        S::append_row(self.batch_sender.builder_mut(), row)
            .map_err(|error| StorageError::Conversion(error.into()))?;
        self.batch_sender
            .send_if_needed()
            .await
            .map_err(|error| StorageError::Generic(error.into()))
    }
}

struct ElapsedCompute {
    time: Time,
    start: Option<Instant>,
}

impl ElapsedCompute {
    fn new(time: Time) -> Self {
        Self { time, start: None }
    }

    fn start(&mut self) {
        self.start.get_or_insert_with(Instant::now);
    }
}

impl Drop for ElapsedCompute {
    fn drop(&mut self) {
        if let Some(start) = &self.start {
            self.time.add_elapsed(*start)
        }
    }
}
