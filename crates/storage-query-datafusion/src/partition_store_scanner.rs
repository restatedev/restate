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
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::scan_metrics::ScanMetrics;
use crate::table_providers::ScanPartition;
use crate::table_util::BatchSender;

pub trait ScanLocalPartitionFilter {
    fn new(range: KeyRange, access_predicate: Option<Arc<dyn PhysicalExpr>>) -> Self;
}

impl ScanLocalPartitionFilter for KeyRange {
    fn new(range: KeyRange, _access_predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        range
    }
}

pub trait ScanLocalPartition: Send + Sync + Debug + 'static {
    type Builder: crate::table_util::Builder + Send;
    type Item<'a>: Send;
    type ConversionError;
    type Filter: ScanLocalPartitionFilter + Send + Sync + 'static;

    /// Metrics are scoped to this call. Implementations pass the optional probe
    /// to instrumented native scans; unsupported paths leave it unmarked so their
    /// zero counters are not mistaken for measured zero work.
    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        metrics: Option<restate_partition_store::IteratorMetrics>,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>;

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
        access_predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        metrics: ScanMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let filter = S::Filter::new(range, access_predicate);
        let partition_store_manager = self.partition_store_manager.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 1);
        let tx = stream_builder.tx();

        let producer_metrics = metrics.clone();
        let background_task = async move {
            let partition_store = partition_store_manager.get_partition_store(partition_id).await.ok_or_else(|| {
                // make sure that the consumer of this stream to learn about the fact that this node does not have
                // that partition anymore, so that it can decide how to react to this.
                // for example, they can retry or fail the query with a useful message.
                let err = anyhow!("partition {} doesn't exist on this node, this is benign if the partition is being transferred out of/into this node.", partition_id);
                DataFusionError::External(err.into())
            })?;

            // timer starts on first row, stops on scanner drop
            let mut elapsed_compute = ElapsedCompute::new(producer_metrics.elapsed_compute.clone());

            let mut batch_sender =
                BatchSender::new(projection, tx, predicate.clone(), batch_size, limit);

            let row_metrics = producer_metrics.clone();
            S::for_each_row(
                &partition_store,
                filter,
                Some(producer_metrics.iterator_metrics()),
                move |row| {
                    elapsed_compute.start();
                    row_metrics.record_emitted();
                    match S::append_row(batch_sender.builder_mut(), row) {
                        Ok(()) => {}
                        err => return ControlFlow::Break(err),
                    }
                    batch_sender.send_if_needed().map_break(Ok)
                },
            )
            .map_err(|err| DataFusionError::External(err.into()))?
            .await
            .map_err(|err| DataFusionError::External(err.into()))?;

            producer_metrics.finish();
            Ok(())
        };
        stream_builder.spawn(background_task);
        Ok(metrics.observe(stream_builder.build()))
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
        access_predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        metrics: ScanMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        self.scan_partition(
            partition_id,
            range,
            projection,
            predicate,
            access_predicate,
            batch_size,
            limit,
            metrics,
        )
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
