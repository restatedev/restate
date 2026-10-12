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
use std::{fmt::Debug, ops::ControlFlow};

use anyhow::anyhow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use tokio::time::Instant;

use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_storage_api::StorageError;
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::access::{PrimaryKeyKind, PrimaryRead};
use crate::table_providers::ScanPartition;
use crate::table_util::BatchSender;

pub trait ScanLocalPartitionFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self;

    fn planned(
        range: KeyRange,
        access: &PrimaryRead,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        anyhow::ensure!(
            matches!(access, PrimaryRead::Range),
            "primary lookup is unavailable for this source"
        );
        Ok(Self::new(range, predicate))
    }
}

impl ScanLocalPartitionFilter for KeyRange {
    fn new(range: KeyRange, _predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        range
    }
}

pub trait ScanLocalPartition: Send + Sync + Debug + 'static {
    const PRIMARY_KEY: Option<PrimaryKeyKind> = None;
    type Builder: crate::table_util::Builder + Send;
    type Item<'a>: Send;
    type ConversionError;
    type Filter: ScanLocalPartitionFilter + Send + Sync + 'static;

    fn for_each_row<
        F: for<'a> FnMut(Self::Item<'a>) -> ControlFlow<Result<(), Self::ConversionError>>
            + Send
            + Sync
            + 'static,
    >(
        partition_store: &PartitionStore,
        filter: Self::Filter,
        f: F,
    ) -> Result<impl Future<Output = restate_storage_api::Result<()>> + Send, StorageError>;

    fn append_row<'a>(
        row_builder: &mut Self::Builder,
        value: Self::Item<'a>,
    ) -> Result<(), Self::ConversionError>;
}

#[derive(Clone, derive_more::Debug)]
pub struct LocalPartitionsScanner<T> {
    #[debug(skip)]
    partition_store_manager: Arc<PartitionStoreManager>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> LocalPartitionsScanner<T>
where
    T: ScanLocalPartition,
{
    pub fn new(partition_store_manager: Arc<PartitionStoreManager>) -> Self {
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
        filter: S::Filter,
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

            // timer starts on first row, stops on scanner drop
            let mut elapsed_compute = ElapsedCompute::new(elapsed_compute);

            let mut batch_sender =
                BatchSender::new(projection, tx, predicate.clone(), batch_size, limit);

            S::for_each_row(&partition_store, filter, move |row| {
                elapsed_compute.start();
                match S::append_row(batch_sender.builder_mut(), row) {
                    Ok(()) => {}
                    err => return ControlFlow::Break(err),
                }
                batch_sender.send_if_needed().map_break(Ok)
            })
            .map_err(|err| DataFusionError::External(err.into()))?
            .await
            .map_err(|err| DataFusionError::External(err.into()))?;

            Ok(())
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
    fn primary_key_kind(&self) -> Option<PrimaryKeyKind> {
        S::PRIMARY_KEY
    }

    fn read_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream> {
        let filter = S::Filter::planned(range, &access, predicate.clone())?;
        self.scan_partition(
            partition_id,
            filter,
            projection,
            predicate,
            batch_size,
            limit,
            elapsed_compute,
        )
    }

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
            S::Filter::new(range, predicate.clone()),
            projection,
            predicate,
            batch_size,
            limit,
            elapsed_compute,
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
            self.time.add_duration(start.elapsed())
        }
    }
}
