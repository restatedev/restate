// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! V1 storage scans on the query service's shared IO pool.
//!
//! A partition scan admits one snapshot, then carries its Arrow builder through
//! the physical layouts in order. Each read lends records to a typed consumer on
//! an IO worker; sending a full batch awaits output room without blocking that
//! worker. The full SQL predicate runs before the output limit is applied.
//!
//! For example, a journal query reaching LIMIT in the legacy layout returns
//! `Stopped` and never opens the newer layout. Exhaustion carries the partial
//! batch into the next layout. A decode/filter error discards that batch and
//! fails the stream; only successful exhaustion flushes the final partial batch,
//! after releasing storage. Dropping the stream cancels its submitted read.

use std::marker::PhantomData;
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
use tokio_util::sync::CancellationToken;

use restate_partition_store::{PartitionStoreManager, QueryIoExecutor, ReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_storage_query_api::scan::{CursorScan, Scan, Table};
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::table_providers::ScanPartition;
use crate::table_util::{BatchSender, Builder};

#[cfg(test)]
mod tests;

pub trait ScanLocalPartitionFilter {
    fn new(range: KeyRange, predicate: Option<Arc<dyn PhysicalExpr>>) -> Self;
}

impl ScanLocalPartitionFilter for KeyRange {
    fn new(range: KeyRange, _predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        range
    }
}

pub(crate) trait ScanLocalPartition: Send + Sync + Debug + 'static {
    type Builder: Builder + Send + 'static;
    type Filter: ScanLocalPartitionFilter + Send + Sync + 'static;

    /// Reads each physical layout through the same admitted snapshot. Return a
    /// stopped outcome immediately, before submitting another layout's cursor.
    fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        filter: Self::Filter,
        rows: Rows<Self::Builder>,
    ) -> impl Future<Output = restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>>> + Send;
}

/// One output builder carried between physical scans. For example, an exhausted
/// legacy state scan returns it for the scoped scan; a LIMIT stop returns it
/// without opening the scoped cursor. Errors drop it without flushing.
pub(crate) struct Rows<B: Builder> {
    output: BatchSender<B>,
    elapsed: ElapsedCompute,
}

impl<B: Builder + Send + 'static> Rows<B> {
    pub(crate) async fn read<T: Table>(
        self,
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        name: &'static str,
        request: Arc<T::Scan>,
        append: impl for<'a> FnMut(&mut B, T::Record<'a>) -> restate_storage_api::Result<()>
        + Send
        + 'static,
    ) -> restate_storage_api::Result<ScanOutcome<Self>>
    where
        ReadSnapshot: CursorScan<T>,
        for<'a> T::Record<'a>: Send,
    {
        let outcome = snapshot
            .scan::<T, _>(
                executor,
                name,
                request,
                TableRows {
                    rows: self,
                    append,
                    table: PhantomData,
                },
            )?
            .await?;
        Ok(outcome.map(|consumer| consumer.rows))
    }
}

struct TableRows<B: Builder, T: Table, F> {
    rows: Rows<B>,
    append: F,
    table: PhantomData<fn() -> T>,
}

impl<B, T, F> RowConsumer for TableRows<B, T, F>
where
    B: Builder + Send + 'static,
    T: Table,
    for<'a> T::Record<'a>: Send,
    F: for<'a> FnMut(&mut B, T::Record<'a>) -> restate_storage_api::Result<()> + Send + 'static,
{
    type Item<'a> = T::Record<'a>;

    async fn consume(
        &mut self,
        record: Self::Item<'_>,
    ) -> restate_storage_api::Result<ControlFlow<()>> {
        self.rows.elapsed.start();
        (self.append)(self.rows.output.builder_mut(), record)?;
        self.rows
            .output
            .send_if_needed()
            .await
            .map_err(|err| StorageError::Generic(err.into()))
    }
}

#[derive(Clone, derive_more::Debug)]
pub(crate) struct LocalPartitionsScanner<T> {
    #[debug(skip)]
    partition_store_manager: Arc<PartitionStoreManager>,
    #[debug(skip)]
    io_executor: QueryIoExecutor,
    _marker: std::marker::PhantomData<T>,
}

impl<T> LocalPartitionsScanner<T>
where
    T: ScanLocalPartition,
{
    pub fn new(
        partition_store_manager: Arc<PartitionStoreManager>,
        io_executor: QueryIoExecutor,
    ) -> Self {
        Self {
            partition_store_manager,
            io_executor,
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
        let io_executor = self.io_executor.clone();
        let mut stream_builder = RecordBatchReceiverStream::builder(projection.clone(), 1);
        let tx = stream_builder.tx();

        let background_task = async move {
            // LIMIT 0 reads nothing.
            if limit == Some(0) {
                return Ok(());
            }
            let partition_store = partition_store_manager.get_partition_store(partition_id).await.ok_or_else(|| {
                // make sure that the consumer of this stream to learn about the fact that this node does not have
                // that partition anymore, so that it can decide how to react to this.
                // for example, they can retry or fail the query with a useful message.
                let err = anyhow!("partition {} doesn't exist on this node, this is benign if the partition is being transferred out of/into this node.", partition_id);
                DataFusionError::External(err.into())
            })?;

            let cancel = CancellationToken::new();
            let _cancel_on_drop = cancel.clone().drop_guard();
            let (snapshot, _closed) = partition_store
                .query_read(cancel)
                .map_err(|err| DataFusionError::External(err.into()))?;
            let rows = Rows {
                output: BatchSender::new(projection, tx, predicate.clone(), batch_size, limit),
                elapsed: ElapsedCompute::new(elapsed_compute),
            };
            let outcome = S::scan(
                &snapshot,
                &io_executor,
                S::Filter::new(range, predicate),
                rows,
            )
            .await
            .map_err(|err| DataFusionError::External(err.into()))?;
            // The returned Arrow builder owns its values. Release storage before
            // a final partial batch waits for output room, so partition drainage
            // never depends on the client consuming that batch.
            drop(snapshot);
            drop(partition_store);
            if let ScanOutcome::Exhausted(rows) = outcome {
                rows.output.finish().await?;
            }
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
