// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! V1 table scans on the query IO pool.
//!
//! A scan of one partition takes one read snapshot and reads every stored layout
//! of its table through it, in order. Some tables have two layouts: the journal
//! (legacy and current entries), state and promises (unscoped and scoped). Rows
//! go into one Arrow batch builder that moves from layout to layout:
//!
//! ```text
//! snapshot ──▶ legacy journal ──▶ journal ──▶ send the last batch
//!                    │
//!                    └── LIMIT reached: stop, never open the next layout
//! ```
//!
//! - **Backpressure:** when a batch is full, the scan waits until the query takes
//!   it. The IO thread runs other scans meanwhile.
//! - **Filters, then LIMIT:** a row counts towards the limit only if it passes
//!   the SQL filter.
//! - **Errors:** a read or decode error fails the query and discards rows not
//!   sent yet. The last, partly filled batch is sent only after a successful
//!   scan, and after the snapshot is released.
//! - **Cancellation:** dropping the query's stream cancels its read.

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

    /// Reads the table's layouts in order through `snapshot`. When a read stops,
    /// because the LIMIT was reached or the query went away, return its outcome
    /// at once without opening the next layout.
    fn scan(
        snapshot: &ReadSnapshot,
        executor: &QueryIoExecutor,
        filter: Self::Filter,
        rows: Rows<Self::Builder>,
    ) -> impl Future<Output = restate_storage_api::Result<ScanOutcome<Rows<Self::Builder>>>> + Send;
}

/// The output of one partition scan: an Arrow batch builder and the channel to
/// the query. Each layout's read hands it back, so the next layout continues the
/// same batch. For example, the unscoped state read returns it when done, and
/// the scoped read continues it; if the unscoped read reached the LIMIT, the
/// scoped read never starts. An error drops it without sending buffered rows.
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
            .map_err(scan_error)?;
            // The builder holds copies of its values. Release the snapshot before
            // waiting to send the last batch, so a slow client can't keep the
            // partition from closing.
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

/// The error a failed scan reports to the query. Errors raised while building or
/// sending batches, such as a SQL filter failing on a row, cross the scan as
/// `StorageError::Generic`. They are returned as raised, so the query shows the
/// filter's error instead of a storage error.
fn scan_error(err: StorageError) -> DataFusionError {
    match err {
        StorageError::Generic(err) => match err.downcast::<DataFusionError>() {
            Ok(err) => err,
            Err(err) => DataFusionError::External(StorageError::Generic(err).into()),
        },
        err => DataFusionError::External(err.into()),
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
