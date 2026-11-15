// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Snapshot-bound candidate batches. A partition read runs as a scan of its
//! snapshot on the query-owned IO pool shared with V1: the pool's worker drives
//! the cursor, and [`BatchConsumer`] turns the records into batches and sends each
//! full one on, waiting for room without holding the worker's thread.

use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::metrics::Time;
use tokio::sync::mpsc;

use restate_partition_store::{QueryIoExecutor, ReadSnapshot};
use restate_storage_api::StorageError;
use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_storage_query_api::scan::{CursorScan, Scan, Table};
use restate_storage_query_api::{
    AsyncRowSink, LogicalSchema, PreparedRead, RowBuilder, RowSink, SchemaBinding,
};

pub(crate) struct Batches<S: LogicalSchema> {
    builder: S::RowBuilder,
    batch_size: usize,
    remaining: usize,
    elapsed_compute: Time,
}

impl<S: LogicalSchema> Batches<S> {
    pub(crate) fn new(
        schema: SchemaRef,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> Self {
        Self {
            builder: S::RowBuilder::new(schema),
            batch_size,
            remaining: limit.unwrap_or(usize::MAX),
            elapsed_compute,
        }
    }

    fn full(&self) -> bool {
        self.builder.num_rows() >= self.batch_size || self.remaining == 0
    }

    fn take(&mut self) -> Result<Option<RecordBatch>> {
        (self.builder.num_rows() != 0)
            .then(|| self.builder.finish())
            .transpose()
    }
}

impl<S: LogicalSchema> RowSink<S> for Batches<S> {
    fn append_with(
        &mut self,
        write: impl FnOnce(&mut S::RowBuilder) -> restate_storage_api::Result<()>,
    ) -> ControlFlow<restate_storage_api::Result<()>> {
        if self.full() {
            return ControlFlow::Break(Err(StorageError::Generic(anyhow::anyhow!(
                "materialization exceeded reserved row capacity"
            ))));
        }
        let _timer = self.elapsed_compute.timer();
        if let Err(error) = write(&mut self.builder) {
            return ControlFlow::Break(Err(error));
        }
        self.remaining -= 1;
        ControlFlow::Continue(())
    }
}

/// Where a partition read sends its batches. It sends each batch to `tx` as soon
/// as the batch is full, waiting for room, and stops once the row limit is
/// reached or the receiver is gone. Both happen before the read takes its next
/// storage step, so a later storage failure can't swallow a full batch or turn a
/// satisfied limit into an error.
pub(crate) struct BatchSink<S: LogicalSchema> {
    batches: Batches<S>,
    tx: mpsc::Sender<Result<RecordBatch>>,
}

impl<S: LogicalSchema> BatchSink<S> {
    pub(crate) fn new(batches: Batches<S>, tx: mpsc::Sender<Result<RecordBatch>>) -> Self {
        Self { batches, tx }
    }

    /// Sends the batch built so far, if any; `false` once the receiver is gone.
    async fn flush(&mut self) -> restate_storage_api::Result<bool> {
        match self
            .batches
            .take()
            .map_err(|error| StorageError::Generic(error.into()))?
        {
            Some(batch) => Ok(self.tx.send(Ok(batch)).await.is_ok()),
            None => Ok(true),
        }
    }

    /// Makes room for one more row: sends a full batch first, waiting for room,
    /// and stops at the row limit or when the receiver is gone.
    async fn make_room(&mut self) -> restate_storage_api::Result<ControlFlow<()>> {
        if self.batches.remaining == 0 {
            return Ok(ControlFlow::Break(()));
        }
        if self.batches.full() && !self.flush().await? {
            return Ok(ControlFlow::Break(()));
        }
        Ok(ControlFlow::Continue(()))
    }

    /// Sends the last, partly filled batch, or the scan's failure.
    async fn finish(mut self, outcome: restate_storage_api::Result<ScanOutcome<()>>) {
        let result = match outcome {
            Ok(_) => self.flush().await.map(|_| ()),
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            let _ = self.tx.send(Err(external(error))).await;
        }
    }
}

impl<S: LogicalSchema> RowSink<S> for BatchSink<S> {
    fn append_with(
        &mut self,
        write: impl FnOnce(&mut S::RowBuilder) -> restate_storage_api::Result<()>,
    ) -> ControlFlow<restate_storage_api::Result<()>> {
        self.batches.append_with(write)
    }
}

impl<S: LogicalSchema> AsyncRowSink<S> for BatchSink<S> {
    async fn reserve(&mut self) -> restate_storage_api::Result<ControlFlow<()>> {
        self.make_room().await
    }

    async fn record_done(&mut self) -> restate_storage_api::Result<ControlFlow<()>> {
        self.make_room().await
    }
}

/// Feeds the records of a scan that gives one row per record to a [`BatchSink`],
/// through `emit`.
struct RowsConsumer<T: Table, S: LogicalSchema, F> {
    sink: BatchSink<S>,
    emit: F,
    _table: PhantomData<fn() -> T>,
}

impl<T, S, F> RowConsumer for RowsConsumer<T, S, F>
where
    T: Table,
    for<'a> T::Record<'a>: Send,
    S: LogicalSchema,
    F: for<'a> FnMut(T::Record<'a>, &mut Batches<S>) -> restate_storage_api::Result<()>
        + Send
        + 'static,
{
    type Item<'a> = T::Record<'a>;

    async fn consume(
        &mut self,
        row: T::Record<'_>,
    ) -> restate_storage_api::Result<ControlFlow<()>> {
        // Each row leaves room for the next: a full batch is sent, or a reached
        // limit stops the scan, before the cursor moves on.
        (self.emit)(row, &mut self.sink.batches)?;
        self.sink.make_room().await
    }
}

fn external(error: StorageError) -> DataFusionError {
    DataFusionError::External(error.into())
}

/// Reads `request` from `snapshot` as one scan on the query IO pool, turning each
/// record into one row with `emit` and sending the rows to `sink`'s receiver in
/// batches. A failure is sent there too. Dropping the future releases the scan
/// and its snapshot holders if no worker has claimed it yet, and otherwise stops
/// the worker at its next wait.
///
/// The scan keeps the snapshot as long as it needs it, so this releases its
/// own `snapshot` once the scan is submitted. Waiting to send the last batch or
/// the failure then doesn't keep storage open.
pub(crate) async fn scan_rows_into<T, S, F>(
    snapshot: ReadSnapshot,
    executor: &QueryIoExecutor,
    request: Arc<T::Scan>,
    sink: BatchSink<S>,
    emit: F,
) -> Result<()>
where
    T: Table,
    for<'a> T::Record<'a>: Send,
    ReadSnapshot: CursorScan<T>,
    S: LogicalSchema,
    F: for<'a> FnMut(T::Record<'a>, &mut Batches<S>) -> restate_storage_api::Result<()>
        + Send
        + 'static,
{
    let tx = sink.tx.clone();
    let consumer = RowsConsumer::<T, S, F> {
        sink,
        emit,
        _table: PhantomData,
    };
    let scan = snapshot.scan::<T, _>(executor, S::NAME, request, consumer);
    drop(snapshot);
    let outcome = match scan {
        Ok(scan) => scan.await,
        Err(error) => Err(error),
    };
    match outcome {
        Ok(ScanOutcome::Exhausted(consumer) | ScanOutcome::Stopped(consumer)) => {
            consumer.sink.finish(Ok(ScanOutcome::Exhausted(()))).await;
        }
        Err(error) => {
            let _ = tx.send(Err(external(error))).await;
        }
    }
    Ok(())
}

/// Reads `prepared` from `snapshot` through binding `B` on the query IO pool and
/// sends its rows to `sink`'s receiver in batches; see [`scan_rows_into`].
pub(crate) async fn read_binding_into<B>(
    snapshot: ReadSnapshot,
    executor: &QueryIoExecutor,
    prepared: &PreparedRead<B::Table>,
    sink: BatchSink<B::Schema>,
) -> Result<()>
where
    B: SchemaBinding,
    ReadSnapshot: CursorScan<B::Table>,
    for<'a> <B::Table as Table>::Record<'a>: Copy + Send,
{
    let tx = sink.tx.clone();
    let read = B::read(&snapshot, executor, prepared, sink);
    drop(snapshot);
    let outcome = match read {
        Ok(read) => read.await,
        Err(error) => Err(error),
    };
    match outcome {
        Ok(ScanOutcome::Exhausted(sink) | ScanOutcome::Stopped(sink)) => {
            sink.finish(Ok(ScanOutcome::Exhausted(()))).await;
        }
        Err(error) => {
            let _ = tx.send(Err(external(error))).await;
        }
    }
    Ok(())
}
