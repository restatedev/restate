// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coalesce::{LimitedBatchCoalescer, PushBatchStatus};
use datafusion::physical_plan::metrics::Time;
use tokio::sync::mpsc::Sender;
use tokio_util::sync::PollSender;

use restate_storage_api::scan_sink::{ScanFlow, ScanSink};

use crate::partition_store_scanner::ScanLocalPartition;
use crate::table_util::Builder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Takes rows.
    Scanning,
    /// Takes no more rows: the limit was reached, or the scan is over. The
    /// coalescer is finished, and only has batches left to send.
    Finished,
    /// Takes no more rows, and sends nothing after the error in `pending`.
    Failed,
}

/// The sink of a partition-store scan for a DataFusion query. Appends the
/// scanned rows of `S` to record batches, filters and coalesces them, and sends
/// them to the query's channel.
///
/// It never blocks: completed batches wait in the coalescer until the channel
/// has room, and the scan waits meanwhile. Once the scan is over, its owner
/// sends the rest with [`Self::close`].
pub struct RowSink<S: ScanLocalPartition> {
    builder: S::Builder,
    tx: PollSender<DataFusionResult<RecordBatch>>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    coalescer: LimitedBatchCoalescer,
    batch_size: usize,
    /// A batch, or the error that failed the scan, waiting for room in the
    /// channel.
    pending: Option<DataFusionResult<RecordBatch>>,
    state: State,
    /// Starts on the first row, stops when the sink is dropped.
    elapsed_compute: ElapsedCompute,
}

impl<S: ScanLocalPartition> RowSink<S> {
    pub(crate) fn new(
        projection: SchemaRef,
        tx: Sender<DataFusionResult<RecordBatch>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> Self {
        Self {
            builder: S::Builder::new(projection.clone()),
            tx: PollSender::new(tx),
            predicate,
            coalescer: LimitedBatchCoalescer::new(projection, batch_size, limit),
            batch_size,
            pending: None,
            state: State::Scanning,
            elapsed_compute: ElapsedCompute::new(elapsed_compute),
        }
    }

    /// Sends the rows that weren't sent yet, once the scan is over. Waits for
    /// room in the channel, and returns early if the receiver is gone.
    pub(crate) async fn close(mut self) {
        std::future::poll_fn(|cx| {
            if self.state == State::Scanning {
                let batch = self.builder.finish_and_new();
                self.push(batch, true);
            }
            self.poll_flush(cx).map(|_| ())
        })
        .await
    }

    /// Turns the appended rows into a batch once there are enough of them; the
    /// batch is sent later, once the channel has room.
    fn on_row_appended(&mut self) -> ControlFlow<()> {
        // TODO: this check could be based on selectivity metrics
        // ie, we could finish the batch when we predict that post-filter it will be big enough to send
        if self.state == State::Scanning && self.builder.num_rows() >= self.batch_size {
            let batch = self.builder.finish_and_new();
            self.push(batch, false);
        }
        match self.state {
            State::Scanning => ControlFlow::Continue(()),
            State::Finished | State::Failed => ControlFlow::Break(()),
        }
    }

    /// Fails the scan. `error` is sent before any batch that wasn't sent yet, and
    /// nothing is sent after it.
    fn fail(&mut self, error: DataFusionError) {
        self.state = State::Failed;
        self.pending = Some(Err(error));
    }

    /// Filters `batch` and pushes it to the coalescer. Finishes the coalescer if
    /// the batch is the `last` one, or once the limit is reached.
    fn push(&mut self, batch: DataFusionResult<RecordBatch>, last: bool) {
        let pushed = batch
            .and_then(|batch| self.apply_filter(batch))
            .and_then(|batch| self.coalescer.push_batch(batch));
        let limit_reached = match pushed {
            Ok(status) => matches!(status, PushBatchStatus::LimitReached),
            Err(e) => {
                self.fail(e);
                return;
            }
        };
        if last || limit_reached {
            self.state = State::Finished;
            if let Err(e) = self.coalescer.finish() {
                self.fail(e);
            }
        }
    }

    fn apply_filter(&self, batch: RecordBatch) -> DataFusionResult<RecordBatch> {
        let Some(predicate) = &self.predicate else {
            return Ok(batch);
        };

        let filter_result = predicate.evaluate(&batch)?;
        let filter_array = filter_result.into_array(batch.num_rows())?;
        let filter_array = as_boolean_array(&filter_array).map_err(|_| {
            DataFusionError::Internal(
                "Cannot create filter_array from non-boolean predicates".into(),
            )
        })?;

        Ok(filter_record_batch(&batch, filter_array)?)
    }

    /// Sends what's ready to send, as long as the channel has room. Fails once
    /// the receiver is gone, or the error that failed the scan is sent.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
        loop {
            let item = match self.pending.take() {
                Some(item) => item,
                None if self.state == State::Failed => return Poll::Ready(Err(())),
                None => match self.coalescer.next_completed_batch() {
                    Some(batch) => Ok(batch),
                    None => return Poll::Ready(Ok(())),
                },
            };
            match self.tx.poll_reserve(cx) {
                Poll::Ready(Ok(())) => {
                    if self.tx.send_item(item).is_err() {
                        return Poll::Ready(Err(()));
                    }
                }
                Poll::Ready(Err(_)) => return Poll::Ready(Err(())),
                Poll::Pending => {
                    self.pending = Some(item);
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<S: ScanLocalPartition> ScanFlow for RowSink<S> {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<ControlFlow<()>> {
        let flushed = std::task::ready!(self.poll_flush(cx));
        Poll::Ready(match (flushed, self.state) {
            (Ok(()), State::Scanning) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(()),
        })
    }
}

impl<'a, S: ScanLocalPartition> ScanSink<S::Item<'a>> for RowSink<S> {
    fn on_item(&mut self, row: S::Item<'a>) -> ControlFlow<()> {
        self.elapsed_compute.start();
        if let Err(err) = S::append_row(&mut self.builder, row) {
            self.fail(DataFusionError::External(Box::new(err)));
            return ControlFlow::Break(());
        }
        self.on_row_appended()
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
