// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Budgeted reads, which reserve a row's budget before decoding the row:
//! [`budgeted_get`] for one key, and [`BudgetedScan`] for the rows of a scan.

use std::pin::Pin;
use std::task::{Context, Poll};

use std::sync::Arc;

use bytes::Bytes;
use futures::future::{self, Either};
use futures::{Stream, ready};
use tokio::sync::mpsc;

use restate_core::io::{Gate, IoExecutor};
use restate_memory::{
    ClaimedMemoryPool, LentMemoryPool, LocalMemoryLease, OutOfMemory, PinHandle,
    PinnableMemoryStream, ReserveMemory,
};
use restate_rocksdb::reads::{InlineVisitor, IterationError, IterationOutcome, IteratorVisitor};
use restate_rocksdb::{Inline, IterAction, RocksDbPerfExt, RocksDbReadPerfGuard};
use restate_storage_api::{BudgetedReadError, StorageError};

use crate::keys::KeyKind;
use crate::partition_store::StorageRead;
use crate::scan::PhysicalScan;
use crate::{PartitionReadSnapshot, TableScanIterationDecision};

/// How many rows a budgeted scan of an invocation's journal or state reads on
/// its caller's task before it continues on a storage worker, so that a long
/// scan doesn't hold up the task even when all of its rows are in memory.
pub(crate) const MAX_INLINE_ROWS: usize = 128;

/// A row of a budgeted read, or why the read is incomplete.
pub(crate) type Row<T> = Result<T, BudgetedReadError>;

/// Reads the value of `key`. Its `cost` is reserved from `budget` before
/// `decode` gets the value with its lease. Returns `None` if the key has no
/// value.
///
/// Waits for the budget if needed, without holding on to the value: it's read
/// again afterwards, as it may have changed unless `storage` is a snapshot.
pub(crate) async fn budgeted_get<S, B, T>(
    storage: &S,
    name: &'static str,
    key_kind: KeyKind,
    key: &[u8],
    budget: &mut B,
    cost: impl Fn(&[u8]) -> usize,
    decode: impl FnOnce(&[u8], LocalMemoryLease) -> Result<T, StorageError>,
) -> Row<Option<T>>
where
    S: StorageRead,
    B: ReserveMemory,
{
    async move {
        let mut reserved: Option<LocalMemoryLease> = None;
        loop {
            let cost = {
                let Some(value) = storage.get(key_kind, key)? else {
                    return Ok(None);
                };

                let cost = cost(&value);
                match &mut reserved {
                    // Reserved while waiting, and the value hasn't grown since.
                    Some(lease) if lease.size() >= cost => lease.shrink(lease.size() - cost),
                    _ => {
                        drop(reserved.take());
                        reserved = budget.try_reserve(cost);
                    }
                }
                if let Some(lease) = reserved.take() {
                    return Ok(Some(decode(&value, lease)?));
                }
                cost
            };
            // The value is dropped: it isn't kept in memory while waiting.
            reserved = Some(budget.reserve(cost).await?);
        }
    }
    .with_read_perf(name)
    .await
}

/// A budgeted scan of a snapshot that has been prepared but hasn't read
/// anything yet, from
/// [`PartitionStoreSnapshot::prepare_budgeted_scan`](crate::PartitionStoreSnapshot::prepare_budgeted_scan).
/// Running it says where its rows are read.
#[must_use = "run the scan"]
pub(crate) struct BudgetedScan<'a, C, D> {
    /// The gate the scan counts in, inside the database of `snapshot`.
    pub(crate) gate: &'a Arc<Gate>,
    pub(crate) snapshot: &'a PartitionReadSnapshot,
    /// Labels the scan's RocksDB perf metrics.
    pub(crate) name: &'static str,
    pub(crate) scan: PhysicalScan<Bytes>,
    pub(crate) budget: ClaimedMemoryPool<'a>,
    pub(crate) cost: C,
    pub(crate) decode: D,
}

impl<C, D> BudgetedScan<'_, C, D> {
    /// Reads rows on the calling task while they are in memory and their budget
    /// is free. From the first row that would need disk I/O or waiting for
    /// budget, and after `max_inline_rows` at the latest, the scan continues on
    /// a storage worker of `executor`.
    ///
    /// The scan reads ahead of the returned stream, as far as the budget
    /// allows. The budget is [lent](ClaimedMemoryPool::lend) to it until it's
    /// done. Dropping the stream cancels the part on the storage worker.
    ///
    /// The rows read on the calling task are one perf measurement, and the part
    /// on the storage worker another.
    pub(crate) fn run_cached_then_offload<T>(
        self,
        max_inline_rows: usize,
        executor: &impl IoExecutor,
    ) -> Result<impl PinnableMemoryStream<Item = Row<T>> + Send + 'static, StorageError>
    where
        T: Send + 'static,
        C: Fn(&[u8], &[u8]) -> usize + Send + 'static,
        D: FnMut(&[u8], &[u8], LocalMemoryLease) -> TableScanIterationDecision<T> + Send + 'static,
    {
        let Self {
            gate,
            snapshot,
            name,
            scan,
            budget,
            cost,
            decode,
        } = self;
        let (pool, pins) = budget.lend();
        let (tx, rows) = mpsc::unbounded_channel();
        let visitor = BudgetedVisitor {
            pool,
            tx,
            cost,
            decode,
        };
        let scan = snapshot.prepare_scan(gate, scan, visitor);
        let inline = {
            let _x = RocksDbReadPerfGuard::new(name);
            scan.call_cached(max_inline_rows)
                .map_err(|_| StorageError::OperationalError)?
        };
        let scan = match inline {
            Inline::Done(result) => Either::Left(future::ready(scan_result(result))),
            Inline::WouldBlock(rest) => {
                let result = executor
                    .spawn_cancellable(rest.with_read_perf(name))
                    .map_err(|_| StorageError::OperationalError)?;
                Either::Right(async move {
                    match result.await {
                        Ok(result) => scan_result(result),
                        Err(_) => Err(StorageError::OperationalError.into()),
                    }
                })
            }
        };
        Ok(RowStream {
            rows,
            pins,
            scan: Some(scan),
        })
    }
}

/// What a scan that ended tells its reader. Drops the scan's visitor, which
/// closes the channel of its rows.
fn scan_result<V>(
    result: Result<IterationOutcome<V>, IterationError<OutOfMemory>>,
) -> Result<(), BudgetedReadError> {
    match result {
        Ok(_) => Ok(()),
        Err(IterationError::RocksDb(err)) => Err(StorageError::Generic(err.into()).into()),
        Err(IterationError::Visitor(oom)) => Err(oom.into()),
        Err(IterationError::Cancelled) => Err(StorageError::OperationalError.into()),
    }
}

/// Reserves each row's budget from a lent pool, then lets `decode` build the
/// row with its lease, and sends it to a [`RowStream`]. Fails the scan if the
/// budget can't cover a row.
///
/// On its caller's task, it only takes a row whose budget is free. On a
/// storage worker, it waits for the budget.
///
/// Dropping it gives the lent pool back.
struct BudgetedVisitor<T, C, D> {
    pool: LentMemoryPool,
    tx: mpsc::UnboundedSender<Row<T>>,
    cost: C,
    decode: D,
}

impl<T, C, D> IteratorVisitor for BudgetedVisitor<T, C, D>
where
    T: Send + 'static,
    C: Fn(&[u8], &[u8]) -> usize + Send + 'static,
    D: FnMut(&[u8], &[u8], LocalMemoryLease) -> TableScanIterationDecision<T> + Send + 'static,
{
    type Error = OutOfMemory;

    async fn visit(&mut self, key: &[u8], value: &[u8]) -> Result<IterAction, OutOfMemory> {
        let lease = self.pool.reserve((self.cost)(key, value)).await?;
        Ok(self.send(key, value, lease))
    }
}

impl<T, C, D> InlineVisitor for BudgetedVisitor<T, C, D>
where
    T: Send + 'static,
    C: Fn(&[u8], &[u8]) -> usize + Send + 'static,
    D: FnMut(&[u8], &[u8], LocalMemoryLease) -> TableScanIterationDecision<T> + Send + 'static,
{
    fn try_visit(&mut self, key: &[u8], value: &[u8]) -> Option<Result<IterAction, OutOfMemory>> {
        // Without the budget, the storage worker waits for it, or fails the
        // scan if it can't cover the row.
        let lease = self.pool.try_reserve((self.cost)(key, value))?;
        Some(Ok(self.send(key, value, lease)))
    }
}

impl<T, C, D> BudgetedVisitor<T, C, D>
where
    D: FnMut(&[u8], &[u8], LocalMemoryLease) -> TableScanIterationDecision<T>,
{
    /// Lets `decode` build the row with the budget reserved for it, and sends
    /// the row if there is one. Returns how the scan goes on.
    fn send(&mut self, key: &[u8], value: &[u8], lease: LocalMemoryLease) -> IterAction {
        let (row, more) = match (self.decode)(key, value, lease) {
            TableScanIterationDecision::Emit(row) => (row, true),
            TableScanIterationDecision::BreakWith(row) => (row, false),
            TableScanIterationDecision::Continue => return IterAction::Next,
            TableScanIterationDecision::Break => return IterAction::Stop,
        };
        // Stop at the first error, and once the reader is gone.
        let more = more && row.is_ok();
        let sent = self.tx.send(row.map_err(BudgetedReadError::from)).is_ok();
        if sent && more {
            IterAction::Next
        } else {
            IterAction::Stop
        }
    }
}

/// Yields the rows that a [`BudgetedVisitor`] sends during `scan`, and tells
/// the visitor's pool what memory its reader pins.
///
/// Polls `scan` alongside the rows: the visitor is only dropped once the scan
/// is done. An error from the scan is an incomplete read: it's yielded, never
/// turned into the end of the stream. Dropping the stream drops `scan`.
#[pin_project::pin_project]
struct RowStream<T, F> {
    rows: mpsc::UnboundedReceiver<Row<T>>,
    pins: PinHandle,
    /// `None` once the scan is done.
    #[pin]
    scan: Option<F>,
}

impl<T, F> Stream for RowStream<T, F>
where
    F: Future<Output = Result<(), BudgetedReadError>>,
{
    type Item = Row<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            let rows_pending = match this.rows.poll_recv(cx) {
                Poll::Ready(Some(row)) => return Poll::Ready(Some(row)),
                Poll::Ready(None) => false,
                Poll::Pending => true,
            };
            let Some(scan) = this.scan.as_mut().as_pin_mut() else {
                return if rows_pending {
                    Poll::Pending
                } else {
                    Poll::Ready(None)
                };
            };
            // The channel can close before the scan's result arrives: poll the
            // scan either way, so that its error isn't lost.
            let result = ready!(scan.poll(cx));
            this.scan.set(None);
            if let Err(err) = result {
                return Poll::Ready(Some(Err(err)));
            }
            // The scan's visitor is gone now: drain the rows it sent.
        }
    }
}

impl<T, F> PinnableMemoryStream for RowStream<T, F>
where
    F: Future<Output = Result<(), BudgetedReadError>>,
{
    fn pin_memory(self: Pin<&mut Self>, amount: usize) {
        self.pins.pin(amount)
    }

    fn unpin_memory(self: Pin<&mut Self>, amount: usize) {
        self.pins.unpin(amount)
    }
}
