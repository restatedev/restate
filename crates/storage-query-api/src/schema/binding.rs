// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Schema bindings: how a physical table's records become the rows of a logical
//! schema, and how a logical selection becomes the table's read request.
//!
//! # Concepts
//!
//! - **Binding** ([`SchemaBinding`]): the query-side pairing of one logical
//!   schema with one physical table. Neither side knows the other: a table can
//!   serve several schemas, and a schema can be read from several tables.
//! - **Prepared read** ([`PreparedRead`]): the table's request for a selection,
//!   shared behind an `Arc` so it can be executed many times, together with the
//!   **precision** of what it selects: exact, or a superset the caller must still
//!   filter with the original predicate, the *residual*.
//! - **Record** and **row**: a record is what the table yields, often bytes
//!   borrowed from the iterator; a row is the schema's logical row. One record
//!   becomes zero rows (filtered out), one row, or many rows when the record
//!   holds a list.
//! - **Sink** ([`RowSink`]): where rows go. It carries the query's projection, so
//!   a binding decodes only the values the query asked for, and it is where the
//!   reader can stop, be cancelled or apply backpressure.
//! - **Materializer**: a binding's owned continuation for one record that expands
//!   into many rows, so the reader can yield between rows and resume later, even
//!   on another thread ([`RecordProgress::More`]).
//!
//! # The flow
//!
//! ```text
//! selection ──SchemaBinding::prepare──▶ PreparedRead { request, precision }
//!                                                │
//! cursor.advance() ──▶ record ──materialize_next(record, state, sink)──▶ row
//!                        ▲              │ Done: on to the next record
//!                        └──────────────┘ More: same record, next call
//! ```
//!
//! For example, a binding of the entry-status table, keyed by
//! `partition_key | entry_id`, to `sys_vqueue_entry_status` turns the clause
//! `entry_id = 'inv_1…'` into a point lookup, reports it as exact relative to
//! that selection, and decodes the stored entry into one row. A binding over a
//! record that holds many rows returns `More` until the record is drained.
//!
//! A sink driven by `materialize_next` has already reserved the row: it must
//! write it or fail. A sink that stopped successfully instead would lose that
//! row on resume, which is why [`row_written`] reports it as an error.

use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::sync::Arc;

use restate_storage_api::scan::{RowConsumer, ScanOutcome};
use restate_storage_api::{Result, StorageError};

use crate::scan::{CursorScan, SCAN_SLICE, Scan, Table, yield_now};
use crate::selection::{Selection, SelectionPrecision};

use super::{LogicalSchema, RowBuilder};

/// Where materialized rows go. The sink carries the query's projection, which
/// must include every column a residual filter needs as well as the output
/// columns.
///
/// Each append is one row and one chance to stop, cancel or apply backpressure,
/// also when one physical record expands into many rows. A failed write is
/// final: the sink must discard its unfinished batch.
pub trait RowSink<S: LogicalSchema> {
    /// Appends a fully materialized logical row using the schema's adapter.
    fn append(&mut self, row: S::Row<'_>) -> ControlFlow<Result<()>> {
        self.append_with(|builder| {
            builder.append(row);
            Ok(())
        })
    }

    /// Writes one row through the projected builder, so that `write` decodes only
    /// the values the projection asks for; errors of those values propagate. A
    /// cancelled sink may stop without calling `write`, except when it is driven
    /// by [`SchemaBinding::materialize_next`], which has reserved the row: there
    /// it writes the row or fails (see [`row_written`]).
    fn append_with(
        &mut self,
        write: impl FnOnce(&mut S::RowBuilder) -> Result<()>,
    ) -> ControlFlow<Result<()>>;
}

/// A projected sink that can await capacity before a materialization call.
///
/// A successful reservation permits at most one row through [`RowSink`]. The
/// subsequent append must write or fail, never stop successfully. A record can
/// produce no row, so reserving again must also work without an intervening
/// append. Cancellation may drop the sink or a pending reservation at any time.
pub trait AsyncRowSink<S: LogicalSchema>: RowSink<S> + Send + 'static {
    /// Waits for room for one row, or stops before reserving it. Successful stops
    /// belong here, between calls to [`SchemaBinding::materialize_next`].
    fn reserve(&mut self) -> impl Future<Output = Result<ControlFlow<()>>>;
}

/// Whether the current physical record has more logical rows to materialize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordProgress {
    More,
    Done,
}

/// The query-side pairing of one logical schema with one physical table: it
/// prepares a selection into the table's request and turns the table's records
/// into the schema's rows. Neither side refers back to the binding, so several
/// bindings may read one table, or expose one schema from different tables.
pub trait SchemaBinding: Sized + 'static {
    type Schema: LogicalSchema;
    type Table: Table;
    /// The binding's owned continuation for one physical record, kept between
    /// calls of [`Self::materialize_next`]. It must not borrow the record's bytes,
    /// because the reader may move to another thread between calls. A binding
    /// that yields one row per record uses `()`; one that expands a record into
    /// many rows keeps what it decoded and how far it got.
    type Materializer: Default + Send + 'static;

    /// Writes at most one row of `record` to `emit`, which has already reserved
    /// room for it. `Done` means the record is finished and `state` can be reset;
    /// `More` means the same record has further rows, so the caller passes the
    /// same `record` and `state` on the next call. A record that yields no row
    /// returns `Done` without writing. A row written before a yield is never
    /// written again after it.
    ///
    /// Because the row is reserved, the sink must write it or fail; a sink that
    /// stops successfully here would lose the row on resume, so the binding
    /// reports that as an error ([`row_written`] does the mapping). Stopping early
    /// is the caller's business between calls. Reset `state` to `Default` before
    /// moving to another record.
    fn materialize_next(
        record: <Self::Table as Table>::Record<'_>,
        state: &mut Self::Materializer,
        emit: &mut impl RowSink<Self::Schema>,
    ) -> Result<RecordProgress>;

    /// Turns a logical selection into the table's read request. A clause the
    /// table cannot use widens the request and makes the result inexact; a literal
    /// that cannot be converted is an error. Precision is relative to this
    /// selection, not to the SQL predicate it came from.
    fn prepare(selection: &Selection<Self::Schema>) -> Result<PreparedRead<Self::Table>>;

    /// Materializes zero or more complete logical rows from one physical record. Borrowed
    /// emission allows dimensions to be reused when one record expands to many rows.
    fn materialize(
        record: <Self::Table as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(
            <Self::Schema as LogicalSchema>::Row<'a>,
        ) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>>;

    /// Materializes candidates into a projected destination. Bindings may use
    /// its fallible writer to avoid decoding unrequested values, while still
    /// validating record structure and identity. The default uses complete rows.
    fn materialize_projected(
        record: <Self::Table as Table>::Record<'_>,
        emit: &mut impl RowSink<Self::Schema>,
    ) -> ControlFlow<Result<()>> {
        Self::materialize(record, &mut |row| emit.append(row))
    }

    /// Runs a prepared read into an awaitable projected sink. Physical records
    /// are cheap, copyable views so an expanding binding can revisit the same
    /// record without copying its bytes. Materialization yields every
    /// [`SCAN_SLICE`] calls, including across record boundaries; physical skips
    /// have their own budget.
    ///
    /// The caller retains SQL residuals and their projection dependencies, and
    /// applies limits only where those residuals permit. On success the sink is
    /// returned for final flushing or the next read; errors discard it.
    fn read<S, C>(
        backend: &S,
        executor: &S::Executor,
        prepared: &PreparedRead<Self::Table>,
        sink: C,
    ) -> Result<impl Future<Output = Result<ScanOutcome<C>>> + Send + 'static>
    where
        S: Scan + CursorScan<Self::Table>,
        C: AsyncRowSink<Self::Schema>,
        for<'a> <Self::Table as Table>::Record<'a>: Copy + Send,
    {
        let result = backend.scan::<Self::Table, _>(
            executor,
            Self::Schema::NAME,
            Arc::clone(prepared.request()),
            BindingConsumer::<Self, C>::new(sink),
        )?;
        Ok(async move {
            result
                .await
                .map(|outcome| outcome.map(|consumer| consumer.sink))
        })
    }
}

/// Turns each borrowed physical record into logical rows, reserving output room
/// before each materialization call and yielding even across record boundaries.
struct BindingConsumer<B, C> {
    sink: C,
    /// Materialization calls left before the next cooperative yield, not rows.
    until_yield: usize,
    _binding: PhantomData<fn() -> B>,
}

impl<B, C> BindingConsumer<B, C> {
    fn new(sink: C) -> Self {
        Self {
            sink,
            until_yield: SCAN_SLICE,
            _binding: PhantomData,
        }
    }
}

impl<B, C> RowConsumer for BindingConsumer<B, C>
where
    B: SchemaBinding,
    C: AsyncRowSink<B::Schema>,
    for<'a> <B::Table as Table>::Record<'a>: Copy + Send,
{
    type Item<'a> = <B::Table as Table>::Record<'a>;

    async fn consume(&mut self, record: Self::Item<'_>) -> Result<ControlFlow<()>> {
        let mut state = B::Materializer::default();
        loop {
            if self.sink.reserve().await?.is_break() {
                return Ok(ControlFlow::Break(()));
            }
            let progress = B::materialize_next(record, &mut state, &mut self.sink)?;
            self.until_yield -= 1;
            if self.until_yield == 0 {
                self.until_yield = SCAN_SLICE;
                yield_now().await;
            }
            if progress == RecordProgress::Done {
                return Ok(ControlFlow::Continue(()));
            }
        }
    }
}

/// The result of writing the row reserved by [`SchemaBinding::materialize_next`]
/// through a [`RowSink`]: a sink that stopped without writing it breaks the
/// contract above and is reported as an error, never as progress.
pub fn row_written(outcome: ControlFlow<Result<()>>) -> Result<()> {
    match outcome {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(Err(error)) => Err(error),
        ControlFlow::Break(Ok(())) => Err(StorageError::Generic(anyhow::anyhow!(
            "row sink stopped inside materialize_next, losing the reserved row"
        ))),
    }
}

/// A prepared request, shared so it can be executed many times without preparing
/// again, and the precision of what it selects: exact, or a superset that the
/// residual filter must narrow. Precision says nothing about output order.
pub struct PreparedRead<T: Table> {
    request: Arc<T::Scan>,
    precision: SelectionPrecision,
}

impl<T: Table> PreparedRead<T> {
    pub fn new(request: T::Scan, precision: SelectionPrecision) -> Self {
        Self {
            request: Arc::new(request),
            precision,
        }
    }

    pub fn precision(&self) -> SelectionPrecision {
        self.precision
    }

    pub fn request(&self) -> &Arc<T::Scan> {
        &self.request
    }
}

impl<T: Table> Clone for PreparedRead<T> {
    fn clone(&self) -> Self {
        Self {
            request: Arc::clone(&self.request),
            precision: self.precision,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::FutureExt;
    use futures::channel::oneshot;

    use super::*;
    use fixture::{Expanded, ExpandedBatchBuilder, ExpandedRow};

    // The declaration also generates selection and documentation helpers, which
    // this materialization fixture does not use.
    #[allow(dead_code)]
    mod fixture {
        use datafusion::arrow::datatypes::DataType;

        use crate::{ShardingScheme, define_schema};

        define_schema! {
            expanded {
                name: "expanded",
                sharding: ShardingScheme::Unsharded(None),
                columns { value: u32 => DataType::UInt32 }
            }
        }
    }

    struct Binding;

    impl Table for Binding {
        type Record<'a> = &'a [u32];
        type Scan = ();
    }

    impl SchemaBinding for Binding {
        type Schema = Expanded;
        type Table = Self;
        type Materializer = usize;

        fn prepare(_: &Selection<Expanded>) -> Result<PreparedRead<Self>> {
            Ok(PreparedRead::new((), SelectionPrecision::Exact))
        }

        fn materialize_next(
            record: &[u32],
            position: &mut usize,
            emit: &mut impl RowSink<Expanded>,
        ) -> Result<RecordProgress> {
            let Some(&value) = record.get(*position) else {
                return Ok(RecordProgress::Done);
            };
            row_written(emit.append(&ExpandedRow { value }))?;
            *position += 1;
            Ok(if *position == record.len() {
                RecordProgress::Done
            } else {
                RecordProgress::More
            })
        }

        fn materialize(
            record: &[u32],
            emit: &mut impl for<'a> FnMut(&'a ExpandedRow) -> ControlFlow<Result<()>>,
        ) -> ControlFlow<Result<()>> {
            for &value in record {
                emit(&ExpandedRow { value })?;
            }
            ControlFlow::Continue(())
        }
    }

    struct Sink {
        builder: ExpandedBatchBuilder,
        written: Arc<AtomicUsize>,
        limit: usize,
        wait: Option<oneshot::Receiver<()>>,
        stop_in_append: bool,
    }

    impl AsyncRowSink<Expanded> for Sink {
        async fn reserve(&mut self) -> Result<ControlFlow<()>> {
            if let Some(wait) = self.wait.take() {
                wait.await.unwrap();
            }
            Ok(if self.builder.num_rows() == self.limit {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            })
        }
    }

    impl RowSink<Expanded> for Sink {
        fn append_with(
            &mut self,
            write: impl FnOnce(&mut ExpandedBatchBuilder) -> Result<()>,
        ) -> ControlFlow<Result<()>> {
            if self.stop_in_append {
                return ControlFlow::Break(Ok(()));
            }
            match write(&mut self.builder) {
                Ok(()) => {
                    self.written.fetch_add(1, Ordering::Relaxed);
                    ControlFlow::Continue(())
                }
                Err(error) => ControlFlow::Break(Err(error)),
            }
        }
    }

    #[test]
    fn expanding_records_yield_resume_and_stop_only_between_reserved_rows() {
        let written = Arc::new(AtomicUsize::new(0));
        let mut consumer = BindingConsumer::<Binding, _>::new(Sink {
            builder: ExpandedBatchBuilder::new(Expanded::arrow_schema()),
            written: Arc::clone(&written),
            limit: usize::MAX,
            wait: None,
            stop_in_append: false,
        });
        // The logical budget survives record boundaries: 100 rows in the first
        // record leave only 28 before the next yield in the second record.
        let record: Vec<u32> = (0..200).collect();
        assert!(
            futures::executor::block_on(consumer.consume(&record[..100]))
                .unwrap()
                .is_continue()
        );
        let mut consume = Box::pin(consumer.consume(&record[100..]));
        assert!(consume.as_mut().now_or_never().is_none());
        assert_eq!(written.load(Ordering::Relaxed), SCAN_SLICE);
        assert!(futures::executor::block_on(consume).unwrap().is_continue());
        assert_eq!(written.load(Ordering::Relaxed), 200);

        // Capacity can suspend while borrowing the record. Resumption writes
        // exactly the reserved rows; a limit stops before the next reservation.
        consumer.sink.limit = 202;
        let (release, wait) = oneshot::channel();
        consumer.sink.wait = Some(wait);
        let mut consume = Box::pin(consumer.consume(&record));
        assert!(consume.as_mut().now_or_never().is_none());
        assert_eq!(written.load(Ordering::Relaxed), 200);
        release.send(()).unwrap();
        assert!(futures::executor::block_on(consume).unwrap().is_break());
        assert_eq!(written.load(Ordering::Relaxed), 202);
        let batch = consumer.sink.builder.finish().unwrap();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::UInt32Array>()
            .unwrap();
        assert_eq!(
            values.values().as_ref(),
            (0..200).chain([0, 1]).collect::<Vec<_>>()
        );

        // A sink violating the write-or-error contract must fail, not report
        // progress or turn a partially written record into successful exhaustion.
        consumer.sink.stop_in_append = true;
        assert!(futures::executor::block_on(consumer.consume(&record)).is_err());
    }
}
