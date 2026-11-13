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

use std::ops::ControlFlow;
use std::sync::Arc;

use restate_storage_api::{Result, StorageError};

use crate::scan::Table;
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
