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

use restate_storage_api::Result;

use crate::scan::{Scan, Table};
use crate::selection::{Selection, SelectionPrecision};

use super::{LogicalSchema, RowBuilder};

/// A projection-aware logical-row destination. Its projection must include every
/// column needed by remaining residuals, as well as output columns.
///
/// Each append is one row and one opportunity for backpressure or cancellation,
/// including when a physical record expands into many rows. A failed write is
/// terminal: the destination must discard its unfinished batch.
pub trait RowSink<S: LogicalSchema> {
    /// Appends a fully materialized logical row using the schema's adapter.
    fn append(&mut self, row: S::Row<'_>) -> ControlFlow<Result<()>> {
        self.append_with(|builder| {
            builder.append(row);
            Ok(())
        })
    }

    /// Writes one row directly through the projected builder. The closure may
    /// decode only requested values and must propagate errors for those values.
    /// A cancelled destination may stop without invoking the closure.
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

/// A query-side association between a logical schema and a physical table.
/// Neither schema nor table requires a reverse association. Multiple bindings may
/// read one table or expose one schema through different physical sources.
pub trait SchemaBinding: Sized + 'static {
    type Schema: LogicalSchema;
    type Table: Table;
    /// Owned continuation for one physical record. It must not borrow cursor
    /// bytes: the caller can move the reader between execution quanta. Single-row
    /// bindings use `()`; expanding bindings retain decoded dimensions/progress.
    type Materializer: Default + Send + 'static;

    /// Materializes at most one logical row, with capacity already reserved by
    /// the caller. `Done` releases this record's continuation; `More` resumes the
    /// same positioned record on the next call. Empty records return `Done`
    /// without emitting. No previously emitted row is replayed after a yield.
    /// The sink must accept the reserved row or fail; successful early stopping
    /// is the caller's responsibility between calls. Reset the continuation to
    /// `Default` before advancing to another physical record.
    fn materialize_next(
        record: <Self::Table as Table>::Record<'_>,
        state: &mut Self::Materializer,
        emit: &mut impl RowSink<Self::Schema>,
    ) -> Result<RecordProgress>;

    /// Translates logical restrictions to the table's native scan request.
    /// Unsupported acceleration widens candidates; real errors propagate.
    /// Precision is relative to this selection, not to the original SQL predicate.
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

    /// Adapts a prepared physical read into logical candidates. Callers retain
    /// SQL residuals and materialize their dependencies before final projection
    /// and unsafe LIMIT placement. This helper does not buffer or allocate rows.
    fn read<'s, S: Scan<Self::Table>, F>(
        backend: &'s S,
        prepared: &PreparedRead<Self::Table>,
        mut emit: F,
    ) -> Result<impl Future<Output = Result<()>> + Send + use<'s, Self, S, F>>
    where
        F: for<'a> FnMut(<Self::Schema as LogicalSchema>::Row<'a>) -> ControlFlow<Result<()>>
            + Send
            + 'static,
    {
        backend.scan(Arc::clone(&prepared.request), move |record| {
            Self::materialize(record, &mut emit)
        })
    }
}

/// An immutable physical request and the binding's precision assessment.
/// Sharing the request permits repeated execution without repeating preparation.
/// The precision describes logical candidate selection, not output ordering.
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
