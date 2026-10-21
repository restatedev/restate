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

use super::LogicalSchema;

/// A query-side association between a logical schema and a physical table.
/// Neither schema nor table requires a reverse association. Multiple bindings may
/// read one table or expose one schema through different physical sources.
pub trait SchemaBinding: Sized + 'static {
    type Schema: LogicalSchema;
    type Table: Table;

    /// Translates logical restrictions to the table's native scan request.
    /// Unsupported acceleration widens candidates; real errors propagate.
    /// Precision is relative to this selection, not to the original SQL predicate.
    fn prepare(selection: &Selection<Self::Schema>) -> Result<PreparedRead<Self::Table>>;

    /// Materializes zero or more logical rows from one physical record. Borrowed
    /// emission allows dimensions to be reused when one record expands to many rows.
    fn materialize(
        record: <Self::Table as Table>::Record<'_>,
        emit: &mut impl for<'a> FnMut(
            <Self::Schema as LogicalSchema>::Row<'a>,
        ) -> ControlFlow<Result<()>>,
    ) -> ControlFlow<Result<()>>;

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
