// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use std::fmt::Write;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tracing::error;

#[macro_export]
macro_rules! log_data_corruption_error {
    ($table_name:expr, $key:expr, $field_name:expr, $err:expr) => {
        tracing::error!(
            error = %$err,
            "Cannot convert field '{}' for '{}' table with row key {:?}. This might indicate a data corruption problem.",
            $field_name,
            $table_name,
            $key
        )
    };
}

pub(crate) fn find_sort_columns(
    ordering: &[String],
    schema: &Schema,
) -> Vec<Arc<dyn PhysicalExpr>> {
    // find the maximal ordered prefix from @ordering, that exists in @schema.
    ordering
        .iter()
        .map_while(|column_name| col(column_name, schema).ok())
        .collect()
}

pub(crate) fn make_ordering(columns: Vec<Arc<dyn PhysicalExpr>>) -> Vec<PhysicalSortExpr> {
    columns
        .into_iter()
        .map(|expr| PhysicalSortExpr {
            expr,
            options: Default::default(),
        })
        .collect()
}

#[inline]
pub(crate) fn format_using<'a>(output: &'a mut String, what: &impl std::fmt::Display) -> &'a str {
    output.clear();
    if let Err(e) = write!(output, "{what}") {
        error!(error = %e, "Cannot format the string")
    }
    output
}

pub(crate) trait Builder {
    fn new(projected_schema: SchemaRef) -> Self;

    fn full(&self) -> bool;

    fn empty(&self) -> bool;

    fn finish(self) -> datafusion::common::Result<RecordBatch>;
}

pub(crate) struct BatchSender<B: Builder> {
    projection: SchemaRef,
    builder: B,
    tx: Sender<datafusion::error::Result<RecordBatch>>,
}

impl<B: Builder> BatchSender<B> {
    pub(crate) fn new(
        projection: SchemaRef,
        tx: Sender<datafusion::error::Result<RecordBatch>>,
    ) -> Self {
        let builder = B::new(projection.clone());
        Self {
            projection,
            builder,
            tx,
        }
    }

    pub(crate) fn send(
        &mut self,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<datafusion::error::Result<RecordBatch>>>
    {
        let old_builder = std::mem::replace(&mut self.builder, B::new(self.projection.clone()));
        let result = old_builder.finish();
        self.tx.blocking_send(result)
    }

    pub(crate) fn builder_mut(&mut self) -> &mut B {
        &mut self.builder
    }

    pub(crate) fn full(&self) -> bool {
        self.builder.full()
    }
}

impl<B: Builder> Drop for BatchSender<B> {
    fn drop(&mut self) {
        if !self.builder.empty() {
            let _ = self.send();
        }
    }
}
