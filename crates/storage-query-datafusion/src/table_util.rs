// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::PhysicalSortExpr;
use std::fmt::Write;
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

pub(crate) fn compute_ordering(schema: SchemaRef) -> Option<Vec<PhysicalSortExpr>> {
    let ordering = vec![PhysicalSortExpr {
        expr: col("partition_key", &schema).ok()?,
        options: Default::default(),
    }];

    Some(ordering)
}

#[inline]
pub(crate) fn format_using<'a>(output: &'a mut String, what: &impl std::fmt::Display) -> &'a str {
    output.clear();
    if let Err(e) = write!(output, "{}", what) {
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
