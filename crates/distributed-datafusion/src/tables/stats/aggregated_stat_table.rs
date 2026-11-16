// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::datasource::{TableProvider, ViewTable, provider_as_source};
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::logical_expr::{LogicalPlanBuilder, col};

const NUM_ENTRIES_COLUMN: &str = "num_entries";

/// Builds the public view of a partition-local gauge statistic.
///
/// Every raw column other than `num_entries` is a dimension. The raw provider is embedded directly in
/// the view rather than registered in the catalog. This keeps physical partition details private
/// while allowing DataFusion to push dimension filters through the aggregate and into the
/// partitioned scan.
pub(crate) fn gauge_stat_sum_view(
    name: &str,
    raw_table: Arc<dyn TableProvider>,
) -> datafusion::common::Result<ViewTable> {
    let schema = raw_table.schema();
    let group_expr = schema
        .fields()
        .iter()
        .filter(|field| field.name() != NUM_ENTRIES_COLUMN)
        .map(|field| col(field.name()));
    let logical_plan = LogicalPlanBuilder::scan(
        format!("__{name}_partitioned"),
        provider_as_source(raw_table),
        None,
    )?
    .aggregate(
        group_expr,
        [sum(col(NUM_ENTRIES_COLUMN)).alias(NUM_ENTRIES_COLUMN)],
    )?
    .build()?;

    Ok(ViewTable::new(logical_plan, None))
}
