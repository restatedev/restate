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
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::PhysicalSortExpr;

pub(crate) fn compute_ordering(schema: SchemaRef) -> Option<Vec<PhysicalSortExpr>> {
    let ordering = vec![PhysicalSortExpr {
        expr: col("partition_key", &schema).ok()?,
        options: Default::default(),
    }];

    Some(ordering)
}
