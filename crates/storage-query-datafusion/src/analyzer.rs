// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{Join, LogicalPlan};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::Expr;

#[derive(Debug)]
pub(crate) struct UseSymmetricHashJoinWhenPartitionKeyIsPresent;

impl UseSymmetricHashJoinWhenPartitionKeyIsPresent {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for UseSymmetricHashJoinWhenPartitionKeyIsPresent {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        let res = plan.transform_up(&|plan| {
            let LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equality,
            }) = &plan
            else {
                return Ok(Transformed::no(plan));
            };

            // if a partition_key exists add that to the join, otherwise use u64::max.
            let left_pk = left
                .schema()
                .qualified_fields_with_unqualified_name("partition_key")
                .first()
                .cloned()
                .map(|c| Column::new(c.0.cloned(), c.1.name().to_owned()));

            // if a partition_key exists add that to the join, otherwise use u64::max.
            let right_pk = right
                .schema()
                .qualified_fields_with_unqualified_name("partition_key")
                .first()
                .cloned()
                .map(|c| Column::new(c.0.cloned(), c.1.name().to_owned()));

            let both_have_pk = left_pk.is_some() && right_pk.is_some();
            if !both_have_pk {
                return Ok(Transformed::no(plan));
            }
            //
            // both sides have a partition_key, lets do a equijoin.
            //
            let mut new_on = on.to_vec();
            new_on.push((
                Expr::Column(left_pk.unwrap()),
                Expr::Column(right_pk.unwrap()),
            ));

            let new_plan = LogicalPlan::Join(Join {
                left: left.clone(),
                right: right.clone(),
                on: new_on,
                filter: filter.clone(),
                join_type: *join_type,
                join_constraint: *join_constraint,
                schema: schema.clone(),
                null_equality: *null_equality,
            });

            Ok(Transformed::yes(new_plan))
        });

        res.data()
    }

    fn name(&self) -> &str {
        "join_checker"
    }
}
