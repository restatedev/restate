// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion_expr::{col, Join, LogicalPlan};

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
        plan.transform_up(&|plan| {
            let LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equals_null,
            }) = &plan
            else {
                return Ok(Transformed::No(plan));
            };

            // if a partition_key exists add that to the join, otherwise use u64::max.
            let left_pk = left
                .schema()
                .field_with_unqualified_name("partition_key")
                .map(|df| col(df.qualified_column()))
                .ok();

            // if a partition_key exists add that to the join, otherwise use u64::max.
            let right_pk = right
                .schema()
                .field_with_unqualified_name("partition_key")
                .map(|df| col(df.qualified_column()))
                .ok();

            let both_have_pk = left_pk.is_some() && right_pk.is_some();
            if !both_have_pk {
                return Ok(Transformed::No(plan));
            }
            //
            // both sides have a partition_key, lets do a equijoin.
            //
            let mut new_on = on.to_vec();
            new_on.push((left_pk.unwrap(), right_pk.unwrap()));
            let new_plan = LogicalPlan::Join(Join {
                left: left.clone(),
                right: right.clone(),
                on: new_on,
                filter: filter.clone(),
                join_type: *join_type,
                join_constraint: *join_constraint,
                schema: schema.clone(),
                null_equals_null: *null_equals_null,
            });

            Ok(Transformed::Yes(new_plan))
        })
    }

    fn name(&self) -> &str {
        "join_checker"
    }
}
