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
use datafusion::error::DataFusionError;
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
            }) = &plan else {
                return Ok(Transformed::No(plan));
            };

            let mut new_on = on.to_vec();

            let left_pk = left.schema().field_with_unqualified_name("partition_key");
            let right_pk = right.schema().field_with_unqualified_name("partition_key");

            if left_pk.is_ok() != right_pk.is_ok() {
                return Err(DataFusionError::Plan(
                    "Unable to find a partition_key column, which is required in a join"
                        .to_string(),
                ));
            }
            if left_pk.is_err() {
                return Ok(Transformed::No(plan));
            }
            let left_pk = col(left_pk.unwrap().qualified_column());
            let right_pk = col(right_pk.unwrap().qualified_column());

            new_on.push((left_pk, right_pk));

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
