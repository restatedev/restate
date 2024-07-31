// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{
    HashJoinExec, StreamJoinPartitionMode, SymmetricHashJoinExec,
};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub(crate) struct JoinRewrite;

impl JoinRewrite {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

fn is_partition_key_column(arg: &PhysicalExprRef) -> bool {
    let Some(col) = arg.as_any().downcast_ref::<Column>() else {
        return false;
    };
    return col.name() == "partition_key";
}

impl PhysicalOptimizerRule for JoinRewrite {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let res = plan.transform_up(&|plan: Arc<dyn ExecutionPlan>| {
            let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(plan));
            };

            let has_partition_key = hash_join
                .on()
                .iter()
                .any(|(l, r)| is_partition_key_column(l) && is_partition_key_column(r));
            if !has_partition_key {
                return Ok(Transformed::no(plan));
            }

            let Ok(new_plan) = SymmetricHashJoinExec::try_new(
                hash_join.left().clone(),
                hash_join.right().clone(),
                hash_join.on().to_vec(),
                hash_join.filter().cloned(),
                hash_join.join_type(),
                hash_join.null_equals_null(),
                hash_join
                    .left()
                    .properties()
                    .output_ordering()
                    .map(|s| s.to_vec()),
                hash_join
                    .right()
                    .properties()
                    .output_ordering()
                    .map(|s| s.to_vec()),
                StreamJoinPartitionMode::Partitioned,
            ) else {
                return Ok(Transformed::no(plan));
            };

            let new_plan: Arc<dyn ExecutionPlan> = Arc::new(new_plan);

            Ok(Transformed::yes(new_plan))
        });

        res.data()
    }

    fn name(&self) -> &str {
        "join_rewrite"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
