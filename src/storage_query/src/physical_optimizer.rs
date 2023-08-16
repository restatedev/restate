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
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, SymmetricHashJoinExec};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub(crate) struct JoinRewrite;

impl JoinRewrite {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for JoinRewrite {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::No(plan));
            };

            let Ok(new_plan) = SymmetricHashJoinExec::try_new(
                hash_join.left().clone(),
                hash_join.right().clone(),
                hash_join.on().to_vec(),
                hash_join.filter().cloned(),
                hash_join.join_type(),
                hash_join.null_equals_null(),
            ) else {
                return Ok(Transformed::No(plan));
            };

            Ok(Transformed::Yes(Arc::new(new_plan)))
        })
    }

    fn name(&self) -> &str {
        "join_rewrite"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
