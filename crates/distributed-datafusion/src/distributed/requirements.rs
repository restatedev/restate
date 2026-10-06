// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Stage-scoped distribution and ordering. DataFusion owns exchange selection,
//! join-input alignment, and sort enforcement; Restate supplies the input budget
//! and prevents coordinator optimization from crossing a completed owner stage.

use std::fmt;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PhysicalExpr,
    PlanProperties, StatisticsArgs,
};
use datafusion_distributed::{NetworkBoundary, NetworkCoalesceExec, Stage};

/// Settle correctness requirements before assigning work to owners. Optional
/// fan-out is deferred until each stage has its own budget; a query-wide target
/// would otherwise widen independent branches and obscure their owner inputs.
pub(super) fn prepare(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut config = config.clone();
    config.optimizer.enable_round_robin_repartition = false;
    optimize(plan, &config)
}

/// Each input lane can supply work independently. Exchanges inside the stage
/// do not create more input capacity, so count leaves rather than current outputs.
fn optimize(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut lanes = 0usize;
    plan.apply(|node| {
        if node.children().is_empty() {
            lanes = lanes.saturating_add(node.output_partitioning().partition_count());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let mut config = config.clone();
    config.execution.target_partitions = config.execution.target_partitions.min(lanes).max(1);
    // Preserve ORDER BY and fetch while native enforcement removes and rebuilds
    // exchanges. In particular, a stage-local sort must remain partition-local.
    let plan = OutputRequirements::new_add_mode().optimize(plan, &config)?;
    let plan = EnsureRequirements::new().optimize(plan, &config)?;
    let plan = OutputRequirements::new_remove_mode().optimize(plan, &config)?;
    SanityCheckPlan::new().optimize(plan, &config)
}

pub(super) fn finalize(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = plan
        .transform_down(|plan| {
            let Some(boundary) = plan.downcast_ref::<NetworkCoalesceExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Stage::Local(stage) = boundary.input_stage() else {
                return internal_err!("owner stage must be local during planning");
            };
            let input = optimize(Arc::clone(&stage.plan), config)?;
            let boundary = super::owner_boundary(input, stage.query_id, stage.num)?;
            Ok(Transformed::new(
                Arc::new(StageInput(boundary)) as Arc<dyn ExecutionPlan>,
                true,
                TreeNodeRecursion::Jump,
            ))
        })?
        .data;
    let plan = optimize(plan, config)?;
    plan.transform_up(|plan| {
        if let Some(input) = plan.downcast_ref::<StageInput>() {
            Ok(Transformed::yes(Arc::clone(&input.0)))
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .map(|plan| plan.data)
}

/// An opaque input for coordinator optimization. Its properties and statistics
/// describe the completed stage, but native rules cannot rewrite that stage's
/// internals with the coordinator's budget. Removed before encoding/execution.
#[derive(Debug)]
struct StageInput(Arc<dyn ExecutionPlan>);

impl DisplayAs for StageInput {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt_as(t, f)
    }
}

impl ExecutionPlan for StageInput {
    fn name(&self) -> &str {
        "StageInput"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.0.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("stage inputs cannot have children");
        }
        Ok(self)
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        internal_err!("stage input escaped physical planning")
    }

    fn statistics_from_inputs(
        &self,
        inputs: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.0.statistics_from_inputs(inputs, args)
    }
}
