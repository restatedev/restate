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
use datafusion::physical_optimizer::limit_pushdown::LimitPushdown;
use datafusion::physical_optimizer::output_requirements::OutputRequirements;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::statistics::StatisticsContext;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PhysicalExpr,
    PlanProperties, StatisticsArgs,
};
use datafusion_distributed::{NetworkBoundary, NetworkCoalesceExec, Stage};

use super::source::PartitionReadExec;

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
    // LimitPushdown can fold a global limit into a coalesce's fetch.
    // EnsureRequirements removes coalesces, and OutputRequirements only captures
    // fetches on sorts/ordered merges. Make the row bound an explicit operator
    // before rebuilding distribution, even in subqueries and templates.
    let plan = plan
        .transform_down(|plan| {
            if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>()
                && let Some(fetch) = coalesce.fetch()
            {
                let input = Arc::new(CoalescePartitionsExec::new(Arc::clone(coalesce.input())));
                let limit: Arc<dyn ExecutionPlan> =
                    Arc::new(GlobalLimitExec::new(input, 0, Some(fetch)));
                return Ok(Transformed::yes(limit));
            }
            Ok(Transformed::no(plan))
        })?
        .data;
    // Union branches are independent pipelines. Finalize each with its own
    // input budget, then prevent the enclosing stage's target from widening
    // every branch again (for example, a filter on each single-partition scan).
    // A partition read's template settles on one partition, its unions included
    // ([`ensure_requirements`]).
    let plan = plan
        .transform_down(|plan| {
            if plan.is::<PartitionReadExec>() {
                return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
            }
            let Some(union) = plan.downcast_ref::<UnionExec>() else {
                return Ok(Transformed::no(plan));
            };
            let inputs = union
                .inputs()
                .iter()
                .map(|input| {
                    let input = optimize(Arc::clone(input), config)?;
                    Ok(Arc::new(PlanningInput::UnionBranch(input)) as Arc<dyn ExecutionPlan>)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Transformed::new(
                UnionExec::try_new(inputs)?,
                true,
                TreeNodeRecursion::Jump,
            ))
        })?
        .data;
    let mut lanes = 0usize;
    plan.apply(|node| {
        if node.name() == "PartitionReadExec" {
            lanes = lanes.saturating_add(node.output_partitioning().partition_count());
            return Ok(TreeNodeRecursion::Jump);
        }
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
    let plan = ensure_requirements(plan, &config)?;
    let plan = OutputRequirements::new_remove_mode().optimize(plan, &config)?;
    // Restore native fetches after requirement enforcement has rebuilt exchanges.
    // In particular, absorb source limits before they can split same-owner stages.
    let plan = LimitPushdown::new().optimize(plan, &config)?;
    let plan = SanityCheckPlan::new().optimize(plan, &config)?;
    plan.transform_up(|plan| {
        if let Some(PlanningInput::UnionBranch(input)) = plan.downcast_ref::<PlanningInput>() {
            Ok(Transformed::yes(Arc::clone(input)))
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .map(|plan| plan.data)
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
                Arc::new(PlanningInput::OwnerStage(boundary)) as Arc<dyn ExecutionPlan>,
                true,
                TreeNodeRecursion::Jump,
            ))
        })?
        .data;
    let plan = optimize(plan, config)?;
    plan.transform_up(|plan| {
        if let Some(PlanningInput::OwnerStage(input)) = plan.downcast_ref::<PlanningInput>() {
            Ok(Transformed::yes(Arc::clone(input)))
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .map(|plan| plan.data)
}

/// DataFusion's `EnsureRequirements`, with each partition read's template settled
/// as a pipeline of its own, on one partition. A partition read runs its template
/// once per storage partition, and its lanes read several partitions at once; a
/// split inside the template, which `EnsureRequirements` adds for parallelism to
/// joins and filters, would only hash one partition's rows and lose the order its
/// reads come in. The rest of the plan is settled with the query's budget.
pub(crate) fn ensure_requirements(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut one_partition = config.clone();
    one_partition.execution.target_partitions = 1;
    // Its reads come in their orders; combining several keeps them, never sorts.
    one_partition.optimizer.prefer_existing_sort = true;
    let plan = plan
        .transform_down(|node| {
            let Some(read) = node.downcast_ref::<PartitionReadExec>() else {
                return Ok(Transformed::no(node));
            };
            let template =
                EnsureRequirements::new().optimize(Arc::clone(read.input()), &one_partition)?;
            let read = Arc::clone(&node).replace_children(
                vec![Arc::new(PlanningInput::Template(template))],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?;
            Ok(Transformed::new(read, true, TreeNodeRecursion::Jump))
        })?
        .data;
    EnsureRequirements::new()
        .optimize(plan, config)?
        .transform_up(|node| match node.downcast_ref::<PlanningInput>() {
            Some(PlanningInput::Template(template)) => Ok(Transformed::yes(Arc::clone(template))),
            _ => Ok(Transformed::no(node)),
        })
        .map(|plan| plan.data)
}

/// [`ensure_requirements`] in place of DataFusion's `EnsureRequirements`.
#[derive(Debug)]
pub(crate) struct TemplateRequirements;

impl PhysicalOptimizerRule for TemplateRequirements {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        ensure_requirements(plan, config)
    }

    fn name(&self) -> &str {
        "restate_ensure_requirements"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// An opaque, completed pipeline whose properties and statistics remain visible
/// while native rules optimize its consumer with a different budget. Each scope
/// removes its own wrappers before returning; none reach encoding/execution.
#[derive(Debug)]
enum PlanningInput {
    OwnerStage(Arc<dyn ExecutionPlan>),
    UnionBranch(Arc<dyn ExecutionPlan>),
    /// A partition read's template ([`ensure_requirements`]).
    Template(Arc<dyn ExecutionPlan>),
}

impl PlanningInput {
    fn input(&self) -> &Arc<dyn ExecutionPlan> {
        match self {
            Self::OwnerStage(input) | Self::UnionBranch(input) | Self::Template(input) => input,
        }
    }
}

impl DisplayAs for PlanningInput {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.input().fmt_as(t, f)
    }
}

impl ExecutionPlan for PlanningInput {
    fn name(&self) -> &str {
        "PlanningInput"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input().properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.input().apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("planning inputs cannot have children");
        }
        Ok(self)
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        internal_err!("planning input escaped physical planning")
    }

    fn statistics_from_inputs(
        &self,
        _: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        // The hidden input may itself have children (filters, aggregates, unions).
        // Supply their statistics through the native traversal rather than
        // forwarding this opaque leaf's empty input-statistics slice.
        StatisticsContext::new().compute(self.input().as_ref(), args)
    }
}
