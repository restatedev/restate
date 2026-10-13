// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Move a bounded set of native operators into already owner-bound stages.
//! Partition-local work can cross an owner union; work that combines partitions
//! can only cross a single owner boundary. Global requirements stay outside.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::LocalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use datafusion_distributed::{NetworkBoundary, NetworkCoalesceExec, Stage};

/// Return a rewritten plan only when every affected input is an owner boundary.
/// The caller traverses bottom-up after ordinary physical optimization.
pub(super) fn push_into_sources(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let children = plan.children();
    let [input] = children.as_slice() else {
        return Ok(None);
    };
    let aggregate = plan.downcast_ref::<AggregateExec>();
    let sort = plan.downcast_ref::<SortExec>();
    // This is a placement contract, not a list of SQL-plan patterns. In
    // particular, UnspecifiedDistribution alone does not imply partition-local
    // execution (RepartitionExec also declares it).
    let per_lane = plan.is::<ProjectionExec>()
        || plan.is::<FilterExec>()
        || plan.is::<CooperativeExec>()
        || plan.is::<LocalLimitExec>()
        || sort.is_some_and(SortExec::preserve_partitioning)
        || aggregate.is_some_and(|aggregate| *aggregate.mode() == AggregateMode::Partial);
    let within_owner = per_lane
        || aggregate.is_some()
        || sort.is_some()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<RepartitionExec>();

    if within_owner && let Some(boundary) = input.downcast_ref::<NetworkCoalesceExec>() {
        return move_operator(plan, boundary).map(Some);
    }
    if per_lane && let Some(union) = input.downcast_ref::<UnionExec>() {
        return move_into_union(plan, union);
    }
    Ok(None)
}

fn move_into_union(
    operator: &Arc<dyn ExecutionPlan>,
    union: &UnionExec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if !union
        .inputs()
        .iter()
        .all(|input| input.is::<NetworkCoalesceExec>())
    {
        return Ok(None);
    }
    // These operators preserve lane count and do not require rows from
    // another lane. Final aggregation and hash redistribution cannot cross
    // this union: the same group may occur on multiple owners.
    let inputs = union
        .inputs()
        .iter()
        .map(|input| {
            move_operator(
                operator,
                input.downcast_ref::<NetworkCoalesceExec>().unwrap(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    UnionExec::try_new(inputs).map(Some)
}

fn move_operator(
    operator: &Arc<dyn ExecutionPlan>,
    boundary: &NetworkCoalesceExec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Stage::Local(stage) = boundary.input_stage() else {
        return datafusion::common::internal_err!(
            "source pushdown requires a local stage description"
        );
    };
    let input = Arc::clone(operator).replace_children(
        vec![Arc::clone(&stage.plan)],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?;
    // Rebuild the boundary from the new input so schema, lane count and
    // ordering reflect the pushed operator. The library's with_new_children
    // intentionally retains the old boundary properties.
    super::owner_boundary(input, stage.query_id, stage.num)
}
