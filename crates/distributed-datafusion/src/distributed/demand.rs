// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! What a query asks of a source's rows: the first `fetch` of them in an order,
//! after the conditions it applies on the way. Found before access selection,
//! while the planner's bounded sort still sits above the source:
//!
//! ```text
//! SortExec: TopK(fetch=51), expr=[created_at DESC NULLS LAST]   ← order, fetch
//!   FilterExec: target_service_name = Promises AND …            ← residual
//!     TableScanExec: sys_invocation_status
//! ```
//!
//! Only filters and projections may separate the sort from the source: neither
//! reorders rows or adds any. Everything is carried into the source's own
//! columns, so a projection passes the demand only if every column it needs,
//! its order's and its conditions', is a plain column of the projection's input;
//! the projection's other columns may be anything. Anything else in between, or
//! a sort without a fetch, demands nothing.
//!
//! An outer JOIN may separate them too, for an ordered demand on the side it
//! keeps, `ss` below, if the JOIN also keeps that side's order. The JOIN
//! outputs every row of that side at least once,
//! with that side's values, so if the order and the conditions only read that
//! side's columns, the first `fetch` output rows come from the joined rows of
//! that side's first `fetch` rows:
//!
//! ```text
//! SortExec: TopK(fetch=50), expr=[created_at DESC NULLS LAST]   ← ss's column
//!   ProjectionExec: …, CASE … END AS status, …
//!     HashJoinExec: join_type=Right, on=[(id, id)]
//!       TableScanExec: sys_invocation_state sis
//!       TableScanExec: sys_invocation_status ss                  ← 50 rows
//! ```
//!
//! A hash JOIN streams its probe side, the right one, in order, and builds a
//! table of the other side first ([`ExecutionPlan::maintains_input_order`]). So a
//! demand crosses to the right side of a RIGHT JOIN, as above; the left side of
//! a LEFT JOIN is drained into the table before the first row comes out, and a
//! demand doesn't cross to it. Only a JOIN evaluated shard by shard keeps that
//! order across partitions ([`super::shard_joins`]), so only its lowering reads
//! such a demand ([`Demands::on_join_input`]); any other scan beneath a JOIN
//! reads as before. A demand crosses one JOIN at most: a second one, evaluated
//! as usual above the first, would lose the order again.
//!
//! Access is chosen before DataFusion's `JoinSelection` runs. That rule swaps a
//! hash JOIN's sides only when statistics show the build side larger than the
//! probe side; partitioned sources report none, so it doesn't today. If it did,
//! the JOIN would no longer declare the order, and the plan would sort above
//! it: the same rows, without stopping early.
//!
//! A LIMIT without an ORDER BY demands any `fetch` rows that meet the
//! conditions, in no order: an **unordered** demand, with an empty ordering.
//! Repartitioning and coalescing may also separate it from the source, as
//! they keep every row. A sort's ordered demand on a source takes precedence:
//!
//! ```text
//! CoalescePartitionsExec: fetch=250                              ← fetch
//!   FilterExec: status = suspended AND …, fetch=250              ← residual
//!     RepartitionExec: partitioning=RoundRobinBatch(2)
//!       TableScanExec: sys_invocation_status
//! ```
//!
//! A demand belongs to one consumer of the source. A plan may reach the same
//! source node more than once, such as from both inputs of a union; then
//! nothing is demanded of it, as another consumer may need other rows:
//!
//! ```text
//! UnionExec
//!   SortExec: TopK(fetch=51)        ← needs 51 rows
//!     FilterExec: P
//!       S                           ┐ one node: no demand
//!   FilterExec: P                   │ ← needs every row
//!     S                             ┘
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinSide, JoinType, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr, split_conjunction};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::joins::utils::{ColumnIndex, build_join_schema};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;

use super::source::TableScanExec;

/// The first `fetch` rows of a source in `ordering`, among those that meet
/// every condition in `residual`. All in the source's columns.
#[derive(Debug, Clone)]
pub(crate) struct Demand {
    pub(crate) ordering: Vec<PhysicalSortExpr>,
    /// The sort's fetch: its OFFSET is already included.
    pub(crate) fetch: usize,
    /// Every condition evaluated between the source and the sort.
    pub(crate) residual: Vec<Arc<dyn PhysicalExpr>>,
}

/// The demand on each table scan that a plan reaches once.
pub(crate) struct Demands(
    /// By the scan's address. Each scan is kept, so that no other plan node
    /// takes over its address while the plan is rewritten. With whether the
    /// demand crossed an outer JOIN.
    HashMap<*const (), (Arc<dyn ExecutionPlan>, Demand, bool)>,
);

impl Demands {
    pub(crate) fn of(plan: &Arc<dyn ExecutionPlan>) -> Self {
        let mut found = HashMap::new();
        let mut reached = HashMap::new();
        visit(plan, &mut found, &mut reached);
        found.retain(|scan, _| reached.get(scan) == Some(&1));
        Self(found)
    }

    /// The demand on `scan`, a table scan of the plan, if any and if no JOIN
    /// separates the scan from the demand.
    pub(crate) fn on(&self, scan: &Arc<dyn ExecutionPlan>) -> Option<&Demand> {
        self.0
            .get(&address(scan))
            .filter(|(_, _, through_join)| !through_join)
            .map(|(_, demand, _)| demand)
    }

    /// The demand on `scan`, the scan of a JOIN's input, if one crossed that
    /// JOIN: for a JOIN evaluated shard by shard, which keeps the order its kept
    /// side is read in. A demand found beneath the JOIN without crossing it, such
    /// as a LIMIT DataFusion pushed onto the kept side, demands nothing here, so
    /// such an input reads as before.
    pub(crate) fn on_join_input(&self, scan: &TableScanExec) -> Option<&Demand> {
        self.0
            .get(&(std::ptr::from_ref(scan) as *const ()))
            .filter(|(_, _, through_join)| *through_join)
            .map(|(_, demand, _)| demand)
    }
}

fn address(plan: &Arc<dyn ExecutionPlan>) -> *const () {
    Arc::as_ptr(plan) as *const ()
}

type Found = HashMap<*const (), (Arc<dyn ExecutionPlan>, Demand, bool)>;

/// Finds the demands under `plan`, counting how often it reaches each scan.
fn visit(
    plan: &Arc<dyn ExecutionPlan>,
    found: &mut Found,
    reached: &mut HashMap<*const (), usize>,
) {
    if plan.downcast_ref::<TableScanExec>().is_some() {
        *reached.entry(address(plan)).or_default() += 1;
    }
    if let Some(sort) = plan.downcast_ref::<SortExec>()
        && let Some(fetch) = sort.fetch()
    {
        let demand = Demand {
            ordering: sort.expr().iter().cloned().collect(),
            fetch,
            residual: Vec::new(),
        };
        if let Some(followed) = follow(sort.input(), demand, false) {
            found.insert(address(&followed.0), followed);
        }
    } else if let Some((fetch, input)) = unordered_limit(plan) {
        let demand = Demand {
            ordering: Vec::new(),
            fetch,
            residual: Vec::new(),
        };
        // An ordered demand above, found first, takes precedence.
        if let Some(followed) = follow(input, demand, false) {
            found.entry(address(&followed.0)).or_insert(followed);
        }
    }
    for child in plan.children() {
        visit(child, found, reached);
    }
}

/// A LIMIT's rows, its OFFSET included, and its input, for a node that keeps
/// the first rows its input gives.
fn unordered_limit(plan: &Arc<dyn ExecutionPlan>) -> Option<(usize, &Arc<dyn ExecutionPlan>)> {
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
        // A sum past `usize` demands nothing: the scan reads as it would.
        return Some((limit.skip().checked_add(limit.fetch()?)?, limit.input()));
    }
    if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        return Some((coalesce.fetch()?, coalesce.input()));
    }
    None
}

/// Carries `demand`, in the columns of `plan`'s output, down to a table scan,
/// and tells whether it crossed an outer JOIN (`through_join`) on the way.
fn follow(
    plan: &Arc<dyn ExecutionPlan>,
    mut demand: Demand,
    through_join: bool,
) -> Option<(Arc<dyn ExecutionPlan>, Demand, bool)> {
    if plan.downcast_ref::<TableScanExec>().is_some() {
        return Some((Arc::clone(plan), demand, through_join));
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        // A computed column is no input column: a demand that needs one stops.
        let inputs: Vec<_> = projection
            .expr()
            .iter()
            .map(|expr| expr.expr.downcast_ref::<Column>().cloned())
            .collect();
        demand = remap(demand, |index| inputs.get(index).cloned().flatten())?;
        return follow(projection.input(), demand, through_join);
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        if let Some(projection) = filter.projection() {
            let schema = filter.input().schema();
            demand = remap(demand, |index| {
                let input = *projection.get(index)?;
                Some(Column::new(schema.field(input).name(), input))
            })?;
        }
        demand
            .residual
            .extend(split_conjunction(filter.predicate()).into_iter().cloned());
        return follow(filter.input(), demand, through_join);
    }
    if !demand.ordering.is_empty()
        && !through_join
        && let Some(join) = plan.downcast_ref::<HashJoinExec>()
    {
        return through_outer_join(join, demand);
    }
    // Without an order, nodes that keep every row in its columns, or the
    // first of them per partition, change nothing the demand needs.
    if demand.ordering.is_empty() {
        if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
            return follow(repartition.input(), demand, through_join);
        }
        if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
            return follow(coalesce.input(), demand, through_join);
        }
        if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
            return follow(limit.input(), demand, through_join);
        }
    }
    None
}

/// Carries an ordered `demand` on `join`'s output to the side an outer JOIN
/// keeps, if the JOIN keeps that side's order too and the demand's order and
/// conditions only read that side's columns (see the module docs). Any other
/// JOIN, or a demand on the other side's columns, demands nothing.
fn through_outer_join(
    join: &HashJoinExec,
    demand: Demand,
) -> Option<(Arc<dyn ExecutionPlan>, Demand, bool)> {
    let (kept, input, at) = match join.join_type() {
        JoinType::Left => (JoinSide::Left, join.left(), 0),
        JoinType::Right => (JoinSide::Right, join.right(), 1),
        _ => return None,
    };
    // A hash JOIN keeps only its probe side's order: a kept build side is read
    // to the end before the first row comes out.
    if !join.maintains_input_order()[at] {
        return None;
    }
    let (_, columns) = build_join_schema(
        &join.left().schema(),
        &join.right().schema(),
        join.join_type(),
    );
    let columns: Vec<ColumnIndex> = match &join.projection {
        Some(projection) => projection
            .iter()
            .map(|&index| columns.get(index).cloned())
            .collect::<Option<_>>()?,
        None => columns,
    };
    let schema = input.schema();
    let demand = remap(demand, |index| {
        let column = columns.get(index)?;
        (column.side == kept).then(|| Column::new(schema.field(column.index).name(), column.index))
    })?;
    follow(input, demand, true)
}

/// Rewrites every column of `demand` to the column `input` gives for its index.
fn remap(demand: Demand, input: impl Fn(usize) -> Option<Column>) -> Option<Demand> {
    let rewrite = |expr: &Arc<dyn PhysicalExpr>| -> Option<Arc<dyn PhysicalExpr>> {
        let mut unmapped = false;
        let rewritten = Arc::clone(expr)
            .transform(|expr| -> Result<_> {
                match expr.downcast_ref::<Column>() {
                    Some(column) => match input(column.index()) {
                        Some(mapped) => Ok(Transformed::yes(Arc::new(mapped) as _)),
                        None => {
                            unmapped = true;
                            Ok(Transformed::no(expr))
                        }
                    },
                    None => Ok(Transformed::no(expr)),
                }
            })
            .ok()?
            .data;
        (!unmapped).then_some(rewritten)
    };
    Some(Demand {
        ordering: demand
            .ordering
            .iter()
            .map(|sort| {
                Some(PhysicalSortExpr {
                    expr: rewrite(&sort.expr)?,
                    options: sort.options,
                })
            })
            .collect::<Option<_>>()?,
        fetch: demand.fetch,
        residual: demand.residual.iter().map(rewrite).collect::<Option<_>>()?,
    })
}
