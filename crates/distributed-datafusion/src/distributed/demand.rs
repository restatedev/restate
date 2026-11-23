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
//! Only filters and column-only projections may separate the sort from the
//! source: neither reorders rows or adds any. Everything is carried into the
//! source's own columns. Anything else in between, or a sort without a fetch,
//! demands nothing.
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

use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr, split_conjunction};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
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
    /// takes over its address while the plan is rewritten.
    HashMap<*const (), (Arc<dyn ExecutionPlan>, Demand)>,
);

impl Demands {
    pub(crate) fn of(plan: &Arc<dyn ExecutionPlan>) -> Self {
        let mut found = HashMap::new();
        let mut reached = HashMap::new();
        visit(plan, &mut found, &mut reached);
        found.retain(|scan, _| reached.get(scan) == Some(&1));
        Self(found)
    }

    /// The demand on `scan`, a table scan of the plan, if any.
    pub(crate) fn on(&self, scan: &Arc<dyn ExecutionPlan>) -> Option<&Demand> {
        self.0.get(&address(scan)).map(|(_, demand)| demand)
    }
}

fn address(plan: &Arc<dyn ExecutionPlan>) -> *const () {
    Arc::as_ptr(plan) as *const ()
}

type Found = HashMap<*const (), (Arc<dyn ExecutionPlan>, Demand)>;

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
        if let Some((scan, demand)) = follow(sort.input(), demand) {
            found.insert(address(&scan), (scan, demand));
        }
    } else if let Some((fetch, input)) = unordered_limit(plan) {
        let demand = Demand {
            ordering: Vec::new(),
            fetch,
            residual: Vec::new(),
        };
        // An ordered demand above, found first, takes precedence.
        if let Some((scan, demand)) = follow(input, demand) {
            found.entry(address(&scan)).or_insert((scan, demand));
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

/// Carries `demand`, in the columns of `plan`'s output, down to a table scan.
fn follow(
    plan: &Arc<dyn ExecutionPlan>,
    mut demand: Demand,
) -> Option<(Arc<dyn ExecutionPlan>, Demand)> {
    if plan.downcast_ref::<TableScanExec>().is_some() {
        return Some((Arc::clone(plan), demand));
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let inputs = projection
            .expr()
            .iter()
            .map(|expr| expr.expr.downcast_ref::<Column>().cloned())
            .collect::<Option<Vec<_>>>()?;
        demand = remap(demand, |index| inputs.get(index).cloned())?;
        return follow(projection.input(), demand);
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
        return follow(filter.input(), demand);
    }
    // Without an order, nodes that keep every row in its columns, or the
    // first of them per partition, change nothing the demand needs.
    if demand.ordering.is_empty() {
        if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
            return follow(repartition.input(), demand);
        }
        if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
            return follow(coalesce.input(), demand);
        }
        if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
            return follow(limit.input(), demand);
        }
    }
    None
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
