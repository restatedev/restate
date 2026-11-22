// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shard-local JOINs: evaluate an equi-join separately within each shard when its
//! equality keys prove that matching rows always share a shard.
//!
//! The proof comes from declared sharding columns (see
//! `restate_storage_query_api::schema::sharding`), traced through projections and
//! filters to each input's scan. If some equality pair compares sharding columns
//! whose values imply their shard keys the same way (`ShardKeyKind::colocates`),
//! matching rows have equal shard keys, and therefore share a shard under any
//! topology:
//!
//! ```text
//! sys_invocation_status s JOIN sys_invocation_state l ON s.id = l.id
//!     both InvocationId              -> shard-local
//! sys_invocation_status s JOIN sys_vqueue_entry_status e ON s.id = e.entry_id
//!     InvocationId, BaseEntryId: both read the ID's key -> shard-local
//! sys_user_limits u JOIN sys_invocation_status s ON u.scope = s.id
//!     Scope, InvocationId: a hash, an ID's key -> global
//! ... ON s.invoked_by_id = l.id      (a caller's ID, not a sharding column) -> global
//! ... ON a.scope IS NOT DISTINCT FROM b.scope
//!     NULL scopes match but determine no shard   -> global
//! ```
//!
//! Then `A ⋈ B = ⋃ₛ (Aₛ ⋈ Bₛ)`, including outer joins: a row unmatched within its
//! shard is unmatched everywhere. All original join predicates are retained.
//!
//! Only scan/filter/projection inputs, plus the bounds below, are eligible:
//! aggregates and other operators may depend on rows from multiple partitions and
//! stay outside admission.
//!
//! # Limits
//!
//! A `LIMIT n OFFSET k` above an outer JOIN needs `n + k` of its rows, and each
//! row of the preserved side yields output on its own. A bound of at least `n + k`
//! on the preserved input therefore still leaves enough rows when it applies per
//! shard. DataFusion adds exactly that bound, and the limit itself stays above all
//! shards. Each scheduling lane of a shard-local group stops opening partitions
//! once it produced `n + k` rows:
//!
//! ```text
//! SELECT … FROM status s LEFT JOIN state l ON s.id = l.id LIMIT 3
//!   Limit 3                 kept above the shard-local groups
//!     LEFT JOIN
//!       Limit 3 on s        any 3 rows of s give 3 rows     -> shard-local
//!
//! SELECT … FROM (SELECT … FROM status LIMIT 1) s LEFT JOIN state l … LIMIT 3
//!       Limit 1 on s        1 < 3: picks one row globally   -> global
//! ```
//!
//! Bounds anywhere else (the other side, inputs of an inner JOIN, an OFFSET on an
//! input) select rows globally and keep ordinary execution.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::common::JoinType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{NullEquality, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, displayable};

use restate_storage_query_api::schema::sharding::ShardingColumn;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::access::select_access;
use crate::node_fan_out::{NodeFanOutExecutionPlan, TargetNode};

use super::{PartitionReadExec, SourceWork, TableScanExec, external, plan_partition_access};

/// Traces a JOIN key through row-preserving operators to the sharding column it
/// reads. Only plain column references qualify: any other expression, including a
/// cast, could change which values compare equal.
fn sharding_column(
    plan: &Arc<dyn ExecutionPlan>,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<ShardingColumn<usize>> {
    column_lineage(plan, expr.downcast_ref::<Column>()?.index())
}

fn column_lineage(plan: &Arc<dyn ExecutionPlan>, index: usize) -> Option<ShardingColumn<usize>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return sharding_column(projection.input(), &projection.expr().get(index)?.expr);
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        // A fused projection selects input columns by position.
        let index = match filter.projection() {
            Some(projection) => *projection.get(index)?,
            None => index,
        };
        return column_lineage(filter.input(), index);
    }
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
        return column_lineage(limit.input(), index);
    }
    if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
        return column_lineage(limit.input(), index);
    }
    let columns = match plan.downcast_ref::<TableScanExec>() {
        Some(scan) => &scan.0.planning.as_ref()?.source.sharding_columns,
        None => {
            &plan
                .downcast_ref::<NodeFanOutExecutionPlan>()?
                .scan()?
                .sharding_columns
        }
    };
    columns
        .iter()
        .find(|column| column.column == index)
        .copied()
}

/// Whether equal values of `left` and `right` place matching rows in one shard:
/// both columns must imply shard keys of one space the same way
/// (`ShardKeyKind::colocates`), and NULLs must not match each other, because a
/// NULL determines no shard key.
fn colocates(
    left: ShardingColumn<usize>,
    right: ShardingColumn<usize>,
    nulls: NullEquality,
) -> bool {
    left.kind.colocates(right.kind)
        && (nulls == NullEquality::NullEqualsNothing || !(left.nullable && right.nullable))
}

/// The scan beneath a JOIN input, which may only filter and project its rows.
#[derive(Clone, Copy)]
enum ShardScan<'a> {
    Partitions(&'a TableScanExec),
    Nodes(&'a NodeFanOutExecutionPlan),
}

/// Whether two topology snapshots agree: each partition owns the same key range in
/// both, and no key belongs to two partitions. A shard key then has one partition
/// across both snapshots, even when they list different partitions.
fn consistent_topologies(
    left: &[(PartitionId, Partition)],
    right: &[(PartitionId, Partition)],
) -> bool {
    let mut ranges = BTreeMap::new();
    for (id, partition) in left.iter().chain(right) {
        if *ranges.entry(*id).or_insert(partition.key_range) != partition.key_range {
            return false;
        }
    }
    let mut ranges: Vec<_> = ranges.into_values().collect();
    ranges.sort_unstable_by_key(|range| range.start());
    ranges
        .windows(2)
        .all(|pair| pair[0].end() < pair[1].start())
}

/// The scan beneath a JOIN input, which may only filter, project, and bound its
/// rows. A bound (a limit or fetch) is accepted only if `needed` allows it: when
/// an enclosing limit needs at most `needed` rows of this input, any bound of at
/// least `needed` rows still leaves enough of them. Without `needed`, a bound
/// selects rows globally and must not be applied per shard.
fn shard_scan(plan: &Arc<dyn ExecutionPlan>, needed: Option<usize>) -> Option<ShardScan<'_>> {
    let allowed = |bound: Option<usize>| {
        bound.is_none_or(|bound| needed.is_some_and(|needed| bound >= needed))
    };
    if !allowed(plan.fetch()) {
        return None;
    }
    if let Some(scan) = plan.downcast_ref::<TableScanExec>() {
        let bound = scan.0.descriptor.limit.map(|limit| limit as usize);
        return (allowed(bound) && scan.0.planning.is_some())
            .then_some(ShardScan::Partitions(scan));
    }
    if let Some(fan_out) = plan.downcast_ref::<NodeFanOutExecutionPlan>() {
        return fan_out
            .scan()
            .is_some_and(|scan| allowed(scan.limit))
            .then_some(ShardScan::Nodes(fan_out));
    }
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
        // An OFFSET skips rows globally.
        return (limit.skip() == 0)
            .then(|| shard_scan(limit.input(), needed))
            .flatten();
    }
    if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
        return shard_scan(limit.input(), needed);
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        return shard_scan(filter.input(), needed);
    }
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return shard_scan(projection.input(), needed);
    }
    None
}

/// Replaces the scan beneath a JOIN input with `leaf`, keeping its filters and
/// projections.
fn with_leaf(
    input: &Arc<dyn ExecutionPlan>,
    leaf: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::clone(input)
        .transform_up(|node| {
            if node.is::<TableScanExec>() || node.is::<NodeFanOutExecutionPlan>() {
                Ok(Transformed::yes(Arc::clone(leaf)))
            } else {
                Ok(Transformed::no(node))
            }
        })?
        .data)
}

pub(super) fn group_join(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // A limit above a JOIN states how many of its rows are needed.
    let grouped = match plan.downcast_ref::<GlobalLimitExec>() {
        Some(limit) => match limit.fetch() {
            Some(fetch) => group_under_limit(limit.input(), limit.skip() + fetch, config)?
                .map(|input| {
                    Arc::clone(&plan).replace_children(
                        vec![input],
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )
                })
                .transpose()?,
            None => None,
        },
        None => group(&plan, None, config)?,
    };
    Ok(match grouped {
        Some(grouped) => Transformed::yes(grouped),
        None => Transformed::no(plan),
    })
}

/// Groups the JOIN beneath row-preserving projections of an enclosing limit that
/// needs `needed` rows.
fn group_under_limit(
    plan: &Arc<dyn ExecutionPlan>,
    needed: usize,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return group_under_limit(projection.input(), needed, config)?
            .map(|input| {
                Arc::clone(plan).replace_children(
                    vec![input],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )
            })
            .transpose();
    }
    group(plan, Some(needed), config)
}

/// Groups a JOIN by shard. `needed` is how many of its rows an enclosing limit
/// needs, if any. `None` keeps ordinary execution.
fn group(
    plan: &Arc<dyn ExecutionPlan>,
    needed: Option<usize>,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if plan.fetch().is_some() {
        return Ok(None);
    }
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(None);
    };
    // Only the preserved side of an outer JOIN may be bounded: each of its rows
    // yields output on its own, so any `needed` of them produce enough rows.
    let (left_needed, right_needed) = match join.join_type() {
        JoinType::Left => (needed, None),
        JoinType::Right => (None, needed),
        _ => (None, None),
    };
    let (Some(left), Some(right)) = (
        shard_scan(join.left(), left_needed),
        shard_scan(join.right(), right_needed),
    ) else {
        return Ok(None);
    };
    // `NOT IN` needs the whole build side: one NULL anywhere empties the result,
    // so evaluating it shard by shard would be wrong.
    if join.null_aware {
        return Ok(None);
    }
    let nulls = join.null_equality();
    if !join.on().iter().any(|(left, right)| {
        sharding_column(join.left(), left)
            .zip(sharding_column(join.right(), right))
            .is_some_and(|(left, right)| colocates(left, right, nulls))
    }) {
        return Ok(None);
    }
    // Colocated kinds share a space, so one sharding scheme: the scans are of one kind.
    Ok(match (left, right) {
        (ShardScan::Partitions(left), ShardScan::Partitions(right)) => group_partitions(
            plan,
            join,
            left,
            right,
            config.execution.target_partitions,
            needed,
        )?,
        (ShardScan::Nodes(left), ShardScan::Nodes(right)) => {
            group_nodes(join, left, right, config)?
        }
        _ => None,
    })
}

/// Partitions: one native JOIN per owner, inside a [`PartitionReadExec`] that
/// admits both inputs' partitions once. With `needed`, each of its scheduling lanes
/// stops opening partitions once it produced that many rows, so several lanes may
/// each produce them; the enclosing limit stays above and bounds the result.
/// `None` keeps ordinary execution.
fn group_partitions(
    plan: &Arc<dyn ExecutionPlan>,
    join: &HashJoinExec,
    left: &TableScanExec,
    right: &TableScanExec,
    parallelism: usize,
    needed: Option<usize>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let left_planning = left.0.planning.as_ref().unwrap();
    let right_planning = right.0.planning.as_ref().unwrap();
    if left_planning.local != right_planning.local {
        return Ok(None);
    }
    let (SourceWork::TableScan(left_work), SourceWork::TableScan(right_work)) =
        (&left.0.descriptor.work, &right.0.descriptor.work)
    else {
        return Ok(None);
    };
    let mut placement = left_work.placement;
    placement.require_leader |= right_work.placement.require_leader;
    placement.options.require_leader |= right_work.placement.options.require_leader;

    // Each provider listed the partitions of its own source. Matching shard keys
    // share a partition only if both lists agree wherever they overlap; a split
    // between the two reads breaks that, so keep ordinary execution. The lists may
    // still select different partitions, and each input keeps its own.
    if !consistent_topologies(&left_planning.topology, &right_planning.topology) {
        return Ok(None);
    }
    let mut owners = BTreeSet::new();
    let mut inputs = Vec::new();
    for (input, scan) in [(join.left(), left), (join.right(), right)] {
        let planning = scan.0.planning.as_ref().unwrap();
        let mut groups: BTreeMap<Option<GenerationalNodeId>, Vec<_>> = BTreeMap::new();
        for (partition, metadata) in &planning.partitions {
            let owner = if planning.local {
                None
            } else {
                Some(
                    planning
                        .source
                        .manager
                        .partition_owner(*partition, placement)
                        .map_err(external)?,
                )
            };
            groups
                .entry(owner)
                .or_default()
                .push((*partition, metadata.clone()));
            owners.insert(owner);
        }
        let mut by_owner = BTreeMap::new();
        for (owner, partitions) in groups {
            let access = select_access(
                &planning.source,
                scan.0.predicate.as_ref(),
                None,
                &scan.0.schema,
            )
            .map_err(external)?;
            let lowered = plan_partition_access(
                planning.source.table.clone(),
                &planning.source.manager,
                partitions,
                placement,
                Arc::clone(&scan.0.schema),
                Arc::clone(&scan.0.statistics),
                scan.0.descriptor.orders(),
                scan.0.predicate.clone(),
                // An accepted bound applies to each partition's rows.
                scan.0.descriptor.limit.map(|limit| limit as usize),
                access,
                planning.local,
                parallelism,
            )?;
            let lowered = if let Some(group) = lowered.downcast_ref::<PartitionReadExec>() {
                Arc::clone(group.input())
            } else {
                // All primary keys may have been pruned for this owner.
                if !lowered.is::<EmptyExec>() {
                    return datafusion::common::plan_err!(
                        "partition join ownership changed during planning"
                    );
                }
                lowered
            };
            by_owner.insert(owner, with_leaf(input, &lowered)?);
        }
        inputs.push(by_owner);
    }
    let mut groups = Vec::new();
    for owner in owners {
        let left = inputs[0]
            .remove(&owner)
            .unwrap_or_else(|| Arc::new(EmptyExec::new(join.left().schema())));
        let right = inputs[1]
            .remove(&owner)
            .unwrap_or_else(|| Arc::new(EmptyExec::new(join.right().schema())));
        let joined = Arc::clone(plan).replace_children(
            vec![left, right],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let mut has_source = false;
        joined.apply(|node| {
            has_source |= super::source_plan(node.as_ref()).is_some();
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;
        if !has_source {
            continue;
        }
        groups.push(
            Arc::new(PartitionReadExec::new(vec![joined], parallelism, needed)?)
                as Arc<dyn ExecutionPlan>,
        );
    }
    if groups.is_empty() {
        return Ok(Some(Arc::new(EmptyExec::new(plan.schema()))));
    }
    Ok(Some(UnionExec::try_new(groups)?))
}

/// Nodes: one fan-out task per node runs the whole JOIN on that node. A failing
/// input fails that node's task: rows it already emitted remain, a warning reports
/// the incomplete output, and the failed input never acts as an empty one, so no
/// unmatched outer rows are invented. `None` keeps ordinary execution.
fn group_nodes(
    join: &HashJoinExec,
    left: &NodeFanOutExecutionPlan,
    right: &NodeFanOutExecutionPlan,
    config: &ConfigOptions,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let (Some(left_scan), Some(right_scan)) = (left.scan(), right.scan()) else {
        return Ok(None);
    };
    if left_scan.distributed != right_scan.distributed {
        return Ok(None);
    }
    // Each input read the nodes configuration separately. A node seen with two
    // generations would have tasks with two owners.
    let mut nodes: BTreeMap<PlainNodeId, &TargetNode> = BTreeMap::new();
    for node in left.target_nodes().iter().chain(right.target_nodes()) {
        if nodes
            .insert(node.plain_node_id, node)
            .is_some_and(|seen| seen.node_id != node.node_id)
        {
            return Ok(None);
        }
    }
    // One lane per node task: node tables are small, and a single lane keeps a
    // failure behind the rows already produced, so they still reach the output.
    let mut task_config = config.clone();
    task_config.execution.target_partitions = 1;
    let mut tasks = Vec::with_capacity(nodes.len());
    let mut task = None;
    for node in nodes.values() {
        let mut children = Vec::with_capacity(2);
        for (input, fan_out) in [(join.left(), left), (join.right(), right)] {
            let targeted = fan_out
                .target_nodes()
                .iter()
                .any(|target| target.plain_node_id == node.plain_node_id);
            children.push(if targeted {
                let scan = fan_out.scan().expect("JOIN inputs are plain scans");
                match scan.leaf(&fan_out.schema(), node)? {
                    Some(leaf) => Some(with_leaf(input, &leaf)?),
                    None => None,
                }
            } else {
                Some(Arc::new(EmptyExec::new(input.schema())) as Arc<dyn ExecutionPlan>)
            });
        }
        // A side this coordinator cannot scan makes the whole node unavailable.
        let Some(children) = children.into_iter().collect::<Option<Vec<_>>>() else {
            tasks.push(None);
            continue;
        };
        // Later physical rules never see an opaque task, so settle the join mode
        // here. Collecting the build side is correct for any probe-side layout.
        let joined = join
            .builder()
            .with_new_children(children)?
            .with_partition_mode(PartitionMode::CollectLeft)
            .reset_state()
            .recompute_properties()
            .build_exec()?;
        // Every node runs the same join over its own rows.
        task.get_or_insert_with(|| {
            format!("shard-local {}", displayable(joined.as_ref()).one_line())
                .trim_end()
                .to_owned()
        });
        tasks.push(Some(if left_scan.distributed {
            crate::distributed::plan_owner_task(joined, &task_config)?
        } else {
            joined
        }));
    }
    Ok(Some(Arc::new(NodeFanOutExecutionPlan::for_tasks(
        join.schema(),
        nodes.into_values().cloned().collect(),
        task.unwrap_or_else(|| "shard-local join".to_owned()),
        tasks,
    ))))
}
