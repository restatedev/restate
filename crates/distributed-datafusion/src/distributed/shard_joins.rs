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
//! stay outside admission. A union of such inputs is one too, such as the scans of
//! a table's layouts, which a table that changed its layout reads together:
//!
//! ```text
//! sys_invocation_status s JOIN sys_journal j ON s.id = j.id
//!     j = Union(journal V1 scan, journal V2 scan): id is InvocationId in both -> shard-local
//! ```
//!
//! Each shard then reads every layout's share of its partitions. A column of a
//! union qualifies only as the same sharding column in each of its inputs.
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
//!
//! # Merges
//!
//! Within a shard, both inputs of an ID JOIN often come in the same order already:
//! scans and lookups of invocations and of their vqueue entries both read in
//! `base_entry_id` order ([`OrderFunction`]). Such a JOIN merges its inputs instead
//! of hashing one of them, stepping through both in that order:
//!
//! ```text
//! SELECT … FROM sys_invocation_status s LEFT JOIN sys_vqueue_entry_status e
//!   ON s.id = e.entry_id
//!
//! s, by base_entry_id(id)          e, by base_entry_id(entry_id)
//! 07 | invocation | uuid 1         07 | invocation | uuid 1        match
//! 07 | invocation | uuid 256                                       no entry: padded
//! 08 | invocation | uuid 3         08 | invocation | uuid 3        match
//! ```
//!
//! Equal values of a function mean equal IDs, so the merge matches the rows the
//! hash JOIN would. It builds no hash table and sorts nothing, and a LIMIT stops
//! both reads early. Each read emits the values it is ordered by as a key field
//! next to its columns, `base_entry_id(id)`, from the ID it holds, and the merge
//! compares those. JOINs without a common order, and owners that read an index,
//! keep hashing ([`merge_plan`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::JoinType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{JoinSide, NullEquality, Result, Statistics};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column};
use datafusion::physical_expr::{PhysicalSortExpr, conjunction};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::joins::utils::{
    ColumnIndex, JoinFilter, JoinOnRef, build_join_schema,
};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, displayable};
use strum::VariantArray;

use restate_storage_query_api::ordering::OrderFunction;
use restate_storage_query_api::schema::sharding::{ShardKeyKind, ShardingColumn};
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_types::{GenerationalNodeId, PlainNodeId};

use crate::access::{Access, select_access};
use crate::distributed::demand::Demands;
use crate::node_fan_out::{NodeFanOutExecutionPlan, TargetNode};
use crate::order_key::order_key;

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
    if let Some(union) = plan.downcast_ref::<UnionExec>() {
        let mut inputs = union
            .inputs()
            .iter()
            .map(|input| column_lineage(input, index));
        let first = inputs.next()??;
        let mut nullable = first.nullable;
        for column in inputs {
            let column = column?;
            if column.kind != first.kind {
                return None;
            }
            nullable |= column.nullable;
        }
        return Some(ShardingColumn {
            column: index,
            kind: first.kind,
            nullable,
        });
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

/// The scans beneath a JOIN input, which may only filter and project its rows:
/// one, or those of a union.
enum ShardScan<'a> {
    Partitions(Vec<&'a TableScanExec>),
    Nodes(&'a NodeFanOutExecutionPlan),
}

/// Whether topology snapshots agree: each partition owns the same key range in
/// all of them, and no key belongs to two partitions. A shard key then has one
/// partition across the snapshots, even when they list different partitions.
fn consistent_topologies<'a>(
    topologies: impl IntoIterator<Item = &'a [(PartitionId, Partition)]>,
) -> bool {
    let mut ranges = BTreeMap::new();
    for (id, partition) in topologies.into_iter().flatten() {
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
            .then(|| ShardScan::Partitions(vec![scan]));
    }
    if let Some(union) = plan.downcast_ref::<UnionExec>() {
        let mut scans = Vec::new();
        for input in union.inputs() {
            let ShardScan::Partitions(inputs) = shard_scan(input, needed)? else {
                return None;
            };
            scans.extend(inputs);
        }
        return Some(ShardScan::Partitions(scans));
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

/// Replaces each scan beneath a JOIN input with its `leaves` entry, keeping the
/// filters, projections and unions above them.
fn with_scan_leaves(
    input: &Arc<dyn ExecutionPlan>,
    leaves: &[(&TableScanExec, Arc<dyn ExecutionPlan>)],
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::clone(input)
        .transform_up(|node| {
            let Some(scan) = node.downcast_ref::<TableScanExec>() else {
                return Ok(Transformed::no(node));
            };
            match leaves.iter().find(|(known, _)| std::ptr::eq(*known, scan)) {
                Some((_, leaf)) => Ok(Transformed::yes(Arc::clone(leaf))),
                None => datafusion::common::plan_err!("a JOIN input's scan wasn't lowered"),
            }
        })?
        .data)
}

/// `plan` without the inputs of its unions that read nothing, such as a layout
/// without partitions on an owner: they add no rows, and a union of the rest keeps
/// the orders they share. A union left with one input is that input.
fn without_empty_inputs(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|node| {
            let Some(union) = node.downcast_ref::<UnionExec>() else {
                return Ok(Transformed::no(node));
            };
            let inputs: Vec<_> = union
                .inputs()
                .iter()
                .filter(|input| !input.is::<EmptyExec>())
                .cloned()
                .collect();
            if inputs.len() == union.inputs().len() {
                return Ok(Transformed::no(node));
            }
            Ok(Transformed::yes(if inputs.is_empty() {
                Arc::new(EmptyExec::new(node.schema()))
            } else {
                UnionExec::try_new(inputs)?
            }))
        })?
        .data)
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

/// Groups `plan`, if it is a JOIN that qualifies. Its inputs' scans read what
/// `demands` asks of them through it ([`Demands::on_join_input`]).
pub(super) fn group_join(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
    demands: &Demands,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // A limit above a JOIN states how many of its rows are needed.
    let grouped = match plan.downcast_ref::<GlobalLimitExec>() {
        Some(limit) => match limit.fetch() {
            Some(fetch) => group_under_limit(limit.input(), limit.skip() + fetch, config, demands)?
                .map(|input| {
                    Arc::clone(&plan).replace_children(
                        vec![input],
                        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                    )
                })
                .transpose()?,
            None => None,
        },
        None => group(&plan, None, config, demands)?,
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
    demands: &Demands,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        return group_under_limit(projection.input(), needed, config, demands)?
            .map(|input| {
                Arc::clone(plan).replace_children(
                    vec![input],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )
            })
            .transpose();
    }
    group(plan, Some(needed), config, demands)
}

/// Groups a JOIN by shard. `needed` is how many of its rows an enclosing limit
/// needs, if any. `None` keeps ordinary execution.
fn group(
    plan: &Arc<dyn ExecutionPlan>,
    needed: Option<usize>,
    config: &ConfigOptions,
    demands: &Demands,
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
            &left,
            &right,
            config.execution.target_partitions,
            needed,
            demands,
        )?,
        (ShardScan::Nodes(left), ShardScan::Nodes(right)) => {
            group_nodes(join, left, right, config)?
        }
        _ => None,
    })
}

/// Partitions: one native JOIN per owner, inside a [`PartitionReadExec`] that
/// admits all inputs' partitions once. Each side reads its scans' partitions on
/// that owner: one scan's, or each scan's of a union. With `needed`, each of its
/// scheduling lanes stops opening partitions once it produced that many rows, so
/// several lanes may each produce them; the enclosing limit stays above and bounds
/// the result. `None` keeps ordinary execution.
fn group_partitions(
    plan: &Arc<dyn ExecutionPlan>,
    join: &HashJoinExec,
    left: &[&TableScanExec],
    right: &[&TableScanExec],
    parallelism: usize,
    needed: Option<usize>,
    demands: &Demands,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let scans = || left.iter().chain(right).copied();
    let Some(local) = scans()
        .next()
        .map(|scan| scan.0.planning.as_ref().unwrap().local)
    else {
        return Ok(None);
    };
    if scans().any(|scan| scan.0.planning.as_ref().unwrap().local != local) {
        return Ok(None);
    }
    let mut placement = None;
    for scan in scans() {
        let SourceWork::TableScan(work) = &scan.0.descriptor.work else {
            return Ok(None);
        };
        let placement = placement.get_or_insert(work.placement);
        placement.require_leader |= work.placement.require_leader;
        placement.options.require_leader |= work.placement.options.require_leader;
    }
    let placement = placement.expect("a JOIN input has a scan");

    // Each provider listed the partitions of its own source. Matching shard keys
    // share a partition only if all lists agree wherever they overlap; a split
    // between the reads breaks that, so keep ordinary execution. The lists may
    // still select different partitions, and each scan keeps its own.
    if !consistent_topologies(scans().map(|scan| &scan.0.planning.as_ref().unwrap().topology[..])) {
        return Ok(None);
    }
    let merge = merge_plan(join)?;
    // Each scan's partitions by owner, with the access that reads them.
    let mut owners = BTreeSet::new();
    let mut sides = Vec::new();
    for side in [left, right] {
        let mut accesses: BTreeMap<Option<GenerationalNodeId>, Vec<_>> = BTreeMap::new();
        for &scan in side {
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
            for (owner, partitions) in groups {
                // An ordered demand on the JOIN's output may reach the side it
                // keeps, which an index can then serve in that order.
                let access = select_access(
                    &planning.source,
                    scan.0.predicate.as_ref(),
                    demands.on_join_input(scan),
                    &scan.0.schema,
                )
                .map_err(external)?;
                accesses
                    .entry(owner)
                    .or_default()
                    .push((scan, partitions, access));
            }
        }
        sides.push((side, accesses));
    }
    // An owner merges where all of its reads come in key order: scans and lookups,
    // not index reads.
    let keyed = |owner: &Option<GenerationalNodeId>| {
        merge.is_some()
            && sides.iter().all(|(_, accesses)| {
                accesses.get(owner).is_some_and(|reads| {
                    reads
                        .iter()
                        .all(|(_, _, access)| matches!(access, Access::Scan | Access::Primary(_)))
                })
            })
    };
    let keyed_owners: BTreeSet<_> = owners.iter().copied().filter(keyed).collect();
    let mut groups = Vec::new();
    for owner in owners {
        let keyed = keyed_owners.contains(&owner);
        let mut inputs = Vec::new();
        for (index, ((side, accesses), input)) in sides
            .iter_mut()
            .zip([join.left(), join.right()])
            .enumerate()
        {
            let Some(reads) = accesses.remove(&owner) else {
                inputs.push(Arc::new(EmptyExec::new(input.schema())) as Arc<dyn ExecutionPlan>);
                continue;
            };
            let field = merge
                .as_ref()
                .filter(|_| keyed)
                .map(|merge| (merge.function, merge.fields[index].as_str()));
            let schema = |scan: &TableScanExec| match field {
                None => (Arc::clone(&scan.0.schema), Arc::clone(&scan.0.statistics)),
                Some((function, field)) => with_key_field(scan, function, field),
            };
            let mut leaves = Vec::new();
            for (scan, partitions, access) in reads {
                let planning = scan.0.planning.as_ref().unwrap();
                let (schema, statistics) = schema(scan);
                let lowered = plan_partition_access(
                    planning.source.table.clone(),
                    &planning.source.manager,
                    partitions,
                    placement,
                    schema,
                    statistics,
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
                leaves.push((scan, lowered));
            }
            // A scan of a union without partitions on this owner reads nothing here.
            for &scan in side.iter() {
                if !leaves.iter().any(|(read, _)| std::ptr::eq(*read, scan)) {
                    let empty = Arc::new(EmptyExec::new(schema(scan).0));
                    leaves.push((scan, empty));
                }
            }
            let lowered = with_scan_leaves(input, &leaves)?;
            let lowered = match field {
                Some(_) => {
                    let reads: Vec<_> = leaves.into_iter().map(|(_, read)| read).collect();
                    carry_key(&lowered, &reads)?
                }
                None => lowered,
            };
            inputs.push(without_empty_inputs(lowered)?);
        }
        let [left, right]: [_; 2] = inputs.try_into().expect("a JOIN has two inputs");
        let joined = match merge.as_ref().filter(|_| keyed) {
            Some(merge) => merge_on_keys(plan, join, left, right, merge)?,
            None => Arc::clone(plan).replace_children(
                vec![left, right],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
        };
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

/// How a shard's JOIN merges its inputs instead of hashing them: on the equality
/// at `at`, whose keys both inputs deliver in the order of `function`, then on the
/// equalities at `extends`, whose keys follow in that order. Each read emits the
/// function's value of its key column as the key field `fields[side]`
/// ([`OrderFunction::key_field`]), which storage computes from each record's ID
/// ([`Table::order_key`](restate_storage_query_api::Table::order_key)), and the
/// merge compares those fields, then the keys of `extends`: `ON s.id = e.entry_id`
/// merges on `base_entry_id(id)` of both, and journal entries
/// `ON a.id = b.id AND a.index = b.index` on `(invocation_id(id), index)`. Equal
/// values of a function mean equal IDs, so the merge matches the same rows.
struct Merge {
    at: usize,
    extends: Vec<usize>,
    function: OrderFunction,
    fields: [String; 2],
}

/// A [`Merge`] for a shard-local JOIN, if one of its equalities qualifies: on both
/// sides the key is the ID column whose function order every scan beneath it
/// declares itself ([`SourceOrders`](crate::source_registry::SourceOrders)), so its
/// reads can emit the key field, and the order reaches the JOIN. The declaration
/// matters: an order on a function of a column that a filter fixed holds
/// trivially, whatever the column holds. Further equalities extend the merge while
/// both inputs come in the order they continue. A JOIN with another condition as
/// well as equalities to check keeps hashing.
fn merge_plan(join: &HashJoinExec) -> Result<Option<Merge>> {
    let ordered = |input: &Arc<dyn ExecutionPlan>, keys: &[Arc<dyn PhysicalExpr>]| {
        input
            .properties()
            .equivalence_properties()
            .ordering_satisfy(
                keys.iter()
                    .map(|key| PhysicalSortExpr::new_default(Arc::clone(key))),
            )
    };
    for (at, (left_key, right_key)) in join.on().iter().enumerate() {
        for &function in OrderFunction::VARIANTS {
            let (Some(left_field), Some(right_field)) = (
                key_field(join.left(), left_key, function),
                key_field(join.right(), right_key, function),
            ) else {
                continue;
            };
            let mut keys = [
                vec![order_key(
                    function,
                    Arc::clone(left_key),
                    &join.left().schema(),
                )?],
                vec![order_key(
                    function,
                    Arc::clone(right_key),
                    &join.right().schema(),
                )?],
            ];
            if !ordered(join.left(), &keys[0])? || !ordered(join.right(), &keys[1])? {
                continue;
            }
            let mut merge = Merge {
                at,
                extends: Vec::new(),
                function,
                fields: [left_field, right_field],
            };
            // Extend the key along both inputs' order.
            'extend: loop {
                for (index, (left, right)) in join.on().iter().enumerate() {
                    if index == at
                        || merge.extends.contains(&index)
                        || partition_keys(join, left, right)
                    {
                        continue;
                    }
                    let extended =
                        |keys: &[Arc<dyn PhysicalExpr>], next: &Arc<dyn PhysicalExpr>| {
                            keys.iter()
                                .cloned()
                                .chain([Arc::clone(next)])
                                .collect::<Vec<_>>()
                        };
                    let (left_keys, right_keys) =
                        (extended(&keys[0], left), extended(&keys[1], right));
                    if ordered(join.left(), &left_keys)? && ordered(join.right(), &right_keys)? {
                        keys = [left_keys, right_keys];
                        merge.extends.push(index);
                        continue 'extend;
                    }
                }
                break;
            }
            return Ok(
                (join.filter().is_none() || checked_pairs(join, &merge).is_empty())
                    .then_some(merge),
            );
        }
    }
    Ok(None)
}

/// The key field of `function` of the JOIN key `key` of `input`, if `key` is a
/// sharding column the function applies to, and each scan beneath `input` reads it
/// from a column whose order by `function` its source declares, under one name.
fn key_field(
    input: &Arc<dyn ExecutionPlan>,
    key: &Arc<dyn PhysicalExpr>,
    function: OrderFunction,
) -> Option<String> {
    if !function.applies_to(sharding_column(input, key)?.kind) {
        return None;
    }
    let mut name = None;
    for (scan, column) in scan_columns(input, key.downcast_ref::<Column>()?.index())? {
        let column = scan.0.schema.field(column).name();
        let declared = scan.0.descriptor.orders().0.iter().any(|order| {
            order.terms.first().is_some_and(|term| {
                &term.column == column && term.function.as_deref() == Some(function.name())
            })
        });
        if !declared || name.is_some_and(|name: &String| name != column) {
            return None;
        }
        name = Some(column);
    }
    Some(function.key_field(name?))
}

/// The scans beneath `plan` that its column `index` reads, each with the column it
/// reads there, through row-preserving operators and unions.
fn scan_columns(
    plan: &Arc<dyn ExecutionPlan>,
    index: usize,
) -> Option<Vec<(&TableScanExec, usize)>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let column = projection
            .expr()
            .get(index)?
            .expr
            .downcast_ref::<Column>()?;
        return scan_columns(projection.input(), column.index());
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let index = match filter.projection() {
            Some(projection) => *projection.get(index)?,
            None => index,
        };
        return scan_columns(filter.input(), index);
    }
    if let Some(limit) = plan.downcast_ref::<GlobalLimitExec>() {
        return scan_columns(limit.input(), index);
    }
    if let Some(limit) = plan.downcast_ref::<LocalLimitExec>() {
        return scan_columns(limit.input(), index);
    }
    if let Some(union) = plan.downcast_ref::<UnionExec>() {
        let mut columns = Vec::new();
        for input in union.inputs() {
            columns.extend(scan_columns(input, index)?);
        }
        return Some(columns);
    }
    Some(vec![(plan.downcast_ref::<TableScanExec>()?, index)])
}

/// The equalities a merge checks on merged pairs, by their index in the JOIN's
/// keys: all but the merged ones and those between partition keys, which hold for
/// every merged pair as both IDs carry the key.
fn checked_pairs(join: &HashJoinExec, merge: &Merge) -> Vec<usize> {
    join.on()
        .iter()
        .enumerate()
        .filter(|&(index, (left, right))| {
            index != merge.at
                && !merge.extends.contains(&index)
                && !partition_keys(join, left, right)
        })
        .map(|(index, _)| index)
        .collect()
}

/// Whether an equality compares the partition keys of both inputs.
fn partition_keys(
    join: &HashJoinExec,
    left: &Arc<dyn PhysicalExpr>,
    right: &Arc<dyn PhysicalExpr>,
) -> bool {
    let partition_key = |plan, expr| {
        sharding_column(plan, expr).is_some_and(|column| column.kind == ShardKeyKind::PartitionKey)
    };
    partition_key(join.left(), left) && partition_key(join.right(), right)
}

/// `scan`'s schema and statistics with `function`'s key field appended.
fn with_key_field(
    scan: &TableScanExec,
    function: OrderFunction,
    field: &str,
) -> (SchemaRef, Arc<Statistics>) {
    let mut fields = scan.0.schema.fields().to_vec();
    fields.push(Arc::new(Field::new(
        field,
        DataType::FixedSizeBinary(function.width() as i32),
        false,
    )));
    let mut statistics = scan.0.statistics.as_ref().clone();
    statistics
        .column_statistics
        .push(datafusion::common::ColumnStatistics::new_unknown());
    (Arc::new(Schema::new(fields)), Arc::new(statistics))
}

/// Passes the key field of `reads`, their last column, up through the filters,
/// projections, limits and unions between them and the JOIN, as their last column
/// too. It is followed by position, never by name: a column above the reads may be
/// named like it, `SELECT id, x AS "base_entry_id(id)"`.
fn carry_key(
    node: &Arc<dyn ExecutionPlan>,
    reads: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    if reads.iter().any(|read| Arc::ptr_eq(node, read)) {
        return Ok(Arc::clone(node));
    }
    if let Some(union) = node.downcast_ref::<UnionExec>() {
        let inputs = union
            .inputs()
            .iter()
            .map(|input| carry_key(input, reads))
            .collect::<Result<Vec<_>>>()?;
        return UnionExec::try_new(inputs);
    }
    let [input] = node.children()[..] else {
        return datafusion::common::plan_err!("no key field below {}", node.name());
    };
    let input = carry_key(input, reads)?;
    let schema = input.schema();
    let at = schema.fields().len() - 1;
    if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
        let field = schema.field(at).name();
        let mut exprs = projection.expr().to_vec();
        exprs.push(ProjectionExpr::new(Arc::new(Column::new(field, at)), field));
        return Ok(Arc::new(ProjectionExec::try_new(exprs, input)?));
    }
    if let Some(filter) = node.downcast_ref::<FilterExec>() {
        // A filter without a projection passes the key on as its last column.
        let projection = filter.projection().as_ref().map(|projection| {
            let mut projection = projection.to_vec();
            projection.push(at);
            projection
        });
        // Clear the projection first: applying one composes it with the old.
        return Ok(Arc::new(
            FilterExecBuilder::from(filter)
                .with_input(input)
                .apply_projection(None)?
                .apply_projection(projection)?
                .build()?,
        ));
    }
    if node.is::<GlobalLimitExec>() || node.is::<LocalLimitExec>() {
        return Arc::clone(node).replace_children(
            vec![input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        );
    }
    datafusion::common::plan_err!("cannot carry a key field through {}", node.name())
}

/// The shard's JOIN `planned` (`plan`) as a merge of `left` and `right`, whose last
/// columns are the key fields of `merge` ([`carry_key`]), followed by its extending
/// keys, with the JOIN's own output columns. An input of several partitions in key
/// order, such as a union of a table's layouts, is combined into one in that order
/// when the template's requirements are settled ([`super::requirements`]). An
/// input that doesn't come in key order after all hashes as planned.
fn merge_on_keys(
    plan: &Arc<dyn ExecutionPlan>,
    planned: &HashJoinExec,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    merge: &Merge,
) -> Result<Arc<dyn ExecutionPlan>> {
    // The key field, then the extending keys, which read the planned columns: the
    // key field comes after them.
    let keys = |input: &Arc<dyn ExecutionPlan>, side: usize| -> Vec<Arc<dyn PhysicalExpr>> {
        let schema = input.schema();
        let at = schema.fields().len() - 1;
        let field: Arc<dyn PhysicalExpr> = Arc::new(Column::new(schema.field(at).name(), at));
        let extends = merge.extends.iter().map(|&index| {
            let (left, right) = &planned.on()[index];
            Arc::clone(if side == 0 { left } else { right })
        });
        std::iter::once(field).chain(extends).collect()
    };
    let (left_keys, right_keys) = (keys(&left, 0), keys(&right, 1));
    let ordered = |input: &Arc<dyn ExecutionPlan>, keys: &[Arc<dyn PhysicalExpr>]| {
        input
            .properties()
            .equivalence_properties()
            .ordering_satisfy(
                keys.iter()
                    .map(|key| PhysicalSortExpr::new_default(Arc::clone(key))),
            )
    };
    if !ordered(&left, &left_keys)? || !ordered(&right, &right_keys)? {
        let without_key = |input: Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
            let schema = input.schema();
            Ok(Arc::new(ProjectionExec::try_new(
                (0..schema.fields().len() - 1).map(|index| {
                    let name = schema.field(index).name();
                    ProjectionExpr::new(Arc::new(Column::new(name, index)), name)
                }),
                input,
            )?))
        };
        return Arc::clone(plan).replace_children(
            vec![without_key(left)?, without_key(right)?],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        );
    }
    let checked: Vec<_> = checked_pairs(planned, merge)
        .into_iter()
        .map(|index| planned.on()[index].clone())
        .collect();
    let filter = if checked.is_empty() {
        planned.filter().cloned()
    } else {
        Some(equalities(
            &checked,
            &left.schema(),
            &right.schema(),
            planned.null_equality(),
        ))
    };
    let join_type = *planned.join_type();
    let sort_options = vec![SortOptions::default(); left_keys.len()];
    let merged: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
        Arc::clone(&left),
        Arc::clone(&right),
        left_keys.into_iter().zip(right_keys).collect(),
        filter,
        join_type,
        sort_options,
        planned.null_equality(),
    )?);
    // The planned JOIN's output columns, found among the merge's by their side
    // and index, then the planned JOIN's own projection.
    let (_, planned_columns) = build_join_schema(
        &planned.left().schema(),
        &planned.right().schema(),
        &join_type,
    );
    let (_, merged_columns) = build_join_schema(&left.schema(), &right.schema(), &join_type);
    let position = |column: &ColumnIndex| {
        merged_columns
            .iter()
            .position(|merged| merged.side == column.side && merged.index == column.index)
            .expect("a merge keeps every column of its inputs")
    };
    let outputs: Vec<usize> = match &planned.projection {
        Some(projection) => projection
            .iter()
            .map(|&at| position(&planned_columns[at]))
            .collect(),
        None => planned_columns.iter().map(position).collect(),
    };
    let names = planned.schema();
    Ok(Arc::new(ProjectionExec::try_new(
        outputs.into_iter().enumerate().map(|(output, at)| {
            ProjectionExpr::new(
                Arc::new(Column::new(names.field(output).name(), at)),
                names.field(output).name(),
            )
        }),
        merged,
    )?))
}

/// The JOIN condition `left = right` for each pair, over a schema of just the
/// columns they read.
fn equalities(
    pairs: JoinOnRef<'_>,
    left: &Schema,
    right: &Schema,
    nulls: NullEquality,
) -> JoinFilter {
    let mut indices: Vec<ColumnIndex> = Vec::new();
    let mut fields = Vec::new();
    let mut read = |expr: &Arc<dyn PhysicalExpr>, side: JoinSide, schema: &Schema| {
        Arc::clone(expr)
            .transform(|node| {
                let Some(column) = node.downcast_ref::<Column>() else {
                    return Ok(Transformed::no(node));
                };
                let at = match indices
                    .iter()
                    .position(|read| read.index == column.index() && read.side == side)
                {
                    Some(at) => at,
                    None => {
                        indices.push(ColumnIndex {
                            index: column.index(),
                            side,
                        });
                        fields.push(schema.field(column.index()).clone());
                        indices.len() - 1
                    }
                };
                Ok(Transformed::yes(
                    Arc::new(Column::new(column.name(), at)) as Arc<dyn PhysicalExpr>
                ))
            })
            .expect("column remapping is infallible")
            .data
    };
    let operator = match nulls {
        NullEquality::NullEqualsNothing => Operator::Eq,
        NullEquality::NullEqualsNull => Operator::IsNotDistinctFrom,
    };
    let condition = conjunction(pairs.iter().map(|(left_key, right_key)| {
        let left_key = read(left_key, JoinSide::Left, left);
        let right_key = read(right_key, JoinSide::Right, right);
        Arc::new(BinaryExpr::new(left_key, operator, right_key)) as Arc<dyn PhysicalExpr>
    }));
    JoinFilter::new(condition, indices, Arc::new(Schema::new(fields)))
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
