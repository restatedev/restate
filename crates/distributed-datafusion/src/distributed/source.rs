// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Concrete access leaves share placement, serialization and local resource binding.
//! Their access path is fixed before distribution; workers never choose it from SQL.

use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Formatter};
use std::ops::RangeBounds;
use std::sync::Arc;

use bilrost::{Message as _, OwnedMessage};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, Statistics, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, StatisticsArgs,
};
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::PhysicalExprNode;
use futures::{StreamExt, TryStreamExt, stream};
use prost::Message;

use restate_types::GenerationalNodeId;
use restate_types::identifiers::{PartitionId, WithPartitionKey};
use restate_types::sharding::KeyRange;
use restate_util_string::ReString;

use crate::access::{InvocationBounds, PrimaryKeyKind, PrimaryKeys, PrimaryRead};
use crate::placement::PartitionPlacement;
use crate::remote_query_scanner_manager::{
    PartitionLocation, PartitionedSource, RemoteScannerManager,
};
use crate::statistics::estimate_source_statistics;
use crate::table_providers::{Scan, ScanPartition};
use crate::table_util::{find_sort_columns, make_ordering};
use crate::{decode_schema, encode_schema};

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;

#[derive(Debug, Clone, Copy, bilrost::Message)]
struct ScanRange {
    #[bilrost(1)]
    partition: PartitionId,
    #[bilrost(2)]
    range: KeyRange,
    #[bilrost(3)]
    invocation: Option<InvocationBounds>,
}

#[derive(Debug, Clone, bilrost::Message)]
struct PrimaryLookup {
    #[bilrost(1)]
    range: ScanRange,
    #[bilrost(2)]
    keys: PrimaryKeys,
}

#[derive(Debug, Clone, bilrost::Message)]
struct Lane<T> {
    #[bilrost(1)]
    reads: Vec<T>,
}

#[derive(Debug, Clone, bilrost::Message)]
struct PartitionWork<T> {
    #[bilrost(1)]
    placement: PartitionPlacement,
    #[bilrost(2)]
    lanes: Vec<Lane<T>>,
}

impl<T> PartitionWork<T> {
    fn from_reads(placement: PartitionPlacement, reads: Vec<T>, target_partitions: usize) -> Self {
        let count = target_partitions.max(1).min(reads.len());
        let mut lanes: Vec<_> = (0..count).map(|_| Lane { reads: vec![] }).collect();
        for (i, read) in reads.into_iter().enumerate() {
            lanes[i % count].reads.push(read);
        }
        Self { placement, lanes }
    }

    fn fmt_summary(&self, f: &mut Formatter, partition: impl Fn(&T) -> PartitionId) -> fmt::Result {
        let mut partitions: Vec<_> = self
            .lanes
            .iter()
            .flat_map(|lane| &lane.reads)
            .map(partition)
            .collect();
        partitions.sort_unstable();
        partitions.dedup();
        write!(f, ", lanes={}, partitions={partitions:?}", self.lanes.len())
    }

    fn validate(&self, range: impl Fn(&T) -> &ScanRange) -> Result<usize> {
        if self.lanes.is_empty() || self.lanes.iter().any(|lane| lane.reads.is_empty()) {
            return plan_err!("storage stage has an empty execution lane");
        }
        let mut ranges = Vec::new();
        for lane in &self.lanes {
            let mut previous = None;
            for read in &lane.reads {
                let read = range(read);
                if previous.is_some_and(|end| end >= read.range.start()) {
                    return plan_err!("storage lane ranges must be ordered and disjoint");
                }
                previous = Some(read.range.end());
                ranges.push(read);
            }
        }
        ranges.sort_unstable_by_key(|read| (read.partition, read.range.start()));
        if ranges.windows(2).any(|pair| {
            pair[0].partition == pair[1].partition && pair[0].range.end() >= pair[1].range.start()
        }) {
            return plan_err!("storage lanes contain overlapping partition ranges");
        }
        Ok(self.lanes.len())
    }
}

#[derive(Debug, Clone, bilrost::Message, bilrost::Oneof)]
enum SourceWork {
    Unknown,
    #[bilrost(1)]
    TableScan(PartitionWork<ScanRange>),
    #[bilrost(2)]
    Node(()),
    #[bilrost(3)]
    MultiGet(PartitionWork<PrimaryLookup>),
}

#[derive(Debug, Clone, bilrost::Message)]
struct SourceDescriptor {
    #[bilrost(1)]
    table: ReString,
    #[bilrost(2)]
    owner: Option<GenerationalNodeId>,
    #[bilrost(3)]
    work: SourceWork,
    #[bilrost(4)]
    ordering: Vec<String>,
    #[bilrost(5)]
    limit: Option<u64>,
}

#[derive(Debug)]
enum LocalBinding {
    Partition(RemoteScannerManager, Arc<dyn ScanPartition>),
    Node(Arc<dyn Scan>),
}

/// Shared source metadata and resources, not an execution operator or access selector.
#[derive(Debug)]
struct SourcePlan {
    descriptor: SourceDescriptor,
    schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    properties: Arc<PlanProperties>,
    binding: Option<LocalBinding>,
    metrics: ExecutionPlanMetricsSet,
    // Coordinator planning hints. Serialized tasks already have their join order
    // fixed, so workers can decode the existing wire format with unknown estimates.
    statistics: Arc<Statistics>,
    planning: Option<ScanPlanning>,
}

#[derive(Debug)]
struct ScanPlanning {
    source: PartitionedSource,
    local: bool,
    target_partitions: usize,
}

#[derive(Debug)]
pub(crate) struct TableScanExec(SourcePlan);

#[derive(Debug)]
pub(crate) struct MultiGetExec(SourcePlan);

#[derive(Debug)]
pub(crate) struct NodeScanExec(SourcePlan);

impl TableScanExec {
    /// Providers create query-specific plans. The first physical rule selects
    /// access and placement before ordering/distribution enforcement can rely on
    /// the resulting properties.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_query(
        source: PartitionedSource,
        ranges: Vec<(PartitionId, KeyRange)>,
        placement: PartitionPlacement,
        target_partitions: usize,
        schema: SchemaRef,
        statistics: Arc<Statistics>,
        ordering: Vec<String>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
        local: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut ranges = ranges;
        ranges.sort_unstable_by_key(|(_, range)| range.start());
        let work = PartitionWork::from_reads(
            placement,
            ranges
                .into_iter()
                .map(|(partition, range)| ScanRange {
                    partition,
                    range,
                    invocation: None,
                })
                .collect(),
            target_partitions,
        );
        let descriptor = SourceDescriptor {
            table: source.table.clone(),
            owner: None,
            work: SourceWork::TableScan(work),
            ordering,
            limit: limit.map(|l| l as u64),
        };
        let mut plan = SourcePlan::new(descriptor, schema, predicate, None, Some(statistics))?;
        plan.planning = Some(ScanPlanning {
            source,
            local,
            target_partitions,
        });
        Ok(Arc::new(Self(plan)))
    }
}

/// Static primary selection is a physical rewrite, before owner-stage lowering.
/// Residual FilterExec nodes remain in the plan, including unsupported conjuncts.
#[derive(Debug)]
pub(crate) struct PrimaryAccessRule;

impl PhysicalOptimizerRule for PrimaryAccessRule {
    fn name(&self) -> &str {
        "restate_primary_access"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(scan) = plan.downcast_ref::<TableScanExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(planning) = &scan.0.planning else {
                return Ok(Transformed::no(plan));
            };
            let SourceWork::TableScan(work) = &scan.0.descriptor.work else {
                unreachable!("table scan planning")
            };
            let keys = planning
                .source
                .primary_key
                .map(|kind| kind.select(scan.0.predicate.as_slice()))
                .transpose()
                .map_err(external)?
                .flatten();
            let ranges = work
                .lanes
                .iter()
                .flat_map(|lane| &lane.reads)
                .map(|read| (read.partition, read.range))
                .collect();
            Ok(Transformed::yes(plan_partition_access(
                planning.source.table.clone(),
                &planning.source.manager,
                ranges,
                work.placement,
                planning.target_partitions,
                Arc::clone(&scan.0.schema),
                Arc::clone(&scan.0.statistics),
                scan.0.descriptor.ordering.clone(),
                scan.0.predicate.clone(),
                scan.0.descriptor.limit.map(|l| l as usize),
                keys,
                planning.source.primary_key == Some(PrimaryKeyKind::InvocationRange),
                planning.local,
            )?))
        })
        .map(|result| result.data)
    }
}

/// Select primary access before owner/lane assignment. Exact-key holes remain
/// explicit even when partition pruning used an enclosing range.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_partition_access(
    table: ReString,
    manager: &RemoteScannerManager,
    ranges: Vec<(PartitionId, KeyRange)>,
    placement: PartitionPlacement,
    target_partitions: usize,
    schema: SchemaRef,
    statistics: Arc<Statistics>,
    ordering: Vec<String>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
    keys: Option<PrimaryKeys>,
    invocation_range: bool,
    local: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut placements = HashMap::new();
    let mut owners: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (partition, range) in ranges {
        let selected = keys.as_ref().map(|keys| keys.within(range));
        if selected.as_ref().is_some_and(|keys| keys.len() == 0) {
            continue;
        }
        let owner = if local {
            None
        } else {
            Some(match placements.entry((partition, placement)) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => *entry.insert(
                    manager
                        .partition_owner(partition, placement)
                        .map_err(external)?,
                ),
            })
        };
        let invocation = if invocation_range {
            match &selected {
                Some(PrimaryKeys::Invocation(ids)) => Some(InvocationBounds {
                    first: *ids.first().expect("empty selections were removed"),
                    last: *ids.last().expect("empty selections were removed"),
                }),
                None => None,
                _ => return plan_err!("incompatible invocation range keys"),
            }
        } else {
            None
        };
        owners.entry(owner).or_default().push((
            ScanRange {
                partition,
                range,
                invocation,
            },
            selected,
        ));
    }
    let total_reads = owners.values().map(Vec::len).sum::<usize>();
    let mut plans = Vec::with_capacity(owners.len());
    for (owner, mut reads) in owners {
        let max_rows = (keys.is_some() && !invocation_range).then(|| {
            reads
                .iter()
                .filter_map(|(_, keys)| keys.as_ref())
                .map(PrimaryKeys::len)
                .sum()
        });
        let statistics = estimate_source_statistics(
            &statistics,
            reads.len() as f64 / total_reads as f64,
            max_rows,
        )?;
        reads.sort_unstable_by_key(|(range, _)| range.range.start());
        let work = if keys.is_some() && !invocation_range {
            SourceWork::MultiGet(PartitionWork::from_reads(
                placement,
                reads
                    .into_iter()
                    .map(|(range, keys)| PrimaryLookup {
                        range,
                        keys: keys.expect("selected primary keys"),
                    })
                    .collect(),
                target_partitions,
            ))
        } else {
            SourceWork::TableScan(PartitionWork::from_reads(
                placement,
                reads.into_iter().map(|(range, _)| range).collect(),
                target_partitions,
            ))
        };
        let descriptor = SourceDescriptor {
            table: table.clone(),
            owner,
            work,
            ordering: ordering.clone(),
            limit: limit.map(|l| l as u64),
        };
        plans.push(
            SourcePlan::new(
                descriptor,
                Arc::clone(&schema),
                predicate.clone(),
                local.then_some(manager),
                Some(statistics),
            )?
            .into_plan(),
        );
    }
    match plans.len() {
        0 => Ok(Arc::new(EmptyExec::new(schema))),
        1 => Ok(plans.pop().unwrap()),
        _ => UnionExec::try_new(plans),
    }
}

fn external(error: anyhow::Error) -> DataFusionError {
    DataFusionError::External(error.into())
}

impl NodeScanExec {
    pub(crate) fn for_node(
        table: ReString,
        owner: GenerationalNodeId,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(Self(SourcePlan::new(
            SourceDescriptor {
                table,
                owner: Some(owner),
                work: SourceWork::Node(()),
                ordering: vec![],
                limit: limit.map(|l| l as u64),
            },
            schema,
            None,
            None,
            None,
        )?))
    }
}

impl SourcePlan {
    fn new(
        descriptor: SourceDescriptor,
        schema: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        manager: Option<&RemoteScannerManager>,
        statistics: Option<Arc<Statistics>>,
    ) -> Result<Self> {
        let lanes = match &descriptor.work {
            SourceWork::TableScan(work) => {
                for read in work.lanes.iter().flat_map(|lane| &lane.reads) {
                    if let Some(bounds) = read.invocation
                        && (bounds.first > bounds.last
                            || !read.range.contains(&bounds.first.partition_key())
                            || !read.range.contains(&bounds.last.partition_key()))
                    {
                        return plan_err!("invalid invocation range bounds");
                    }
                }
                work.validate(|read| read)?
            }
            SourceWork::MultiGet(work) => {
                let count = work.validate(|read| &read.range)?;
                let mut kind = None;
                for lookup in work.lanes.iter().flat_map(|lane| &lane.reads) {
                    if lookup.range.invocation.is_some() {
                        return plan_err!("multi-get cannot carry invocation range bounds");
                    }
                    lookup.keys.validate(lookup.range.range).map_err(external)?;
                    if kind.is_some() && kind != lookup.keys.kind() {
                        return plan_err!("mixed primary key kinds in a lookup stage");
                    }
                    kind = lookup.keys.kind();
                }
                count
            }
            SourceWork::Node(()) => 1,
            SourceWork::Unknown => return plan_err!("unknown query source scope"),
        };
        let binding = manager
            .map(|manager| {
                if descriptor
                    .owner
                    .is_some_and(|owner| owner != manager.node_id())
                {
                    return exec_err!("storage task delivered to the wrong owner");
                }
                match &descriptor.work {
                    SourceWork::Node(()) => manager
                        .local_node_scanner(&descriptor.table)
                        .map(LocalBinding::Node)
                        .ok_or_else(|| {
                            datafusion::common::exec_datafusion_err!(
                                "local node source '{}' is unavailable",
                                descriptor.table
                            )
                        }),
                    SourceWork::TableScan(work) => Self::bind_partition(
                        manager,
                        &descriptor.table,
                        work.placement,
                        work.lanes
                            .iter()
                            .flat_map(|lane| &lane.reads)
                            .any(|read| read.invocation.is_some())
                            .then_some(PrimaryKeyKind::InvocationRange),
                    ),
                    SourceWork::MultiGet(work) => Self::bind_partition(
                        manager,
                        &descriptor.table,
                        work.placement,
                        work.lanes[0].reads[0].keys.kind(),
                    ),
                    SourceWork::Unknown => plan_err!("unknown query source scope"),
                }
            })
            .transpose()?;
        // Point lookups do not automatically inherit the table iterator's SQL
        // ordering (notably string IDs versus their physical key representation).
        let ordering = match descriptor.work {
            SourceWork::TableScan(_) => {
                make_ordering(find_sort_columns(&descriptor.ordering, &schema))
            }
            _ => make_ordering(vec![]),
        };
        let properties = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new_with_orderings(Arc::clone(&schema), [ordering]),
                Partitioning::UnknownPartitioning(lanes),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            .with_scheduling_type(SchedulingType::Cooperative),
        );
        let statistics = statistics.unwrap_or_else(|| Arc::new(Statistics::new_unknown(&schema)));
        if statistics.column_statistics.len() != schema.fields().len() {
            return plan_err!("source statistics do not match the projected schema");
        }
        Ok(Self {
            descriptor,
            schema,
            predicate,
            properties,
            binding,
            metrics: ExecutionPlanMetricsSet::new(),
            statistics,
            planning: None,
        })
    }

    fn bind_partition(
        manager: &RemoteScannerManager,
        table: &str,
        placement: PartitionPlacement,
        keys: Option<crate::access::PrimaryKeyKind>,
    ) -> Result<LocalBinding> {
        let Some(reader) = manager.local_partition_scanner(table) else {
            return exec_err!("local query source '{table}' is unavailable");
        };
        if manager.local_node_scanner(table).is_some()
            || reader.partition_source() != placement.source
        {
            return exec_err!("local query source has incompatible placement requirements");
        }
        if keys.is_some() && reader.primary_key_kind() != keys {
            return exec_err!("local query source has incompatible primary lookup capability");
        }
        Ok(LocalBinding::Partition(manager.clone(), reader))
    }

    fn into_plan(self) -> Arc<dyn ExecutionPlan> {
        match self.descriptor.work {
            SourceWork::TableScan(_) => Arc::new(TableScanExec(self)),
            SourceWork::MultiGet(_) => Arc::new(MultiGetExec(self)),
            SourceWork::Node(()) => Arc::new(NodeScanExec(self)),
            SourceWork::Unknown => unreachable!("validated source work"),
        }
    }

    fn execute(&self, lane: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let Some(binding) = &self.binding else {
            return exec_err!("unbound owner-only storage scan reached the coordinator");
        };
        let (manager, reader) = match binding {
            LocalBinding::Node(reader) => {
                if lane != 0 {
                    return exec_err!("invalid node scan lane {lane}");
                }
                return Ok(reader.scan(
                    Arc::clone(&self.schema),
                    &[],
                    context.session_config().batch_size(),
                    self.descriptor.limit.map(|l| l as usize),
                ));
            }
            LocalBinding::Partition(manager, reader) => (manager.clone(), Arc::clone(reader)),
        };
        let (placement, reads): (_, Vec<_>) = match &self.descriptor.work {
            SourceWork::TableScan(work) => (
                work.placement,
                work.lanes
                    .get(lane)
                    .ok_or_else(|| {
                        datafusion::common::exec_datafusion_err!("invalid storage lane {lane}")
                    })?
                    .reads
                    .iter()
                    .map(|range| {
                        (
                            *range,
                            range
                                .invocation
                                .map_or(PrimaryRead::Range, PrimaryRead::InvocationRange),
                        )
                    })
                    .collect(),
            ),
            SourceWork::MultiGet(work) => (
                work.placement,
                work.lanes
                    .get(lane)
                    .ok_or_else(|| {
                        datafusion::common::exec_datafusion_err!("invalid storage lane {lane}")
                    })?
                    .reads
                    .iter()
                    .map(|lookup| {
                        (
                            lookup.range,
                            PrimaryRead::MultiGet(Arc::new(lookup.keys.clone())),
                        )
                    })
                    .collect(),
            ),
            _ => return exec_err!("query source binding has the wrong scope"),
        };
        let metrics = BaselineMetrics::new(&self.metrics, lane);
        let schema = Arc::clone(&self.schema);
        let predicate = self.predicate.clone();
        let limit = self.descriptor.limit.map(|l| l as usize);
        let batch_size = context.session_config().batch_size();
        let compute = metrics.elapsed_compute().clone();
        let requests = MetricBuilder::new(&self.metrics).counter("storage_requests", lane);
        let requested_keys = MetricBuilder::new(&self.metrics).counter("requested_keys", lane);
        let stream = stream::iter(reads)
            .map(move |(range, access)| {
                if !matches!(
                    manager
                        .get_partition_target_node(range.partition, placement)
                        .map_err(external)?,
                    PartitionLocation::Local
                ) {
                    return exec_err!(
                        "storage ownership changed for partition {}",
                        range.partition
                    );
                }
                requests.add(1);
                if let PrimaryRead::MultiGet(keys) = &access {
                    requested_keys.add(keys.len());
                }
                reader
                    .read_partition(
                        range.partition,
                        range.range,
                        access,
                        Arc::clone(&schema),
                        predicate.clone(),
                        batch_size,
                        limit,
                        compute.clone(),
                    )
                    .map_err(external)
            })
            .try_flatten()
            .inspect(move |batch| {
                if let Ok(batch) = batch {
                    metrics.record_output(batch.num_rows());
                }
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

fn source_plan(plan: &dyn ExecutionPlan) -> Option<&SourcePlan> {
    plan.downcast_ref::<TableScanExec>()
        .map(|plan| &plan.0)
        .or_else(|| plan.downcast_ref::<MultiGetExec>().map(|plan| &plan.0))
        .or_else(|| plan.downcast_ref::<NodeScanExec>().map(|plan| &plan.0))
}

/// Shared source discovery for placement, stage construction and transport.
pub(super) fn source_owner(plan: &dyn ExecutionPlan) -> Option<GenerationalNodeId> {
    source_plan(plan).and_then(|source| source.descriptor.owner)
}

macro_rules! impl_access_exec {
    ($ty:ident) => {
        impl DisplayAs for $ty {
            fn fmt_as(&self, format: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
                let source = &self.0;
                write!(f, "{}: table={}", self.name(), source.descriptor.table)?;
                if let Some(owner) = source.descriptor.owner {
                    write!(f, ", owner={owner}")?;
                }
                match (&source.descriptor.work, format) {
                    (work, DisplayFormatType::Verbose) => write!(f, ", work={work:?}")?,
                    (SourceWork::TableScan(work), _) => {
                        work.fmt_summary(f, |read| read.partition)?;
                    }
                    (SourceWork::MultiGet(work), _) => {
                        work.fmt_summary(f, |lookup| lookup.range.partition)?;
                    }
                    (work, _) => write!(f, ", work={work:?}")?,
                }
                write!(f, ", bound={}", source.binding.is_some())?;
                if let Some(predicate) = &source.predicate {
                    write!(f, ", predicate={predicate}")?;
                }
                Ok(())
            }
        }
        impl ExecutionPlan for $ty {
            fn name(&self) -> &str {
                stringify!($ty)
            }
            fn properties(&self) -> &Arc<PlanProperties> {
                &self.0.properties
            }
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![]
            }
            fn with_new_children(
                self: Arc<Self>,
                children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                if !children.is_empty() {
                    return plan_err!("storage access leaves cannot have children");
                }
                Ok(self)
            }
            fn apply_expressions(
                &self,
                f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
            ) -> Result<TreeNodeRecursion> {
                datafusion::physical_plan::apply_expression_roots(self.0.predicate.iter(), f)
            }
            fn execute(
                &self,
                partition: usize,
                context: Arc<TaskContext>,
            ) -> Result<SendableRecordBatchStream> {
                self.0.execute(partition, context)
            }
            fn metrics(&self) -> Option<MetricsSet> {
                Some(self.0.metrics.clone_inner())
            }
            fn statistics_from_inputs(
                &self,
                _: &[Arc<Statistics>],
                args: &StatisticsArgs,
            ) -> Result<Arc<Statistics>> {
                if let Some(partition) = args.partition() {
                    let lanes = self.0.properties.partitioning.partition_count();
                    if partition >= lanes {
                        return plan_err!("invalid source statistics partition {partition}");
                    }
                    estimate_source_statistics(&self.0.statistics, 1.0 / lanes as f64, None)
                } else {
                    Ok(Arc::clone(&self.0.statistics))
                }
            }
        }
    };
}

impl_access_exec!(TableScanExec);
impl_access_exec!(MultiGetExec);
impl_access_exec!(NodeScanExec);

#[derive(Debug, Default)]
pub(super) struct SourceCodec {
    pub manager: Option<RemoteScannerManager>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SourceProto {
    #[prost(bytes = "vec", tag = "101")]
    descriptor: Vec<u8>,
    #[prost(bytes = "vec", tag = "102")]
    schema: Vec<u8>,
    #[prost(message, optional, tag = "103")]
    predicate: Option<PhysicalExprNode>,
}

impl PhysicalExtensionCodec for SourceCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return plan_err!("storage access leaves cannot have children");
        }
        let proto = SourceProto::decode(buf).map_err(|err| external(err.into()))?;
        let descriptor = SourceDescriptor::decode(proto.descriptor.as_slice())
            .map_err(|err| external(err.into()))?;
        if descriptor.owner.is_none() {
            return plan_err!("distributed source is missing its owner");
        }
        let schema = Arc::new(decode_schema(&proto.schema).map_err(external)?);
        let predicate = proto
            .predicate
            .as_ref()
            .map(|predicate| {
                converter.proto_to_physical_expr(
                    predicate,
                    &schema,
                    &PhysicalPlanDecodeContext::new(ctx, self),
                )
            })
            .transpose()?;
        Ok(
            SourcePlan::new(descriptor, schema, predicate, self.manager.as_ref(), None)?
                .into_plan(),
        )
    }

    fn try_encode(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(source) = source_plan(plan.as_ref()) else {
            return plan_err!("unsupported Restate query source");
        };
        if source.planning.is_some() || source.descriptor.owner.is_none() {
            return plan_err!("cannot encode an unplanned or local-only query source");
        }
        SourceProto {
            descriptor: source.descriptor.encode_to_vec(),
            schema: encode_schema(&source.schema),
            predicate: source
                .predicate
                .as_ref()
                .map(|expr| converter.physical_expr_to_proto(expr, self))
                .transpose()?,
        }
        .encode(buf)
        .map_err(|err| external(err.into()))
    }
}
