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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Formatter};
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
use datafusion::physical_plan::limit::LimitStream;
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
use futures::{TryStreamExt, stream};
use prost::Message;

use restate_types::GenerationalNodeId;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_util_string::ReString;

use crate::access::{Access, PrimaryKeys, PrimaryRead, select_access};
use crate::placement::PartitionPlacement;
use crate::source_registry::{PartitionLocation, PartitionSourceDescriptor, SourceRegistry};
use crate::statistics::estimate_source_statistics;
use crate::table_providers::{
    PartitionReadContext, PreparedPartitionRead, ProjectedColumns, ReadMetrics, Scan,
};
use crate::table_util::{find_sort_columns, make_ordering};
use crate::{decode_schema, encode_schema};

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "partition_read_tests.rs"]
mod partition_read_tests;

#[path = "partition_reads.rs"]
mod partition_reads;
#[path = "shard_joins.rs"]
mod shard_joins;
use partition_reads::{PartitionReadExec, PartitionReadsProto};

#[derive(Debug, Clone, Copy, bilrost::Message)]
struct PartitionScan {
    #[bilrost(1)]
    partition: PartitionId,
}

#[derive(Debug, Clone, bilrost::Message)]
struct PrimaryLookup {
    #[bilrost(1)]
    partition: PartitionId,
    #[bilrost(tag(2), encoding(restate_encoding::Arced))]
    keys: Arc<PrimaryKeys>,
}

#[derive(Debug, Clone, bilrost::Message)]
struct PartitionWork<T> {
    #[bilrost(1)]
    placement: PartitionPlacement,
    // One source partition per storage partition. Scan bounds and navigation are
    // prepared by the owner from the predicate, never serialized as range work.
    #[bilrost(2)]
    reads: Vec<T>,
}

impl<T> PartitionWork<T> {
    fn from_reads(placement: PartitionPlacement, reads: Vec<T>) -> Self {
        Self { placement, reads }
    }

    fn fmt_summary(&self, f: &mut Formatter, partition: impl Fn(&T) -> PartitionId) -> fmt::Result {
        let mut partitions: Vec<_> = self.reads.iter().map(partition).collect();
        partitions.sort_unstable();
        partitions.dedup();
        write!(f, ", partitions={partitions:?}, reads={}", self.reads.len(),)
    }

    fn validate(&self, partition: impl Fn(&T) -> PartitionId) -> Result<usize> {
        if self.reads.is_empty() {
            return plan_err!("storage source has no partitions");
        }
        let mut partitions = BTreeSet::new();
        if self
            .reads
            .iter()
            .any(|read| !partitions.insert(partition(read)))
        {
            return plan_err!("storage source contains duplicate partitions");
        }
        Ok(self.reads.len())
    }
}

/// Partition reads through a complete index of the source, chosen by the planner.
/// Workers derive the index restriction from the shipped predicate.
#[derive(Debug, Clone, bilrost::Message)]
struct IndexWork {
    #[bilrost(1)]
    index: ReString,
    #[bilrost(2)]
    work: PartitionWork<PartitionScan>,
}

/// Workers that predate a variant decode it as `Unknown` and fail the query.
#[derive(Debug, Clone, bilrost::Message, bilrost::Oneof)]
enum SourceWork {
    Unknown,
    #[bilrost(1)]
    TableScan(PartitionWork<PartitionScan>),
    #[bilrost(2)]
    Node(()),
    #[bilrost(3)]
    MultiGet(PartitionWork<PrimaryLookup>),
    #[bilrost(4)]
    IndexScan(IndexWork),
}

impl SourceWork {
    /// Partition scans of table and index work.
    fn scans(&self) -> Option<&PartitionWork<PartitionScan>> {
        match self {
            Self::TableScan(work) | Self::IndexScan(IndexWork { work, .. }) => Some(work),
            _ => None,
        }
    }
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

#[derive(Debug, Clone)]
enum LocalBinding {
    Partition {
        manager: SourceRegistry,
        placement: PartitionPlacement,
        reads: Arc<[BoundPartitionRead]>,
    },
    Node(Arc<dyn Scan>),
}

#[derive(Debug)]
struct BoundPartitionRead {
    partition: PartitionId,
    requested_keys: usize,
    read: Arc<dyn PreparedPartitionRead>,
}

/// Shared source metadata and resources, not an execution operator or access selector.
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
struct ScanPlanning {
    source: PartitionSourceDescriptor,
    local: bool,
    /// The source's partitions as the provider read them. A shard-local JOIN
    /// requires both inputs' snapshots to agree, so that a split between two
    /// provider reads cannot place matching keys in different partitions.
    topology: Arc<[(PartitionId, Partition)]>,
    // Topology metadata is only needed to group complete primary keys. Scan
    // execution receives partition identities and the original predicate.
    partitions: Vec<(PartitionId, Partition)>,
}

#[derive(Debug)]
pub(crate) struct TableScanExec(SourcePlan);

#[derive(Debug)]
pub(crate) struct MultiGetExec(SourcePlan);

#[derive(Debug)]
pub(crate) struct IndexScanExec(SourcePlan);

#[derive(Debug)]
pub(crate) struct NodeScanExec(SourcePlan);

impl TableScanExec {
    /// Providers create query-specific plans. The first physical rule selects
    /// access and placement before ordering/distribution enforcement can rely on
    /// the resulting properties.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_query(
        source: PartitionSourceDescriptor,
        topology: Arc<[(PartitionId, Partition)]>,
        partitions: Vec<(PartitionId, Partition)>,
        placement: PartitionPlacement,
        schema: SchemaRef,
        statistics: Arc<Statistics>,
        ordering: Vec<String>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
        local: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The placeholder advertises the same partition count as the access
        // leaves produced by the first physical rule, before primary-key pruning.
        let work = PartitionWork::from_reads(
            placement,
            partitions
                .iter()
                .map(|(partition, _)| PartitionScan {
                    partition: *partition,
                })
                .collect(),
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
            topology,
            partitions,
        });
        Ok(Arc::new(Self(plan)))
    }
}

/// Static access selection is a physical rewrite, before owner-stage lowering.
/// Residual FilterExec nodes remain in the plan, including unsupported conjuncts.
/// The same selection applies to ordinary and shard-JOIN inputs.
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
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = plan
            .transform_down(|plan| shard_joins::group_join(plan, config))?
            .data;
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
            let access =
                select_access(&planning.source, scan.0.predicate.as_ref()).map_err(external)?;
            Ok(Transformed::yes(plan_partition_access(
                planning.source.table.clone(),
                &planning.source.manager,
                planning.partitions.clone(),
                work.placement,
                Arc::clone(&scan.0.schema),
                Arc::clone(&scan.0.statistics),
                scan.0.descriptor.ordering.clone(),
                scan.0.predicate.clone(),
                scan.0.descriptor.limit.map(|l| l as usize),
                access,
                planning.local,
                config.execution.target_partitions,
            )?))
        })
        .and_then(|result| {
            result.data.transform_up(|plan| {
                partition_reads::group_sources(plan, config.execution.target_partitions)
            })
        })
        .map(|result| result.data)
    }
}

/// Group each physical source by owner, retaining explicit storage partition IDs.
/// PartitionReadExec owns admission and bounds concurrently active partition scopes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_partition_access(
    table: ReString,
    manager: &SourceRegistry,
    partitions: Vec<(PartitionId, Partition)>,
    placement: PartitionPlacement,
    schema: SchemaRef,
    statistics: Arc<Statistics>,
    ordering: Vec<String>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    limit: Option<usize>,
    access: Access,
    local: bool,
    parallelism: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let total_partitions = partitions.len();
    let keys = match &access {
        Access::Primary(keys) => Some(keys),
        _ => None,
    };
    let mut owners: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for (partition, metadata) in partitions {
        let selected = keys.and_then(|keys| keys.within(metadata.key_range));
        if keys.is_some() && selected.is_none() {
            continue;
        }
        let owner = if local {
            None
        } else {
            Some(
                manager
                    .partition_owner(partition, placement)
                    .map_err(external)?,
            )
        };
        owners.entry(owner).or_default().push((partition, selected));
    }
    if owners.is_empty() {
        return Ok(Arc::new(EmptyExec::new(schema)));
    }
    let plans = owners
        .into_iter()
        .map(|(owner, reads)| {
            let count = reads.len();
            let max_rows = keys.map(|_| {
                reads
                    .iter()
                    .map(|(_, keys)| keys.as_ref().expect("selected keys").len())
                    .sum()
            });
            let statistics = estimate_source_statistics(
                &statistics,
                count as f64 / total_partitions as f64,
                max_rows,
            )?;
            let scans = |reads: Vec<(PartitionId, Option<PrimaryKeys>)>| {
                PartitionWork::from_reads(
                    placement,
                    reads
                        .into_iter()
                        .map(|(partition, _)| PartitionScan { partition })
                        .collect(),
                )
            };
            let work = match &access {
                Access::Primary(_) => SourceWork::MultiGet(PartitionWork::from_reads(
                    placement,
                    reads
                        .into_iter()
                        .map(|(partition, keys)| PrimaryLookup {
                            partition,
                            keys: Arc::new(keys.expect("selected primary keys")),
                        })
                        .collect(),
                )),
                Access::Scan => SourceWork::TableScan(scans(reads)),
                Access::Index(index) => SourceWork::IndexScan(IndexWork {
                    index: index.clone(),
                    work: scans(reads),
                }),
            };
            let descriptor = SourceDescriptor {
                table: table.clone(),
                owner,
                work,
                ordering: ordering.clone(),
                limit: limit.map(|l| l as u64),
            };
            let source = SourcePlan::new(
                descriptor,
                Arc::clone(&schema),
                predicate.clone(),
                local.then_some(manager),
                Some(statistics),
            )?
            .into_plan();
            Ok(
                Arc::new(PartitionReadExec::new(vec![source], parallelism, None)?)
                    as Arc<dyn ExecutionPlan>,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    UnionExec::try_new(plans)
}

fn external(error: anyhow::Error) -> DataFusionError {
    // The ordinary anyhow-to-Box conversion hides the underlying type from
    // downcasting, which would erase admission metadata before transport.
    DataFusionError::External(error.reallocate_into_boxed_dyn_error_without_backtrace())
}

impl NodeScanExec {
    pub(crate) fn for_node(
        table: ReString,
        owner: GenerationalNodeId,
        schema: SchemaRef,
        limit: Option<usize>,
        statistics: Option<Arc<Statistics>>,
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
            statistics,
        )?))
    }
}

impl SourcePlan {
    fn new(
        descriptor: SourceDescriptor,
        schema: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        manager: Option<&SourceRegistry>,
        statistics: Option<Arc<Statistics>>,
    ) -> Result<Self> {
        match &descriptor.work {
            SourceWork::TableScan(work) | SourceWork::IndexScan(IndexWork { work, .. }) => {
                work.validate(|read| read.partition)?
            }
            SourceWork::MultiGet(work) => {
                let count = work.validate(|read| read.partition)?;
                for lookup in &work.reads {
                    lookup.keys.validate().map_err(external)?;
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
                    SourceWork::TableScan(work) | SourceWork::IndexScan(IndexWork { work, .. }) => {
                        Self::bind_partition(
                            manager,
                            &descriptor.table,
                            work.placement,
                            &descriptor.work,
                            &schema,
                            &predicate,
                            descriptor.limit,
                        )
                    }
                    SourceWork::MultiGet(work) => Self::bind_partition(
                        manager,
                        &descriptor.table,
                        work.placement,
                        &descriptor.work,
                        &schema,
                        &predicate,
                        descriptor.limit,
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
                // Concrete sources are single-partition templates. The enclosing
                // PartitionReadExec supplies partition identity and resources.
                Partitioning::UnknownPartitioning(1),
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

    #[allow(clippy::too_many_arguments)]
    fn bind_partition(
        manager: &SourceRegistry,
        table: &str,
        placement: PartitionPlacement,
        work: &SourceWork,
        schema: &SchemaRef,
        predicate: &Option<Arc<dyn PhysicalExpr>>,
        limit: Option<u64>,
    ) -> Result<LocalBinding> {
        let Some(reader) = manager.local_partition_scanner(table) else {
            return exec_err!("local query source '{table}' is unavailable");
        };
        if reader.requires_leader() && !placement.require_leader {
            return exec_err!("local query source has incompatible placement requirements");
        }
        if matches!(work, SourceWork::MultiGet(_)) && !reader.supports_primary_lookup() {
            return exec_err!("local query source has incompatible primary lookup capability");
        }
        if let SourceWork::IndexScan(IndexWork { index, .. }) = work
            && !reader.supports_index(index)
        {
            return exec_err!("local query source '{table}' cannot read index '{index}'");
        }
        let mut requests: Vec<_> = match work {
            SourceWork::TableScan(work) => work
                .reads
                .iter()
                .map(|read| (read.partition, PrimaryRead::Scan))
                .collect(),
            SourceWork::IndexScan(IndexWork { index, work }) => work
                .reads
                .iter()
                .map(|read| (read.partition, PrimaryRead::Index(index.clone())))
                .collect(),
            SourceWork::MultiGet(work) => work
                .reads
                .iter()
                .map(|lookup| {
                    (
                        lookup.partition,
                        PrimaryRead::MultiGet(Arc::clone(&lookup.keys)),
                    )
                })
                .collect(),
            _ => return plan_err!("partition binding requires partition work"),
        };
        requests.sort_unstable_by_key(|(partition, _)| *partition);
        // Scans prepare once from the predicate and share it across partitions.
        let shared = match work {
            SourceWork::TableScan(_) => Some(PrimaryRead::Scan),
            SourceWork::IndexScan(IndexWork { index, .. }) => {
                Some(PrimaryRead::Index(index.clone()))
            }
            _ => None,
        };
        let scan = shared
            .map(|access| {
                Arc::clone(&reader)
                    .prepare_read(
                        access,
                        Arc::clone(schema),
                        predicate.clone(),
                        limit.map(|l| l as usize),
                    )
                    .map_err(external)
            })
            .transpose()?;
        let reads = requests
            .into_iter()
            .map(|(partition, access)| {
                let requested_keys = match &access {
                    PrimaryRead::MultiGet(keys) => keys.len(),
                    _ => 0,
                };
                Ok(BoundPartitionRead {
                    partition,
                    requested_keys,
                    read: match &scan {
                        Some(scan) => Arc::clone(scan),
                        None => Arc::clone(&reader)
                            .prepare_read(
                                access,
                                Arc::clone(schema),
                                predicate.clone(),
                                limit.map(|l| l as usize),
                            )
                            .map_err(external)?,
                    },
                })
            })
            .collect::<Result<Arc<[_]>>>()?;
        Ok(LocalBinding::Partition {
            manager: manager.clone(),
            placement,
            reads,
        })
    }

    fn into_plan(self) -> Arc<dyn ExecutionPlan> {
        match self.descriptor.work {
            SourceWork::TableScan(_) => Arc::new(TableScanExec(self)),
            SourceWork::MultiGet(_) => Arc::new(MultiGetExec(self)),
            SourceWork::IndexScan(_) => Arc::new(IndexScanExec(self)),
            SourceWork::Node(()) => Arc::new(NodeScanExec(self)),
            SourceWork::Unknown => unreachable!("validated source work"),
        }
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Self> {
        let mut source = self.clone();
        source.descriptor.limit = self
            .descriptor
            .limit
            .into_iter()
            .chain(limit.map(|limit| limit as u64))
            .min();
        if source.descriptor.limit != self.descriptor.limit
            && let Some(LocalBinding::Partition {
                manager, placement, ..
            }) = &self.binding
        {
            source.binding = Some(
                Self::bind_partition(
                    manager,
                    &source.descriptor.table,
                    *placement,
                    &source.descriptor.work,
                    &source.schema,
                    &source.predicate,
                    source.descriptor.limit,
                )
                .ok()?,
            );
        }
        Some(source)
    }

    fn execute(&self, lane: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let Some(binding) = &self.binding else {
            return exec_err!("unbound owner-only storage scan reached the coordinator");
        };
        let (manager, placement, reads) = match binding {
            LocalBinding::Node(reader) => {
                if lane != 0 {
                    return exec_err!("invalid node scan lane {lane}");
                }
                let stream = reader.scan(
                    Arc::clone(&self.schema),
                    &[],
                    context.session_config().batch_size(),
                    self.descriptor.limit.map(|l| l as usize),
                );
                return Ok(Box::pin(LimitStream::new(
                    stream,
                    0,
                    self.descriptor.limit.map(|l| l as usize),
                    BaselineMetrics::new(&self.metrics, lane),
                )));
            }
            LocalBinding::Partition {
                manager,
                placement,
                reads,
            } => (manager.clone(), *placement, reads),
        };
        if lane != 0 {
            return exec_err!("invalid partition source lane {lane}");
        }
        let scope = context
            .session_config()
            .get_extension::<PartitionReadContext>()
            .ok_or_else(|| {
                datafusion::common::exec_datafusion_err!(
                    "partition source has no admitted execution scope"
                )
            })?;
        let Ok(index) = reads.binary_search_by_key(&scope.partition_id(), |read| read.partition)
        else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::empty(),
            )));
        };
        let read = &reads[index];
        let partition = read.partition;
        let prepared = Arc::clone(&read.read);
        let num_keys = read.requested_keys;
        let metrics = BaselineMetrics::new(&self.metrics, lane);
        // A safe source fetch also bounds the batch target. Zero fetches are
        // short-circuited by the stream; readers still require a positive size.
        let batch_size = context.session_config().batch_size().min(
            self.descriptor
                .limit
                .map_or(usize::MAX, |limit| limit as usize)
                .max(1),
        );
        let compute = metrics.elapsed_compute().clone();
        let leaf_metrics = self.metrics.clone();
        let requests = MetricBuilder::new(&self.metrics).counter("storage_requests", lane);
        let requested_keys = MetricBuilder::new(&self.metrics).counter("requested_keys", lane);
        let stream = stream::once(async move {
            if !matches!(
                manager
                    .get_partition_target_node(partition, placement)
                    .map_err(external)?,
                PartitionLocation::Local
            ) {
                return exec_err!("storage ownership changed for partition {partition}");
            }
            requests.add(1);
            requested_keys.add(num_keys);
            prepared
                .execute(
                    scope.as_ref().clone(),
                    batch_size,
                    ReadMetrics::new(compute, &leaf_metrics, lane),
                )
                .map_err(external)
        })
        .try_flatten();
        // Enforce the physical fetch contract even for registered readers that
        // treat their limit as a hint. Production readers also stop materializing
        // at the bound, rather than relying on cancellation after a full batch.
        Ok(Box::pin(LimitStream::new(
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream,
            )),
            0,
            self.descriptor.limit.map(|l| l as usize),
            metrics,
        )))
    }
}

fn source_plan(plan: &dyn ExecutionPlan) -> Option<&SourcePlan> {
    plan.downcast_ref::<TableScanExec>()
        .map(|plan| &plan.0)
        .or_else(|| plan.downcast_ref::<MultiGetExec>().map(|plan| &plan.0))
        .or_else(|| plan.downcast_ref::<IndexScanExec>().map(|plan| &plan.0))
        .or_else(|| plan.downcast_ref::<NodeScanExec>().map(|plan| &plan.0))
}

/// Shared source discovery for placement, stage construction and transport.
pub(super) fn source_owner(plan: &dyn ExecutionPlan) -> Option<GenerationalNodeId> {
    plan.downcast_ref::<PartitionReadExec>()
        .and_then(PartitionReadExec::owner)
        .or_else(|| {
            plan.downcast_ref::<NodeScanExec>()
                .and_then(|source| source.0.descriptor.owner)
        })
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
                let placement = match &source.descriptor.work {
                    SourceWork::TableScan(work) => {
                        work.fmt_summary(f, |read| read.partition)?;
                        Some(work.placement)
                    }
                    SourceWork::MultiGet(work) => {
                        work.fmt_summary(f, |lookup| lookup.partition)?;
                        let keys: usize = work.reads.iter().map(|lookup| lookup.keys.len()).sum();
                        write!(f, ", keys={keys}")?;
                        Some(work.placement)
                    }
                    SourceWork::IndexScan(IndexWork { index, work }) => {
                        write!(f, ", index={index}")?;
                        work.fmt_summary(f, |read| read.partition)?;
                        Some(work.placement)
                    }
                    _ => None,
                };
                if let Some(limit) = source.descriptor.limit {
                    write!(f, ", fetch={limit}")?;
                }
                if matches!(format, DisplayFormatType::Verbose) {
                    if let Some(placement) = placement {
                        write!(f, ", require_leader={}", placement.requires_leader())?;
                    }
                    write!(
                        f,
                        ", projection=[{}], bound={}",
                        ProjectedColumns(&source.schema),
                        source.binding.is_some()
                    )?;
                }
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
            fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
                Some(Arc::new(Self(self.0.with_fetch(limit)?)))
            }
            fn fetch(&self) -> Option<usize> {
                self.0.descriptor.limit.map(|limit| limit as usize)
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
impl_access_exec!(IndexScanExec);
impl_access_exec!(NodeScanExec);

#[derive(Debug, Default)]
pub(super) struct SourceCodec {
    pub manager: Option<SourceRegistry>,
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
        let group = PartitionReadsProto::decode(buf).map_err(|err| external(err.into()))?;
        if let Some(parallelism) = group.parallelism {
            return Ok(Arc::new(PartitionReadExec::new(
                inputs.to_vec(),
                parallelism as usize,
                group.fetch.map(|v| v as usize),
            )?));
        }
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
        if let Some(group) = plan.downcast_ref::<PartitionReadExec>() {
            return group
                .to_proto()
                .encode(buf)
                .map_err(|err| external(err.into()));
        }
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
