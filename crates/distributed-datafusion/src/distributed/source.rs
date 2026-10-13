// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Formatter};
use std::sync::Arc;

use bilrost::{Message as _, OwnedMessage};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result, Statistics, exec_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
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
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;
use restate_util_string::ReString;

use crate::placement::PartitionPlacement;
use crate::remote_query_scanner_manager::{PartitionLocation, RemoteScannerManager};
use crate::statistics::estimate_source_statistics;
use crate::table_providers::{Scan, ScanPartition};
use crate::table_util::{find_sort_columns, make_ordering};
use crate::{decode_schema, encode_schema};

#[derive(Debug, Clone, Copy, bilrost::Message)]
pub(crate) struct ScanRange {
    #[bilrost(1)]
    pub partition: PartitionId,
    #[bilrost(2)]
    pub range: KeyRange,
}

#[derive(Debug, Clone, bilrost::Message)]
pub(crate) struct ScanLane {
    #[bilrost(1)]
    pub ranges: Vec<ScanRange>,
}

#[derive(Debug, Clone, bilrost::Message)]
struct ScanDescriptor {
    #[bilrost(1)]
    table: ReString,
    #[bilrost(2)]
    owner: GenerationalNodeId,
    #[bilrost(3)]
    work: SourceWork,
    #[bilrost(4)]
    ordering: Vec<String>,
    #[bilrost(5)]
    limit: Option<u64>,
}

#[derive(Debug, Clone, bilrost::Message)]
struct PartitionWork {
    #[bilrost(1)]
    placement: PartitionPlacement,
    #[bilrost(2)]
    lanes: Vec<ScanLane>,
}

#[derive(Debug, Clone, bilrost::Message, bilrost::Oneof)]
enum SourceWork {
    Unknown,
    #[bilrost(1)]
    Partition(PartitionWork),
    #[bilrost(2)]
    Node(()),
}

#[derive(Debug)]
enum LocalBinding {
    Partition(RemoteScannerManager, Arc<dyn ScanPartition>),
    Node(Arc<dyn Scan>),
}

/// An unbound coordinator descriptor becomes a strictly local scanner on decode.
/// Neither execution nor decoding may forward a storage read to another node.
#[derive(Debug)]
pub(crate) struct SourceExec {
    descriptor: ScanDescriptor,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    properties: Arc<PlanProperties>,
    binding: Option<LocalBinding>,
    metrics: ExecutionPlanMetricsSet,
    // Coordinator planning hints. Serialized tasks already have their join order
    // fixed, so workers can decode the existing wire format with unknown estimates.
    statistics: Arc<Statistics>,
}

impl SourceExec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_scan(
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut placements = HashMap::new();
        let mut owners: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for (partition, range) in ranges {
            // Snapshot placement once per selected partition and contract. A lane
            // count never caps the number of owners or drops logical work.
            let owner = match placements.entry((partition, placement)) {
                std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
                std::collections::hash_map::Entry::Vacant(entry) => *entry.insert(
                    manager
                        .partition_owner(partition, placement)
                        .map_err(|err| DataFusionError::External(err.into()))?,
                ),
            };
            owners
                .entry(owner)
                .or_default()
                .push(ScanRange { partition, range });
        }
        if owners.is_empty() {
            return plan_err!("storage stage has no ranges");
        }
        let total_ranges = owners.values().map(Vec::len).sum::<usize>();
        let plans = owners
            .into_iter()
            .map(|(owner, mut ranges)| {
                let statistics = estimate_source_statistics(
                    &statistics,
                    ranges.len() as f64 / total_ranges as f64,
                );
                ranges.sort_unstable_by_key(|range| range.range.start());
                let mut lanes =
                    vec![ScanLane { ranges: vec![] }; target_partitions.max(1).min(ranges.len())];
                let count = lanes.len();
                for (i, range) in ranges.into_iter().enumerate() {
                    lanes[i % count].ranges.push(range);
                }
                Ok(Arc::new(Self::new(
                    ScanDescriptor {
                        table: table.clone(),
                        owner,
                        work: SourceWork::Partition(PartitionWork { placement, lanes }),
                        ordering: ordering.clone(),
                        limit: limit.map(|l| l as u64),
                    },
                    Arc::clone(&schema),
                    predicate.clone(),
                    None,
                    Some(statistics),
                )?) as Arc<dyn ExecutionPlan>)
            })
            .collect::<Result<Vec<_>>>()?;
        if plans.len() == 1 {
            Ok(Arc::clone(&plans[0]))
        } else {
            UnionExec::try_new(plans)
        }
    }

    pub(crate) fn for_node(
        table: ReString,
        owner: GenerationalNodeId,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Result<Self> {
        Self::new(
            ScanDescriptor {
                table,
                owner,
                work: SourceWork::Node(()),
                ordering: vec![],
                limit: limit.map(|l| l as u64),
            },
            schema,
            None,
            None,
            None,
        )
    }

    fn new(
        descriptor: ScanDescriptor,
        schema: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        binding: Option<LocalBinding>,
        statistics: Option<Arc<Statistics>>,
    ) -> Result<Self> {
        let lanes = match &descriptor.work {
            SourceWork::Partition(work) => {
                if work.lanes.is_empty() || work.lanes.iter().any(|lane| lane.ranges.is_empty()) {
                    return plan_err!("storage stage has an empty execution lane");
                }
                work.lanes.len()
            }
            SourceWork::Node(()) => 1,
            SourceWork::Unknown => return plan_err!("unknown query source scope"),
        };
        let statistics = statistics.unwrap_or_else(|| Arc::new(Statistics::new_unknown(&schema)));
        if statistics.column_statistics.len() != schema.fields().len() {
            return plan_err!("source statistics do not match the projected schema");
        }
        let ordering = make_ordering(find_sort_columns(&descriptor.ordering, &schema));
        let equivalence = EquivalenceProperties::new_with_orderings(schema, [ordering]);
        let properties = Arc::new(PlanProperties::new(
            equivalence,
            Partitioning::UnknownPartitioning(lanes),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            descriptor,
            predicate,
            properties,
            binding,
            metrics: ExecutionPlanMetricsSet::new(),
            statistics,
        })
    }

    pub(crate) fn owner(&self) -> GenerationalNodeId {
        self.descriptor.owner
    }
}

impl DisplayAs for SourceExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{}: table={}, owner={}, work={:?}, bound={}",
            self.name(),
            self.descriptor.table,
            self.owner(),
            self.descriptor.work,
            self.binding.is_some()
        )
    }
}

impl ExecutionPlan for SourceExec {
    fn name(&self) -> &str {
        match self.descriptor.work {
            SourceWork::Node(()) => "NodeScanExec",
            _ => "StorageScanExec",
        }
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("storage scans cannot have children");
        }
        Ok(self)
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        datafusion::physical_plan::apply_expression_roots(self.predicate.iter(), f)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(binding) = &self.binding else {
            return exec_err!("unbound owner-only storage scan reached the coordinator");
        };
        let (manager, scanner, work) = match (binding, &self.descriptor.work) {
            (LocalBinding::Node(scanner), SourceWork::Node(())) => {
                if partition != 0 {
                    return exec_err!("invalid node scan lane {partition}");
                }
                return Ok(scanner.scan(
                    self.schema(),
                    &[],
                    context.session_config().batch_size(),
                    self.descriptor.limit.map(|l| l as usize),
                ));
            }
            (LocalBinding::Partition(manager, scanner), SourceWork::Partition(work)) => {
                (manager, scanner, work)
            }
            _ => return exec_err!("query source binding has the wrong scope"),
        };
        let Some(lane) = work.lanes.get(partition) else {
            return exec_err!("invalid storage lane {partition}");
        };
        for range in &lane.ranges {
            if !matches!(
                manager
                    .get_partition_target_node(range.partition, work.placement)
                    .map_err(|err| DataFusionError::External(err.into()))?,
                PartitionLocation::Local
            ) {
                return exec_err!(
                    "storage ownership changed for partition {}",
                    range.partition
                );
            }
        }
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        let scanner = Arc::clone(scanner);
        let schema = self.schema();
        let predicate = self.predicate.clone();
        let limit = self.descriptor.limit.map(|limit| limit as usize);
        let batch_size = context.session_config().batch_size();
        let compute = metrics.elapsed_compute().clone();
        let stream = stream::iter(lane.ranges.clone())
            .map(move |range| {
                scanner
                    .scan_partition(
                        range.partition,
                        range.range,
                        Arc::clone(&schema),
                        predicate.clone(),
                        batch_size,
                        limit,
                        compute.clone(),
                    )
                    .map_err(|err| DataFusionError::External(err.into()))
            })
            .try_flatten()
            .inspect(move |batch| {
                if let Ok(batch) = batch {
                    metrics.record_output(batch.num_rows());
                }
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn statistics_from_inputs(
        &self,
        _: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        if let Some(partition) = args.partition() {
            let lanes = self.properties.partitioning.partition_count();
            if partition >= lanes {
                return plan_err!("invalid source statistics partition {partition}");
            }
            Ok(estimate_source_statistics(
                &self.statistics,
                1.0 / lanes as f64,
            ))
        } else {
            Ok(Arc::clone(&self.statistics))
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct SourceCodec {
    pub manager: Option<RemoteScannerManager>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SourceProto {
    // Separate envelope tags from the library's extension nodes.
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
            return plan_err!("storage scans cannot have children");
        }
        let proto =
            SourceProto::decode(buf).map_err(|err| DataFusionError::External(Box::new(err)))?;
        let descriptor = ScanDescriptor::decode(proto.descriptor.as_slice())
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        let schema = Arc::new(
            decode_schema(&proto.schema).map_err(|err| DataFusionError::External(err.into()))?,
        );
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
        let binding = if let Some(manager) = &self.manager {
            if descriptor.owner != manager.node_id() {
                return exec_err!("storage task delivered to the wrong owner");
            }
            Some(match &descriptor.work {
                SourceWork::Node(()) => {
                    let Some(scanner) = manager.local_node_scanner(&descriptor.table) else {
                        return exec_err!(
                            "local node source '{}' is unavailable",
                            descriptor.table
                        );
                    };
                    LocalBinding::Node(scanner)
                }
                SourceWork::Partition(work) => {
                    let Some(scanner) = manager.local_partition_scanner(&descriptor.table) else {
                        return exec_err!(
                            "local query source '{}' is unavailable",
                            descriptor.table
                        );
                    };
                    if manager.local_node_scanner(&descriptor.table).is_some()
                        || scanner.partition_source() != work.placement.source
                    {
                        return exec_err!(
                            "local query source has incompatible placement requirements"
                        );
                    }
                    LocalBinding::Partition(manager.clone(), scanner)
                }
                SourceWork::Unknown => return exec_err!("unknown query source scope"),
            })
        } else {
            None
        };
        Ok(Arc::new(SourceExec::new(
            descriptor, schema, predicate, binding, None,
        )?))
    }

    fn try_encode(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(source) = plan.downcast_ref::<SourceExec>() else {
            return plan_err!("unsupported Restate query source");
        };
        let proto = SourceProto {
            descriptor: source.descriptor.encode_to_vec(),
            schema: encode_schema(&source.schema()),
            predicate: source
                .predicate
                .as_ref()
                .map(|expr| converter.physical_expr_to_proto(expr, self))
                .transpose()?,
        };
        proto
            .encode(buf)
            .map_err(|err| DataFusionError::External(Box::new(err)))
    }
}
