// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Physical execution nodes for partition-routed table scans.
//!
//! `PartitionedTableProvider::scan` creates a [`PartitionScanExec`] for every
//! location group produced by `partition_planning`. Local groups remain plain
//! scans; remote groups are wrapped in an opaque [`RemoteNodeExec`]. When a
//! table spans locations, [`LocationAwareScanExec`] joins those disjoint
//! branches while retaining statistics for the original table scan.
//!
//! The optimizer-facing boundary is intentional. Generic DataFusion rules see
//! the table scan as a leaf and cannot accidentally move arbitrary operators
//! across the network boundary. Restate's fragment rules may attach one
//! validated `RemoteFragment`: it is bound directly above local scans and sent
//! by remote nodes to the planned owner. At execution time the shared scan
//! helper iterates each logical lane's physical partitions sequentially, but
//! the node type—not the scanner—decides whether the local or remote entry
//! point is used.
//!
//! # Physical planning order
//!
//! 1. While converting a logical table scan to a physical plan, DataFusion
//!    calls `PartitionedTableProvider::scan`.
//! 2. The provider applies partition-key pruning, resolves ownership, and uses
//!    `plan_partitions_by_location` to group the selected partitions into
//!    execution lanes that never cross a node boundary.
//! 3. The provider creates one [`PartitionScanExec`] per location group. A
//!    local scan is inserted directly; a remote scan is wrapped in a
//!    [`RemoteNodeExec`] for the selected owner. No groups produce an
//!    `EmptyExec`, one group is returned directly, and multiple groups are
//!    collected under [`LocationAwareScanExec`].
//! 4. DataFusion builds the remaining physical operators above that scan shape
//!    and begins physical optimization.
//! 5. Restate's `PartialAggregationPushdown` runs immediately before
//!    DataFusion's final `FilterPushdown(Post)` pass. When eligible, it turns
//!    the partial aggregate and its stable input operators into a
//!    `RemoteFragment`, attaches the fragment to every placement branch, and
//!    leaves a partial-reduce aggregate at the coordinator.
//! 6. `FilterPushdown(Post)` moves remaining predicates—including stateful
//!    dynamic TopK predicates—as close to each [`PartitionScanExec`] as
//!    DataFusion permits.
//! 7. Restate's `ScanFragmentPushdown` then turns eligible stable
//!    filter/projection chains into a `RemoteFragment` and applies it uniformly
//!    to all placement branches. Local branches bind the fragment as ordinary
//!    DataFusion operators; each [`RemoteNodeExec`] stores it for transmission
//!    to the partition owner during execution.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::filter_pushdown::{
    FilterDescription, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{self, StreamExt, TryStreamExt};

use restate_types::NodeId;

use crate::partition_planning::LogicalPartition;
use crate::remote_fragment::{RemoteFragment, RemoteFragmentExecution};
use crate::table_providers::{DistributedPartitionScanner, MeteredStream, ProjectedColumns};

/// Represents one logical table scan after its partitions have been assigned
/// to their execution locations.
///
/// Each child scans a disjoint subset of the table: a [`PartitionScanExec`]
/// reads locally owned partitions, while a [`RemoteNodeExec`] reads the
/// partitions assigned to one remote owner. Execution is union-like, but this
/// dedicated node preserves the fact that all children belong to the same
/// table scan. Fragment optimizers use that boundary to verify that a fragment
/// can be applied uniformly, then bind it to every local and remote branch.
/// An ordinary [`UnionExec`] would be indistinguishable from a SQL union and
/// would not provide that guarantee.
///
/// Whole-scan statistics are retained here rather than summed from the
/// children. Each child has only a placement-specific subset and cannot derive
/// an accurate share of the table's static estimate.
#[derive(Debug, Clone)]
pub(crate) struct LocationAwareScanExec {
    union: Arc<dyn ExecutionPlan>,
    statistics: Arc<Statistics>,
}

impl LocationAwareScanExec {
    pub(super) fn try_new(
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        statistics: Arc<Statistics>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            union: UnionExec::try_new(inputs)?,
            statistics,
        }))
    }

    /// Returns whether every placement branch is an unmodified partition scan
    /// and at least one branch crosses a remote boundary.
    ///
    /// A fragment cannot be inserted after a scan-level limit because doing so
    /// independently in each placement branch would change query semantics.
    pub(crate) fn supports_fragment_pushdown(&self) -> bool {
        let mut has_remote_branch = false;
        let supported = self.union.children().into_iter().all(|input| {
            if let Some(scan) = input.downcast_ref::<PartitionScanExec>() {
                return scan.limit.is_none();
            }
            if let Some(remote) = input.downcast_ref::<RemoteNodeExec>() {
                has_remote_branch = true;
                return remote.can_accept_fragment();
            }
            false
        });
        supported && has_remote_branch
    }

    /// Applies the same single-input fragment to every placement branch.
    ///
    /// Local branches bind the fragment directly to their scan. Remote
    /// branches retain an opaque [`RemoteNodeExec`] and carry the serialized
    /// fragment to the selected owner at execution time.
    pub(crate) fn with_fragment(
        &self,
        fragment: Arc<RemoteFragment>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let inputs = self
            .union
            .children()
            .into_iter()
            .map(|input| {
                if let Some(local) = input.downcast_ref::<PartitionScanExec>() {
                    fragment.bind_input(Arc::new(local.clone()))
                } else if let Some(remote) = input.downcast_ref::<RemoteNodeExec>() {
                    remote.with_fragment(Arc::clone(&fragment))
                } else {
                    Err(DataFusionError::Internal(format!(
                        "unsupported location-aware scan branch {}",
                        input.name()
                    )))
                }
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        Self::try_new(
            inputs,
            Arc::new(Statistics::new_unknown(&fragment.output_schema())),
        )
    }
}

impl ExecutionPlan for LocationAwareScanExec {
    fn name(&self) -> &str {
        "LocationAwareScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.union.properties()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.union.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.union.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.union.children()
    }

    /// Routes optimizer-produced predicates to every placement branch while
    /// retaining this node as the statistics-preserving union boundary.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::common::Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Self::try_new(children, self.statistics.clone())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        self.union.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.union.metrics()
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Arc<Statistics>> {
        match partition {
            Some(partition) => self.union.partition_statistics(Some(partition)),
            None => Ok(self.statistics.clone()),
        }
    }
}

impl DisplayAs for LocationAwareScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "LocationAwareScanExec")
    }
}

/// Scans one location-specific set of logical partitions.
///
/// The predicate is a single expression tree: provider filters are present at
/// construction, while later optimizer passes may conjoin mutable expressions
/// whose generation changes during execution.
#[derive(Debug, Clone)]
pub(crate) struct PartitionScanExec {
    logical_partitions: Vec<LogicalPartition>,
    projected_schema: SchemaRef,
    limit: Option<usize>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    scanner: Arc<dyn DistributedPartitionScanner>,
    plan: Arc<PlanProperties>,
    statistics: Arc<Statistics>,
    metrics: ExecutionPlanMetricsSet,
}

impl PartitionScanExec {
    pub(super) fn new(
        logical_partitions: Vec<LogicalPartition>,
        projected_schema: SchemaRef,
        limit: Option<usize>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        scanner: Arc<dyn DistributedPartitionScanner>,
        plan: PlanProperties,
        statistics: Arc<Statistics>,
    ) -> Self {
        Self {
            logical_partitions,
            projected_schema,
            limit,
            predicate,
            scanner,
            plan: Arc::new(plan),
            statistics,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for PartitionScanExec {
    fn name(&self) -> &str {
        "PartitionScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        new_children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !new_children.is_empty() {
            return Err(DataFusionError::Internal(
                "PartitionScanExec does not support children".to_owned(),
            ));
        }

        Ok(self)
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::common::Result<Arc<Statistics>> {
        Ok(self.statistics.clone())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        self.execute_scan(PartitionScanTarget::Local, partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn handle_child_pushdown_result(
        &self,
        phase: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase,
        child_pushdown_result: datafusion::physical_plan::filter_pushdown::ChildPushdownResult,
        _config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<
        datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation<
            Arc<dyn ExecutionPlan>,
        >,
    > {
        if !matches!(phase, FilterPushdownPhase::Post) {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }

        if child_pushdown_result.parent_filters.is_empty() {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }

        // As in the planning-time case above, the predicate *should* have the correct column
        // indices, but bugs in DataFusion can create mixups.
        let mut filters: Vec<_> = child_pushdown_result
            .parent_filters
            .iter()
            .map(|f| {
                datafusion::physical_expr::utils::reassign_expr_columns(
                    f.filter.clone(),
                    &self.projected_schema,
                )
            })
            .collect::<Result<_, _>>()?;

        if let Some(predicate) = &self.predicate {
            filters.push(Arc::clone(predicate));
        }

        let predicate = datafusion::physical_expr::conjunction(filters);
        let mut plan = self.clone();
        plan.predicate = Some(predicate);

        Ok(FilterPushdownPropagation {
            // we report all filters as unsupported as we don't guarantee to apply them exactly as there can be a delay before new filters are used
            filters: child_pushdown_result
                .parent_filters
                .iter()
                .map(|_| PushedDown::No)
                .collect(),
            updated_node: Some(Arc::new(plan)),
        })
    }
}

/// Execution backend selected by the physical-plan node.
///
/// Keeping the fragment in the remote variant prevents local scans from
/// accidentally entering the fragment-negotiation path.
#[derive(Clone)]
enum PartitionScanTarget {
    Local,
    Remote {
        target_node: NodeId,
        fragment: Option<RemoteFragmentExecution>,
    },
}

impl PartitionScanExec {
    /// Builds one pull-driven stream for a local or remote physical-plan leaf.
    /// Placement has already been encoded by the calling execution-plan node;
    /// this helper only shares partition iteration and metrics plumbing.
    fn execute_scan(
        &self,
        target: PartitionScanTarget,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let output_schema = match &target {
            PartitionScanTarget::Local => self.projected_schema.clone(),
            PartitionScanTarget::Remote {
                fragment: Some(fragment),
                ..
            } => fragment.output_schema(),
            PartitionScanTarget::Remote { fragment: None, .. } => self.projected_schema.clone(),
        };

        let physical_partitions = self
            .logical_partitions
            .get(partition)
            .expect("partition exists")
            .physical_partitions
            .to_vec();

        let sequential_scanners_stream = stream::iter(physical_partitions)
            .map({
                let scanner = Arc::clone(&self.scanner);
                let schema = self.projected_schema.clone();
                let limit = self.limit;
                let predicate = self.predicate.clone();
                let batch_size = context.session_config().batch_size();
                let elapsed_compute = baseline_metrics.elapsed_compute().clone();
                move |(partition_id, partition)| {
                    match &target {
                        PartitionScanTarget::Local => scanner.scan_local_partition(
                            partition_id,
                            partition.key_range,
                            schema.clone(),
                            predicate.clone(),
                            batch_size,
                            limit,
                            elapsed_compute.clone(),
                        ),
                        PartitionScanTarget::Remote {
                            target_node,
                            fragment,
                        } => scanner.scan_remote_partition(
                            *target_node,
                            partition_id,
                            partition.key_range,
                            schema.clone(),
                            predicate.clone(),
                            batch_size,
                            limit,
                            fragment.clone(),
                        ),
                    }
                    .map_err(|error| DataFusionError::External(error.into()))
                }
            })
            .try_flatten();

        let metered = MeteredStream {
            inner: sequential_scanners_stream,
            baseline_metrics,
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            metered,
        )))
    }
}

/// An explicit physical-plan boundary for work assigned to another node.
///
/// The scan is deliberately opaque to DataFusion's default optimizer rules: an
/// operator must only appear inside this boundary once `RemoteNodeExec` knows how
/// to serialize and execute it remotely. Custom rules can match and enrich this
/// node without rediscovering placement.
#[derive(Debug, Clone)]
pub(crate) struct RemoteNodeExec {
    target_node: NodeId,
    scan: PartitionScanExec,
    fragment: Option<Arc<RemoteFragment>>,
    plan: Arc<PlanProperties>,
}

impl RemoteNodeExec {
    pub(super) fn new(target_node: NodeId, scan: PartitionScanExec) -> Self {
        let plan = scan.properties().clone();
        Self {
            target_node,
            scan,
            fragment: None,
            plan,
        }
    }

    /// A fragment must run directly over the unlimited raw scan, and attaching
    /// one must never overwrite work selected by an earlier optimizer pass.
    pub(crate) fn can_accept_fragment(&self) -> bool {
        self.scan.limit.is_none() && self.fragment.is_none()
    }

    /// Returns a new remote boundary that applies `fragment` at the partition
    /// owner. The fragment is bound to the private scan to derive its output
    /// partitioning and execution behavior, but ordering is discarded because
    /// the worker binds the same fragment to an unordered stream boundary.
    pub(crate) fn with_fragment(
        &self,
        fragment: Arc<RemoteFragment>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if !self.can_accept_fragment() {
            return Err(DataFusionError::Internal(
                "RemoteNodeExec cannot replace an existing fragment or push one below a scan limit"
                    .to_owned(),
            ));
        }
        let bound = fragment.bind_input(Arc::new(self.scan.clone()))?;
        let mut eq_properties = bound.properties().eq_properties.clone();
        eq_properties.clear_orderings();
        let plan = Arc::new(
            bound
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(eq_properties),
        );
        Ok(Arc::new(Self {
            target_node: self.target_node,
            scan: self.scan.clone(),
            fragment: Some(fragment),
            plan,
        }))
    }
}

impl ExecutionPlan for RemoteNodeExec {
    fn name(&self) -> &str {
        "RemoteNodeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        new_children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !new_children.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "RemoteNodeExec does not support children, got {}",
                new_children.len()
            )));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let fragment = self.fragment.clone().map(|fragment| {
            RemoteFragmentExecution::new(fragment, Arc::clone(&context))
                .with_metrics(&self.scan.metrics, partition)
        });
        self.scan.execute_scan(
            PartitionScanTarget::Remote {
                target_node: self.target_node,
                fragment,
            },
            partition,
            context,
        )
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::common::Result<Arc<Statistics>> {
        if self.fragment.is_some() {
            Ok(Arc::new(Statistics::new_unknown(&self.schema())))
        } else {
            self.scan.partition_statistics(partition)
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.scan.metrics()
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: datafusion::physical_plan::filter_pushdown::ChildPushdownResult,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::common::Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if self.fragment.is_some() {
            return Ok(FilterPushdownPropagation {
                filters: child_pushdown_result
                    .parent_filters
                    .iter()
                    .map(|_| PushedDown::No)
                    .collect(),
                updated_node: None,
            });
        }
        let propagation =
            self.scan
                .handle_child_pushdown_result(phase, child_pushdown_result, config)?;
        let updated_node = propagation.updated_node.map(|updated_scan| {
            let updated_scan = updated_scan
                .downcast_ref::<PartitionScanExec>()
                .expect("PartitionScanExec updates preserve their type")
                .clone();
            Arc::new(Self::new(self.target_node, updated_scan)) as Arc<dyn ExecutionPlan>
        });

        Ok(FilterPushdownPropagation {
            filters: propagation.filters,
            updated_node,
        })
    }
}

impl DisplayAs for RemoteNodeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RemoteNodeExec: target_node={}", self.target_node)?;
                if let Some(fragment) = &self.fragment {
                    write!(f, ", fragment=[")?;
                    fragment.fmt_pipeline(f)?;
                    write!(f, "]")?;
                }
                write!(f, ", scan=[")?;
                self.scan.fmt_as(DisplayFormatType::Default, f)?;
                write!(f, "]")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "target_node={}", self.target_node)?;
                if let Some(fragment) = &self.fragment {
                    write!(f, "fragment=[")?;
                    fragment.fmt_pipeline(f)?;
                    writeln!(f, "]")?;
                }
                write!(f, "scan=[")?;
                self.scan.fmt_as(DisplayFormatType::Default, f)?;
                writeln!(f, "]")
            }
        }
    }
}

impl DisplayAs for PartitionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PartitionScanExec: scanner={:?}, partitions={}, projection=[{}]",
                    self.scanner,
                    self.logical_partitions.len(),
                    ProjectedColumns(&self.projected_schema),
                )?;
                if let Some(predicate) = &self.predicate {
                    write!(f, ", predicate={predicate}")?;
                }
                if let Some(limit) = self.limit {
                    write!(f, ", limit={limit}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "scanner={:?}", self.scanner)?;
                writeln!(f, "partitions={}", self.logical_partitions.len())?;
                writeln!(
                    f,
                    "projection=[{}]",
                    ProjectedColumns(&self.projected_schema)
                )?;
                if let Some(predicate) = &self.predicate {
                    writeln!(f, "predicate={predicate}")?;
                }
                if let Some(limit) = self.limit {
                    writeln!(f, "limit={limit}")?;
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::TableProvider;
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
    use datafusion::physical_plan::expressions::Column;
    use datafusion::physical_plan::metrics::Time;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use restate_types::GenerationalNodeId;
    use restate_types::errors::GenericError;
    use restate_types::identifiers::PartitionId;
    use restate_types::partition_table::Partition;
    use restate_types::sharding::KeyRange;

    use super::*;
    use crate::context::SelectPartitions;
    use crate::filter::FirstMatchingPartitionKeyExtractor;
    use crate::partial_aggregation::PartialAggregationPushdown;
    use crate::remote_fragment::FragmentLeafExec;
    use crate::remote_query_scanner_manager::PartitionLocation;
    use crate::scan_fragment::ScanFragmentPushdown;
    use crate::table_providers::PartitionedTableProvider;

    fn physical_partition(id: u16) -> (PartitionId, Partition) {
        let partition_id = PartitionId::new_unchecked(id);
        (partition_id, Partition::new(partition_id, KeyRange::FULL))
    }

    #[derive(Debug, Clone)]
    struct TestPartitionSelector;

    #[async_trait]
    impl SelectPartitions for TestPartitionSelector {
        async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
            Ok((0..4).map(physical_partition).collect())
        }
    }

    #[derive(Debug)]
    struct TestPartitionScanner;

    impl DistributedPartitionScanner for TestPartitionScanner {
        fn partition_location(
            &self,
            partition_id: PartitionId,
        ) -> anyhow::Result<PartitionLocation> {
            if partition_id == PartitionId::MIN {
                Ok(PartitionLocation::Local)
            } else {
                Ok(PartitionLocation::Remote {
                    node_id: GenerationalNodeId::new(2, 1).into(),
                })
            }
        }

        fn scan_local_partition(
            &self,
            _partition_id: PartitionId,
            _range: KeyRange,
            _projection: SchemaRef,
            _predicate: Option<Arc<dyn PhysicalExpr>>,
            _batch_size: usize,
            _limit: Option<usize>,
            _elapsed_compute: Time,
        ) -> anyhow::Result<SendableRecordBatchStream> {
            unreachable!("plan-shape test does not execute the scan")
        }

        fn scan_remote_partition(
            &self,
            _target_node: NodeId,
            _partition_id: PartitionId,
            _range: KeyRange,
            _projection: SchemaRef,
            _predicate: Option<Arc<dyn PhysicalExpr>>,
            _batch_size: usize,
            _limit: Option<usize>,
            _fragment: Option<RemoteFragmentExecution>,
        ) -> anyhow::Result<SendableRecordBatchStream> {
            unreachable!("plan-shape test does not execute the scan")
        }
    }

    #[tokio::test]
    async fn physical_plan_has_an_explicit_remote_node() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let provider = PartitionedTableProvider::new(
            TestPartitionSelector,
            schema.clone(),
            vec!["value".to_owned()],
            TestPartitionScanner,
            FirstMatchingPartitionKeyExtractor::default(),
        )
        .with_statistics(Statistics::new_unknown(&schema).with_num_rows(Precision::Inexact(1024)));
        let context = SessionContext::new();

        let plan = provider
            .scan(&context.state(), None, &[], None)
            .await
            .expect("physical plan should build");
        let scan = plan
            .downcast_ref::<LocationAwareScanExec>()
            .expect("local and remote placements should form one scan");
        assert_eq!(scan.children().len(), 2);
        assert_eq!(
            scan.partition_statistics(None)
                .expect("statistics should be available")
                .num_rows,
            Precision::Inexact(1024)
        );

        let remote = scan
            .children()
            .into_iter()
            .find_map(|child| child.downcast_ref::<RemoteNodeExec>())
            .expect("remote placement should have an explicit boundary");
        assert_eq!(
            remote.target_node,
            NodeId::from(GenerationalNodeId::new(2, 1))
        );
        assert!(remote.children().is_empty());
        assert_eq!(remote.scan.name(), "PartitionScanExec");
        assert!(remote.scan.predicate.is_none());
        assert!(remote.properties().output_ordering().is_some());

        let fragment = Arc::new(
            RemoteFragment::try_new(Arc::new(FragmentLeafExec::new(remote.schema())))
                .expect("identity fragment"),
        );
        let remote = remote
            .with_fragment(Arc::clone(&fragment))
            .expect("first fragment should attach");
        let remote = remote
            .downcast_ref::<RemoteNodeExec>()
            .expect("fragment preserves the remote boundary");
        assert!(remote.properties().output_ordering().is_none());
        assert!(!remote.can_accept_fragment());
        assert!(remote.with_fragment(fragment).is_err());
    }

    #[tokio::test]
    async fn topk_dynamic_filter_reaches_the_remote_scan() {
        let provider = PartitionedTableProvider::new(
            TestPartitionSelector,
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            Vec::new(),
            TestPartitionScanner,
            FirstMatchingPartitionKeyExtractor::default(),
        );
        let context = SessionContext::new();
        let scan = provider
            .scan(&context.state(), None, &[], None)
            .await
            .expect("physical plan should build");
        let sort = Arc::new(
            SortExec::new(
                [PhysicalSortExpr::new_default(Arc::new(Column::new(
                    "value", 0,
                )))]
                .into(),
                scan,
            )
            .with_fetch(Some(10)),
        );
        let mut config = ConfigOptions::new();
        config.optimizer.enable_topk_dynamic_filter_pushdown = true;

        let optimized = PartialAggregationPushdown
            .optimize(sort, &config)
            .expect("partial aggregation rule should preserve the TopK plan");
        let optimized = FilterPushdown::new_post_optimization()
            .optimize(optimized, &config)
            .expect("TopK filter pushdown should succeed after partial aggregation pushdown");
        let optimized = ScanFragmentPushdown
            .optimize(optimized, &config)
            .expect("scan fragment pushdown should preserve the TopK plan");
        let display = datafusion::physical_plan::displayable(optimized.as_ref())
            .indent(true)
            .to_string();
        assert!(
            display.contains("RemoteNodeExec: target_node=N2:1"),
            "{display}"
        );
        assert!(display.contains("predicate=DynamicFilter"), "{display}");
        let scan = optimized.children()[0]
            .downcast_ref::<LocationAwareScanExec>()
            .expect("sort input should remain a location-aware scan");
        let remote = scan
            .children()
            .into_iter()
            .find_map(|child| child.downcast_ref::<RemoteNodeExec>())
            .expect("remote boundary should remain explicit");
        let local = scan
            .children()
            .into_iter()
            .find_map(|child| child.downcast_ref::<PartitionScanExec>())
            .expect("local scan should remain explicit");

        for predicate in [&remote.scan.predicate, &local.predicate] {
            let predicate = predicate
                .as_ref()
                .expect("TopK should push a scan predicate to every placement");
            assert_ne!(
                datafusion::physical_expr_common::physical_expr::snapshot_generation(predicate),
                0,
                "TopK scan predicate should remain dynamically updateable"
            );
        }
    }
}
