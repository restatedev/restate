// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared scanner contracts and DataFusion table providers.

use std::fmt::{self, Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

use restate_types::NodeId;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::partition_planning::plan_partitions_by_location;
use crate::partitioned_scan::{LocationAwareScanExec, PartitionScanExec, RemoteNodeExec};
use crate::remote_fragment::RemoteFragmentExecution;
use crate::remote_query_scanner_manager::PartitionLocation;
use crate::table_util::{find_sort_columns, make_ordering};

/// Opens raw partition data from storage on the current node.
///
/// The remote scanner server also uses this interface after it has validated
/// that the request reached the planned owner.
pub(crate) trait ScanPartition: Send + Sync + Debug + 'static {
    #[allow(clippy::too_many_arguments)]
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream>;
}

/// Planning and execution interface for a partitioned table spanning nodes.
///
/// Physical planning selects and records each partition's location. The
/// resulting local and remote execution-plan nodes use their corresponding
/// entry point, so execution may validate that choice but cannot replace it.
pub(crate) trait DistributedPartitionScanner: Send + Sync + Debug + 'static {
    fn partition_location(&self, partition_id: PartitionId) -> anyhow::Result<PartitionLocation>;

    #[allow(clippy::too_many_arguments)]
    fn scan_local_partition(
        &self,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream>;

    /// Opens a partition on the remote owner selected during physical planning.
    ///
    /// Implementations must use `target_node` as-is rather than resolving
    /// ownership again. The serving node performs the corresponding validation.
    #[allow(clippy::too_many_arguments)]
    fn scan_remote_partition(
        &self,
        target_node: NodeId,
        partition_id: PartitionId,
        range: KeyRange,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        fragment: Option<RemoteFragmentExecution>,
    ) -> anyhow::Result<SendableRecordBatchStream>;
}

/// Produces node-level or global data that is not routed by partition key.
pub trait Scan: Debug + Send + Sync + 'static {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream;
}

pub(crate) type ScannerRef = Arc<dyn Scan>;

/// Stream wrapper that records [`BaselineMetrics`] using [`BaselineMetrics::record_poll`].
pub(crate) struct MeteredStream<S> {
    pub(crate) inner: S,
    pub(crate) baseline_metrics: BaselineMetrics,
}

impl<S> Stream for MeteredStream<S>
where
    S: Stream<Item = datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>>
        + Unpin,
{
    type Item = datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

/// Display helper: comma-separated column names from a schema.
pub(crate) struct ProjectedColumns<'a>(pub(crate) &'a SchemaRef);

impl Display for ProjectedColumns<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for field in self.0.fields() {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}", field.name())?;
            first = false;
        }
        Ok(())
    }
}

/// Builds a physical scan whose branches encode each selected partition's
/// planned local or remote placement.
#[derive(Debug)]
pub(crate) struct PartitionedTableProvider<S> {
    partition_selector: S,
    schema: SchemaRef,
    ordering: Vec<String>,
    partition_scanner: Arc<dyn DistributedPartitionScanner>,
    partition_key_extractor: FirstMatchingPartitionKeyExtractor,
    statistics: Statistics,
}

impl<S> PartitionedTableProvider<S> {
    pub(crate) fn new<T: DistributedPartitionScanner>(
        partition_selector: S,
        schema: SchemaRef,
        ordering: Vec<String>,
        partition_scanner: T,
        partition_key_extractor: FirstMatchingPartitionKeyExtractor,
    ) -> Self {
        let statistics = Statistics::new_unknown(&schema);
        Self {
            partition_selector,
            schema,
            ordering,
            partition_scanner: Arc::new(partition_scanner),
            partition_key_extractor,
            statistics,
        }
    }

    pub(crate) fn with_statistics(self, statistics: Statistics) -> Self {
        Self { statistics, ..self }
    }
}

#[async_trait]
impl<S> TableProvider for PartitionedTableProvider<S>
where
    S: SelectPartitions,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(projection) => SchemaRef::new(self.schema.project(projection)?),
            None => self.schema.clone(),
        };

        // Inexact filter pushdown keeps every filter column in the projected
        // schema, so all filters can be planned against that single schema.
        let filters = filters
            .iter()
            .map(|filter| {
                let filter =
                    datafusion::physical_expr::planner::logical2physical(filter, &projected_schema);
                // Column indices should already be correct, but DataFusion can
                // produce stale indices. Names are unambiguous in these tables.
                datafusion::physical_expr::utils::reassign_expr_columns(filter, &projected_schema)
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let partition_key_selection = self
            .partition_key_extractor
            .try_extract_selection(&filters)
            .map_err(|error| DataFusionError::External(error.into()))?;
        let predicate = datafusion::physical_expr::conjunction_opt(filters);

        let physical_partitions =
            self.partition_selector
                .get_live_partitions()
                .await
                .map_err(DataFusionError::External)?
                .into_iter()
                .flat_map(|(partition_id, partition)| match &partition_key_selection {
                    None => itertools::Either::Left(Some((partition_id, partition)).into_iter()),
                    // Bound fan-out by grouping point reads into one key range per
                    // Restate partition when requested or when the set is large.
                    Some(selection)
                        if selection.fanout == PointReadFanout::PerPartition
                            || selection.keys.len() > 4096 =>
                    {
                        let mut keys = selection.keys.range(partition.key_range).copied();
                        let selected = keys.next().map(|first| {
                            let last = keys.next_back().unwrap_or(first);
                            (
                                partition_id,
                                Partition::new(partition_id, KeyRange::new(first, last)),
                            )
                        });
                        itertools::Either::Left(selected.into_iter())
                    }
                    // Keep smaller point-read sets independent so DataFusion can
                    // distribute them across execution lanes.
                    Some(selection) => itertools::Either::Right(
                        selection.keys.range(partition.key_range).copied().map(
                            move |partition_key| {
                                (
                                    partition_id,
                                    Partition::new(
                                        partition_id,
                                        KeyRange::new(partition_key, partition_key),
                                    ),
                                )
                            },
                        ),
                    ),
                })
                .collect::<Vec<(PartitionId, Partition)>>();

        let located_partitions = plan_partitions_by_location(
            physical_partitions,
            state.config().target_partitions(),
            |partition_id| self.partition_scanner.partition_location(partition_id),
        )
        .map_err(|error| DataFusionError::External(error.into()))?;

        if located_partitions.is_empty() {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let sort_columns = find_sort_columns(&self.ordering, &projected_schema);
        let eq_properties = if sort_columns.is_empty() {
            EquivalenceProperties::new(projected_schema.clone())
        } else {
            EquivalenceProperties::new_with_orderings(
                projected_schema.clone(),
                [make_ordering(sort_columns)],
            )
        };

        let statistics = Arc::new(self.statistics.clone().project(projection));
        let branch_statistics = if located_partitions.len() == 1 {
            Arc::clone(&statistics)
        } else {
            Arc::new(Statistics::new_unknown(&projected_schema))
        };
        let mut inputs = Vec::with_capacity(located_partitions.len());
        for (location, logical_partitions) in located_partitions {
            let plan = PlanProperties::new(
                eq_properties.clone(),
                Partitioning::UnknownPartitioning(logical_partitions.len()),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            .with_scheduling_type(
                datafusion::physical_plan::execution_plan::SchedulingType::Cooperative,
            );
            let scan = PartitionScanExec::new(
                logical_partitions,
                projected_schema.clone(),
                limit,
                predicate.clone(),
                Arc::clone(&self.partition_scanner),
                plan,
                Arc::clone(&branch_statistics),
            );

            inputs.push(match location {
                PartitionLocation::Local => Arc::new(scan) as Arc<dyn ExecutionPlan>,
                PartitionLocation::Remote { node_id } => {
                    Arc::new(RemoteNodeExec::new(node_id, scan)) as Arc<dyn ExecutionPlan>
                }
            });
        }

        match inputs.len() {
            1 => Ok(inputs.pop().expect("one scan input")),
            _ => LocationAwareScanExec::try_new(inputs, statistics),
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        // Inexact pushdown retains a coordinator FilterExec and ensures its
        // columns remain available in the scan projection.
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

/// DataFusion provider for node-level or global data.
#[derive(Debug)]
pub(crate) struct GenericTableProvider {
    schema: SchemaRef,
    scanner: ScannerRef,
    statistics: Statistics,
}

impl GenericTableProvider {
    pub(crate) fn new(schema: SchemaRef, scanner: ScannerRef) -> Self {
        let statistics = Statistics::new_unknown(&schema);
        Self {
            schema,
            scanner,
            statistics,
        }
    }

    pub(crate) fn with_statistics(self, statistics: Statistics) -> Self {
        Self { statistics, ..self }
    }
}

#[async_trait]
impl TableProvider for GenericTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(projection) => SchemaRef::new(self.schema.project(projection)?),
            None => self.schema.clone(),
        };

        Ok(Arc::new(GenericExecutionPlan::new(
            projected_schema,
            filters,
            limit,
            Arc::clone(&self.scanner),
            self.statistics.clone().project(projection),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

#[derive(Debug, Clone)]
struct GenericExecutionPlan {
    projected_schema: SchemaRef,
    scanner: ScannerRef,
    limit: Option<usize>,
    filters: Vec<Expr>,
    plan_properties: Arc<PlanProperties>,
    statistics: Arc<Statistics>,
    metrics: ExecutionPlanMetricsSet,
}

impl GenericExecutionPlan {
    fn new(
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        scanner: ScannerRef,
        statistics: Statistics,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            projected_schema,
            scanner,
            limit,
            filters: filters.to_vec(),
            plan_properties: Arc::new(plan_properties),
            statistics: Arc::new(statistics),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for GenericExecutionPlan {
    fn name(&self) -> &str {
        "GenericExecutionPlan"
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
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
                "GenericExecutionPlan does not support children".to_owned(),
            ));
        }

        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let inner = self.scanner.scan(
            self.projected_schema.clone(),
            &self.filters,
            context.session_config().batch_size(),
            self.limit,
        );
        let metered = MeteredStream {
            inner,
            baseline_metrics,
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            metered,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _: Option<usize>) -> datafusion::error::Result<Arc<Statistics>> {
        Ok(self.statistics.clone())
    }
}

impl DisplayAs for GenericExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "GenericExecutionPlan: scanner={:?}, projection=[{}]",
                    self.scanner,
                    ProjectedColumns(&self.projected_schema),
                )?;
                if !self.filters.is_empty() {
                    write!(f, ", filters=[{}]", ExprList(&self.filters))?;
                }
                if let Some(limit) = self.limit {
                    write!(f, ", limit={limit}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "scanner={:?}", self.scanner)?;
                writeln!(
                    f,
                    "projection=[{}]",
                    ProjectedColumns(&self.projected_schema)
                )?;
                if !self.filters.is_empty() {
                    writeln!(f, "filters=[{}]", ExprList(&self.filters))?;
                }
                if let Some(limit) = self.limit {
                    writeln!(f, "limit={limit}")?;
                }
                Ok(())
            }
        }
    }
}

/// Display helper: comma-separated logical expressions.
struct ExprList<'a>(&'a [Expr]);

impl Display for ExprList<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for expr in self.0 {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{expr}")?;
            first = false;
        }
        Ok(())
    }
}
