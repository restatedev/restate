// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::any::Any;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::RangeInclusive;
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
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{self, Stream, StreamExt, TryStreamExt};

use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::partition_table::Partition;

use crate::context::SelectPartitions;
use crate::partition_filter::{FirstMatchingPartitionKeyExtractor, PartitionKeyExtractor};
use crate::table_util::{find_sort_columns, make_ordering};

pub trait ScanPartition: Send + Sync + Debug + 'static {
    #[allow(clippy::too_many_arguments)]
    fn scan_partition(
        &self,
        partition_id: PartitionId,
        range: RangeInclusive<PartitionKey>,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        batch_size: usize,
        limit: Option<usize>,
        elapsed_compute: Time,
    ) -> anyhow::Result<SendableRecordBatchStream>;
}

#[derive(Debug)]
pub(crate) struct PartitionedTableProvider<T, S> {
    partition_selector: S,
    schema: SchemaRef,
    ordering: Vec<String>,
    partition_scanner: T,
    partition_key_extractor: FirstMatchingPartitionKeyExtractor,
    statistics: Statistics,
}

impl<T, S> PartitionedTableProvider<T, S> {
    pub(crate) fn new(
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
            partition_scanner,
            partition_key_extractor,
            statistics,
        }
    }

    pub(crate) fn with_statistics(self, statistics: Statistics) -> Self {
        Self { statistics, ..self }
    }
}

#[derive(Debug, Clone)]
struct LogicalPartition {
    physical_partitions: Vec<(PartitionId, Partition)>,
}

impl LogicalPartition {
    fn new(physical_partitions: Vec<(PartitionId, Partition)>) -> Self {
        Self {
            physical_partitions,
        }
    }
}

fn physical_partitions_to_logical(
    physical_partitions: Vec<(PartitionId, Partition)>,
    target_partitions: usize,
) -> Vec<LogicalPartition> {
    if physical_partitions.len() <= target_partitions {
        // don't bother to coalesce physical partitions together, just
        // use them as-is.
        return physical_partitions
            .into_iter()
            .map(|p| LogicalPartition::new(vec![p]))
            .collect();
    }

    let mut logical_partitions = vec![LogicalPartition::new(Default::default()); target_partitions];
    let mut logical_index = 0;

    for partition in physical_partitions {
        logical_partitions[logical_index]
            .physical_partitions
            .push(partition);
        logical_index = (logical_index + 1) % target_partitions;
    }

    logical_partitions
}

#[async_trait]
impl<T, S> TableProvider for PartitionedTableProvider<T, S>
where
    T: ScanPartition + Clone,
    S: SelectPartitions,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

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
            Some(p) => SchemaRef::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        // as we report our filter pushdown as inexact, all columns needed for the filters will be in the projection
        let filters: Vec<_> = filters
            .iter()
            .map(|p| {
                let p = datafusion::physical_expr::planner::logical2physical(p, &projected_schema);
                // The predicate *should* have the correct column indices but bugs in datafusion can create mixups.
                // Most datafusion table providers seem to use reassign_expr_columns so they are tolerant to this.
                // The column indices are not important as all columns should refer to fields in this table
                // and we don't have any duplicate field names.
                datafusion::physical_expr::utils::reassign_expr_columns(p, &projected_schema)
            })
            .collect::<datafusion::common::Result<_>>()?;

        let partition_keys = self
            .partition_key_extractor
            .try_extract(&filters)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let predicate = datafusion::physical_expr::conjunction_opt(filters);

        let physical_partitions: Vec<(PartitionId, Partition)> = self
            .partition_selector
            .get_live_partitions()
            .await
            .map_err(DataFusionError::External)?
            .into_iter()
            .flat_map(|(partition_id, partition)| {
                match &partition_keys {
                    // User requested a full scan of all partitions, return one physical partition per restate partition
                    None => itertools::Either::Left([(partition_id, partition)].into_iter()),
                    // User requested too many point reads; for safety reasons we will ignore them
                    Some(partition_keys) if partition_keys.len() > 4096 => {
                        itertools::Either::Left([(partition_id, partition)].into_iter())
                    }
                    // User requested a list of point reads
                    Some(partition_keys) => {
                        itertools::Either::Right(
                            partition_keys
                                // Find requested partition keys that are in this partition
                                .range(partition.key_range)
                                .cloned()
                                .map(move |partition_key| {
                                    // We create a 'physical partition' per partition key.
                                    // If the user provided a single point read (`id = 'inv_...'`),
                                    // then we will have 1 physical partition overall -> 1 logical partition.
                                    // If they provided N point reads (`id in ('inv_1', 'inv_2', ..)`),
                                    // we will have N physical partitions, perhaps even for a single restate partition.
                                    // Those will then be round-robined to the underlying logical partitions.
                                    // As a result, separate point reads on the same partition ID might end up
                                    // on separate logical partitions,but that's ok because they *can* be done
                                    // in parallel efficiently.
                                    (
                                        partition_id,
                                        Partition::new(partition_id, partition_key..=partition_key),
                                    )
                                }),
                        )
                    }
                }
            })
            .collect();

        let target_partitions = state.config().target_partitions();
        let logical_partitions =
            physical_partitions_to_logical(physical_partitions, target_partitions);

        let sort_columns = find_sort_columns(&self.ordering, &projected_schema);

        let eq_properties = if sort_columns.is_empty() {
            EquivalenceProperties::new(projected_schema.clone())
        } else {
            let ordering = make_ordering(sort_columns.clone());
            EquivalenceProperties::new_with_orderings(projected_schema.clone(), [ordering])
        };

        let plan = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(logical_partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(
            // all our scan functions use RecordBatchReceiverStream to build the result, which is cooperative
            datafusion::physical_plan::execution_plan::SchedulingType::Cooperative,
        );

        Ok(Arc::new(PartitionedExecutionPlan {
            logical_partitions,
            projected_schema,
            limit,
            predicate,
            scanner: self.partition_scanner.clone(),
            plan,
            statistics: self.statistics.clone().project(projection),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        let res = filters
            .iter()
            // if we set this to exact, we might be able to remove a FilterExec higher up the plan.
            // however, it means that fields we filter on won't end up in our projection, meaning we
            // have to manage a projected schema and a filter schema - defer this complexity for
            // future optimization.
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect();

        Ok(res)
    }
}

#[derive(Debug, Clone)]
struct PartitionedExecutionPlan<T> {
    logical_partitions: Vec<LogicalPartition>,
    projected_schema: SchemaRef,
    limit: Option<usize>,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    scanner: T,
    plan: PlanProperties,
    statistics: Statistics,
    metrics: ExecutionPlanMetricsSet,
}

impl<T> ExecutionPlan for PartitionedExecutionPlan<T>
where
    T: ScanPartition + Clone + Send,
{
    fn name(&self) -> &str {
        "PartitionedExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
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
                "PartitionedExecutionPlan does not support children".to_owned(),
            ));
        }

        Ok(self)
    }

    fn statistics(&self) -> datafusion::common::Result<Statistics> {
        Ok(self.statistics.clone())
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::common::Result<Statistics> {
        Ok(self.statistics.clone())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let physical_partitions = self
            .logical_partitions
            .get(partition)
            .expect("partition exists")
            .physical_partitions
            .to_vec();

        let sequential_scanners_stream = stream::iter(physical_partitions)
            .map({
                let scanner = self.scanner.clone();
                let schema = self.projected_schema.clone();
                let limit = self.limit;
                let predicate = self.predicate.clone();
                let batch_size = context.session_config().batch_size();
                let elapsed_compute = baseline_metrics.elapsed_compute().clone();
                move |(partition_id, partition)| {
                    scanner
                        .scan_partition(
                            partition_id,
                            partition.key_range,
                            schema.clone(),
                            predicate.clone(),
                            batch_size,
                            limit,
                            elapsed_compute.clone(),
                        )
                        .map_err(|e| DataFusionError::External(e.into()))
                }
            })
            .try_flatten();

        let metered = MeteredStream {
            inner: sequential_scanners_stream,
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

        // As in the static case above, the predicate *should* have the correct column indices,
        // but bugs in datafusion can create mixups.
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
            filters.push(predicate.clone());
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

impl<T> DisplayAs for PartitionedExecutionPlan<T>
where
    T: Debug,
{
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PartitionedExecutionPlan: scanner={:?}, partitions={}, projection=[{}]",
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

// Generic-based table provider that provides node-level or global data rather than
// partition-keyed data.
pub(crate) trait Scan: Debug + Send + Sync + 'static {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream;
}

pub(crate) type ScannerRef = Arc<dyn Scan>;

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
            Some(p) => SchemaRef::new(self.schema.project(p)?),
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
        let res = filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Inexact)
            .collect();

        Ok(res)
    }
}

#[derive(Debug, Clone)]
struct GenericExecutionPlan {
    projected_schema: SchemaRef,
    scanner: ScannerRef,
    limit: Option<usize>,
    filters: Vec<Expr>,
    plan_properties: PlanProperties,
    statistics: Statistics,
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
        let eq_properties = EquivalenceProperties::new(projected_schema.clone());

        let plan_properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            projected_schema,
            scanner,
            limit,
            filters: filters.to_vec(),
            plan_properties,
            statistics,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for GenericExecutionPlan {
    fn name(&self) -> &str {
        "GenericExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
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

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(self.statistics.clone())
    }

    fn partition_statistics(&self, _: Option<usize>) -> datafusion::error::Result<Statistics> {
        Ok(self.statistics.clone())
    }
}

impl DisplayAs for GenericExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
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

/// Display helper: comma-separated column names from a schema.
struct ProjectedColumns<'a>(&'a SchemaRef);

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

/// Stream wrapper that records [`BaselineMetrics`] using [`BaselineMetrics::record_poll`].
struct MeteredStream<S> {
    inner: S,
    baseline_metrics: BaselineMetrics,
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
