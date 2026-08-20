// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
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

use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;

use crate::context::SelectPartitions;
use crate::filter::{FirstMatchingPartitionKeyExtractor, PointReadFanout};
use crate::query_warnings::{QueryWarnings, WarningOrigin, try_record};
use crate::remote_query_scanner_manager::PartitionUnavailable;
use crate::table_util::{find_sort_columns, make_ordering};

#[async_trait]
pub trait ScanPartition: Send + Sync + Debug + 'static {
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

    /// Whether `partition_id` can be scanned right now.
    ///
    /// Consulted while the query is planned, so that a partition which cannot be scanned is
    /// dropped from the plan rather than failing the query once it executes. Detecting it
    /// here is also what keeps the resulting warning visible: it happens before the response
    /// headers are written.
    ///
    /// Async because the only way to ask whether a partition store is open is to await it —
    /// every public lookup on `PartitionStoreManager` is async. Scanners that cannot answer
    /// cheaply keep the default and are simply attempted.
    async fn check_available(
        &self,
        _partition_id: PartitionId,
    ) -> Result<(), PartitionUnavailable> {
        Ok(())
    }
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

        let partition_key_selection = self
            .partition_key_extractor
            .try_extract_selection(&filters)
            .map_err(|e| DataFusionError::External(e.into()))?;

        let predicate = datafusion::physical_expr::conjunction_opt(filters);

        // Drop partitions that cannot be scanned — they route nowhere, or their store is not
        // open on the node that would serve them — recording each one, so that a single
        // unavailable partition degrades the result instead of failing the query. Filtering
        // here rather than at execution time also keeps such a partition from taking down the
        // ones sharing its sequential scan bucket, and makes the warnings known before the
        // response starts streaming, which is what keeps them visible on the Arrow path.
        let warnings = state.config().get_extension::<QueryWarnings>();
        let live_partitions = self
            .partition_selector
            .get_live_partitions()
            .await
            .map_err(DataFusionError::External)?;

        let mut available_partitions = Vec::with_capacity(live_partitions.len());
        for (partition_id, partition) in live_partitions {
            match self.partition_scanner.check_available(partition_id).await {
                Ok(()) => available_partitions.push((partition_id, partition)),
                Err(unavailable) => {
                    if !try_record(
                        warnings.as_ref(),
                        WarningOrigin::Partition(partition_id),
                        unavailable.to_string(),
                    ) {
                        return Err(DataFusionError::External(unavailable.into()));
                    }
                }
            }
        }

        let physical_partitions: Vec<(PartitionId, Partition)> = available_partitions
            .into_iter()
            .flat_map(|(partition_id, partition)| {
                match &partition_key_selection {
                    // User requested a full scan of all partitions, return one physical partition per restate partition
                    None => itertools::Either::Left(Some((partition_id, partition)).into_iter()),
                    // Group selected keys into one physical scan per Restate partition if the number
                    // of keys is too large (to bound the number of concurrent scans) or if the fanout
                    // was set to per-partition.
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
                    // User requested a list of point reads
                    Some(selection) => {
                        itertools::Either::Right(
                            selection
                                .keys
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
                                        Partition::new(
                                            partition_id,
                                            KeyRange::new(partition_key, partition_key),
                                        ),
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
            plan: Arc::new(plan),
            statistics: Arc::new(self.statistics.clone().project(projection)),
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
    plan: Arc<PlanProperties>,
    statistics: Arc<Statistics>,
    metrics: ExecutionPlanMetricsSet,
}

impl<T> ExecutionPlan for PartitionedExecutionPlan<T>
where
    T: ScanPartition + Clone + Send,
{
    fn name(&self) -> &str {
        "PartitionedExecutionPlan"
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
                "PartitionedExecutionPlan does not support children".to_owned(),
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
                let warnings = context.session_config().get_extension::<QueryWarnings>();
                move |(partition_id, partition)| {
                    let err = match scanner.scan_partition(
                        partition_id,
                        partition.key_range,
                        schema.clone(),
                        predicate.clone(),
                        batch_size,
                        limit,
                        elapsed_compute.clone(),
                    ) {
                        // Guard each partition's scan separately: a failure only excuses the
                        // partition it happened in, and several partitions are scanned
                        // sequentially through the `try_flatten` below.
                        Ok(stream) => {
                            return Ok(Box::pin(PartitionScanStream::new(
                                stream,
                                partition_id,
                                warnings.clone(),
                            )) as SendableRecordBatchStream);
                        }
                        Err(err) => err,
                    };

                    // A synchronous failure means the scan never started. The only
                    // operational one is `PartitionUnavailable` — the partition became
                    // unscannable since we planned — so skip it as we would have at plan
                    // time. Anything else synchronous is a wiring bug and must stay loud.
                    match err.downcast_ref::<PartitionUnavailable>() {
                        Some(unavailable)
                            if try_record(
                                warnings.as_ref(),
                                WarningOrigin::Partition(partition_id),
                                unavailable.to_string(),
                            ) =>
                        {
                            Ok(Box::pin(RecordBatchStreamAdapter::new(
                                schema.clone(),
                                stream::empty(),
                            )) as SendableRecordBatchStream)
                        }
                        _ => Err(DataFusionError::External(err.into())),
                    }
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

/// Wraps one partition's scan so that a failure degrades the query instead of failing it.
///
/// On an error the partition's stream ends gracefully and a warning is recorded — but only
/// if the partition had not yet produced any rows. A partition that fails *after* yielding
/// rows propagates the error, because an arbitrary truncated subset of one partition is a
/// result no warning can describe honestly.
///
/// The "produced nothing" rule is deliberately about *when* the failure happened rather than
/// *why*: a failure relayed from another node arrives as `DataFusionError::Internal(String)`
/// with no recoverable type, so there is nothing to classify on.
struct PartitionScanStream {
    inner: SendableRecordBatchStream,
    partition_id: PartitionId,
    warnings: Option<Arc<QueryWarnings>>,
    rows_emitted: bool,
    done: bool,
}

impl PartitionScanStream {
    fn new(
        inner: SendableRecordBatchStream,
        partition_id: PartitionId,
        warnings: Option<Arc<QueryWarnings>>,
    ) -> Self {
        Self {
            inner,
            partition_id,
            warnings,
            rows_emitted: false,
            done: false,
        }
    }
}

impl Stream for PartitionScanStream {
    type Item = datafusion::common::Result<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Err(err))) => {
                self.done = true;

                if self.rows_emitted {
                    return Poll::Ready(Some(Err(err)));
                }

                let recorded = try_record(
                    self.warnings.as_ref(),
                    WarningOrigin::Partition(self.partition_id),
                    err.to_string(),
                );

                if recorded {
                    // End this partition's stream; its siblings keep scanning.
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(err)))
                }
            }
            Poll::Ready(Some(Ok(batch))) => {
                self.rows_emitted = true;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl datafusion::execution::RecordBatchStream for PartitionScanStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::DataFusionError;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use futures::executor::block_on;

    use restate_types::identifiers::PartitionId;

    use super::PartitionScanStream;
    use crate::query_warnings::QueryWarnings;

    fn one_row() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap()
    }

    /// Builds a stream yielding `batches` successful batches, then a failure.
    fn failing_after(batches: usize) -> super::SendableRecordBatchStream {
        let batch = one_row();
        let schema = batch.schema();
        let items = (0..batches)
            .map(move |_| Ok(batch.clone()))
            .chain(std::iter::once(Err(DataFusionError::Internal(
                "partition 2 doesn't exist on this node".to_owned(),
            ))));

        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(items.collect::<Vec<_>>()),
        ))
    }

    #[test]
    fn a_partition_that_produced_nothing_is_skipped() {
        let warnings = Arc::new(QueryWarnings::default());
        let stream = PartitionScanStream::new(
            failing_after(0),
            PartitionId::from(2),
            Some(Arc::clone(&warnings)),
        );

        let items = block_on(stream.collect::<Vec<_>>());

        // the failure is swallowed: this partition simply contributes no rows
        assert!(
            items.iter().all(Result::is_ok),
            "the error should not have propagated"
        );
        assert!(items.is_empty());

        let collected = warnings.collect();
        assert_eq!(1, collected.len());
        assert_eq!("partition 2", collected[0].origin.to_string());
    }

    /// The invariant the whole design rests on: tolerating a partition that already emitted
    /// rows would return an arbitrary truncated subset of it, which no warning can describe.
    #[test]
    fn a_partition_that_already_emitted_rows_fails_the_query() {
        let warnings = Arc::new(QueryWarnings::default());
        let stream = PartitionScanStream::new(
            failing_after(1),
            PartitionId::from(2),
            Some(Arc::clone(&warnings)),
        );

        let items = block_on(stream.collect::<Vec<_>>());

        assert_eq!(2, items.len());
        assert!(items[0].is_ok());
        assert!(items[1].is_err(), "the error must propagate");
        assert!(
            warnings.collect().is_empty(),
            "a truncated partition must not be recorded as a tolerated skip"
        );
    }

    #[test]
    fn without_a_sink_the_error_propagates() {
        // Nowhere to report the skip, so degrading silently is not an option.
        let stream = PartitionScanStream::new(failing_after(0), PartitionId::from(2), None);

        let items = block_on(stream.collect::<Vec<_>>());

        assert_eq!(1, items.len());
        assert!(items[0].is_err());
    }
}
