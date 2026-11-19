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
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Statistics};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{Stream, StreamExt};

use restate_partition_store::ordered_runs::RetainedMemory;
use restate_partition_store::{QueryIoExecutor, ReadSnapshot};
pub use restate_storage_query_api::Scan;
use restate_storage_query_api::schema::sharding::{Sharding, Unsharded};
use restate_storage_query_api::{LogicalSchema, ShardingScheme};
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_util_string::ReString;

use crate::QuerySource;
use crate::access::PrimaryRead;
use crate::context::SelectPartitions;
use crate::placement::{PartitionPlacement, StoragePlacementOptions};
use crate::selection;
use crate::sharding::{PartitionCandidates, PartitionSource};
use crate::source_registry::{PartitionLocation, PartitionSourceDescriptor, SourceRegistry};

pub trait ScanPartition: Send + Sync + Debug + 'static {
    /// Minimum serving eligibility needed by this backend.
    fn requires_leader(&self) -> bool {
        false
    }

    /// Primary lookup capability exposed by the worker-local resource binding.
    fn supports_primary_lookup(&self) -> bool {
        false
    }

    /// Whether this backend reads its rows through the named complete index.
    fn supports_index(&self, _index: &str) -> bool {
        false
    }

    /// Prepares the query's candidate read independently of partition ownership.
    /// Repeated executions share preparation and bind fresh cursors to their local
    /// partition. Scan navigation belongs to the reader, not the planner.
    /// `limit` is a safe per-execution candidate bound. The planner keeps limits
    /// above residual filters and ordering when they cannot safely reach the source.
    fn prepare_read(
        self: Arc<Self>,
        access: PrimaryRead,
        projection: SchemaRef,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>>;
}

/// Worker-local prepared source, shared by executions of an installed plan.
pub trait PreparedPartitionRead: Send + Sync + Debug + 'static {
    /// Captures needed by this leaf. Admission combines these across one subtree.
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
        Default::default()
    }

    fn execute(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<SendableRecordBatchStream>;

    /// [`Self::execute`] for an access that declares an order: also resolves,
    /// before the first row, to whether the rows come in that order on this
    /// partition's snapshot. The engine sorts them if not, and waits for the
    /// answer before it consumes any row: the answer must not wait for the rows.
    /// A read that wraps another must forward this, or the other's report is lost.
    fn execute_in_order(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: ReadMetrics,
    ) -> anyhow::Result<(SendableRecordBatchStream, InOrder)> {
        Ok((
            self.execute(context, batch_size, metrics)?,
            Box::pin(std::future::ready(true)),
        ))
    }
}

/// Whether a read's rows come in its declared order, known before its first row.
pub(crate) type InOrder = futures::future::BoxFuture<'static, bool>;

/// A read's charge on the query's memory pool, released when dropped.
struct PoolMemory(MemoryReservation);

impl RetainedMemory for PoolMemory {
    fn grow(&mut self, bytes: usize) -> restate_storage_api::Result<()> {
        self.0
            .try_grow(bytes)
            .map_err(|error| restate_storage_api::StorageError::Generic(error.into()))
    }

    fn shrink(&mut self, bytes: usize) {
        self.0.shrink(bytes)
    }
}

/// Metrics of one execution of a prepared read, recorded on its access leaf.
/// Reads register their own counters, so only reads that do the counted work
/// report them. The default records into a detached set.
#[derive(Debug, Clone, Default)]
pub struct ReadMetrics {
    pub elapsed_compute: Time,
    set: ExecutionPlanMetricsSet,
    lane: usize,
}

impl ReadMetrics {
    pub(crate) fn new(elapsed_compute: Time, set: &ExecutionPlanMetricsSet, lane: usize) -> Self {
        Self {
            elapsed_compute,
            set: set.clone(),
            lane,
        }
    }

    pub fn counter(&self, name: &'static str) -> Count {
        MetricBuilder::new(&self.set).counter(name, self.lane)
    }
}

/// Resources for one partition subtree, shared across its physical sources.
#[derive(derive_more::Debug, Clone)]
pub struct PartitionReadContext {
    partition_id: PartitionId,
    pub(crate) snapshot: Option<ReadSnapshot>,
    pub(crate) captured: restate_worker_api::CapturedPartitionState,
    /// The query-owned IO pool, shared with V1, that storage reads run on.
    #[debug(skip)]
    query_io: Option<QueryIoExecutor>,
    /// The query's memory pool, for reads that charge what they hold.
    #[debug(skip)]
    memory_pool: Option<Arc<dyn MemoryPool>>,
}

impl PartitionReadContext {
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    pub fn live(partition_id: PartitionId) -> Self {
        Self {
            partition_id,
            snapshot: None,
            captured: Default::default(),
            query_io: None,
            memory_pool: None,
        }
    }

    pub fn snapshot(read: ReadSnapshot) -> Self {
        Self {
            partition_id: read.partition_id(),
            snapshot: Some(read),
            captured: Default::default(),
            query_io: None,
            memory_pool: None,
        }
    }

    /// Runs this context's storage reads on `executor`.
    pub fn with_query_io(mut self, executor: QueryIoExecutor) -> Self {
        self.query_io = Some(executor);
        self
    }

    /// Charges what this context's reads hold to the query's `pool`.
    pub(crate) fn with_memory_pool(mut self, pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = Some(pool);
        self
    }

    /// A charge for what one read holds, on the query's memory pool. A read
    /// outside a query plan, as in tests that call a reader directly, charges
    /// nothing.
    pub(crate) fn retained_memory(&self, read: &str) -> Box<dyn RetainedMemory> {
        match &self.memory_pool {
            Some(pool) => Box::new(PoolMemory(MemoryConsumer::new(read).register(pool))),
            None => Box::new(()),
        }
    }

    /// The storage snapshot and the IO pool to read it on.
    pub fn storage_read(&self) -> anyhow::Result<(&ReadSnapshot, &QueryIoExecutor)> {
        let executor = self
            .query_io
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("partition read has no query IO pool"))?;
        Ok((self.storage()?, executor))
    }

    pub fn storage(&self) -> anyhow::Result<&ReadSnapshot> {
        self.snapshot
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("partition read has no admitted snapshot"))
    }

    pub fn captured(&self) -> &restate_worker_api::CapturedPartitionState {
        &self.captured
    }

    pub fn from_capture(read: restate_worker_api::CapturedPartitionRead) -> Self {
        Self {
            partition_id: read.partition,
            snapshot: read.storage,
            captured: read.live,
            query_io: None,
            memory_pool: None,
        }
    }
}

#[derive(derive_more::Debug)]
#[debug("PartitionedTableProvider")]
pub(crate) struct PartitionedTableProvider<S, L: LogicalSchema> {
    partition_selector: S,
    schema: SchemaRef,
    sources: Vec<PartitionSourceDescriptor>,
    logical: PhantomData<fn() -> L>,
    statistics: Statistics,
}

impl<S, L: LogicalSchema> PartitionedTableProvider<S, L> {
    pub(crate) fn declared<T: PartitionSource<Schema = L>>(
        partition_selector: S,
        manager: &SourceRegistry,
    ) -> Self {
        let schema = T::Schema::arrow_schema();
        let statistics = Statistics::new_unknown(&schema);
        Self {
            partition_selector,
            schema,
            sources: vec![manager.create_declared_source::<T>()],
            logical: PhantomData,
            statistics,
        }
    }

    /// Compose another physical layout of the same logical schema. Each source
    /// retains its own eligibility and access selection; native unions combine it.
    pub(crate) fn with_source<T: PartitionSource<Schema = L>>(
        mut self,
        manager: &SourceRegistry,
    ) -> Self {
        self.sources.push(manager.create_declared_source::<T>());
        self
    }

    pub(crate) fn with_statistics(self, statistics: Statistics) -> Self {
        Self { statistics, ..self }
    }
}

#[async_trait]
impl<S, L> TableProvider for PartitionedTableProvider<S, L>
where
    S: SelectPartitions,
    L: LogicalSchema,
    <L::Sharding as Sharding>::Candidates: Into<PartitionCandidates>,
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
        let filters = selection::physical_filters(filters, &projected_schema)?;
        let candidates: PartitionCandidates = L::candidate_shard_keys(&filters)
            .map_err(|e| DataFusionError::External(e.into()))?
            .into();

        // Shard-local JOINs compare the snapshots their inputs were resolved against.
        let topology: Arc<[(PartitionId, Partition)]> = self
            .partition_selector
            .get_live_partitions()
            .await
            .map_err(DataFusionError::External)?
            .into();
        let physical_partitions = candidates.resolve(&topology);

        if physical_partitions.is_empty() {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )));
        }

        let distributed = state
            .config()
            .get_extension::<crate::distributed::DistributedExecution>()
            .is_some();
        let options = state
            .config()
            .get_extension::<StoragePlacementOptions>()
            .as_deref()
            .copied()
            .unwrap_or_default();
        let mut plans = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            if !(source.can_read)(&filters).map_err(|e| DataFusionError::External(e.into()))? {
                continue;
            }
            let placement = PartitionPlacement {
                require_leader: source.require_leader,
                options,
            };
            let local = !distributed;
            if local
                && physical_partitions.iter().any(|(id, _)| {
                    !matches!(
                        source.manager.get_partition_target_node(*id, placement),
                        Ok(PartitionLocation::Local)
                    )
                })
            {
                return datafusion::common::plan_err!(
                    "remote sources require distributed execution"
                );
            }
            plans.push(crate::distributed::source::TableScanExec::for_query(
                source.project(projection.map(Vec::as_slice)),
                Arc::clone(&topology),
                physical_partitions.clone(),
                placement,
                Arc::clone(&projected_schema),
                Arc::new(self.statistics.clone().project(projection).to_inexact()),
                source.ordering.clone(),
                datafusion::physical_expr::conjunction_opt(filters.clone()),
                limit,
                local,
            )?);
        }
        if plans.is_empty() {
            Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )))
        } else {
            datafusion::physical_plan::union::UnionExec::try_new(plans)
        }
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

pub(crate) type ScannerRef = Arc<dyn Scan>;

#[derive(Debug)]
pub(crate) struct GenericTableProvider {
    schema: SchemaRef,
    table_name: ReString,
    sharding: ShardingScheme,
    scanner: Option<ScannerRef>,
    sources: Option<SourceRegistry>,
    statistics: Statistics,
    can_match: fn(&[Arc<dyn PhysicalExpr>]) -> anyhow::Result<bool>,
}

impl GenericTableProvider {
    pub(crate) fn declared<T: QuerySource>(
        scanner: Option<ScannerRef>,
        sources: Option<SourceRegistry>,
    ) -> Self
    where
        T::Schema: LogicalSchema<Sharding = Unsharded>,
    {
        let schema = T::Schema::arrow_schema();
        let statistics = Statistics::new_unknown(&schema);
        assert!(matches!(
            T::Schema::SHARDING,
            ShardingScheme::Coordinator | ShardingScheme::Unsharded(_)
        ));
        Self {
            schema,
            table_name: T::identity(),
            sharding: T::Schema::SHARDING,
            scanner,
            sources,
            statistics,
            can_match: |filters| Ok(T::Schema::candidate_shard_keys(filters)?.contains(&())),
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
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => SchemaRef::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        if !(self.can_match)(&selection::physical_filters(filters, &projected_schema)?)
            .map_err(|err| DataFusionError::External(err.into()))?
        {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )));
        }

        let scanner = self.scanner.clone().or_else(|| {
            self.sources
                .as_ref()
                .and_then(|sources| sources.local_node_scanner(&self.table_name))
        });
        if let ShardingScheme::Unsharded(role) = self.sharding {
            if let Some(sources) = &self.sources {
                let owner = sources
                    .singleton_owner(role, scanner.is_some())
                    .map_err(|error| DataFusionError::External(error.into()))?;
                if owner != sources.node_id() {
                    if state
                        .config()
                        .get_extension::<crate::distributed::DistributedExecution>()
                        .is_none()
                    {
                        return datafusion::common::plan_err!(
                            "remote sources require distributed execution"
                        );
                    }
                    return Ok(Arc::new(crate::distributed::NodeScanExec::for_node(
                        self.table_name.clone(),
                        owner,
                        projected_schema,
                        limit,
                        Some(Arc::new(self.statistics.clone().project(projection))),
                    )?));
                }
            } else if role.is_some() {
                return datafusion::common::plan_err!(
                    "role-restricted singleton source requires node placement metadata"
                );
            }
        }
        let Some(scanner) = scanner else {
            return datafusion::common::exec_err!(
                "local singleton source '{}' is unavailable",
                self.table_name
            );
        };
        Ok(Arc::new(GenericExecutionPlan::new(
            projected_schema,
            filters,
            limit,
            scanner,
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
pub(crate) struct GenericExecutionPlan {
    projected_schema: SchemaRef,
    scanner: ScannerRef,
    limit: Option<usize>,
    filters: Vec<Expr>,
    plan_properties: Arc<PlanProperties>,
    statistics: Arc<Statistics>,
    metrics: ExecutionPlanMetricsSet,
}

impl GenericExecutionPlan {
    pub(crate) fn new(
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::common::Result<TreeNodeRecursion>,
    ) -> datafusion::common::Result<TreeNodeRecursion> {
        // The scanner consumes logical filters, not physical expressions.
        Ok(TreeNodeRecursion::Continue)
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
