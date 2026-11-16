// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Node-level fan-out table provider for cluster introspection queries.
//!
//! Unlike [`PartitionedTableProvider`] which fans out by partition ID, this
//! provider fans out by **node ID**, querying each node in a target set
//! (e.g., all log-server nodes) and combining the results.
//!
//! Each target node runs an owner-bound task and streams Arrow record batches
//! back. A task is a plain scan or, for a shard-local JOIN of two node tables
//! (equal plain or generational node IDs), the whole JOIN of that node's rows.
//! Every node-level introspection
//! table includes `plain_node_id` and `gen_node_id` columns (both `Utf8`).
//! The `plain_node_id` column enables predicate pushdown so queries like
//! `WHERE plain_node_id = 'N5'` target only the relevant node.

#[cfg(test)]
use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, PlanProperties,
};
use futures::{Stream, StreamExt};

use restate_core::Metadata;
use restate_platform::sync::Mutex;
use restate_storage_query_api::schema::sharding::{NodeSharding, ShardingColumn};
use restate_storage_query_api::{LogicalSchema, NodeWarning, NodeWarnings, ShardingScheme};
use restate_types::nodes_config::Role;
use restate_types::{GenerationalNodeId, PlainNodeId};
use restate_util_string::ReString;
use restate_util_string::ToReString;

use crate::QuerySource;
use crate::selection;
#[cfg(test)]
use crate::selection::Domain;
use crate::source_registry::SourceRegistry;
use crate::table_providers::{GenericExecutionPlan, MeteredStream, ProjectedColumns, Scan};

/// Determines the set of target nodes for a fan-out query.
pub(crate) trait NodeLocator: Send + Sync + Debug + 'static {
    /// Returns the set of target nodes for this table.
    fn target_nodes(&self) -> anyhow::Result<Vec<TargetNode>>;
}

#[derive(Debug, Clone)]
pub(crate) struct TargetNode {
    pub plain_node_id: PlainNodeId,
    pub node_id: GenerationalNodeId,
    pub is_local: bool,
}

/// Locates all nodes in the cluster.
#[derive(Clone)]
pub(crate) struct AllNodeLocator {
    metadata: Metadata,
}

impl Debug for AllNodeLocator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AllNodeLocator").finish()
    }
}

impl AllNodeLocator {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
}

impl NodeLocator for AllNodeLocator {
    fn target_nodes(&self) -> anyhow::Result<Vec<TargetNode>> {
        let nodes_config = self.metadata.nodes_config_snapshot();
        let my_node_id = self.metadata.my_node_id();

        Ok(nodes_config
            .iter()
            .map(|(plain_id, config)| TargetNode {
                plain_node_id: plain_id,
                node_id: config.current_generation,
                is_local: config.current_generation == my_node_id,
            })
            .collect())
    }
}

/// Locates target nodes by filtering [`NodesConfiguration`] by role.
#[derive(Clone)]
pub(crate) struct RoleBasedNodeLocator {
    role: Role,
    metadata: Metadata,
}

impl Debug for RoleBasedNodeLocator {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoleBasedNodeLocator")
            .field("role", &self.role)
            .finish()
    }
}

impl RoleBasedNodeLocator {
    pub fn new(role: Role, metadata: Metadata) -> Self {
        Self { role, metadata }
    }
}

impl NodeLocator for RoleBasedNodeLocator {
    fn target_nodes(&self) -> anyhow::Result<Vec<TargetNode>> {
        let nodes_config = self.metadata.nodes_config_snapshot();
        let my_node_id = self.metadata.my_node_id();

        Ok(nodes_config
            .iter_role(self.role)
            .map(|(plain_id, config)| TargetNode {
                plain_node_id: plain_id,
                node_id: config.current_generation,
                is_local: config.current_generation == my_node_id,
            })
            .collect())
    }
}

#[cfg(test)]
fn select_nodes(
    filters: &[Arc<dyn PhysicalExpr>],
    nodes: Vec<TargetNode>,
) -> anyhow::Result<Vec<TargetNode>> {
    if filters.is_empty() {
        return Ok(nodes);
    }
    let plain: HashMap<_, _> = nodes
        .iter()
        .map(|node| (node.plain_node_id.to_restring(), node.node_id))
        .collect();
    let generations: HashMap<_, _> = nodes
        .iter()
        .map(|node| (node.node_id.to_restring(), node.node_id))
        .collect();
    let domain = selection::analyze(filters, |column, op, value| {
        if !matches!(column, "plain_node_id" | "gen_node_id")
            || op != datafusion::logical_expr::Operator::Eq
        {
            return Ok(Domain::all());
        }
        Ok(match value.try_as_str() {
            Some(Some(value)) => {
                let identities = if column == "plain_node_id" {
                    &plain
                } else {
                    &generations
                };
                Domain::values(identities.get(value).copied())
            }
            Some(None) => Domain::empty(),
            None => Domain::all(),
        })
    })?;
    // These are SQL strings, not parsable identifiers: '5' must not match 'N5',
    // and an exact generation must never resolve to its replacement generation.
    Ok(nodes
        .into_iter()
        .filter(|node| domain.contains(&node.node_id))
        .collect())
}

/// A DataFusion [`TableProvider`] that fans out scans to multiple nodes.
///
/// Each target node (determined by [`NodeLocator`]) becomes a logical
/// partition in the execution plan. The `plain_node_id` column allows
/// predicate pushdown to skip nodes not matching the filter. Both
/// `plain_node_id` and `gen_node_id` are `Utf8` columns present in every
/// node-level introspection table.
#[derive(Debug)]
pub(crate) struct NodeFanOutTableProvider {
    schema: SchemaRef,
    node_locator: Arc<dyn NodeLocator>,
    local_scanner: Option<Arc<dyn Scan>>,
    table_name: ReString,
    statistics: Statistics,
    targeting: NodeTargeting,
    /// Sharding columns at ordinals of the unprojected schema.
    sharding_columns: Vec<ShardingColumn<usize>>,
}

type NodeTargeting =
    fn(&[Arc<dyn PhysicalExpr>], Vec<TargetNode>) -> anyhow::Result<Vec<TargetNode>>;

fn declared_nodes<S: LogicalSchema<Sharding = NodeSharding>>(
    filters: &[Arc<dyn PhysicalExpr>],
    nodes: Vec<TargetNode>,
) -> anyhow::Result<Vec<TargetNode>> {
    let candidates = S::candidate_shard_keys(filters)?;
    Ok(nodes
        .into_iter()
        .filter(|node| candidates.contains(node.node_id))
        .collect())
}

impl NodeFanOutTableProvider {
    pub(crate) fn declared<T: QuerySource>(
        metadata: Metadata,
        sources: SourceRegistry,
        local_scanner: Option<Arc<dyn Scan>>,
    ) -> Self
    where
        T::Schema: LogicalSchema<Sharding = NodeSharding>,
    {
        let schema = T::Schema::arrow_schema();
        let statistics = Statistics::new_unknown(&schema);
        let ShardingScheme::ByNode(role) = T::Schema::SHARDING else {
            unreachable!("node fan-out requires a node-sharded declaration")
        };
        let node_locator: Arc<dyn NodeLocator> = match role {
            Some(role) => Arc::new(RoleBasedNodeLocator::new(role, metadata)),
            None => Arc::new(AllNodeLocator::new(metadata)),
        };
        Self {
            schema,
            node_locator,
            local_scanner: local_scanner.or_else(|| sources.local_node_scanner(&T::identity())),
            table_name: T::identity(),
            statistics,
            targeting: declared_nodes::<T::Schema>,
            sharding_columns: crate::sharding::exposed_sharding_columns::<T::Schema>(),
        }
    }

    #[cfg(test)]
    pub fn new(
        schema: SchemaRef,
        node_locator: Arc<dyn NodeLocator>,
        local_scanner: Option<Arc<dyn Scan>>,
        table_name: impl Into<ReString>,
    ) -> Self {
        let statistics = Statistics::new_unknown(&schema);
        Self {
            schema,
            node_locator,
            local_scanner,
            table_name: table_name.into(),
            statistics,
            targeting: select_nodes,
            sharding_columns: Vec::new(),
        }
    }
}

#[async_trait]
impl datafusion::catalog::TableProvider for NodeFanOutTableProvider {
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

        let target_nodes = self.node_locator.target_nodes().map_err(|e| {
            DataFusionError::Internal(format!("Failed to locate target nodes: {e}"))
        })?;

        let physical_filters = selection::physical_filters(filters, &projected_schema)?;
        let filtered_nodes = (self.targeting)(&physical_filters, target_nodes)
            .map_err(|err| DataFusionError::External(err.into()))?;
        if filtered_nodes.is_empty() {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )));
        }

        let scan = NodeScan {
            table_name: self.table_name.clone(),
            local_scanner: self.local_scanner.clone(),
            filters: filters.to_vec(),
            limit,
            sharding_columns: crate::sharding::project_sharding_columns(
                &self.sharding_columns,
                projection.map(Vec::as_slice),
            ),
            distributed: state
                .config()
                .get_extension::<crate::distributed::DistributedExecution>()
                .is_some(),
        };
        Ok(Arc::new(NodeFanOutExecutionPlan::for_scan(
            projected_schema,
            filtered_nodes,
            scan,
            self.statistics.clone().project(projection),
        )?))
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

/// A plain node-table scan, as planned by [`NodeFanOutTableProvider`]. Shard-local
/// JOINs rebuild per-node leaves from it.
#[derive(Debug, Clone)]
pub(crate) struct NodeScan {
    table_name: ReString,
    local_scanner: Option<Arc<dyn Scan>>,
    filters: Vec<Expr>,
    pub(crate) limit: Option<usize>,
    /// Sharding columns at ordinals of the projected schema.
    pub(crate) sharding_columns: Vec<ShardingColumn<usize>>,
    pub(crate) distributed: bool,
}

impl NodeScan {
    /// The scan of one node: an owner-bound source when distributed, otherwise the
    /// local scanner. `None` when this coordinator cannot scan the node.
    pub(crate) fn leaf(
        &self,
        schema: &SchemaRef,
        node: &TargetNode,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        if self.distributed {
            return Ok(Some(Arc::new(crate::distributed::NodeScanExec::for_node(
                self.table_name.clone(),
                node.node_id,
                Arc::clone(schema),
                self.limit,
                None,
            )?)));
        }
        Ok(node
            .is_local
            .then(|| self.local_scanner.clone())
            .flatten()
            .map(|scanner| {
                Arc::new(GenericExecutionPlan::new(
                    Arc::clone(schema),
                    &self.filters,
                    self.limit,
                    scanner,
                    Statistics::new_unknown(schema),
                )) as Arc<dyn ExecutionPlan>
            }))
    }
}

/// Execution plan that runs one isolated task per node, in parallel.
///
/// Each logical partition corresponds to one target node. Its task is a plain
/// scan of a node table, or a shard-local JOIN of two node tables. When a node is
/// unreachable or its task fails, the error is captured as a [`NodeWarning`]
/// instead of failing the entire query. Output streams: rows a failing task
/// already emitted remain, the rest of that node's output is missing, and the
/// warning reports the incomplete result.
/// Callers can inspect the accumulated warnings via
/// [`NodeFanOutExecutionPlan::node_warnings()`].
#[derive(Debug, Clone)]
pub(crate) struct NodeFanOutExecutionPlan {
    projected_schema: SchemaRef,
    target_nodes: Vec<TargetNode>,
    /// The plain scan this plan performs; `None` for a shard-local JOIN.
    scan: Option<NodeScan>,
    /// One line describing the per-node task when it is not a plain scan, since
    /// tasks are opaque to `EXPLAIN`.
    task: Option<String>,
    // Each node's task: a distributed owner stage or a local subtree, or `None`
    // when this coordinator cannot run it. Each best-effort node has an isolated
    // library runtime so an installation error cannot abort another node's
    // result. Tasks are opaque to the outer planner: it must not combine their
    // admission/failure domains.
    tasks: Vec<Option<Arc<dyn ExecutionPlan>>>,
    plan_properties: Arc<PlanProperties>,
    statistics: Arc<Statistics>,
    metrics: ExecutionPlanMetricsSet,
    node_warnings: NodeWarnings,
}

impl NodeFanOutExecutionPlan {
    fn for_scan(
        projected_schema: SchemaRef,
        target_nodes: Vec<TargetNode>,
        scan: NodeScan,
        statistics: Statistics,
    ) -> datafusion::common::Result<Self> {
        let mut tasks = Vec::with_capacity(target_nodes.len());
        for node in &target_nodes {
            let task = scan.leaf(&projected_schema, node)?;
            tasks.push(match task {
                Some(task) if scan.distributed => Some(crate::distributed::plan(task)?),
                task => task,
            });
        }
        Ok(Self::new(
            projected_schema,
            target_nodes,
            Some(scan),
            None,
            tasks,
            statistics,
        ))
    }

    /// Scans `table` on exactly `nodes`, for a node source that no SQL table
    /// exposes, such as the worker reports `partition_state` reconciles.
    pub(crate) fn for_nodes(
        schema: SchemaRef,
        table: ReString,
        nodes: Vec<TargetNode>,
        sources: &SourceRegistry,
        distributed: bool,
    ) -> datafusion::common::Result<Self> {
        let scan = NodeScan {
            local_scanner: sources.local_node_scanner(&table),
            table_name: table,
            filters: Vec::new(),
            limit: None,
            sharding_columns: Vec::new(),
            distributed,
        };
        let statistics = Statistics::new_unknown(&schema);
        Self::for_scan(schema, nodes, scan, statistics)
    }

    /// One task per node, such as a shard-local JOIN of two node tables, described
    /// by `task` in plans.
    pub(crate) fn for_tasks(
        schema: SchemaRef,
        target_nodes: Vec<TargetNode>,
        task: String,
        tasks: Vec<Option<Arc<dyn ExecutionPlan>>>,
    ) -> Self {
        let statistics = Statistics::new_unknown(&schema);
        Self::new(schema, target_nodes, None, Some(task), tasks, statistics)
    }

    fn new(
        projected_schema: SchemaRef,
        target_nodes: Vec<TargetNode>,
        scan: Option<NodeScan>,
        task: Option<String>,
        tasks: Vec<Option<Arc<dyn ExecutionPlan>>>,
        statistics: Statistics,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(target_nodes.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            projected_schema,
            target_nodes,
            scan,
            task,
            tasks,
            plan_properties: Arc::new(plan_properties),
            statistics: Arc::new(statistics),
            metrics: ExecutionPlanMetricsSet::new(),
            node_warnings: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The plain scan this plan performs; `None` for a shard-local JOIN.
    pub(crate) fn scan(&self) -> Option<&NodeScan> {
        self.scan.as_ref()
    }

    pub(crate) fn target_nodes(&self) -> &[TargetNode] {
        &self.target_nodes
    }

    /// Returns the shared warnings collector. The gRPC layer uses this to
    /// attach per-node errors to the final [`QueryResponse`].
    pub fn node_warnings(&self) -> &NodeWarnings {
        &self.node_warnings
    }

    /// The stream of one target node, with its errors: [`ExecutionPlan::execute`]
    /// turns them into node warnings instead. Callers that need each node's
    /// outcome themselves, such as `partition_state`, use this.
    pub(crate) fn execute_node(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        if partition >= self.target_nodes.len() {
            return Err(DataFusionError::Internal(format!(
                "NodeFanOutExecutionPlan: partition {} out of range ({})",
                partition,
                self.target_nodes.len()
            )));
        }
        Ok(match &self.tasks[partition] {
            Some(task) => {
                // Defer even synchronous runtime failures into the warning boundary.
                let task = Arc::clone(task);
                let stream =
                    futures::TryStreamExt::try_flatten(futures::stream::once(async move {
                        task.execute(0, context)
                    }));
                Box::pin(
                    datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                        self.schema(),
                        MeteredStream {
                            inner: stream.boxed(),
                            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
                        },
                    ),
                )
            }
            None => Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    self.schema(),
                    futures::stream::once(async {
                        datafusion::common::exec_err!(
                            "node source requires distributed execution or a registered local scanner"
                        )
                    }),
                ),
            ),
        })
    }
}

impl ExecutionPlan for NodeFanOutExecutionPlan {
    fn name(&self) -> &str {
        "NodeFanOutExecutionPlan"
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
        // Node selection and local scans consume logical filters.
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
                "NodeFanOutExecutionPlan does not support children".to_owned(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let inner = self.execute_node(partition, context)?;
        let node_label = self.target_nodes[partition].plain_node_id.to_string();
        Ok(Box::pin(ErrorCatchingStream::new(
            inner,
            node_label.into(),
            self.node_warnings.clone(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _: Option<usize>) -> datafusion::error::Result<Arc<Statistics>> {
        Ok(self.statistics.clone())
    }
}

impl DisplayAs for NodeFanOutExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let nodes = NodeList(&self.target_nodes);
        let projection = ProjectedColumns(&self.projected_schema);
        match (t, &self.scan) {
            (DisplayFormatType::Default | DisplayFormatType::Verbose, Some(scan)) => {
                write!(
                    f,
                    "NodeFanOutExecutionPlan: table={}, target_nodes=[{nodes}], projection=[{projection}], distributed={}",
                    scan.table_name, scan.distributed,
                )?;
                if let Some(limit) = scan.limit {
                    write!(f, ", limit={limit}")?;
                }
                Ok(())
            }
            (DisplayFormatType::Default | DisplayFormatType::Verbose, None) => write!(
                f,
                "NodeFanOutExecutionPlan: target_nodes=[{nodes}], projection=[{projection}], task=[{}]",
                self.task.as_deref().unwrap_or_default(),
            ),
            (DisplayFormatType::TreeRender, scan) => {
                match scan {
                    Some(scan) => writeln!(f, "table={}", scan.table_name)?,
                    None => writeln!(f, "task=[{}]", self.task.as_deref().unwrap_or_default())?,
                }
                writeln!(f, "target_nodes=[{nodes}]")?;
                writeln!(f, "projection=[{projection}]")?;
                if let Some(limit) = scan.as_ref().and_then(|scan| scan.limit) {
                    writeln!(f, "limit={limit}")?;
                }
                Ok(())
            }
        }
    }
}

struct NodeList<'a>(&'a [TargetNode]);

impl Display for NodeList<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for node in self.0 {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "{}", node.node_id)?;
            if node.is_local {
                write!(f, "(local)")?;
            }
            first = false;
        }
        Ok(())
    }
}

/// Stream adapter that catches errors from a per-node [`SendableRecordBatchStream`],
/// records them as [`NodeWarning`]s, and terminates the individual stream gracefully
/// instead of propagating the error through DataFusion.
struct ErrorCatchingStream {
    inner: SendableRecordBatchStream,
    node_label: ReString,
    warnings: NodeWarnings,
    done: bool,
}

impl ErrorCatchingStream {
    fn new(inner: SendableRecordBatchStream, node_label: ReString, warnings: NodeWarnings) -> Self {
        Self {
            inner,
            node_label,
            warnings,
            done: false,
        }
    }
}

impl Stream for ErrorCatchingStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Err(err))) => {
                self.done = true;
                self.warnings.lock().push(NodeWarning {
                    node_id: self.node_label.clone(),
                    message: err.to_restring(),
                });
                // Terminate this partition's stream gracefully
                Poll::Ready(None)
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl datafusion::execution::RecordBatchStream for ErrorCatchingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::TreeNode;
    use datafusion::prelude::{SessionContext, col, lit};

    use restate_core::test_env::{TestCoreEnvBuilder, create_mock_nodes_config};
    use restate_storage_query_api::tables::bifrost_read_streams::BifrostReadStreams;

    use crate::tables::bifrost_read_stream::BifrostReadStreamsTable;

    use super::*;

    #[restate_core::test]
    async fn declared_node_eligibility_includes_all_roles_and_preserves_role_restrictions() {
        restate_storage_query_api::define_schema! {
            log_server_streams {
                name: "log_server_streams",
                sharding: ShardingScheme::ByNode(Some(Role::LogServer)),
                sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
                columns {
                    plain_node_id: ReString => DataType::Utf8,
                    gen_node_id: ReString => DataType::Utf8,
                }
            }
        }
        crate::tables::table_adapter!(LogServerStreamsTable, LogServerStreams);

        let mut nodes = create_mock_nodes_config(1, 1);
        for (id, generation, role) in [(2, 1, Role::Admin), (3, 2, Role::LogServer)] {
            let mut node = create_mock_nodes_config(id, generation)
                .find_node_by_id(GenerationalNodeId::new(id, generation))
                .unwrap()
                .clone();
            node.roles = role.into();
            nodes.upsert_node(node);
        }
        let core = TestCoreEnvBuilder::with_incoming_only_connector()
            .set_nodes_config(nodes)
            .build()
            .await;
        let sources = SourceRegistry::local_only(core.metadata.clone());
        let context = SessionContext::new();
        context
            .register_table(
                "bifrost_read_streams",
                BifrostReadStreamsTable::create_provider(
                    core.metadata.clone(),
                    sources.clone(),
                    None,
                ),
            )
            .unwrap();
        context
            .register_table(
                "log_server_streams",
                Arc::new(NodeFanOutTableProvider::declared::<LogServerStreamsTable>(
                    core.metadata,
                    sources,
                    None,
                )),
            )
            .unwrap();

        #[rustfmt::skip]
        let cases: &[(&str, &[&str])] = &[
            ("SELECT plain_node_id FROM bifrost_read_streams",                              &["N1:1", "N2:1", "N3:2"]),
            ("SELECT plain_node_id FROM bifrost_read_streams WHERE plain_node_id = 'N2'",    &["N2:1"]),
            ("SELECT plain_node_id FROM bifrost_read_streams WHERE gen_node_id = 'N3:2'",    &["N3:2"]),
            ("SELECT plain_node_id FROM bifrost_read_streams WHERE gen_node_id = 'N3:1'",    &[]),
            ("SELECT plain_node_id FROM log_server_streams",                                &["N3:2"]),
            ("SELECT plain_node_id FROM log_server_streams WHERE plain_node_id = 'N2'",      &[]),
        ];
        for &(sql, expected) in cases {
            let plan = context
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            let mut targets = Vec::new();
            plan.apply(|plan| {
                if let Some(fan_out) = plan.downcast_ref::<NodeFanOutExecutionPlan>() {
                    targets.extend(
                        fan_out
                            .target_nodes
                            .iter()
                            .map(|node| node.node_id.to_string()),
                    );
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
            targets.sort_unstable();
            assert_eq!(
                targets, expected,
                "node_roles_and_identity_filters_compose_before_dispatch: {sql}"
            );
        }
    }

    #[test]
    fn identity_domains_preserve_sql_strings_and_generations() {
        let schema = Schema::new(vec![
            Field::new("plain_node_id", DataType::Utf8, false),
            Field::new("gen_node_id", DataType::Utf8, false),
            Field::new("other", DataType::Utf8, true),
        ]);
        let nodes: Vec<_> = [1, 2, 3]
            .into_iter()
            .map(|id| TargetNode {
                plain_node_id: id.into(),
                node_id: GenerationalNodeId::new(id, 2),
                is_local: id == 1,
            })
            .collect();
        let plain = col("plain_node_id");
        let generation = col("gen_node_id");
        for (filter, expected) in [
            (plain.clone().eq(lit("N2")), vec![2]),
            (lit("N2").eq(plain.clone()), vec![2]),
            (generation.clone().eq(lit("N2:2")), vec![2]),
            (generation.clone().eq(lit("N2:1")), vec![]),
            (plain.clone().eq(lit("2")), vec![]),
            (plain.clone().eq(lit("not-a-node")), vec![]),
            (
                plain
                    .clone()
                    .in_list(vec![lit("N1"), lit("N3"), lit("N1")], false),
                vec![1, 3],
            ),
            (
                plain
                    .clone()
                    .eq(lit("N2"))
                    .and(generation.clone().eq(lit("N1:2"))),
                vec![],
            ),
            (
                plain.clone().eq(lit("N2")).and(plain.clone().eq(lit("N3"))),
                vec![],
            ),
            (
                plain.clone().eq(lit("N2")).or(plain.clone().eq(lit("N3"))),
                vec![2, 3],
            ),
            (
                plain.clone().eq(lit("N2")).or(col("other").eq(lit("x"))),
                vec![1, 2, 3],
            ),
            (plain.clone().in_list(vec![lit("N1")], true), vec![2, 3]),
            (plain.clone().gt(lit("N10")), vec![2, 3]),
            (
                plain
                    .clone()
                    .eq(lit("N1"))
                    .and(generation.clone().eq(lit("N2:2")))
                    .or(plain.eq(lit("N2")).and(generation.eq(lit("N1:2")))),
                vec![],
            ),
        ] {
            let filters =
                selection::physical_filters(std::slice::from_ref(&filter), &schema).unwrap();
            let selected = declared_nodes::<BifrostReadStreams>(&filters, nodes.clone()).unwrap();
            assert_eq!(
                selected
                    .into_iter()
                    .map(|node| node.node_id.raw_id())
                    .collect::<Vec<_>>(),
                expected,
                "{filter}"
            );
        }
    }
}
