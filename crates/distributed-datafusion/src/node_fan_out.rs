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
//! Each target node runs a local scanner (via the same [`RemoteDataFusionService`]
//! RPC) and streams Arrow record batches back. Every node-level introspection
//! table includes `plain_node_id` and `gen_node_id` columns (both `Utf8`).
//! The `plain_node_id` column enables predicate pushdown so queries like
//! `WHERE plain_node_id = 'N5'` target only the relevant node.

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
use restate_storage_query_api::{NodeWarning, NodeWarnings};
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::sharding::KeyRange;
use restate_types::{GenerationalNodeId, PlainNodeId};
use restate_util_string::{ReString, ToReString};

use crate::remote_query_scanner_client::remote_scan_as_datafusion_stream;
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::selection::{self, Domain};
use crate::table_providers::{MeteredStream, ProjectedColumns, Scan};

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
    remote_scanner_manager: RemoteScannerManager,
    local_scanner: Option<Arc<dyn Scan>>,
    table_name: ReString,
    statistics: Statistics,
}

impl NodeFanOutTableProvider {
    pub fn new(
        schema: SchemaRef,
        node_locator: Arc<dyn NodeLocator>,
        remote_scanner_manager: RemoteScannerManager,
        local_scanner: Option<Arc<dyn Scan>>,
        table_name: impl Into<ReString>,
    ) -> Self {
        let statistics = Statistics::new_unknown(&schema);
        Self {
            schema,
            node_locator,
            remote_scanner_manager,
            local_scanner,
            table_name: table_name.into(),
            statistics,
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
        let filtered_nodes = select_nodes(&physical_filters, target_nodes)
            .map_err(|err| DataFusionError::External(err.into()))?;
        if filtered_nodes.is_empty() {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )));
        }

        Ok(Arc::new(NodeFanOutExecutionPlan::new(
            projected_schema,
            filtered_nodes,
            self.remote_scanner_manager.clone(),
            self.local_scanner.clone(),
            self.table_name.clone(),
            filters.to_vec(),
            limit,
            self.statistics.clone().project(projection),
            state
                .config()
                .get_extension::<crate::distributed::DistributedExecution>()
                .is_some(),
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

/// Execution plan that scans multiple nodes in parallel.
///
/// Each logical partition corresponds to one target node. When a node is
/// unreachable or returns an error, the error is captured as a [`NodeWarning`]
/// instead of failing the entire query. Callers can inspect the accumulated
/// warnings via [`NodeFanOutExecutionPlan::node_warnings()`].
#[derive(Debug, Clone)]
pub(crate) struct NodeFanOutExecutionPlan {
    projected_schema: SchemaRef,
    target_nodes: Vec<TargetNode>,
    remote_scanner_manager: RemoteScannerManager,
    local_scanner: Option<Arc<dyn Scan>>,
    table_name: ReString,
    filters: Vec<Expr>,
    limit: Option<usize>,
    plan_properties: Arc<PlanProperties>,
    statistics: Arc<Statistics>,
    metrics: ExecutionPlanMetricsSet,
    node_warnings: NodeWarnings,
    // Each best-effort node has an isolated library runtime so an installation
    // error cannot abort another node's result. These are opaque to the outer
    // planner: it must not combine their admission/failure domains.
    task_plans: Option<Vec<Arc<dyn ExecutionPlan>>>,
}

impl NodeFanOutExecutionPlan {
    #[allow(clippy::too_many_arguments)]
    fn new(
        projected_schema: SchemaRef,
        target_nodes: Vec<TargetNode>,
        remote_scanner_manager: RemoteScannerManager,
        local_scanner: Option<Arc<dyn Scan>>,
        table_name: ReString,
        filters: Vec<Expr>,
        limit: Option<usize>,
        statistics: Statistics,
        distributed: bool,
    ) -> datafusion::common::Result<Self> {
        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let num_partitions = target_nodes.len().max(1);

        let plan_properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        let task_plans = distributed
            .then(|| {
                target_nodes
                    .iter()
                    .map(|node| {
                        crate::distributed::plan(Arc::new(
                            crate::distributed::SourceExec::for_node(
                                table_name.clone(),
                                node.node_id,
                                Arc::clone(&projected_schema),
                                limit,
                            )?,
                        ))
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()
            })
            .transpose()?;

        Ok(Self {
            projected_schema,
            target_nodes,
            remote_scanner_manager,
            local_scanner,
            table_name,
            filters,
            limit,
            plan_properties: Arc::new(plan_properties),
            statistics: Arc::new(statistics),
            metrics: ExecutionPlanMetricsSet::new(),
            node_warnings: Arc::new(Mutex::new(Vec::new())),
            task_plans,
        })
    }

    /// Returns the shared warnings collector. The gRPC layer uses this to
    /// attach per-node errors to the final [`QueryResponse`].
    pub fn node_warnings(&self) -> &NodeWarnings {
        &self.node_warnings
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
        let target = self.target_nodes.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "NodeFanOutExecutionPlan: partition {} out of range ({})",
                partition,
                self.target_nodes.len()
            ))
        })?;

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let batch_size = context.session_config().batch_size();
        let node_label = target.plain_node_id.to_string();

        if let Some(plans) = &self.task_plans {
            // Defer even synchronous runtime failures into the warning boundary.
            let plan = Arc::clone(&plans[partition]);
            let stream = futures::TryStreamExt::try_flatten(futures::stream::once(async move {
                plan.execute(0, context)
            }));
            return Ok(Box::pin(ErrorCatchingStream::new(
                Box::pin(
                    datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                        self.schema(),
                        MeteredStream {
                            inner: stream.boxed(),
                            baseline_metrics,
                        },
                    ),
                ),
                node_label.into(),
                Arc::clone(&self.node_warnings),
            )));
        }

        let inner: SendableRecordBatchStream = if target.is_local
            && let Some(local_scanner) = &self.local_scanner
        {
            let inner = local_scanner.scan(
                self.projected_schema.clone(),
                &self.filters,
                batch_size,
                self.limit,
            );

            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    self.projected_schema.clone(),
                    MeteredStream {
                        inner,
                        baseline_metrics,
                    },
                ),
            )
        } else {
            // Remote scan: use a sentinel partition_id since this is a node-level table
            let scanner_id = self.remote_scanner_manager.allocate_scanner_id();
            let inner = remote_scan_as_datafusion_stream(
                self.remote_scanner_manager.remote_scanner_service(),
                target.node_id.into(),
                scanner_id,
                PartitionId::MIN,
                KeyRange::FULL,
                self.table_name.clone(),
                self.projected_schema.clone(),
                None, // predicate is applied locally after combining
                batch_size,
                self.limit,
            );

            Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    self.projected_schema.clone(),
                    MeteredStream {
                        inner,
                        baseline_metrics,
                    },
                ),
            )
        };

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
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "NodeFanOutExecutionPlan: table={}, target_nodes=[{}], projection=[{}], distributed={}",
                    self.table_name,
                    NodeList(&self.target_nodes),
                    ProjectedColumns(&self.projected_schema),
                    self.task_plans.is_some(),
                )?;
                if let Some(limit) = self.limit {
                    write!(f, ", limit={limit}")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "table={}", self.table_name)?;
                writeln!(f, "target_nodes=[{}]", NodeList(&self.target_nodes))?;
                writeln!(
                    f,
                    "projection=[{}]",
                    ProjectedColumns(&self.projected_schema)
                )?;
                if let Some(limit) = self.limit {
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
    use datafusion::prelude::{col, lit};

    use super::*;

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
                plain.clone().eq(lit("N2")).and(generation.eq(lit("N1:2"))),
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
            (plain.in_list(vec![lit("N1")], true), vec![1, 2, 3]),
        ] {
            let filters =
                selection::physical_filters(std::slice::from_ref(&filter), &schema).unwrap();
            let selected = select_nodes(&filters, nodes.clone()).unwrap();
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
