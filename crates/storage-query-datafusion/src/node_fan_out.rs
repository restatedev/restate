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

use std::any::Any;
use std::fmt::{self, Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::Arc;

use parking_lot::Mutex;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};

use restate_core::Metadata;
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::sharding::KeyRange;
use restate_types::{NodeId, PlainNodeId};

use crate::remote_query_scanner_client::{RemoteScannerService, remote_scan_as_datafusion_stream};
use crate::table_providers::{MeteredStream, ProjectedColumns, Scan};

/// A warning collected from a node that failed during query execution.
#[derive(Debug, Clone)]
pub struct NodeWarning {
    pub node_id: String,
    pub message: String,
}

/// Shared collection of per-node warnings accumulated during fan-out execution.
///
/// Each partition (node) stream that encounters an error will push a warning
/// here instead of propagating the error through DataFusion.
pub type NodeWarnings = Arc<Mutex<Vec<NodeWarning>>>;

/// Determines the set of target nodes for a fan-out query.
pub(crate) trait NodeLocator: Send + Sync + Debug + 'static {
    /// Returns the set of target nodes for this table.
    fn target_nodes(&self) -> anyhow::Result<Vec<TargetNode>>;
}

#[derive(Debug, Clone)]
pub(crate) struct TargetNode {
    pub plain_node_id: PlainNodeId,
    pub node_id: NodeId,
    pub is_local: bool,
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
                node_id: NodeId::from(config.current_generation),
                is_local: config.current_generation == my_node_id,
            })
            .collect())
    }
}

/// Extracts `plain_node_id` filter values from DataFusion predicates.
///
/// The `plain_node_id` column is `Utf8` and uses the `Display` format of
/// [`PlainNodeId`] (e.g. `"N5"`). Literal values are parsed via
/// [`PlainNodeId::from_str`] which accepts both `"N5"` and `"5"`.
///
/// Supports `plain_node_id = 'N5'`, `plain_node_id IN ('N1', 'N2')`, and
/// combinations thereof.
fn extract_node_ids_from_filters(filters: &[Expr]) -> Option<Vec<PlainNodeId>> {
    use datafusion::logical_expr::expr::InList;

    let mut node_ids = Vec::new();

    for filter in filters {
        match filter {
            Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Eq => {
                if let (Expr::Column(col), Expr::Literal(val, _))
                | (Expr::Literal(val, _), Expr::Column(col)) = (&*binary.left, &*binary.right)
                    && col.name == "plain_node_id"
                    && let Some(id) = literal_to_plain_node_id(val)
                {
                    node_ids.push(id);
                }
            }
            Expr::InList(InList {
                expr,
                list,
                negated: false,
            }) => {
                if let Expr::Column(col) = &**expr
                    && col.name == "plain_node_id"
                {
                    for item in list {
                        if let Expr::Literal(val, _) = item
                            && let Some(id) = literal_to_plain_node_id(val)
                        {
                            node_ids.push(id);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    if node_ids.is_empty() {
        None
    } else {
        Some(node_ids)
    }
}

/// Attempts to parse a [`PlainNodeId`] from a DataFusion scalar literal.
///
/// The `plain_node_id` column is `Utf8`, so filter predicates will normally
/// produce `ScalarValue::Utf8`. We also handle numeric scalars as a fallback.
fn literal_to_plain_node_id(val: &datafusion::common::ScalarValue) -> Option<PlainNodeId> {
    match val {
        datafusion::common::ScalarValue::Utf8(Some(s))
        | datafusion::common::ScalarValue::LargeUtf8(Some(s)) => s.parse::<PlainNodeId>().ok(),
        datafusion::common::ScalarValue::UInt32(Some(v)) => Some(PlainNodeId::from(*v)),
        datafusion::common::ScalarValue::Int64(Some(v)) if *v >= 0 && *v <= u32::MAX as i64 => {
            Some(PlainNodeId::from(*v as u32))
        }
        datafusion::common::ScalarValue::UInt64(Some(v)) if *v <= u32::MAX as u64 => {
            Some(PlainNodeId::from(*v as u32))
        }
        _ => None,
    }
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
    remote_scanner: Arc<dyn RemoteScannerService>,
    local_scanner: Option<Arc<dyn Scan>>,
    table_name: String,
    statistics: Statistics,
}

impl NodeFanOutTableProvider {
    pub fn new(
        schema: SchemaRef,
        node_locator: Arc<dyn NodeLocator>,
        remote_scanner: Arc<dyn RemoteScannerService>,
        local_scanner: Option<Arc<dyn Scan>>,
        table_name: impl Into<String>,
    ) -> Self {
        let statistics = Statistics::new_unknown(&schema);
        Self {
            schema,
            node_locator,
            remote_scanner,
            local_scanner,
            table_name: table_name.into(),
            statistics,
        }
    }
}

#[async_trait]
impl datafusion::catalog::TableProvider for NodeFanOutTableProvider {
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

        let target_nodes = self.node_locator.target_nodes().map_err(|e| {
            DataFusionError::Internal(format!("Failed to locate target nodes: {e}"))
        })?;

        // Apply plain_node_id predicate pushdown
        let filtered_nodes = match extract_node_ids_from_filters(filters) {
            Some(target_ids) => target_nodes
                .into_iter()
                .filter(|n| target_ids.contains(&n.plain_node_id))
                .collect(),
            None => target_nodes,
        };

        Ok(Arc::new(NodeFanOutExecutionPlan::new(
            projected_schema,
            filtered_nodes,
            self.remote_scanner.clone(),
            self.local_scanner.clone(),
            self.table_name.clone(),
            filters.to_vec(),
            limit,
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
    remote_scanner: Arc<dyn RemoteScannerService>,
    local_scanner: Option<Arc<dyn Scan>>,
    table_name: String,
    filters: Vec<Expr>,
    limit: Option<usize>,
    plan_properties: PlanProperties,
    statistics: Statistics,
    metrics: ExecutionPlanMetricsSet,
    node_warnings: NodeWarnings,
}

impl NodeFanOutExecutionPlan {
    #[allow(clippy::too_many_arguments)]
    fn new(
        projected_schema: SchemaRef,
        target_nodes: Vec<TargetNode>,
        remote_scanner: Arc<dyn RemoteScannerService>,
        local_scanner: Option<Arc<dyn Scan>>,
        table_name: String,
        filters: Vec<Expr>,
        limit: Option<usize>,
        statistics: Statistics,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(projected_schema.clone());
        let num_partitions = target_nodes.len().max(1);

        let plan_properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            projected_schema,
            target_nodes,
            remote_scanner,
            local_scanner,
            table_name,
            filters,
            limit,
            plan_properties,
            statistics,
            metrics: ExecutionPlanMetricsSet::new(),
            node_warnings: Arc::new(Mutex::new(Vec::new())),
        }
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
            let inner = remote_scan_as_datafusion_stream(
                self.remote_scanner.clone(),
                target.node_id,
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
            node_label,
            self.node_warnings.clone(),
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

impl DisplayAs for NodeFanOutExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "NodeFanOutExecutionPlan: table={}, target_nodes=[{}], projection=[{}]",
                    self.table_name,
                    NodeList(&self.target_nodes),
                    ProjectedColumns(&self.projected_schema),
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
            write!(f, "{}", node.plain_node_id)?;
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
    node_label: String,
    warnings: NodeWarnings,
    done: bool,
}

impl ErrorCatchingStream {
    fn new(inner: SendableRecordBatchStream, node_label: String, warnings: NodeWarnings) -> Self {
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
                    message: err.to_string(),
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
    use super::*;

    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Operator;
    use datafusion::logical_expr::expr::InList;

    /// Helper: build `plain_node_id = <literal>`.
    fn eq_filter(val: ScalarValue) -> Expr {
        Expr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "plain_node_id",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(val, None)),
        })
    }

    /// Helper: build `plain_node_id IN (<literals>)`.
    fn in_list_filter(vals: Vec<ScalarValue>) -> Expr {
        Expr::InList(InList {
            expr: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "plain_node_id",
            ))),
            list: vals.into_iter().map(|v| Expr::Literal(v, None)).collect(),
            negated: false,
        })
    }

    #[test]
    fn extract_utf8_equality() {
        // `plain_node_id = 'N5'` — the standard Utf8 format produced by Display
        let filters = vec![eq_filter(ScalarValue::Utf8(Some("N5".into())))];
        let result = extract_node_ids_from_filters(&filters);
        assert_eq!(result, Some(vec![PlainNodeId::from(5)]));

        // `plain_node_id = '42'` — bare numeric string (also accepted by FromStr)
        let filters = vec![eq_filter(ScalarValue::Utf8(Some("42".into())))];
        let result = extract_node_ids_from_filters(&filters);
        assert_eq!(result, Some(vec![PlainNodeId::from(42)]));
    }

    #[test]
    fn extract_utf8_in_list() {
        let filters = vec![in_list_filter(vec![
            ScalarValue::Utf8(Some("N1".into())),
            ScalarValue::Utf8(Some("N3".into())),
            ScalarValue::Utf8(Some("N7".into())),
        ])];
        let result = extract_node_ids_from_filters(&filters);
        assert_eq!(
            result,
            Some(vec![
                PlainNodeId::from(1),
                PlainNodeId::from(3),
                PlainNodeId::from(7),
            ])
        );
    }

    #[test]
    fn extract_numeric_fallback() {
        // UInt32 literal (fallback path, not typical for Utf8 columns)
        let filters = vec![eq_filter(ScalarValue::UInt32(Some(10)))];
        let result = extract_node_ids_from_filters(&filters);
        assert_eq!(result, Some(vec![PlainNodeId::from(10)]));

        // Int64 literal
        let filters = vec![eq_filter(ScalarValue::Int64(Some(99)))];
        let result = extract_node_ids_from_filters(&filters);
        assert_eq!(result, Some(vec![PlainNodeId::from(99)]));
    }

    #[test]
    fn extract_returns_none_for_empty_or_unrelated_filters() {
        // No filters at all
        assert_eq!(extract_node_ids_from_filters(&[]), None);

        // Filter on a different column
        let other_col = Expr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr {
            left: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "other_column",
            ))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some("N5".into())), None)),
        });
        assert_eq!(extract_node_ids_from_filters(&[other_col]), None);

        // Unparseable string value
        let bad = eq_filter(ScalarValue::Utf8(Some("not-a-node".into())));
        assert_eq!(extract_node_ids_from_filters(&[bad]), None);

        // Negated IN list should be ignored
        let negated = Expr::InList(InList {
            expr: Box::new(Expr::Column(datafusion::common::Column::new_unqualified(
                "plain_node_id",
            ))),
            list: vec![Expr::Literal(ScalarValue::Utf8(Some("N1".into())), None)],
            negated: true,
        });
        assert_eq!(extract_node_ids_from_filters(&[negated]), None);
    }

    #[test]
    fn extract_combines_multiple_filters() {
        // Two separate equality filters are combined into a single Vec
        let filters = vec![
            eq_filter(ScalarValue::Utf8(Some("N2".into()))),
            eq_filter(ScalarValue::Utf8(Some("N8".into()))),
        ];
        let result = extract_node_ids_from_filters(&filters);
        assert_eq!(
            result,
            Some(vec![PlainNodeId::from(2), PlainNodeId::from(8)])
        );
    }
}
