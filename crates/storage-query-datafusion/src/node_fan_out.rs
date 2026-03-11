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
//! RPC) and streams Arrow record batches back. The `node_id` column enables
//! predicate pushdown so queries can target specific nodes.

use std::any::Any;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
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

use restate_core::Metadata;
use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::nodes_config::Role;
use restate_types::{NodeId, PlainNodeId};

use crate::remote_query_scanner_client::{RemoteScannerService, remote_scan_as_datafusion_stream};
use crate::table_providers::{MeteredStream, ProjectedColumns, Scan};

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

/// Extracts `node_id` filter values from DataFusion predicates.
///
/// Supports `node_id = X`, `node_id IN (X, Y)`, and ORs thereof.
fn extract_node_ids_from_filters(filters: &[Expr]) -> Option<Vec<PlainNodeId>> {
    use datafusion::logical_expr::expr::InList;

    let mut node_ids = Vec::new();

    for filter in filters {
        match filter {
            Expr::BinaryExpr(binary) if binary.op == datafusion::logical_expr::Operator::Eq => {
                if let (Expr::Column(col), Expr::Literal(val, _))
                | (Expr::Literal(val, _), Expr::Column(col)) = (&*binary.left, &*binary.right)
                    && col.name == "node_id"
                    && let Some(id) = literal_to_u32(val)
                {
                    node_ids.push(PlainNodeId::from(id));
                }
            }
            Expr::InList(InList {
                expr,
                list,
                negated: false,
            }) => {
                if let Expr::Column(col) = &**expr
                    && col.name == "node_id"
                {
                    for item in list {
                        if let Expr::Literal(val, _) = item
                            && let Some(id) = literal_to_u32(val)
                        {
                            node_ids.push(PlainNodeId::from(id));
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

fn literal_to_u32(val: &datafusion::common::ScalarValue) -> Option<u32> {
    match val {
        datafusion::common::ScalarValue::UInt32(Some(v)) => Some(*v),
        datafusion::common::ScalarValue::Int64(Some(v)) if *v >= 0 && *v <= u32::MAX as i64 => {
            Some(*v as u32)
        }
        datafusion::common::ScalarValue::UInt64(Some(v)) if *v <= u32::MAX as u64 => {
            Some(*v as u32)
        }
        _ => None,
    }
}

/// A DataFusion [`TableProvider`] that fans out scans to multiple nodes.
///
/// Each target node (determined by [`NodeLocator`]) becomes a logical
/// partition in the execution plan. The `node_id` column allows predicate
/// pushdown to skip nodes not matching the filter.
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

        // Apply node_id predicate pushdown
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
/// Each logical partition corresponds to one target node.
#[derive(Debug, Clone)]
struct NodeFanOutExecutionPlan {
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
        }
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

        if target.is_local
            && let Some(local_scanner) = &self.local_scanner
        {
            let inner = local_scanner.scan(
                self.projected_schema.clone(),
                &self.filters,
                batch_size,
                self.limit,
            );

            return Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    self.projected_schema.clone(),
                    MeteredStream {
                        inner,
                        baseline_metrics,
                    },
                ),
            ));
        }

        // Remote scan: use a sentinel partition_id since this is a node-level table
        let inner = remote_scan_as_datafusion_stream(
            self.remote_scanner.clone(),
            target.node_id,
            PartitionId::MIN,
            RangeInclusive::new(PartitionKey::MIN, PartitionKey::MAX),
            self.table_name.clone(),
            self.projected_schema.clone(),
            None, // predicate is applied locally after combining
            batch_size,
            self.limit,
        );

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.projected_schema.clone(),
                MeteredStream {
                    inner,
                    baseline_metrics,
                },
            ),
        ))
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
            write!(f, "N{}", node.plain_node_id)?;
            if node.is_local {
                write!(f, "(local)")?;
            }
            first = false;
        }
        Ok(())
    }
}
