// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `partition_state`: each partition processor's state, asked from the workers.
//!
//! At planning, the coordinator captures which replicas it expects (its partition
//! table and observed memberships) and which workers it asks (every worker in its
//! nodes configuration, except those its gossip sees as dead at that generation).
//! At execution, it collects every asked worker's report, each within the same
//! bound, and only then emits one row per expected replica, report or not:
//!
//! | partition_id | plain_node_id | report              | effective_mode |
//! |--------------|---------------|---------------------|----------------|
//! | 0            | N1            | `reported`          | leader         |
//! | 0            | N2            | `node_unavailable`  | NULL           |
//! | 1            | N1            | `not_reported`      | NULL           |
//! | 2            | NULL          | `no_known_replicas` | NULL           |
//!
//! Here N2 didn't answer in time (the query carries a warning for it), N1's
//! complete report has no entry for partition 1, and partition 2 has no known
//! replicas. A report for a partition or node the coordinator didn't expect is
//! still a `reported` row.

use std::collections::{BTreeMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, AsArray, RecordBatch, RecordBatchOptions, StringArray, UInt32Array, new_null_array,
};
use datafusion::arrow::compute::{concat_batches, take};
use datafusion::arrow::datatypes::{SchemaRef, UInt32Type};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, Statistics, internal_datafusion_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;

use restate_core::Metadata;
use restate_platform::sync::Mutex;
use restate_storage_query_api::tables::partition_state::PartitionState;
use restate_storage_query_api::{NodeWarning, NodeWarnings};
use restate_types::cluster::cluster_state::PartitionProcessorStatus;
use restate_types::cluster_state::{ClusterState, NodeState};
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::net::connect_opts::CommonClientConnectionOptions;
use restate_types::nodes_config::Role;
use restate_types::partitions::state::PartitionReplicaSetStates;
use restate_types::{GenerationalNodeId, PlainNodeId};
use restate_util_string::{ReString, ToReString};
use restate_worker_api::ProcessorsManagerHandle;

use crate::QuerySource;
use crate::node_fan_out::{NodeFanOutExecutionPlan, TargetNode};
use crate::source_registry::SourceRegistry;
use crate::table_providers::Scan;

crate::tables::table_adapter!(pub(crate) PartitionStateTable, PartitionStateBuilder, PartitionState);

/// What one worker reports: the `partition_state` columns except `report`. No SQL
/// table exposes it; `partition_state` asks every worker for it.
#[derive(Debug, Clone)]
pub(crate) struct PartitionStateReports;

impl QuerySource for PartitionStateReports {
    type Schema = PartitionState;

    fn identity() -> ReString {
        ReString::from_static("partition_state_reports")
    }
}

const REPORTED: &str = "reported";
const NODE_UNAVAILABLE: &str = "node_unavailable";
const NOT_REPORTED: &str = "not_reported";
const NO_KNOWN_REPLICAS: &str = "no_known_replicas";

pub(crate) fn reports_schema() -> SchemaRef {
    let schema = PartitionState::arrow_schema();
    let columns: Vec<_> = (0..schema.fields().len())
        .filter(|&i| schema.field(i).name() != "report")
        .collect();
    Arc::new(schema.project(&columns).expect("report columns exist"))
}

/// How long the coordinator waits for each worker's report, and each worker for
/// its partition processor manager: the networking keep-alive timeout, the same
/// bound the cluster controller gives the workers' state.
fn report_timeout() -> Duration {
    Configuration::pinned().networking.keep_alive_timeout()
}

impl PartitionStateTable {
    pub(crate) fn create_provider(
        metadata: Metadata,
        sources: SourceRegistry,
        cluster_state: ClusterState,
        replica_set_states: PartitionReplicaSetStates,
    ) -> Arc<dyn TableProvider> {
        Arc::new(PartitionStateProvider {
            metadata,
            sources,
            cluster_state,
            replica_set_states,
        })
    }

    /// The worker's side: its partition processor manager's report, with one row
    /// per partition the manager has a processor status for.
    pub(crate) fn create_reports_scanner(
        processors: ProcessorsManagerHandle,
        metadata: Metadata,
    ) -> Arc<dyn Scan> {
        Arc::new(ReportsScanner {
            processors,
            metadata,
        })
    }
}

struct PartitionStateProvider {
    metadata: Metadata,
    sources: SourceRegistry,
    /// This coordinator's gossip view, to skip workers it sees as dead.
    cluster_state: ClusterState,
    /// The memberships this coordinator has observed.
    replica_set_states: PartitionReplicaSetStates,
}

impl Debug for PartitionStateProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionStateProvider").finish()
    }
}

#[async_trait]
impl TableProvider for PartitionStateProvider {
    fn schema(&self) -> SchemaRef {
        PartitionState::arrow_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Filters and limits apply to the reconciled rows, above this scan.
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(projection) => Arc::new(PartitionState::arrow_schema().project(projection)?),
            None => PartitionState::arrow_schema(),
        };
        let expected = Expected::capture(&self.metadata, &self.replica_set_states);
        let (targets, mut not_asked) = worker_targets(&self.metadata, &self.cluster_state);
        for node in expected.partitions.iter().flat_map(|(_, nodes)| nodes) {
            if !targets.iter().any(|target| target.plain_node_id == *node) {
                not_asked.entry(*node).or_insert(NotAsked::NotAWorker);
            }
        }
        let fan_out = if targets.is_empty() {
            None
        } else {
            let distributed = state
                .config()
                .get_extension::<crate::distributed::DistributedExecution>()
                .is_some();
            Some(Arc::new(NodeFanOutExecutionPlan::for_nodes(
                reports_schema(),
                PartitionStateReports::identity(),
                targets,
                &self.sources,
                distributed,
            )?))
        };
        Ok(Arc::new(PartitionStateExec::new(
            schema,
            Arc::new(Inventory {
                partitions: expected.partitions,
                not_asked,
            }),
            fan_out,
            report_timeout(),
        )))
    }
}

/// The replicas this coordinator expects, by partition.
struct Expected {
    /// Each partition of the partition table with the plain IDs of its observed
    /// current and next members; a node in both appears once.
    partitions: Vec<(PartitionId, Vec<PlainNodeId>)>,
}

impl Expected {
    fn capture(metadata: &Metadata, replica_set_states: &PartitionReplicaSetStates) -> Self {
        let partition_table = metadata.partition_table_snapshot();
        let partitions = partition_table
            .iter()
            .map(|(id, _)| {
                let membership = replica_set_states.membership_state(*id);
                let mut nodes: Vec<PlainNodeId> = Vec::new();
                let next = membership.observed_next_membership.iter();
                for member in membership
                    .observed_current_membership
                    .members
                    .iter()
                    .chain(next.flat_map(|next| next.members.iter()))
                {
                    if !nodes.contains(&member.node_id) {
                        nodes.push(member.node_id);
                    }
                }
                (*id, nodes)
            })
            .collect();
        Self { partitions }
    }
}

/// The workers to ask, at their current generation, and the workers skipped
/// because gossip sees that generation as dead. A node gossip doesn't know, or
/// knows at another generation, is asked: owner validation then decides.
fn worker_targets(
    metadata: &Metadata,
    cluster_state: &ClusterState,
) -> (Vec<TargetNode>, BTreeMap<PlainNodeId, NotAsked>) {
    let nodes_config = metadata.nodes_config_snapshot();
    let my_node_id = metadata.my_node_id();
    let mut targets = Vec::new();
    let mut not_asked = BTreeMap::new();
    for (plain_node_id, config) in nodes_config.iter_role(Role::Worker) {
        let node_id = config.current_generation;
        if cluster_state.get_node_state_and_generation(plain_node_id)
            == Some((node_id, NodeState::Dead))
        {
            not_asked.insert(plain_node_id, NotAsked::GossipDead);
        } else {
            targets.push(TargetNode {
                plain_node_id,
                node_id,
                is_local: node_id == my_node_id,
            });
        }
    }
    targets.sort_unstable_by_key(|target| target.plain_node_id);
    (targets, not_asked)
}

#[derive(Debug, Clone, Copy)]
enum NotAsked {
    GossipDead,
    /// An expected member that isn't a worker in the nodes configuration.
    NotAWorker,
}

impl NotAsked {
    fn message(self) -> &'static str {
        match self {
            NotAsked::GossipDead => "skipped: gossip reports the node dead",
            NotAsked::NotAWorker => "not asked: not a worker in the nodes configuration",
        }
    }
}

/// What planning captured; executions only read it.
#[derive(Debug)]
struct Inventory {
    partitions: Vec<(PartitionId, Vec<PlainNodeId>)>,
    not_asked: BTreeMap<PlainNodeId, NotAsked>,
}

/// Collects the workers' reports and reconciles them with the expected replicas.
///
/// It holds the fan-out asking the workers, built at planning, rather than having
/// it as a child: optimizer rules may wrap children, and this plan reads each
/// worker's stream itself. The collection belongs to the stream `execute`
/// returns: dropping that stream stops it, even while the plan is kept, for
/// example by query diagnostics.
#[derive(Debug)]
pub(crate) struct PartitionStateExec {
    schema: SchemaRef,
    inventory: Arc<Inventory>,
    /// The fan-out asking every worker; `None` when there is none to ask.
    fan_out: Option<Arc<NodeFanOutExecutionPlan>>,
    /// How long each worker's report may take, from the start of the collection.
    timeout: Duration,
    properties: Arc<PlanProperties>,
    /// Workers that weren't asked or gave no complete report, appended once the
    /// collection ends.
    warnings: NodeWarnings,
}

impl PartitionStateExec {
    fn new(
        schema: SchemaRef,
        inventory: Arc<Inventory>,
        fan_out: Option<Arc<NodeFanOutExecutionPlan>>,
        timeout: Duration,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            schema,
            inventory,
            fan_out,
            timeout,
            properties: Arc::new(properties),
            warnings: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn node_warnings(&self) -> &NodeWarnings {
        &self.warnings
    }
}

impl ExecutionPlan for PartitionStateExec {
    fn name(&self) -> &str {
        "PartitionStateExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("PartitionStateExec has no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("PartitionStateExec has one partition, not {partition}");
        }
        // Start every worker's request; the stream below drives them.
        let mut nodes = Vec::new();
        if let Some(fan_out) = &self.fan_out {
            for (i, node) in fan_out.target_nodes().iter().enumerate() {
                nodes.push((node.clone(), fan_out.execute_node(i, Arc::clone(&context))?));
            }
        }
        let schema = Arc::clone(&self.schema);
        let inventory = Arc::clone(&self.inventory);
        let warnings = Arc::clone(&self.warnings);
        let timeout = self.timeout;
        let output = futures::stream::once(async move {
            let deadline = tokio::time::Instant::now() + timeout;
            let reports = futures::future::join_all(
                nodes
                    .into_iter()
                    .map(|(node, stream)| collect(node, stream, deadline, timeout)),
            )
            .await;
            reconcile(&schema, &inventory, &reports, &warnings)
        })
        .filter_map(|batch| async move { batch.transpose() });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            output,
        )))
    }

    fn partition_statistics(&self, _: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

impl DisplayAs for PartitionStateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        let asked = AskedList(self.fan_out.as_deref());
        let not_asked = NotAskedList(&self.inventory.not_asked);
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "PartitionStateExec: partitions={}, asked=[{asked}], not_asked=[{not_asked}], timeout={:?}",
                self.inventory.partitions.len(),
                self.timeout,
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "partitions={}", self.inventory.partitions.len())?;
                writeln!(f, "asked=[{asked}]")?;
                writeln!(f, "not_asked=[{not_asked}]")?;
                writeln!(f, "timeout={:?}", self.timeout)
            }
        }
    }
}

struct AskedList<'a>(Option<&'a NodeFanOutExecutionPlan>);

impl fmt::Display for AskedList<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let nodes = self.0.map(NodeFanOutExecutionPlan::target_nodes);
        for (i, node) in nodes.unwrap_or_default().iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", node.node_id)?;
            if node.is_local {
                write!(f, "(local)")?;
            }
        }
        Ok(())
    }
}

struct NotAskedList<'a>(&'a BTreeMap<PlainNodeId, NotAsked>);

impl fmt::Display for NotAskedList<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (i, (node, reason)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            let reason = match reason {
                NotAsked::GossipDead => "gossip dead",
                NotAsked::NotAWorker => "not a worker",
            };
            write!(f, "{node} ({reason})")?;
        }
        Ok(())
    }
}

/// One worker's answer: the rows it sent, and why its report is incomplete, if it is.
struct NodeReport {
    node: TargetNode,
    batches: Vec<RecordBatch>,
    failure: Option<String>,
}

async fn collect(
    node: TargetNode,
    mut stream: SendableRecordBatchStream,
    deadline: tokio::time::Instant,
    timeout: Duration,
) -> NodeReport {
    let mut batches = Vec::new();
    let failure = loop {
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(batch))) => batches.push(batch),
            Ok(Some(Err(err))) => break Some(err.to_string()),
            Ok(None) => break None,
            Err(_) => break Some(format!("no complete report within {timeout:?}")),
        }
    };
    NodeReport {
        node,
        batches,
        failure,
    }
}

/// The output rows: one per expected replica, with its report if there is one,
/// then the reports nobody expected.
#[derive(Default)]
struct Rows {
    partition_id: Vec<u32>,
    node: Vec<Option<PlainNodeId>>,
    /// The row of the reports holding this row's report.
    report_row: Vec<Option<u32>>,
    report: Vec<&'static str>,
}

impl Rows {
    fn push(
        &mut self,
        partition: u32,
        node: Option<PlainNodeId>,
        report_row: Option<u32>,
        report: &'static str,
    ) {
        self.partition_id.push(partition);
        self.node.push(node);
        self.report_row.push(report_row);
        self.report.push(report);
    }
}

fn reconcile(
    schema: &SchemaRef,
    inventory: &Inventory,
    reports: &[NodeReport],
    warnings: &NodeWarnings,
) -> Result<Option<RecordBatch>> {
    {
        let mut warnings = warnings.lock();
        for (node, reason) in &inventory.not_asked {
            warnings.push(NodeWarning {
                node_id: node.to_restring(),
                message: ReString::from_static(reason.message()),
            });
        }
        for report in reports {
            if let Some(failure) = &report.failure {
                warnings.push(NodeWarning {
                    node_id: report.node.plain_node_id.to_restring(),
                    message: failure.as_str().into(),
                });
            }
        }
    }

    let values = concat_batches(
        &reports_schema(),
        reports.iter().flat_map(|report| &report.batches),
    )?;
    let partition_ids = values
        .column_by_name("partition_id")
        .ok_or_else(|| internal_datafusion_err!("reports lack partition_id"))?
        .as_primitive::<UInt32Type>();
    // Each partition's reports, as (node, row).
    let mut reported: BTreeMap<u32, Vec<(PlainNodeId, u32)>> = BTreeMap::new();
    let mut row = 0u32;
    for report in reports {
        for batch in &report.batches {
            for _ in 0..batch.num_rows() {
                reported
                    .entry(partition_ids.value(row as usize))
                    .or_default()
                    .push((report.node.plain_node_id, row));
                row += 1;
            }
        }
    }
    let complete: HashSet<PlainNodeId> = reports
        .iter()
        .filter(|report| report.failure.is_none())
        .map(|report| report.node.plain_node_id)
        .collect();

    let mut rows = Rows::default();
    for (partition, expected) in &inventory.partitions {
        let partition = u32::from(*partition);
        let mut reports = reported.remove(&partition).unwrap_or_default();
        if expected.is_empty() && reports.is_empty() {
            rows.push(partition, None, None, NO_KNOWN_REPLICAS);
        }
        for node in expected {
            match reports.iter().position(|(reporter, _)| reporter == node) {
                Some(i) => {
                    let (_, row) = reports.remove(i);
                    rows.push(partition, Some(*node), Some(row), REPORTED);
                }
                None if complete.contains(node) => {
                    rows.push(partition, Some(*node), None, NOT_REPORTED)
                }
                None => rows.push(partition, Some(*node), None, NODE_UNAVAILABLE),
            }
        }
        for (node, row) in reports {
            rows.push(partition, Some(node), Some(row), REPORTED);
        }
    }
    // Reports of partitions this coordinator's partition table doesn't have.
    for (partition, reports) in reported {
        for (node, row) in reports {
            rows.push(partition, Some(node), Some(row), REPORTED);
        }
    }
    if rows.report.is_empty() {
        return Ok(None);
    }

    let report_rows = UInt32Array::from(rows.report_row);
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef> {
            Ok(match field.name().as_str() {
                "partition_id" => Arc::new(UInt32Array::from(rows.partition_id.clone())),
                "plain_node_id" => Arc::new(StringArray::from_iter(
                    rows.node
                        .iter()
                        .map(|node| node.map(|node| node.to_string())),
                )),
                "report" => Arc::new(StringArray::from_iter_values(rows.report.iter())),
                name => {
                    let column = values
                        .column_by_name(name)
                        .ok_or_else(|| internal_datafusion_err!("reports lack {name}"))?;
                    if column.is_empty() {
                        new_null_array(field.data_type(), report_rows.len())
                    } else {
                        take(column, &report_rows, None)?
                    }
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(report_rows.len())),
    )?))
}

/// A worker's partition processor manager, asked for its report.
struct ReportsScanner {
    processors: ProcessorsManagerHandle,
    metadata: Metadata,
}

impl Debug for ReportsScanner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReportsScanner").finish()
    }
}

impl Scan for ReportsScanner {
    fn scan(
        &self,
        projection: SchemaRef,
        _filters: &[Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        let processors = self.processors.clone();
        let node_id = self.metadata.my_node_id();
        let timeout = report_timeout();
        let mut stream = RecordBatchReceiverStream::builder(Arc::clone(&projection), 2);
        let tx = stream.tx();
        stream.spawn(async move {
            // A manager that is alive but stuck fails this scan on its own, even if
            // the coordinator's cancellation never arrives.
            let state = match tokio::time::timeout(timeout, processors.get_state()).await {
                Ok(Ok(state)) => state,
                Ok(Err(err)) => {
                    return Err(datafusion::error::DataFusionError::External(err.into()));
                }
                Err(_) => {
                    return datafusion::common::exec_err!(
                        "the partition processor manager didn't answer within {timeout:?}"
                    );
                }
            };
            let mut builder = PartitionStateBuilder::new(projection);
            for (partition, status) in state.iter().take(limit.unwrap_or(usize::MAX)) {
                append_partition_row(&mut builder, node_id, *partition, status);
                if builder.num_rows() >= batch_size && tx.send(builder.finish()).await.is_err() {
                    return Ok(());
                }
            }
            if builder.num_rows() > 0 {
                let _ = tx.send(builder.into_batch()).await;
            }
            Ok(())
        });
        stream.build()
    }
}

fn append_partition_row(
    builder: &mut PartitionStateBuilder,
    node_id: GenerationalNodeId,
    partition_id: PartitionId,
    state: &PartitionProcessorStatus,
) {
    let mut row = builder.row();
    row.partition_id(u32::from(partition_id));
    row.fmt_plain_node_id(node_id.as_plain());
    row.fmt_gen_node_id(node_id);
    row.fmt_target_mode(state.planned_mode);
    row.fmt_effective_mode(state.effective_mode());
    row.updated_at(state.updated_at.as_u64() as i64);
    if let Some(epoch) = state.last_observed_leader_epoch {
        row.leader_epoch(u64::from(epoch));
    }
    if let Some(leader) = &state.last_observed_leader_node {
        row.fmt_leader(leader);
    }
    if let Some(lsn) = state.last_applied_log_lsn {
        row.applied_log_lsn(u64::from(lsn));
    }
    if let Some(ts) = state.last_record_applied_at {
        row.last_record_applied_at(ts.as_u64() as i64);
    }
    if state.is_broken() {
        row.fmt_broken_reason(state.broken_reason);
    } else {
        row.fmt_replay_status(state.replay_status);
    }
    if let Some(lsn) = state.durable_lsn {
        row.durable_log_lsn(u64::from(lsn));
    }
    if let Some(lsn) = state.last_archived_log_lsn {
        row.archived_log_lsn(u64::from(lsn));
    }
    if let Some(lsn) = state.target_tail_lsn {
        row.target_tail_lsn(u64::from(lsn));
    }
    if let Some(version) = state.last_applied_rule_book_version {
        row.applied_rule_book_version(u32::from(version));
    }
    if let Some(version) = state.last_applied_schema_version {
        row.applied_schema_version(u32::from(version));
    }
    if let Some(lsn) = state.byte_offsets_base_lsn {
        row.byte_offsets_base_lsn(u64::from(lsn));
    }
    if let Some(offset) = state.log_byte_offset {
        row.log_byte_offset(offset);
    }
    if let Some(offset) = state.partition_store_byte_offset {
        row.partition_store_byte_offset(offset);
    }
    if row.is_enabled_features_defined() {
        row.enabled_features(Some(
            state
                .enabled_features
                .enabled_names()
                .map(|name| Some(ReString::from_static(name)))
                .collect(),
        ));
    }
    if row.is_enabled_storage_features_defined() {
        row.enabled_storage_features(Some(
            state
                .enabled_storage_features
                .iter()
                .cloned()
                .map(Some)
                .collect(),
        ));
    }
    if row.is_latest_snapshot_features_defined() {
        row.latest_snapshot_features(Some(
            state
                .latest_snapshot_features
                .enabled_names()
                .map(ReString::from_static)
                .collect(),
        ));
    }
}
