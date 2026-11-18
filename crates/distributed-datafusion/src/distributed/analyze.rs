// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! What the parts of a task's plan that actually ran did, for `EXPLAIN
//! ANALYZE`.
//!
//! A [`PartitionReadExec`] runs a fresh copy of the plan below it for every
//! storage partition it reads: DataFusion's reset gives operators such as
//! `FilterExec` and `SortExec` new metric sets, so the plan's own nodes, the
//! *template*, never see those numbers. When a task was asked for a report,
//! each copy's metrics are merged into an [`AnalyzeCollector`] by the template
//! node they belong to as soon as the copy's stream is gone, so nothing runs its
//! operators any more, and then the copy is dropped. It must not outlive its
//! stream: the copy can hold its partition's storage open. The copy counts as
//! settled once that storage is released:
//!
//! ```text
//! template                     copy for partition 3          copy for partition 7
//! FilterExec ◀── merged ────── FilterExec (rows=12) ──┐      FilterExec (rows=5)
//!   TableScanExec (shared) ─── TableScanExec ─ same node ─── TableScanExec
//! ```
//!
//! A node the copy shares with the template, such as a scan leaf that returns
//! itself on reset, already holds every copy's numbers: it is skipped, as is
//! any metric a copy shares with its template, so nothing is counted twice. A
//! template node that occurs at two places in the plan holds both places'
//! numbers: the report gives it none, and counts it as ambiguous. Groups merge
//! as DataFusion merges metrics of the same kind, name, labels and partition:
//! counts, times and gauges add, timestamps keep the earliest start and the
//! latest end.
//!
//! The collector holds at most one task report's budget of groups: a group
//! that doesn't fit is counted as left out. Once frozen for the report, later
//! merges change nothing.
//!
//! [`PartitionReadExec`]: super::partition_reads::PartitionReadExec

use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use datafusion::physical_plan::metrics::{Count, MetricCategory, MetricType, MetricValue, Time};
use datafusion::physical_plan::{ExecutionPlan, Metric};
use tokio::sync::Notify;
use tokio::time::Instant;

use restate_platform::sync::Mutex;
use restate_types::GenerationalNodeId;

use restate_types::net::distributed_query::{
    MetricRecord, NodeMetrics, PlanNodeShape, QueryTaskReport, TaskOutcome,
};

use super::report::{
    Budget, Encoded, Fnv, MAX_BYTES, MAX_LABELS, MAX_NODES, MAX_RECORDS, charge, encode,
};

/// Collects the metrics of a task's executed plan copies, and what the task
/// sent; present in a task's session config only when the task was asked for a
/// report.
#[derive(Debug)]
pub(crate) struct AnalyzeCollector {
    state: Mutex<State>,
    settled: Notify,
    /// When the task was installed.
    installed: Instant,
}

impl Default for AnalyzeCollector {
    fn default() -> Self {
        Self {
            state: Default::default(),
            settled: Default::default(),
            installed: Instant::now(),
        }
    }
}

#[derive(Debug, Default)]
struct State {
    frozen: bool,
    /// Copies registered and not yet settled: running, or releasing their
    /// storage.
    pending: usize,
    groups: Vec<Group>,
    /// The groups by the hash of their key. The key's strings live only in the
    /// group's metric, which the budget charged for.
    index: HashMap<u64, Vec<usize>>,
    budget: Option<Budget>,
    /// Metrics of groups that didn't fit in the budget, of kinds the report
    /// can't carry, or of copies of another shape; a lower bound once a walk
    /// stops at its limit.
    left_out: u32,
    /// Copies that ended without being merged: failed, cancelled or dropped.
    unmerged: u32,
    /// Copies merged after their stream was dropped before its end, say past
    /// a LIMIT: their producers may have run on, so their numbers are partial.
    partial: u32,
    /// Metrics that two executed nodes share, which no node gets.
    ambiguous: u32,
    /// The rows, encoded bytes and batches of the task's replies so far; one
    /// batch at a time, under the same lock as the freeze.
    sent: [usize; 3],
}

#[derive(Debug)]
struct Group {
    /// The template node, by address; the template lives as long as the task.
    node: usize,
    metric: Metric,
}

/// What the collector holds when the report is built.
#[derive(Debug, Default)]
pub(crate) struct Collected {
    /// Merged metrics by template node address.
    pub(crate) by_node: HashMap<usize, Vec<Metric>>,
    pub(crate) left_out: u32,
    /// Copies still running or releasing their storage, and copies that ended
    /// without being merged: their numbers are missing.
    pub(crate) unsettled: usize,
    /// The metrics of the task as a whole.
    pub(crate) task: Vec<Metric>,
    /// Metrics that two executed nodes shared, which no node got.
    pub(crate) ambiguous: u32,
}

/// One executed copy, registered before any of its work starts and settled
/// when dropped: once its storage is released, or when it fails or is
/// cancelled.
pub(crate) struct CopyGuard {
    collector: Arc<AnalyzeCollector>,
    /// The copy's metrics by template node: handles, which don't keep the
    /// copy, or its storage, alive.
    observed: Vec<(usize, Arc<Metric>)>,
    /// Whether its stream reached its end.
    complete: bool,
    merged: bool,
}

impl CopyGuard {
    /// Keeps the handles of `executed`'s metrics, by node of `template`, once
    /// its stream is gone and before it's dropped.
    ///
    /// A metric is kept only if exactly one executed node owns it and no node
    /// of the template does: a metric the template owns counts in the
    /// template's own numbers, and one that two executed nodes share can't be
    /// given to either, so it's counted as ambiguous.
    ///
    /// The walk visits at most [`MAX_NODES`] positions, shared ones included,
    /// and inspects at most [`MAX_OBSERVED`] metrics. What it doesn't reach
    /// counts once, as a lower bound of what's left out. Each node's metrics
    /// come from DataFusion's `metrics()`, which copies that node's list of
    /// handles; that copy is outside these bounds.
    pub(crate) fn observe(
        &mut self,
        template: &Arc<dyn ExecutionPlan>,
        executed: &Arc<dyn ExecutionPlan>,
        complete: bool,
    ) {
        self.complete = complete;
        let mut left_out = 0u32;
        let mut ambiguous = 0u32;
        let mut visited = 0usize;
        // Template nodes, and the executed nodes that aren't the template's own.
        let mut templates = Vec::new();
        let mut copies = Vec::new();
        let mut frontier = vec![(Arc::clone(template), Arc::clone(executed))];
        while let Some((template, executed)) = frontier.pop() {
            visited += 1;
            let (template_children, executed_children) = (template.children(), executed.children());
            if template_children.len() != executed_children.len()
                || template.name() != executed.name()
            {
                // A copy of another shape: its numbers can't be attributed.
                left_out = left_out.saturating_add(1);
                continue;
            }
            if visited + frontier.len() + template_children.len() > MAX_NODES {
                left_out = left_out.saturating_add(1);
                break;
            }
            frontier.extend(
                template_children
                    .into_iter()
                    .cloned()
                    .zip(executed_children.into_iter().cloned()),
            );
            if !Arc::ptr_eq(&template, &executed) {
                copies.push((address(&template), executed));
            }
            templates.push(template);
        }
        // Who owns each metric: the template, or how many executed nodes.
        let mut inspected = 0usize;
        let mut template_owned: HashSet<*const Metric> = HashSet::new();
        'templates: for template in &templates {
            for metric in template.metrics().unwrap_or_default().iter() {
                if inspected == MAX_OBSERVED {
                    left_out = left_out.saturating_add(1);
                    break 'templates;
                }
                inspected += 1;
                template_owned.insert(Arc::as_ptr(metric));
            }
        }
        let mut owned_by_copies: HashMap<*const Metric, u32> = HashMap::new();
        let mut handles = Vec::new();
        'copies: for (node, executed) in &copies {
            let mut own = HashSet::new();
            for metric in executed.metrics().unwrap_or_default().iter() {
                if inspected == MAX_OBSERVED {
                    left_out = left_out.saturating_add(1);
                    break 'copies;
                }
                inspected += 1;
                let pointer = Arc::as_ptr(metric);
                if template_owned.contains(&pointer) || !own.insert(pointer) {
                    continue;
                }
                *owned_by_copies.entry(pointer).or_default() += 1;
                handles.push((*node, Arc::clone(metric)));
            }
        }
        // Ownership is proven only over a complete census: if the walk or the
        // inspection stopped early, a position it didn't reach may share any of
        // these metrics, so none is kept.
        if left_out > 0 {
            left_out = left_out.saturating_add(u32::try_from(handles.len()).unwrap_or(u32::MAX));
            handles.clear();
        }
        for (node, metric) in handles {
            if owned_by_copies[&Arc::as_ptr(&metric)] > 1 {
                ambiguous = ambiguous.saturating_add(1);
            } else {
                self.observed.push((node, metric));
            }
        }
        if left_out > 0 || ambiguous > 0 {
            let mut state = self.collector.state.lock();
            state.left_out = state.left_out.saturating_add(left_out);
            state.ambiguous = state.ambiguous.saturating_add(ambiguous);
        }
    }

    /// Merges what was observed, once the copy's storage is released, and
    /// settles the copy. A copy whose stream didn't reach its end is partial.
    pub(crate) fn finish(mut self) {
        let mut state = self.collector.state.lock();
        if !state.frozen {
            for (node, metric) in std::mem::take(&mut self.observed) {
                state.add(node, &metric);
            }
            if !self.complete {
                state.partial = state.partial.saturating_add(1);
            }
        }
        self.merged = true;
    }
}

impl Drop for CopyGuard {
    fn drop(&mut self) {
        let mut state = self.collector.state.lock();
        state.pending -= 1;
        if !self.merged {
            state.unmerged = state.unmerged.saturating_add(1);
        }
        if state.pending == 0 {
            self.collector.settled.notify_waiters();
        }
    }
}

/// The most metric handles one copy keeps until it's merged.
const MAX_OBSERVED: usize = 4 * MAX_RECORDS;

impl AnalyzeCollector {
    /// Counts a batch of `rows` that the task encoded into a reply, `bytes`
    /// long: counted before the reply is sent, not confirmed delivered.
    pub(crate) fn sent(&self, rows: usize, bytes: usize) {
        let mut state = self.state.lock();
        if !state.frozen {
            for (sent, value) in state.sent.iter_mut().zip([rows, bytes, 1]) {
                *sent = sent.saturating_add(value);
            }
        }
    }

    /// Registers a copy, before any of its work starts.
    pub(crate) fn begin_copy(self: &Arc<Self>) -> CopyGuard {
        self.state.lock().pending += 1;
        CopyGuard {
            collector: Arc::clone(self),
            observed: Vec::new(),
            complete: false,
            merged: false,
        }
    }

    /// Waits until no copy is pending, or until `deadline`. Whether every copy
    /// had settled.
    pub(crate) async fn settle(&self, deadline: Instant) -> bool {
        loop {
            let settled = self.settled.notified();
            if self.state.lock().pending == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, settled).await.is_err() {
                return self.state.lock().pending == 0;
            }
        }
    }

    /// Stops merging and hands out what was merged.
    pub(crate) fn freeze(&self) -> Collected {
        let mut state = self.state.lock();
        state.frozen = true;
        let mut by_node: HashMap<usize, Vec<Metric>> = HashMap::new();
        for group in std::mem::take(&mut state.groups) {
            by_node.entry(group.node).or_default().push(group.metric);
        }
        state.index.clear();
        let sent = |name: &'static str, at: usize, category| {
            let count = Count::new();
            count.add(state.sent[at]);
            Metric::new(
                MetricValue::Count {
                    name: name.into(),
                    count,
                },
                None,
            )
            .with_type(MetricType::Summary)
            .with_category(category)
        };
        let elapsed = Time::new();
        elapsed.add_duration(self.installed.elapsed());
        Collected {
            by_node,
            left_out: state.left_out,
            unsettled: state.pending + state.unmerged as usize + state.partial as usize,
            ambiguous: state.ambiguous,
            task: vec![
                sent("transport_rows", 0, MetricCategory::Rows),
                sent("transport_bytes", 1, MetricCategory::Bytes),
                sent("transport_batches", 2, MetricCategory::Rows),
                Metric::new(
                    MetricValue::Time {
                        name: "task_elapsed".into(),
                        time: elapsed,
                    },
                    None,
                )
                .with_type(MetricType::Summary)
                .with_category(MetricCategory::Timing),
            ],
        }
    }
}

impl State {
    /// Merges `metric` of template node `node` into its group, or starts one if
    /// the budget has room for it. Nothing is copied before that's known.
    fn add(&mut self, node: usize, metric: &Metric) {
        let value = metric.value();
        // Bounded before its strings are hashed, compared or copied: a metric
        // with more labels, or longer strings, than a report could hold is left
        // out unread.
        if matches!(value, MetricValue::Custom { .. })
            || metric.labels().len() > MAX_LABELS
            || value.name().len() > MAX_BYTES
            || metric
                .labels()
                .iter()
                .map(|label| label.name().len() + label.value().len())
                .sum::<usize>()
                > MAX_BYTES
        {
            self.left_out = self.left_out.saturating_add(1);
            return;
        }
        let hash = group_hash(node, metric);
        if let Some(&index) = self.index.get(&hash).and_then(|candidates| {
            candidates
                .iter()
                .find(|&&index| same_group(&self.groups[index], node, metric))
        }) {
            merge(self.groups[index].metric.value_mut(), value);
            return;
        }
        let budget = self.budget.get_or_insert_with(|| Budget::for_task(0));
        if !budget.take(charge(metric)) {
            self.left_out = self.left_out.saturating_add(1);
            return;
        }
        // A snapshot of its own, the group's only copy of the metric's strings:
        // the copy's counters live on with the copy.
        let mut snapshot = value.new_empty();
        merge(&mut snapshot, value);
        let mut snapshot =
            Metric::new_with_labels(snapshot, metric.partition(), metric.labels().to_vec())
                .with_type(metric.metric_type());
        if let Some(category) = metric.metric_category() {
            snapshot = snapshot.with_category(category);
        }
        self.index.entry(hash).or_default().push(self.groups.len());
        self.groups.push(Group {
            node,
            metric: snapshot,
        });
    }
}

/// Adds `from` into `into`, as DataFusion does, except that a zero time adds
/// nothing: DataFusion's own aggregation would count it as a nanosecond.
fn merge(into: &mut MetricValue, from: &MetricValue) {
    let zero_time = match from {
        MetricValue::ElapsedCompute(time) | MetricValue::Time { time, .. } => time.value() == 0,
        _ => false,
    };
    if !zero_time {
        into.aggregate(from);
    }
}

/// The hash of what groups `metric` of node `node`: its kind, name, labels,
/// partition, type and category.
fn group_hash(node: usize, metric: &Metric) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    node.hash(&mut hasher);
    std::mem::discriminant(metric.value()).hash(&mut hasher);
    metric.value().name().hash(&mut hasher);
    for label in metric.labels() {
        label.name().hash(&mut hasher);
        label.value().hash(&mut hasher);
    }
    metric.partition().hash(&mut hasher);
    (metric.metric_type() == MetricType::Summary).hash(&mut hasher);
    metric
        .metric_category()
        .map(|category| category as u8)
        .hash(&mut hasher);
    hasher.finish()
}

/// Whether `metric` of node `node` belongs to `group`.
fn same_group(group: &Group, node: usize, metric: &Metric) -> bool {
    let kept = &group.metric;
    group.node == node
        && std::mem::discriminant(kept.value()) == std::mem::discriminant(metric.value())
        && kept.value().name() == metric.value().name()
        && kept.partition() == metric.partition()
        && kept.metric_type() == metric.metric_type()
        && kept.metric_category() == metric.metric_category()
        && kept.labels().len() == metric.labels().len()
        && kept
            .labels()
            .iter()
            .zip(metric.labels())
            .all(|(kept, label)| kept.name() == label.name() && kept.value() == label.value())
}

/// A plan's nodes in pre-order with their shape entries, taken as the plan is
/// installed, before anything runs: a running node can display more, such as
/// the current bound of a TopK's dynamic filter.
pub(crate) struct Shape {
    nodes: Vec<Arc<dyn ExecutionPlan>>,
    entries: Vec<PlanNodeShape>,
}

/// `plan`'s shape, or `None` if it has more than [`MAX_NODES`] nodes, names
/// longer than a report holds, or a node without an identity ([`identity`]).
/// Nothing grows past those bounds.
pub(crate) fn shape(plan: &Arc<dyn ExecutionPlan>) -> Option<Shape> {
    let mut shape = Shape {
        nodes: Vec::new(),
        entries: Vec::new(),
    };
    let mut names = 0usize;
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        let children = node.children();
        names = names.saturating_add(node.name().len());
        if shape.nodes.len() + stack.len() + children.len() >= MAX_NODES || names > MAX_BYTES {
            return None;
        }
        shape.entries.push(PlanNodeShape {
            name: node.name().into(),
            children: u32::try_from(children.len()).unwrap_or(u32::MAX),
            identity: identity(node.as_ref())?,
        });
        stack.extend(children.into_iter().rev().cloned());
        shape.nodes.push(node);
    }
    Some(shape)
}

/// A task's report: the `shape` of its plan as installed, if it has one, and
/// the metrics of its nodes, within one report's budget. The metrics of a node
/// are what its executed copies merged into `collected` plus its own.
pub(crate) fn build_report(
    shape: Option<&Shape>,
    mut collected: Collected,
    outcome: TaskOutcome,
) -> QueryTaskReport {
    // Room for the report's own fields and its shape's framing.
    let mut budget = Budget::for_task(RESERVED);
    let mut report = QueryTaskReport {
        outcome,
        unsettled_lanes: u32::try_from(collected.unsettled).unwrap_or(u32::MAX),
        ..Default::default()
    };
    report.left_out.metrics = collected.left_out;
    // The task's own metrics first: they're few, and they don't depend on the
    // shape.
    for metric in &collected.task {
        if let Some(record) = record(&mut report, metric, &mut budget) {
            report.task.push(record);
        }
    }
    // Then the shape: without all of it, no node metrics can be attributed.
    let Some(shape) = shape.filter(|shape| {
        shape
            .entries
            .iter()
            .all(|entry| budget.take_bytes(SHAPE_ENTRY.saturating_add(entry.name.len())))
    }) else {
        report.left_out.shape = true;
        return report;
    };
    report.shape.clone_from(&shape.entries);
    let nodes = &shape.nodes;
    // A node that occurs more than once, or a metric that two nodes share,
    // holds the numbers of all of them: none of them can be given any.
    // Each node's own metrics come from DataFusion's `metrics()`, which copies
    // its list of handles: at most `MAX_NODES` such lists, outside the bounds
    // below. Ownership is counted over at most `MAX_OBSERVED` metrics; nodes
    // beyond that get none, and are left out.
    let owned: Vec<_> = nodes
        .iter()
        .map(|node| node.metrics().unwrap_or_default())
        .collect();
    let mut occurrences: HashMap<usize, u32> = HashMap::with_capacity(nodes.len());
    let mut owners: HashMap<*const Metric, u32> = HashMap::new();
    let mut inspected = 0usize;
    let mut counted = 0;
    for (node, own) in nodes.iter().zip(&owned) {
        let count = own.iter().count();
        if inspected + count > MAX_OBSERVED {
            break;
        }
        inspected += count;
        counted += 1;
        *occurrences.entry(address(node)).or_default() += 1;
        for metric in own.iter() {
            *owners.entry(Arc::as_ptr(metric)).or_default() += 1;
        }
    }
    report.ambiguous_nodes = collected.ambiguous;
    // Ownership is proven only over every node: if the census stopped early, a
    // node it didn't reach may share any metric, so no node gets any.
    if counted < nodes.len() {
        report.left_out.nodes = u32::try_from(nodes.len()).unwrap_or(u32::MAX);
        return report;
    }
    for (ordinal, (node, own)) in nodes.iter().zip(&owned).enumerate() {
        let address = address(node);
        let shared = |metric: &Arc<Metric>| owners[&Arc::as_ptr(metric)] > 1;
        if occurrences[&address] > 1 || own.iter().any(shared) {
            report.ambiguous_nodes = report.ambiguous_nodes.saturating_add(1);
            continue;
        }
        let merged = collected.by_node.remove(&address).unwrap_or_default();
        let count = merged.len() + own.iter().count();
        if count == 0 {
            continue;
        }
        if !budget.take_bytes(NODE_ENTRY) {
            report.left_out.nodes = report.left_out.nodes.saturating_add(1);
            report.left_out.metrics = report
                .left_out
                .metrics
                .saturating_add(u32::try_from(count).unwrap_or(u32::MAX));
            continue;
        }
        let metrics = merged
            .iter()
            .chain(own.iter().map(|metric| metric.as_ref()));
        let records = metrics
            .filter_map(|metric| record(&mut report, metric, &mut budget))
            .collect::<Vec<_>>();
        if !records.is_empty() {
            report.nodes.push(NodeMetrics {
                ordinal: u32::try_from(ordinal).unwrap_or(u32::MAX),
                metrics: records,
            });
        }
    }
    report
}

/// `metric` as a record of `report`, if it can carry it within `budget`; if
/// not, the report counts it.
fn record(
    report: &mut QueryTaskReport,
    metric: &Metric,
    budget: &mut Budget,
) -> Option<MetricRecord> {
    match encode(metric, budget) {
        Encoded::Record(record) => return Some(record),
        Encoded::Unsupported => report.unsupported = report.unsupported.saturating_add(1),
        Encoded::OverBudget => report.left_out.metrics = report.left_out.metrics.saturating_add(1),
    }
    None
}

/// What a report keeps free of metric records: its own fields and markers.
const RESERVED: usize = 256;
/// What a shape entry costs at most besides its name: its framing, child count
/// and identity.
const SHAPE_ENTRY: usize = 48;
/// What a node's metric list costs at most besides its records: its framing
/// and ordinal.
const NODE_ENTRY: usize = 24;

/// A node by its address, which identifies it while its plan lives.
pub(crate) fn address(node: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(node) as *const () as usize
}

/// A stable hash of `node`'s one-line display and output schema: its kind and
/// what it shows of its configuration, such as a predicate, an order or a
/// fetch. It tells apart nodes of the same kind in one plan; that the plan is
/// the one the coordinator sent is the report's `plan_digest`. FNV-1a, hashed
/// as it's formatted.
/// At most [`MAX_IDENTITY_BYTES`] of the display are hashed: formatting stops
/// there.
/// `None` if the display is longer: two such nodes can't be told apart by a
/// prefix, so they have no identity, and their plan no shape.
pub(crate) fn identity(node: &dyn ExecutionPlan) -> Option<u64> {
    use std::fmt::Write;
    let mut hash = Capped(Fnv::default(), MAX_IDENTITY_BYTES);
    write!(
        hash,
        "{}{:?}",
        datafusion::physical_plan::displayable(node).one_line(),
        node.schema().fields()
    )
    .ok()?;
    Some(hash.0.0)
}

/// The most bytes of a node's display its identity hashes.
const MAX_IDENTITY_BYTES: usize = 64 * 1024;

/// A hasher that refuses more than its remaining bytes, which ends formatting.
struct Capped(Fnv, usize);

impl std::fmt::Write for Capped {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let Some(rest) = self.1.checked_sub(text.len()) else {
            let fits = text.floor_char_boundary(self.1);
            self.0.write(&text.as_bytes()[..fits]);
            self.1 = 0;
            return Err(std::fmt::Error);
        };
        self.0.write(text.as_bytes());
        self.1 = rest;
        Ok(())
    }
}

/// How long, at most, an `EXPLAIN ANALYZE` waits for its tasks' reports once the
/// first task finishes. One deadline for the whole query, however many tasks.
const REPORT_GRACE: Duration = Duration::from_secs(5);

/// The reports of one `EXPLAIN ANALYZE`'s remote tasks, by stage and task
/// number; present in the coordinator's task context only for such a query.
#[derive(Debug, Default)]
pub(crate) struct ReportRegistry {
    state: Mutex<RegistryState>,
    deadline: OnceLock<Instant>,
    arrived: Notify,
}

#[derive(Debug, Default)]
struct RegistryState {
    frozen: bool,
    tasks: BTreeMap<(usize, usize), TaskReport>,
}

/// What became of one task's report.
#[derive(Debug, Clone)]
pub(crate) enum TaskReport {
    /// Asked for and acknowledged; not arrived yet. The digest of the plan the
    /// task was installed with.
    Pending { plan_digest: u64 },
    /// The worker didn't acknowledge the request: it predates reports.
    Unsupported,
    /// Not received by the deadline, or its close failed.
    Missing,
    /// Received, but malformed, oversized or out of bounds.
    Invalid,
    /// Received, but about another plan than the task was installed with.
    Foreign,
    Received {
        owner: GenerationalNodeId,
        report: Box<QueryTaskReport>,
    },
}

/// What a task's close brought.
pub(crate) enum Arrival {
    Report(GenerationalNodeId, QueryTaskReport),
    Invalid,
    Missing,
}

impl ReportRegistry {
    /// Records that the task `key`, installed with a plan of `plan_digest`, was
    /// asked for a report, and whether its worker acknowledged the request.
    pub(crate) fn installed(&self, key: (usize, usize), acknowledged: bool, plan_digest: u64) {
        let mut state = self.state.lock();
        if !state.frozen {
            state.tasks.insert(
                key,
                if acknowledged {
                    TaskReport::Pending { plan_digest }
                } else {
                    TaskReport::Unsupported
                },
            );
        }
    }

    /// The query's one deadline for reports, set when first asked.
    pub(crate) fn deadline(&self) -> Instant {
        *self.deadline.get_or_init(|| Instant::now() + REPORT_GRACE)
    }

    /// Records what task `key`'s close brought. Ignored once frozen or past the
    /// deadline, or for a task not waiting for its report.
    pub(crate) fn received(&self, key: (usize, usize), arrival: Arrival) {
        let mut state = self.state.lock();
        if state.frozen || Instant::now() > self.deadline() {
            return;
        }
        let Some(entry) = state.tasks.get_mut(&key) else {
            return;
        };
        let TaskReport::Pending { plan_digest } = *entry else {
            return;
        };
        *entry = match arrival {
            Arrival::Report(_, report) if report.plan_digest != plan_digest => TaskReport::Foreign,
            Arrival::Report(owner, report) => TaskReport::Received {
                owner,
                report: Box::new(report),
            },
            Arrival::Invalid => TaskReport::Invalid,
            Arrival::Missing => TaskReport::Missing,
        };
        drop(state);
        self.arrived.notify_waiters();
    }

    /// Waits until no report is pending, or until the deadline, then freezes:
    /// what is still pending is missing, and nothing changes afterwards.
    pub(crate) async fn finish(&self) -> BTreeMap<(usize, usize), TaskReport> {
        let deadline = self.deadline();
        loop {
            let arrived = self.arrived.notified();
            let pending = self
                .state
                .lock()
                .tasks
                .values()
                .any(|report| matches!(report, TaskReport::Pending { .. }));
            if !pending || tokio::time::timeout_at(deadline, arrived).await.is_err() {
                break;
            }
        }
        let mut state = self.state.lock();
        state.frozen = true;
        for report in state.tasks.values_mut() {
            if matches!(report, TaskReport::Pending { .. }) {
                *report = TaskReport::Missing;
            }
        }
        state.tasks.clone()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::VecDeque;
    use std::fmt;

    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::common::{Result, ScalarValue};
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::execution_plan::reset_plan_states;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::metrics::MetricsSet;
    use datafusion::physical_plan::metrics::{
        Count, ExecutionPlanMetricsSet, Label, MetricBuilder,
    };
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, Partitioning, PhysicalExpr, PlanProperties,
        SendableRecordBatchStream, collect,
    };

    use restate_types::net::distributed_query::MetricRecordValue;

    use super::super::report::MAX_BYTES;
    use super::*;

    /// A leaf whose reset copies are new nodes that share its metrics, as a
    /// node could; Restate's storage leaves are their own copies. Each
    /// execution emits the next of its batches.
    #[derive(Debug, Clone)]
    pub(crate) struct Rows {
        properties: Arc<PlanProperties>,
        batches: Arc<Mutex<VecDeque<RecordBatch>>>,
        metrics: ExecutionPlanMetricsSet,
    }

    impl Rows {
        pub(crate) fn plan(runs: &[std::ops::RangeInclusive<i64>]) -> Arc<dyn ExecutionPlan> {
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
            let batches = runs
                .iter()
                .map(|run| {
                    let values = Int64Array::from_iter_values(run.clone());
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap()
                })
                .collect();
            Arc::new(Self {
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                batches: Arc::new(Mutex::new(batches)),
                metrics: ExecutionPlanMetricsSet::new(),
            })
        }
    }

    impl DisplayAs for Rows {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Rows")
        }
    }

    impl ExecutionPlan for Rows {
        fn name(&self) -> &str {
            "Rows"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn apply_expressions(
            &self,
            _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(self.as_ref().clone()))
        }

        fn execute(
            &self,
            partition: usize,
            _: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let batch = self.batches.lock().pop_front().expect("a batch per copy");
            MetricBuilder::new(&self.metrics)
                .output_rows(partition)
                .add(batch.num_rows());
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter([Ok(batch)]),
            )))
        }

        fn metrics(&self) -> Option<MetricsSet> {
            Some(self.metrics.clone_inner())
        }
    }

    /// `x % 2 = 0`, or `x = value` for another predicate of the same kind.
    pub(crate) fn even(value: Option<i64>) -> Arc<dyn PhysicalExpr> {
        let x = Arc::new(Column::new("x", 0)) as Arc<dyn PhysicalExpr>;
        let literal = |value| Arc::new(Literal::new(ScalarValue::Int64(Some(value))));
        let left = match value {
            Some(_) => x,
            None => Arc::new(BinaryExpr::new(x, Operator::Modulo, literal(2))),
        };
        Arc::new(BinaryExpr::new(
            left,
            Operator::Eq,
            literal(value.unwrap_or(0)),
        ))
    }

    fn output_rows(report: &QueryTaskReport, ordinal: u32) -> u64 {
        report
            .nodes
            .iter()
            .filter(|node| node.ordinal == ordinal)
            .flat_map(|node| &node.metrics)
            .map(|record| match record.value {
                MetricRecordValue::OutputRows(rows) => rows,
                _ => 0,
            })
            .sum()
    }

    /// Two partitions' copies of a Filter and a TopK, which reset to fresh
    /// metrics, add up; the leaf they share counts once; a copy cut short is
    /// partial, and one unsettled at the report changes nothing once frozen.
    #[tokio::test]
    async fn copies_add_up_and_shared_nodes_count_once() {
        let leaf = Rows::plan(&[1..=10, 11..=17, 18..=20]);
        let filter = Arc::new(FilterExec::try_new(even(None), Arc::clone(&leaf)).unwrap());
        let order =
            LexOrdering::new([PhysicalSortExpr::new_default(Arc::new(Column::new("x", 0)))])
                .unwrap();
        let template: Arc<dyn ExecutionPlan> =
            Arc::new(SortExec::new(order, filter).with_fetch(Some(2)));
        let context = Arc::new(TaskContext::default());
        let collector = Arc::new(AnalyzeCollector::default());
        let run = async |mut copy: CopyGuard, complete| {
            let executed = reset_plan_states(Arc::clone(&template)).unwrap();
            let rows: usize = collect(Arc::clone(&executed), Arc::clone(&context))
                .await
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum();
            copy.observe(&template, &executed, complete);
            // Only handles outlive the copy, until its storage is released.
            drop(executed);
            copy.finish();
            rows
        };
        // Evens: five, then three; the TopK keeps two of each. The second copy
        // is cut short.
        assert_eq!(run(collector.begin_copy(), true).await, 2);
        assert_eq!(run(collector.begin_copy(), false).await, 2);
        let late = collector.begin_copy();
        assert!(!collector.settle(Instant::now()).await);
        let collected = collector.freeze();
        assert_eq!(collected.unsettled, 2, "one pending, one partial");
        let report = build_report(
            shape(&template).as_ref(),
            collected,
            TaskOutcome::INCOMPLETE,
        );
        assert_eq!(
            report
                .shape
                .iter()
                .map(|node| (node.name.as_str(), node.children))
                .collect::<Vec<_>>(),
            [("SortExec(TopK)", 1), ("FilterExec", 1), ("Rows", 0)]
        );
        assert_eq!(
            (
                report.outcome,
                report.unsettled_lanes,
                report.ambiguous_nodes
            ),
            (TaskOutcome::INCOMPLETE, 2, 0)
        );
        assert_eq!(
            [0, 1, 2].map(|ordinal| output_rows(&report, ordinal)),
            [4, 8, 17]
        );
        // The late copy finishes after the freeze: nothing more is collected.
        assert_eq!(run(late, true).await, 2);
        assert!(collector.freeze().by_node.is_empty());
        assert!(collector.settle(Instant::now()).await);

        // A node at two places, or two nodes sharing their metrics, hold the
        // numbers of both: neither gets any.
        let twin = reset_plan_states(Arc::clone(&leaf)).unwrap();
        for union in [
            UnionExec::try_new(vec![Arc::clone(&leaf), Arc::clone(&leaf)]).unwrap(),
            UnionExec::try_new(vec![Arc::clone(&leaf), twin]).unwrap(),
        ] {
            let report = build_report(
                shape(&union).as_ref(),
                Collected::default(),
                TaskOutcome::COMPLETED,
            );
            assert_eq!(report.ambiguous_nodes, 2);
            assert_eq!([1, 2].map(|ordinal| output_rows(&report, ordinal)), [0, 0]);
        }

        // A plan wider than a report can describe has no shape, and so no node
        // metrics; a copy that wide is walked only up to the limit.
        let wide = UnionExec::try_new((0..MAX_NODES).map(|_| Rows::plan(&[])).collect()).unwrap();
        let report = build_report(
            shape(&wide).as_ref(),
            Collected::default(),
            TaskOutcome::COMPLETED,
        );
        assert!(report.left_out.shape && report.shape.is_empty() && report.nodes.is_empty());
        let collector = Arc::new(AnalyzeCollector::default());
        let mut copy = collector.begin_copy();
        copy.observe(&wide, &reset_plan_states(Arc::clone(&wide)).unwrap(), true);
        copy.finish();
        assert!(collector.freeze().left_out >= 1);
    }

    /// A node with given children and metrics, for plans observed by hand.
    #[derive(Debug)]
    struct Node {
        properties: Arc<PlanProperties>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        metrics: ExecutionPlanMetricsSet,
        label: String,
    }

    impl Node {
        fn plan(
            children: Vec<Arc<dyn ExecutionPlan>>,
            metrics: &ExecutionPlanMetricsSet,
        ) -> Arc<dyn ExecutionPlan> {
            Self::labelled(children, metrics, String::new())
        }

        fn labelled(
            children: Vec<Arc<dyn ExecutionPlan>>,
            metrics: &ExecutionPlanMetricsSet,
            label: String,
        ) -> Arc<dyn ExecutionPlan> {
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
            Arc::new(Self {
                properties: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
                children,
                metrics: metrics.clone(),
                label,
            })
        }
    }

    impl DisplayAs for Node {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Node{}", self.label)
        }
    }

    impl ExecutionPlan for Node {
        fn name(&self) -> &str {
            "Node"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.children.iter().collect()
        }

        fn apply_expressions(
            &self,
            _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Node::plan(children, &self.metrics))
        }

        fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            unimplemented!("observed by hand only")
        }

        fn metrics(&self) -> Option<MetricsSet> {
            Some(self.metrics.clone_inner())
        }
    }

    /// Ownership of copied metrics is checked over every position: a metric
    /// two executed nodes share goes to neither, one a shared template node
    /// owns isn't copied again, and walks stop at their bounds however the
    /// plan shares its nodes.
    #[test]
    fn observed_copies_attribute_each_metric_once_and_stay_bounded() {
        let set = |values: &[usize]| {
            let set = ExecutionPlanMetricsSet::new();
            for (partition, value) in values.iter().enumerate() {
                MetricBuilder::new(&set).output_rows(partition).add(*value);
            }
            set
        };
        let observe = |template: &Arc<dyn ExecutionPlan>, executed: &Arc<dyn ExecutionPlan>| {
            let collector = Arc::new(AnalyzeCollector::default());
            let mut copy = collector.begin_copy();
            copy.observe(template, executed, true);
            copy.finish();
            collector.freeze()
        };
        let rows = |collected: &Collected, node: &Arc<dyn ExecutionPlan>| {
            collected.by_node.get(&address(node)).map_or(0, |metrics| {
                metrics
                    .iter()
                    .map(|metric| metric.value().as_usize())
                    .sum::<usize>()
            })
        };

        // Two distinct executed nodes share one fresh metric: neither gets it.
        // A metric of their own still counts.
        let (left, right) = (Node::plan(vec![], &set(&[])), Node::plan(vec![], &set(&[])));
        let template = Node::plan(vec![Arc::clone(&left), Arc::clone(&right)], &set(&[]));
        let shared = set(&[7]);
        let own = set(&[3]);
        MetricBuilder::new(&own).output_rows(1).add(0);
        let executed = Node::plan(
            vec![Node::plan(vec![], &shared), Node::plan(vec![], &shared)],
            &own,
        );
        let collected = observe(&template, &executed);
        assert_eq!((rows(&collected, &left), rows(&collected, &right)), (0, 0));
        assert_eq!(rows(&collected, &template), 3);
        assert_eq!(collected.ambiguous, 2);

        // A fresh parent sharing a metric with a template leaf it shares: the
        // leaf owns it, and the parent doesn't copy it.
        let leaf_metrics = set(&[5]);
        let leaf = Node::plan(vec![], &leaf_metrics);
        let template = Node::plan(vec![Arc::clone(&leaf)], &set(&[]));
        let executed = Node::plan(vec![Arc::clone(&leaf)], &leaf_metrics);
        let collected = observe(&template, &executed);
        assert_eq!((rows(&collected, &template), collected.ambiguous), (0, 0));

        // A plan sharing one node at many places is walked only up to the
        // bound, which the report says.
        let wide = Node::plan(vec![Arc::clone(&leaf); 10_000], &set(&[]));
        let collected = observe(&wide, &wide);
        assert!(collected.left_out >= 1);

        // A metric whose other owner the census doesn't reach: a distinct
        // executed node or the same node again past the inspection cap, or a
        // node below a copy of another shape. No handle is kept.
        let shared = set(&[7]);
        let fresh = || Node::plan(vec![], &set(&[]));
        let wide = || Node::plan(vec![], &set(&vec![1; MAX_OBSERVED]));
        let again = Node::plan(vec![], &shared);
        let three = |children: [Arc<dyn ExecutionPlan>; 3]| Node::plan(children.into(), &set(&[]));
        for (template, executed) in [
            (
                three([fresh(), fresh(), fresh()]),
                three([
                    Node::plan(vec![], &shared),
                    wide(),
                    Node::plan(vec![], &shared),
                ]),
            ),
            (
                three([fresh(), fresh(), fresh()]),
                three([Arc::clone(&again), wide(), Arc::clone(&again)]),
            ),
            (
                three([fresh(), fresh(), fresh()]),
                three([
                    Node::plan(vec![], &shared),
                    Node::plan(vec![Node::plan(vec![], &shared)], &set(&[])),
                    fresh(),
                ]),
            ),
        ] {
            let collected = observe(&template, &executed);
            assert!(collected.by_node.is_empty() && collected.left_out > 1);
        }

        // The same for the report's own census: past the cap, no node gets
        // metrics, and the task's own still show.
        let template = Node::plan(
            vec![
                Node::plan(vec![], &shared),
                Node::plan(vec![], &set(&vec![1; MAX_OBSERVED])),
                Node::plan(vec![], &shared),
            ],
            &set(&[]),
        );
        let task = Collected {
            task: vec![Metric::new(
                MetricValue::Count {
                    name: "transport_rows".into(),
                    count: Count::new(),
                },
                None,
            )],
            ..Default::default()
        };
        let report = build_report(shape(&template).as_ref(), task, TaskOutcome::COMPLETED);
        assert!(report.nodes.is_empty(), "{:?}", report.nodes.len());
        assert_eq!((report.left_out.nodes, report.task.len()), (4, 1));

        // Two nodes whose displays differ only past what's hashed have no
        // identity, and their plan no shape.
        let long =
            |end: &str| Node::labelled(vec![], &set(&[]), format!("{}{end}", "x".repeat(70_000)));
        assert_eq!(identity(long("a").as_ref()), None);
        assert!(shape(&Node::plan(vec![long("a"), long("b")], &set(&[]))).is_none());

        // A copy with more metrics than a copy inspects: it keeps that many
        // handles, and counts the rest once, before anything is merged.
        let many = set(&vec![1; MAX_OBSERVED + 10]);
        let template = Node::plan(vec![], &set(&[]));
        let collector = Arc::new(AnalyzeCollector::default());
        let mut copy = collector.begin_copy();
        copy.observe(&template, &Node::plan(vec![], &many), true);
        // Ownership can't be proven past the cap: none of the 4,096 handles
        // inspected is kept, and they count as left out, with the rest once.
        assert!(copy.observed.is_empty());
        assert_eq!(collector.state.lock().left_out, 1 + MAX_OBSERVED as u32);
    }

    /// One deadline for the whole query: a report still pending then is
    /// missing, and one arriving later changes nothing, even before the view
    /// freezes. A report attaches only to the plan its task was installed with.
    #[tokio::test(start_paused = true)]
    async fn reports_wait_for_one_query_deadline() {
        let registry = ReportRegistry::default();
        for (task, acknowledged) in [(1, true), (2, true), (3, false), (4, true), (5, true)] {
            registry.installed((task, 0), acknowledged, 7);
        }
        let deadline = registry.deadline();
        let owner = GenerationalNodeId::new(2, 1);
        let report = |plan_digest| {
            Arrival::Report(
                owner,
                QueryTaskReport {
                    plan_digest,
                    ..Default::default()
                },
            )
        };
        registry.received((1, 0), report(7));
        registry.received((4, 0), report(8));
        registry.received((5, 0), Arrival::Invalid);
        // Past the deadline, before the view freezes: too late.
        tokio::time::advance(REPORT_GRACE + Duration::from_millis(1)).await;
        registry.received((2, 0), report(7));
        assert_eq!(registry.deadline(), deadline);
        let reports = registry.finish().await;
        assert!(matches!(reports[&(1, 0)], TaskReport::Received { .. }));
        assert!(matches!(reports[&(2, 0)], TaskReport::Missing));
        assert!(matches!(reports[&(3, 0)], TaskReport::Unsupported));
        assert!(matches!(reports[&(4, 0)], TaskReport::Foreign));
        assert!(matches!(reports[&(5, 0)], TaskReport::Invalid));

        // Waiting ends at the deadline, the same for every task.
        let registry = ReportRegistry::default();
        registry.installed((1, 0), true, 7);
        let deadline = registry.deadline();
        assert!(matches!(
            registry.finish().await[&(1, 0)],
            TaskReport::Missing
        ));
        assert_eq!(Instant::now(), deadline);
    }

    /// However many groups the copies bring, the collector retains one
    /// report's worth, and counts the rest. A group's strings are kept once,
    /// in its metric, which the budget charged for, even for a long name or
    /// many labels.
    #[test]
    fn collected_groups_stay_within_a_report() {
        let collector = AnalyzeCollector::default();
        let add = |name: String, labels: usize| {
            let count = Count::new();
            count.add(1);
            let metric = Metric::new_with_labels(
                MetricValue::Count {
                    name: name.into(),
                    count,
                },
                None,
                (0..labels)
                    .map(|i| Label::new(format!("label_{i}"), "x".repeat(100)))
                    .collect(),
            );
            collector.state.lock().add(1, &metric);
        };
        // Held while live: every retained string is in a charged metric.
        let retained = || {
            let state = collector.state.lock();
            let strings = state
                .groups
                .iter()
                .map(|group| {
                    group.metric.value().name().len()
                        + group
                            .metric
                            .labels()
                            .iter()
                            .map(|label| label.name().len() + label.value().len())
                            .sum::<usize>()
                })
                .sum::<usize>();
            let charged = state
                .groups
                .iter()
                .map(|group| charge(&group.metric))
                .sum::<usize>();
            (strings, charged)
        };
        // A zero time merged stays zero: DataFusion's own aggregation would
        // count it as a nanosecond.
        for _ in 0..2 {
            let zero = Metric::new(MetricValue::ElapsedCompute(Time::new()), Some(0));
            collector.state.lock().add(2, &zero);
        }
        // More labels than a report keeps: left out before it's hashed, though
        // its strings would fit.
        add("many_labels".to_owned(), MAX_LABELS + 1);
        assert_eq!(collector.state.lock().groups.len(), 1);
        add("n".repeat(60_000), 0);
        add("labelled".to_owned(), 20);
        let (strings, charged) = retained();
        assert!(strings >= 60_000 && strings <= charged && charged <= MAX_BYTES);
        let offered = 2 * MAX_RECORDS;
        for i in 0..offered {
            add(format!("count_{i}"), 0);
        }
        let (strings, charged) = retained();
        assert!(
            strings <= charged && charged <= MAX_BYTES,
            "{strings} {charged}"
        );
        // An existing group merges in place, without taking more room.
        add("labelled".to_owned(), 20);
        let collected = collector.freeze();
        let kept = &collected.by_node[&1];
        assert!(kept.len() < MAX_RECORDS, "{}", kept.len());
        assert_eq!(kept.len() + collected.left_out as usize, offered + 3);
        let [zero] = collected.by_node[&2].as_slice() else {
            panic!("{:?}", collected.by_node[&2])
        };
        assert_eq!(zero.value().as_usize(), 0);
        assert_eq!(
            kept.iter()
                .find(|metric| metric.value().name() == "labelled")
                .map(|metric| metric.value().as_usize()),
            Some(2)
        );
    }
}
