// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The coordinator's side of `EXPLAIN ANALYZE` over a distributed plan: runs
//! the plan unchanged, then shows each remote stage with what its task
//! reported.
//!
//! DataFusion's `AnalyzeExec` renders its input's tree once the input ends. A
//! [`ReportView`] is that input: it executes the [`DistributedExec`] below it,
//! and before ending its stream it collects the tasks' reports and builds a
//! copy of the plan for display, in which each stage's nodes carry the metrics
//! their task reported:
//!
//! ```text
//! AnalyzeExec
//!   ReportView ─ executes ─▶ DistributedExec ─▶ the stages' tasks, as usual
//!     │ at the end: the reports, within the query's one deadline
//!     ▼
//!   CoalescePartitionsExec                     the plan, for display only:
//!     NetworkCoalesceExec, metrics=[tasks_completed=1]  task markers
//!       FilterExec, metrics=[output_rows=12, …]         reported metrics
//!         TableScanExec, metrics=[read_rows=40, …]
//! ```
//!
//! A report attaches to its stage only if it describes the same plan: the same
//! nodes, in the same order, with the same configuration, read from the stage's
//! owner. Otherwise its stage says why it has no numbers, with a marker such as
//! `metrics_shape_mismatch=1` or `metrics_missing=1`, and none are guessed.
//!
//! [`DistributedExec`]: datafusion_distributed::DistributedExec

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, OnceLock};

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, Statistics, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{Count, Label, MetricType, MetricValue, MetricsSet};
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Metric, PhysicalExpr,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion_distributed::{NetworkBoundaryExt, Stage};
use futures::StreamExt;

use restate_platform::sync::Mutex;
use restate_types::net::distributed_query::{QueryTaskReport, TaskOutcome};

use super::analyze::{ReportRegistry, TaskReport, identity};
use super::report::{Budget, Encoded, decode, encode};
use super::source::source_owner;

pub(super) type Reports = BTreeMap<(usize, usize), TaskReport>;

/// The input of an `EXPLAIN ANALYZE` over a distributed plan: executes the plan
/// unchanged, and once it has ended shows it with its tasks' reports.
#[derive(Debug)]
pub(crate) struct ReportView {
    /// The executable plan, a `DistributedExec`.
    inner: Arc<dyn ExecutionPlan>,
    /// Applied to the display tree once it is complete.
    present: fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>,
    /// The display tree's children, set once, before the stream ends.
    attached: Arc<OnceLock<Vec<Arc<dyn ExecutionPlan>>>>,
    /// The coordinator operators' slots for the copies that ran, emptied once
    /// their numbers are copied; `None` if the operators couldn't be observed.
    observed: Option<Arc<Vec<Slot>>>,
    /// The root's own metrics, copied when the display tree is published.
    root: Arc<OnceLock<MetricsSet>>,
}

/// Empties the coordinator's slots when dropped: once the display is
/// published, or when the stream ends before that, by an error or a drop.
struct Release(Option<Arc<Vec<Slot>>>);

impl Drop for Release {
    fn drop(&mut self) {
        for slot in self.0.iter().flat_map(|slots| slots.iter()) {
            slot.lock().take();
        }
    }
}

/// Copies the coordinator's numbers for display, within one report's budget,
/// through the report's own encoding: independent values, zero times kept.
#[derive(Debug)]
pub(super) struct Copier {
    budget: Budget,
    /// Metrics that didn't fit, or of kinds a report can't carry.
    omitted: u32,
}

impl Default for Copier {
    fn default() -> Self {
        Self {
            budget: Budget::for_task(0),
            omitted: 0,
        }
    }
}

impl Copier {
    fn snapshot(&mut self, metrics: Option<MetricsSet>) -> Option<MetricsSet> {
        let metrics = metrics?;
        let mut copied = MetricsSet::new();
        for metric in metrics.iter() {
            let copy = match encode(metric, &mut self.budget) {
                Encoded::Record(record) => decode(&record),
                Encoded::Unsupported | Encoded::OverBudget => None,
            };
            match copy {
                Some(copy) => copied.push(Arc::new(copy)),
                None => self.omitted = self.omitted.saturating_add(1),
            }
        }
        Some(copied)
    }
}

/// Where an [`Observed`] operator records the copy that ran.
type Slot = Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>;

impl ReportView {
    /// A view of `inner`, a `DistributedExec`, whose coordinator operators are
    /// [`Observed`], so that they show the numbers of the copies that ran.
    pub(crate) fn new(
        inner: Arc<dyn ExecutionPlan>,
        present: fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>,
    ) -> Self {
        let mut slots = Vec::new();
        let observed = inner
            .children()
            .into_iter()
            .map(|child| observe(child, &mut slots))
            .collect::<Result<Vec<_>>>()
            .and_then(|children| {
                Arc::clone(&inner).replace_children(
                    children,
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )
            });
        match observed {
            Ok(observed) => Self {
                inner: observed,
                present,
                attached: Default::default(),
                observed: Some(Arc::new(slots)),
                root: Default::default(),
            },
            // Unobserved, the coordinator's operators show no numbers, and the
            // view says so.
            Err(_) => Self {
                inner,
                present,
                attached: Default::default(),
                observed: None,
                root: Default::default(),
            },
        }
    }
}

/// `plan` with each operator above the stages [`Observed`]; the stages are
/// left as they are, so that nothing of this reaches a worker.
fn observe(plan: &Arc<dyn ExecutionPlan>, slots: &mut Vec<Slot>) -> Result<Arc<dyn ExecutionPlan>> {
    if plan.is_network_boundary() {
        return Ok(Arc::clone(plan));
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| observe(child, slots))
        .collect::<Result<Vec<_>>>()?;
    let inner = if children.is_empty() {
        Arc::clone(plan)
    } else {
        Arc::clone(plan).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?
    };
    let ran = Slot::default();
    slots.push(Arc::clone(&ran));
    Ok(Arc::new(Observed { inner, ran }))
}

/// A coordinator operator under `EXPLAIN ANALYZE`. The distributed runtime
/// prepares the plan by rebuilding every operator above a stage, so the copy
/// that runs isn't the one in the plan; through this node, the plan's operator
/// shows the numbers of the copy that ran:
///
/// ```text
/// plan:      Observed(UnionExec) ──┐ shares `ran`
/// prepared:  Observed(UnionExec′) ─┘ executes UnionExec′ and records it in `ran`
/// ```
#[derive(Debug)]
struct Observed {
    inner: Arc<dyn ExecutionPlan>,
    /// The copy that ran, shared by every rebuild of this node.
    ran: Arc<Mutex<Option<Arc<dyn ExecutionPlan>>>>,
}

impl DisplayAs for Observed {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for Observed {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: Arc::clone(&self.inner).replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            ran: Arc::clone(&self.ran),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        *self.ran.lock() = Some(Arc::clone(&self.inner));
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        match &*self.ran.lock() {
            Some(ran) => ran.metrics(),
            None => self.inner.metrics(),
        }
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.inner.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.inner.statistics_from_inputs(input_stats, args)
    }
}

impl DisplayAs for ReportView {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for ReportView {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match self.attached.get() {
            Some(children) => children.iter().collect(),
            None => self.inner.children(),
        }
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // A new plan gets a view of its own, without this one's display tree.
        Ok(Arc::new(Self::new(
            Arc::clone(&self.inner).replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            self.present,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The transport finds the registry in the context and asks every task
        // of this execution for its report.
        let registry = Arc::new(ReportRegistry::default());
        let mut config = context.session_config().clone();
        config.set_extension(Arc::clone(&registry));
        let context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        let output = self.inner.execute(partition, context)?;
        let schema = output.schema();
        let plan = Arc::clone(&self.inner);
        let present = self.present;
        let attached = Arc::clone(&self.attached);
        // Only after the last batch: an error ends the stream before this.
        let unobserved = self.observed.is_none();
        let release = Release(self.observed.clone());
        let root = Arc::clone(&self.root);
        let attach = futures::stream::once(async move {
            let reports = registry.finish().await;
            let mut copier = Copier::default();
            let children = plan
                .children()
                .into_iter()
                .map(|child| present(attach(child, &reports, &mut copier)))
                .collect();
            let mut metrics = copier.snapshot(plan.metrics()).unwrap_or_default();
            if unobserved {
                metrics.push(marker("coordinator_metrics_unavailable", 1, &[]));
            }
            if copier.omitted > 0 {
                metrics.push(marker(
                    "coordinator_metrics_left_out",
                    copier.omitted as usize,
                    &[],
                ));
            }
            // Executed once; a second execution fails in the plan below.
            let _ = root.set(metrics);
            let _ = attached.set(children);
            // The coordinator's numbers are copied: let the copies that ran go.
            drop(release);
            None
        })
        .filter_map(std::future::ready);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            output.chain(attach),
        )))
    }

    /// The copy taken when the display was published; before that, the live
    /// metrics.
    fn metrics(&self) -> Option<MetricsSet> {
        match self.root.get() {
            Some(root) => Some(root.clone()),
            None => self.inner.metrics(),
        }
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.inner.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.inner.statistics_from_inputs(input_stats, args)
    }
}

/// `plan` for display, with every stage below it showing its tasks' reports.
pub(super) fn attach(
    plan: &Arc<dyn ExecutionPlan>,
    reports: &Reports,
    copier: &mut Copier,
) -> Arc<dyn ExecutionPlan> {
    if let Some(boundary) = plan.as_network_boundary()
        && let Stage::Local(stage) = boundary.input_stage()
    {
        let mut nodes = Vec::new();
        stage_nodes(&stage.plan, &mut nodes);
        let mut attributed = vec![MetricsSet::new(); nodes.len()];
        let mut markers = copier.snapshot(plan.metrics()).unwrap_or_default();
        for task in 0..stage.tasks {
            attribute(
                &nodes,
                (stage.num, task),
                reports.get(&(stage.num, task)),
                &mut attributed,
                &mut markers,
            );
        }
        let mut attributed = attributed.into_iter();
        let stage_plan = rebuild(&stage.plan, &mut attributed, reports, copier);
        return Reported::node(plan, vec![stage_plan], Some(markers));
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| attach(child, reports, copier))
        .collect();
    Reported::node(plan, children, copier.snapshot(plan.metrics()))
}

/// The nodes of a stage's plan in pre-order, as its task executes it: a stage
/// within the stage is a node without children there.
fn stage_nodes(plan: &Arc<dyn ExecutionPlan>, nodes: &mut Vec<Arc<dyn ExecutionPlan>>) {
    nodes.push(Arc::clone(plan));
    if !plan.is_network_boundary() {
        for child in plan.children() {
            stage_nodes(child, nodes);
        }
    }
}

/// The stage's plan for display, each node with its `attributed` metrics, in
/// the pre-order of [`stage_nodes`].
fn rebuild(
    plan: &Arc<dyn ExecutionPlan>,
    attributed: &mut impl Iterator<Item = MetricsSet>,
    reports: &Reports,
    copier: &mut Copier,
) -> Arc<dyn ExecutionPlan> {
    let metrics = attributed.next().unwrap_or_default();
    if plan.is_network_boundary() {
        return attach(plan, reports, copier);
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| rebuild(child, attributed, reports, copier))
        .collect();
    Reported::node(plan, children, Some(metrics))
}

/// Adds the metrics of task `key`'s `report` to the `attributed` metrics of
/// the stage's `nodes` if the report describes them, and the task's markers.
fn attribute(
    nodes: &[Arc<dyn ExecutionPlan>],
    key: (usize, usize),
    report: Option<&TaskReport>,
    attributed: &mut [MetricsSet],
    markers: &mut MetricsSet,
) {
    fn mark(markers: &mut MetricsSet, name: &'static str, value: usize, labels: &[Label]) {
        if value > 0 {
            markers.push(marker(name, value, labels));
        }
    }
    let mut labels = vec![
        Label::new("stage", key.0.to_string()),
        Label::new("task", key.1.to_string()),
    ];
    let (owner, report) = match report {
        None | Some(TaskReport::Pending { .. } | TaskReport::Missing) => {
            return mark(markers, "metrics_missing", 1, &labels);
        }
        Some(TaskReport::Unsupported) => return mark(markers, "metrics_unsupported", 1, &labels),
        Some(TaskReport::Invalid) => return mark(markers, "metrics_invalid", 1, &labels),
        Some(TaskReport::Foreign) => return mark(markers, "metrics_shape_mismatch", 1, &labels),
        Some(TaskReport::Received { owner, report }) => (*owner, report),
    };
    labels.insert(0, Label::new("owner", owner.to_string()));
    // A report from another node than the stage's sources' owner isn't this
    // stage's: nothing of it attaches.
    if nodes
        .iter()
        .any(|node| source_owner(node.as_ref()).is_some_and(|source| source != owner))
    {
        return mark(markers, "metrics_wrong_owner", 1, &labels);
    }
    let labelled = |metric: Metric| {
        Arc::new(
            labels
                .iter()
                .fold(metric, |metric, label| metric.with_label(label.clone())),
        )
    };
    let size = |value: u32| usize::try_from(value).unwrap_or(usize::MAX);
    let outcome = match report.outcome {
        TaskOutcome::COMPLETED => "tasks_completed",
        TaskOutcome::INCOMPLETE => "tasks_incomplete",
        TaskOutcome::FAILED => "tasks_failed",
        // Unknown, or an outcome a newer worker added.
        _ => "tasks_unknown",
    };
    mark(markers, outcome, 1, &labels);
    // What the report couldn't carry, whatever its shape.
    mark(
        markers,
        "metrics_partial",
        size(report.unsettled_lanes),
        &labels,
    );
    mark(
        markers,
        "metrics_ambiguous",
        size(report.ambiguous_nodes),
        &labels,
    );
    let truncated =
        report.left_out.shape || report.left_out.nodes > 0 || report.left_out.metrics > 0;
    mark(
        markers,
        "metrics_truncated",
        usize::from(truncated),
        &labels,
    );
    mark(
        markers,
        "metrics_left_out",
        size(report.left_out.metrics),
        &labels,
    );
    // The task's own metrics, such as what it sent, show on its stage whatever
    // its plan's shape.
    let mut dropped = size(report.unsupported);
    for record in &report.task {
        match decode(record) {
            Some(metric) => markers.push(labelled(metric)),
            None => dropped += 1,
        }
    }
    if !report.left_out.shape {
        if describes(nodes, report) {
            for node in &report.nodes {
                let set = &mut attributed[size(node.ordinal)];
                for record in &node.metrics {
                    match decode(record) {
                        Some(metric) => set.push(labelled(metric)),
                        None => dropped += 1,
                    }
                }
            }
        } else {
            mark(markers, "metrics_shape_mismatch", 1, &labels);
        }
    }
    mark(markers, "metrics_dropped", dropped, &labels);
}

/// Whether `report` describes the stage's `nodes`: the same nodes in the same
/// order and configuration, and metrics only for those nodes. Its size was
/// checked as it was read.
fn describes(nodes: &[Arc<dyn ExecutionPlan>], report: &QueryTaskReport) -> bool {
    if report.shape.len() != nodes.len() {
        return false;
    }
    let same = nodes.iter().zip(&report.shape).all(|(node, shape)| {
        let children = if node.is_network_boundary() {
            0
        } else {
            node.children().len()
        };
        shape.name.as_str() == node.name()
            && usize::try_from(shape.children).is_ok_and(|count| count == children)
            && identity(node.as_ref()) == Some(shape.identity)
    });
    // Ordinals strictly increase, so each node's metrics come once.
    let mut next = 0;
    same && report.nodes.iter().all(|node| {
        let ordinal = usize::try_from(node.ordinal).unwrap_or(usize::MAX);
        let fits = ordinal >= next && ordinal < nodes.len();
        next = ordinal.saturating_add(1);
        fits
    })
}

fn marker(name: &'static str, value: usize, labels: &[Label]) -> Arc<Metric> {
    let count = Count::new();
    count.add(value);
    Arc::new(
        Metric::new_with_labels(
            MetricValue::Count {
                name: name.into(),
                count,
            },
            None,
            labels.to_vec(),
        )
        .with_type(MetricType::Summary),
    )
}

/// A node of a plan for display: shows `inner` with other children and
/// metrics. Never executed.
#[derive(Debug)]
struct Reported {
    inner: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
    metrics: Option<MetricsSet>,
}

impl Reported {
    /// `inner` shown with `children` and `metrics`.
    fn node(
        inner: &Arc<dyn ExecutionPlan>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        metrics: Option<MetricsSet>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            inner: Arc::clone(inner),
            children,
            metrics,
        })
    }
}

impl DisplayAs for Reported {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for Reported {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Rewrites operate on executable plans, discarding this presentation.
        Arc::clone(&self.inner).replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        internal_err!("{} is shown for EXPLAIN ANALYZE only", self.name())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.metrics.clone()
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.inner.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.inner.statistics_from_inputs(input_stats, args)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::metrics::Count;
    use datafusion::physical_plan::union::UnionExec;
    use uuid::Uuid;

    use restate_types::GenerationalNodeId;

    use super::super::analyze::tests::{Rows, even};
    use super::super::analyze::{Collected, address, build_report, shape};
    use super::*;

    /// `x = first` above `x = second` above a leaf, and where `x = first` is.
    fn filters(first: i64, second: i64) -> (Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>) {
        let inner = Arc::new(FilterExec::try_new(even(Some(second)), Rows::plan(&[])).unwrap());
        let outer: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(even(Some(first)), inner).unwrap());
        (Arc::clone(&outer), outer)
    }

    /// A task's report of `plan`, whose node `at` output five rows, and which
    /// sent three.
    fn report(plan: &Arc<dyn ExecutionPlan>, at: &Arc<dyn ExecutionPlan>) -> QueryTaskReport {
        let count = |value| {
            let count = Count::new();
            count.add(value);
            count
        };
        let collected = Collected {
            by_node: HashMap::from([(
                address(at),
                vec![Metric::new(MetricValue::OutputRows(count(5)), Some(0))],
            )]),
            task: vec![Metric::new(
                MetricValue::Count {
                    name: "transport_rows".into(),
                    count: count(3),
                },
                None,
            )],
            ..Default::default()
        };
        build_report(shape(plan).as_ref(), collected, TaskOutcome::COMPLETED)
    }

    /// The display keeps copies of the coordinator's numbers: a handle that
    /// changes afterwards, say of a copy still running down, doesn't change
    /// them. A zero time stays zero, and the copies take one report's room at
    /// most. Once published, the copies that ran are let go, and so they are
    /// if the stream ends before.
    #[tokio::test]
    async fn coordinator_numbers_are_copied() {
        let rows = Count::new();
        rows.add(4);
        let mut original = MetricsSet::new();
        original.push(Arc::new(Metric::new(
            MetricValue::OutputRows(rows.clone()),
            Some(0),
        )));
        original.push(Arc::new(Metric::new(
            MetricValue::ElapsedCompute(datafusion::physical_plan::metrics::Time::new()),
            Some(0),
        )));
        let mut copier = Copier::default();
        let copied = copier.snapshot(Some(original)).unwrap();
        rows.add(10);
        assert_eq!(copied.output_rows(), Some(4));
        // Read directly: DataFusion's own sum counts a zero time as 1 ns.
        let elapsed = copied
            .iter()
            .find(|metric| matches!(metric.value(), MetricValue::ElapsedCompute(_)))
            .map(|metric| metric.value().as_usize());
        assert_eq!(elapsed, Some(0));
        assert_eq!(copier.omitted, 0);

        // More than one report's room: the rest is counted, not copied.
        let mut many = MetricsSet::new();
        for partition in 0..2 * super::super::report::MAX_RECORDS {
            let count = Count::new();
            count.add(1);
            many.push(Arc::new(Metric::new(
                MetricValue::OutputRows(count),
                Some(partition),
            )));
        }
        let mut copier = Copier::default();
        let copied = copier.snapshot(Some(many)).unwrap();
        assert!(copier.omitted > 0);
        assert_eq!(
            copied.iter().count() + copier.omitted as usize,
            2 * super::super::report::MAX_RECORDS
        );

        // The slots let go of the copies that ran when released, however.
        let ran: Arc<dyn ExecutionPlan> = Rows::plan(&[]);
        let weak = Arc::downgrade(&ran);
        let slot = Slot::default();
        *slot.lock() = Some(ran);
        drop(Release(Some(Arc::new(vec![Arc::clone(&slot)]))));
        assert!(weak.upgrade().is_none());

        // The root's own numbers are frozen when published: read again after
        // its live handle grows, they're the same.
        let leaf = Rows::plan(&[1..=4, 1..=2]);
        let view: Arc<dyn ExecutionPlan> =
            Arc::new(ReportView::new(Arc::clone(&leaf), |plan| plan));
        let context = Arc::new(TaskContext::default());
        datafusion::physical_plan::collect(Arc::clone(&view), Arc::clone(&context))
            .await
            .unwrap();
        datafusion::physical_plan::collect(leaf, context)
            .await
            .unwrap();
        for _ in 0..2 {
            assert_eq!(view.metrics().and_then(|set| set.output_rows()), Some(4));
        }
    }

    /// Reports attach only to the stage plan they describe; every other stage
    /// says why it shows no operator numbers. A task's own metrics show
    /// whatever the shape.
    #[test]
    fn reports_attach_only_to_the_plan_they_describe() {
        let owner = GenerationalNodeId::new(2, 1);
        let received = |report| TaskReport::Received {
            owner,
            report: Box::new(report),
        };
        let stages = (1..=5)
            .map(|stage| super::super::owner_boundary(filters(1, 2).0, Uuid::nil(), stage))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let (same, at) = filters(1, 2);
        // Same kinds and arity, other predicates: swapped, it must not attach.
        let (swapped, swapped_at) = filters(2, 1);
        let mut truncated = report(&same, &at);
        truncated.left_out.shape = true;
        truncated.shape.clear();
        truncated.nodes.clear();
        let reports = Reports::from([
            ((1, 0), received(report(&same, &at))),
            ((2, 0), received(report(&swapped, &swapped_at))),
            ((3, 0), TaskReport::Unsupported),
            ((4, 0), received(truncated)),
            // Stage 5 never reported.
        ]);
        let plan = UnionExec::try_new(stages).unwrap();
        let shown = DisplayableExecutionPlan::with_metrics(
            attach(&plan, &reports, &mut Copier::default()).as_ref(),
        )
        .indent(true)
        .to_string();
        let stages: Vec<_> = shown.split("NetworkCoalesceExec").skip(1).collect();
        let [first, swapped, unsupported, truncated, missing] = stages.as_slice() else {
            panic!("{shown}")
        };
        assert!(first.contains("tasks_completed=1"), "{shown}");
        assert!(
            first.contains("FilterExec: x@0 = 1, metrics=[output_rows=5]"),
            "{shown}"
        );
        assert!(first.contains("FilterExec: x@0 = 2, metrics=[]"), "{shown}");
        assert!(swapped.contains("metrics_shape_mismatch=1"), "{shown}");
        assert!(unsupported.contains("metrics_unsupported=1"), "{shown}");
        assert!(truncated.contains("metrics_truncated=1"), "{shown}");
        assert!(missing.contains("metrics_missing=1"), "{shown}");
        for stage in [swapped, unsupported, truncated, missing] {
            assert!(!stage.contains("output_rows"), "{shown}");
        }
        for stage in [first, swapped, truncated] {
            assert!(stage.contains("transport_rows=3"), "{shown}");
        }
        for stage in [unsupported, missing] {
            assert!(!stage.contains("transport_rows"), "{shown}");
        }
    }
}
