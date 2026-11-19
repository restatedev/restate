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
use super::report::{Budget, Encoded, MAX_NODES, charge, decode, encode};
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
    /// Executions so far: only the first owns the observation.
    attempts: Arc<std::sync::atomic::AtomicUsize>,
    /// This view's mark, which its [`Observed`] operators record under.
    owner: ObservationOwner,
}

/// Closes the coordinator's slots when dropped: once the display is
/// published, or when the execution ends before that, by an immediate error,
/// a stream error or a drop. A closed slot lets go of its copy, and keeps
/// nothing an operator records later.
struct Release(Option<Arc<Vec<Slot>>>);

impl Drop for Release {
    fn drop(&mut self) {
        for slot in self.0.iter().flat_map(|slots| slots.iter()) {
            let mut slot = slot.lock();
            slot.copy = None;
            slot.closed = true;
        }
    }
}

/// Copies the coordinator's numbers for display, within one report's budget,
/// through the report's own encoding: independent values, zero times kept.
///
/// The work is bounded too: it inspects at most [`MAX_INSPECTED`] metrics,
/// whatever their kind, and decodes at most as many report records; it stops
/// copying once the budget is spent, builds at most [`MAX_NODES`] display
/// positions, reserved before a node's children are visited, and attributes
/// at most [`MAX_TASKS`] stage tasks. What it doesn't reach counts once per
/// list, subtree or stage, as a lower bound.
#[derive(Debug)]
pub(super) struct Copier {
    budget: Budget,
    /// Metrics that didn't fit, or of kinds a report can't carry, and lists
    /// or subtrees left unread, once each.
    omitted: u32,
    /// Metrics inspected, report records decoded, display positions and
    /// stage tasks attributed.
    inspected: usize,
    decoded: usize,
    positions: usize,
    tasks: usize,
    /// The budget ran out: later lists aren't read.
    spent: bool,
    /// Whether the coordinator's operators were observed for this execution;
    /// if not, their numbers aren't shown.
    coordinator: bool,
}

/// The most coordinator metrics a publication inspects.
const MAX_INSPECTED: usize = 4 * super::report::MAX_RECORDS;

impl Default for Copier {
    fn default() -> Self {
        // Room for the root's own markers, which are added after the copies.
        let mut budget = Budget::for_task(0);
        for name in [
            "coordinator_metrics_unavailable",
            "coordinator_metrics_left_out",
        ] {
            budget.take(charge(&marker(name, usize::MAX, &[])));
        }
        Self {
            budget,
            omitted: 0,
            inspected: 0,
            decoded: 0,
            positions: 0,
            tasks: 0,
            spent: false,
            coordinator: true,
        }
    }
}

impl Copier {
    fn snapshot(&mut self, metrics: Option<MetricsSet>) -> Option<MetricsSet> {
        let metrics = metrics?;
        let mut copied = MetricsSet::new();
        for metric in metrics.iter() {
            if self.spent || self.inspected == MAX_INSPECTED {
                self.omitted = self.omitted.saturating_add(1);
                break;
            }
            self.inspected += 1;
            match encode(metric, &mut self.budget) {
                Encoded::Record(record) => match decode(&record) {
                    Some(copy) => copied.push(Arc::new(copy)),
                    None => self.omitted = self.omitted.saturating_add(1),
                },
                Encoded::Unsupported => self.omitted = self.omitted.saturating_add(1),
                Encoded::OverBudget => {
                    self.spent = true;
                    self.omitted = self.omitted.saturating_add(1);
                    break;
                }
            }
        }
        Some(copied)
    }

    /// Takes `count` display positions, all of them or none; with none, the
    /// subtree that needed them is left out, counted once.
    fn reserve(&mut self, count: usize) -> bool {
        if count > MAX_NODES - self.positions {
            self.omitted = self.omitted.saturating_add(1);
            return false;
        }
        self.positions += count;
        true
    }

    /// Takes the attribution of one more stage task, if any is left.
    fn task(&mut self) -> bool {
        if self.tasks == MAX_TASKS {
            return false;
        }
        self.tasks += 1;
        true
    }

    /// Takes the decoding of one more report record, if any is left.
    fn decode(&mut self) -> bool {
        if self.decoded == MAX_INSPECTED {
            return false;
        }
        self.decoded += 1;
        true
    }
}

/// The most stage tasks a publication attributes.
const MAX_TASKS: usize = MAX_NODES;

/// Marks the context of the execution that owns a view's observation (its
/// first) with the view's own number: an [`Observed`] operator records only
/// under its own view's number. Every execution of a view sets its context's
/// mark, so a context carried over from another execution, or another view,
/// marks nothing here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObservationOwner(u64);

/// No view's number: the mark of an execution that observes nothing.
const UNOBSERVED: ObservationOwner = ObservationOwner(0);

/// The next view's number.
static NEXT_VIEW: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Where an [`Observed`] operator records the copy that ran, until the
/// execution ends ([`Release`]).
type Slot = Arc<Mutex<Ran>>;

#[derive(Debug, Default)]
struct Ran {
    copy: Option<Arc<dyn ExecutionPlan>>,
    /// The execution ended: nothing more is recorded.
    closed: bool,
}

impl ReportView {
    /// A view of `inner`, a `DistributedExec`, whose coordinator operators are
    /// [`Observed`], so that they show the numbers of the copies that ran.
    pub(crate) fn new(
        inner: Arc<dyn ExecutionPlan>,
        present: fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>,
    ) -> Self {
        let owner = ObservationOwner(NEXT_VIEW.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
        let (mut slots, mut positions) = (Vec::new(), 0);
        let children = inner.children();
        let observed = reserve(&mut positions, children.len())
            .and_then(|()| {
                children
                    .into_iter()
                    .map(|child| observe(child, owner, &mut slots, &mut positions, 1))
                    .collect::<Result<Vec<_>>>()
            })
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
                attempts: Default::default(),
                owner,
            },
            // Unobserved, the coordinator's operators show no numbers, and the
            // view says so.
            Err(_) => Self {
                inner,
                present,
                attached: Default::default(),
                observed: None,
                root: Default::default(),
                attempts: Default::default(),
                owner,
            },
        }
    }
}

/// The most nested levels the coordinator's walks over a plan go down, stages
/// included: the observation is refused, or the display shows a subtree as it
/// is, below that.
const MAX_DEPTH: usize = 128;

/// Takes `count` more of the [`MAX_NODES`] positions of an observation.
fn reserve(positions: &mut usize, count: usize) -> Result<()> {
    if count > MAX_NODES - *positions {
        return internal_err!("more coordinator operators than are observed");
    }
    *positions += count;
    Ok(())
}

/// `plan`, at `depth`, with each operator above the stages [`Observed`]; the
/// stages are left as they are, so that nothing of this reaches a worker. A
/// plan of more than [`MAX_NODES`] positions, boundaries included, or deeper
/// than [`MAX_DEPTH`], isn't observed at all: every child's position is taken
/// before any is visited.
fn observe(
    plan: &Arc<dyn ExecutionPlan>,
    owner: ObservationOwner,
    slots: &mut Vec<Slot>,
    positions: &mut usize,
    depth: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if depth > MAX_DEPTH {
        return internal_err!("deeper coordinator operators than are observed");
    }
    if plan.is_network_boundary() {
        return Ok(Arc::clone(plan));
    }
    let children = plan.children();
    reserve(positions, children.len())?;
    let children = children
        .into_iter()
        .map(|child| observe(child, owner, slots, positions, depth + 1))
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
    Ok(Arc::new(Observed { inner, ran, owner }))
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
    ran: Slot,
    /// The view whose owning execution records here.
    owner: ObservationOwner,
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
            owner: self.owner,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Recorded only for the execution that owns this view's observation.
        let owned = context
            .session_config()
            .get_extension::<ObservationOwner>()
            .is_some_and(|mark| *mark == self.owner);
        let mut ran = self.ran.lock();
        if owned && !ran.closed {
            ran.copy = Some(Arc::clone(&self.inner));
        }
        drop(ran);
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        match &self.ran.lock().copy {
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
        // Only the first execution owns the observation. A retry, as after a
        // cancellation before the plan was prepared, or an overlapping one, runs
        // as natively, and shows the coordinator's numbers as unavailable: never
        // another execution's, or the never-executed plan's own.
        let owner = self
            .attempts
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            == 0;
        let observed = owner.then(|| self.observed.clone()).flatten();
        let unobserved = observed.is_none();
        let registry = Arc::new(ReportRegistry::default());
        let mut config = context.session_config().clone();
        config.set_extension(Arc::clone(&registry));
        // Set either way: a mark carried in from another execution is replaced.
        config.set_extension(Arc::new(if unobserved { UNOBSERVED } else { self.owner }));
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
        // Before anything executes: an immediate error closes the slots too, and
        // so does the first error the stream yields, which also stops the
        // display from being published.
        let release = Arc::new(Mutex::new(Some(Release(observed))));
        let output = self.inner.execute(partition, context)?;
        let schema = output.schema();
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let output = output.inspect({
            let (release, failed) = (Arc::clone(&release), Arc::clone(&failed));
            move |item| {
                if item.is_err() {
                    failed.store(true, std::sync::atomic::Ordering::Relaxed);
                    release.lock().take();
                }
            }
        });
        let plan = Arc::clone(&self.inner);
        let present = self.present;
        let attached = Arc::clone(&self.attached);
        let root = Arc::clone(&self.root);
        let attach = futures::stream::once(async move {
            if failed.load(std::sync::atomic::Ordering::Relaxed) {
                return None;
            }
            let reports = registry.finish().await;
            let mut copier = Copier {
                coordinator: !unobserved,
                ..Default::default()
            };
            let children = plan.children();
            // Too many to show: the plan shows as it is.
            let children = copier.reserve(children.len()).then(|| {
                children
                    .into_iter()
                    .map(|child| present(attach_at(child, &reports, &mut copier, 1)))
                    .collect()
            });
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
            // The first published display stays; a later one changes nothing.
            let _ = root.set(metrics);
            if let Some(children) = children {
                let _ = attached.set(children);
            }
            // The coordinator's numbers are copied: let the copies that ran go.
            release.lock().take();
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
#[cfg(test)]
pub(super) fn attach(
    plan: &Arc<dyn ExecutionPlan>,
    reports: &Reports,
    copier: &mut Copier,
) -> Arc<dyn ExecutionPlan> {
    attach_at(plan, reports, copier, 1)
}

/// [`attach`] of `plan` at `depth`, stages included: below [`MAX_DEPTH`], the
/// plan shows as it is.
fn attach_at(
    plan: &Arc<dyn ExecutionPlan>,
    reports: &Reports,
    copier: &mut Copier,
    depth: usize,
) -> Arc<dyn ExecutionPlan> {
    if depth > MAX_DEPTH {
        copier.omitted = copier.omitted.saturating_add(1);
        return Arc::clone(plan);
    }
    // `plan`'s own position was taken with its siblings'.
    if let Some(boundary) = plan.as_network_boundary()
        && let Stage::Local(stage) = boundary.input_stage()
    {
        let mut nodes = Vec::new();
        // More nodes than a report describes, or than are left to show: the
        // stage shows as it is, counted once. The listing stops at the
        // positions left, so stages past them cost nothing more.
        if !stage_nodes(
            &stage.plan,
            &mut nodes,
            depth + 1,
            MAX_NODES - copier.positions,
        ) {
            copier.omitted = copier.omitted.saturating_add(1);
            return Arc::clone(plan);
        }
        if !copier.reserve(nodes.len()) {
            return Arc::clone(plan);
        }
        let mut attributed = vec![MetricsSet::new(); nodes.len()];
        let mut markers = copier.snapshot(plan.metrics()).unwrap_or_default();
        let mut left_out = 0;
        for task in 0..stage.tasks {
            if !copier.task() {
                left_out = stage.tasks - task;
                break;
            }
            attribute(
                &nodes,
                (stage.num, task),
                reports.get(&(stage.num, task)),
                &mut attributed,
                &mut markers,
                copier,
            );
        }
        if left_out > 0 {
            markers.push(marker(
                "tasks_left_out",
                left_out,
                &[Label::new("stage", stage.num.to_string())],
            ));
        }
        let mut attributed = attributed.into_iter();
        let stage_plan = rebuild(&stage.plan, &mut attributed, reports, copier, depth + 1);
        return Reported::node(plan, vec![stage_plan], Some(markers));
    }
    let children = plan.children();
    if !copier.reserve(children.len()) {
        return Arc::clone(plan);
    }
    let children = children
        .into_iter()
        .map(|child| attach_at(child, reports, copier, depth + 1))
        .collect();
    // A coordinator operator's numbers, only where this execution observed it.
    let metrics = if copier.coordinator {
        copier.snapshot(plan.metrics())
    } else {
        None
    };
    Reported::node(plan, children, metrics)
}

/// The nodes of a stage's plan in pre-order, as its task executes it, from
/// `depth`: a stage within the stage is a node without children there. `false`
/// past `limit` nodes, at most [`MAX_NODES`], or past [`MAX_DEPTH`], where it
/// stops.
fn stage_nodes(
    plan: &Arc<dyn ExecutionPlan>,
    nodes: &mut Vec<Arc<dyn ExecutionPlan>>,
    depth: usize,
    limit: usize,
) -> bool {
    if nodes.len() >= limit.min(MAX_NODES) || depth > MAX_DEPTH {
        return false;
    }
    nodes.push(Arc::clone(plan));
    plan.is_network_boundary()
        || plan
            .children()
            .into_iter()
            .all(|child| stage_nodes(child, nodes, depth + 1, limit))
}

/// The stage's plan for display, from `depth`, each node with its
/// `attributed` metrics, in the pre-order of [`stage_nodes`]. Its positions are
/// reserved and [`stage_nodes`] went through every node within [`MAX_DEPTH`]:
/// every node is shown, so that the metrics stay aligned, and a stage within
/// it continues from its depth.
fn rebuild(
    plan: &Arc<dyn ExecutionPlan>,
    attributed: &mut impl Iterator<Item = MetricsSet>,
    reports: &Reports,
    copier: &mut Copier,
    depth: usize,
) -> Arc<dyn ExecutionPlan> {
    let metrics = attributed.next().unwrap_or_default();
    if plan.is_network_boundary() {
        return attach_at(plan, reports, copier, depth);
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| rebuild(child, attributed, reports, copier, depth + 1))
        .collect();
    Reported::node(plan, children, Some(metrics))
}

/// Adds the metrics of task `key`'s `report` to the `attributed` metrics of
/// the stage's `nodes` if the report describes them, and the task's markers.
/// Records past `copier`'s decoding allowance count as dropped.
fn attribute(
    nodes: &[Arc<dyn ExecutionPlan>],
    key: (usize, usize),
    report: Option<&TaskReport>,
    attributed: &mut [MetricsSet],
    markers: &mut MetricsSet,
    copier: &mut Copier,
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
    let mut decode = |record| copier.decode().then(|| decode(record)).flatten();
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

    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::metrics::Count;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion_distributed::{NetworkBoundary, NetworkCoalesceExec};
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
        // Copying stops at the first metric that doesn't fit, which counts
        // the rest once, and a later list isn't read.
        let mut copier = Copier::default();
        let copied = copier.snapshot(Some(many.clone())).unwrap();
        assert!(copied.iter().count() < super::super::report::MAX_RECORDS);
        assert_eq!(
            (copier.inspected, copier.omitted),
            (copied.iter().count() + 1, 1)
        );
        let later = copier.snapshot(Some(many)).unwrap();
        assert_eq!(
            (later.iter().count(), copier.inspected, copier.omitted),
            (0, copied.iter().count() + 1, 2)
        );
        // Metrics a report can't carry take no room, but count as inspected:
        // past the allowance, the rest is counted once.
        let mut far = MetricsSet::new();
        for _ in 0..2 * MAX_INSPECTED {
            let at = datafusion::physical_plan::metrics::Timestamp::new();
            at.set(
                datafusion::arrow::temporal_conversions::timestamp_s_to_datetime(10_000_000_000)
                    .unwrap()
                    .and_utc(),
            );
            far.push(Arc::new(Metric::new(MetricValue::StartTimestamp(at), None)));
        }
        let mut copier = Copier::default();
        copier.snapshot(Some(far));
        assert_eq!(
            (copier.inspected, copier.omitted as usize),
            (MAX_INSPECTED, MAX_INSPECTED + 1)
        );
        // A plan with more operators than are observed isn't observed at all,
        // wide or deep: each operator takes its position before its children.
        // Its display reserves a node's children before visiting them: a node
        // with more than are left shows as it is, its subtree untouched and
        // counted once.
        let leaf = Rows::plan(&[]);
        let wide = UnionExec::try_new(vec![Arc::clone(&leaf); 2 * MAX_NODES]).unwrap();
        assert!(
            ReportView::new(Arc::clone(&wide), |plan| plan)
                .observed
                .is_none()
        );
        let mut copier = Copier::default();
        let shown = attach(&wide, &Reports::new(), &mut copier);
        assert!(Arc::ptr_eq(&shown, &wide));
        assert_eq!((copier.positions, copier.omitted), (0, 1));
        // A stage with more nodes than a report describes shows as it is, and
        // so does a small one when no position is left: each counted once.
        let stage = super::super::owner_boundary(wide, Uuid::nil(), 1).unwrap();
        let mut copier = Copier::default();
        let shown = attach(&stage, &Reports::new(), &mut copier);
        assert!(Arc::ptr_eq(&shown, &stage));
        assert_eq!((copier.positions, copier.omitted), (0, 1));
        let small = super::super::owner_boundary(Rows::plan(&[]), Uuid::nil(), 1).unwrap();
        let mut copier = Copier {
            positions: MAX_NODES,
            ..Default::default()
        };
        let shown = attach(&small, &Reports::new(), &mut copier);
        assert!(Arc::ptr_eq(&shown, &small));
        assert_eq!(copier.omitted, 1);
        // A stage of many tasks without reports: at most so many are
        // attributed, each said missing, and the rest counted on the stage.
        let boundary = NetworkCoalesceExec::try_new(Rows::plan(&[]), 1, 1).unwrap();
        let mut many = boundary.input_stage().clone();
        if let Stage::Local(local) = &mut many {
            local.tasks = 10_000;
        }
        let stage = boundary.with_input_stage(many).unwrap() as Arc<dyn ExecutionPlan>;
        let mut copier = Copier::default();
        let shown = attach(&stage, &Reports::new(), &mut copier);
        let markers = shown.metrics().unwrap();
        let count = |name: &str| {
            markers
                .iter()
                .filter(|metric| metric.value().name() == name)
                .map(|metric| metric.value().as_usize())
                .sum::<usize>()
        };
        assert_eq!(
            (
                copier.tasks,
                count("metrics_missing"),
                count("tasks_left_out")
            ),
            (MAX_TASKS, MAX_TASKS, 10_000 - MAX_TASKS)
        );
        assert_eq!(copier.positions, 1);
        // Reports of more records than a publication decodes: the rest count
        // as dropped.
        let (plan, at) = filters(1, 2);
        let boundary = NetworkCoalesceExec::try_new(plan, 1, 1).unwrap();
        let mut tasks = boundary.input_stage().clone();
        if let Stage::Local(local) = &mut tasks {
            local.tasks = MAX_TASKS;
        }
        let stage = boundary.with_input_stage(tasks).unwrap() as Arc<dyn ExecutionPlan>;
        let Stage::Local(local) = stage.as_network_boundary().unwrap().input_stage() else {
            unreachable!()
        };
        let mut sent = report(&local.plan, &at);
        sent.task = vec![sent.task[0].clone(); 5];
        let reports = (0..MAX_TASKS)
            .map(|task| {
                (
                    (local.num, task),
                    TaskReport::Received {
                        owner: GenerationalNodeId::new(2, 1),
                        report: Box::new(sent.clone()),
                    },
                )
            })
            .collect::<Reports>();
        let mut copier = Copier::default();
        let markers = attach(&stage, &reports, &mut copier).metrics().unwrap();
        let dropped = markers
            .iter()
            .filter(|metric| metric.value().name() == "metrics_dropped")
            .map(|metric| metric.value().as_usize())
            .sum::<usize>();
        assert_eq!(
            (copier.decoded, dropped),
            (MAX_INSPECTED, MAX_TASKS * 6 - MAX_INSPECTED)
        );

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

    /// Listing a stage's nodes stops at the display positions left, so stages
    /// past them cost at most one node each, not a whole stage's.
    #[test]
    fn a_stage_listing_stops_at_the_positions_left() {
        let chain = (0..100).fold(Rows::plan(&[]), |plan, _| {
            Arc::new(CoalescePartitionsExec::new(plan)) as Arc<dyn ExecutionPlan>
        });
        let listed = |limit| {
            let mut nodes = Vec::new();
            (stage_nodes(&chain, &mut nodes, 1, limit), nodes.len())
        };
        assert_eq!(listed(MAX_NODES), (true, 101));
        assert_eq!(listed(101), (true, 101));
        assert_eq!(listed(100), (false, 100));
        assert_eq!(listed(0), (false, 0));
    }

    /// The coordinator's walks stay within [`MAX_DEPTH`] levels, stages
    /// included, and [`MAX_NODES`] positions, boundaries included: deeper or
    /// wider, the observation is refused and the display shows the subtree as
    /// it is. Every phase runs on a thread of the default 2 MiB stack.
    #[test]
    fn coordinator_walks_stay_within_their_depth_and_positions() {
        // `levels` operators above a leaf.
        let chain = |levels: usize, leaf: Arc<dyn ExecutionPlan>| {
            (0..levels).fold(leaf, |plan, _| {
                Arc::new(CoalescePartitionsExec::new(plan)) as Arc<dyn ExecutionPlan>
            })
        };
        let observed = |plan: &Arc<dyn ExecutionPlan>| {
            ReportView::new(Arc::clone(plan), |plan| plan)
                .observed
                .is_some()
        };
        let shown = |plan: &Arc<dyn ExecutionPlan>| {
            let mut copier = Copier::default();
            let shown = attach(plan, &Reports::new(), &mut copier);
            (shown, copier.positions, copier.omitted)
        };
        std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || {
                // Below, at and past the depth: the view's input's children are
                // at depth 1, so a chain of `MAX_DEPTH` levels is the deepest
                // observed.
                assert!(observed(&chain(MAX_DEPTH - 1, Rows::plan(&[]))));
                assert!(observed(&chain(MAX_DEPTH, Rows::plan(&[]))));
                assert!(!observed(&chain(MAX_DEPTH + 1, Rows::plan(&[]))));
                // The display, from depth 1 for the node itself: a chain of
                // `MAX_DEPTH` nodes is shown whole; past it, the rest shows as
                // it is, counted once.
                let (_, positions, omitted) = shown(&chain(MAX_DEPTH - 1, Rows::plan(&[])));
                assert_eq!((positions, omitted), (MAX_DEPTH - 1, 0));
                let (_, positions, omitted) = shown(&chain(MAX_DEPTH, Rows::plan(&[])));
                assert_eq!((positions, omitted), (MAX_DEPTH, 1));
                // Far deeper than both bounds: setup, display and drop return,
                // each separately.
                let deep = chain(MAX_NODES + 100, Rows::plan(&[]));
                assert!(!observed(&deep));
                let (shown_deep, positions, omitted) = shown(&deep);
                assert_eq!((positions, omitted), (MAX_DEPTH, 1));
                drop(shown_deep);
                drop(deep);
                // Nested stages carry the depth on: three stages of 60 levels
                // each reach past it, and the innermost shows as it is.
                let inner =
                    super::super::owner_boundary(chain(60, Rows::plan(&[])), Uuid::nil(), 3)
                        .unwrap();
                let middle =
                    super::super::owner_boundary(chain(60, inner), Uuid::nil(), 2).unwrap();
                let outer = chain(60, middle);
                let (_, positions, omitted) = shown(&outer);
                assert!(
                    positions < MAX_NODES && omitted == 1,
                    "{positions} {omitted}"
                );
                // Boundary-only fanout: one stage under twice as many edges as
                // there are positions isn't observed, and shows as it is.
                let stage = super::super::owner_boundary(Rows::plan(&[]), Uuid::nil(), 1).unwrap();
                let fanout = UnionExec::try_new(vec![stage; 2 * MAX_NODES]).unwrap();
                assert!(!observed(&fanout));
                // The same below the view's input: counted where it's visited.
                assert!(!observed(&chain(1, Arc::clone(&fanout))));
                let (shown_fanout, positions, omitted) = shown(&fanout);
                assert!(Arc::ptr_eq(&shown_fanout, &fanout));
                assert_eq!((positions, omitted), (0, 1));
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// How a view's execution ends: [`Driver`].
    #[derive(Debug, Clone, Copy)]
    enum Ending {
        Immediately,
        InStream,
        Normally,
    }

    /// Stands in for a `DistributedExec`: executes its one child, which the
    /// view observes, then ends as `ending` says.
    #[derive(Debug)]
    struct Driver {
        child: Arc<dyn ExecutionPlan>,
        ending: Ending,
    }

    impl DisplayAs for Driver {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Driver")
        }
    }

    impl ExecutionPlan for Driver {
        fn name(&self) -> &str {
            "Driver"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.child.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.child]
        }

        fn apply_expressions(
            &self,
            _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                child: children.remove(0),
                ending: self.ending,
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            // As the distributed runtime does, it runs a rebuilt copy of its
            // child, which only the child's slot keeps once it runs.
            let copy = Arc::clone(&self.child).replace_children(
                self.child.children().into_iter().cloned().collect(),
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?;
            let output = copy.execute(partition, context)?;
            match self.ending {
                Ending::Immediately => internal_err!("failed at once"),
                Ending::InStream => {
                    let failure =
                        futures::stream::once(async { internal_err!("failed in the stream") });
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        output.schema(),
                        output.chain(failure),
                    )))
                }
                Ending::Normally => Ok(output),
            }
        }
    }

    /// However a view's execution ends, at once with an error, with an error
    /// in its stream, dropped before publication, or published, its slots let
    /// go of the copies that ran and record nothing later, and the original
    /// errors stay.
    #[tokio::test]
    async fn coordinator_observations_end_with_the_execution() {
        let context = Arc::new(TaskContext::default());
        // A view whose child has `batches` executions of four rows each.
        let view = |ending, batches: usize| {
            let runs: Vec<_> = (0..batches).map(|_| 1..=4).collect();
            ReportView::new(
                Arc::new(Driver {
                    child: Rows::plan(&runs),
                    ending,
                }),
                |plan| plan,
            )
        };
        // Per slot: whether it holds a copy, and whether it's closed.
        let slots = |view: &ReportView| {
            view.observed
                .as_ref()
                .unwrap()
                .iter()
                .map(|slot| {
                    let slot = slot.lock();
                    (slot.copy.is_some(), slot.closed)
                })
                .collect::<Vec<_>>()
        };

        let failing = view(Ending::Immediately, 1);
        let error = failing.execute(0, Arc::clone(&context)).err().unwrap();
        assert!(error.to_string().contains("failed at once"), "{error}");
        assert_eq!(slots(&failing), [(false, true)]);

        // Its first error closes the slots, with the stream still held, lets
        // the copy that ran go, and keeps the display from being published.
        let failing = view(Ending::InStream, 1);
        let mut stream = failing.execute(0, Arc::clone(&context)).unwrap();
        let ran = failing.observed.as_ref().unwrap()[0]
            .lock()
            .copy
            .as_ref()
            .map(Arc::downgrade)
            .unwrap();
        stream.next().await.unwrap().unwrap();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(
            error.to_string().contains("failed in the stream"),
            "{error}"
        );
        assert_eq!(slots(&failing), [(false, true)]);
        assert!(ran.upgrade().is_none());
        assert!(stream.next().await.is_none());
        assert!(failing.attached.get().is_none() && failing.root.get().is_none());
        drop(stream);

        // Recorded while it runs; dropped before publication, let go. A copy
        // the runtime rebuilds and runs afterwards records nothing.
        let dropped = view(Ending::Normally, 2);
        let mut stream = dropped.execute(0, Arc::clone(&context)).unwrap();
        assert_eq!(slots(&dropped), [(true, false)]);
        stream.next().await.unwrap().unwrap();
        drop(stream);
        assert_eq!(slots(&dropped), [(false, true)]);
        let late = Arc::clone(dropped.inner.children()[0])
            .replace_children(
                vec![],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap();
        drop(late.execute(0, Arc::clone(&context)).unwrap());
        assert_eq!(slots(&dropped), [(false, true)]);

        // A retry of the same view, as after a cancellation before the plan
        // was prepared: it runs, and shows the coordinator's numbers as
        // unavailable, not the closed slots' or the never-executed plan's.
        let retried = view(Ending::Normally, 2);
        drop(retried.execute(0, Arc::clone(&context)).unwrap());
        assert_eq!(slots(&retried), [(false, true)]);
        datafusion::physical_plan::common::collect(
            retried.execute(0, Arc::clone(&context)).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(slots(&retried), [(false, true)]);
        let root = retried.metrics().unwrap();
        assert!(
            root.iter()
                .any(|metric| metric.value().name() == "coordinator_metrics_unavailable")
        );
        assert!(retried.children()[0].metrics().is_none());

        // Overlapping executions: the second neither records into the first's
        // slots nor closes them; the first publishes what it observed.
        let overlapped = view(Ending::Normally, 2);
        let first = overlapped.execute(0, Arc::clone(&context)).unwrap();
        assert_eq!(slots(&overlapped), [(true, false)]);
        let observed = overlapped.observed.as_ref().unwrap()[0]
            .lock()
            .copy
            .clone()
            .unwrap();
        drop(overlapped.execute(0, Arc::clone(&context)).unwrap());
        assert_eq!(slots(&overlapped), [(true, false)]);
        assert!(Arc::ptr_eq(
            overlapped.observed.as_ref().unwrap()[0]
                .lock()
                .copy
                .as_ref()
                .unwrap(),
            &observed
        ));
        drop(observed);
        datafusion::physical_plan::common::collect(first)
            .await
            .unwrap();
        assert_eq!(slots(&overlapped), [(false, true)]);
        // Published from the first's copy, checked above. (`Rows` copies share
        // their metrics, so its number counts both executions' rows.)
        assert!(
            overlapped.children()[0]
                .metrics()
                .and_then(|set| set.output_rows())
                .is_some()
        );

        // A context carried in from the owner, marked with the view's number,
        // gives an overlapping execution no observation: its mark is replaced.
        let inherited = view(Ending::Normally, 2);
        let first = inherited.execute(0, Arc::clone(&context)).unwrap();
        let copy = |view: &ReportView| view.observed.as_ref().unwrap()[0].lock().copy.clone();
        let observed = copy(&inherited).unwrap();
        let mut config = context.session_config().clone();
        config.set_extension(Arc::new(inherited.owner));
        let marked = Arc::new(TaskContext::default().with_session_config(config));
        drop(inherited.execute(0, marked).unwrap());
        assert!(Arc::ptr_eq(&copy(&inherited).unwrap(), &observed));
        drop(observed);
        datafusion::physical_plan::common::collect(first)
            .await
            .unwrap();
        assert_eq!(slots(&inherited), [(false, true)]);

        // A view rewritten from another observes for itself: the other's
        // slots, which the operators it wraps still carry, stay untouched.
        // (As `with_new_children` builds it: a new view over the old view's
        // input, whose operators are the old view's `Observed` ones.)
        let original = view(Ending::Normally, 1);
        let rewritten = ReportView::new(Arc::clone(&original.inner), |plan| plan);
        let mut stream = rewritten.execute(0, Arc::clone(&context)).unwrap();
        assert_eq!(
            (slots(&original), slots(&rewritten)),
            (vec![(false, false)], vec![(true, false)])
        );
        while stream.next().await.is_some() {}
        assert_eq!(
            (slots(&original), slots(&rewritten)),
            (vec![(false, false)], vec![(false, true)])
        );

        // Published: the display keeps the copied numbers.
        let published = view(Ending::Normally, 1);
        datafusion::physical_plan::common::collect(published.execute(0, context).unwrap())
            .await
            .unwrap();
        assert_eq!(slots(&published), [(false, true)]);
        assert_eq!(
            published.children()[0]
                .metrics()
                .and_then(|set| set.output_rows()),
            Some(4)
        );
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
