// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Bounded, expression-aware presentation of the final EXPLAIN plans.
//!
//! DataFusion has no expression formatter hook shared by logical and physical
//! plans. Keep its node formatting, substituting only that node's exact typed
//! expression renderings, rather than parsing its (not necessarily escaped)
//! display text. Temporary expression copies are used only to render replacements;
//! they are never planned, executed, or sent to a worker.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::display::{PlanType, StringifiedPlan};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Result, ScalarValue, Statistics};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SessionState, SessionStateBuilder, TaskContext};
use datafusion::logical_expr::{ExplainFormat, Expr, LogicalPlan};
use datafusion::physical_expr::expressions::{InListExpr, Literal};
use datafusion::physical_optimizer::{PhysicalOptimizerContext, PhysicalOptimizerRule};
use datafusion::physical_plan::analyze::AnalyzeExec;
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream, displayable,
};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_distributed::DistributedExec;

use restate_platform::sync::Mutex;

use crate::distributed::analyze_view::ReportView;

const INLINE_VALUES: usize = 8;
const SAMPLE_VALUES: usize = 3;
const SAMPLE_CHARS: usize = 48;

#[derive(Debug)]
pub(crate) struct ExplainPlanner;

#[async_trait]
impl QueryPlanner for ExplainPlanner {
    async fn create_physical_plan(
        &self,
        plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planner = DefaultPhysicalPlanner::default();
        if let LogicalPlan::Analyze(analyze) = plan {
            let native = planner.create_physical_plan(plan, session).await?;
            let compact = !analyze.verbose && analyze.format == ExplainFormat::Indent;
            let Some(analyze) = native.downcast_ref::<AnalyzeExec>() else {
                return Ok(native);
            };
            // Install presentation only after native planning/optimization. The
            // native AnalyzeExec still owns execution, cancellation and rendering
            // of metrics/statistics, including all statement/session filters.
            let input = Arc::clone(analyze.input());
            let input = if input.is::<DistributedExec>() {
                // Remote stages show what their tasks report, once executed; a
                // compact display applies to that completed tree.
                Arc::new(ReportView::new(
                    input,
                    if compact {
                        compact_analyze
                    } else {
                        |plan| plan
                    },
                ))
            } else if !compact {
                return Ok(native);
            } else if let Ok(input) = ExplainDisplay::default().analyze_input(input) {
                input
            } else {
                // A display-only expression copy must not make a valid query fail.
                return Ok(native);
            };
            return native.replace_children(
                vec![input],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            );
        }
        let LogicalPlan::Explain(explain) = plan else {
            return planner.create_physical_plan(plan, session).await;
        };
        // Verbose retains DataFusion's complete values and optimizer history.
        // Other output formats have their own structure and escaping rules.
        if explain.verbose
            || !explain.logical_optimization_succeeded
            || explain.explain_format != ExplainFormat::Indent
        {
            return planner.create_physical_plan(plan, session).await;
        }

        // Let DataFusion handle EXPLAIN itself, including its distinction between
        // initial-planning errors, optimizer diagnostics, and propagated errors.
        // Observe the last rule's result without adding a rule or planning twice.
        let Some(state) = session.as_any().downcast_ref::<SessionState>() else {
            return planner.create_physical_plan(plan, session).await;
        };
        let mut rules = state.physical_optimizers().to_vec();
        let Some(last) = rules.last_mut() else {
            return planner.create_physical_plan(plan, session).await;
        };
        let observed = Arc::new(ObservePhysicalPlan {
            rule: Arc::clone(last),
            plan: Mutex::new(None),
        });
        *last = observed.clone();
        // Rebuilding must not recreate the shared default catalog. Restore the
        // original configuration after building so planning sees the same options.
        let mut observing_state = SessionStateBuilder::new_from_existing(state.clone())
            .with_session_id(state.session_id().to_owned())
            .with_config(
                state
                    .config()
                    .clone()
                    .with_create_default_catalog_and_schema(false),
            )
            .with_physical_optimizer_rules(rules)
            .build();
        *observing_state.config_mut() = state.config().clone();
        let native = planner.create_physical_plan(plan, &observing_state).await?;
        let Some(native_explain) = native.downcast_ref::<ExplainExec>() else {
            return Ok(native);
        };
        let config = &session.config_options().explain;
        if !config.logical_plan_only
            && !native_explain
                .stringified_plans()
                .iter()
                .any(|plan| plan.plan_type == PlanType::FinalPhysicalPlan)
        {
            // Failed planning: retain the entire native diagnostic history.
            return Ok(native);
        }
        let physical = observed.plan.lock().take();
        let mut display = ExplainDisplay::default();
        let plans = native_explain
            .stringified_plans()
            .iter()
            .map(|plan| {
                let text = match (&plan.plan_type, &physical) {
                    (PlanType::FinalLogicalPlan, _) => {
                        let mut visitor = LogicalDisplay {
                            display: &mut display,
                            output: String::new(),
                            depth: 0,
                        };
                        explain.plan.visit_with_subqueries(&mut visitor)?;
                        visitor.output
                    }
                    (PlanType::FinalPhysicalPlan, Some(physical)) => {
                        let mut text = String::new();
                        display.physical_plan(
                            physical.as_ref(),
                            explain.show_statistics.unwrap_or(config.show_statistics),
                            config.show_schema,
                            0,
                            &mut text,
                        )?;
                        text
                    }
                    _ => return Ok(plan.clone()),
                };
                Ok(StringifiedPlan::new(plan.plan_type.clone(), text))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(ExplainExec::new(
            native_explain.schema(),
            plans,
            native_explain.verbose(),
        )))
    }
}

/// The compact display of an executed ANALYZE tree, or the tree itself if its
/// expressions can't be copied for display.
fn compact_analyze(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    ExplainDisplay::default()
        .analyze_input(Arc::clone(&plan))
        .unwrap_or(plan)
}

/// A display tree over the original, optimized ANALYZE input. Only `fmt_as`
/// changes: native AnalyzeExec appends metrics/statistics outside this text.
/// `execute` enters the original root directly, so these display children never
/// participate in execution or worker serialization. Downcasts remain native for
/// diagnostics such as node-warning collection.
#[derive(Debug)]
struct AnalyzeDisplay {
    inner: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
    replacements: Vec<(String, String)>,
}

impl DisplayAs for AnalyzeDisplay {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if t != DisplayFormatType::Default {
            return self.inner.fmt_as(t, f);
        }
        struct Node<'a>(&'a dyn ExecutionPlan);
        impl fmt::Display for Node<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt_as(DisplayFormatType::Default, f)
            }
        }
        f.write_str(&substitute(
            &Node(self.inner.as_ref()).to_string(),
            self.replacements.clone(),
            &self.inner.schema(),
        ))
    }
}

impl ExecutionPlan for AnalyzeDisplay {
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

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
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

/// A query-local, pass-through observer. Keep the original rule's name,
/// context-aware entry point, schema checks, and error behavior.
#[derive(Debug)]
struct ObservePhysicalPlan {
    rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    plan: Mutex<Option<Arc<dyn ExecutionPlan>>>,
}

impl PhysicalOptimizerRule for ObservePhysicalPlan {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.rule.optimize(plan, config)?;
        *self.plan.lock() = Some(Arc::clone(&plan));
        Ok(plan)
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let plan = self.rule.optimize_with_context(plan, context)?;
        *self.plan.lock() = Some(Arc::clone(&plan));
        Ok(plan)
    }

    fn name(&self) -> &str {
        self.rule.name()
    }

    fn schema_check(&self) -> bool {
        self.rule.schema_check()
    }
}

#[derive(Default)]
struct ExplainDisplay {
    // Exact equality, rather than a truncated hash. IDs are local to one EXPLAIN;
    // ordering and UTF-8 storage representation do not change a set's identity.
    sets: Vec<HashSet<ScalarValue>>,
}

impl ExplainDisplay {
    fn analyze_input(&mut self, inner: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let replacements = self.physical_replacements(inner.as_ref())?;
        let children = inner
            .children()
            .into_iter()
            .map(|child| self.analyze_input(Arc::clone(child)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(AnalyzeDisplay {
            inner,
            children,
            replacements,
        }))
    }

    fn physical_replacements(&mut self, plan: &dyn ExecutionPlan) -> Result<Vec<(String, String)>> {
        let mut replacements = Vec::new();
        plan.apply_expressions(&mut |expr| {
            self.physical(expr, &mut replacements)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(replacements)
    }

    fn physical_plan(
        &mut self,
        plan: &dyn ExecutionPlan,
        show_statistics: bool,
        show_schema: bool,
        depth: usize,
        output: &mut String,
    ) -> Result<()> {
        let text = displayable(plan)
            .set_show_statistics(show_statistics)
            .set_show_schema(show_schema)
            .one_line()
            .to_string();
        let replacements = self.physical_replacements(plan)?;
        output.push_str(&"  ".repeat(depth));
        output.push_str(&substitute(&text, replacements, &plan.schema()));
        for child in plan.children() {
            self.physical_plan(
                child.as_ref(),
                show_statistics,
                show_schema,
                depth + 1,
                output,
            )?;
        }
        Ok(())
    }

    fn summary(&mut self, values: Vec<&ScalarValue>) -> String {
        let set: HashSet<_> = values
            .iter()
            .map(|value| match value {
                ScalarValue::Utf8View(value) | ScalarValue::LargeUtf8(value) => {
                    ScalarValue::Utf8(value.clone())
                }
                value => (*value).clone(),
            })
            .collect();
        let unique = set.len();
        let index = match self.sets.iter().position(|previous| *previous == set) {
            Some(index) => index,
            None => {
                self.sets.push(set);
                self.sets.len() - 1
            }
        };
        let sample = values
            .iter()
            .take(SAMPLE_VALUES)
            .map(|value| sample_value(value))
            .collect::<Vec<_>>()
            .join(", ");
        // A NULL member changes what IN and NOT IN return, so its presence is
        // shown whether or not it is among the samples.
        let null = if values.iter().any(|value| value.is_null()) {
            ", with NULL"
        } else {
            ""
        };
        format!(
            "<set#{}: {} values, {unique} distinct{null}, sample=[{sample}, …]>",
            index + 1,
            values.len()
        )
    }

    fn logical(&mut self, expr: &Expr, replacements: &mut Vec<(String, String)>) -> Result<()> {
        let display = expr.clone().transform_up(|mut expr| {
            if let Expr::InList(list) = &mut expr
                && list.list.len() > INLINE_VALUES
                && let Some(values) = list
                    .list
                    .iter()
                    .map(|expr| match expr {
                        Expr::Literal(value, _) => Some(value),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
            {
                let summary = self.summary(values);
                // ScalarVariable displays its name without quoting it. This
                // placeholder lives only in this disposable display expression.
                list.list = vec![Expr::ScalarVariable(
                    Arc::new(Field::new("", DataType::Null, true)),
                    vec![summary],
                )];
                return Ok(Transformed::yes(expr));
            }
            Ok(Transformed::no(expr))
        })?;
        replacements.push((expr.to_string(), display.data.to_string()));
        Ok(())
    }

    fn physical(
        &mut self,
        expr: &Arc<dyn PhysicalExpr>,
        replacements: &mut Vec<(String, String)>,
    ) -> Result<()> {
        let display = Arc::clone(expr).transform_up(|expr| {
            if let Some(list) = expr.downcast_ref::<InListExpr>()
                && list.len() > INLINE_VALUES
                && let Some(values) = list
                    .list()
                    .iter()
                    .map(|expr| expr.downcast_ref::<Literal>().map(Literal::value))
                    .collect::<Option<Vec<_>>>()
            {
                let summary = self.summary(values);
                // Retain the native IN/NOT IN/(SET) rendering and precedence.
                // with_new_children copies the expression; the original list
                // and its precomputed membership filter are never changed.
                let children = vec![
                    Arc::clone(list.expr()),
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(summary)))),
                ];
                let display = expr.with_new_children(children)?;
                return Ok(Transformed::yes(display));
            }
            Ok(Transformed::no(expr))
        })?;
        replacements.push((expr.to_string(), display.data.to_string()));
        Ok(())
    }
}

/// One sample of a literal set, shown by its type: NULL bare, numbers and
/// Booleans bare, and everything else quoted and escaped, so that the string
/// `'NULL'` or `'1'` can't be mistaken for NULL or a number. Commas, quotes,
/// newlines and Unicode in strings are data, not delimiters for this formatter.
fn sample_value(value: &ScalarValue) -> String {
    if value.is_null() {
        return "NULL".to_owned();
    }
    let text = match value {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => text.clone(),
        value => value.to_string(),
    };
    let mut chars = text.chars();
    let mut sample: String = chars.by_ref().take(SAMPLE_CHARS).collect();
    if chars.next().is_some() {
        sample.push('…');
    }
    let bare = value.data_type().is_numeric() || matches!(value, ScalarValue::Boolean(_));
    if bare { sample } else { format!("{sample:?}") }
}

struct LogicalDisplay<'a> {
    display: &'a mut ExplainDisplay,
    output: String,
    depth: usize,
}

impl<'n> TreeNodeVisitor<'n> for LogicalDisplay<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        let mut replacements = Vec::new();
        plan.apply_expressions(|expr| {
            self.display.logical(expr, &mut replacements)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        if self.depth > 0 {
            self.output.push('\n');
        }
        self.output.push_str(&"  ".repeat(self.depth));
        self.output.push_str(&substitute(
            &plan.display().to_string(),
            replacements,
            plan.schema().as_arrow(),
        ));
        self.depth += 1;
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, _: &'n LogicalPlan) -> Result<TreeNodeRecursion> {
        self.depth -= 1;
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Substitute complete typed expression renderings within one node. Include
/// unchanged roots to protect literals containing expression-looking text.
/// Prefer the longest match and never reprocess inserted samples. If different
/// roots have identical native text but different summaries (physical strings
/// are unquoted), retain the native rendering rather than guess. Likewise keep
/// a node intact when a changed spelling occurs in an output name, or more often
/// than its expression roots account for: those occurrences can be labels.
fn substitute(text: &str, mut replacements: Vec<(String, String)>, schema: &Schema) -> String {
    // An empty physical string literal renders as an empty string.
    replacements.retain(|(original, _)| !original.is_empty());
    if replacements.iter().all(|(before, after)| before == after) {
        return text.to_owned();
    }
    if replacements.iter().any(|(before, after)| {
        before != after
            && (schema
                .fields()
                .iter()
                .any(|field| field.name().contains(before))
                || text.matches(before).count()
                    > replacements
                        .iter()
                        .filter(|(root, _)| root == before)
                        .count())
    }) {
        return text.to_owned();
    }
    replacements.sort_unstable_by(|a, b| b.0.len().cmp(&a.0.len()).then(a.0.cmp(&b.0)));
    replacements.dedup_by(|a, b| {
        if a.0 != b.0 {
            return false;
        }
        if a.1 != b.1 {
            b.1.clone_from(&b.0);
        }
        true
    });
    let mut output = String::new();
    let mut remaining = text;
    while let Some((offset, (original, replacement))) = replacements
        .iter()
        .filter_map(|replacement| {
            remaining
                .find(&replacement.0)
                .map(|offset| (offset, replacement))
        })
        .min_by_key(|(offset, _)| *offset)
    {
        output.push_str(&remaining[..offset]);
        output.push_str(replacement);
        remaining = &remaining[offset + original.len()..];
    }
    output.push_str(remaining);
    output
}

#[cfg(test)]
#[path = "explain_tests.rs"]
mod tests;
