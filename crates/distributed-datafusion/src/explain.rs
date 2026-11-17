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
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::catalog::Session;
use datafusion::common::display::{PlanType, StringifiedPlan};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Result, ScalarValue};
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::{ExplainFormat, Expr, LogicalPlan};
use datafusion::physical_expr::expressions::{InListExpr, Literal};
use datafusion::physical_plan::explain::ExplainExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, displayable};
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

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

        let config = &session.config_options().explain;
        let mut display = ExplainDisplay::default();
        let mut plans = Vec::new();
        if !config.physical_plan_only {
            let mut visitor = LogicalDisplay {
                display: &mut display,
                output: String::new(),
                depth: 0,
            };
            explain.plan.visit_with_subqueries(&mut visitor)?;
            plans.push(StringifiedPlan::new(
                PlanType::FinalLogicalPlan,
                visitor.output,
            ));
        }
        if !config.logical_plan_only {
            match planner.create_physical_plan(&explain.plan, session).await {
                Ok(physical) => {
                    let mut text = String::new();
                    display.physical_plan(
                        physical.as_ref(),
                        explain.show_statistics.unwrap_or(config.show_statistics),
                        config.show_schema,
                        0,
                        &mut text,
                    )?;
                    plans.push(StringifiedPlan::new(PlanType::FinalPhysicalPlan, text));
                }
                Err(error) => plans.push(StringifiedPlan::new(
                    PlanType::PhysicalPlanError,
                    error.strip_backtrace(),
                )),
            }
        }
        Ok(Arc::new(ExplainExec::new(
            Arc::clone(explain.schema.inner()),
            plans,
            false,
        )))
    }
}

#[derive(Default)]
struct ExplainDisplay {
    // Exact equality, rather than a truncated hash. IDs are local to one EXPLAIN;
    // ordering and UTF-8 storage representation do not change a set's identity.
    sets: Vec<HashSet<ScalarValue>>,
}

impl ExplainDisplay {
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
        let mut replacements = Vec::new();
        plan.apply_expressions(&mut |expr| {
            self.physical(expr, &mut replacements)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        output.push_str(&"  ".repeat(depth));
        output.push_str(&substitute(&text, replacements));
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
            .map(|value| {
                let value = value.to_string();
                let mut chars = value.chars();
                let mut sample: String = chars.by_ref().take(SAMPLE_CHARS).collect();
                if chars.next().is_some() {
                    sample.push('…');
                }
                // Quote/escape the sample even though native physical display
                // leaves strings bare. Commas, quotes, newlines and Unicode are
                // data, not delimiters for this formatter.
                format!("{sample:?}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "<set#{}: {} values, {unique} distinct, sample=[{sample}, …]>",
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
        self.output
            .push_str(&substitute(&plan.display().to_string(), replacements));
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
/// are unquoted), retain the native rendering rather than guess.
fn substitute(text: &str, mut replacements: Vec<(String, String)>) -> String {
    // An empty physical string literal renders as an empty string.
    replacements.retain(|(original, _)| !original.is_empty());
    if replacements.iter().all(|(before, after)| before == after) {
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
