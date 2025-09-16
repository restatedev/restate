// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::Column;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRewriter};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::utils::{conjunction, disjunction};
use datafusion::logical_expr::{BinaryExpr, Case, Join, LogicalPlan, Operator};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::simplify_expressions::simplify_predicates;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

#[derive(Debug)]
pub(crate) struct UseSymmetricHashJoinWhenPartitionKeyIsPresent;

impl UseSymmetricHashJoinWhenPartitionKeyIsPresent {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for UseSymmetricHashJoinWhenPartitionKeyIsPresent {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        let res = plan.transform_up(&|plan| {
            let LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equality,
            }) = &plan
            else {
                return Ok(Transformed::no(plan));
            };

            // if a partition_key exists add that to the join, otherwise use u64::max.
            let left_pk = left
                .schema()
                .qualified_fields_with_unqualified_name("partition_key")
                .first()
                .cloned()
                .map(|c| Column::new(c.0.cloned(), c.1.name().to_owned()));

            // if a partition_key exists add that to the join, otherwise use u64::max.
            let right_pk = right
                .schema()
                .qualified_fields_with_unqualified_name("partition_key")
                .first()
                .cloned()
                .map(|c| Column::new(c.0.cloned(), c.1.name().to_owned()));

            let both_have_pk = left_pk.is_some() && right_pk.is_some();
            if !both_have_pk {
                return Ok(Transformed::no(plan));
            }
            //
            // both sides have a partition_key, lets do a equijoin.
            //
            let mut new_on = on.to_vec();
            new_on.push((
                Expr::Column(left_pk.unwrap()),
                Expr::Column(right_pk.unwrap()),
            ));

            let new_plan = LogicalPlan::Join(Join {
                left: left.clone(),
                right: right.clone(),
                on: new_on,
                filter: filter.clone(),
                join_type: *join_type,
                join_constraint: *join_constraint,
                schema: schema.clone(),
                null_equality: *null_equality,
            });

            Ok(Transformed::yes(new_plan))
        });

        res.data()
    }

    fn name(&self) -> &str {
        "join_checker"
    }
}

#[derive(Debug)]
pub(crate) struct CaseFilterSimplifier;

impl CaseFilterSimplifier {
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for CaseFilterSimplifier {
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        plan.map_expressions(|expr| expr.rewrite(&mut Self::new()))
    }

    fn name(&self) -> &str {
        "case_filter_simplifier"
    }
}

impl TreeNodeRewriter for CaseFilterSimplifier {
    type Node = Expr;

    /// rewrite the expression simplifying any constant expressions
    fn f_up(&mut self, expr: Expr) -> datafusion::common::Result<Transformed<Expr>> {
        let Expr::BinaryExpr(BinaryExpr {
            left,
            op: op @ (Operator::Eq | Operator::NotEq),
            right,
        }) = &expr
        else {
            return Ok(Transformed::no(expr));
        };

        let (case, lit) = match (&**left, &**right) {
            (Expr::Case(case), Expr::Literal(lit, _)) => (case, lit),
            (Expr::Literal(lit, _), Expr::Case(case)) => (case, lit),
            _ => return Ok(Transformed::no(expr)),
        };

        let Some(equals_condition) = simplify_case_equals(case, lit) else {
            return Ok(Transformed::no(expr));
        };

        match op {
            Operator::Eq => Ok(Transformed::yes(equals_condition)),
            // equals condition is true if a = b, otherwise false, and we know a and b are nonnull
            // in that case, not(equals_condition) is true iff a != b is true
            Operator::NotEq => Ok(Transformed::yes(Expr::Not(Box::new(
                equals_condition.clone(),
            )))),
            _ => Ok(Transformed::no(expr)),
        }
    }
}

pub fn simplify_case_equals(case: &Case, equals_lit: &ScalarValue) -> Option<Expr> {
    if equals_lit.is_null() {
        // we don't support comparison to null literals as they are hard to invert
        return None;
    }

    if case.expr.is_some() {
        // right now we only support `CASE WHEN field="foo"` and not `CASE field WHEN "foo"`
        return None;
    }

    let Some(else_expr) = &case.else_expr else {
        // we don't support cases without an ELSE as they may evaluate to null if nothing matches
        return None;
    };

    // only case statements that always evaluate to non-null literals are supported
    let else_lit = else_expr.as_literal()?;
    if else_lit.is_null() {
        return None;
    }

    let mut not_so_far: Vec<Expr> = Vec::new();
    let mut whens: Vec<Expr> = Vec::new();

    for (when, then) in &case.when_then_expr {
        // only case statements that always evaluate to non-null literals are supported
        let then_lit = then.as_literal()?;

        if then_lit.is_null() {
            return None;
        }

        if then_lit.eq(equals_lit) {
            // if the then condition is true AND if no previous branches matched, we produce the target literal
            let when_predicates: Vec<Expr> = not_so_far
                .iter()
                .cloned()
                .chain(std::iter::once(Expr::clone(when).is_true()))
                .collect();

            let when_predicates = simplify_predicates(when_predicates).ok()?;

            if let Some(when_predicate) = conjunction(when_predicates) {
                whens.push(when_predicate)
            } else {
                // if the filter reduced down to nothing, then this case condition is always true
                whens.push(Expr::Literal(ScalarValue::Boolean(Some(true)), None));
            }
        }

        // case statements skip null when conditions, so for branch N to be matched, all earlier branches must evaluate to false or null; ie IsNotTrue
        let not_when = Expr::clone(when).is_not_true();
        not_so_far.push(not_when);
    }

    if else_lit.eq(equals_lit) {
        // if no previous branches matched, we produce the target literal
        let not_so_far = simplify_predicates(not_so_far).ok()?;
        if let Some(when_predicate) = conjunction(not_so_far) {
            whens.push(when_predicate)
        } else {
            // if the filter reduced down to nothing, then this case condition is always true
            whens.push(Expr::Literal(ScalarValue::Boolean(Some(true)), None));
        }
    }

    // this case expression always evaluates to a non null literal and is compared to a non null literal.
    // as a result, we know that if no cases produce the target literal, the predicate evaluates to false (not null)
    Some(disjunction(whens).unwrap_or(Expr::Literal(ScalarValue::Boolean(Some(false)), None)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use datafusion::arrow::array::{BooleanArray, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::optimizer::OptimizerContext;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("status", DataType::Utf8, true),
            Field::new("priority", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    fn create_test_data() -> RecordBatch {
        let schema = create_test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    Some("pending"),
                    Some("active"),
                    Some("completed"),
                    Some("failed"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("high"),
                    Some("low"),
                    Some("medium"),
                    Some("high"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                ])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_case_filter_simplifier() {
        let optimizer = CaseFilterSimplifier::new();
        let config = OptimizerContext::new();

        // Test cases: (name, case_expr, select_expr, expected_predicate)
        let test_cases = vec![
            (
                "simple_case_filter",
                "CASE WHEN status = 'active' THEN 'running' WHEN status = 'pending' then 'waiting' ELSE 'other' END = 'running'",
                "?table?.status = Utf8(\"active\") IS TRUE",
            ),
            (
                "simple_case_antifilter",
                "CASE WHEN status = 'active' THEN 'running' WHEN status = 'pending' then 'waiting' ELSE 'other' END != 'running'",
                "NOT ?table?.status = Utf8(\"active\") IS TRUE",
            ),
            (
                "else_case_filter",
                "CASE WHEN status = 'active' THEN 'running' WHEN status = 'pending' then 'waiting' ELSE 'other' END = 'other'",
                "?table?.status = Utf8(\"active\") IS NOT TRUE AND ?table?.status = Utf8(\"pending\") IS NOT TRUE",
            ),
            (
                "else_case_antifilter",
                "CASE WHEN status = 'active' THEN 'running' WHEN status = 'pending' then 'waiting' ELSE 'other' END != 'other'",
                "NOT ?table?.status = Utf8(\"active\") IS NOT TRUE AND ?table?.status = Utf8(\"pending\") IS NOT TRUE",
            ),
            (
                "multiple_case_filter",
                "CASE WHEN status = 'active' THEN 'running_pending' WHEN status = 'pending' then 'running_pending' ELSE 'other' END = 'running_pending'",
                "?table?.status = Utf8(\"active\") IS TRUE OR ?table?.status = Utf8(\"active\") IS NOT TRUE AND ?table?.status = Utf8(\"pending\") IS TRUE",
            ),
            (
                "multiple_case_antifilter",
                "CASE WHEN status = 'active' THEN 'running_pending' WHEN status = 'pending' then 'running_pending' ELSE 'other' END != 'running_pending'",
                "NOT ?table?.status = Utf8(\"active\") IS TRUE OR ?table?.status = Utf8(\"active\") IS NOT TRUE AND ?table?.status = Utf8(\"pending\") IS TRUE",
            ),
            (
                "null_case_filter",
                "CASE when status = NULL THEN 'a' ELSE 'b' END = 'a'", // false
                "?table?.status = NULL IS TRUE",                       // false
            ),
            (
                "null_case_filter2",
                "CASE when status = NULL THEN 'a' ELSE 'b' END = 'b'", // true
                "?table?.status = NULL IS NOT TRUE",                   // true
            ),
            (
                "null_case_antifilter",
                "CASE when status = NULL THEN 'a' ELSE 'b' END != 'a'", // true
                "NOT ?table?.status = NULL IS TRUE",                    // true
            ),
            (
                "null_case_antifilter2",
                "CASE when status = NULL THEN 'a' ELSE 'b' END != 'b'", // false
                "NOT ?table?.status = NULL IS NOT TRUE",                // false
            ),
            (
                "isnull_case_filter",
                "CASE when status IS NULL THEN 'a' ELSE 'b' END = 'a'", // true if status is null
                "?table?.status IS NULL IS TRUE",                       // true if status is null
            ),
            (
                "isnull_case_filter2",
                "CASE when status IS NULL THEN 'a' ELSE 'b' END = 'b'", // true if status is not null
                "?table?.status IS NULL IS NOT TRUE", // true if status is not null
            ),
            (
                "isnull_case_antifilter",
                "CASE when status IS NULL THEN 'a' ELSE 'b' END != 'a'", // true if status is not null
                "NOT ?table?.status IS NULL IS TRUE", // true if status is not null
            ),
            (
                "isnull_case_antifilter2",
                "CASE when status IS NULL THEN 'a' ELSE 'b' END != 'b'", // true if status is null
                "NOT ?table?.status IS NULL IS NOT TRUE",                // true if status is null
            ),
        ];

        for (name, select_expr, expected_predicate) in test_cases {
            let ctx = SessionContext::new();
            let batch = create_test_data();
            let df = ctx.read_batch(batch).unwrap();

            let parsed_expr = df.parse_sql_expr(select_expr).unwrap();

            let input_plan = df.filter(parsed_expr).unwrap().logical_plan().clone();

            let result = optimizer.rewrite(input_plan.clone(), &config).unwrap();

            match (result.transformed, &result.data) {
                (true, LogicalPlan::Filter(filter)) => {
                    let predicate = filter.predicate.to_string();
                    assert_eq!(predicate, expected_predicate, "{name}");
                }
                (true, _) => panic!("Expected Filter plan for: {}, got: {:?}", name, result.data),
                (false, _) => {
                    panic!("Transformation failed for: {}", name);
                }
            }
        }
    }
}
