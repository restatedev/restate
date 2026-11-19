// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The conditions a partition read may check before it emits a row.
//!
//! A scan's predicate is applied again by the plan above it, so a read may drop
//! rows early only where doing so can't change the query's outcome: the whole
//! predicate must be pure, unable to fail on valid input, and read the columns
//! it was planned for. Otherwise the read checks nothing and the plan's own
//! filter does all the work, as before:
//!
//! ```text
//! stage = 'finished' AND created_at > 1744020000000     → checked by the read too
//! stage = 'finished' AND length(id) > 3                 → plan only (a function)
//! ```
//!
//! Splitting a predicate instead could change where the left-out part is
//! evaluated: DataFusion may evaluate a conjunction's right side only on the
//! rows its left side keeps, so dropping rows earlier could skip, or newly
//! reach, an error.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};
use datafusion::physical_expr::utils::reassign_expr_columns;

/// `predicate`, rebound to `schema` by column name, if all of it is in the safe
/// subset: Boolean, columns named once in `schema`, and only the expressions
/// and types [`safe`] admits.
pub(crate) fn emission_filter(
    predicate: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Option<Arc<dyn PhysicalExpr>> {
    let mut names = Vec::new();
    collect_columns(predicate, &mut names);
    let unique = |name: &str| {
        schema
            .fields()
            .iter()
            .filter(|field| field.name() == name)
            .count()
            == 1
    };
    if !names.into_iter().all(unique) {
        return None;
    }
    let rebound = reassign_expr_columns(Arc::clone(predicate), schema).ok()?;
    (safe(&rebound, schema)? == DataType::Boolean).then_some(rebound)
}

fn collect_columns<'a>(expr: &'a Arc<dyn PhysicalExpr>, names: &mut Vec<&'a str>) {
    if let Some(column) = expr.downcast_ref::<Column>() {
        names.push(column.name());
    }
    for child in expr.children() {
        collect_columns(child, names);
    }
}

/// The data type of `expr` over `schema`, if `expr` is pure and can't fail on
/// valid input: columns and literals of [`scalar`] types, comparisons of
/// exactly equal types, boolean logic, null checks and literal `IN` lists.
fn safe(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Option<DataType> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        let field = schema.fields().get(column.index())?;
        return (field.name() == column.name())
            .then(|| field.data_type().clone())
            .filter(scalar);
    }
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        // Typed NULLs keep SQL's NULL behavior; an untyped NULL isn't admitted.
        return Some(literal.value().data_type()).filter(scalar);
    }
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        let (left, right) = (safe(binary.left(), schema)?, safe(binary.right(), schema)?);
        return match binary.op() {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => (left == right).then_some(DataType::Boolean),
            Operator::And | Operator::Or => (left == DataType::Boolean
                && right == DataType::Boolean)
                .then_some(DataType::Boolean),
            _ => None,
        };
    }
    if let Some(not) = expr.downcast_ref::<NotExpr>() {
        return (safe(not.arg(), schema)? == DataType::Boolean).then_some(DataType::Boolean);
    }
    if let Some(check) = expr.downcast_ref::<IsNullExpr>() {
        return safe(check.arg(), schema).map(|_| DataType::Boolean);
    }
    if let Some(check) = expr.downcast_ref::<IsNotNullExpr>() {
        return safe(check.arg(), schema).map(|_| DataType::Boolean);
    }
    if let Some(list) = expr.downcast_ref::<InListExpr>() {
        // A column, not an expression, is what the list is matched against.
        list.expr().downcast_ref::<Column>()?;
        let value = safe(list.expr(), schema)?;
        return list
            .list()
            .iter()
            .all(|item| {
                item.downcast_ref::<Literal>()
                    .is_some_and(|literal| literal.value().data_type() == value)
            })
            .then_some(DataType::Boolean);
    }
    None
}

/// The scalar types a read's checks may compare: Boolean, integers, strings
/// and timestamps. Comparing two values of one of these can't fail.
fn scalar(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Timestamp(..)
    )
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::{CastExpr, in_list, lit};

    use super::*;

    /// A read checks a predicate only if all of it is in the subset, rebound by
    /// name to the rows' schema even when planned at another index. A cast,
    /// arithmetic, a comparison across types, an untyped NULL, a column or
    /// literal of an unlisted type, or an ambiguous or missing column anywhere
    /// leaves it all to the plan.
    #[test]
    fn reads_check_only_wholly_pure_non_failing_predicates() {
        let schema = Schema::new(vec![
            Field::new("stage", DataType::Utf8, false),
            Field::new("created_at", DataType::Int64, true),
            Field::new("twice", DataType::Int64, false),
            Field::new("twice", DataType::Int64, false),
            Field::new("ratio", DataType::Float64, false),
        ]);
        let column =
            |name: &str, index| Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>;
        let binary =
            |left, op, right| Arc::new(BinaryExpr::new(left, op, right)) as Arc<dyn PhysicalExpr>;
        let and = |parts: Vec<Arc<dyn PhysicalExpr>>| {
            parts
                .into_iter()
                .reduce(|left, right| binary(left, Operator::And, right))
                .unwrap()
        };
        let safe = and(vec![
            binary(column("stage", 7), Operator::Eq, lit("finished")),
            binary(
                binary(column("created_at", 1), Operator::Gt, lit(5i64)),
                Operator::Or,
                Arc::new(IsNullExpr::new(column("created_at", 1))),
            ),
            in_list(
                column("stage", 0),
                vec![lit("running"), lit(ScalarValue::Utf8(None))],
                &true,
                &schema,
            )
            .unwrap(),
        ]);
        assert_eq!(
            emission_filter(&safe, &schema).unwrap().to_string(),
            "stage@0 = finished AND (created_at@1 > 5 OR created_at@1 IS NULL) \
             AND stage@0 NOT IN (SET) ([running, NULL])"
        );
        for unsafe_part in [
            binary(
                Arc::new(CastExpr::new(column("created_at", 1), DataType::Utf8, None)),
                Operator::Eq,
                lit("5"),
            ),
            binary(
                binary(column("created_at", 1), Operator::Plus, lit(1i64)),
                Operator::Gt,
                lit(5i64),
            ),
            binary(column("created_at", 1), Operator::Gt, lit(5i32)),
            binary(column("stage", 0), Operator::Eq, lit(ScalarValue::Null)),
            binary(column("ratio", 4), Operator::Gt, lit(0.5f64)),
            Arc::new(IsNullExpr::new(column("ratio", 4))),
            binary(column("twice", 2), Operator::Gt, lit(5i64)),
            binary(column("deployment", 5), Operator::Eq, lit("dp")),
        ] {
            assert!(
                emission_filter(&unsafe_part, &schema).is_none(),
                "{unsafe_part}"
            );
            let mixed = and(vec![Arc::clone(&safe), Arc::clone(&unsafe_part)]);
            assert!(emission_filter(&mixed, &schema).is_none(), "{mixed}");
        }
    }
}
