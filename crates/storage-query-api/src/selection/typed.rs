// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compiles DataFusion physical predicates into typed storage clauses.
//!
//! [`parse_clause`] translates one condition using a [`SelectionSchema`]'s logical
//! columns. The target selects each column's value type; this adapter converts
//! literals through [`SelectionValue`] and translates SQL NULL semantics. Physical
//! key layouts, encodings, and scan bounds belong to the storage implementation.
//! [`comparison_domain`] exposes the same literal rules to candidate-domain
//! analysis, including `!=` with SQL NULL semantics.
//!
//! # Input and correctness contract
//!
//! Expressions must already be type-checked and coerced by DataFusion. Column
//! names must identify unique columns of the scanned table and match the target's
//! SQL bindings. Column indices and the Arrow schema's nullability are not used:
//! the target's value type determines whether a stored column can be NULL.
//!
//! The caller splits top-level conjunctions and selects conditions that are safe
//! to retain for the scan's lifetime. In particular, static scan planning must
//! exclude whole conjuncts containing dynamic filters. Live filtering can instead
//! pass resolved snapshots, each of which must be sound on its own.
//!
//! `Some(clause)` preserves which values make the condition SQL TRUE. `None`
//! means unsupported, not unsatisfiable: the caller must leave the whole condition
//! to its residual evaluator. The full row predicate can always be retained.
//! Impossible equality alternatives can be discarded, but an unsupported
//! alternative invalidates the entire IN/OR condition. Out-of-domain ordered
//! endpoints remain residual because they do not necessarily imply an empty range.
//!
//! # Supported conditions
//!
//! - Equality and positive literal IN lists, including reversed equality and
//!   same-column ORs of equalities or positive IN lists.
//! - `<`, `<=`, `>`, and `>=`, including reversed comparisons, when the column's
//!   value type supports order-preserving literal conversion.
//! - `IS NULL`. An `IS NULL OR <comparison or IN>` condition on the same column
//!   can discard the NULL arm only when the target column cannot be NULL.
//! - Literal prefixes expressed as case-sensitive `LIKE 'prefix%'` or the
//!   built-in `starts_with`, on columns declaring prefix support. LIKE escapes
//!   are decoded; other wildcard patterns remain residual.
//!
//! Scalar conversion preserves integer ranges and timestamp millisecond precision;
//! it does not parse strings as numbers or round finer-precision timestamps.

use std::borrow::Cow;
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::functions::string::starts_with::StartsWithFunc;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{
    BinaryExpr, Column, InListExpr, IsNullExpr, LikeExpr, Literal,
};

use crate::SelectionSchema;
use crate::selection::{
    LiteralConversionError, SelectionLiteral, SelectionValue, ValuePredicate, ValuePredicateBuilder,
};

use super::domain::Domain;

/// Derives the values for which `column op literal` is SQL TRUE.
///
/// Supports `=`, `!=`, `<`, `<=`, `>`, and `>=` after the source selects the
/// column's logical value type. Returns `None` for unsupported operators or
/// conversions; callers retain those predicates for residual evaluation.
/// Equality conversion alone is sufficient for `!=`; ordered comparisons still
/// require order-preserving conversion.
///
/// For example, nullable `handler != 'alpha'` excludes both `Some("alpha")` and
/// `None`. A comparison with a NULL literal selects nothing. A literal known to
/// be impossible for the column, such as `-1` for `u64`, can be ignored by `!=`,
/// but the column's NULL values are still excluded.
pub fn comparison_domain<V: SelectionValue + Ord + Clone>(
    op: Operator,
    literal: &ScalarValue,
) -> Option<Domain<V>> {
    if op == Operator::NotEq {
        if literal.is_null() {
            return Some(Domain::empty());
        }
        let excluded = match V::from_literal(selection_literal(literal)?) {
            Ok(value) => Some(value),
            Err(LiteralConversionError::Unrepresentable) => None,
            Err(LiteralConversionError::Unsupported) => return None,
        };
        let null = match V::from_literal(SelectionLiteral::Null) {
            Ok(value) => Some(value),
            Err(LiteralConversionError::Unrepresentable) => None,
            Err(LiteralConversionError::Unsupported) => return None,
        };
        return Some(Domain::excluding_values(excluded.into_iter().chain(null)));
    }

    // Reuse the storage compiler's conversions and NULL-safe bounds.
    let predicate = match op {
        Operator::Eq => LiteralPredicate::Equal(literal),
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq => {
            LiteralPredicate::Comparison(op, literal)
        }
        _ => return None,
    };
    predicate.build::<V>().map(Domain::from)
}

/// Literal membership extracted from one column's equality, IN, or OR predicate.
///
/// Borrows and deduplicates literals without converting them or interpreting SQL
/// NULLs. Shared by typed-clause compilation and partition/ID lookup extraction,
/// whose literal-conversion policies differ. A negated list enumerates excluded
/// values and cannot be used as a positive lookup set.
pub struct InList<'a> {
    /// The column shared by every alternative, matched by name.
    pub col: &'a Column,
    /// Literal alternatives, including any SQL NULLs, in unspecified order.
    pub list: HashSet<&'a ScalarValue>,
    /// Whether the input is a standalone NOT IN expression.
    pub negated: bool,
}

/// Compiles one supported condition against the target's logical column types.
///
/// Returns `None` for unknown columns, unsupported expressions, or conversions that
/// cannot preserve matching semantics. A condition proven to match nothing returns
/// a clause containing an empty [`ValuePredicate::In`], rather than `None`.
///
/// This function does not split conjunctions or snapshot dynamic expressions. See
/// the [module contract](self) for the caller's responsibilities.
pub fn parse_clause<T: SelectionSchema>(predicate: &Arc<dyn PhysicalExpr>) -> Option<T::Clause> {
    if let Some(binary) = predicate.downcast_ref::<BinaryExpr>()
        && *binary.op() == Operator::Or
    {
        if let Some(clause) = parse_null_or::<T>(binary) {
            return Some(clause);
        }
        // DataFusion expands short IN lists into ORs. Only normalize unions of
        // literals on the same column; any unsupported arm keeps the whole OR residual.
        let values = InList::parse(predicate, 64)?;
        return T::value_clause(T::column_from_sql(values.col.name())?, values);
    }
    if let Some(like) = predicate.downcast_ref::<LikeExpr>() {
        if like.negated() || like.case_insensitive() {
            return None;
        }
        let (column, literal) = extract_column_literal(like.expr(), like.pattern())?;
        let prefix = like_prefix(literal.value().try_as_str()??)?;
        return T::starts_with_clause(T::column_from_sql(column.name())?, &prefix);
    }
    if let Some(function) = predicate.downcast_ref::<ScalarFunctionExpr>() {
        // Check the implementation, not a name that a different UDF could reuse.
        function.fun().inner().downcast_ref::<StartsWithFunc>()?;
        let [input, prefix] = function.args() else {
            return None;
        };
        let (column, literal) = extract_column_literal(input, prefix)?;
        return T::starts_with_clause(
            T::column_from_sql(column.name())?,
            literal.value().try_as_str()??,
        );
    }
    let (column, builder) = literal_predicate(predicate)?;
    T::value_clause(T::column_from_sql(column.name())?, builder)
}

/// Borrows the input until the target selects a concrete column type.
enum LiteralPredicate<'a> {
    IsNull,
    Equal(&'a ScalarValue),
    Comparison(Operator, &'a ScalarValue),
    In(&'a [Arc<dyn PhysicalExpr>]),
}

/// Builds the second arm of `col IS NULL OR <predicate>`. The NULL arm can only
/// be dropped for value types that have no NULL; otherwise the OR stays residual.
struct NonNullOr<'a>(LiteralPredicate<'a>);

impl<'a> InList<'a> {
    /// Extracts equality, literal IN/NOT IN, or same-column positive OR alternatives.
    ///
    /// Every OR arm must be understood; mixed columns, negated arms, and nonliteral
    /// alternatives reject the whole expression. Each OR level consumes one unit
    /// of `depth_limit`, and a leaf requires a remaining budget greater than one.
    pub fn parse(predicate: &'a Arc<dyn PhysicalExpr>, depth_limit: usize) -> Option<Self> {
        if depth_limit <= 1 {
            return None;
        }

        if let Some(in_list) = predicate.downcast_ref::<InListExpr>() {
            let col = in_list.expr().downcast_ref::<Column>()?;

            let mut list = HashSet::with_capacity(in_list.len());
            for lit in in_list.list() {
                let lit = lit.downcast_ref::<Literal>()?;
                list.insert(lit.value());
            }

            return Some(Self {
                col,
                list,
                negated: in_list.negated(),
            });
        }

        let binary = predicate.downcast_ref::<BinaryExpr>()?;
        match binary.op() {
            Operator::Eq => {
                let (col, lit) = extract_column_literal(binary.left(), binary.right())
                    .or_else(|| extract_column_literal(binary.right(), binary.left()))?;

                Some(Self {
                    col,
                    list: HashSet::from_iter([lit.value()]),
                    negated: false,
                })
            }
            Operator::Or => {
                let mut left = Self::parse(binary.left(), depth_limit - 1)?;
                let right = Self::parse(binary.right(), depth_limit - 1)?;

                if left.col.name() == right.col.name() && !left.negated && !right.negated {
                    left.list.extend(right.list);
                    Some(left)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

fn extract_column_literal<'a>(
    column: &'a Arc<dyn PhysicalExpr>,
    literal: &'a Arc<dyn PhysicalExpr>,
) -> Option<(&'a Column, &'a Literal)> {
    let col = column.downcast_ref::<Column>()?;
    let lit = literal.downcast_ref::<Literal>()?;
    Some((col, lit))
}

/// Parses `col IS NULL OR <comparison on col>`. TopK dynamic filters take this
/// form for NULLS FIRST orderings, including on columns that cannot be NULL.
fn parse_null_or<T: SelectionSchema>(or: &BinaryExpr) -> Option<T::Clause> {
    let (is_null, other) = match (
        or.left().downcast_ref::<IsNullExpr>(),
        or.right().downcast_ref::<IsNullExpr>(),
    ) {
        (Some(is_null), _) => (is_null, or.right()),
        (None, Some(is_null)) => (is_null, or.left()),
        (None, None) => return None,
    };
    let column = is_null.arg().downcast_ref::<Column>()?;
    let (other_column, builder) = literal_predicate(other)?;
    if other_column.name() != column.name() {
        return None;
    }
    T::value_clause(T::column_from_sql(column.name())?, NonNullOr(builder))
}

/// A column compared with literals, not yet bound to its native value type.
fn literal_predicate(predicate: &Arc<dyn PhysicalExpr>) -> Option<(&Column, LiteralPredicate<'_>)> {
    Some(
        if let Some(is_null) = predicate.downcast_ref::<IsNullExpr>() {
            let column = is_null.arg().downcast_ref::<Column>()?;
            (column, LiteralPredicate::IsNull)
        } else if let Some(binary) = predicate.downcast_ref::<BinaryExpr>()
            && matches!(
                binary.op(),
                Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
            )
        {
            let (column, literal, op) = if let Some((column, literal)) =
                extract_column_literal(binary.left(), binary.right())
            {
                (column, literal, *binary.op())
            } else {
                let (column, literal) = extract_column_literal(binary.right(), binary.left())?;
                (column, literal, binary.op().swap()?)
            };
            let builder = if op == Operator::Eq {
                LiteralPredicate::Equal(literal.value())
            } else {
                LiteralPredicate::Comparison(op, literal.value())
            };
            (column, builder)
        } else {
            let in_list = predicate.downcast_ref::<InListExpr>()?;
            if in_list.negated() {
                return None;
            }
            let column = in_list.expr().downcast_ref::<Column>()?;
            (column, LiteralPredicate::In(in_list.list()))
        },
    )
}

/// Recognizes one literal prefix followed by an unescaped `%`.
///
/// Physical LIKE uses backslash escaping (the planner rejects other escape
/// characters). Escapes quote the next character, including non-wildcards.
/// More complex patterns remain residuals. Unescaped prefixes stay borrowed.
fn like_prefix(pattern: &str) -> Option<Cow<'_, str>> {
    let prefix = pattern.strip_suffix('%')?;
    let mut decoded = None;
    let mut chars = prefix.char_indices();
    while let Some((offset, ch)) = chars.next() {
        match ch {
            '%' | '_' => return None,
            '\\' => {
                // A dangling escape here would quote the final `%`.
                let (_, escaped) = chars.next()?;
                let decoded = decoded.get_or_insert_with(|| {
                    let mut value = String::with_capacity(prefix.len());
                    value.push_str(&prefix[..offset]);
                    value
                });
                decoded.push(escaped);
            }
            ch => {
                if let Some(decoded) = &mut decoded {
                    decoded.push(ch);
                }
            }
        }
    }
    Some(match decoded {
        Some(prefix) => Cow::Owned(prefix),
        None => Cow::Borrowed(prefix),
    })
}

impl ValuePredicateBuilder for LiteralPredicate<'_> {
    fn build<V: SelectionValue>(self) -> Option<ValuePredicate<V>> {
        match self {
            Self::IsNull => match V::from_literal(SelectionLiteral::Null) {
                Ok(value) => Some(ValuePredicate::Equal(value)),
                Err(LiteralConversionError::Unrepresentable) => {
                    Some(ValuePredicate::In(Vec::new()))
                }
                Err(LiteralConversionError::Unsupported) => None,
            },
            Self::Equal(value) => collect_values(std::iter::once(Some(value))),
            Self::Comparison(op, value) => {
                if value.is_null() {
                    return Some(ValuePredicate::In(Vec::new()));
                }
                // An out-of-domain endpoint may lie on either side of the whole
                // column domain. Keep it residual rather than treating it as empty.
                let value = V::from_ordered_literal(selection_literal(value)?).ok()?;
                let (lower, upper) = match op {
                    Operator::Gt => (Excluded(value), Unbounded),
                    Operator::GtEq => (Included(value), Unbounded),
                    Operator::Lt | Operator::LtEq => {
                        // SQL comparisons must not include NULL, even below an
                        // upper endpoint. Option values order None before Some.
                        let lower = match V::from_literal(SelectionLiteral::Null) {
                            Ok(null) => Excluded(null),
                            Err(LiteralConversionError::Unrepresentable) => Unbounded,
                            Err(LiteralConversionError::Unsupported) => return None,
                        };
                        let upper = if op == Operator::Lt {
                            Excluded(value)
                        } else {
                            Included(value)
                        };
                        (lower, upper)
                    }
                    _ => return None,
                };
                Some(ValuePredicate::Range { lower, upper })
            }
            Self::In(values) => collect_values(
                values
                    .iter()
                    .map(|expr| expr.downcast_ref::<Literal>().map(Literal::value)),
            ),
        }
    }
}

impl ValuePredicateBuilder for NonNullOr<'_> {
    fn build<V: SelectionValue>(self) -> Option<ValuePredicate<V>> {
        match V::from_literal(SelectionLiteral::Null) {
            Err(LiteralConversionError::Unrepresentable) => self.0.build(),
            Ok(_) | Err(LiteralConversionError::Unsupported) => None,
        }
    }
}

impl ValuePredicateBuilder for InList<'_> {
    fn build<V: SelectionValue>(self) -> Option<ValuePredicate<V>> {
        if self.negated {
            return None;
        }
        collect_values(self.list.into_iter().map(Some))
    }
}

/// Converts supported scalar representations without allocating or coercing strings.
fn selection_literal(value: &ScalarValue) -> Option<SelectionLiteral<'_>> {
    match value {
        ScalarValue::Boolean(Some(value)) => Some(SelectionLiteral::Bool(*value)),
        ScalarValue::UInt8(Some(value)) => Some(SelectionLiteral::Unsigned((*value).into())),
        ScalarValue::UInt16(Some(value)) => Some(SelectionLiteral::Unsigned((*value).into())),
        ScalarValue::UInt32(Some(value)) => Some(SelectionLiteral::Unsigned((*value).into())),
        ScalarValue::UInt64(Some(value)) => Some(SelectionLiteral::Unsigned(*value)),
        ScalarValue::Int8(Some(value)) => Some(SelectionLiteral::Signed((*value).into())),
        ScalarValue::Int16(Some(value)) => Some(SelectionLiteral::Signed((*value).into())),
        ScalarValue::Int32(Some(value)) => Some(SelectionLiteral::Signed((*value).into())),
        ScalarValue::Int64(Some(value)) => Some(SelectionLiteral::Signed(*value)),
        // DataFusion has already coerced timestamp literals to the comparison's
        // timezone. Zoned Arrow timestamps store UTC epoch values regardless of
        // the display timezone, so no additional timezone adjustment belongs here.
        ScalarValue::TimestampSecond(Some(value), _) => value
            .checked_mul(1_000)
            .map(SelectionLiteral::TimestampMillis),
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Some(SelectionLiteral::TimestampMillis(*value))
        }
        // Do not round finer-precision literals: that could change equality or
        // inclusive/exclusive bounds. Non-aligned literals remain residual.
        ScalarValue::TimestampMicrosecond(Some(value), _) if value % 1_000 == 0 => {
            Some(SelectionLiteral::TimestampMillis(value / 1_000))
        }
        ScalarValue::TimestampNanosecond(Some(value), _) if value % 1_000_000 == 0 => {
            Some(SelectionLiteral::TimestampMillis(value / 1_000_000))
        }
        value => value.try_as_str()?.map(SelectionLiteral::String),
    }
}

/// Drops SQL NULL alternatives and values proven impossible for this column.
/// An empty IN represents no matches. Unsupported expressions or literal types
/// reject the whole conjunct, rather than accidentally restricting its domain.
fn collect_values<'a, V: SelectionValue>(
    literals: impl ExactSizeIterator<Item = Option<&'a ScalarValue>>,
) -> Option<ValuePredicate<V>> {
    let capacity = literals.len();
    let mut first = None;
    let mut values = Vec::new();
    for literal in literals {
        let literal = literal?;
        if literal.is_null() {
            continue;
        }
        let value = match V::from_literal(selection_literal(literal)?) {
            Ok(value) => value,
            Err(LiteralConversionError::Unrepresentable) => continue,
            Err(LiteralConversionError::Unsupported) => return None,
        };
        // Equality and singleton lists need no intermediate vector allocation.
        if let Some(first) = first.take() {
            values.reserve(capacity);
            values.push(first);
            values.push(value);
        } else if values.is_empty() {
            first = Some(value);
        } else {
            values.push(value);
        }
    }
    Some(match first {
        Some(value) => ValuePredicate::Equal(value),
        None => ValuePredicate::In(values),
    })
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeBounds};

    use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, LargeStringArray};
    use datafusion::arrow::compute::filter_record_batch;
    use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::DFSchema;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::functions::string::expr_fn::starts_with;
    use datafusion::logical_expr::{Expr, binary_expr, col, lit};
    use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
    use datafusion::physical_expr::planner::logical2physical;
    use datafusion::prelude::SessionContext;

    use restate_types::vqueues::EntryKind;
    use restate_util_string::ReString;

    use crate::selection::{Selection, compile};

    use super::*;

    // The compiler's semantic fixture is independent of concrete production tables.
    crate::define_schema! {
        TestTable {
            #[starts_with]
            service_name: ReString,
            #[starts_with]
            handler: Option<ReString>,
            kind: EntryKind,
        }
    }
    use TestTableClause as Clause;
    use TestTableColumn as LogicalColumn;

    #[test]
    fn timestamp_literals_preserve_millisecond_precision() {
        use restate_types::time::MillisSinceEpoch;

        for literal in [
            ScalarValue::TimestampSecond(Some(2), None),
            ScalarValue::TimestampMillisecond(Some(2_000), None),
            ScalarValue::TimestampMillisecond(Some(2_000), Some("+00:00".into())),
            ScalarValue::TimestampMillisecond(Some(2_000), Some("Europe/Paris".into())),
            ScalarValue::TimestampMicrosecond(Some(2_000_000), None),
            ScalarValue::TimestampNanosecond(Some(2_000_000_000), None),
        ] {
            let value =
                MillisSinceEpoch::from_literal(selection_literal(&literal).unwrap()).unwrap();
            assert_eq!(value.as_u64(), 2_000);
        }
        for literal in [
            ScalarValue::TimestampMicrosecond(Some(1), None),
            ScalarValue::TimestampNanosecond(Some(1), None),
            ScalarValue::TimestampSecond(Some(i64::MAX), None),
        ] {
            assert!(selection_literal(&literal).is_none());
        }
        assert!(
            LiteralPredicate::Comparison(
                Operator::Gt,
                &ScalarValue::TimestampMillisecond(Some(-1), None)
            )
            .build::<MillisSinceEpoch>()
            .is_none()
        );
    }

    crate::define_schema! {
        OtherTarget {
            #[starts_with]
            label: Option<ReString>,
            count: u64,
            balance: i64,
            enabled: bool,
        }
    }

    #[test]
    fn generated_targets_translate_nullable_and_numeric_columns() {
        let schema = Schema::new(vec![
            ArrowField::new("label", DataType::LargeUtf8, true),
            ArrowField::new("count", DataType::UInt64, false),
            ArrowField::new("balance", DataType::Int64, false),
            ArrowField::new("enabled", DataType::Boolean, false),
        ]);
        let expression = col("label")
            .is_null()
            .and(col("count").in_list(
                vec![lit(7u64), lit(u64::MAX), lit(ScalarValue::UInt64(None))],
                false,
            ))
            .and(col("balance").eq(lit(i64::MIN)))
            .and(col("enabled").eq(lit(true)));
        let Selection::Predicates(fields) =
            compile::<OtherTarget>(Some(&logical2physical(&expression, &schema)))
        else {
            panic!("expected typed constraints without a target-specific adapter")
        };
        assert!(matches!(
            fields.for_column(OtherTargetColumn::Label),
            [OtherTargetClause::Label(ValuePredicate::Equal(None))]
        ));
        let [OtherTargetClause::Count(ValuePredicate::In(values))] =
            fields.for_column(OtherTargetColumn::Count)
        else {
            panic!("expected unsigned membership")
        };
        assert_eq!(values, &[7, u64::MAX]);
        assert!(matches!(
            fields.for_column(OtherTargetColumn::Balance),
            [OtherTargetClause::Balance(ValuePredicate::Equal(i64::MIN))]
        ));
        assert!(matches!(
            fields.for_column(OtherTargetColumn::Enabled),
            [OtherTargetClause::Enabled(ValuePredicate::Equal(true))]
        ));

        // A recognized but impossible alternative may be dropped. An unsupported
        // alternative must leave the whole conjunct to the residual evaluator.
        let alternatives = [
            ScalarValue::Int64(Some(-1)),
            ScalarValue::UInt64(Some(u64::MAX)),
        ];
        assert!(matches!(
            collect_values::<u64>(alternatives.iter().map(Some)),
            Some(ValuePredicate::Equal(u64::MAX))
        ));
        let alternatives = [
            ScalarValue::UInt64(Some(7)),
            ScalarValue::Utf8(Some("8".into())),
        ];
        assert!(collect_values::<u64>(alternatives.iter().map(Some)).is_none());
        assert!(matches!(
            LiteralPredicate::Equal(&ScalarValue::UInt64(Some(u64::MAX))).build::<i64>(),
            Some(ValuePredicate::In(values)) if values.is_empty()
        ));
        assert!(matches!(
            LiteralPredicate::Equal(&ScalarValue::Int64(None)).build::<Option<i64>>(),
            Some(ValuePredicate::In(values)) if values.is_empty()
        ));
        assert!(matches!(
            LiteralPredicate::IsNull.build::<u64>(),
            Some(ValuePredicate::In(values)) if values.is_empty()
        ));
    }

    fn schema() -> Schema {
        Schema::new(
            ["service_name", "handler", "kind", "stage"]
                .map(|name| ArrowField::new(name, DataType::LargeUtf8, true))
                .to_vec(),
        )
    }

    fn string(value: Option<&str>) -> Expr {
        lit(ScalarValue::LargeUtf8(value.map(str::to_owned)))
    }

    fn physical(expr: Expr) -> Arc<dyn PhysicalExpr> {
        logical2physical(&expr, &schema())
    }

    fn translate(expr: Expr) -> Selection<TestTable> {
        compile(Some(&physical(expr)))
    }

    fn sql_truth(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> Vec<Option<bool>> {
        let result = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect()
    }

    /// Checks the SQL oracle, candidate soundness, and residual evaluation on the
    /// selected batch. The returned mask lets callers also require useful pruning.
    fn check_selection<T: SelectionSchema>(
        expr: &Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
        expected: &[Option<bool>],
        matches_clause: impl Fn(&T::Clause, usize) -> bool,
    ) -> Vec<bool> {
        let selection = compile::<T>(Some(expr));
        let candidates: Vec<_> = (0..batch.num_rows())
            .map(|row| match &selection {
                Selection::All => true,
                Selection::Empty => false,
                Selection::Predicates(fields) => fields
                    .iter()
                    .flat_map(|(_, clauses)| clauses)
                    .all(|clause| matches_clause(clause, row)),
            })
            .collect();
        assert_candidates_preserve_sql_results(expr, batch, expected, &candidates);
        candidates
    }

    fn assert_candidates_preserve_sql_results(
        expr: &Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
        expected: &[Option<bool>],
        candidates: &[bool],
    ) {
        assert_eq!(sql_truth(expr, batch), expected, "SQL truth: {expr}");
        for (row, truth) in expected.iter().enumerate() {
            assert!(
                *truth != Some(true) || candidates[row],
                "selection lost matching row {row}: {expr}"
            );
        }

        let selected =
            filter_record_batch(batch, &BooleanArray::from(candidates.to_vec())).unwrap();
        let selected_rows = candidates
            .iter()
            .enumerate()
            .filter_map(|(row, &keep)| keep.then_some(row));
        let actual: Vec<_> = selected_rows
            .zip(sql_truth(expr, &selected))
            .filter_map(|(row, truth)| (truth == Some(true)).then_some(row))
            .collect();
        let expected: Vec<_> = expected
            .iter()
            .enumerate()
            .filter_map(|(row, truth)| (*truth == Some(true)).then_some(row))
            .collect();
        assert_eq!(actual, expected, "selection plus residual: {expr}");
    }

    fn matches_value<V: PartialOrd>(predicate: &ValuePredicate<V>, value: &V) -> bool {
        match predicate {
            ValuePredicate::Equal(expected) => value == expected,
            ValuePredicate::In(values) => values.contains(value),
            ValuePredicate::Range { lower, upper } => {
                (lower.as_ref(), upper.as_ref()).contains(&value)
            }
        }
    }

    crate::define_schema! {
        ExclusionTarget {
            #[values(["finished", "inbox", "paused", "running", "suspended"].map(ReString::from_static))]
            stage: ReString,
            handler: Option<ReString>,
        }
    }

    #[derive(Clone, Copy)]
    enum CandidateExpectation {
        /// Candidates must be exactly the rows where the predicate is SQL TRUE.
        Exact,
        /// Extra candidates are allowed; the residual must remove them.
        Conservative,
    }

    #[test]
    fn branches_preserve_correlations_and_discard_only_proven_impossibilities() {
        use CandidateExpectation::{Conservative, Exact};

        const T: Option<bool> = Some(true);
        const F: Option<bool> = Some(false);
        const U: Option<bool> = None;
        // Sample two stages from the schema's complete universe. Handlers remain
        // an open nullable string domain; sampled values never define a universe.
        let fixture = SelectionFixture::cross_product(
            &["paused", "running"],
            &[None, Some("alpha"), Some("beta")],
        );
        #[rustfmt::skip]
        let cases = [
            // SQL predicate                                                    paused: NULL alpha beta   running: NULL alpha beta   candidates   branches
            ("(stage = 'paused' AND handler = 'alpha') OR \
              (stage = 'running' AND handler = 'beta')",                          [U, T, F,                   U, F, T],                  Exact,       2),
            ("(stage = 'paused' AND stage = 'running') OR \
              (stage = 'running' AND handler = 'beta')",                          [F, F, F,                   U, F, T],                  Exact,       1),
            ("(stage = 'not-a-stage' AND handler = 'alpha') OR \
              (stage = 'running' AND handler = 'beta')",                          [F, F, F,                   U, F, T],                  Exact,       1),
            ("stage NOT IN ('finished', 'inbox', 'paused', 'running', 'suspended')", [F, F, F,                   F, F, F],                  Exact,       0),
            ("stage NOT IN ('finished', 'inbox', 'paused', 'suspended')",           [F, F, F,                   T, T, T],                  Exact,       1),
            ("stage NOT IN ('finished', 'inbox', 'paused', 'running', 'suspended') \
              OR length(handler) = 5",                                           [U, T, F,                   U, T, F],                  Conservative,1),
            ("stage = 'running' OR length(handler) = 5",                          [U, T, F,                   T, T, T],                  Conservative,2),
            ("stage = 'running' AND (handler = 'alpha' OR length(handler) = 4)",  [F, F, F,                   U, T, T],                  Conservative,2),
            ("(stage = 'running' AND handler NOT IN ('alpha')) OR \
              (stage = 'paused' AND handler IN ('alpha'))",                       [U, T, F,                   U, F, T],                  Exact,       2),
            ("(stage = 'paused' AND stage = 'running') OR \
              handler NOT IN ('alpha', NULL)",                                   [U, F, U,                   U, F, U],                  Exact,       0),
            ("stage = 'running' OR handler = 'alpha'",                            [U, T, F,                   T, T, T],                  Exact,       2),
            ("(stage = 'running' OR handler = 'alpha') AND \
              (stage = 'paused' OR handler = 'beta')",                            [U, T, F,                   U, F, T],                  Exact,       2),
            ("FALSE OR (stage = 'running' AND handler = 'beta')",                 [F, F, F,                   U, F, T],                  Exact,       1),
            ("TRUE",                                                              [T, T, T,                   T, T, T],                  Exact,       1),
            ("FALSE",                                                             [F, F, F,                   F, F, F],                  Exact,       0),
        ];
        for (sql, truth, candidates, count) in cases {
            let expr = fixture.physical(sql);
            let branches = fixture.branch_domains(std::slice::from_ref(&expr), 4);
            fixture.assert_branch_candidates(&expr, &branches, &truth, candidates);
            assert_eq!(branches.len(), count, "surviving branches: {sql}");
        }

        let unconstrained = fixture.branch_domains(&[], 4);
        assert_eq!(
            unconstrained.len(),
            1,
            "no filters leave one unconstrained branch"
        );
        assert_eq!(
            unconstrained[0].stage.clone().into_values(),
            Some(
                ["finished", "inbox", "paused", "running", "suspended"]
                    .map(ReString::from_static)
                    .to_vec()
            ),
            "normalization uses the declared universe, including unsampled values"
        );
        let exclusion =
            fixture.physical("stage NOT IN ('finished', 'inbox', 'paused', 'suspended')");
        let remaining = fixture.branch_domains(&[exclusion], 4);
        assert_eq!(
            remaining[0].stage.clone().into_values(),
            Some(vec![ReString::from_static("running")]),
            "finite normalization enumerates exactly the surviving value"
        );
        fixture.assert_branch_candidates(
            &fixture.physical("TRUE"),
            &unconstrained,
            &[T, T, T, T, T, T],
            Exact,
        );

        // Invariant: per-column guarantees can broaden paired alternatives, so
        // deriving from a whole branch must happen before that projection.
        let expr = fixture.physical(
            "(stage = 'paused' AND handler = 'alpha') OR \
             (stage = 'running' AND handler = 'beta')",
        );
        let branches = fixture.branch_domains(&[expr], 4);
        let guarantees = ExclusionTarget::guarantees(&branches).unwrap();
        let crossed_pair = (ReString::from("paused"), Some(ReString::from("beta")));
        assert!(
            guarantees.contains(&crossed_pair),
            "guarantees admit crossed pairs"
        );
        assert!(
            !branches.iter().any(|branch| branch.contains(&crossed_pair)),
            "branches preserve column relationships",
        );
    }

    #[test]
    fn branch_budgets_broaden_safely_and_preserve_independent_conjuncts() {
        use CandidateExpectation::Conservative;

        const T: Option<bool> = Some(true);
        const F: Option<bool> = Some(false);
        const U: Option<bool> = None;
        let fixture = SelectionFixture::cross_product(
            &["paused", "running"],
            &[None, Some("alpha"), Some("beta")],
        );
        #[rustfmt::skip]
        let cases = [
            // SQL predicate                                                    paused: NULL alpha beta   running: NULL alpha beta
            ("(stage = 'paused' AND handler = 'alpha') OR \
              (stage = 'running' AND handler = 'beta')",                          [U, T, F,                   U, F, T]),
            ("(stage = 'running' OR handler = 'alpha') AND \
              (stage = 'paused' OR handler = 'beta')",                            [U, T, F,                   U, F, T]),
        ];
        for (sql, truth) in cases {
            let expr = fixture.physical(sql);
            let branches = fixture.branch_domains(std::slice::from_ref(&expr), 1);
            assert_eq!(branches.len(), 1, "branch limit: {sql}");
            fixture.assert_branch_candidates(&expr, &branches, &truth, Conservative);

            // Exercise both one expression and separately supplied conjuncts.
            let restricted = fixture.physical(&format!("stage = 'running' AND ({sql})"));
            for filters in [
                vec![Arc::clone(&restricted)],
                vec![fixture.physical("stage = 'running'"), Arc::clone(&expr)],
            ] {
                let branches = fixture.branch_domains(&filters, 1);
                fixture.assert_branch_candidates(
                    &restricted,
                    &branches,
                    &[F, F, F, U, F, T],
                    Conservative,
                );
                assert!(
                    branches
                        .iter()
                        .all(|branch| !branch.stage.contains(&ReString::from("paused"))),
                    "independent conjunct survives branch limit: {sql}",
                );
            }
        }

        // Each supplied disjunction fits on its own, but their product exceeds
        // the limit. Keeping the second filter intact must retain the first and
        // the independent stage restriction.
        let filters = [
            fixture.physical("stage = 'running'"),
            fixture.physical("stage = 'running' OR handler = 'alpha'"),
            fixture.physical("stage = 'paused' OR handler = 'beta'"),
        ];
        let expr = fixture.physical(
            "stage = 'running' AND (stage = 'running' OR handler = 'alpha') \
             AND (stage = 'paused' OR handler = 'beta')",
        );
        let branches = fixture.branch_domains(&filters, 2);
        fixture.assert_branch_candidates(&expr, &branches, &[F, F, F, U, F, T], Conservative);
        assert_eq!(branches.len(), 2, "bounded product of separate filters");
        assert!(
            branches
                .iter()
                .all(|branch| !branch.stage.contains(&ReString::from("paused")))
        );

        // Exhaust the traversal depth without constructing exponentially many
        // alternatives. The independently supplied stage restriction still holds.
        let handler = fixture.physical("handler = 'alpha'");
        let mut deep = Arc::clone(&handler);
        for _ in 0..70 {
            deep = Arc::new(BinaryExpr::new(deep, Operator::Or, Arc::clone(&handler)));
        }
        let stage = fixture.physical("stage = 'running'");
        let filters = [Arc::clone(&stage), Arc::clone(&deep)];
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(stage, Operator::And, deep));
        let branches = fixture.branch_domains(&filters, 1);
        fixture.assert_branch_candidates(&expr, &branches, &[F, F, F, U, T, F], Conservative);
        assert!(
            branches
                .iter()
                .all(|branch| !branch.stage.contains(&ReString::from("paused")))
        );
    }

    impl ExclusionTargetDomains {
        fn contains(&self, (stage, handler): &(ReString, Option<ReString>)) -> bool {
            self.stage.contains(stage) && self.handler.contains(handler)
        }
    }

    #[test]
    fn typed_comparison_domains_select_exactly_the_sql_true_rows() {
        const T: Option<bool> = Some(true);
        const F: Option<bool> = Some(false);
        const U: Option<bool> = None;

        let stages = ["finished", "inbox", "paused", "running", "suspended"];
        #[rustfmt::skip]
        let stage_cases = [
            // SQL predicate                        finished inbox paused running suspended
            ("stage = 'running'",                    [F,      F,    F,     T,      F]),
            ("stage != 'running'",                   [T,      T,    T,     F,      T]),
            ("'running' != stage",                   [T,      T,    T,     F,      T]),
            ("stage != 'not-a-stage'",               [T,      T,    T,     T,      T]),
            ("stage != NULL",                        [U,      U,    U,     U,      U]),
        ];

        let handlers = [None, Some(""), Some("alpha"), Some("beta")];
        #[rustfmt::skip]
        let handler_cases = [
            // SQL predicate                                    NULL  ''  alpha beta
            ("handler = 'alpha'",                                [U,   F,  T,    F]),
            ("handler != 'alpha'",                               [U,   T,  F,    T]),
            ("'alpha' != handler",                               [U,   T,  F,    T]),
            ("handler != NULL",                                  [U,   U,  U,    U]),
            ("handler < 'alpha'",                                [U,   T,  F,    F]),
            ("handler <= 'alpha'",                               [U,   T,  T,    F]),
            ("handler > 'alpha'",                                [U,   F,  F,    T]),
            ("handler >= 'alpha'",                               [U,   F,  T,    T]),
            ("handler IN ('alpha', NULL)",                       [U,   U,  T,    U]),
            ("handler != 'alpha' OR handler = 'alpha'",           [U,   T,  T,    T]),
            ("handler != 'alpha' AND handler = 'alpha'",          [U,   F,  F,    F]),
        ];

        let fixture = SelectionFixture::cross_product(&stages, &handlers);
        // Invariant: the typed domain selects every SQL-TRUE row and no others,
        // including when comparisons are combined by the Boolean analyzer.
        for (sql, truth) in stage_cases {
            fixture.assert_exact_comparison_domain(
                sql,
                ExclusionTargetColumn::Stage,
                &fixture.truth_by_stage(&truth),
            );
        }
        for (sql, truth) in handler_cases {
            fixture.assert_exact_comparison_domain(
                sql,
                ExclusionTargetColumn::Handler,
                &fixture.truth_by_handler(&truth),
            );
        }
    }

    #[test]
    fn negated_membership_domains_match_sql_and_intersect_inclusions() {
        const T: Option<bool> = Some(true);
        const F: Option<bool> = Some(false);
        const U: Option<bool> = None;

        let stages = ["finished", "inbox", "paused", "running", "suspended"];
        // Every case also requires: stage IN ('running', 'paused').
        #[rustfmt::skip]
        let stage_cases = [
            // Additional exclusion                     finished inbox paused running suspended
            ("stage NOT IN ('inbox')",                   [F,      F,    T,     T,      F]),
            ("stage NOT IN ('running')",                 [F,      F,    T,     F,      F]),
            ("stage NOT IN ('running', 'running')",      [F,      F,    T,     F,      F]),
            ("stage NOT IN ('running', 'paused')",       [F,      F,    F,     F,      F]),
            ("stage NOT IN ('inbox', NULL)",             [F,      F,    U,     U,      F]),
            ("stage NOT IN ('not-a-stage')",             [F,      F,    T,     T,      F]),
            ("stage NOT IN ('not-a-stage', 'running')",  [F,      F,    T,     F,      F]),
        ];

        // Unknown stage names are ordinary strings outside the complete stage
        // domain. They are not NULL and do not make the comparison unsupported.
        #[rustfmt::skip]
        let unknown_stage_cases = [
            // SQL predicate                                 finished inbox paused running suspended
            ("stage IN ('not-a-stage')",                      [F,      F,    F,     F,      F]),
            ("stage NOT IN ('not-a-stage')",                  [T,      T,    T,     T,      T]),
            ("stage IN ('running', 'not-a-stage')",           [F,      F,    F,     T,      F]),
            ("stage NOT IN ('running', 'not-a-stage')",       [T,      T,    T,     F,      T]),
            ("stage IN ('not-a-stage', 'running')",           [F,      F,    F,     T,      F]),
            ("stage NOT IN ('not-a-stage', 'running')",       [T,      T,    T,     F,      T]),
            ("stage IN ('not-a-stage', NULL)",               [U,      U,    U,     U,      U]),
            ("stage NOT IN ('not-a-stage', NULL)",           [U,      U,    U,     U,      U]),
            ("stage IN ('running', 'not-a-stage', NULL)",    [U,      U,    U,     T,      U]),
            ("stage NOT IN ('running', 'not-a-stage', NULL)",[U,      U,    U,     F,      U]),
        ];

        let handlers = [None, Some(""), Some("alpha"), Some("beta")];
        #[rustfmt::skip]
        let handler_cases = [
            // SQL predicate                                                 NULL  ''  alpha beta
            ("handler NOT IN ('alpha')",                                      [U,   T,  F,    T]),
            ("handler NOT IN ('alpha', 'alpha', 'beta')",                      [U,   T,  F,    F]),
            ("handler NOT IN ('alpha', NULL)",                                [U,   U,  F,    U]),
            ("handler NOT IN (NULL, 'alpha')",                                [U,   U,  F,    U]),
            ("handler NOT IN (NULL)",                                         [U,   U,  U,    U]),
            ("handler NOT IN ('alpha') OR handler = 'alpha'",                  [U,   T,  T,    T]),
            ("handler NOT IN ('alpha') AND handler IN ('alpha', 'beta')",      [U,   F,  F,    T]),
            ("handler NOT IN ('alpha', NULL) OR handler = 'beta'",             [U,   U,  F,    T]),
        ];

        let fixture = SelectionFixture::cross_product(&stages, &handlers);
        // Invariant: exclusions combine with inclusions by intersection, and the
        // resulting domain selects exactly SQL TRUE, including NULL-containing lists.
        for (exclusion, truth) in stage_cases {
            let sql = format!("stage IN ('running', 'paused') AND ({exclusion})");
            fixture.assert_exact_comparison_domain(
                &sql,
                ExclusionTargetColumn::Stage,
                &fixture.truth_by_stage(&truth),
            );
        }
        for (sql, truth) in unknown_stage_cases {
            fixture.assert_exact_comparison_domain(
                sql,
                ExclusionTargetColumn::Stage,
                &fixture.truth_by_stage(&truth),
            );
        }
        for (sql, truth) in handler_cases {
            fixture.assert_exact_comparison_domain(
                sql,
                ExclusionTargetColumn::Handler,
                &fixture.truth_by_handler(&truth),
            );
        }
    }

    #[test]
    fn comparison_domains_distinguish_impossible_literals_from_unsupported_conversions() {
        let samples = [None, Some(0u64), Some(7), Some(u64::MAX)];
        #[rustfmt::skip]
        let cases = [
            // Literal in `nullable_u64 != literal`   NULL   0      7      MAX
            (ScalarValue::UInt64(Some(7)),             [false, true,  false, true]),
            (ScalarValue::Int64(Some(-1)),             [false, true,  true,  true]),
            (ScalarValue::UInt64(None),                [false, false, false, false]),
        ];
        for (literal, expected) in cases {
            let domain = comparison_domain::<Option<u64>>(Operator::NotEq, &literal).unwrap();
            assert_eq!(
                samples.map(|value| domain.contains(&value)),
                expected,
                "!= {literal}"
            );
        }

        // An impossible equality value removes nothing from a non-nullable column.
        let negative = ScalarValue::Int64(Some(-1));
        assert!(
            comparison_domain::<u64>(Operator::NotEq, &negative)
                .unwrap()
                .is_all()
        );
        // An out-of-domain ordered endpoint does not imply an empty domain.
        assert!(comparison_domain::<u64>(Operator::Lt, &negative).is_none());
        // Unsupported conversions must stay residual, rather than exclude values.
        let numeric_string = ScalarValue::Utf8(Some("7".into()));
        assert!(comparison_domain::<u64>(Operator::NotEq, &numeric_string).is_none());

        // Equality conversion is enough for !=, even without SQL-order conversion.
        let invocation = ScalarValue::Utf8(Some("invocation".into()));
        let domain = comparison_domain::<EntryKind>(Operator::NotEq, &invocation).unwrap();
        assert!(!domain.contains(&EntryKind::Invocation));
        assert!(domain.contains(&EntryKind::StateMutation));
        assert!(comparison_domain::<EntryKind>(Operator::Gt, &invocation).is_none());
    }

    #[test]
    fn exclusions_preserve_sql_truth_and_residual_results() {
        use CandidateExpectation::{Conservative, Exact};

        // SQL truth values: TRUE, FALSE, UNKNOWN. Only TRUE survives WHERE.
        const T: Option<bool> = Some(true);
        const F: Option<bool> = Some(false);
        const U: Option<bool> = None;

        // Promote Conservative cases to Exact as exclusion pushdown is implemented.
        let stages = ["finished", "inbox", "paused", "running", "suspended"];
        #[rustfmt::skip]
        let stage_cases = [
            // SQL predicate                        finished inbox paused running suspended   candidates
            ("stage = 'running'",                    [F,      F,    F,     T,      F],        Exact),
            ("stage != 'running'",                   [T,      T,    T,     F,      T],        Conservative),
            ("stage != 'not-a-stage'",               [T,      T,    T,     T,      T],        Conservative),
            ("stage NOT IN ('running', NULL)",       [U,      U,    U,     F,      U],        Conservative),
        ];

        // Every case also requires: stage IN ('running', 'paused').
        #[rustfmt::skip]
        let stage_membership_cases = [
            // Additional exclusion                  finished inbox paused running suspended   candidates
            ("stage NOT IN ('inbox')",                [F,      F,    T,     T,      F],        Exact),
            ("stage NOT IN ('running')",              [F,      F,    T,     F,      F],        Conservative),
            ("stage NOT IN ('running', 'paused')",    [F,      F,    F,     F,      F],        Conservative),
            ("stage NOT IN ('inbox', NULL)",          [F,      F,    U,     U,      F],        Conservative),
        ];

        let handlers = [None, Some(""), Some("alpha"), Some("beta")];
        #[rustfmt::skip]
        let handler_cases = [
            // SQL predicate                        NULL  ''  'alpha' 'beta'   candidates
            ("handler = 'alpha'",                    [U,   F,  T,      F],      Exact),
            ("handler = NULL",                       [U,   U,  U,      U],      Exact),
            ("handler IS NULL",                      [T,   F,  F,      F],      Exact),
            ("handler IS NOT NULL",                  [F,   T,  T,      T],      Conservative),
            ("handler != 'alpha'",                   [U,   T,  F,      T],      Conservative),
            ("handler != NULL",                      [U,   U,  U,      U],      Conservative),
            ("handler IN ('alpha', NULL)",           [U,   U,  T,      U],      Exact),
            ("handler NOT IN ('alpha')",             [U,   T,  F,      T],      Conservative),
            ("handler NOT IN ('alpha', NULL)",       [U,   U,  F,      U],      Conservative),
            ("handler NOT IN (NULL)",                [U,   U,  U,      U],      Conservative),
            // NOT preserves UNKNOWN; it cannot simply complement the SQL-TRUE set.
            ("NOT (handler IN ('alpha', NULL))",      [U,   U,  F,      U],      Conservative),
        ];

        let fixture = SelectionFixture::cross_product(&stages, &handlers);

        // Invariant: selection never loses a SQL-TRUE row, and selection plus
        // residual produces exactly the SQL result. Exact cases also forbid extras.
        // Adding a supported conjunct must still enforce that conjunct.
        for (sql, truth, candidates) in stage_cases {
            let expected = fixture.truth_by_stage(&truth);
            fixture.assert_preserves_sql_results(sql, &expected, candidates);
            fixture.assert_running_conjunct_is_enforced(sql, &expected, candidates);
        }
        for (exclusion, truth, candidates) in stage_membership_cases {
            let sql = format!("stage IN ('running', 'paused') AND ({exclusion})");
            let expected = fixture.truth_by_stage(&truth);
            fixture.assert_preserves_sql_results(&sql, &expected, candidates);
            fixture.assert_running_conjunct_is_enforced(&sql, &expected, candidates);
        }
        for (sql, truth, candidates) in handler_cases {
            let expected = fixture.truth_by_handler(&truth);
            fixture.assert_preserves_sql_results(sql, &expected, candidates);
            fixture.assert_running_conjunct_is_enforced(sql, &expected, candidates);
        }
    }

    struct SelectionFixture {
        rows: Vec<(ReString, Option<ReString>)>,
        handlers_per_stage: usize,
        batch: RecordBatch,
        schema: DFSchema,
        context: SessionContext,
    }

    impl SelectionFixture {
        fn branch_domains(
            &self,
            filters: &[Arc<dyn PhysicalExpr>],
            limit: usize,
        ) -> Vec<ExclusionTargetDomains> {
            let branches =
                ExclusionTarget::analyze(filters, std::num::NonZeroUsize::new(limit).unwrap())
                    .unwrap();
            assert!(branches.len() <= limit, "bounded branch count");
            branches
        }

        fn assert_branch_candidates(
            &self,
            expr: &Arc<dyn PhysicalExpr>,
            branches: &[ExclusionTargetDomains],
            truth: &[Option<bool>],
            expectation: CandidateExpectation,
        ) {
            let candidates: Vec<_> = self
                .rows
                .iter()
                .map(|row| branches.iter().any(|branch| branch.contains(row)))
                .collect();
            assert_candidates_preserve_sql_results(expr, &self.batch, truth, &candidates);
            if matches!(expectation, CandidateExpectation::Exact) {
                assert_eq!(
                    candidates,
                    truth
                        .iter()
                        .map(|value| *value == Some(true))
                        .collect::<Vec<_>>(),
                    "exact branch candidates: {expr}",
                );
            }
        }

        // Cross every stage with every handler so NULL behavior cannot accidentally
        // depend on a correlation between columns in the fixture.
        fn cross_product(stages: &[&str], handlers: &[Option<&str>]) -> Self {
            let mut rows = Vec::new();
            for &stage in stages {
                for &handler in handlers {
                    rows.push((ReString::from(stage), handler.map(ReString::from)));
                }
            }
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    ArrowField::new("stage", DataType::LargeUtf8, false),
                    ArrowField::new("handler", DataType::LargeUtf8, true),
                ])),
                vec![
                    Arc::new(LargeStringArray::from(
                        rows.iter()
                            .map(|(stage, _)| stage.as_str())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(LargeStringArray::from(
                        rows.iter()
                            .map(|(_, handler)| handler.as_ref().map(ReString::as_str))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let schema = DFSchema::try_from(batch.schema().as_ref().clone()).unwrap();
            Self {
                rows,
                handlers_per_stage: handlers.len(),
                batch,
                schema,
                context: SessionContext::new(),
            }
        }

        fn truth_by_stage(&self, truth: &[Option<bool>]) -> Vec<Option<bool>> {
            assert_eq!(truth.len() * self.handlers_per_stage, self.rows.len());
            truth
                .iter()
                .flat_map(|&value| std::iter::repeat_n(value, self.handlers_per_stage))
                .collect()
        }

        fn truth_by_handler(&self, truth: &[Option<bool>]) -> Vec<Option<bool>> {
            assert_eq!(truth.len(), self.handlers_per_stage);
            truth.repeat(self.rows.len() / self.handlers_per_stage)
        }

        fn assert_exact_comparison_domain(
            &self,
            sql: &str,
            field: ExclusionTargetColumn,
            expected: &[Option<bool>],
        ) {
            fn typed_domain<V: SelectionValue + Ord + Clone>(
                expr: &Arc<dyn PhysicalExpr>,
                column: &str,
            ) -> Domain<V> {
                crate::selection::domain::column_domain(
                    std::slice::from_ref(expr),
                    column,
                    |op, literal| {
                        Ok(comparison_domain(op, literal)
                            .expect("test comparison must be supported"))
                    },
                )
                .unwrap()
            }

            let expr = self.physical(sql);
            assert_eq!(sql_truth(&expr, &self.batch), expected, "SQL truth: {sql}");
            let column = ExclusionTarget::tag(field);
            let selected: Vec<_> = match field {
                ExclusionTargetColumn::Stage => {
                    let domain = typed_domain::<ReString>(&expr, column);
                    self.rows
                        .iter()
                        .map(|(stage, _)| domain.contains(stage))
                        .collect()
                }
                ExclusionTargetColumn::Handler => {
                    let domain = typed_domain::<Option<ReString>>(&expr, column);
                    self.rows
                        .iter()
                        .map(|(_, handler)| domain.contains(handler))
                        .collect()
                }
            };
            assert_eq!(
                selected,
                expected
                    .iter()
                    .map(|truth| *truth == Some(true))
                    .collect::<Vec<_>>(),
                "exact comparison domain: {sql}",
            );
        }

        fn assert_preserves_sql_results(
            &self,
            sql: &str,
            expected: &[Option<bool>],
            expectation: CandidateExpectation,
        ) -> Vec<bool> {
            let expr = self.physical(sql);
            let candidates =
                check_selection::<ExclusionTarget>(&expr, &self.batch, expected, |clause, row| {
                    match clause {
                        ExclusionTargetClause::Stage(predicate) => {
                            matches_value(predicate, &self.rows[row].0)
                        }
                        ExclusionTargetClause::Handler(predicate) => {
                            matches_value(predicate, &self.rows[row].1)
                        }
                    }
                });
            if matches!(expectation, CandidateExpectation::Exact) {
                assert_eq!(
                    candidates,
                    expected
                        .iter()
                        .map(|truth| *truth == Some(true))
                        .collect::<Vec<_>>(),
                    "exact selection: {sql} ({expr})"
                );
            }
            candidates
        }

        fn assert_running_conjunct_is_enforced(
            &self,
            sql: &str,
            expected: &[Option<bool>],
            expectation: CandidateExpectation,
        ) {
            let sql = format!("stage = 'running' AND ({sql})");
            let expected: Vec<_> = expected
                .iter()
                .zip(&self.rows)
                .map(|(&truth, (stage, _))| {
                    if stage.as_str() == "running" {
                        truth
                    } else {
                        Some(false)
                    }
                })
                .collect();
            let candidates = self.assert_preserves_sql_results(&sql, &expected, expectation);
            for (row, selected) in candidates.into_iter().enumerate() {
                if selected {
                    assert_eq!(
                        self.rows[row].0.as_str(),
                        "running",
                        "lost stage constraint: {sql}"
                    );
                }
            }
        }

        fn physical(&self, sql: &str) -> Arc<dyn PhysicalExpr> {
            // Type the fixture's SQL literals directly as LargeUtf8, matching its
            // columns, so these cases exercise literal clauses rather than casts.
            let expr = self
                .context
                .parse_sql_expr(sql, &self.schema)
                .expect(sql)
                .transform(|expr| match expr {
                    Expr::Literal(value, metadata)
                        if matches!(value, ScalarValue::Utf8(_) | ScalarValue::Null) =>
                    {
                        Ok(Transformed::yes(Expr::Literal(
                            value.cast_to(&DataType::LargeUtf8)?,
                            metadata,
                        )))
                    }
                    expr => Ok(Transformed::no(expr)),
                })
                .expect(sql)
                .data;
            self.context
                .create_physical_expr(expr, &self.schema)
                .expect(sql)
        }
    }

    #[test]
    fn ordered_comparisons_preserve_endpoints_and_exclude_null() {
        let inputs = vec![
            None,
            Some(""),
            Some("LargeStat"),
            Some("LargeState"),
            Some("LargeState\0"),
            Some("LargeStateful"),
            Some("Z"),
            Some("é"),
        ];
        let array: ArrayRef = Arc::new(LargeStringArray::from(inputs.clone()));
        let batch = RecordBatch::try_new(Arc::new(schema()), vec![array; 4]).unwrap();
        for op in [Operator::Gt, Operator::GtEq, Operator::Lt, Operator::LtEq] {
            for expression in [
                binary_expr(col("handler"), op, string(Some("LargeState"))),
                binary_expr(
                    string(Some("LargeState")),
                    op.swap().unwrap(),
                    col("handler"),
                ),
            ] {
                let expression = physical(expression);
                let Selection::Predicates(fields) = compile::<TestTable>(Some(&expression)) else {
                    panic!("expected range for {expression}")
                };
                let [Clause::Handler(ValuePredicate::Range { lower, upper })] =
                    fields.for_column(LogicalColumn::Handler)
                else {
                    panic!("expected nullable range")
                };
                let bounds = (lower.clone(), upper.clone());
                let result = expression
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap();
                let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
                for (index, value) in inputs.iter().enumerate() {
                    assert_eq!(
                        bounds.contains(&value.map(ReString::from)),
                        result.is_valid(index) && result.value(index),
                        "{expression}, {value:?}"
                    );
                }
            }
            assert!(
                matches!(LiteralPredicate::Comparison(op, &ScalarValue::LargeUtf8(None)).build::<Option<ReString>>(),
                Some(ValuePredicate::In(values)) if values.is_empty())
            );
            // Out-of-domain endpoints do not mean that an ordered comparison is false.
            assert!(
                LiteralPredicate::Comparison(op, &ScalarValue::Int64(Some(-1)))
                    .build::<u64>()
                    .is_none()
            );
            assert!(
                LiteralPredicate::Comparison(op, &ScalarValue::UInt64(Some(u64::MAX)))
                    .build::<i64>()
                    .is_none()
            );
        }

        let Selection::Predicates(fields) =
            translate(col("service_name").gt_eq(string(Some("LargeState"))))
        else {
            panic!("expected service range")
        };
        assert!(matches!(fields.for_column(LogicalColumn::ServiceName),
            [Clause::ServiceName(ValuePredicate::Range { lower: Included(value), upper: Unbounded })] if value.as_str() == "LargeState"));

        let schema = Schema::new(vec![ArrowField::new("count", DataType::UInt64, false)]);
        let expression = logical2physical(&col("count").lt(lit(7u64)), &schema);
        let Selection::Predicates(fields) = compile::<OtherTarget>(Some(&expression)) else {
            panic!("expected numeric range")
        };
        assert!(matches!(
            fields.for_column(OtherTargetColumn::Count),
            [OtherTargetClause::Count(ValuePredicate::Range {
                lower: Unbounded,
                upper: Excluded(7)
            })]
        ));
    }

    #[test]
    fn prefixes_preserve_datafusion_escaping_and_null_semantics() {
        let inputs = vec![
            None,
            Some(""),
            Some("alpha"),
            Some("alp"),
            Some("Alpha"),
            Some("beta"),
            Some("a_"),
            Some("a_tail"),
            Some("a%tail"),
            Some(r"a\tail"),
            Some("a\n"),
            Some("é\0tail"),
        ];
        let array: ArrayRef = Arc::new(LargeStringArray::from(inputs.clone()));
        let batch = RecordBatch::try_new(Arc::new(schema()), vec![array; 4]).unwrap();
        for (pattern, expected) in [
            ("alp%", "alp"),
            ("%", ""),
            (r"a\_%", "a_"),
            (r"a\%%", "a%"),
            (r"a\\%", r"a\"),
            (r"\a%", "a"),
            ("é\0%", "é\0"),
        ] {
            // Exercise both the physical function and the LIKE form produced by
            // DataFusion's starts_with simplification.
            for expression in [
                col("handler").like(string(Some(pattern))),
                starts_with(col("handler"), string(Some(expected))),
            ] {
                let expression = physical(expression);
                let Selection::Predicates(fields) = compile::<TestTable>(Some(&expression)) else {
                    panic!("expected prefix for {expression}")
                };
                let [Clause::HandlerStartsWith(prefix)] = fields.for_column(LogicalColumn::Handler)
                else {
                    panic!("expected nullable-handler prefix")
                };
                assert_eq!(prefix.as_str(), expected);
                let result = expression
                    .evaluate(&batch)
                    .unwrap()
                    .into_array(batch.num_rows())
                    .unwrap();
                let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
                for (index, value) in inputs.iter().enumerate() {
                    assert_eq!(
                        result.is_valid(index) && result.value(index),
                        value.is_some_and(|value| value.starts_with(prefix.as_str())),
                        "{expression}, input {value:?}",
                    );
                }
            }
        }

        let schema = Schema::new(vec![ArrowField::new("label", DataType::LargeUtf8, true)]);
        let expression = logical2physical(&col("label").like(string(Some("pre%"))), &schema);
        let Selection::Predicates(fields) = compile::<OtherTarget>(Some(&expression)) else {
            panic!("expected prefix for another target")
        };
        assert!(matches!(fields.for_column(OtherTargetColumn::Label),
            [OtherTargetClause::LabelStartsWith(prefix)] if prefix.as_str() == "pre"));
        assert!(matches!(like_prefix("alp%"), Some(Cow::Borrowed("alp"))));
    }

    fn assert_static_service(filter: &Selection<TestTable>) {
        let Selection::Predicates(fields) = filter else {
            panic!("expected static service constraint")
        };
        let [Clause::ServiceName(ValuePredicate::Equal(name))] =
            fields.for_column(LogicalColumn::ServiceName)
        else {
            panic!("expected service equality")
        };
        assert_eq!(name.as_str(), "alpha");
        assert!(fields.for_column(LogicalColumn::Kind).is_empty());
    }

    #[test]
    fn equality_membership_and_nulls_produce_typed_clauses() {
        let filter = translate(
            string(Some("alpha"))
                .eq(col("service_name"))
                .and(col("handler").is_null())
                .and(col("kind").in_list(
                    vec![
                        string(Some("state-mutation")),
                        string(Some("invalid")),
                        string(None),
                        string(Some("invocation")),
                    ],
                    false,
                )),
        );
        let Selection::Predicates(fields) = filter else {
            panic!("expected clauses")
        };
        let [Clause::ServiceName(ValuePredicate::Equal(name))] =
            fields.for_column(LogicalColumn::ServiceName)
        else {
            panic!("expected reversed equality")
        };
        assert_eq!(name.as_str(), "alpha");
        assert!(matches!(
            fields.for_column(LogicalColumn::Handler),
            [Clause::Handler(ValuePredicate::Equal(None))]
        ));
        let [Clause::Kind(ValuePredicate::In(values))] = fields.for_column(LogicalColumn::Kind)
        else {
            panic!("expected membership")
        };
        assert_eq!(values, &[EntryKind::StateMutation, EntryKind::Invocation]);

        for expression in [
            col("handler").eq(string(None)),
            col("handler").in_list(vec![string(None)], false),
        ] {
            let Selection::Predicates(fields) = translate(expression) else {
                panic!("expected empty domain")
            };
            assert!(
                matches!(fields.for_column(LogicalColumn::Handler), [Clause::Handler(ValuePredicate::In(values))] if values.is_empty())
            );
        }
        let Selection::Predicates(fields) = translate(col("kind").eq(string(Some("unknown"))))
        else {
            panic!("expected empty kind domain")
        };
        assert!(
            matches!(fields.for_column(LogicalColumn::Kind), [Clause::Kind(ValuePredicate::In(values))] if values.is_empty())
        );

        let Selection::Predicates(fields) = translate(
            col("service_name")
                .in_list(vec![string(Some("alpha")), string(Some("gamma"))], false)
                .and(col("service_name").eq(string(Some("alpha")))),
        ) else {
            panic!("expected both constraints")
        };
        let [
            Clause::ServiceName(ValuePredicate::In(values)),
            Clause::ServiceName(ValuePredicate::Equal(_)),
        ] = fields.for_column(LogicalColumn::ServiceName)
        else {
            panic!("expected exact set and equality")
        };
        assert_eq!(
            values.iter().map(ReString::as_str).collect::<Vec<_>>(),
            ["alpha", "gamma"]
        );
    }

    #[test]
    fn unsupported_conjuncts_do_not_narrow_the_scan() {
        assert!(matches!(compile::<TestTable>(None), Selection::All));
        for expression in [
            col("service_name").in_list(vec![string(Some("alpha"))], true),
            col("service_name").in_list(vec![string(Some("alpha")), col("handler")], false),
            col("service_name")
                .eq(string(Some("alpha")))
                .or(col("stage").eq(string(Some("paused")))),
            col("service_name")
                .eq(string(Some("alpha")))
                .or(col("service_name").gt(string(Some("beta")))),
            col("service_name")
                .eq(string(Some("alpha")))
                .and(col("handler").is_null())
                .or(col("service_name").eq(string(Some("beta")))),
            col("kind").gt(string(Some("invocation"))),
            col("kind").lt(string(Some("not-a-kind"))),
            col("service_name").not_eq(string(Some("alpha"))),
            col("stage").eq(string(Some("paused"))),
            col("handler").is_not_null(),
            col("service_name").not_like(string(Some("alp%"))),
            col("service_name").ilike(string(Some("alp%"))),
            col("service_name").like(string(Some("alp"))),
            col("service_name").like(string(Some("al_p%"))),
            col("service_name").like(string(Some("al%p%"))),
            col("service_name").like(string(Some(r"alp\%"))),
            col("service_name").like(string(Some("alp\\"))),
            col("service_name").like(string(None)),
            col("service_name").like(col("handler")),
            starts_with(col("service_name"), string(None)),
            starts_with(col("service_name"), col("handler")),
            starts_with(string(Some("alpha")), col("service_name")),
            starts_with(col("kind"), string(Some("inv"))),
            col("kind").like(string(Some("inv%"))),
        ] {
            assert!(
                matches!(translate(expression.clone()), Selection::All),
                "{expression}"
            );
            assert_static_service(&translate(
                col("service_name")
                    .eq(string(Some("alpha")))
                    .and(expression),
            ));
        }
        assert!(
            LiteralPredicate::Equal(&ScalarValue::UInt64(Some(1)))
                .build::<ReString>()
                .is_none()
        );
    }

    #[test]
    fn null_first_thresholds_preserve_nullable_and_mixed_column_predicates() {
        fn threshold(filter: &Selection<TestTable>) -> (Bound<&str>, Bound<&str>) {
            let Selection::Predicates(fields) = filter else {
                panic!("expected constraints")
            };
            let [Clause::ServiceName(ValuePredicate::Range { lower, upper })] =
                fields.for_column(LogicalColumn::ServiceName)
            else {
                panic!("expected a service range")
            };
            (
                lower.as_ref().map(ReString::as_str),
                upper.as_ref().map(ReString::as_str),
            )
        }

        // TopK filters for NULLS FIRST orderings keep an impossible NULL arm on
        // non-nullable columns; the NULL arm of a nullable column stays residual.
        let topk = col("service_name")
            .is_null()
            .or(col("service_name").gt(string(Some("beta"))));
        assert_eq!(threshold(&translate(topk)), (Excluded("beta"), Unbounded));
        assert!(matches!(
            translate(
                col("handler")
                    .is_null()
                    .or(col("handler").gt(string(Some("beta"))))
            ),
            Selection::All
        ));
        assert!(matches!(
            translate(
                col("service_name")
                    .is_null()
                    .or(col("handler").gt(string(Some("beta"))))
            ),
            Selection::All
        ));
    }

    #[test]
    fn static_constraints_survive_dynamic_updates() {
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("kind", 2))],
            physical(col("kind").eq(string(Some("invocation")))),
        ));
        let initial: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            physical(col("service_name").eq(string(Some("alpha")))),
            Operator::And,
            dynamic.clone(),
        ));
        assert_static_service(&compile(Some(&initial)));

        // A wrapper around the entire predicate makes the whole conjunct dynamic.
        let rows = Arc::new(DynamicFilterPhysicalExpr::new(Vec::new(), initial.clone()));
        let rows_expr = rows.clone() as Arc<dyn PhysicalExpr>;
        assert!(matches!(
            compile::<TestTable>(Some(&rows_expr)),
            Selection::All
        ));
        rows.update(physical(
            col("service_name")
                .eq(string(Some("alpha")))
                .and(col("kind").eq(string(Some("state-mutation")))),
        ))
        .unwrap();
        dynamic
            .update(physical(col("kind").eq(string(Some("state-mutation")))))
            .unwrap();
        assert_static_service(&compile(Some(&initial)));
        assert!(matches!(
            compile::<TestTable>(Some(&rows_expr)),
            Selection::All
        ));
        assert!(
            rows.current()
                .unwrap()
                .to_string()
                .contains("state-mutation")
        );
    }
}
