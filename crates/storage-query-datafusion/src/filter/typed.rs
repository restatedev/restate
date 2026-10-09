// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
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

use restate_storage_api::filter::{
    Filter, FilterLiteral, FilterTarget, FilterValue, LiteralConversionError, ValuePredicate,
    ValuePredicateBuilder,
};
use restate_types::sharding::KeyRange;

use crate::partition_store_scanner::ScanLocalPartitionFilter;

use super::{InList, extract_column_literal, static_conjuncts};

impl<T: FilterTarget> ScanLocalPartitionFilter for Filter<T> {
    /// Extracts static key constraints. The complete live predicate remains with
    /// the batch filter to enforce unsupported expressions and dynamic updates.
    fn new(_range: KeyRange, access_predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        let mut filter = Self::All;
        for conjunct in static_conjuncts(access_predicate.as_ref()) {
            if let Some(clause) = parse_clause::<T>(conjunct) {
                filter = filter.and(clause);
            }
        }
        filter
    }
}

/// Converts one supported conjunct; `None` leaves it entirely as a residual.
fn parse_clause<T: FilterTarget>(predicate: &Arc<dyn PhysicalExpr>) -> Option<T::Clause> {
    if let Some(binary) = predicate.downcast_ref::<BinaryExpr>()
        && *binary.op() == Operator::Or
    {
        // DataFusion expands short IN lists into ORs. Only normalize unions of
        // literals on the same column; any unsupported arm keeps the whole OR residual.
        let values = InList::parse(predicate, 64)?;
        return T::value_clause(T::field_from_tag(values.col.name())?, values);
    }
    if let Some(like) = predicate.downcast_ref::<LikeExpr>() {
        if like.negated() || like.case_insensitive() {
            return None;
        }
        let (column, literal) = extract_column_literal(like.expr(), like.pattern())?;
        let prefix = like_prefix(literal.value().try_as_str()??)?;
        return T::starts_with_clause(T::field_from_tag(column.name())?, &prefix);
    }
    if let Some(function) = predicate.downcast_ref::<ScalarFunctionExpr>() {
        // Check the implementation, not a name that a different UDF could reuse.
        function.fun().inner().downcast_ref::<StartsWithFunc>()?;
        let [input, prefix] = function.args() else {
            return None;
        };
        let (column, literal) = extract_column_literal(input, prefix)?;
        return T::starts_with_clause(
            T::field_from_tag(column.name())?,
            literal.value().try_as_str()??,
        );
    }
    let (column, builder) = if let Some(is_null) = predicate.downcast_ref::<IsNullExpr>() {
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
    };
    T::value_clause(T::field_from_tag(column.name())?, builder)
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

/// Borrows the input until the target selects a concrete field type.
enum LiteralPredicate<'a> {
    IsNull,
    Equal(&'a ScalarValue),
    Comparison(Operator, &'a ScalarValue),
    In(&'a [Arc<dyn PhysicalExpr>]),
}

impl ValuePredicateBuilder for LiteralPredicate<'_> {
    fn build<V: FilterValue>(self) -> Option<ValuePredicate<V>> {
        match self {
            Self::IsNull => match V::from_literal(FilterLiteral::Null) {
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
                // field domain. Keep it residual rather than treating it as empty.
                let value = V::from_ordered_literal(filter_literal(value)?).ok()?;
                let (lower, upper) = match op {
                    Operator::Gt => (Excluded(value), Unbounded),
                    Operator::GtEq => (Included(value), Unbounded),
                    Operator::Lt | Operator::LtEq => {
                        // SQL comparisons must not include NULL, even below an
                        // upper endpoint. Option values order None before Some.
                        let lower = match V::from_literal(FilterLiteral::Null) {
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

impl ValuePredicateBuilder for InList<'_> {
    fn build<V: FilterValue>(self) -> Option<ValuePredicate<V>> {
        if self.negated {
            return None;
        }
        collect_values(self.list.into_iter().map(Some))
    }
}

/// Converts supported scalar representations without allocating or coercing strings.
fn filter_literal(value: &ScalarValue) -> Option<FilterLiteral<'_>> {
    match value {
        ScalarValue::Boolean(Some(value)) => Some(FilterLiteral::Bool(*value)),
        ScalarValue::UInt8(Some(value)) => Some(FilterLiteral::Unsigned((*value).into())),
        ScalarValue::UInt16(Some(value)) => Some(FilterLiteral::Unsigned((*value).into())),
        ScalarValue::UInt32(Some(value)) => Some(FilterLiteral::Unsigned((*value).into())),
        ScalarValue::UInt64(Some(value)) => Some(FilterLiteral::Unsigned(*value)),
        ScalarValue::Int8(Some(value)) => Some(FilterLiteral::Signed((*value).into())),
        ScalarValue::Int16(Some(value)) => Some(FilterLiteral::Signed((*value).into())),
        ScalarValue::Int32(Some(value)) => Some(FilterLiteral::Signed((*value).into())),
        ScalarValue::Int64(Some(value)) => Some(FilterLiteral::Signed(*value)),
        // DataFusion has already coerced timestamp literals to the comparison's
        // timezone. Zoned Arrow timestamps store UTC epoch values regardless of
        // the display timezone, so no additional timezone adjustment belongs here.
        ScalarValue::TimestampSecond(Some(value), _) => {
            value.checked_mul(1_000).map(FilterLiteral::TimestampMillis)
        }
        ScalarValue::TimestampMillisecond(Some(value), _) => {
            Some(FilterLiteral::TimestampMillis(*value))
        }
        // Do not round finer-precision literals: that could change equality or
        // inclusive/exclusive bounds. Non-aligned literals remain residual.
        ScalarValue::TimestampMicrosecond(Some(value), _) if value % 1_000 == 0 => {
            Some(FilterLiteral::TimestampMillis(value / 1_000))
        }
        ScalarValue::TimestampNanosecond(Some(value), _) if value % 1_000_000 == 0 => {
            Some(FilterLiteral::TimestampMillis(value / 1_000_000))
        }
        value => value.try_as_str()?.map(FilterLiteral::String),
    }
}

/// Drops SQL NULL alternatives and values proven impossible for this field.
/// An empty IN represents no matches. Unsupported expressions or literal types
/// reject the whole conjunct, rather than accidentally restricting its domain.
fn collect_values<'a, V: FilterValue>(
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
        let value = match V::from_literal(filter_literal(literal)?) {
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
    use std::ops::RangeBounds;

    use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, LargeStringArray};
    use datafusion::arrow::datatypes::{DataType, Field as ArrowField, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion::functions::string::expr_fn::starts_with;
    use datafusion::logical_expr::{Expr, binary_expr, col, lit};
    use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
    use datafusion::physical_expr::planner::logical2physical;
    use datafusion::prelude::SessionContext;

    use restate_storage_api::stats::service_load::{
        ServiceLoad, ServiceLoadClause as Clause, ServiceLoadField as Field,
    };
    use restate_types::vqueues::EntryKind;
    use restate_util_string::ReString;

    use super::*;

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
            let value = MillisSinceEpoch::from_literal(filter_literal(&literal).unwrap()).unwrap();
            assert_eq!(value.as_u64(), 2_000);
        }
        for literal in [
            ScalarValue::TimestampMicrosecond(Some(1), None),
            ScalarValue::TimestampNanosecond(Some(1), None),
            ScalarValue::TimestampSecond(Some(i64::MAX), None),
        ] {
            assert!(filter_literal(&literal).is_none());
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

    restate_storage_api::define_table! { OtherTarget; }

    restate_storage_api::define_filter! {
        OtherTarget {
            label: Option<ReString> => starts_with,
            count: u64,
            balance: i64,
            enabled: bool,
        }
    }

    #[test]
    fn generated_targets_translate_nullable_and_numeric_fields() {
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
        let Filter::Predicates(fields) = Filter::<OtherTarget>::new(
            KeyRange::FULL,
            Some(logical2physical(&expression, &schema)),
        ) else {
            panic!("expected typed constraints without a target-specific adapter")
        };
        assert!(matches!(
            fields.for_field(OtherTargetField::Label),
            [OtherTargetClause::Label(ValuePredicate::Equal(None))]
        ));
        let [OtherTargetClause::Count(ValuePredicate::In(values))] =
            fields.for_field(OtherTargetField::Count)
        else {
            panic!("expected unsigned membership")
        };
        assert_eq!(values, &[7, u64::MAX]);
        assert!(matches!(
            fields.for_field(OtherTargetField::Balance),
            [OtherTargetClause::Balance(ValuePredicate::Equal(i64::MIN))]
        ));
        assert!(matches!(
            fields.for_field(OtherTargetField::Enabled),
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

    fn translate(expr: Expr) -> Filter<ServiceLoad> {
        Filter::new(KeyRange::FULL, Some(physical(expr)))
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
                let Filter::Predicates(fields) =
                    Filter::<ServiceLoad>::new(KeyRange::FULL, Some(expression.clone()))
                else {
                    panic!("expected range for {expression}")
                };
                let [Clause::Handler(ValuePredicate::Range { lower, upper })] =
                    fields.for_field(Field::Handler)
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

        let Filter::Predicates(fields) =
            translate(col("service_name").gt_eq(string(Some("LargeState"))))
        else {
            panic!("expected service range")
        };
        assert!(matches!(fields.for_field(Field::ServiceName),
            [Clause::ServiceName(ValuePredicate::Range { lower: Included(value), upper: Unbounded })] if value.as_str() == "LargeState"));

        let schema = Schema::new(vec![ArrowField::new("count", DataType::UInt64, false)]);
        let expression = logical2physical(&col("count").lt(lit(7u64)), &schema);
        let Filter::Predicates(fields) =
            Filter::<OtherTarget>::new(KeyRange::FULL, Some(expression))
        else {
            panic!("expected numeric range")
        };
        assert!(matches!(
            fields.for_field(OtherTargetField::Count),
            [OtherTargetClause::Count(ValuePredicate::Range {
                lower: Unbounded,
                upper: Excluded(7)
            })]
        ));
    }

    #[test]
    fn prefixes_preserve_datafusion_escaping_and_null_semantics() {
        let task_ctx = SessionContext::new().task_ctx();
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
            // DataFusion's starts_with simplification, including remote transport.
            for expression in [
                col("handler").like(string(Some(pattern))),
                starts_with(col("handler"), string(Some(expected))),
            ] {
                let expression = physical(expression);
                let expression = crate::decode_expr(
                    &task_ctx,
                    &schema(),
                    &crate::encode_expr(&expression).unwrap(),
                )
                .unwrap();
                let Filter::Predicates(fields) =
                    Filter::<ServiceLoad>::new(KeyRange::FULL, Some(expression.clone()))
                else {
                    panic!("expected prefix for {expression}")
                };
                let [Clause::HandlerStartsWith(prefix)] = fields.for_field(Field::Handler) else {
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
        let Filter::Predicates(fields) =
            Filter::<OtherTarget>::new(KeyRange::FULL, Some(expression))
        else {
            panic!("expected prefix for another target")
        };
        assert!(matches!(fields.for_field(OtherTargetField::Label),
            [OtherTargetClause::LabelStartsWith(prefix)] if prefix.as_str() == "pre"));
        assert!(matches!(like_prefix("alp%"), Some(Cow::Borrowed("alp"))));
    }

    fn assert_static_service(filter: &Filter<ServiceLoad>) {
        let Filter::Predicates(fields) = filter else {
            panic!("expected static service constraint")
        };
        let [Clause::ServiceName(ValuePredicate::Equal(name))] =
            fields.for_field(Field::ServiceName)
        else {
            panic!("expected service equality")
        };
        assert_eq!(name.as_str(), "alpha");
        assert!(fields.for_field(Field::Kind).is_empty());
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
        let Filter::Predicates(fields) = filter else {
            panic!("expected clauses")
        };
        let [Clause::ServiceName(ValuePredicate::Equal(name))] =
            fields.for_field(Field::ServiceName)
        else {
            panic!("expected reversed equality")
        };
        assert_eq!(name.as_str(), "alpha");
        assert!(matches!(
            fields.for_field(Field::Handler),
            [Clause::Handler(ValuePredicate::Equal(None))]
        ));
        let [Clause::Kind(ValuePredicate::In(values))] = fields.for_field(Field::Kind) else {
            panic!("expected membership")
        };
        assert_eq!(values, &[EntryKind::StateMutation, EntryKind::Invocation]);

        for expression in [
            col("handler").eq(string(None)),
            col("handler").in_list(vec![string(None)], false),
        ] {
            let Filter::Predicates(fields) = translate(expression) else {
                panic!("expected empty domain")
            };
            assert!(
                matches!(fields.for_field(Field::Handler), [Clause::Handler(ValuePredicate::In(values))] if values.is_empty())
            );
        }
        let Filter::Predicates(fields) = translate(col("kind").eq(string(Some("unknown")))) else {
            panic!("expected empty kind domain")
        };
        assert!(
            matches!(fields.for_field(Field::Kind), [Clause::Kind(ValuePredicate::In(values))] if values.is_empty())
        );

        let Filter::Predicates(fields) = translate(
            col("service_name")
                .in_list(vec![string(Some("alpha")), string(Some("gamma"))], false)
                .and(col("service_name").eq(string(Some("alpha")))),
        ) else {
            panic!("expected both constraints")
        };
        let [
            Clause::ServiceName(ValuePredicate::In(values)),
            Clause::ServiceName(ValuePredicate::Equal(_)),
        ] = fields.for_field(Field::ServiceName)
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
        assert!(matches!(
            Filter::<ServiceLoad>::new(KeyRange::FULL, None),
            Filter::All
        ));
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
                matches!(translate(expression.clone()), Filter::All),
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
    fn remote_access_predicate_preserves_static_constraints_across_updates() {
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("kind", 2))],
            physical(col("kind").eq(string(Some("invocation")))),
        ));
        let initial: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            physical(col("service_name").eq(string(Some("alpha")))),
            Operator::And,
            dynamic.clone(),
        ));
        assert_static_service(&Filter::new(KeyRange::FULL, Some(initial.clone())));
        let access = crate::decode_expr(
            &TaskContext::default(),
            &schema(),
            &crate::encode_expr(&initial).unwrap(),
        )
        .unwrap();
        assert_static_service(&Filter::new(KeyRange::FULL, Some(access.clone())));

        // ScannerTask adds a transport-update wrapper around the entire predicate.
        // It is live row-filter state, not the source for static access planning.
        let rows = Arc::new(DynamicFilterPhysicalExpr::new(Vec::new(), access.clone()));
        assert!(matches!(
            Filter::<ServiceLoad>::new(KeyRange::FULL, Some(rows.clone())),
            Filter::All
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
        assert_static_service(&Filter::new(KeyRange::FULL, Some(initial)));
        assert_static_service(&Filter::new(KeyRange::FULL, Some(access)));
        assert!(
            rows.current()
                .unwrap()
                .to_string()
                .contains("state-mutation")
        );
    }
}
