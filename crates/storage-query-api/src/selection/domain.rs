// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conservative candidate domains derived from Boolean predicates.
//!
//! An analyzed domain is a superset of values that can satisfy a predicate.
//! Unsupported expressions select everything; the original predicate remains a
//! residual. Sources supply column semantics and are responsible for preserving
//! SQL comparison and NULL semantics when constructing domains. Physical scan
//! ranges and routing destinations are derived by consumers.

use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, InListExpr, Literal};

use crate::selection::ValuePredicate;

use super::comparison_domain;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Span<T> {
    lower: Bound<T>,
    upper: Bound<T>,
}

/// Sorted, disjoint intervals. An empty domain selects no candidates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Domain<T>(Vec<Span<T>>);

/// Preserves the matching values of an already typed storage predicate.
impl<T: Ord + Clone> From<ValuePredicate<T>> for Domain<T> {
    fn from(predicate: ValuePredicate<T>) -> Self {
        match predicate {
            ValuePredicate::Equal(value) => Self::comparison(Operator::Eq, value),
            ValuePredicate::In(values) => Self::values(values),
            ValuePredicate::Range { lower, upper } => {
                let span = Span { lower, upper };
                if span.valid() {
                    Self(vec![span])
                } else {
                    Self::empty()
                }
            }
        }
    }
}

fn lower_cmp<T: Ord>(a: &Bound<T>, b: &Bound<T>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Ordering::Less),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Ordering::Greater),
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
            a.cmp(b)
        }
    }
}

fn upper_cmp<T: Ord>(a: &Bound<T>, b: &Bound<T>) -> Ordering {
    match (a, b) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Greater,
        (_, Bound::Unbounded) => Ordering::Less,
        (Bound::Included(a), Bound::Excluded(b)) => a.cmp(b).then(Ordering::Greater),
        (Bound::Excluded(a), Bound::Included(b)) => a.cmp(b).then(Ordering::Less),
        (Bound::Included(a), Bound::Included(b)) | (Bound::Excluded(a), Bound::Excluded(b)) => {
            a.cmp(b)
        }
    }
}

impl<T: Ord> Span<T> {
    fn valid(&self) -> bool {
        match (&self.lower, &self.upper) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
            (Bound::Included(a), Bound::Included(b)) => a <= b,
            (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => {
                a < b
            }
        }
    }

    fn contains(&self, value: &T) -> bool {
        let lower = match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(v) => value >= v,
            Bound::Excluded(v) => value > v,
        };
        lower
            && match &self.upper {
                Bound::Unbounded => true,
                Bound::Included(v) => value <= v,
                Bound::Excluded(v) => value < v,
            }
    }
}

impl<T: Ord + Clone> Domain<T> {
    /// Selects every value.
    pub fn all() -> Self {
        Self(vec![Span {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }])
    }

    /// Selects no values.
    pub fn empty() -> Self {
        Self(vec![])
    }

    /// Selects a finite set, sorting and deduplicating its values.
    pub fn values(values: impl IntoIterator<Item = T>) -> Self {
        Self(
            values
                .into_iter()
                .map(|value| Span {
                    lower: Bound::Included(value.clone()),
                    upper: Bound::Included(value),
                })
                .collect(),
        )
        .normalize()
    }

    /// Selects everything except the listed values. Order and duplicates do not
    /// matter; an empty list selects everything.
    ///
    /// For example:
    /// - `excluding_values([2, 5])` selects every value other than `2` and `5`.
    ///   Intersecting it with `Domain::values([1, 2, 3])` leaves just `1` and `3`.
    /// - `excluding_values([Some("alpha")])` still selects `None`. To exclude NULL
    ///   too, use `excluding_values([None, Some("alpha")])`.
    ///
    /// SQL adapters must account for SQL's NULL rules. For example,
    /// `handler NOT IN ('alpha', NULL)` is never TRUE, so it needs [`Self::empty`],
    /// rather than a domain excluding just those two values.
    ///
    /// Only exclude values proved to be forbidden. If analysis says a predicate
    /// *might* match `1` or `2`, negating it cannot safely exclude both: the predicate
    /// may actually match only `1`. Likewise, excluding one record ID does not
    /// justify excluding its entire partition.
    pub fn excluding_values(values: impl IntoIterator<Item = T>) -> Self {
        let mut values: Vec<_> = values.into_iter().collect();
        values.sort_unstable();
        values.dedup();

        let mut spans = Vec::with_capacity(values.len() + 1);
        let mut lower = Bound::Unbounded;
        for value in values {
            spans.push(Span {
                lower,
                upper: Bound::Excluded(value.clone()),
            });
            lower = Bound::Excluded(value);
        }
        spans.push(Span {
            lower,
            upper: Bound::Unbounded,
        });
        Self(spans)
    }

    /// Constructs an equality or ordered comparison using `T`'s ordering.
    ///
    /// Other operators select everything. Callers must first establish that
    /// conversion and ordering preserve the source column's SQL semantics.
    pub fn comparison(op: Operator, value: T) -> Self {
        let (lower, upper) = match op {
            Operator::Eq => (Bound::Included(value.clone()), Bound::Included(value)),
            Operator::Gt => (Bound::Excluded(value), Bound::Unbounded),
            Operator::GtEq => (Bound::Included(value), Bound::Unbounded),
            Operator::Lt => (Bound::Unbounded, Bound::Excluded(value)),
            Operator::LtEq => (Bound::Unbounded, Bound::Included(value)),
            _ => return Self::all(),
        };
        Self(vec![Span { lower, upper }])
    }

    /// Whether this domain has no constraints.
    pub fn is_all(&self) -> bool {
        matches!(
            self.0.as_slice(),
            [Span {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded
            }]
        )
    }

    /// Whether a value belongs to any interval.
    pub fn contains(&self, value: &T) -> bool {
        self.0.iter().any(|span| span.contains(value))
    }

    /// Borrows sorted, disjoint intervals for source-specific lowering.
    pub fn ranges(&self) -> impl Iterator<Item = (Bound<&T>, Bound<&T>)> {
        self.0
            .iter()
            .map(|span| (span.lower.as_ref(), span.upper.as_ref()))
    }

    /// A finite selection, including the proven-empty set. Unbounded or non-point
    /// intervals cannot be implemented by an exact-key lookup.
    pub fn into_values(self) -> Option<Vec<T>> {
        self.0
            .into_iter()
            .map(|span| match (span.lower, span.upper) {
                (Bound::Included(a), Bound::Included(b)) if a == b => Some(a),
                _ => None,
            })
            .collect()
    }

    /// Intersects two candidate domains without introducing new values.
    pub fn intersect(self, other: Self) -> Self {
        if self.is_all() {
            return other;
        }
        if other.is_all() {
            return self;
        }
        let mut result = Vec::new();
        let (mut a, mut b) = (0, 0);
        while let (Some(left), Some(right)) = (self.0.get(a), other.0.get(b)) {
            let span = Span {
                lower: if lower_cmp(&left.lower, &right.lower).is_lt() {
                    right.lower.clone()
                } else {
                    left.lower.clone()
                },
                upper: if upper_cmp(&left.upper, &right.upper).is_gt() {
                    right.upper.clone()
                } else {
                    left.upper.clone()
                },
            };
            if span.valid() {
                result.push(span);
            }
            if upper_cmp(&left.upper, &right.upper).is_le() {
                a += 1;
            } else {
                b += 1;
            }
        }
        Self(result)
    }

    /// Unions two domains, normalizing overlaps and duplicates.
    pub fn union(mut self, other: Self) -> Self {
        self.0.extend(other.0);
        self.normalize()
    }

    fn normalize(mut self) -> Self {
        self.0
            .sort_unstable_by(|a, b| lower_cmp(&a.lower, &b.lower));
        let mut spans: Vec<Span<T>> = Vec::with_capacity(self.0.len());
        for span in self.0 {
            if let Some(last) = spans.last_mut() {
                let overlap = Span {
                    lower: span.lower.clone(),
                    upper: last.upper.clone(),
                }
                .valid();
                if overlap {
                    if upper_cmp(&span.upper, &last.upper).is_gt() {
                        last.upper = span.upper;
                    }
                    continue;
                }
            }
            spans.push(span);
        }
        Self(spans)
    }
}

/// Interprets Boolean structure while sources supply typed column semantics.
///
/// Filters are conjoined. Unsupported expressions or exhausted traversal budgets
/// select everything. A callback must return a superset of matching candidates;
/// a condition on an unrelated column must not narrow the domain.
///
/// `IN` unions equality domains; `NOT IN` intersects inequality domains. The source
/// must interpret `!=` directly, including its NULL semantics. An approximate
/// equality domain cannot safely be complemented to obtain an inequality domain.
pub fn analyze<T: Ord + Clone>(
    filters: &[Arc<dyn PhysicalExpr>],
    literal: impl Fn(&str, Operator, &ScalarValue) -> anyhow::Result<Domain<T>>,
) -> anyhow::Result<Domain<T>> {
    fn not_in<T: Ord + Clone>(
        values: &[Arc<dyn PhysicalExpr>],
        column: &str,
        literal: &impl Fn(&str, Operator, &ScalarValue) -> anyhow::Result<Domain<T>>,
    ) -> anyhow::Result<Domain<T>> {
        match values {
            [] => Ok(Domain::all()),
            [expr] => match expr.downcast_ref::<Literal>() {
                Some(value) => literal(column, Operator::NotEq, value.value()),
                None => Ok(Domain::all()),
            },
            _ => {
                // Balance intersections so a long list does not repeatedly scan
                // an ever-growing domain. Unsupported members stay unconstrained.
                let (left, right) = values.split_at(values.len() / 2);
                Ok(not_in(left, column, literal)?.intersect(not_in(right, column, literal)?))
            }
        }
    }

    fn visit<T: Ord + Clone>(
        expr: &Arc<dyn PhysicalExpr>,
        literal: &impl Fn(&str, Operator, &ScalarValue) -> anyhow::Result<Domain<T>>,
        depth: usize,
    ) -> anyhow::Result<Domain<T>> {
        if depth == 0 {
            return Ok(Domain::all());
        }
        if let Some(value) = expr.downcast_ref::<Literal>() {
            return Ok(match value.value() {
                ScalarValue::Boolean(Some(false) | None) | ScalarValue::Null => Domain::empty(),
                _ => Domain::all(),
            });
        }
        if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
            match binary.op() {
                Operator::And | Operator::Or => {
                    let left = visit(binary.left(), literal, depth - 1)?;
                    let right = visit(binary.right(), literal, depth - 1)?;
                    return Ok(if *binary.op() == Operator::And {
                        left.intersect(right)
                    } else {
                        left.union(right)
                    });
                }
                op => {
                    let mut op = *op;
                    let pair = binary
                        .left()
                        .downcast_ref::<Column>()
                        .zip(binary.right().downcast_ref::<Literal>())
                        .or_else(|| {
                            op = match op {
                                Operator::Gt => Operator::Lt,
                                Operator::GtEq => Operator::LtEq,
                                Operator::Lt => Operator::Gt,
                                Operator::LtEq => Operator::GtEq,
                                other => other,
                            };
                            binary
                                .right()
                                .downcast_ref::<Column>()
                                .zip(binary.left().downcast_ref::<Literal>())
                        });
                    if let Some((col, value)) = pair {
                        return literal(col.name(), op, value.value());
                    }
                }
            }
        }
        if let Some(list) = expr.downcast_ref::<InListExpr>()
            && let Some(col) = list.expr().downcast_ref::<Column>()
        {
            if list.negated() {
                return not_in(list.list(), col.name(), literal);
            }
            let mut result = Domain::empty();
            for expr in list.list() {
                let Some(value) = expr.downcast_ref::<Literal>() else {
                    return Ok(Domain::all());
                };
                result
                    .0
                    .extend(literal(col.name(), Operator::Eq, value.value())?.0);
            }
            return Ok(result.normalize());
        }
        Ok(Domain::all())
    }
    filters.iter().try_fold(Domain::all(), |domain, filter| {
        Ok(domain.intersect(visit(filter, &literal, 64)?))
    })
}

/// Derives a domain for one column, leaving other columns unconstrained.
pub fn column_domain<T: Ord + Clone>(
    filters: &[Arc<dyn PhysicalExpr>],
    column: &str,
    literal: impl Fn(Operator, &ScalarValue) -> anyhow::Result<Domain<T>>,
) -> anyhow::Result<Domain<T>> {
    analyze(filters, |name, op, value| {
        if name == column {
            literal(op, value)
        } else {
            Ok(Domain::all())
        }
    })
}

/// Derives a conservative domain for a non-nullable unsigned column.
///
/// Uses typed literal conversion for equality, inequality, and ordered comparisons.
/// Unsupported operators and conversions select everything; comparisons with SQL
/// NULL select nothing.
pub fn unsigned_domain(
    filters: &[Arc<dyn PhysicalExpr>],
    column: &str,
) -> anyhow::Result<Domain<u64>> {
    column_domain(filters, column, |op, value| {
        Ok(comparison_domain(op, value).unwrap_or_else(Domain::all))
    })
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use datafusion::arrow::array::{Array, BooleanArray, RecordBatch, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::planner::logical2physical;
    use datafusion::prelude::{col, lit};

    use super::*;

    #[test]
    fn exclusions_remove_only_named_values_including_null() {
        let samples = [None, Some(""), Some("alpha"), Some("beta"), Some("other")];
        const T: bool = true;
        const F: bool = false;
        #[rustfmt::skip]
        let cases: &[(&[Option<&str>], [bool; 5])] = &[
            // Excluded values                               NULL  ''  alpha beta other
            (&[],                                            [T,   T,  T,    T,   T]),
            (&[Some("alpha")],                               [T,   T,  F,    T,   T]),
            (&[None],                                        [F,   T,  T,    T,   T]),
            (&[None, Some("alpha")],                         [F,   T,  F,    T,   T]),
            // Input order and duplicates have no effect.
            (&[Some("beta"), Some("alpha"), Some("beta")],   [T,   T,  F,    F,   T]),
            // Excluding these samples does not close the string domain.
            (&[None, Some(""), Some("alpha"), Some("beta")], [F,   F,  F,    F,   T]),
        ];

        for &(excluded, expected) in cases {
            let domain = Domain::excluding_values(excluded.iter().copied());
            assert_selects_each_value_once(&domain, &samples, expected);
        }
        assert!(Domain::<u64>::excluding_values([]).is_all());
    }

    #[test]
    fn exclusions_compose_and_resolve_against_a_complete_source_domain() {
        // Union admits a value allowed by either side; intersection requires both.
        // The endpoints exercise exclusions at the unsigned domain's extremes.
        let samples = [0, 1, 2, 3, u64::MAX - 1, u64::MAX];
        let left = Domain::excluding_values([u64::MAX, 2, 0, 2]);
        let right = Domain::excluding_values([1, 2]);
        assert_selects_each_value_once(
            &left.clone().union(right.clone()),
            &samples,
            [true, true, false, true, true, true],
        );
        assert_selects_each_value_once(
            &left.intersect(right),
            &samples,
            [false, false, false, true, true, false],
        );

        // Once the source supplies its complete domain, an exclusion becomes an
        // exact finite candidate set. Excluding an absent value removes nothing.
        let stages = ["finished", "inbox", "paused", "running", "suspended"];
        let candidates =
            Domain::values(stages).intersect(Domain::excluding_values(["running", "not-a-stage"]));
        assert_eq!(
            candidates.into_values(),
            Some(vec!["finished", "inbox", "paused", "suspended"])
        );
        assert_eq!(
            Domain::values(stages)
                .intersect(Domain::excluding_values(stages))
                .into_values(),
            Some(vec![])
        );

        // Invariant: normalization against a complete source domain never emits
        // an unknown value, whether the literal is included or excluded.
        #[rustfmt::skip]
        let cases: &[(&[&str], &[&str], &[&str])] = &[
            // Included                    Excluded                     Remaining candidates
            (&["not-a-stage"],              &[],                         &[]),
            (&["not-a-stage", "running"],   &[],                         &["running"]),
            (&stages,                      &["not-a-stage"],            &stages),
            (&["not-a-stage", "running"],   &["not-a-stage"],            &["running"]),
            (&["not-a-stage", "running"],   &["running"],                &[]),
            (&["not-a-stage", "running"],   &["not-a-stage", "running"], &[]),
        ];
        for &(included, excluded, expected) in cases {
            let candidates = Domain::values(stages)
                .intersect(Domain::values(included.iter().copied()))
                .intersect(Domain::excluding_values(excluded.iter().copied()))
                .into_values();
            assert_eq!(
                candidates.as_deref(),
                Some(expected),
                "including {included:?}, excluding {excluded:?}",
            );
        }

        // Invariant: combining an inclusion with exclusions is set subtraction.
        let included = ["running", "paused"];
        #[rustfmt::skip]
        let cases: &[(&[&str], &[&str])] = &[
            // Excluded              Remaining candidates
            (&["inbox"],             &["paused", "running"]),
            (&["running"],           &["paused"]),
            (&["running", "paused"], &[]),
        ];
        for &(excluded, expected) in cases {
            let candidates = Domain::values(included)
                .intersect(Domain::excluding_values(excluded.iter().copied()))
                .into_values();
            assert_eq!(
                candidates.as_deref(),
                Some(expected),
                "excluding {excluded:?}"
            );
        }
    }

    /// Check both membership and the no-duplicate-candidates interval contract.
    fn assert_selects_each_value_once<T: Ord + Clone + Debug, const N: usize>(
        domain: &Domain<T>,
        samples: &[T; N],
        expected: [bool; N],
    ) {
        assert_eq!(
            samples.each_ref().map(|value| domain.contains(value)),
            expected,
            "membership in {domain:?}"
        );
        for (value, selected) in samples.iter().zip(expected) {
            let matching_intervals = domain.0.iter().filter(|span| span.contains(value)).count();
            assert_eq!(
                matching_intervals,
                usize::from(selected),
                "{value:?} in {domain:?}"
            );
        }
    }

    #[test]
    fn not_in_keeps_unknown_parts_conservative() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("other", DataType::UInt64, false),
        ]);
        // Several IDs can share a partition. Knowing where an equality may match
        // does not tell us which partitions an inequality can safely exclude.
        let partition_for_equality = |op, _: &ScalarValue| {
            Ok(if op == Operator::Eq {
                Domain::values([42u64])
            } else {
                Domain::all()
            })
        };
        let include = logical2physical(
            &col("id").in_list(vec![lit(1u64), lit(2u64)], false),
            &schema,
        );
        let exclude = logical2physical(
            &col("id").in_list(vec![lit(1u64), lit(2u64)], true),
            &schema,
        );
        assert_eq!(
            column_domain(&[include], "id", partition_for_equality)
                .unwrap()
                .into_values(),
            Some(vec![42]),
        );
        assert!(
            column_domain(&[exclude], "id", partition_for_equality)
                .unwrap()
                .is_all()
        );

        // id NOT IN (1, other) still implies id != 1. The unknown member stays
        // residual instead of cancelling the supported inequality or excluding more.
        let filter = logical2physical(
            &col("id").in_list(vec![lit(1u64), col("other")], true),
            &schema,
        );
        assert_selects_each_value_once(
            &unsigned_domain(&[filter], "id").unwrap(),
            &[0, 1, 2],
            [true, false, true],
        );

        // A NULL member, however, proves that the whole NOT IN cannot be TRUE.
        let filter = logical2physical(
            &col("id").in_list(vec![col("other"), lit(ScalarValue::UInt64(None))], true),
            &schema,
        );
        assert_eq!(
            unsigned_domain(&[filter], "id").unwrap().into_values(),
            Some(vec![])
        );
    }

    #[test]
    fn domains_cover_sql_truth_without_duplicate_ranges() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "partition_key",
            DataType::UInt64,
            false,
        )]));
        let values = [0, 1, 2, 3, 4, 5, 42, u64::MAX - 1, u64::MAX];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt64Array::from(values.to_vec()))],
        )
        .unwrap();
        let key = col("partition_key");
        let atoms = [
            key.clone().eq(lit(3u64)),
            key.clone().lt(lit(3u64)),
            key.clone().lt_eq(lit(3u64)),
            key.clone().gt(lit(3u64)),
            key.clone().gt_eq(lit(3u64)),
            lit(3u64).lt(key.clone()),
            key.clone().lt(lit(0u64)),
            key.clone().gt(lit(u64::MAX)),
            key.clone()
                .in_list(vec![lit(0u64), lit(3u64), lit(3u64), lit(u64::MAX)], false),
            key.clone().eq(lit(ScalarValue::UInt64(None))),
            key.clone().not_eq(lit(3u64)),
            key.clone()
                .in_list(vec![lit(0u64), lit(3u64), lit(3u64), lit(u64::MAX)], true),
            key.clone()
                .in_list(vec![lit(3u64), lit(ScalarValue::UInt64(None))], true),
            key.clone().in_list(vec![], true),
            key.clone().eq(key), // Unsupported comparisons must conservatively broaden.
        ];
        for left in &atoms {
            for right in &atoms {
                for expr in [
                    left.clone().and(right.clone()),
                    left.clone().or(right.clone()),
                ] {
                    let filter = logical2physical(&expr, &schema);
                    let domain =
                        unsigned_domain(std::slice::from_ref(&filter), "partition_key").unwrap();
                    let ranges: Vec<_> = domain.ranges().collect();
                    assert!(
                        ranges.windows(2).all(|pair| !Span {
                            lower: pair[1].0,
                            upper: pair[0].1,
                        }
                        .valid()),
                        "{expr}: {ranges:?}"
                    );
                    let truth = filter
                        .evaluate(&batch)
                        .unwrap()
                        .into_array(values.len())
                        .unwrap();
                    let truth = truth.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for (row, value) in values.iter().enumerate() {
                        if truth.is_valid(row) && truth.value(row) {
                            assert!(domain.contains(value), "lost {value} for {expr}");
                        }
                    }
                }
            }
        }
        let filter = logical2physical(
            &col("partition_key")
                .gt(lit(2u64))
                .and(col("partition_key").lt_eq(lit(5u64))),
            &schema,
        );
        let domain = unsigned_domain(&[filter], "partition_key").unwrap();
        assert_eq!(
            domain.ranges().collect::<Vec<_>>(),
            [(Bound::Excluded(&2), Bound::Included(&5))]
        );
        assert!(domain.into_values().is_none());
        assert_eq!(Domain::values([3, 1, 3]).into_values(), Some(vec![1, 3]));
        assert_eq!(Domain::<u64>::empty().into_values(), Some(vec![]));
    }
}
