// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Conservative domains shared by partition and node work selection.
//! A domain is a superset of values that can satisfy a predicate. Unsupported
//! expressions select everything; the original SQL predicate remains a residual.

use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, InListExpr, Literal};

use restate_types::sharding::KeyRange;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Span<T> {
    lower: Bound<T>,
    upper: Bound<T>,
}

/// Sorted, disjoint intervals. An empty vector selects no work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Domain<T>(Vec<Span<T>>);

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
    pub(crate) fn all() -> Self {
        Self(vec![Span {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        }])
    }

    pub(crate) fn empty() -> Self {
        Self(vec![])
    }

    pub(crate) fn values(values: impl IntoIterator<Item = T>) -> Self {
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

    pub(crate) fn comparison(op: Operator, value: T) -> Self {
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

    pub(crate) fn is_all(&self) -> bool {
        matches!(
            self.0.as_slice(),
            [Span {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded
            }]
        )
    }

    pub(crate) fn contains(&self, value: &T) -> bool {
        self.0.iter().any(|span| span.contains(value))
    }

    /// A finite selection, including the proven-empty set. Unbounded or non-point
    /// intervals cannot be implemented by an exact-key lookup.
    pub(crate) fn into_values(self) -> Option<Vec<T>> {
        self.0
            .into_iter()
            .map(|span| match (span.lower, span.upper) {
                (Bound::Included(a), Bound::Included(b)) if a == b => Some(a),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn intersect(self, other: Self) -> Self {
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

    pub(crate) fn union(mut self, other: Self) -> Self {
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

impl Domain<u64> {
    pub(crate) fn key_ranges(&self, within: KeyRange) -> impl Iterator<Item = KeyRange> + '_ {
        self.0.iter().filter_map(move |span| {
            let start = match span.lower {
                Bound::Unbounded => 0,
                Bound::Included(v) => v,
                Bound::Excluded(v) => v.checked_add(1)?,
            }
            .max(within.start());
            let end = match span.upper {
                Bound::Unbounded => u64::MAX,
                Bound::Included(v) => v,
                Bound::Excluded(v) => v.checked_sub(1)?,
            }
            .min(within.end());
            (start <= end).then(|| KeyRange::new(start, end))
        })
    }
}

pub(crate) fn physical_filters(
    filters: &[Expr],
    schema: &Schema,
) -> datafusion::common::Result<Vec<Arc<dyn PhysicalExpr>>> {
    filters
        .iter()
        .map(|filter| {
            let filter = datafusion::physical_expr::planner::logical2physical(filter, schema);
            datafusion::physical_expr::utils::reassign_expr_columns(filter, schema)
        })
        .collect()
}

/// Interpret Boolean structure once, while sources supply typed column semantics.
pub(crate) fn analyze<T: Ord + Clone>(
    filters: &[Arc<dyn PhysicalExpr>],
    literal: impl Fn(&str, Operator, &ScalarValue) -> anyhow::Result<Domain<T>>,
) -> anyhow::Result<Domain<T>> {
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
            && !list.negated()
            && let Some(col) = list.expr().downcast_ref::<Column>()
        {
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

pub(crate) fn column_domain<T: Ord + Clone>(
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

pub(crate) fn unsigned_domain(
    filters: &[Arc<dyn PhysicalExpr>],
    column: &str,
) -> anyhow::Result<Domain<u64>> {
    column_domain(filters, column, |op, value| {
        if !matches!(
            op,
            Operator::Eq | Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
        ) {
            return Ok(Domain::all());
        }
        let value = match value {
            ScalarValue::UInt64(Some(v)) => *v,
            ScalarValue::UInt32(Some(v)) => u64::from(*v),
            ScalarValue::Int64(Some(v)) if *v >= 0 => *v as u64,
            ScalarValue::Int32(Some(v)) if *v >= 0 => *v as u64,
            value if value.is_null() => return Ok(Domain::empty()),
            _ => return Ok(Domain::all()),
        };
        Ok(Domain::comparison(op, value))
    })
}

#[cfg(test)]
mod tests {
    use std::ops::RangeBounds;

    use datafusion::arrow::array::{Array, BooleanArray, RecordBatch, UInt64Array};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::prelude::{col, lit};

    use super::*;

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
            key.not_eq(lit(3u64)), // Unsupported comparison must conservatively broaden.
        ];
        for left in &atoms {
            for right in &atoms {
                for expr in [
                    left.clone().and(right.clone()),
                    left.clone().or(right.clone()),
                ] {
                    let filters = physical_filters(std::slice::from_ref(&expr), &schema).unwrap();
                    let domain = unsigned_domain(&filters, "partition_key").unwrap();
                    let ranges: Vec<_> = domain.key_ranges(KeyRange::FULL).collect();
                    assert!(
                        ranges
                            .windows(2)
                            .all(|pair| pair[0].end() < pair[1].start()),
                        "{expr}: {ranges:?}"
                    );
                    let truth = filters[0]
                        .evaluate(&batch)
                        .unwrap()
                        .into_array(values.len())
                        .unwrap();
                    let truth = truth.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for (row, value) in values.iter().enumerate() {
                        if truth.is_valid(row) && truth.value(row) {
                            assert!(
                                ranges.iter().any(|range| range.contains(value)),
                                "lost {value} for {expr}"
                            );
                        }
                    }
                }
            }
        }
        let filters = physical_filters(
            &[col("partition_key")
                .gt(lit(2u64))
                .and(col("partition_key").lt_eq(lit(5u64)))],
            &schema,
        )
        .unwrap();
        assert_eq!(
            unsigned_domain(&filters, "partition_key")
                .unwrap()
                .key_ranges(KeyRange::FULL)
                .collect::<Vec<_>>(),
            vec![KeyRange::new(3, 5)]
        );
    }
}
