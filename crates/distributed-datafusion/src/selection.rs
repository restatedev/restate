// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Adapts shared candidate domains to query planning and partition-key scans.

use std::ops::Bound;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::PhysicalExpr;

pub(crate) use restate_storage_query_api::selection::domain::Domain;
#[cfg(test)]
pub(crate) use restate_storage_query_api::selection::domain::analyze;
use restate_types::sharding::KeyRange;

/// Lowers logical partition-key intervals to inclusive ranges within one scan.
pub(crate) fn key_ranges(
    domain: &Domain<u64>,
    within: KeyRange,
) -> impl Iterator<Item = KeyRange> + '_ {
    domain.ranges().filter_map(move |(lower, upper)| {
        let start = match lower {
            Bound::Unbounded => 0,
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v.checked_add(1)?,
        }
        .max(within.start());
        let end = match upper {
            Bound::Unbounded => u64::MAX,
            Bound::Included(v) => *v,
            Bound::Excluded(v) => v.checked_sub(1)?,
        }
        .min(within.end());
        (start <= end).then(|| KeyRange::new(start, end))
    })
}

/// Cover a candidate domain with the single interval supported by a backend.
pub(crate) fn key_range(domain: &Domain<u64>) -> Option<KeyRange> {
    let mut ranges = key_ranges(domain, KeyRange::FULL);
    ranges
        .next()
        .map(|first| KeyRange::new(first.start(), ranges.last().unwrap_or(first).end()))
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

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::Operator;

    use super::*;

    #[test]
    fn partition_ranges_preserve_bounds_and_scan_scope() {
        for (domain, within, expected) in [
            (
                Domain::all(),
                KeyRange::new(2, 5),
                vec![KeyRange::new(2, 5)],
            ),
            (Domain::empty(), KeyRange::FULL, vec![]),
            (
                Domain::values([0, 2, 2, 4, u64::MAX]),
                KeyRange::new(2, 5),
                vec![KeyRange::new(2, 2), KeyRange::new(4, 4)],
            ),
            (
                Domain::comparison(Operator::Gt, u64::MAX),
                KeyRange::FULL,
                vec![],
            ),
            (Domain::comparison(Operator::Lt, 0), KeyRange::FULL, vec![]),
            (
                Domain::comparison(Operator::Gt, 2)
                    .intersect(Domain::comparison(Operator::LtEq, 5)),
                KeyRange::FULL,
                vec![KeyRange::new(3, 5)],
            ),
            (
                Domain::comparison(Operator::LtEq, 4).union(Domain::comparison(Operator::GtEq, 3)),
                KeyRange::new(2, 5),
                vec![KeyRange::new(2, 5)],
            ),
            (
                Domain::values([0, u64::MAX]),
                KeyRange::FULL,
                vec![KeyRange::new(0, 0), KeyRange::new(u64::MAX, u64::MAX)],
            ),
        ] {
            assert_eq!(key_ranges(&domain, within).collect::<Vec<_>>(), expected);
        }
    }
}
