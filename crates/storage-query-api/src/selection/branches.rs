// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::{BinaryExpr, Literal};

/// Source-defined analysis of one conjunctive branch.
pub enum BranchOutcome<B> {
    /// The source proved that no record can make this branch SQL TRUE.
    Impossible,
    /// Conservative candidates, including all records when nothing is understood.
    Candidates(B),
}

/// Analyzes alternatives while preserving the conditions that belong together.
///
/// `filters` are conjoined. Each callback receives a conjunction of expressions
/// and returns source-defined candidate information for that entire branch.
/// Sources can build typed column domains, reject contradictions, and derive
/// routing consequences before the caller combines the returned alternatives.
///
/// Returning [`BranchOutcome::Impossible`] proves a branch cannot be SQL TRUE.
/// An unsupported condition must instead leave its candidates unconstrained.
/// The union of the returned candidates must cover every matching row; callers
/// retain the original filters for residual evaluation. Results are alternatives,
/// not disjoint scan requests: a record can satisfy more than one branch.
///
/// At most `max_branches` alternatives are expanded, with a traversal depth of 64.
/// When expansion would exceed either limit, the original subtree is passed to
/// the callback intact. The callback must handle any remaining Boolean structure
/// conservatively, for example with [`super::domain::column_domain`]. This keeps
/// independent conjuncts available even when a subtree cannot be expanded.
///
/// Filters must be safe for the analysis lifetime. Static callers must exclude
/// changing predicates; live callers must supply independently sound snapshots.
pub fn analyze_branches<B>(
    filters: &[Arc<dyn PhysicalExpr>],
    max_branches: NonZeroUsize,
    mut analyze: impl FnMut(&[Arc<dyn PhysicalExpr>]) -> anyhow::Result<BranchOutcome<B>>,
) -> anyhow::Result<Vec<B>> {
    let limit = max_branches.get();
    let mut branches = vec![Vec::new()];
    for filter in filters {
        let alternatives = expand(filter, limit, 64);
        if fits_product(branches.len(), alternatives.len(), limit) {
            branches = conjoin(branches, alternatives);
        } else {
            for branch in &mut branches {
                branch.push(Arc::clone(filter));
            }
        }
        if branches.is_empty() {
            break;
        }
    }

    let mut result = Vec::with_capacity(branches.len());
    for branch in branches {
        if let BranchOutcome::Candidates(candidates) = analyze(&branch)? {
            result.push(candidates);
        }
    }
    Ok(result)
}

type Branch = Vec<Arc<dyn PhysicalExpr>>;

fn fits_product(left: usize, right: usize, limit: usize) -> bool {
    left.checked_mul(right).is_some_and(|size| size <= limit)
}

fn conjoin(left: Vec<Branch>, right: Vec<Branch>) -> Vec<Branch> {
    let mut branches = Vec::with_capacity(left.len() * right.len());
    for left in left {
        for right in &right {
            let mut branch = Vec::with_capacity(left.len() + right.len());
            branch.extend(left.iter().cloned());
            branch.extend(right.iter().cloned());
            branches.push(branch);
        }
    }
    branches
}

fn expand(expr: &Arc<dyn PhysicalExpr>, limit: usize, depth: usize) -> Vec<Branch> {
    if depth > 0 {
        if let Some(literal) = expr.downcast_ref::<Literal>() {
            match literal.value() {
                ScalarValue::Boolean(Some(false) | None) | ScalarValue::Null => {
                    return Vec::new();
                }
                ScalarValue::Boolean(Some(true)) => return vec![Vec::new()],
                _ => {}
            }
        }
        if let Some(binary) = expr.downcast_ref::<BinaryExpr>()
            && matches!(binary.op(), Operator::And | Operator::Or)
        {
            let left = expand(binary.left(), limit, depth - 1);
            let right = expand(binary.right(), limit, depth - 1);
            match binary.op() {
                Operator::And if fits_product(left.len(), right.len(), limit) => {
                    return conjoin(left, right);
                }
                Operator::Or
                    if left
                        .len()
                        .checked_add(right.len())
                        .is_some_and(|size| size <= limit) =>
                {
                    let mut branches = left;
                    branches.extend(right);
                    return branches;
                }
                _ => {}
            }
        }
    }
    vec![vec![Arc::clone(expr)]]
}
