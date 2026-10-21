// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Native conjunctions, column clauses, and literal conversions.

mod branches;
pub mod domain;
mod model;
mod typed;
mod value;

use std::sync::Arc;

use datafusion::common::tree_node::TreeNode;
use datafusion::physical_expr::split_conjunction;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::expressions::DynamicFilterPhysicalExpr;

use crate::SelectionSchema;

/// Precision of a translation relative to its immediate input predicate.
/// `Exact` requires semantic equivalence, not merely recognizing every column.
/// `Inexact` is a sound over-approximation (including lack of proof of exactness).
/// Exact key preparation does not make earlier SQL analysis exact; consumers
/// retain SQL residuals until the complete translation chain is proven exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectionPrecision {
    Exact,
    Inexact,
}

pub use model::{
    ColumnPredicates, LiveSelection, Selection, ValuePredicate, ValuePredicateBuilder,
};
pub use value::{LiteralConversionError, SelectionLiteral, SelectionValue};

pub use typed::{InList, comparison_domain, parse_clause};

/// Compiles supported static conjuncts against a logical table schema.
///
/// Expressions must already be type-checked and coerced by DataFusion, with column
/// names matching the table's SQL bindings. Unsupported conjuncts remain residual.
/// Every conjunct containing a dynamic filter is excluded: snapshotting it here
/// would bake a changing constraint into static scan bounds. A caller compiling a
/// live selection must supply an independently sound, resolved snapshot instead.
/// An absent input predicate produces [`Selection::All`].
pub fn compile<T: SelectionSchema>(predicate: Option<&Arc<dyn PhysicalExpr>>) -> Selection<T> {
    let mut selection = Selection::All;
    for conjunct in predicate
        .into_iter()
        .flat_map(split_conjunction)
        .filter(|conjunct| !contains_dynamic_filter(conjunct))
    {
        if let Some(clause) = parse_clause::<T>(conjunct) {
            selection = selection.and(clause);
        }
    }
    selection
}

/// Whether an expression contains a dynamic filter anywhere in its tree.
///
/// Such expressions may change after planning and cannot constrain a static scan
/// without an independently sound snapshot contract.
pub fn contains_dynamic_filter(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.exists(|e| Ok(e.downcast_ref::<DynamicFilterPhysicalExpr>().is_some()))
        .expect("traversal closure is infallible")
}

pub use branches::{BranchOutcome, analyze_branches};
