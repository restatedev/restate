// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Typed selections: which rows of a logical schema a read must return.
//!
//! A [`Selection<T>`] is a conjunction (an AND) of **clauses**, each a typed
//! condition on one column of a [`SelectionSchema`] `T`, such as
//! `kind = EntryKind::Invocation` or `handler` starts with `"get"`. The schema
//! declaration ([`crate::define_schema!`]) generates the clause type, so a
//! selection can only name columns and value types the schema has.
//!
//! A selection says *which* rows match, not *how* to find them. Storage may use
//! only the clauses it can turn into key bounds, and must then enforce exactly
//! those; the query keeps evaluating the original predicate on every returned row
//! (the **residual**). Returning extra candidate rows is therefore safe; returning
//! too few is not.
//!
//! Two translations from a DataFusion predicate live here:
//!
//! - [`compile`] turns the supported conjuncts of a predicate into a `Selection`;
//!   [`parse_clause`] lists the supported forms. Unsupported conjuncts stay residual.
//! - [`domain`] derives a **candidate domain** per column: a superset of the
//!   values that can satisfy the predicate, `OR` branches included.
//!   [`SelectionSchema::analyze`] expands `OR` alternatives within a branch and
//!   depth budget and runs the schema's own derivations on each resulting
//!   branch; a subtree beyond the budget is analyzed conservatively, without
//!   expansion. Callers combine the returned domains as they need.
//!
//! For example, `kind = 'invocation' AND handler LIKE '%get%'` compiles to the
//! single clause `kind = Invocation`; the `LIKE` stays residual. Its candidate
//! domain for `kind` is `{Invocation}`, and `handler` stays unrestricted.
//!
//! A conjunct containing a **dynamic filter**, a predicate DataFusion may tighten
//! after planning, is never baked into a selection: that would freeze a changing
//! constraint. [`static_conjuncts`] drops such conjuncts.

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

pub use branches::{BranchOutcome, analyze_branches};
pub use model::{
    ColumnPredicates, LiveSelection, Selection, ValuePredicate, ValuePredicateBuilder,
};
pub use typed::{InList, comparison_domain, parse_clause};
pub use value::{LiteralConversionError, SelectionLiteral, SelectionValue};

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

/// Static conjuncts of `filters`, excluding every conjunct with a dynamic filter.
///
/// Planning-time analysis must not freeze a predicate that can change afterwards.
pub fn static_conjuncts(filters: &[Arc<dyn PhysicalExpr>]) -> Vec<Arc<dyn PhysicalExpr>> {
    filters
        .iter()
        .flat_map(split_conjunction)
        .filter(|expr| !contains_dynamic_filter(expr))
        .cloned()
        .collect()
}

/// Whether an expression contains a dynamic filter anywhere in its tree.
///
/// Such expressions may change after planning and cannot constrain a static scan
/// without an independently sound snapshot contract.
pub fn contains_dynamic_filter(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.exists(|e| Ok(e.downcast_ref::<DynamicFilterPhysicalExpr>().is_some()))
        .expect("traversal closure is infallible")
}
