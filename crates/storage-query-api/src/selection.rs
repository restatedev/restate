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

mod model;
mod value;

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
