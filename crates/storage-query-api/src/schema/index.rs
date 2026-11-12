// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logical primary/index relationships, independently of physical storage bindings.
//!
//! A **primary schema** is a logical schema whose rows can be looked up by a
//! complete key. An **index schema** is a logical schema whose rows are the entries
//! of a secondary index over one primary schema: some of the primary's columns in
//! a declared key order, ending with a reference to the primary row. The index
//! inherits the primary's sharding. [`index_restriction`] carries a predicate on
//! the primary over to the index's columns, so a reader can navigate the index for
//! the rows that may satisfy it.

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::physical_plan::PhysicalExpr;

use restate_storage_api::PrimaryKey;

use super::LogicalSchema;
use crate::SelectionSchema;
use crate::selection::static_conjuncts;

/// A logical schema with a complete key for looking up its current record.
pub trait PrimarySchema: LogicalSchema {
    /// Native lookup identity. An index reference may carry additional identity,
    /// such as the incarnation sequence absent from an entry-status lookup key.
    type PrimaryKey;

    /// The lookup-key column may be absent from the primary SQL view.
    const PRIMARY_KEY_COLUMN: Self::Column;
}

/// One column in a secondary index's declared key order.
///
/// This is not a SQL ordering guarantee. A binding must account for codec order,
/// fixed leading columns, and partition boundaries before advertising ordering.
#[derive(Debug, Clone, Copy)]
pub struct IndexColumn<C> {
    pub column: C,
    pub options: SortOptions,
}

/// A virtual logical schema related to one primary schema.
///
/// Index reads inherit primary sharding and serving requirements. This relationship
/// does not establish completeness: inspection of persisted index records is valid
/// even when the index cannot substitute for a primary-table read.
pub trait IndexSchema:
    LogicalSchema<Sharding = <Self::Primary as LogicalSchema>::Sharding>
{
    type Primary: PrimarySchema;
    /// Complete identity stored in the index's suffix, subject to the storage
    /// index-identity allowlist independently of the primary's lookup-key type.
    type Reference: PrimaryKey;

    /// Physical key order, ending with the complete index reference.
    const KEY_COLUMNS: &'static [IndexColumn<Self::Column>];
    const REFERENCE_COLUMN: Self::Column;

    /// Produces a locator, not proof that the current primary record matches the
    /// reference. Index-to-primary reads must also validate any incarnation data.
    fn primary_key(reference: &Self::Reference) -> <Self::Primary as PrimarySchema>::PrimaryKey;

    /// Returns the corresponding primary column, or `None` for index-only fields.
    fn primary_column(
        column: Self::Column,
    ) -> Option<<Self::Primary as super::SelectionSchema>::Column>;

    /// Carries one alternative's primary-column domains to the corresponding
    /// index columns. Index-only columns admit every value.
    fn from_primary_domains(
        primary: &<Self::Primary as SelectionSchema>::Domains,
    ) -> <Self as SelectionSchema>::Domains;
}

/// Necessary restrictions on index rows whose primary records can satisfy
/// `filters`, a conjunction over the primary schema.
///
/// The primary predicate is analyzed per Boolean alternative (dynamic filters
/// excluded, at most 16 alternatives) and each surviving alternative is mapped to
/// the index's columns before the alternatives are unioned. The union may admit
/// crossed combinations of the alternatives' domains, never fewer rows, so it is
/// sound for navigation but callers retain the original predicate as a residual.
/// `None` means no alternative can be true: the read is empty.
///
/// The function is deterministic in the predicate, so a coordinator choosing an
/// index and the worker navigating it derive the same restriction.
pub fn index_restriction<I: IndexSchema>(
    filters: &[Arc<dyn PhysicalExpr>],
) -> anyhow::Result<Option<<I as SelectionSchema>::Domains>> {
    let branches = I::Primary::analyze(&static_conjuncts(filters), NonZeroUsize::new(16).unwrap())?;
    Ok(branches
        .iter()
        .map(I::from_primary_domains)
        .reduce(I::union_domains))
}
