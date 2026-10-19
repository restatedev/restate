// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::CanonicalEntryId;
use restate_types::vqueues::VQueueId;

/// A complete, non-optional record identity.
///
/// Secondary indexes use this bound for the identity stored alongside indexed
/// fields. The supported types are [`VQueueId`] for a queue and
/// [`CanonicalEntryId`] for one incarnation of a queue entry.
///
/// This trait is sealed: only storage-api can add supported key types.
/// `Option<T>` does not implement it. A record may be absent from an index, but
/// every existing index entry must contain a complete primary key.
pub trait PrimaryKey: sealed::Sealed {}

impl PrimaryKey for VQueueId {}
impl PrimaryKey for CanonicalEntryId {}

mod sealed {
    pub trait Sealed {}

    impl Sealed for restate_types::vqueues::VQueueId {}
    impl Sealed for restate_types::identifiers::CanonicalEntryId {}
}

/// Declares a logical table marker for typed filters and statistics.
///
/// The marker is an uninhabited enum that only names the table in types such as
/// [`Filter<ServiceLoad>`](crate::filter::Filter).
/// [`define_filter!`](crate::define_filter) implements its filter schema, and
/// storage implementations attach their traits to the same marker to define its
/// physical representation; the marker alone creates no storage. Attributes,
/// including doc comments and derives, are copied to it.
///
/// # Usage
/// ```
/// use restate_storage_api::define_table;
///
/// define_table! {
///     /// Service-load dimensions.
///     #[derive(Debug)]
///     pub ServiceLoad;
/// }
/// ```
/// # Expansion
/// ```
/// /// Service-load dimensions.
/// #[derive(Debug)]
/// pub enum ServiceLoad {}
/// ```
#[macro_export]
macro_rules! define_table {
    (
        $(#[$meta:meta])*
        $vis:vis $target:ident;
    ) => {
        $(#[$meta])*
        $vis enum $target {}
    };
}
