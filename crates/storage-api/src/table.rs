// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::identifiers::InvocationId;

/// A logical table whose records are identified by a primary key.
///
/// Secondary indexes refer to this table and inherit its primary-key type.
/// Physical key encoding and record lookup belong to the storage implementation.
pub trait Table {
    /// The complete identity of a record in this table.
    type PrimaryKey: PrimaryKey;
}

/// A complete, non-optional record identity.
///
/// This trait is sealed so key types are explicitly supported by storage-api.
/// In particular, `Option<T>` does not implement it: absence is a property of
/// index membership, not of the primary-key suffix of an existing entry.
pub trait PrimaryKey: sealed::Sealed + Eq {}

impl PrimaryKey for InvocationId {}

mod sealed {
    pub trait Sealed {}

    impl Sealed for restate_types::identifiers::InvocationId {}
}

/// Declares a logical table marker. Implement [`Table`] to associate a primary key.
///
/// # Usage
/// ```
/// use restate_storage_api::define_table;
///
/// // define a data table
/// define_table! {
///     /// Service-load dimensions.
///     pub ServiceLoad;
/// }
/// ```
/// # Expanded output
/// ```
/// /// Service-load dimensions.
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

#[cfg(test)]
mod tests {
    use restate_types::identifiers::InvocationId;

    use super::{PrimaryKey, Table};
    use crate::invocation_status_table::InvocationStatusTable;

    static_assertions::assert_impl_all!(InvocationId: PrimaryKey);
    static_assertions::assert_type_eq_all!(
        <InvocationStatusTable as Table>::PrimaryKey,
        InvocationId
    );
    static_assertions::assert_not_impl_any!(Option<InvocationId>: PrimaryKey);
}
