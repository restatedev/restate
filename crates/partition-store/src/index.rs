// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Secondary indexes over the vqueue entries of a partition.
//!
//! # Concepts
//!
//! - **Secondary index**: one key per member entry, ordered by chosen fields, so
//!   that a query such as "entries of service `greeter` in stage `running`,
//!   newest first" scans a short key range instead of every entry. The entry's
//!   own key is the last field, so an index key identifies its entry and the
//!   value stays empty.
//! - **Index key**: the header `xI | pad | partition | index ID`
//!   ([`IndexKeyPrefix`]) followed by the declared fields in order, each written
//!   by an ordered codec (`define_secondary_index!`); for an index of entries by
//!   stage and service:
//!
//! ```text
//!  header (IndexKeyPrefix)           fields, in declaration order
//! ┌────┬─────┬───────────┬──────────┐ ┌───────┬──────────────┬─────────────────┬──────────────┐
//! │ xI │ pad │ partition │ index ID │ │ stage │ service_name │ transitioned_at │ canonical_id │
//! └────┴─────┴───────────┴──────────┘ └───────┴──────────────┴─────────────────┴──────────────┘
//!                                                              descending        the entry's key
//! ```
//!
//! - **Registered index** ([`RegisteredIndex`]): an index whose builds the
//!   registry keeps under a versioned definition name. Each replica's registry
//!   issues the index ID of its build, and a read resolves the Ready one
//!   ([`ready_index_scope`]).
//! - **Scope** ([`SecondaryIndex::Scope`]): where one partition's keys of the
//!   index live: the partition plus the index ID.
//!
//! This module is the machinery; the index definitions come in a commit of their
//! own. Writes keep an index up to date (`maintenance`); reads go through the
//! physical table every declaration generates, like any other ordered key
//! ([`crate::table`]).

use bytes::BufMut;
use zerocopy::IntoBytes;

use restate_storage_api::PrimaryKey;

use crate::keys::header::KeyScope;
use crate::keys::{IndexKeyPrefix, KeyEncode};
use crate::table::ReadMetadata;

pub(crate) mod macros;
mod maintenance;

/// Identifies a secondary index and the primary-key type it references.
pub trait SecondaryIndex {
    /// The complete record identity stored as the terminal suffix of each key.
    type PrimaryKey: PrimaryKey;
    /// Where the keys of one partition live: an `IndexScope` for an index the
    /// registry keeps ([`RegisteredIndex`]).
    type Scope: KeyScope;

    /// The scope a read covers; a registered definition resolves its Ready index and
    /// fails with `IndexNotReady` when there is none.
    fn scope(metadata: &ReadMetadata<'_>) -> crate::Result<Self::Scope>;

    /// The key prefix of `scope`.
    fn prefix(scope: Self::Scope) -> IndexKeyPrefix;
}

/// An index definition whose builds the registry keeps (see [`crate::registry`]).
pub trait RegisteredIndex: SecondaryIndex<Scope = crate::keys::header::IndexScope> {
    /// The name of its registry definition, including the version.
    const DEFINITION: &'static str;
}

/// A complete secondary key, including its required primary-key suffix.
/// Scan-prefix builders do not implement this trait.
pub trait SecondaryIndexKey: KeyEncode {
    type Index: SecondaryIndex;

    fn primary_key(&self) -> &<Self::Index as SecondaryIndex>::PrimaryKey;

    /// Appends the index prefix of `scope` and the complete payload to the
    /// caller's buffer.
    fn encode_key<B: BufMut>(&self, scope: <Self::Index as SecondaryIndex>::Scope, buffer: &mut B) {
        buffer.put_slice(<Self::Index as SecondaryIndex>::prefix(scope).as_bytes());
        self.encode(buffer);
    }
}

/// The Ready index for definition `I` in `store`.
#[cfg(any(test, feature = "test-util"))]
pub fn ready_index_scope<I: RegisteredIndex>(
    store: &crate::PartitionStore,
) -> crate::keys::header::IndexScope {
    crate::keys::header::IndexScope {
        partition: store.partition_id(),
        index: store
            .maintained_indexes()
            .ready_index(I::DEFINITION, crate::registry::IndexKind::Secondary)
            .unwrap(),
    }
}
