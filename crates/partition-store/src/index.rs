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
//!   own key is the last field, so an index key identifies its entry.
//! - **Index value** ([`IndexValue`]): what each key stores besides itself. Empty
//!   for an index of keys only; otherwise covered data, columns a read returns
//!   without fetching the entry but that the key need not order by, such as an
//!   entry's handler.
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
//!   issues the index ID of its build, and a read resolves the Complete one
//!   ([`complete_index_scope`]).
//! - **Scope** ([`SecondaryIndex::Scope`]): where one partition's keys of the
//!   index live: the partition plus the index ID.
//!
//! This module is the machinery; definitions live with their source table, such
//! as the entry indexes in `crate::vqueue_table`. Writes keep an index up to date
//! (`maintenance`); reads go through the physical table every declaration
//! generates, like any other ordered key ([`crate::table`]).

use bytes::BufMut;
use zerocopy::IntoBytes;

use restate_storage_api::{PrimaryKey, StorageError};

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
    /// What each key stores besides itself; `()` for an index of keys only.
    type Value: IndexValue;

    /// The scope a read covers; a registered definition resolves its Complete index and
    /// fails with `IndexNotComplete` when there is none.
    fn scope(metadata: &ReadMetadata<'_>) -> crate::Result<Self::Scope>;

    /// The key prefix of `scope`.
    fn prefix(scope: Self::Scope) -> IndexKeyPrefix;
}

/// An index definition whose builds the registry keeps (see [`crate::registry`]).
pub trait RegisteredIndex: SecondaryIndex<Scope = crate::keys::header::IndexScope> {
    /// The name of its registry definition, including the version.
    const DEFINITION: &'static str;
    /// The Restate version that introduced the definition. A node of an older
    /// version can neither maintain nor read its indexes, so a query coordinator
    /// must choose one only where every node serving its partitions runs at least
    /// this version. The V2 planner checks this before it reads an index in place
    /// of its primary.
    /// Code, not stored data: it is never persisted.
    const INTRODUCED: restate_types::SemanticRestateVersion;
}

/// What an index stores with each key ([`SecondaryIndex::Value`]).
///
/// A value is written whole with its key and compared as bytes: an update that
/// changes only the value removes the key and writes it again, which starts a new
/// SingleDelete lifetime (see `maintenance`). Index values hold what an entry
/// never changes, so in practice they are written once with their key.
pub trait IndexValue {
    /// The value as a write encodes it and a read decodes it, borrowing its data.
    type Ref<'a>: IndexValueRef<'a>;
}

/// The borrowed form of an [`IndexValue`].
pub trait IndexValueRef<'a>: Sized {
    fn encoded_len(&self) -> usize;
    fn encode<B: BufMut>(&self, buffer: &mut B);
    /// Decodes a stored value, failing on bytes this encoding didn't write.
    fn decode(bytes: &'a [u8]) -> crate::Result<Self>;
}

/// The borrowed value of index `I`.
pub type IndexValueOf<'a, I> = <<I as SecondaryIndex>::Value as IndexValue>::Ref<'a>;

/// No value: an index of keys only.
impl IndexValue for () {
    type Ref<'a> = ();
}

impl IndexValueRef<'_> for () {
    fn encoded_len(&self) -> usize {
        0
    }

    fn encode<B: BufMut>(&self, _: &mut B) {}

    fn decode(bytes: &[u8]) -> crate::Result<Self> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(StorageError::DataIntegrityError)
        }
    }
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

/// The Complete index for definition `I` in `store`.
#[cfg(any(test, feature = "test-util"))]
pub fn complete_index_scope<I: RegisteredIndex>(
    store: &crate::PartitionStore,
) -> crate::keys::header::IndexScope {
    crate::keys::header::IndexScope {
        partition: store.partition_id(),
        index: store
            .published_registry()
            .complete_index(I::DEFINITION, crate::registry::IndexKind::Secondary)
            .unwrap(),
    }
}
