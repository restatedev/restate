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
//!   value stays empty. The busy-vqueue index is the exception: a *covering*
//!   index whose values hold the queue's counts.
//! - **Index key**: the header `xI | pad | partition | index ID`
//!   ([`IndexKeyPrefix`]) followed by the declared fields in order, each written
//!   by an ordered codec (`define_secondary_index!`):
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
//!   ([`ready_index_scope`]). The entry indexes are registered.
//! - **Static index**: an index with a fixed ID ([`IndexId`]) that is not
//!   backfilled. Only the busy-vqueue index is static.
//! - **Scope** ([`SecondaryIndex::Scope`]): where one partition's keys of the
//!   index live: the partition plus the index ID for a registered index, the
//!   partition alone for a static one.
//!
//! Writes keep the indexes up to date (`maintenance` and `vqueue_table::index`);
//! reads go through the physical table every declaration generates, like any
//! other ordered key ([`crate::table`]).

use bytes::BufMut;
use zerocopy::IntoBytes;

use restate_storage_api::PrimaryKey;

use crate::keys::header::KeyScope;
use crate::keys::{IndexKeyPrefix, KeyEncode};
use crate::table::ReadMetadata;

mod binding;
mod entry;
mod macros;
mod maintenance;
mod scan;
mod virtual_object;
mod vqueue;

pub use binding::EntryByServiceBinding;
pub use entry::{
    EntryByStage, EntryByStageKey, EntryByStageKeyView, EntryByStageService,
    EntryByStageServiceKey, EntryByStageServiceKeyView, EntryNextAtByStage, EntryNextAtByStageKey,
    EntryNextAtByStageKeyView, EntryNextAtByStageService, EntryNextAtByStageServiceKey,
    EntryNextAtByStageServiceKeyView,
};
pub use virtual_object::{
    EntryByVirtualObjectStage, EntryByVirtualObjectStageKey, EntryByVirtualObjectStageKeyView,
    EntryNextAtByVirtualObjectStage, EntryNextAtByVirtualObjectStageKey,
    EntryNextAtByVirtualObjectStageKeyView,
};
pub use vqueue::{BusyVQueue, BusyVQueueKey, BusyVQueueKeyView};

/// Identifies a secondary index and the primary-key type it references.
pub trait SecondaryIndex {
    /// The complete record identity stored as the terminal suffix of each key.
    type PrimaryKey: PrimaryKey;
    /// Where the keys of one partition live: an `IndexScope` for an index the
    /// registry keeps ([`RegisteredIndex`]), the partition for one with a static
    /// [`IndexId`].
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

/// Completes the entry indexes of `store`, as an open with
/// `complete_indexes_v1` does, without the open's other migrations.
#[cfg(any(test, feature = "test-util"))]
pub async fn complete_indexes(store: &mut crate::PartitionStore) {
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::registry::{COMPLETENESS_CHUNK, DEFINITIONS, FenceState, Requests};

    let db = store.partition_db().clone();
    let current = restate_types::SemanticRestateVersion::current();
    let mut features = crate::features::LoadedStorageFeatures::load(
        get_storage_features_from_partition_db(&db).unwrap(),
        get_storage_version_from_partition_db(&db).unwrap(),
        current,
    )
    .unwrap();
    let mut min_restate_version = get_min_restate_version_from_partition_db(&db).unwrap();
    let definitions = crate::vqueue_table::ENTRY_INDEXES.definitions.to_vec();
    store
        .reconcile_registry(
            DEFINITIONS,
            &Requests {
                maintain: definitions.clone(),
                complete: definitions,
                ..Default::default()
            },
            FenceState {
                features: &mut features,
                current_version: current,
                min_restate_version: &mut min_restate_version,
            },
            COMPLETENESS_CHUNK,
            &Default::default(),
        )
        .await
        .unwrap();
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

/// The static index identities. Only [`IndexId::BusyVQueue`] still uses its
/// own; the entry indexes are registered, and keys under their static IDs predate
/// the registry and are not read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[allow(clippy::enum_variant_names)]
pub enum IndexId {
    /// Stage, service, descending transition time, canonical ID.
    EntryByStageService = 1,
    /// Stage, descending transition time, canonical ID.
    EntryByStage = 2,
    /// Stage, next transition time, sequence, canonical ID.
    EntryNextAtByStage = 3,
    BusyVQueue = 4,
    /// Service, scope, object key, stage, descending transition time, canonical ID.
    EntryByVirtualObjectStage = 5,
    /// Stage, service, next transition time, sequence, canonical ID.
    EntryNextAtByStageService = 6,
    /// Service, scope, object key, stage, next transition time, sequence, canonical ID.
    EntryNextAtByVirtualObjectStage = 7,
}

impl IndexId {
    pub const fn as_u32(self) -> u32 {
        match self {
            Self::EntryByStageService => 1,
            Self::EntryByStage => 2,
            Self::EntryNextAtByStage => 3,
            Self::BusyVQueue => 4,
            Self::EntryByVirtualObjectStage => 5,
            Self::EntryNextAtByStageService => 6,
            Self::EntryNextAtByVirtualObjectStage => 7,
        }
    }
    pub const fn from_u32(value: u32) -> Option<Self> {
        match value {
            1 => Some(Self::EntryByStageService),
            2 => Some(Self::EntryByStage),
            3 => Some(Self::EntryNextAtByStage),
            4 => Some(Self::BusyVQueue),
            5 => Some(Self::EntryByVirtualObjectStage),
            6 => Some(Self::EntryNextAtByStageService),
            7 => Some(Self::EntryNextAtByVirtualObjectStage),
            _ => None,
        }
    }
}
