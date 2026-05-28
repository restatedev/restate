// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! On-disk storage version of the partition store.
//!
//! Persisted in the FSM under `fsm_variable::STORAGE_VERSION`, mirrored in
//! memory on the `PartitionStore`, and surfaced through
//! `PartitionProcessorStatus::storage_version` so cluster-wide observers
//! (e.g. `restatectl partition ls`) can read it without inspecting node
//! logs.

use restate_encoding::NetSerde;

/// On-disk storage version of the partition store.
///
/// NOTE: The representation numbers here must be strictly monotonically
/// increasing; the migration loop in `restate-partition-store` advances the
/// version one discriminant at a time. Re-using a discriminant — or skipping
/// one — would break that loop.
#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    bilrost::Enumeration,
    NetSerde,
    strum::FromRepr,
)]
#[repr(u16)]
pub enum StorageVersion {
    /// Before 1.5
    #[default]
    None = 0,
    /// Migrations:
    /// * Invocation status V1 -> V2
    V1_5 = 1,
    /// Migrations:
    /// * unscoped `state_table` -> scoped `state_table` (with `scope = None`)
    /// * unscoped `promise_table` -> scoped `promise_table` (with `scope = None`)
    ///
    /// Gated by `experimental_enable_migrate_scoped_tables`.
    /// Since v1.7.0
    ScopedStateAndPromise = 2,
}

impl StorageVersion {
    /// Returns `true` once the partition store has migrated the unscoped state
    /// and promise tables into their scoped variants. After this point all
    /// state/promise reads and writes use the scoped tables (with
    /// `scope = None` for entries that were unscoped pre-migration).
    pub fn is_scope_migrated(self) -> bool {
        self >= StorageVersion::ScopedStateAndPromise
    }
}

/// Error returned when a discriminant cannot be mapped to a known
/// [`StorageVersion`]. Wraps the raw value so the caller can report it.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error(
    "unknown partition-store storage version {0}; this binary is older than \
     the data it would open. Roll forward to a server version that recognizes \
     this storage version."
)]
pub struct UnknownStorageVersion(pub u16);

impl TryFrom<u16> for StorageVersion {
    type Error = UnknownStorageVersion;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        StorageVersion::from_repr(value).ok_or(UnknownStorageVersion(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_round_trips_known_variants() {
        for known in [
            StorageVersion::None,
            StorageVersion::V1_5,
            StorageVersion::ScopedStateAndPromise,
        ] {
            assert_eq!(StorageVersion::try_from(known as u16).unwrap(), known);
        }
    }

    #[test]
    fn try_from_rejects_unknown_value() {
        let err = StorageVersion::try_from(9999_u16).unwrap_err();
        assert_eq!(err.0, 9999);
        let msg = err.to_string();
        assert!(msg.contains("9999") && msg.contains("storage version"));
    }

    #[test]
    fn is_scope_migrated_only_for_scoped_state_and_promise() {
        assert!(!StorageVersion::None.is_scope_migrated());
        assert!(!StorageVersion::V1_5.is_scope_migrated());
        assert!(StorageVersion::ScopedStateAndPromise.is_scope_migrated());
    }
}
