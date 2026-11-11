// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The local registry of secondary-index and statistic instances.
//!
//! A **definition** is a code-defined index or statistic with a stable name that
//! includes its semantic version, such as `"entry_by_stage_service/1"`. Changing
//! its membership, key or value encoding means a new name. A **registration** is
//! one physical instance of a definition in this store: a **physical ID** that
//! addresses its keys (the index prefix's `idx_id`, the statistic prefix's `id`)
//! and that entries list while the instance is being built, plus its lifecycle
//! [`RegistrationState`] and, while it is being backfilled, the walk doing so
//! (see [`crate::walk`]).
//!
//! The registry is local to a replica and is not replicated: replicas may assign
//! different physical IDs to the same definition. Queries therefore name the
//! definition and resolve the physical ID against an admitted snapshot.
//!
//! # Physical IDs
//!
//! One counter issues IDs for indexes and statistics alike, starting at
//! [`FIRST_PHYSICAL_ID`], so that an entry's list of tracking IDs is never
//! ambiguous. IDs are never reused, not even after a registration is disabled,
//! and fit the `u32` of the index and statistic key prefixes. Registering a definition that
//! already has an active instance returns that instance; ordinary reopening
//! therefore consumes no IDs.
//!
//! ```
//! use restate_partition_store::registry::{Registry, RegistrationKind, RegistrationState};
//!
//! let mut registry = Registry::default();
//! let by_service = registry.register("entry_by_stage_service/1", RegistrationKind::Index).unwrap();
//! let gauges = registry.register("service_gauges/1", RegistrationKind::Statistic).unwrap();
//! assert_eq!((by_service, gauges), (64, 65));
//! // An active instance is returned as is.
//! assert_eq!(registry.register("entry_by_stage_service/1", RegistrationKind::Index).unwrap(), 64);
//!
//! // Disabling keeps the ID reserved; enabling again builds a fresh instance.
//! registry.disable(64).unwrap();
//! assert_eq!(registry.register("entry_by_stage_service/1", RegistrationKind::Index).unwrap(), 66);
//! assert_eq!(registry.get(64).unwrap().state, RegistrationState::Disabled);
//! ```

use bytes::Bytes;

use restate_util_string::ReString;

mod active;
mod complete;
mod reconcile;

pub(crate) use active::{ActiveSet, DEFINITIONS, Definition};
pub(crate) use complete::{CHUNK as COMPLETENESS_CHUNK, IndexBackfill};
pub(crate) use reconcile::{FAMILIES, Family, FenceState, Requests};

/// The first physical ID the registry issues. Lower IDs belong to the static
/// index (1–7) and statistic (1–3) identities that predate the registry.
///
/// This floor constrains registry envelopes only: a registration below it is
/// invalid. Opening a partition deletes the legacy status gauges
/// (`PartitionStore::remove_legacy_status_gauges`); keys under the static index
/// identities are not handled here.
pub const FIRST_PHYSICAL_ID: u32 = 64;

/// The persisted registry format this binary writes. It also reads format 1,
/// whose in-progress passes kept their cursor in the registration; opening the
/// store upgrades them to walks (see [`crate::walk`]).
const FORMAT: u32 = 2;

/// Whether a registration is an index (keys) or a statistic (merge deltas).
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum RegistrationKind {
    #[bilrost(0)]
    Unknown,
    #[bilrost(1)]
    Index,
    #[bilrost(2)]
    Statistic,
}

/// The lifecycle of one registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, bilrost::Enumeration)]
pub enum RegistrationState {
    #[bilrost(0)]
    Unknown,
    /// Maintained on every write. Entries written since registration list its ID;
    /// older entries are not reflected yet, so queries must not use it.
    #[bilrost(1)]
    Maintaining,
    /// Maintained, and every entry is reflected: queries may use it.
    #[bilrost(2)]
    Ready,
    /// Neither maintained nor readable; its keys were deleted. The ID stays
    /// reserved, so stale tracking IDs in entries never match a new instance.
    #[bilrost(3)]
    Disabled,
}

/// One physical instance of a definition in this store.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct Registration {
    #[bilrost(1)]
    pub definition: ReString,
    #[bilrost(2)]
    pub kind: RegistrationKind,
    #[bilrost(3)]
    pub id: u32,
    #[bilrost(4)]
    pub state: RegistrationState,
    /// Format 1 only: the exclusive resume position of an unfinished completeness
    /// pass. Format 2 keeps it in the instance's walk.
    #[bilrost(5)]
    pub cursor: Option<Bytes>,
    /// The walk backfilling this Maintaining instance, once started.
    #[bilrost(6)]
    pub walk: Option<u64>,
}

impl Registration {
    /// Maintained on writes: Maintaining or Ready.
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            RegistrationState::Maintaining | RegistrationState::Ready
        )
    }
}

/// A query needs an instance that is not Ready in the partition snapshot it reads:
/// not registered, still being completed, disabled, or the registry is unreadable.
/// Reading it anyway could miss entries, so the read fails instead.
#[derive(Debug, thiserror::Error)]
#[error("'{definition}' is not ready on partition {partition}: {reason}")]
pub struct NotReady {
    pub definition: ReString,
    pub partition: restate_types::identifiers::PartitionId,
    pub reason: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("all {} physical index and statistic IDs of this store are allocated", u32::MAX - FIRST_PHYSICAL_ID + 1)]
    Exhausted,
    #[error("unsupported registry format {0}; this binary supports formats 1 to {FORMAT}")]
    UnsupportedFormat(u32),
    #[error("'{0}' is registered as a different kind")]
    KindMismatch(ReString),
    #[error("registrations must be indexes or statistics")]
    UnknownKind,
    #[error("no registration has physical ID {0}")]
    UnknownId(u32),
    #[error("registration {id} cannot change from {from:?} to {to:?}")]
    InvalidTransition {
        id: u32,
        from: RegistrationState,
        to: RegistrationState,
    },
    #[error("malformed registry: {0}")]
    Malformed(#[from] bilrost::DecodeError),
    #[error("invalid registry: {0}")]
    Invalid(&'static str),
}

/// The registrations of one partition store and its physical-ID counter.
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct Registry {
    /// A newer format must not be rewritten by this binary: re-encoding would
    /// drop fields it does not know.
    #[bilrost(1)]
    format: u32,
    /// The next ID to issue. Beyond `u32::MAX` the counter is exhausted.
    #[bilrost(2)]
    next_id: u64,
    #[bilrost(3)]
    entries: Vec<Registration>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            format: FORMAT,
            next_id: u64::from(FIRST_PHYSICAL_ID),
            entries: Vec::new(),
        }
    }
}

impl Registry {
    /// Decodes a persisted registry and checks the invariants that allocation and
    /// maintenance rely on; a registry violating them is an error, not repaired.
    pub fn decode(bytes: &[u8]) -> Result<Self, RegistryError> {
        let registry = <Self as bilrost::OwnedMessage>::decode(bytes)?;
        if !(1..=FORMAT).contains(&registry.format) {
            return Err(RegistryError::UnsupportedFormat(registry.format));
        }
        registry.validate()?;
        Ok(registry)
    }

    fn validate(&self) -> Result<(), RegistryError> {
        let invalid = |reason| Err(RegistryError::Invalid(reason));
        if self.next_id < u64::from(FIRST_PHYSICAL_ID) {
            return invalid("the ID counter is below the first physical ID");
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut active = std::collections::BTreeSet::new();
        for entry in &self.entries {
            if entry.id < FIRST_PHYSICAL_ID || u64::from(entry.id) >= self.next_id {
                return invalid("a physical ID was not issued by the counter");
            }
            if !ids.insert(entry.id) {
                return invalid("a physical ID is registered twice");
            }
            if entry.kind == RegistrationKind::Unknown || entry.state == RegistrationState::Unknown
            {
                return invalid("a registration has an unknown kind or state");
            }
            if entry.is_active() && !active.insert(&entry.definition) {
                return invalid("a definition has more than one active instance");
            }
            if (entry.cursor.is_some() || entry.walk.is_some())
                && entry.state != RegistrationState::Maintaining
            {
                return invalid("only a Maintaining instance can be backfilled");
            }
            if self.format == 1 && entry.walk.is_some() {
                return invalid("a format-1 registration links to a walk");
            }
            if self.format != 1 && entry.cursor.is_some() {
                return invalid("a registration keeps a format-1 cursor");
            }
        }
        Ok(())
    }

    pub fn format(&self) -> u32 {
        self.format
    }

    /// Upgrades a format-1 registry to the current format: removes the cursors of
    /// in-progress passes and returns them with their instances, grouping equal
    /// cursors, in ascending order of each group's lowest ID. The caller starts a
    /// walk for each group and links its instances.
    pub(crate) fn take_legacy_cursors(&mut self) -> Vec<(Bytes, Vec<u32>)> {
        let mut groups: Vec<(Bytes, Vec<u32>)> = Vec::new();
        let mut entries: Vec<_> = self.entries.iter_mut().collect();
        entries.sort_unstable_by_key(|entry| entry.id);
        for entry in entries {
            if let Some(cursor) = entry.cursor.take() {
                match groups.iter_mut().find(|(position, _)| *position == cursor) {
                    Some((_, ids)) => ids.push(entry.id),
                    None => groups.push((cursor, vec![entry.id])),
                }
            }
        }
        self.format = FORMAT;
        groups
    }

    /// Links a Maintaining instance to the walk backfilling it.
    pub(crate) fn link(&mut self, id: u32, walk: u64) -> Result<(), RegistryError> {
        use RegistrationState::Maintaining;
        self.transition(id, &[Maintaining], Maintaining)?.walk = Some(walk);
        Ok(())
    }

    /// The registrations a walk backfills.
    pub fn members(&self, walk: u64) -> impl Iterator<Item = &Registration> {
        self.entries
            .iter()
            .filter(move |entry| entry.walk == Some(walk))
    }

    pub fn encode(&self) -> Vec<u8> {
        bilrost::Message::encode_to_vec(self)
    }

    /// Returns the active instance of `definition`, registering a new one in
    /// state Maintaining with a fresh ID if there is none. Errors leave the
    /// registry unchanged.
    pub fn register(
        &mut self,
        definition: &str,
        kind: RegistrationKind,
    ) -> Result<u32, RegistryError> {
        if kind == RegistrationKind::Unknown {
            return Err(RegistryError::UnknownKind);
        }
        if let Some(active) = self.active(definition) {
            if active.kind != kind {
                return Err(RegistryError::KindMismatch(active.definition.clone()));
            }
            return Ok(active.id);
        }
        let id = u32::try_from(self.next_id).map_err(|_| RegistryError::Exhausted)?;
        self.next_id += 1;
        self.entries.push(Registration {
            definition: ReString::from(definition),
            kind,
            id,
            state: RegistrationState::Maintaining,
            cursor: None,
            walk: None,
        });
        Ok(id)
    }

    /// The active (Maintaining or Ready) instance of `definition`, if any.
    pub fn active(&self, definition: &str) -> Option<&Registration> {
        self.entries
            .iter()
            .find(|entry| entry.is_active() && entry.definition == definition)
    }

    pub fn get(&self, id: u32) -> Option<&Registration> {
        self.entries.iter().find(|entry| entry.id == id)
    }

    /// The registration `id`, which must currently be in one of `from`.
    fn transition(
        &mut self,
        id: u32,
        from: &[RegistrationState],
        to: RegistrationState,
    ) -> Result<&mut Registration, RegistryError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.id == id)
            .ok_or(RegistryError::UnknownId(id))?;
        if !from.contains(&entry.state) {
            return Err(RegistryError::InvalidTransition {
                id,
                from: entry.state,
                to,
            });
        }
        Ok(entry)
    }

    /// All registrations, including disabled ones, in allocation order.
    pub fn entries(&self) -> &[Registration] {
        &self.entries
    }

    /// Records a format-1 resume position for a Maintaining instance, as a binary
    /// predating walks did: turns the registry into format 1. Tests only.
    #[cfg(test)]
    pub fn set_cursor(&mut self, id: u32, cursor: Bytes) -> Result<(), RegistryError> {
        use RegistrationState::Maintaining;
        self.transition(id, &[Maintaining], Maintaining)?.cursor = Some(cursor);
        self.format = 1;
        Ok(())
    }

    /// Completes a Maintaining instance: unlinks its walk and makes it Ready.
    pub fn set_ready(&mut self, id: u32) -> Result<(), RegistryError> {
        use RegistrationState::{Maintaining, Ready};
        let entry = self.transition(id, &[Maintaining], Ready)?;
        entry.cursor = None;
        entry.walk = None;
        entry.state = Ready;
        Ok(())
    }

    /// Stops maintaining an active instance, keeping its ID reserved. The caller
    /// deletes its keys in the same batch that persists this change.
    pub fn disable(&mut self, id: u32) -> Result<(), RegistryError> {
        use RegistrationState::{Disabled, Maintaining, Ready};
        let entry = self.transition(id, &[Maintaining, Ready], Disabled)?;
        entry.cursor = None;
        entry.walk = None;
        entry.state = Disabled;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_shared_monotonic_and_bounded() {
        let mut registry = Registry::default();
        assert_eq!(
            registry.register("a/1", RegistrationKind::Index),
            Ok(FIRST_PHYSICAL_ID)
        );
        assert_eq!(
            registry.register("b/1", RegistrationKind::Statistic),
            Ok(FIRST_PHYSICAL_ID + 1),
            "one_counter_for_both_kinds"
        );
        assert_eq!(
            registry.register("b/1", RegistrationKind::Index),
            Err(RegistryError::KindMismatch("b/1".into()))
        );
        let unchanged = registry.clone();
        assert_eq!(
            registry.register("c/1", RegistrationKind::Unknown),
            Err(RegistryError::UnknownKind)
        );
        assert_eq!(registry, unchanged, "rejected_registrations_change_nothing");
        registry.set_cursor(64, Bytes::from_static(b"k")).unwrap();
        assert_eq!(
            Registry::decode(&registry.encode())
                .unwrap()
                .get(64)
                .unwrap()
                .cursor,
            Some(Bytes::from_static(b"k")),
            "cursors_persist"
        );
        registry.set_ready(64).unwrap();
        assert_eq!(registry.get(64).unwrap().state, RegistrationState::Ready);
        assert_eq!(registry.get(64).unwrap().cursor, None);

        // Round trips preserve the counter: reopening consumes no IDs.
        let mut reopened = Registry::decode(&registry.encode()).unwrap();
        assert_eq!(reopened, registry);
        assert_eq!(reopened.register("a/1", RegistrationKind::Index), Ok(64));
        reopened.disable(64).unwrap();
        assert!(reopened.active("a/1").is_none());
        assert_eq!(
            reopened.register("a/1", RegistrationKind::Index),
            Ok(66),
            "disabled_ids_stay_reserved"
        );
        assert_eq!(reopened.entries().len(), 3);

        // The last ID fits the key prefixes' u32; the next registration fails
        // without changing anything.
        reopened.next_id = u64::from(u32::MAX);
        assert_eq!(
            reopened.register("c/1", RegistrationKind::Index),
            Ok(u32::MAX)
        );
        let exhausted = reopened.clone();
        assert_eq!(
            reopened.register("d/1", RegistrationKind::Index),
            Err(RegistryError::Exhausted)
        );
        assert_eq!(reopened, exhausted);
        assert_eq!(
            Registry::decode(&reopened.encode()).unwrap(),
            reopened,
            "exhaustion_is_persisted"
        );

        let newer = Registry {
            format: FORMAT + 1,
            ..Registry::default()
        };
        assert_eq!(
            Registry::decode(&newer.encode()),
            Err(RegistryError::UnsupportedFormat(FORMAT + 1))
        );
    }

    #[test]
    fn transitions_and_persisted_invariants_are_checked() {
        let mut registry = Registry::default();
        let id = registry.register("a/1", RegistrationKind::Index).unwrap();
        let missing = Err(RegistryError::UnknownId(99));
        assert_eq!(registry.set_ready(99), missing);
        assert_eq!(registry.disable(99), missing);
        assert_eq!(registry.set_cursor(99, Bytes::new()), missing);
        registry.set_ready(id).unwrap();
        let ready_again = Err(RegistryError::InvalidTransition {
            id,
            from: RegistrationState::Ready,
            to: RegistrationState::Ready,
        });
        assert_eq!(registry.set_ready(id), ready_again);
        assert!(
            registry.set_cursor(id, Bytes::new()).is_err(),
            "ready_instances_have_no_cursor"
        );
        registry.disable(id).unwrap();
        assert!(
            registry.disable(id).is_err(),
            "disabled_instances_stay_disabled"
        );

        // Each persisted state that breaks an invariant is rejected on load.
        let valid = {
            let mut valid = Registry::default();
            valid.register("a/1", RegistrationKind::Index).unwrap();
            valid.register("b/1", RegistrationKind::Statistic).unwrap();
            valid
        };
        type Corruption = (&'static str, fn(&mut Registry));
        let broken: [Corruption; 7] = [
            ("counter below the floor", |r| r.next_id = 10),
            ("ID not issued", |r| r.next_id = 65),
            ("ID below the floor", |r| r.entries[0].id = 3),
            ("duplicate ID", |r| r.entries[1].id = 64),
            ("unknown kind", |r| {
                r.entries[0].kind = RegistrationKind::Unknown
            }),
            ("two active instances", |r| {
                r.entries[1].definition = "a/1".into()
            }),
            ("cursor on a Ready instance", |r| {
                r.entries[0].state = RegistrationState::Ready;
                r.entries[0].cursor = Some(Bytes::from_static(b"k"));
            }),
        ];
        assert!(Registry::decode(&valid.encode()).is_ok());
        for (case, corrupt) in broken {
            let mut registry = valid.clone();
            corrupt(&mut registry);
            assert!(
                matches!(
                    Registry::decode(&registry.encode()),
                    Err(RegistryError::Invalid(_))
                ),
                "{case}"
            );
        }
    }
}
