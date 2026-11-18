// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! One-time migrations in the registry: their entries, their lifecycle, and how
//! a walk runs one. To write a migration, read the guide in
//! `crate::migrations::online` instead; this is the machinery behind it.
//!
//! A **migration entry** records that this store runs or ran a migration
//! definition, under an ID from the registry's one counter, which also issues
//! index IDs. The entry, not the job record, is authoritative: its job has the
//! same ID and only reports how the work goes.
//!
//! ```text
//! open (fenced) ──register──▶ InProgress ──walk exhausted──▶ Complete (kept forever)
//!                   │              └─ walk lost: the next open restarts one
//!                   └─ empty source ──────────────────────────▶ Complete (no walk, no job)
//! ```
//!
//! - **Gate.** An open registers migrations only on a store that carries the
//!   `registry` storage-feature fence, set before this open or by its index
//!   registrations; a migration never sets the fence itself. A store without it
//!   gets no entry, no walk and no job, so no new version floor.
//! - **Completion.** The slice that exhausts the walk sets `Complete` in the
//!   registry it commits and publishes, like an index becoming Ready; its job
//!   finishes `Completed`. Expiring job records never restart a migration.
//! - **Unknown definitions**, a newer release's migrations, follow the entry's
//!   [`MigrationOnUnknown`]: `Keep` leaves the entry as it is, unlinked from its
//!   walk if the open discarded that walk; `Refuse` fails the open, naming the
//!   migration.
//!
//! The adapter at the bottom runs an `OnlineMigration` as a `WalkJob`: a fresh
//! value of the migration per slice, one job, the entry's completion.

use tracing::{error, info};

use restate_storage_api::StorageError;
use restate_types::SemanticRestateVersion;
use restate_util_string::ReString;

use crate::job::{MigrationJob, MigrationJobKind, MigrationJobRecords, Stamp};
use crate::migrations::online::{MigrationDefinition, OnlineMigration};
use crate::walk::Walks;
use crate::walk::engine::{self, Settlement, SliceInput, Staged, StagedSlice, WalkJob};
use crate::{PartitionDb, PartitionStoreTransaction};

use super::{Registry, RegistryError};

/// Where a migration is in its life, stored on its entry as its number.
///
/// **Compatibility.** The entry stores the number, not a bilrost enumeration, so
/// a state a later release adds doesn't make the whole registry unreadable: this
/// binary keeps such an entry as it is. Never reuse a number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationState {
    /// Registered; its walk runs, or the next open restarts one.
    InProgress = 1,
    /// Done, for good: kept forever, never run again.
    Complete = 2,
}

impl MigrationState {
    /// The state stored as `number`, if this binary knows it.
    pub fn from_number(number: u32) -> Option<Self> {
        match number {
            1 => Some(Self::InProgress),
            2 => Some(Self::Complete),
            _ => None,
        }
    }
}

/// What an open by a binary that doesn't know a migration does with its entry,
/// stored on the entry as its number.
///
/// **Compatibility.** A number this binary doesn't know means `Refuse`, so an
/// unset or unknown policy fails safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationOnUnknown {
    /// The open fails, naming the migration. A migration with this policy
    /// raises the version floor before it registers.
    Refuse = 0,
    /// The open leaves the entry as it is. If the entry is in progress, its walk
    /// follows the walk's own policy, `Discard`: the entry is unlinked, and the
    /// next binary that knows the migration restarts it from the beginning.
    Keep = 1,
}

impl MigrationOnUnknown {
    pub fn from_number(number: u32) -> Self {
        if number == Self::Keep as u32 {
            Self::Keep
        } else {
            Self::Refuse
        }
    }
}

/// When a migration went through its states, for operators. Every field is
/// `None` until its event happens, or when unknown. Nothing reads it to decide
/// anything. Times are wall-clock milliseconds since the epoch on this replica.
#[derive(Debug, Clone, Default, PartialEq, Eq, bilrost::Message)]
pub struct MigrationTimeline {
    #[bilrost(1)]
    pub registered_at: Option<u64>,
    /// The Restate version that registered it.
    #[bilrost(2)]
    pub registered_by: Option<ReString>,
    #[bilrost(3)]
    pub completed_at: Option<u64>,
    /// The Restate version that completed it.
    #[bilrost(4)]
    pub completed_by: Option<ReString>,
}

/// One migration definition run in this store: its ID, state, policy and,
/// while in progress, the walk running it. Persisted tags never change meaning
/// (README rule 8).
#[derive(Debug, Clone, PartialEq, Eq, bilrost::Message)]
pub struct Migration {
    /// The versioned name, such as `"jc_orphan_cleanup/1"`.
    #[bilrost(1)]
    pub definition: ReString,
    #[bilrost(2)]
    pub id: u32,
    /// A [`MigrationState`] number.
    #[bilrost(3)]
    pub state: u32,
    /// The walk running it, while in progress.
    #[bilrost(4)]
    pub walk: Option<u64>,
    /// A [`MigrationOnUnknown`] number.
    #[bilrost(5)]
    pub on_unknown: u32,
    #[bilrost(6)]
    pub timeline: MigrationTimeline,
}

impl Migration {
    /// The state, if this binary knows it.
    pub fn state(&self) -> Option<MigrationState> {
        MigrationState::from_number(self.state)
    }

    pub fn on_unknown(&self) -> MigrationOnUnknown {
        MigrationOnUnknown::from_number(self.on_unknown)
    }

    fn is_in_progress(&self) -> bool {
        self.state() == Some(MigrationState::InProgress)
    }
}

impl Registry {
    /// All migrations, complete ones included, in registration order.
    pub fn migrations(&self) -> &[Migration] {
        &self.migrations
    }

    /// The migration of `definition`, if this store registered it.
    pub fn migration(&self, definition: &str) -> Option<&Migration> {
        self.migrations
            .iter()
            .find(|migration| migration.definition == definition)
    }

    /// The migration with ID `id`, if any.
    pub fn get_migration(&self, id: u32) -> Option<&Migration> {
        self.migrations.iter().find(|migration| migration.id == id)
    }

    /// The migration that walk `walk` runs, if any.
    pub fn migration_of_walk(&self, walk: u64) -> Option<&Migration> {
        self.migrations
            .iter()
            .find(|migration| migration.walk == Some(walk))
    }

    /// The walk running entry `id`, an index or a migration.
    pub fn walk_of(&self, id: u32) -> Option<u64> {
        match self.get(id) {
            Some(index) => index.walk,
            None => self.get_migration(id).and_then(|migration| migration.walk),
        }
    }

    /// Registers `definition` in progress under a fresh ID, with `on_unknown`,
    /// at `at` by `by`, or returns the ID it already has: a definition registers
    /// once, ever. Errors leave the registry unchanged.
    pub fn register_migration(
        &mut self,
        definition: &str,
        on_unknown: MigrationOnUnknown,
        at: u64,
        by: ReString,
    ) -> Result<u32, RegistryError> {
        if let Some(migration) = self.migration(definition) {
            return Ok(migration.id);
        }
        let id = self.issue_id()?;
        self.migrations.push(Migration {
            definition: ReString::from(definition),
            id,
            state: MigrationState::InProgress as u32,
            walk: None,
            on_unknown: on_unknown as u32,
            timeline: MigrationTimeline {
                registered_at: Some(at),
                registered_by: Some(by),
                ..MigrationTimeline::default()
            },
        });
        Ok(id)
    }

    /// Links the in-progress migration `id` to `walk` (`Some`), or unlinks
    /// migration `id`, whatever its state, from the walk it had (`None`).
    pub(crate) fn set_migration_walk(
        &mut self,
        id: u32,
        walk: Option<u64>,
    ) -> Result<(), RegistryError> {
        let migration = match walk {
            Some(_) => self.in_progress(id)?,
            None => self.migration_mut(id)?,
        };
        migration.walk = walk;
        Ok(())
    }

    /// Completes the in-progress migration `id` at `at` by `by`: unlinks its walk
    /// and makes it Complete, for good.
    pub fn complete(&mut self, id: u32, at: u64, by: ReString) -> Result<(), RegistryError> {
        let migration = self.in_progress(id)?;
        migration.state = MigrationState::Complete as u32;
        migration.walk = None;
        migration.timeline.completed_at = Some(at);
        migration.timeline.completed_by = Some(by);
        Ok(())
    }

    fn migration_mut(&mut self, id: u32) -> Result<&mut Migration, RegistryError> {
        self.migrations
            .iter_mut()
            .find(|migration| migration.id == id)
            .ok_or(RegistryError::UnknownId(id))
    }

    fn in_progress(&mut self, id: u32) -> Result<&mut Migration, RegistryError> {
        let migration = self.migration_mut(id)?;
        if !migration.is_in_progress() {
            return Err(RegistryError::NotInProgress(id));
        }
        Ok(migration)
    }
}

/// Registers each of the `known` definitions that this store hasn't, and
/// restarts the walk of each in-progress one that lost it, at `stamp`. A
/// definition over an empty source completes at once, with no walk and no job.
/// Complete entries, and entries in a state this binary doesn't know, are left
/// alone. The caller checks the gate, and settles the jobs afterwards, which
/// links a restarted walk's job or rebuilds a lost one.
pub(super) fn reconcile(
    db: &PartitionDb,
    registry: &mut Registry,
    walks: &mut Walks,
    jobs: &mut MigrationJobRecords,
    known: &[&'static MigrationDefinition],
    stamp: &Stamp,
) -> Result<(), StorageError> {
    let registry_error = |err: RegistryError| StorageError::Conversion(err.into());
    for definition in known {
        let (id, registered) = match registry.migration(definition.name) {
            None => match registry.register_migration(
                definition.name,
                definition.on_unknown,
                stamp.at,
                stamp.by.clone(),
            ) {
                Ok(id) => (id, true),
                Err(RegistryError::Exhausted) => {
                    error!(
                        migration = definition.name,
                        "Cannot register: all IDs of this partition's registry are allocated"
                    );
                    continue;
                }
                Err(err) => return Err(registry_error(err)),
            },
            Some(migration) if migration.is_in_progress() && migration.walk.is_none() => {
                (migration.id, false)
            }
            Some(_) => continue,
        };
        match engine::last_key(db, definition.source)? {
            Some(end) => {
                let walk = walks
                    .start_migration(registry, definition, end)
                    .map_err(engine::walk_error)?;
                registry
                    .set_migration_walk(id, Some(walk))
                    .map_err(registry_error)?;
                if registered {
                    jobs.unfinished.insert(MigrationJob::new(
                        id,
                        MigrationJobKind::Migration,
                        ReString::from_static(definition.name),
                        Some(walk),
                        Some(stamp.at),
                        Some(stamp.by.clone()),
                    ));
                }
                info!(
                    migration = definition.name,
                    id,
                    walk,
                    restart = !registered,
                    "Started the walk of a migration"
                );
            }
            None => {
                registry
                    .complete(id, stamp.at, stamp.by.clone())
                    .map_err(registry_error)?;
                info!(
                    migration = definition.name,
                    id, "Completed a migration: its source is empty"
                );
            }
        }
    }
    Ok(())
}

/// Stages one slice of migration `M`'s walk, as the job of entry `id`: the
/// [`crate::migrations::online::MigrationDefinition::run`] of `M`.
#[cfg_attr(not(test), expect(dead_code, reason = "ONLINE_MIGRATIONS is empty"))]
pub(crate) fn run<M: OnlineMigration>(
    tx: &mut PartitionStoreTransaction<'_>,
    input: &SliceInput<'_>,
    id: u32,
    examined: &mut usize,
) -> Result<StagedSlice, StorageError> {
    let job = MigrationWalk::<M> {
        migration: M::default(),
        id,
    };
    engine::stage_slice(tx, input, job, examined)
}

/// An [`OnlineMigration`] as the one job of its walk. A fresh value runs each
/// slice ([`run`]), so nothing it decided outlives the slice's transaction.
#[cfg_attr(not(test), expect(dead_code, reason = "ONLINE_MIGRATIONS is empty"))]
struct MigrationWalk<M> {
    migration: M,
    /// The migration's entry ID, which is its job's ID.
    id: u32,
}

impl<M: OnlineMigration> WalkJob for MigrationWalk<M> {
    type Entry = M::Row;

    fn decode(raw: &[u8], value: &[u8]) -> Result<M::Row, StorageError> {
        M::decode(raw, value)
    }

    fn job_ids(&self) -> impl Iterator<Item = u32> {
        std::iter::once(self.id)
    }

    fn stage(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        _db: &PartitionDb,
        _raw_key: &[u8],
        row: &M::Row,
    ) -> Result<Staged, StorageError> {
        let migrated = self.migration.migrate(tx, row)?;
        Ok(Staged {
            filled: u64::from(migrated.changed),
            uncovered: 0,
            read_bytes: migrated.read_bytes,
        })
    }

    /// A migration's walk has no members, so its one pass is always full and
    /// without gaps: the migration completes.
    fn finish(
        &self,
        registry: &mut Registry,
        settled: &[(u32, Settlement)],
        at: u64,
    ) -> Result<(), StorageError> {
        for (id, settlement) in settled {
            if *settlement != Settlement::Certified {
                return Err(StorageError::DataIntegrityError);
            }
            let by = ReString::from(SemanticRestateVersion::current().to_string());
            registry
                .complete(*id, at, by)
                .map_err(|err| StorageError::Conversion(err.into()))?;
        }
        Ok(())
    }
}
