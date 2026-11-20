// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The indexes maintained on every entry write, and how a write reaches them.
//!
//! **Coverage.** An index reflects an entry when its keys or counts match that
//! entry's contribution, including an empty contribution outside its membership.
//! A Complete index reflects every entry. A Backfilling index reflects exactly the
//! entries that list its build job's token (the job has the index's ID): the
//! token is the only proof. A walk's cursor only records the walk's progress;
//! no write consults it.
//!
//! A write treats an uncovered entry's before-image as absent (*adopt on write*):
//! a secondary index adds its new key without deleting an old one; an aggregate
//! counts its new bucket without uncounting. After any write the entry holds a
//! token for every Backfilling index, so the list it stores is always the same:
//! the Backfilling indexes' IDs.
//!
//! For example, with index 64 Complete and 70 Backfilling, an entry stored with no
//! list is reflected by 64 only. Updating it removes its old key from 64, adds
//! its new keys to both, and stores the list `[70]`. Once 70 is Complete too, its
//! next write stores no list at all.
//!
//! Inserts have no before-image. A backfill establishes coverage the same way: it
//! applies an entry to its own indexes and adds their tokens to the entry, in one
//! commit (see [`super::backfill`]).

use std::sync::Arc;
use std::time::Duration;

use restate_storage_api::StorageError;
use restate_storage_api::migration_job::MigrationJobIds;
use restate_storage_api::vqueue_table::{EntryChange, EntryContext, EntryStateRef};
use restate_types::SemanticRestateVersion;
use restate_types::config::Configuration;

use crate::PartitionStoreTransaction;
use crate::job::{MigrationJobRecords, MigrationJobs};
use crate::migrations::online::{MigrationDefinition, ONLINE_MIGRATIONS};
use crate::walk::{Walk, Walks};

use super::{IndexKind, Registry};

/// Applies an entry change to one index `id` of a definition. `before` is
/// `None` for insertions and for entries the index does not reflect yet.
pub(crate) type ApplyEntryChange = fn(
    &mut PartitionStoreTransaction<'_>,
    u32,
    &EntryContext<'_>,
    Option<&EntryStateRef<'_>>,
    Option<&EntryStateRef<'_>>,
);

/// Definitions released together, switched on by the same configuration options.
pub(crate) struct Family {
    pub(crate) definitions: &'static [&'static IndexDefinition],
    /// Register and maintain the definitions.
    pub(crate) maintain: fn(&Configuration) -> bool,
    /// Disable their indexes, overriding the other requests.
    pub(crate) reset: fn(&Configuration) -> bool,
}

/// An entry-scoped index or statistic known to this binary.
pub(crate) struct IndexDefinition {
    /// The stable name, including its version.
    pub(crate) name: &'static str,
    pub(crate) kind: IndexKind,
    pub(crate) apply: ApplyEntryChange,
    /// Whether a walk can fill an entry it finds without this definition's token:
    /// apply the entry and add the token (an *active* definition). A *passive* one
    /// is maintained by writes only; its walk counts the entries it can't cover,
    /// and its build waits until writes or deletions have covered them (see
    /// [`crate::job`]). Not persisted: a release may add a definition's fill.
    pub(crate) can_fill: bool,
    /// The Restate version that introduced the definition, from its declaration
    /// ([`crate::index::RegisteredIndex::INTRODUCED`],
    /// [`crate::stats::Stat::INTRODUCED`]). Never newer than the binary. Not
    /// persisted: it describes this binary's code, not a stored index.
    pub(crate) introduced: SemanticRestateVersion,
}

/// The version test definitions claim.
#[cfg(test)]
pub(crate) const TEST_INTRODUCED: SemanticRestateVersion = SemanticRestateVersion::new(1, 8, 2);

/// The definitions this binary maintains. Opening a store retires the indexes of
/// any other definition.
pub(crate) const INDEX_DEFINITIONS: &[&IndexDefinition] = &[
    &crate::vqueue_table::SERVICE_STATS,
    &crate::vqueue_table::DEPLOYMENT_STATS,
    &crate::vqueue_table::VIRTUAL_OBJECT_STATS,
    &crate::vqueue_table::ENTRY_BY_SERVICE_CREATED,
    &crate::vqueue_table::FINISHED_ENTRY_BY_OUTCOME,
    &crate::vqueue_table::ACTIVE_ENTRY_BY_STAGE,
    &crate::vqueue_table::ENTRY_BY_CREATED,
    &crate::vqueue_table::ENTRY_BY_STATUS_CREATED,
    &crate::vqueue_table::ENTRY_BY_STATUS_TRANSITIONED,
];

struct Active {
    id: u32,
    complete: bool,
    definition: &'static IndexDefinition,
    /// The walk backfilling this Backfilling index, once started.
    walk: Option<u64>,
}

/// The committed registry, walks and unfinished jobs, published to every
/// transaction and handle after each commit that changes them, together with
/// the indexes writes maintain, the Backfilling and Complete indexes of known
/// definitions, and the migrations this binary knows, whose walks it runs.
/// Finished jobs are never published: they stay on disk only.
#[derive(Default)]
pub(crate) struct PublishedRegistry {
    /// The indexes writes maintain, sorted by ID.
    entries: Vec<Active>,
    /// The list every written entry stores: the Backfilling IDs as sorted
    /// big-endian `u32`s. Empty when every index is Complete.
    maintaining: Vec<u8>,
    /// The committed registry and walks, from which `entries` is derived. The
    /// registry is shared with the successors of commits that don't change it.
    registry: Arc<Registry>,
    walks: Walks,
    /// The committed unfinished jobs, which slices update, and how long the
    /// record of a job that finishes is kept.
    jobs: MigrationJobs,
    retention: Duration,
    migrations: &'static [&'static MigrationDefinition],
}

impl std::fmt::Debug for PublishedRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(
                self.entries
                    .iter()
                    .map(|entry| (entry.id, entry.definition.name, entry.complete)),
            )
            .finish()
    }
}

impl PublishedRegistry {
    /// Publishes `registry` and `walks`, without job records and with a
    /// retention of 0, until [`Self::with_jobs`] sets both. Writes maintain the
    /// indexes whose definition is in `known`, with a matching kind; opening a
    /// store retires the others before writes. Walks run the migrations of
    /// [`ONLINE_MIGRATIONS`], unless [`Self::with_migrations`] says otherwise.
    pub(crate) fn new(
        registry: &Registry,
        walks: &Walks,
        known: &[&'static IndexDefinition],
    ) -> Self {
        Self::build(Arc::new(registry.clone()), walks.clone(), known)
    }

    /// Adds the committed unfinished `jobs`; a job that finishes from now on is
    /// kept for `retention`.
    pub(crate) fn with_jobs(mut self, jobs: MigrationJobs, retention: Duration) -> Self {
        self.jobs = jobs;
        self.retention = retention;
        self
    }

    /// Runs the walks of `migrations`, the ones this binary knows.
    pub(crate) fn with_migrations(
        mut self,
        migrations: &'static [&'static MigrationDefinition],
    ) -> Self {
        self.migrations = migrations;
        self
    }

    /// The IDs of the migrations in `registry` that this binary doesn't know, a
    /// later release's: no walk of theirs runs here, so their jobs can't
    /// progress.
    pub(crate) fn unknown_migrations(&self, registry: &Registry) -> Vec<u32> {
        registry
            .migrations()
            .iter()
            .filter(|migration| {
                !self
                    .migrations
                    .iter()
                    .any(|known| migration.definition == known.name)
            })
            .map(|migration| migration.id)
            .collect()
    }

    /// What to publish once a commit changed the walks or the jobs to `walks`
    /// and `jobs`, and the registry to `registry` if it changed it, for the same
    /// known definitions and retention.
    pub(crate) fn successor(
        &self,
        registry: Option<Registry>,
        walks: Walks,
        jobs: MigrationJobs,
    ) -> Self {
        let known: Vec<_> = self.entries.iter().map(|entry| entry.definition).collect();
        let registry = registry.map_or_else(|| Arc::clone(&self.registry), Arc::new);
        Self::build(registry, walks, &known)
            .with_jobs(jobs, self.retention)
            .with_migrations(self.migrations)
    }

    fn build(registry: Arc<Registry>, walks: Walks, known: &[&'static IndexDefinition]) -> Self {
        let mut entries: Vec<_> = registry
            .indexes()
            .iter()
            .filter(|index| index.is_maintained())
            .filter_map(|index| {
                known
                    .iter()
                    .find(|definition| {
                        index.definition == definition.name && definition.kind == index.kind
                    })
                    .map(|definition| Active {
                        id: index.id,
                        complete: index.state == super::IndexStatus::Complete,
                        definition,
                        walk: index.walk,
                    })
            })
            .collect();
        entries.sort_unstable_by_key(|entry| entry.id);
        let maintaining = entries
            .iter()
            .filter(|entry| !entry.complete)
            .flat_map(|entry| entry.id.to_be_bytes())
            .collect();
        Self {
            entries,
            maintaining,
            registry,
            walks,
            jobs: MigrationJobs::default(),
            retention: Duration::ZERO,
            migrations: ONLINE_MIGRATIONS,
        }
    }

    /// The committed registry.
    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The committed walks.
    pub(crate) fn walks(&self) -> &Walks {
        &self.walks
    }

    /// The committed unfinished jobs.
    pub(crate) fn jobs(&self) -> &MigrationJobs {
        &self.jobs
    }

    /// The committed unfinished jobs, for an operation to change: the jobs it
    /// finishes are kept for the published retention.
    pub(crate) fn job_records(&self) -> MigrationJobRecords {
        MigrationJobRecords::new(self.jobs.clone(), self.retention)
    }

    /// The indexes walk `walk` backfills, or an error if a member's definition
    /// is unknown here.
    pub(crate) fn members(
        &self,
        walk: u64,
    ) -> Result<Vec<(&'static IndexDefinition, u32)>, StorageError> {
        let members: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.walk == Some(walk))
            .map(|entry| (entry.definition, entry.id))
            .collect();
        if members.len() != self.registry.members(walk).count() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(members)
    }

    /// The migration `walk` runs and its entry's ID, if this binary knows that
    /// migration.
    pub(crate) fn migration_of(&self, walk: &Walk) -> Option<(&'static MigrationDefinition, u32)> {
        let entry = self.registry.migration_of_walk(walk.id)?;
        let definition = self
            .migrations
            .iter()
            .find(|definition| entry.definition == definition.name && definition.runs(walk))?;
        Some((definition, entry.id))
    }

    /// The index ID of the Complete index of `definition`, or why there is none.
    pub(crate) fn complete_index(&self, definition: &str, kind: IndexKind) -> Result<u32, String> {
        let entry = self
            .entries
            .iter()
            .find(|entry| entry.definition.name == definition && entry.definition.kind == kind)
            .ok_or_else(|| "no maintained index".to_owned())?;
        if !entry.complete {
            return Err(format!("index {} is still being completed", entry.id));
        }
        Ok(entry.id)
    }

    /// The token list every entry written now stores.
    pub(crate) fn touched_by_on_write(&self) -> &[u8] {
        &self.maintaining
    }

    /// Applies `change` to every maintained index. The entry's stored list is only
    /// consulted while some index is Backfilling.
    pub(crate) fn apply(
        &self,
        storage: &mut PartitionStoreTransaction<'_>,
        context: &EntryContext<'_>,
        change: &EntryChange<'_>,
    ) {
        let touched_by = change
            .stored_before()
            .map_or(MigrationJobIds::EMPTY, |before| before.touched_by());
        for entry in &self.entries {
            let before = change
                .before()
                .filter(|_| entry.complete || touched_by.contains(entry.id));
            (entry.definition.apply)(storage, entry.id, context, before, change.after());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::index::RegisteredIndex;
    use crate::stats::Stat;
    use crate::stats::aggregated::{DeploymentStats, ServiceStats, VirtualObjectStats};
    use crate::vqueue_table::{
        ActiveEntryByStage, EntryByCreated, EntryByServiceCreated, EntryByStatusCreated,
        EntryByStatusTransitioned, FinishedEntryByOutcome,
    };

    use super::*;

    /// Every definition this binary maintains carries the version that
    /// introduced it, as its declaration and its release notes state, and none
    /// is newer than the binary.
    #[test]
    fn definitions_carry_the_version_that_introduced_them() {
        let declared = [
            (ServiceStats::DEFINITION, ServiceStats::INTRODUCED),
            (DeploymentStats::DEFINITION, DeploymentStats::INTRODUCED),
            (
                VirtualObjectStats::DEFINITION,
                VirtualObjectStats::INTRODUCED,
            ),
            (
                EntryByServiceCreated::DEFINITION,
                EntryByServiceCreated::INTRODUCED,
            ),
            (
                FinishedEntryByOutcome::DEFINITION,
                FinishedEntryByOutcome::INTRODUCED,
            ),
            (
                ActiveEntryByStage::DEFINITION,
                ActiveEntryByStage::INTRODUCED,
            ),
            (EntryByCreated::DEFINITION, EntryByCreated::INTRODUCED),
            (
                EntryByStatusCreated::DEFINITION,
                EntryByStatusCreated::INTRODUCED,
            ),
            (
                EntryByStatusTransitioned::DEFINITION,
                EntryByStatusTransitioned::INTRODUCED,
            ),
        ];
        let carried = INDEX_DEFINITIONS
            .iter()
            .map(|definition| (definition.name, definition.introduced.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            carried, declared,
            "each definition carries its declared version"
        );
        let binary = SemanticRestateVersion::current();
        for definition in INDEX_DEFINITIONS {
            assert_eq!(
                definition.introduced.to_string(),
                "1.8.2",
                "{}",
                definition.name
            );
            assert!(
                binary.release_is_equal_or_newer_than(&definition.introduced),
                "{}",
                definition.name
            );
        }
    }
}
