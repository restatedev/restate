// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Opening a partition reconciles its registry with this binary and its
//! configuration, before the partition processor starts writing:
//!
//! 0. **Discard** the walks of a kind or source this binary doesn't know, if they
//!    allow it (see [`crate::walk::OnUnknown`]); their members and their
//!    migration are unlinked. A walk that doesn't allow it refuses the open
//!    before anything changes, as does a migration this binary doesn't know
//!    whose entry says so (see [`super::MigrationOnUnknown`]).
//! 1. **Retire** maintained indexes of definitions this binary does not know (a
//!    rollback, or another build): their keys are range-deleted and they become
//!    Dropped, keeping their IDs reserved.
//! 2. **Reset** the indexes of families whose reset is requested, the same way.
//! 3. **Register** each requested definition that has no maintained index, under
//!    a fresh ID, with a `Pending` build job.
//!    Running out of IDs skips the request with an error log.
//! 4. **Migrations**, on a store with the `registry` fence only, set before
//!    this open or by its registrations above: register each one this binary
//!    knows that the store hasn't, with a walk and a job, and restart the walk
//!    of each one in progress that lost it (see [`super::migration`]). A
//!    migration over an empty source completes at once.
//! 5. **Settle the job records** (see [`crate::job`]) with the registry, which
//!    decides. Registering and dropping record their times on the indexes'
//!    timelines and jobs. Records that disagree follow the registry: a job whose
//!    index is Ready or dropped, or whose migration is Complete, is finished at
//!    an unknown time, and a Backfilling index or a migration in progress
//!    without a job gets one with unknown times. A job that
//!    finishes gets its final record, which expires after the configured
//!    retention. Unreadable job records are rebuilt or deleted, never fail the
//!    open.
//! 6. **Start** one walk for the eligible Backfilling indexes that have none,
//!    over the entries that exist now (see [`super::backfill`]): new
//!    registrations, indexes whose definition can fill now, and passive ones
//!    whose wait is over. A partition without entries has nothing to walk: those
//!    indexes become Ready instead.
//! 7. **Fence:** once an index was ever registered, the `registry` storage
//!    feature is in the store's ledger, so older binaries cannot open it.
//!
//! ```text
//! open ─▶ load (one scan) + validate ─▶ retire · reset · register · start walks
//!          (an invalid state fails the open)              │
//!                          one batch: the changed records (see `records`) + fence
//!                                                         │
//!                                     flush ─▶ install for writes ─▶ processor runs,
//!                                                                    walks run in slices
//! ```
//!
//! The changed records, the registry, walks (see [`crate::walk`]) and job
//! records, and the fence are written in one batch; records this open didn't
//! change are not rewritten. The store is then flushed whenever it has any
//! registration history, even when this open changed nothing: an earlier
//! attempt may have written registry state or effects without finishing its
//! flush. Only once the flush succeeded are the reconciled indexes installed for
//! writes, so the registry, the walks and the fence are durable before any
//! effect depends on them, and a failed flush publishes nothing.
//!
//! Reconciliation, from its reads to that publication, is work of the
//! partition's session, like the legacy cleanup: closing the partition waits for
//! it, and a closed partition rejects it with `OperationalError`, so the handle
//! of a closed partition can neither write nor publish into a later session.
//!
//! Walks do not run here. They run in the background, a slice per commit,
//! through the walk engine (see [`crate::walk::engine`]) once the processor is
//! running. Until a walk reaches its end, its indexes stay Backfilling: writes
//! maintain them and queries of them fail. A started walk keeps its cursor
//! across restarts and resumes after it; the entries it filled keep their tokens.
//!
//! The registry, the walks, their positions, their links and the fence walks
//! require are validated before any of this, and before any cleanup or
//! storage-feature migration of the same open (`validate_registry`).

use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use rocksdb::WriteBatch;
use tracing::{error, info, warn};
use zerocopy::IntoBytes;

use restate_clock::WallClock;
use restate_rocksdb::{IoMode, Priority};
use restate_storage_api::StorageError;
use restate_types::SemanticRestateVersion;
use restate_types::config::{Configuration, StorageOptions};
use restate_util_string::ReString;

use crate::faults::{self, FaultPoint};
use crate::features::{LoadedStorageFeatures, StorageFeatures};
use crate::fsm_table::get_min_restate_version_from_partition_db;
use crate::job::{
    MigrationJob, MigrationJobId, MigrationJobKind, MigrationJobRecords, MigrationJobStatus, Stamp,
};
use crate::keys::IndexKeyPrefix;
use crate::migrations::online::{MigrationDefinition, ONLINE_MIGRATIONS};
use crate::registry::{MigrationOnUnknown, MigrationState};
use crate::stats::AggregateKeyPrefix;
use crate::walk::{Source, Walks};
use crate::{MigrationError, PartitionDb, PartitionStore};

use super::records::{self, BatchSink, RecordKey, RecordKind, RecordSink, Records, View};
use super::{
    DropReason, Family, Index, IndexDefinition, IndexKind, IndexStatus, PublishedRegistry,
    Registry, RegistryError, backfill,
};

/// The released families.
pub(crate) const FAMILIES: &[Family] = &[
    crate::vqueue_table::STATUS_GAUGES,
    crate::vqueue_table::ENTRY_INDEXES,
];

/// What opening a partition asks of its registry.
pub(crate) struct Requests {
    /// Definitions to register when they have no maintained index.
    pub(crate) maintain: Vec<&'static IndexDefinition>,
    /// Definitions whose maintained index is dropped.
    pub(crate) reset: Vec<&'static IndexDefinition>,
    /// How long the record of a job that finishes is kept
    /// (`worker.storage.finished-migration-jobs-retention`).
    pub(crate) retention: Duration,
    /// The one-time migrations this binary knows. Each registers once, on a
    /// fenced store; entries of any other migration follow their policy.
    pub(crate) migrations: &'static [&'static MigrationDefinition],
}

impl Default for Requests {
    /// No requests and no migrations, with the default retention.
    fn default() -> Self {
        Self {
            maintain: Vec::new(),
            reset: Vec::new(),
            retention: StorageOptions::default()
                .finished_migration_jobs_retention
                .to_std(),
            migrations: &[],
        }
    }
}

impl Requests {
    pub(crate) fn from_config(families: &[Family], config: &Configuration) -> Self {
        let mut requests = Self {
            retention: config
                .worker
                .storage
                .finished_migration_jobs_retention
                .to_std(),
            migrations: ONLINE_MIGRATIONS,
            ..Self::default()
        };
        for family in families {
            if (family.reset)(config) {
                requests.reset.extend(family.definitions);
            } else if (family.maintain)(config) {
                requests.maintain.extend(family.definitions);
            }
        }
        requests
    }
}

/// The storage-feature state of the open in progress, which the fence updates.
pub(crate) struct FenceState<'a> {
    pub(crate) features: &'a mut LoadedStorageFeatures,
    pub(crate) current_version: &'a SemanticRestateVersion,
    pub(crate) min_restate_version: &'a mut SemanticRestateVersion,
}

impl PartitionStore {
    /// Loads the registry records for opening this partition (see
    /// [`records::load`]) and validates the registry and the walks: their
    /// formats and invariants, every walk endpoint and cursor against its
    /// source's key schema and this partition's key range, and the links
    /// between them. Walks and migrations also require the `registry` fence in
    /// `features`, the loaded storage features, which keeps binaries predating
    /// the registry out of a store with them. A migration this binary doesn't
    /// know, and whose entry refuses such binaries, fails the open, as does a
    /// walk that must not be discarded. Runs before any cleanup or migration
    /// mutates the store; an invalid state fails the open with the store
    /// unchanged. Also returns the IDs of the unreadable job records.
    pub(crate) fn validate_registry(
        &self,
        features: &StorageFeatures,
    ) -> Result<(Records, Vec<u64>), MigrationError> {
        self.validate_registry_knowing(features, ONLINE_MIGRATIONS)
    }

    /// [`Self::validate_registry`] by a binary that knows `migrations`.
    fn validate_registry_knowing(
        &self,
        features: &StorageFeatures,
        migrations: &[&'static MigrationDefinition],
    ) -> Result<(Records, Vec<u64>), MigrationError> {
        let db = self.partition_db();
        let (records, unreadable_jobs) = records::load(db)?;
        let (registry, walks) = (records.registry.as_ref(), &records.walks);
        let entries = registry.map_or(&[][..], Registry::migrations);
        // Before anything changes: a migration this binary doesn't know refuses
        // the open if its entry says so, and so does a walk this binary doesn't
        // know and must not discard. Neither records which release started it;
        // the store's minimum Restate version is the closest hint.
        let refused = match entries.iter().find(|migration| {
            migration.on_unknown() == MigrationOnUnknown::Refuse
                && !migrations
                    .iter()
                    .any(|known| migration.definition == known.name)
        }) {
            Some(migration) => Some(format!(
                "migration '{}' ({}) belongs to a release this binary predates, and refuses \
                 binaries that don't know it; open the store with that release or a newer one",
                migration.definition, migration.id
            )),
            None => walks.preflight(migrations).err().map(|err| err.to_string()),
        };
        if let Some(reason) = refused {
            let minimum = get_min_restate_version_from_partition_db(db)?;
            return Err(MigrationError::MigrationBarrier(format!(
                "{reason}; this store's minimum Restate version is {minimum}"
            )));
        }
        let barrier = |what: String, err: StorageError| {
            MigrationError::MigrationBarrier(format!("invalid {what}: {err}"))
        };
        let integrity = |what: String| barrier(what, StorageError::DataIntegrityError);
        if !(walks.entries().is_empty() && entries.is_empty()) && !features.is_registry {
            return Err(MigrationError::MigrationBarrier(
                "walks or migrations exist without the registry storage feature".to_owned(),
            ));
        }
        for index in registry.iter().flat_map(|registry| registry.indexes()) {
            if let Some(walk) = index.walk
                && walks.get(walk).is_none()
            {
                return Err(integrity(format!(
                    "walk link of '{}' ({})",
                    index.definition, index.id
                )));
            }
        }
        for migration in entries {
            if let Some(walk) = migration.walk
                && walks.get(walk).is_none()
            {
                return Err(integrity(format!(
                    "walk link of migration '{}' ({})",
                    migration.definition, migration.id
                )));
            }
        }
        let positions = |walk: &crate::walk::Walk, source: Source, others: &[&bytes::Bytes]| {
            std::iter::once(&walk.end)
                .chain(walk.cursor.as_ref())
                .chain(others.iter().copied())
                .try_for_each(|key| validate_position(db, source, key))
                .map_err(|err| barrier(format!("position of walk {}", walk.id), err))
        };
        for walk in walks.entries() {
            let linked = registry.and_then(|registry| registry.migration_of_walk(walk.id));
            if walk.is_index_backfill() {
                if let Some(migration) = linked {
                    return Err(integrity(format!(
                        "link of migration '{}' to index backfill {}",
                        migration.definition, walk.id
                    )));
                }
                for member in &walk.members {
                    if registry
                        .and_then(|registry| registry.get(member.index))
                        .is_none_or(|index| index.walk != Some(walk.id))
                    {
                        return Err(integrity(format!(
                            "evidence of walk {} for index {}",
                            walk.id, member.index
                        )));
                    }
                }
                let joins: Vec<_> = walk
                    .members
                    .iter()
                    .filter_map(|member| member.joined_at.as_ref())
                    .collect();
                positions(walk, Source::EntryStatus, &joins)?;
                if registry.is_none_or(|registry| registry.members(walk.id).next().is_none()) {
                    return Err(integrity(format!("walk {} without members", walk.id)));
                }
            } else if let Some(definition) = migrations.iter().find(|known| known.runs(walk)) {
                // A migration's walk runs its own entry, with neither members
                // nor parameters.
                if linked.is_none_or(|migration| migration.definition != definition.name)
                    || registry.is_some_and(|registry| registry.members(walk.id).next().is_some())
                    || !walk.members.is_empty()
                    || !walk.params.is_empty()
                    || walk
                        .cursor
                        .as_ref()
                        .is_some_and(|cursor| *cursor > walk.end)
                {
                    return Err(integrity(format!(
                        "walk {} of migration '{}'",
                        walk.id, definition.name
                    )));
                }
                positions(walk, definition.source, &[])?;
            }
            // Any other walk keeps its positions opaque; the open discards it.
        }
        Ok((records, unreadable_jobs))
    }

    /// Reconciles the registry with `known` definitions and `requests`; see the
    /// module documentation. Fails the open on an unreadable or invalid registry.
    pub(crate) async fn reconcile_registry(
        &mut self,
        known: &[&'static IndexDefinition],
        requests: &Requests,
        fence: FenceState<'_>,
    ) -> Result<(), MigrationError> {
        let FenceState {
            features,
            current_version,
            min_restate_version: current_min_restate_version,
        } = fence;
        let db = self.partition_db().clone();
        // Admitted by the session until the reconciled indexes are published: a
        // close waits for it, and a closed session rejects it.
        let _session = db
            .gate()
            .try_acquire()
            .ok_or(StorageError::OperationalError)?;
        let (persisted, unreadable_jobs) =
            self.validate_registry_knowing(features.enabled(), requests.migrations)?;
        let mut registry = persisted.registry.clone().unwrap_or_default();
        let mut walks = persisted.walks.clone();
        let mut jobs = MigrationJobRecords::new(persisted.jobs.clone(), requests.retention);
        let mut batch = WriteBatch::default();
        let stamp = Stamp {
            at: WallClock::now_ms().as_u64(),
            by: ReString::from(current_version.to_string()),
        };

        // Walks this binary doesn't know, and may discard: their members restart
        // under this binary's kind, or are retired below; their migration waits
        // for a binary that knows it.
        let discarded = walks.discard_unknown(requests.migrations);
        if !discarded.is_empty() {
            let on_discarded =
                |walk: Option<u64>| walk.is_some_and(|walk| discarded.contains(&walk));
            let unlinked: Vec<_> = registry
                .indexes()
                .iter()
                .filter(|index| on_discarded(index.walk))
                .map(|index| index.id)
                .collect();
            for id in &unlinked {
                registry.unlink(*id).map_err(registry_error)?;
            }
            let waiting: Vec<_> = registry
                .migrations()
                .iter()
                .filter(|migration| on_discarded(migration.walk))
                .map(|migration| migration.id)
                .collect();
            for id in &waiting {
                registry
                    .set_migration_walk(*id, None)
                    .map_err(registry_error)?;
            }
            warn!(
                walks = ?discarded,
                indexes = ?unlinked,
                migrations = ?waiting,
                "Discarded walks of a kind this binary doesn't know"
            );
        }

        let unknown: Vec<_> = registry
            .indexes()
            .iter()
            .filter(|index| index.is_maintained())
            .filter(|index| {
                !known.iter().any(|definition| {
                    index.definition == definition.name && index.kind == definition.kind
                })
            })
            .map(|index| (index.id, index.kind, index.definition.clone()))
            .collect();
        for (id, kind, definition) in unknown {
            append_instance_delete(&db, &mut batch, kind, id)?;
            drop_and_record(
                &mut registry,
                &mut jobs,
                id,
                DropReason::UnknownDefinition,
                &stamp,
            )
            .map_err(registry_error)?;
            warn!(
                %definition,
                id,
                "Dropped an index this binary does not know; \
                 its data was deleted"
            );
        }

        for definition in &requests.reset {
            if let Some(id) = registry.maintained(definition.name).map(|active| active.id) {
                append_instance_delete(&db, &mut batch, definition.kind, id)?;
                drop_and_record(&mut registry, &mut jobs, id, DropReason::Reset, &stamp)
                    .map_err(registry_error)?;
                info!(definition = definition.name, id, "Reset the index");
            }
        }
        // A walk whose members were all dropped has nothing left to backfill;
        // surviving members keep their walk, its position and their evidence,
        // and the dropped members' evidence goes with them. A migration's walk
        // has no members: its entry keeps it.
        let abandoned: Vec<_> = walks
            .entries()
            .iter()
            .map(|walk| walk.id)
            .filter(|walk| {
                registry.members(*walk).next().is_none()
                    && registry.migration_of_walk(*walk).is_none()
            })
            .collect();
        for walk in abandoned {
            walks.remove(walk).map_err(walk_error)?;
        }
        walks.retain_members(|walk, index| {
            registry
                .get(index)
                .is_some_and(|linked| linked.walk == Some(walk))
        });

        for definition in &requests.maintain {
            debug_assert!(
                restate_types::SemanticRestateVersion::current()
                    .release_is_equal_or_newer_than(&definition.introduced),
                "{} is newer than this binary",
                definition.name
            );
            let registered = registry.maintained(definition.name).is_some();
            match registry.register(definition.name, definition.kind) {
                Ok(id) if !registered => {
                    if let Some(timeline) = registry.timeline_mut(id) {
                        timeline.registered_at = Some(stamp.at);
                        timeline.registered_by = Some(stamp.by.clone());
                    }
                    backfill::register_job(&registry, &mut jobs, id, &stamp)?;
                }
                Ok(_) => {}
                Err(RegistryError::Exhausted) => {
                    error!(
                        definition = definition.name,
                        "Cannot register: all index IDs of this partition are allocated"
                    );
                }
                Err(err) => return Err(registry_error(err)),
            }
        }

        // Migrations run on a fenced store only: fenced before this open, or by
        // its registrations above. A migration never sets the fence itself.
        if features.enabled().is_registry || !registry.indexes().is_empty() {
            super::migration::reconcile(
                &db,
                &mut registry,
                &mut walks,
                &mut jobs,
                requests.migrations,
                &stamp,
            )?;
        }

        settle_jobs(&registry, &walks, &mut jobs, stamp.at);
        // Backfilling indexes without a walk start a pass over the entries that
        // exist now if they are eligible: new registrations, jobs that can fill
        // now (a definition may have gained its fill), and passive jobs whose
        // wait is over.
        let can_fill = |index: &Index| {
            known
                .iter()
                .any(|definition| index.definition == definition.name && definition.can_fill)
        };
        let unstarted: Vec<_> = registry
            .indexes()
            .iter()
            .filter(|index| index.state == IndexStatus::Backfilling && index.walk.is_none())
            .filter(|index| {
                jobs.unfinished
                    .get(MigrationJobId::for_entry(index.id))
                    .is_none_or(|job| {
                        job.status == MigrationJobStatus::Pending
                            || can_fill(index)
                            || job.is_eligible(stamp.at)
                    })
            })
            .map(|index| index.id)
            .collect();
        if !unstarted.is_empty()
            && let Some(walk) = backfill::start(
                &db,
                &mut registry,
                &mut walks,
                &mut jobs,
                &unstarted,
                stamp.at,
            )?
        {
            info!(walk, indexes = ?unstarted, "Started the backfill of indexes");
        }

        let partition = db.partition().id();
        let fenced = !registry.indexes().is_empty()
            && features.append_registry_fence(
                current_version,
                current_min_restate_version,
                db.cf_handle(),
                &mut batch,
                partition,
            )?;
        // Only what changed: a store where nothing was ever registered gets no
        // registry, and an unreadable job record that nothing replaced goes.
        let mut sink = BatchSink {
            batch: &mut batch,
            cf: db.cf_handle(),
        };
        records::stage_changes(
            &mut sink,
            partition,
            View {
                registry: persisted.registry.as_ref(),
                walks: &persisted.walks,
                jobs: &persisted.jobs,
            },
            View {
                registry: (persisted.registry.is_some() || registry != Registry::default())
                    .then_some(&registry),
                walks: &walks,
                jobs: &jobs.unfinished,
            },
            &jobs.finished,
        )?;
        for id in unreadable_jobs {
            let replaced = u32::try_from(id).is_ok_and(|id| {
                id != 0 && {
                    let id = MigrationJobId::for_entry(id);
                    jobs.unfinished.get(id).is_some()
                        || jobs.finished.iter().any(|job| job.id() == id)
                }
            });
            if !replaced {
                sink.delete(&RecordKey::new(partition, RecordKind::Job, id));
            }
        }
        if fenced || !batch.is_empty() {
            write(&db, batch).await?;
            features.mark_persisted();
            self.set_storage_features(*features.enabled());
        }

        // Whatever this or an earlier attempt wrote becomes durable before
        // anything depends on it: registry changes, walks, fence, deletions,
        // effects.
        if !registry.indexes().is_empty() || !registry.migrations().is_empty() {
            flush(&db).await?;
        }

        // Writes maintain the reconciled indexes from here on, and walks run on
        // the records as committed.
        db.install_published_registry(Arc::new(
            PublishedRegistry::new(&registry, &walks, known)
                .with_jobs(jobs.unfinished, requests.retention)
                .with_migrations(requests.migrations),
        ));
        Ok(())
    }
}

/// Drops index `id` and records why: its timeline gets the time and `reason`,
/// and its build job, if unfinished, finishes Cancelled.
fn drop_and_record(
    registry: &mut Registry,
    jobs: &mut MigrationJobRecords,
    id: u32,
    reason: DropReason,
    stamp: &Stamp,
) -> Result<(), RegistryError> {
    let job_id = MigrationJobId::for_entry(id);
    // Records this open could not read lack the job of a Backfilling index; it
    // gets one, with unknown times, so that its cancelled build is recorded.
    if let Some(index) = registry.get(id)
        && index.state == IndexStatus::Backfilling
        && jobs.unfinished.get(job_id).is_none()
    {
        jobs.unfinished.insert(MigrationJob::index_backfill(
            id,
            index.definition.clone(),
            index.walk,
            None,
            None,
        ));
    }
    registry.drop_index(id)?;
    if let Some(timeline) = registry.timeline_mut(id) {
        timeline.dropped(stamp.at, reason);
    }
    jobs.finish(
        job_id,
        MigrationJobStatus::Cancelled,
        Some(stamp.at),
        stamp.at,
    );
    Ok(())
}

/// Brings the job records in line with the registry and the walks, which decide.
/// The records lag them when they couldn't be read, or when another binary
/// changed builds or migrations without writing records this one reads.
///
/// - An unfinished job whose index is Ready or whose migration is Complete is
///   finished, completed, at an unknown time; one whose entry is dropped or gone
///   is finished, cancelled. A migration in a state this binary doesn't know
///   leaves its job alone.
/// - A job follows its entry's walk: `Running` once that walk has a cursor,
///   keeping an unknown start if it was still `Pending`; a running job whose
///   entry lost its walk waits, eligible now. A job moved onto a walk that has
///   already advanced gets an unknown pass start: the one it had belongs to an
///   earlier pass.
/// - A Backfilling index or a migration in progress without a job gets one,
///   with unknown times: following its walk, or waiting and eligible now without
///   one.
///
/// A job finished here expires one retention after `now`, its finish time
/// being unknown.
fn settle_jobs(registry: &Registry, walks: &Walks, jobs: &mut MigrationJobRecords, now: u64) {
    let started = |walk: Option<u64>| {
        walk.and_then(|walk| walks.get(walk))
            .is_some_and(|walk| walk.cursor.is_some())
    };
    let unfinished: Vec<_> = jobs
        .unfinished
        .entries()
        .iter()
        .map(MigrationJob::id)
        .collect();
    for id in unfinished {
        match Progress::of(registry, id.as_u32()) {
            Progress::Running(walk) => {
                if let Some(job) = jobs.unfinished.get_mut(id) {
                    if job.walk != walk && started(walk) {
                        job.pass_started_at = None;
                    }
                    job.walk = walk;
                    match job.status {
                        MigrationJobStatus::Pending if started(walk) => {
                            job.status = MigrationJobStatus::Running;
                        }
                        MigrationJobStatus::Waiting if walk.is_some() => {
                            job.status = MigrationJobStatus::Running;
                        }
                        MigrationJobStatus::Running if walk.is_none() => {
                            let interval = job.recheck_interval_ms;
                            job.wait(interval, None);
                        }
                        _ => {}
                    }
                }
            }
            Progress::Done => jobs.finish(id, MigrationJobStatus::Completed, None, now),
            Progress::Gone => jobs.finish(id, MigrationJobStatus::Cancelled, None, now),
            Progress::Unknown => {}
        }
    }
    let indexes = registry
        .indexes()
        .iter()
        .filter(|index| index.state == IndexStatus::Backfilling)
        .map(|index| {
            (
                index.id,
                &index.definition,
                index.walk,
                MigrationJobKind::IndexBackfill,
            )
        });
    let migrations = registry
        .migrations()
        .iter()
        .filter(|migration| migration.state() == Some(MigrationState::InProgress))
        .map(|migration| {
            let kind = MigrationJobKind::Migration;
            (migration.id, &migration.definition, migration.walk, kind)
        });
    for (entry, name, walk, kind) in indexes.chain(migrations) {
        if jobs
            .unfinished
            .get(MigrationJobId::for_entry(entry))
            .is_none()
        {
            let mut job = MigrationJob::new(entry, kind, name.clone(), walk, None, None);
            if walk.is_none() {
                job.wait(None, None);
            } else if started(walk) {
                job.status = MigrationJobStatus::Running;
            }
            jobs.unfinished.insert(job);
        }
    }
}

/// What the registry says about the job of an entry.
enum Progress {
    /// A Backfilling index or a migration in progress, with its walk if any.
    Running(Option<u64>),
    /// A Ready index or a Complete migration.
    Done,
    /// A dropped index, or no entry at all.
    Gone,
    /// A migration in a state this binary doesn't know.
    Unknown,
}

impl Progress {
    fn of(registry: &Registry, id: u32) -> Self {
        if let Some(index) = registry.get(id) {
            return match index.state {
                IndexStatus::Backfilling => Self::Running(index.walk),
                IndexStatus::Ready => Self::Done,
                _ => Self::Gone,
            };
        }
        match registry
            .get_migration(id)
            .map(|migration| (migration.state(), migration.walk))
        {
            Some((Some(MigrationState::InProgress), walk)) => Self::Running(walk),
            Some((Some(MigrationState::Complete), _)) => Self::Done,
            Some((None, _)) => Self::Unknown,
            None => Self::Gone,
        }
    }
}

/// Flushes the memtables, after any fault armed at [`FaultPoint::RegistryFlush`].
async fn flush(db: &PartitionDb) -> Result<(), StorageError> {
    let fault = db.faults().take(FaultPoint::RegistryFlush);
    faults::inject(fault)
        .await
        .map_err(|err| StorageError::Generic(err.into()))?;
    db.flush_memtables(true)
        .await
        .map_err(|err| StorageError::Generic(err.into()))
}

/// Deletes every key of the index `id`, whatever their aggregation kind.
fn append_instance_delete(
    db: &PartitionDb,
    batch: &mut WriteBatch,
    kind: IndexKind,
    id: u32,
) -> Result<(), StorageError> {
    let start = match kind {
        IndexKind::Secondary => IndexKeyPrefix::new(db.partition().id(), id)
            .as_bytes()
            .to_vec(),
        IndexKind::Aggregate => AggregateKeyPrefix::new(db.partition().id(), id)
            .as_bytes()
            .to_vec(),
        IndexKind::Unknown => return Err(StorageError::DataIntegrityError),
    };
    let mut end = start.clone();
    if !crate::convert_to_upper_bound(&mut end) {
        return Err(StorageError::DataIntegrityError);
    }
    batch.delete_range_cf(db.cf_handle(), start, end);
    Ok(())
}

/// A walk's end or cursor must be the exact key of a row of its `source` in
/// this partition (see [`Source::partition_key`]).
fn validate_position(db: &PartitionDb, source: Source, key: &[u8]) -> Result<(), StorageError> {
    if !db
        .partition()
        .key_range
        .contains(&source.partition_key(key)?)
    {
        return Err(StorageError::DataIntegrityError);
    }
    Ok(())
}

/// Commits registry changes through the partition's session; the open flushes
/// them before publishing.
async fn write(db: &PartitionDb, batch: WriteBatch) -> Result<(), StorageError> {
    let mut options = rocksdb::WriteOptions::default();
    options.disable_wal(true);
    let lease = db
        .gate()
        .try_acquire()
        .ok_or(StorageError::OperationalError)?;
    db.rocksdb()
        .write_batch_leased(
            "registry-reconciliation",
            Priority::High,
            IoMode::Default,
            options,
            batch,
            lease,
        )
        .await
        .map_err(|err| StorageError::Generic(err.into()))?;
    Ok(())
}

fn walk_error(err: crate::walk::WalkError) -> MigrationError {
    MigrationError::StorageError(StorageError::Conversion(err.into()))
}

fn registry_error(err: RegistryError) -> MigrationError {
    MigrationError::StorageError(StorageError::Conversion(err.into()))
}

#[cfg(test)]
mod tests {
    use restate_rocksdb::RocksDbManager;
    use restate_types::identifiers::PartitionId;
    use restate_types::partition_table::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::{RESTATE_VERSION_1_8_2, SemanticRestateVersion};

    use super::*;
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::registry::records::get_registry;
    use crate::stats::StatKind;
    use crate::stats::aggregated::backfill::tests::seed;
    use crate::walk::Walks;
    use crate::{PartitionStoreManager, PartitionStoreTransaction};

    #[test]
    fn released_families_consist_of_known_definitions() {
        // An unknown definition would be retired and registered again at every open.
        for family in FAMILIES {
            for definition in family.definitions {
                assert!(
                    crate::registry::INDEX_DEFINITIONS
                        .iter()
                        .any(|known| std::ptr::eq(*known, *definition)),
                    "{}",
                    definition.name
                );
            }
        }
        // Nothing is requested by default; a reset overrides maintenance.
        let mut config = Configuration::default();
        let requests = Requests::from_config(FAMILIES, &config);
        assert!(requests.maintain.is_empty() && requests.reset.is_empty());
        config.common.experimental.set_track_status_gauges(true);
        assert_eq!(Requests::from_config(FAMILIES, &config).maintain.len(), 3);
        config.common.experimental.set_status_gauges_reset(true);
        let requests = Requests::from_config(FAMILIES, &config);
        assert!(requests.maintain.is_empty());
        assert_eq!(requests.reset.len(), 3);
    }

    fn ignore(
        _: &mut PartitionStoreTransaction<'_>,
        _: u32,
        _: &restate_storage_api::vqueue_table::EntryContext<'_>,
        _: Option<&restate_storage_api::vqueue_table::EntryStateRef<'_>>,
        _: Option<&restate_storage_api::vqueue_table::EntryStateRef<'_>>,
    ) {
    }

    static INDEX: IndexDefinition = IndexDefinition {
        name: "index/1",
        kind: IndexKind::Secondary,
        apply: ignore,
        can_fill: true,
        introduced: crate::registry::TEST_INTRODUCED,
    };
    static STAT: IndexDefinition = IndexDefinition {
        name: "stat/1",
        kind: IndexKind::Aggregate,
        apply: ignore,
        can_fill: true,
        introduced: crate::registry::TEST_INTRODUCED,
    };

    async fn reconcile(
        store: &mut PartitionStore,
        known: &[&'static IndexDefinition],
        requests: Requests,
    ) -> Result<(), MigrationError> {
        let db = store.partition_db().clone();
        let mut features = LoadedStorageFeatures::load(
            get_storage_features_from_partition_db(&db).unwrap(),
            get_storage_version_from_partition_db(&db).unwrap(),
            SemanticRestateVersion::current(),
        )
        .unwrap();
        let mut min = get_min_restate_version_from_partition_db(&db).unwrap();
        store
            .reconcile_registry(
                known,
                &requests,
                FenceState {
                    features: &mut features,
                    current_version: SemanticRestateVersion::current(),
                    min_restate_version: &mut min,
                },
            )
            .await
    }

    fn persisted(store: &PartitionStore) -> Registry {
        get_registry(store.partition_db()).unwrap().unwrap()
    }

    fn walks(store: &PartitionStore) -> Walks {
        records::load(store.partition_db()).unwrap().0.walks
    }

    fn maintain(definitions: &[&'static IndexDefinition]) -> Requests {
        Requests {
            maintain: definitions.to_vec(),
            ..Default::default()
        }
    }

    /// Raw keys under `prefix`.
    fn keys(store: &PartitionStore, prefix: &[u8]) -> usize {
        let db = store.partition_db().rocksdb().inner().as_raw_db();
        let cf = db.cf_handle(store.partition().cf_name().as_ref()).unwrap();
        let mut options = rocksdb::ReadOptions::default();
        options.set_total_order_seek(true);
        let mut iterator = db.raw_iterator_cf_opt(&cf, options);
        iterator.seek(prefix);
        let mut count = 0;
        while iterator.key().is_some_and(|key| key.starts_with(prefix)) {
            count += 1;
            iterator.next();
        }
        iterator.status().unwrap();
        count
    }

    #[restate_core::test]
    async fn opens_fence_register_retire_and_reset_indexes() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let db = store.partition_db().clone();
        seed(&mut store).await;

        // Without requests nothing is written and the store stays unfenced.
        reconcile(&mut store, &[&INDEX, &STAT], Requests::default())
            .await
            .unwrap();
        assert!(get_registry(&db).unwrap().is_none());
        assert!(!store.storage_features().is_registry);

        // The first registration persists the fence and the minimum version with
        // it, and starts one walk for both indexes.
        reconcile(&mut store, &[&INDEX, &STAT], maintain(&[&INDEX, &STAT]))
            .await
            .unwrap();
        let registry = persisted(&store);
        assert_eq!(registry.maintained("index/1").unwrap().id, 64);
        assert_eq!(registry.maintained("stat/1").unwrap().id, 65);
        assert_eq!(
            (
                registry.get(64).unwrap().walk,
                registry.get(65).unwrap().walk
            ),
            (Some(1), Some(1)),
            "one_walk_for_both"
        );
        assert_eq!(walks(&store).entries().len(), 1);
        let ledger = LoadedStorageFeatures::load(
            get_storage_features_from_partition_db(&db).unwrap(),
            get_storage_version_from_partition_db(&db).unwrap(),
            SemanticRestateVersion::current(),
        )
        .unwrap();
        assert!(ledger.enabled().is_registry, "fenced_on_disk");
        assert!(
            get_min_restate_version_from_partition_db(&db)
                .unwrap()
                .is_equal_or_newer_than(&RESTATE_VERSION_1_8_2)
        );
        assert_eq!(
            store.published_registry().touched_by_on_write(),
            [0, 0, 0, 64, 0, 0, 0, 65],
            "reconciled_indexes_are_installed"
        );

        // Reopening with the same requests changes nothing and allocates nothing.
        reconcile(&mut store, &[&INDEX, &STAT], maintain(&[&INDEX, &STAT]))
            .await
            .unwrap();
        assert_eq!(persisted(&store), registry);
        assert_eq!(walks(&store).entries().len(), 1);

        // Keys of three neighbouring indexes; only the retired one is deleted.
        let index = |id| {
            IndexKeyPrefix::new(db.partition().id(), id)
                .as_bytes()
                .to_vec()
        };
        let stat = |id| {
            let mut key = AggregateKeyPrefix::new(db.partition().id(), id)
                .as_bytes()
                .to_vec();
            key.push(StatKind::AggregatedGauge as u8);
            key
        };
        let raw = db.rocksdb().inner().as_raw_db();
        for key in [
            index(63),
            index(64),
            index(65),
            stat(64),
            stat(65),
            stat(66),
        ] {
            let mut key = key;
            key.extend_from_slice(b"payload");
            raw.put_cf(db.cf_handle(), key, b"").unwrap();
        }

        // A binary that does not know the definition retires its index; the walk
        // keeps its other member.
        reconcile(&mut store, &[&STAT], maintain(&[&STAT]))
            .await
            .unwrap();
        let registry = persisted(&store);
        assert_eq!(registry.get(64).unwrap().state, IndexStatus::Dropped);
        assert_eq!(
            (
                keys(&store, &index(63)),
                keys(&store, &index(64)),
                keys(&store, &index(65))
            ),
            (1, 0, 1),
            "retirement_deletes_exactly_the_index"
        );
        assert_eq!(
            store.published_registry().touched_by_on_write(),
            [0, 0, 0, 65]
        );
        assert_eq!(walks(&store).entries().len(), 1);

        // Resetting the statistic disables it and deletes its keys only; its walk
        // has no member left and is removed.
        reconcile(
            &mut store,
            &[&INDEX, &STAT],
            Requests {
                reset: vec![&STAT],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let registry = persisted(&store);
        assert_eq!(registry.get(65).unwrap().state, IndexStatus::Dropped);
        assert_eq!(
            (
                keys(&store, &stat(64)),
                keys(&store, &stat(65)),
                keys(&store, &stat(66))
            ),
            (1, 0, 1),
            "reset_deletes_exactly_the_index"
        );
        assert!(store.storage_features().is_registry, "fence_is_kept");
        assert!(walks(&store).entries().is_empty(), "abandoned_walks_go");

        // Enabling again builds fresh indexes on a new walk; old IDs stay reserved.
        reconcile(&mut store, &[&INDEX, &STAT], maintain(&[&INDEX, &STAT]))
            .await
            .unwrap();
        let registry = persisted(&store);
        assert_eq!(
            (
                registry.maintained("index/1").unwrap().id,
                registry.maintained("stat/1").unwrap().id
            ),
            (66, 67)
        );
        assert_eq!(registry.get(66).unwrap().walk, Some(2));
    }

    #[restate_core::test]
    async fn exhaustion_skips_registration_and_invalid_registries_fail() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        let db = store.partition_db().clone();
        let mut registry = Registry::default();
        registry.register("stat/1", IndexKind::Aggregate).unwrap();
        registry.next_id = u64::from(u32::MAX) + 1;
        let write = |registry: &Registry| {
            records::testing::overwrite(&db, registry, &Walks::default());
        };
        write(&registry);

        // A request that cannot be registered is skipped; the existing index
        // stays maintained and starts its walk.
        reconcile(&mut store, &[&INDEX, &STAT], maintain(&[&INDEX, &STAT]))
            .await
            .unwrap();
        assert!(persisted(&store).maintained("index/1").is_none());
        assert_eq!(
            store.published_registry().touched_by_on_write(),
            [0, 0, 0, 64]
        );
        assert_eq!(persisted(&store).get(64).unwrap().walk, Some(1));

        // A registry this binary cannot validate fails the open.
        registry.next_id = 10;
        write(&registry);
        assert!(
            reconcile(&mut store, &[&INDEX, &STAT], Requests::default())
                .await
                .is_err()
        );
    }
}

#[cfg(test)]
mod backfill_tests {
    use std::num::NonZeroUsize;
    use std::ops::ControlFlow;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::FutureExt;
    use tokio::time::Instant;
    use tokio_util::sync::CancellationToken;
    use zerocopy::IntoBytes;

    use restate_rocksdb::{RocksDbManager, RocksError};
    use restate_storage_api::invocation_status_table::{
        ReadInvocationStatusTable, WriteInvocationStatusTable,
    };
    use restate_storage_api::vqueue_table::{
        EntryContext, EntryStateRef, EntryStatusHeader, ReadVQueueTable, StoredEntryRef,
        WriteVQueueTable,
    };
    use restate_storage_api::{StorageError, Transaction};
    use restate_storage_query_api::scan::{BlockingScan, Table};
    use restate_types::config::{StorageOptions, ThrottlingOptions, WorkerOptions};
    use restate_types::identifiers::{BaseEntryId, PartitionId, PartitionKey, SnapshotId};
    use restate_types::logs::LogId;
    use restate_types::partition_table::Partition;
    use restate_types::rate::Rate;
    use restate_types::sharding::KeyRange;
    use restate_types::{SemanticRestateVersion, vqueues::EntryTargetRef};

    use super::*;
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::job::MigrationJobs;
    use crate::keys::header::{HeaderBounds, IndexScope, KeyHeader, KeySchema};
    use crate::keys::selection::KeySelection;
    use crate::keys::{DecodeTableKey, KeyDecode, KeyDecoder, KeyKind};
    use crate::registry::records::{get_job, get_registry, testing};
    use crate::snapshots::{LocalPartitionSnapshot, SnapshotDir};
    use crate::stats::aggregated::Gauge;
    use crate::stats::aggregated::backfill::tests::seed;
    use crate::stats::aggregated::backfill::with_entry_target;
    use crate::stats::{StatKind, StatValueCodec};
    use crate::table::{PhysicalTable, ReadMetadata};
    use crate::vqueue_table::EntryStatusKey;
    use crate::walk::engine::{Slice, SliceBudget};
    use crate::walk::{OnUnknown, Source, Walk, WalkDriver, WalkKind, WalkMember, Walks, engine};
    use crate::{
        IndexSizes, PartitionStoreManager, PartitionStoreTransaction, QueryIoExecutor,
        ReadSnapshot, ReplicaMigrations,
    };

    use super::super::backfill::read_chunk;

    const SEEDED: u64 = 147;

    fn count_key(partition: PartitionId, id: u32) -> Vec<u8> {
        let mut key = AggregateKeyPrefix::new(partition, id).as_bytes().to_vec();
        key.push(StatKind::AggregatedGauge as u8);
        key
    }

    /// A statistic counting every entry it reflects.
    fn apply_count(
        storage: &mut PartitionStoreTransaction<'_>,
        id: u32,
        _: &EntryContext<'_>,
        before: Option<&EntryStateRef<'_>>,
        after: Option<&EntryStateRef<'_>>,
    ) {
        let key = count_key(storage.partition_id(), id);
        match (before.is_some(), after.is_some()) {
            (false, true) => storage.raw_merge_cf(KeyKind::Stats, key, Gauge::add(1)),
            (true, false) => storage.raw_merge_cf(KeyKind::Stats, key, Gauge::sub(1)),
            _ => {}
        }
    }

    static COUNT: IndexDefinition = IndexDefinition {
        name: "count/1",
        kind: IndexKind::Aggregate,
        apply: apply_count,
        can_fill: true,
        introduced: crate::registry::TEST_INTRODUCED,
    };
    static ADOPTED: IndexDefinition = IndexDefinition {
        name: "adopted/1",
        kind: IndexKind::Aggregate,
        apply: apply_count,
        can_fill: true,
        introduced: crate::registry::TEST_INTRODUCED,
    };
    static RESUMED: IndexDefinition = IndexDefinition {
        name: "resumed/1",
        kind: IndexKind::Aggregate,
        apply: apply_count,
        can_fill: true,
        introduced: crate::registry::TEST_INTRODUCED,
    };
    const KNOWN: &[&IndexDefinition] = &[&COUNT, &ADOPTED, &RESUMED];
    /// Counts like COUNT, but can't fill: its walks only find its gaps.
    static PASSIVE: IndexDefinition = IndexDefinition {
        name: "passive/1",
        kind: IndexKind::Aggregate,
        apply: apply_count,
        can_fill: false,
        introduced: crate::registry::TEST_INTRODUCED,
    };
    /// PASSIVE in a release that added its fill.
    static PASSIVE_FILLING: IndexDefinition = IndexDefinition {
        name: "passive/1",
        kind: IndexKind::Aggregate,
        apply: apply_count,
        can_fill: true,
        introduced: crate::registry::TEST_INTRODUCED,
    };

    /// Reconciles as an open does: walks start but do not run.
    async fn reconcile(store: &mut PartitionStore, requests: Requests) {
        try_reconcile(store, KNOWN, requests).await.unwrap();
    }

    async fn try_reconcile(
        store: &mut PartitionStore,
        known: &[&'static IndexDefinition],
        requests: Requests,
    ) -> Result<(), MigrationError> {
        let db = store.partition_db().clone();
        let mut features = LoadedStorageFeatures::load(
            get_storage_features_from_partition_db(&db).unwrap(),
            get_storage_version_from_partition_db(&db).unwrap(),
            SemanticRestateVersion::current(),
        )
        .unwrap();
        let mut min = get_min_restate_version_from_partition_db(&db).unwrap();
        store
            .reconcile_registry(
                known,
                &requests,
                FenceState {
                    features: &mut features,
                    current_version: SemanticRestateVersion::current(),
                    min_restate_version: &mut min,
                },
            )
            .await
    }

    /// Runs every started walk to its end in slices of three entries, as the
    /// background driver does in time.
    async fn run_walks(store: &mut PartitionStore) {
        engine::run_all(store, 3).await.unwrap();
    }

    /// Runs one slice of `walk` over its next `keys` entries.
    async fn fill(store: &mut PartitionStore, walk: u64, keys: usize) {
        let db = store.partition_db().clone();
        let mut tx = store.transaction();
        assert_eq!(
            engine::run_slice(&mut tx, &db, walk, keys).await.unwrap(),
            Slice::Progressed
        );
    }

    fn maintain(definition: &'static IndexDefinition) -> Requests {
        Requests {
            maintain: vec![definition],
            ..Default::default()
        }
    }

    fn maintain_all(definitions: &[&'static IndexDefinition]) -> Requests {
        Requests {
            maintain: definitions.to_vec(),
            ..Default::default()
        }
    }

    fn count(store: &PartitionStore, id: u32) -> u64 {
        let db = store.partition_db();
        db.rocksdb()
            .inner()
            .as_raw_db()
            .get_cf(db.cf_handle(), count_key(db.partition().id(), id))
            .unwrap()
            .map_or(0, |value| {
                <u64 as StatValueCodec>::deserialize_from(&value).unwrap()
            })
    }

    fn registry(store: &PartitionStore) -> Registry {
        get_registry(store.partition_db()).unwrap().unwrap()
    }

    fn walks(store: &PartitionStore) -> Walks {
        records::load(store.partition_db()).unwrap().0.walks
    }

    /// The unfinished jobs of `store`, as an open loads them.
    fn unfinished(store: &PartitionStore) -> MigrationJobs {
        records::load(store.partition_db()).unwrap().0.jobs
    }

    fn registry_after(store: &PartitionStore, name: &str) -> super::super::Index {
        registry(store)
            .indexes()
            .iter()
            .find(|index| index.definition == name)
            .unwrap()
            .clone()
    }

    /// The unfinished build job of index `id`, as committed.
    fn committed_job(store: &PartitionStore, id: u32) -> MigrationJob {
        let job = get_job(store.partition_db(), id).unwrap();
        assert!(!job.status.is_finished(), "{job:?}");
        job
    }

    /// The finished build job of index `id`, as committed, with its expiry.
    fn finished_job(store: &PartitionStore, id: u32) -> MigrationJob {
        let job = get_job(store.partition_db(), id).unwrap();
        assert!(
            job.status.is_finished() && job.expires_at.is_some(),
            "{job:?}"
        );
        job
    }

    /// The unfinished build job of index `id`, as published to `tx`.
    fn published_job(tx: &PartitionStoreTransaction<'_>, id: u32) -> MigrationJob {
        tx.published_registry()
            .jobs()
            .get(MigrationJobId::for_entry(id))
            .unwrap()
            .clone()
    }

    /// Writes `registry` and `walks` as a binary would have committed them.
    fn write_state(store: &PartitionStore, registry: &Registry, walks: &Walks) {
        testing::overwrite(store.partition_db(), registry, walks);
    }

    fn add_counts(store: &PartitionStore, id: u32, delta: u32) {
        let db = store.partition_db();
        db.rocksdb()
            .inner()
            .as_raw_db()
            .merge_cf(
                db.cf_handle(),
                count_key(db.partition().id(), id),
                Gauge::add(delta),
            )
            .unwrap();
    }

    /// Why `definition` is not Ready in a new snapshot of `store`.
    fn reason(store: &PartitionStore, definition: &IndexDefinition) -> String {
        store
            .query_read(CancellationToken::new())
            .unwrap()
            .0
            .ready_index(definition.name, definition.kind)
            .unwrap_err()
            .to_string()
    }

    /// Statistics here ignore the target, so updates can use any.
    const TARGET: EntryTargetRef<'static> = EntryTargetRef::Service {
        scope: None,
        service: "svc",
        handler: "run",
    };

    async fn update(store: &mut PartitionStore, entry: &restate_types::identifiers::BaseEntryId) {
        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(entry).await.unwrap().unwrap();
        tx.update_vqueue_entry_status(
            &EntryContext {
                qid: header.vqueue_id(),
                target: &TARGET,
            },
            StoredEntryRef::from_header(&header),
            EntryStateRef::from_header(&header),
        );
        tx.commit().await.unwrap();
    }

    async fn delete(store: &mut PartitionStore, entry: &restate_types::identifiers::BaseEntryId) {
        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(entry).await.unwrap().unwrap();
        tx.delete_vqueue_entry_status(
            &EntryContext {
                qid: header.vqueue_id(),
                target: &TARGET,
            },
            StoredEntryRef::from_header(&header),
        );
        tx.commit().await.unwrap();
    }

    #[restate_core::test]
    async fn walks_reflect_every_entry_exactly_once() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;

        // Registering starts a walk over the entries; running it in slices of
        // three makes the index Ready.
        reconcile(&mut store, maintain(&COUNT)).await;
        let index = registry(&store).maintained("count/1").unwrap().clone();
        assert_eq!(
            (index.state, index.walk),
            (IndexStatus::Backfilling, Some(1))
        );
        assert!(reason(&store, &COUNT).contains("backfill walk 1 has examined 0 entries"));
        run_walks(&mut store).await;
        let index = registry(&store).maintained("count/1").unwrap().clone();
        assert_eq!((index.state, index.walk), (IndexStatus::Ready, None));
        assert_eq!(count(&store, index.id), SEEDED, "every_entry_once");
        assert!(
            store.published_registry().touched_by_on_write().is_empty(),
            "ready_indexes_are_not_listed"
        );
        assert!(walks(&store).entries().is_empty());

        // Entries adopted while Backfilling are skipped by the walk.
        reconcile(&mut store, maintain(&ADOPTED)).await;
        let adopted = registry(&store).maintained("adopted/1").unwrap().id;
        for (_, entry, _) in &read_chunk(&store, None, 2).unwrap() {
            update(&mut store, entry).await;
        }
        assert_eq!(count(&store, adopted), 2, "writes_adopt");
        run_walks(&mut store).await;
        assert_eq!(
            count(&store, adopted),
            SEEDED,
            "adopted_entries_are_skipped"
        );

        // A walk resumes after its committed cursor, here as if a first slice of
        // ten had committed its counts and position.
        reconcile(&mut store, maintain(&RESUMED)).await;
        let resumed = registry(&store).maintained("resumed/1").unwrap().clone();
        let done = read_chunk(&store, None, 10).unwrap();
        let mut walks = self::walks(&store);
        walks
            .advance(
                resumed.walk.unwrap(),
                Bytes::from(done.last().unwrap().0.clone()),
            )
            .unwrap();
        write_state(&store, &registry(&store), &walks);
        add_counts(&store, resumed.id, done.len() as u32);
        // Reopening installs the committed records.
        reconcile(&mut store, Requests::default()).await;
        run_walks(&mut store).await;
        let index = registry_after(&store, "resumed/1");
        assert_eq!(
            (index.state, count(&store, resumed.id)),
            (IndexStatus::Ready, SEEDED),
            "resumed_walks_finish_without_double_counting"
        );
    }

    /// Registers [`KNOWN`] over the seed and adopts the first two entries by
    /// writes, then moves RESUMED to a walk of its own that committed a first
    /// slice of ten entries. Returns the IDs of COUNT, ADOPTED and RESUMED.
    async fn started(store: &mut PartitionStore) -> [u32; 3] {
        seed(store).await;
        reconcile(store, maintain_all(KNOWN)).await;
        let mut persisted = registry(store);
        let ids = [&COUNT, &ADOPTED, &RESUMED]
            .map(|definition| persisted.maintained(definition.name).unwrap().id);
        let entries = read_chunk(store, None, 10).unwrap();
        for (_, entry, _) in &entries[..2] {
            update(store, entry).await;
        }
        let mut walks = self::walks(store);
        let shared = persisted.get(ids[2]).unwrap().walk.unwrap();
        let end = walks.get(shared).unwrap().end.clone();
        let own = walks
            .start(
                &mut persisted,
                WalkKind::index_backfill(),
                Source::EntryStatus,
                end,
            )
            .unwrap();
        walks
            .advance(own, Bytes::from(entries.last().unwrap().0.clone()))
            .unwrap();
        persisted.link(ids[2], own).unwrap();
        write_state(store, &persisted, &walks);
        // The first slice counted its ten entries but the two adopted ones.
        add_counts(store, ids[2], 8);
        // Reopening installs the written records.
        reconcile(store, Requests::default()).await;
        ids
    }

    #[restate_core::test]
    async fn indexes_of_one_walk_share_its_traversal() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut shared = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let mut single = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();

        // COUNT and ADOPTED share a walk, RESUMED continues on its own.
        let ids = started(&mut shared).await;
        assert_eq!(walks(&shared).entries().len(), 2);
        run_walks(&mut shared).await;
        // The same indexes, each registered at its own open, walk on their own.
        seed(&mut single).await;
        for definition in [&COUNT, &ADOPTED, &RESUMED] {
            reconcile(&mut single, maintain(definition)).await;
        }
        let single_ids = [&COUNT, &ADOPTED, &RESUMED]
            .map(|definition| registry(&single).maintained(definition.name).unwrap().id);
        assert_eq!(walks(&single).entries().len(), 3);
        run_walks(&mut single).await;

        for (store, ids) in [(&shared, ids), (&single, single_ids)] {
            for (definition, id) in [&COUNT, &ADOPTED, &RESUMED].into_iter().zip(ids) {
                let index = registry_after(store, definition.name);
                assert_eq!(
                    (index.state, index.walk, count(store, id)),
                    (IndexStatus::Ready, None, SEEDED),
                    "{} reflects every entry once",
                    definition.name
                );
            }
            assert!(walks(store).entries().is_empty());
        }
    }

    #[restate_core::test]
    async fn writes_replace_what_a_walk_filled_and_adopt_the_rest() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain(&COUNT)).await;
        let index = registry(&store).maintained(COUNT.name).unwrap().clone();
        // A slice fills the first ten entries: it counts them and lists COUNT on
        // them, in the same commit as its cursor.
        fill(&mut store, index.walk.unwrap(), 10).await;
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let listed = |(_, _, header): &engine::SourceEntry| header.touched_by().contains(index.id);
        assert!(entries[..10].iter().all(listed) && !entries[10..].iter().any(listed));
        assert_eq!(count(&store, index.id), 10);

        // Filled and listed: a delete removes its count, an update keeps it.
        delete(&mut store, &entries[4].1).await;
        update(&mut store, &entries[5].1).await;
        assert_eq!(count(&store, index.id), 9);
        // Not filled yet, and unlisted: a delete has nothing to remove, and an
        // update adopts the entry, so it is counted and listed.
        delete(&mut store, &entries[50].1).await;
        update(&mut store, &entries[60].1).await;
        assert_eq!(count(&store, index.id), 10);

        // The walk skips the adopted entry and fills the others: each entry is
        // counted exactly once.
        run_walks(&mut store).await;
        assert_eq!(count(&store, index.id), SEEDED - 2);
        assert_eq!(
            registry(&store).get(index.id).unwrap().state,
            IndexStatus::Ready
        );
    }

    /// Flushes `store`, so that [`testing::written`] then lists the records the
    /// next commits put.
    async fn flushed(store: &PartitionStore) {
        store.partition_db().flush_memtables(true).await.unwrap();
        assert!(testing::written(store.partition_db()).is_empty());
    }

    #[restate_core::test]
    async fn job_records_follow_each_build_and_commits_write_only_changed_records() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let version = ReString::from(SemanticRestateVersion::current().to_string());
        let day = Requests::default().retention.as_millis() as u64;

        // An empty partition has nothing to walk: the index is Ready at once, and
        // its job finishes at once, completed without a pass, expiring a
        // retention later.
        let mut empty = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();
        reconcile(&mut empty, maintain(&COUNT)).await;
        let index = registry(&empty).maintained(COUNT.name).unwrap().clone();
        assert!(unfinished(&empty).entries().is_empty());
        assert!(empty.published_registry().jobs().entries().is_empty());
        let job = finished_job(&empty, index.id);
        assert_eq!((job.status, job.passes), (MigrationJobStatus::Completed, 0));
        let registered_at = index.timeline.registered_at;
        assert!(registered_at.is_some());
        assert_eq!(
            (
                job.created_at,
                job.started_at,
                job.finished_at,
                index.timeline.ready_at
            ),
            (registered_at, registered_at, registered_at, registered_at)
        );
        assert_eq!(job.expires_at, registered_at.map(|at| at + day));
        assert_eq!(index.timeline.registered_by.as_ref(), Some(&version));

        // Three indexes share one walk. A command adopts one entry first, so the
        // walk finds it listed and doesn't fill it.
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain_all(KNOWN)).await;
        let ids = [&COUNT, &ADOPTED, &RESUMED]
            .map(|definition| registry(&store).maintained(definition.name).unwrap().id);
        let walk = registry(&store).get(ids[0]).unwrap().walk.unwrap();
        for id in ids {
            let job = committed_job(&store, id);
            assert_eq!(
                (job.status, job.walk, job.expires_at),
                (MigrationJobStatus::Pending, Some(walk), None)
            );
            assert_eq!(job.created_by.as_ref(), Some(&version));
        }
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        update(&mut store, &entries[3].1).await;
        let jobs = ids.map(|id| (RecordKind::Job, u64::from(id)));

        // An open that changes nothing writes nothing; a slice writes its walk
        // and its jobs only. Each member is charged the walk's full slice.
        let sequence = testing::sequence(store.partition_db());
        reconcile(&mut store, Requests::default()).await;
        assert_eq!(testing::sequence(store.partition_db()), sequence);
        flushed(&store).await;
        fill(&mut store, walk, 10).await;
        assert_eq!(
            testing::written(store.partition_db()),
            [(RecordKind::Walk, walk)]
                .into_iter()
                .chain(jobs)
                .collect::<Vec<_>>()
        );

        // A failed attempt counts a failure and its error, and commits no rows:
        // its record writes the jobs only.
        let invocation = entries[20].1.to_invocation_id().unwrap();
        let mut tx = store.transaction();
        let status = tx.get_invocation_status(&invocation).await.unwrap();
        tx.delete_invocation_status(&invocation).unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        flushed(&store).await;
        assert!(engine::run_all(&mut store, 32).await.is_err());
        assert_eq!(testing::written(store.partition_db()), jobs);
        for id in ids {
            let job = committed_job(&store, id);
            assert_eq!(job.status, MigrationJobStatus::Running);
            assert_eq!(
                (job.rows_examined, job.rows_changed, job.slices),
                (10, 9, 1)
            );
            assert_eq!((job.failures, job.consecutive_failures), (1, 1));
            assert!(job.last_error.is_some() && job.last_error_at.is_some());
        }

        // The finishing slice writes the registry, which stamps the indexes
        // Ready, deletes its walk, and finishes every member's job, completed
        // after one pass, with its expiry; none stays unfinished.
        let mut tx = store.transaction();
        tx.put_invocation_status(&invocation, &status).unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        flushed(&store).await;
        let db = store.partition_db().clone();
        let mut tx = store.transaction();
        assert_eq!(
            engine::run_slice(&mut tx, &db, walk, usize::MAX)
                .await
                .unwrap(),
            Slice::Exhausted
        );
        drop(tx);
        assert_eq!(
            testing::written(&db),
            [(RecordKind::Registry, 0)]
                .into_iter()
                .chain(jobs)
                .collect::<Vec<_>>()
        );
        assert!(walks(&store).entries().is_empty());
        assert!(unfinished(&store).entries().is_empty());
        for id in ids {
            let job = finished_job(&store, id);
            assert_eq!(
                (job.status, job.passes, job.walk),
                (MigrationJobStatus::Completed, 1, None)
            );
            assert_eq!((job.rows_examined, job.rows_changed), (SEEDED, SEEDED - 1));
            assert_eq!(
                (job.consecutive_failures, job.last_error.as_ref()),
                (0, None)
            );
            assert!(job.finished_at.is_some());
            assert_eq!(job.expires_at, job.finished_at.map(|at| at + day));
            assert_eq!(
                registry(&store).get(id).unwrap().timeline.ready_at,
                job.finished_at
            );
        }
        // The next open loads no finished job and writes nothing.
        let sequence = testing::sequence(&db);
        reconcile(&mut store, Requests::default()).await;
        assert_eq!(testing::sequence(&db), sequence);
        assert!(store.published_registry().jobs().entries().is_empty());
    }

    #[restate_core::test]
    async fn opening_settles_job_records_with_the_registry() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain_all(KNOWN)).await;
        let [count, adopted, reset_index] = [&COUNT, &ADOPTED, &RESUMED]
            .map(|definition| registry(&store).maintained(definition.name).unwrap().id);
        let walk = registry(&store).get(count).unwrap().walk.unwrap();

        // The walk advanced without its jobs being updated: the open
        // makes them Running with an unknown start, and a later slice doesn't
        // invent one.
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let mut advanced = walks(&store);
        advanced
            .advance(walk, Bytes::from(entries[4].0.clone()))
            .unwrap();
        write_state(&store, &registry(&store), &advanced);
        reconcile(&mut store, Requests::default()).await;
        fill(&mut store, walk, 10).await;
        let resumed = committed_job(&store, count);
        assert_eq!(
            (resumed.status, resumed.started_at, resumed.slices),
            (MigrationJobStatus::Running, None, 1)
        );

        // Job records this binary can't read are replaced or deleted on disk,
        // never fail the open.
        let db = store.partition_db().clone();
        let corrupt = |ids: &[u32]| {
            let mut batch = WriteBatch::default();
            for &id in ids {
                let key = RecordKey::job(db.partition().id(), MigrationJobId::for_entry(id));
                batch.put_cf(db.cf_handle(), key, b"not a job");
            }
            db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
        };
        // A waiting job moved onto a walk that has already advanced can't know
        // when that pass started. The start it had is from its previous pass, and
        // would stretch `eta` by the wait between passes, so it becomes unknown.
        // A job that stays on its walk keeps its start.
        let mut moved = MigrationJobRecords::new(unfinished(&store), Duration::ZERO);
        let job_id = MigrationJobId::for_entry(count);
        moved.unfinished.get_mut(job_id).unwrap().pass_started_at = Some(1);
        let mut stayed = moved.clone();
        let job = moved.unfinished.get_mut(job_id).unwrap();
        (job.status, job.walk) = (MigrationJobStatus::Waiting, None);
        for jobs in [&mut moved, &mut stayed] {
            settle_jobs(&registry(&store), &walks(&store), jobs, 100);
        }
        let settled = |jobs: &MigrationJobRecords| {
            let job = jobs.unfinished.get(job_id).unwrap();
            (job.status, job.walk, job.pass_started_at)
        };
        assert_eq!(
            settled(&moved),
            (MigrationJobStatus::Running, Some(walk), None)
        );
        assert_eq!(
            settled(&stayed),
            (MigrationJobStatus::Running, Some(walk), Some(1))
        );

        // An unreadable record of no unfinished job goes, as does one under
        // another job's key; the readable unfinished jobs stay as they were.
        let before = unfinished(&store);
        corrupt(&[1_000]);
        let mut misplaced = WriteBatch::default();
        misplaced.put_cf(
            db.cf_handle(),
            RecordKey::job(db.partition().id(), MigrationJobId::for_entry(1_001)),
            committed_job(&store, count).encode(),
        );
        db.rocksdb().inner().as_raw_db().write(&misplaced).unwrap();
        assert_eq!(records::load(&db).unwrap().1, [1_000, 1_001]);
        reconcile(&mut store, Requests::default()).await;
        let (after, unreadable) = records::load(&db).unwrap();
        assert!(unreadable.is_empty(), "both records were deleted");
        assert_eq!(after.jobs, before);

        // Unreadable records of unfinished jobs: the open rebuilds them from the
        // registry, with unknown times, and Running once their walk has started.
        // RESUMED is reset in that same open, and its cancelled build is still
        // recorded.
        corrupt(&[count, adopted, reset_index]);
        reconcile(
            &mut store,
            Requests {
                reset: vec![&RESUMED],
                ..Requests::default()
            },
        )
        .await;
        let reset = finished_job(&store, reset_index);
        assert_eq!(
            (reset.status, reset.created_at, reset.finished_at.is_some()),
            (MigrationJobStatus::Cancelled, None, true)
        );
        assert!(
            records::load(&db).unwrap().1.is_empty(),
            "every record was rewritten"
        );
        let rebuilt = committed_job(&store, count);
        assert_eq!(
            (
                rebuilt.status,
                rebuilt.walk,
                rebuilt.created_at,
                rebuilt.rows_examined
            ),
            (MigrationJobStatus::Running, Some(walk), None, 0)
        );

        // COUNT's build completed and ADOPTED was dropped without their jobs
        // being updated: the open finishes both, at an unknown time, writing
        // their two records and nothing else. They expire a retention after the
        // open, here at once: a retention of 0 keeps no finished job.
        let mut registry_now = registry(&store);
        let mut walks_now = walks(&store);
        registry_now.set_ready(count).unwrap();
        registry_now.drop_index(adopted).unwrap();
        walks_now.remove(walk).unwrap();
        write_state(&store, &registry_now, &walks_now);
        let (sequence, opened) = (testing::sequence(&db), WallClock::now_ms().as_u64());
        reconcile(
            &mut store,
            Requests {
                retention: Duration::ZERO,
                ..Requests::default()
            },
        )
        .await;
        assert_eq!(testing::sequence(&db) - sequence, 2, "two job records");
        assert!(unfinished(&store).entries().is_empty());
        let now = WallClock::now_ms().as_u64();
        for (id, status) in [
            (count, MigrationJobStatus::Completed),
            (adopted, MigrationJobStatus::Cancelled),
        ] {
            let job = finished_job(&store, id);
            assert_eq!((job.status, job.finished_at), (status, None));
            assert!(job.expires_at.is_some_and(|at| at >= opened) && job.is_expired(now));
        }
    }

    #[restate_core::test]
    async fn passive_members_settle_on_their_own_and_wait_for_writes() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        let passive_known: &[&'static IndexDefinition] = &[&COUNT, &PASSIVE];
        try_reconcile(&mut store, passive_known, maintain_all(passive_known))
            .await
            .unwrap();
        let [active, passive] = [&COUNT, &PASSIVE]
            .map(|definition| registry(&store).maintained(definition.name).unwrap().id);
        let index = |store: &PartitionStore, id| registry(store).get(id).unwrap().clone();
        let walk = index(&store, active).walk.unwrap();
        assert_eq!(index(&store, passive).walk, Some(walk), "one walk");

        // A slice fills ten entries for COUNT and counts them as gaps for
        // PASSIVE; reopening keeps that evidence with the walk.
        fill(&mut store, walk, 10).await;
        try_reconcile(&mut store, passive_known, Requests::default())
            .await
            .unwrap();
        assert_eq!(walks(&store).get(walk).unwrap().uncovered(passive), 10);

        // A release adds PASSIVE's fill mid-pass: the rest of the pass fills it,
        // but its gap stays for this pass. At the end each member settles on its
        // own: COUNT is Ready, PASSIVE waits, maintained and unlinked.
        let filling: &[&'static IndexDefinition] = &[&COUNT, &PASSIVE_FILLING];
        try_reconcile(&mut store, filling, Requests::default())
            .await
            .unwrap();
        run_walks(&mut store).await;
        assert_eq!(index(&store, active).state, IndexStatus::Ready);
        let unlinked = index(&store, passive);
        assert_eq!(
            (unlinked.state, unlinked.walk),
            (IndexStatus::Backfilling, None)
        );
        let waiting = committed_job(&store, passive);
        assert_eq!(
            (
                waiting.status,
                waiting.passes,
                waiting.rows_uncovered,
                waiting.rows_uncovered_previous
            ),
            (MigrationJobStatus::Waiting, 1, Some(10), None)
        );
        assert!(waiting.next_eligible_at.is_some());
        assert_eq!(self::count(&store, passive), SEEDED - 10);
        // Queries learn why it isn't Ready: it waits, not "not started".
        assert!(
            reason(&store, &PASSIVE_FILLING).contains(
                "it waits for its next pass; the last full pass found 10 entries it couldn't cover"
            ),
            "{}",
            reason(&store, &PASSIVE_FILLING)
        );

        // Opening with the fill makes it eligible at once, whatever its deadline.
        // A full pass fills the ten entries left and certifies it, recording its
        // zero and keeping what the job recorded before.
        try_reconcile(&mut store, filling, Requests::default())
            .await
            .unwrap();
        assert!(index(&store, passive).walk.is_some());
        run_walks(&mut store).await;
        let done = finished_job(&store, passive);
        assert_eq!(
            (
                done.status,
                done.passes,
                done.rows_uncovered,
                done.rows_uncovered_previous,
                done.next_eligible_at
            ),
            (MigrationJobStatus::Completed, 2, Some(0), Some(10), None)
        );
        assert_eq!(
            (done.created_at, done.started_at, done.rows_examined),
            (waiting.created_at, waiting.started_at, 2 * SEEDED)
        );
        assert!(done.pass_started_at >= waiting.last_pass_finished_at);
        assert_eq!(self::count(&store, passive), SEEDED, "every entry once");

        // Alone and passive for good: an open before its deadline starts nothing.
        // Writes adopt every entry but one, which is deleted; once eligible, a
        // pass finds no gap.
        let mut quiet = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut quiet).await;
        let only: &[&'static IndexDefinition] = &[&PASSIVE];
        try_reconcile(&mut quiet, only, maintain_all(only))
            .await
            .unwrap();
        let id = registry(&quiet).maintained(PASSIVE.name).unwrap().id;
        run_walks(&mut quiet).await;
        assert_eq!(committed_job(&quiet, id).rows_uncovered, Some(SEEDED));
        try_reconcile(&mut quiet, only, Requests::default())
            .await
            .unwrap();
        assert!(walks(&quiet).entries().is_empty(), "not eligible yet");
        let entries = read_chunk(&quiet, None, usize::MAX).unwrap();
        for (_, entry, _) in &entries[1..] {
            update(&mut quiet, entry).await;
        }
        delete(&mut quiet, &entries[0].1).await;
        let mut eligible = committed_job(&quiet, id);
        eligible.next_eligible_at = Some(0);
        testing::put_jobs(quiet.partition_db(), [&eligible]);
        try_reconcile(&mut quiet, only, Requests::default())
            .await
            .unwrap();
        run_walks(&mut quiet).await;
        let done = finished_job(&quiet, id);
        assert_eq!(
            (
                done.status,
                done.passes,
                done.rows_uncovered,
                done.rows_uncovered_previous
            ),
            (MigrationJobStatus::Completed, 2, Some(0), Some(SEEDED))
        );
        assert_eq!(self::count(&quiet, id), SEEDED - 1);
    }

    #[restate_core::test]
    async fn late_joiners_wait_for_a_full_pass() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        let known: &[&'static IndexDefinition] = &[&COUNT, &ADOPTED, &PASSIVE];
        try_reconcile(&mut store, known, maintain(&COUNT))
            .await
            .unwrap();
        let active = registry(&store).maintained(COUNT.name).unwrap().id;
        let walk = registry(&store).get(active).unwrap().walk.unwrap();
        fill(&mut store, walk, 10).await;

        // ADOPTED, which can fill, and PASSIVE, which can't, join the walk after
        // its tenth entry, as a later release may: each with its registry link
        // and its evidence, in one batch.
        let (mut joined, mut walks_now) = (registry(&store), walks(&store));
        let [adopted, passive] = [&ADOPTED, &PASSIVE].map(|definition| {
            let id = joined.register(definition.name, definition.kind).unwrap();
            joined.link(id, walk).unwrap();
            walks_now.join(walk, id).unwrap();
            id
        });
        write_state(&store, &joined, &walks_now);
        try_reconcile(&mut store, known, Requests::default())
            .await
            .unwrap();
        run_walks(&mut store).await;

        // Both saw only a suffix, with or without gaps: no full pass, nothing
        // observed, eligible again at once. COUNT, there from the start, is Ready.
        assert_eq!(
            registry(&store).get(active).unwrap().state,
            IndexStatus::Ready
        );
        for id in [adopted, passive] {
            let job = committed_job(&store, id);
            assert_eq!(
                (
                    job.status,
                    job.passes,
                    job.rows_uncovered,
                    job.next_eligible_at
                ),
                (MigrationJobStatus::Waiting, 0, None, None),
                "{id}"
            );
        }
        assert_eq!(self::count(&store, adopted), SEEDED - 10, "the suffix");

        // The next open starts a full pass for both: ADOPTED completes, PASSIVE
        // records its first full pass's gaps.
        try_reconcile(&mut store, known, Requests::default())
            .await
            .unwrap();
        run_walks(&mut store).await;
        let done = finished_job(&store, adopted);
        assert_eq!(
            (done.status, done.passes, done.rows_uncovered),
            (MigrationJobStatus::Completed, 1, Some(0))
        );
        assert_eq!(self::count(&store, adopted), SEEDED);
        let gaps = committed_job(&store, passive);
        assert_eq!(
            (gaps.status, gaps.passes, gaps.rows_uncovered),
            (MigrationJobStatus::Waiting, 1, Some(SEEDED))
        );
    }

    #[restate_core::test]
    async fn started_walks_wait_for_the_driver_with_their_positions() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();

        // A partition without entries has nothing to walk: its index is Ready
        // at once, and no walk starts.
        let mut empty = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();
        reconcile(&mut empty, maintain(&COUNT)).await;
        let index = registry(&empty).maintained(COUNT.name).unwrap().clone();
        assert_eq!((index.state, index.walk), (IndexStatus::Ready, None));
        assert!(walks(&empty).entries().is_empty());

        // COUNT's walk filled ten entries and RESUMED's twenty, each started at
        // its own open, when the partition reopens with ADOPTED requested too.
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        let mut paused = Vec::new();
        for (definition, filled) in [(&COUNT, 10), (&RESUMED, 20)] {
            reconcile(&mut store, maintain(definition)).await;
            let index = registry(&store)
                .maintained(definition.name)
                .unwrap()
                .clone();
            fill(&mut store, index.walk.unwrap(), filled).await;
            paused.push((definition, index.id, index.walk.unwrap(), filled));
        }
        let walks = self::walks(&store);
        reconcile(&mut store, maintain_all(&[&COUNT, &RESUMED, &ADOPTED])).await;
        let adopted = registry(&store).maintained(ADOPTED.name).unwrap().clone();
        assert_eq!(
            (adopted.state, adopted.walk),
            (IndexStatus::Backfilling, Some(3)),
            "a_requested_index_starts_its_own_walk"
        );
        for (definition, _, walk, filled) in &paused {
            assert!(reason(&store, definition).contains(&format!(
                "is still being completed: backfill walk {walk} has examined {filled} entries"
            )));
        }
        assert!(reason(&store, &ADOPTED).contains("backfill walk 3 has examined 0 entries"));
        let started = self::walks(&store);
        assert_eq!(
            &started.entries()[..2],
            walks.entries(),
            "started_walks_wait"
        );

        // Filling keeps the tokens an entry already lists: the fifth entry lists
        // both walks' indexes, the fifteenth RESUMED's only. Deleting them removes
        // exactly the counts they contributed.
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let tokens = |entry: usize| {
            let listed = entries[entry].2.touched_by();
            paused
                .iter()
                .map(|(_, id, ..)| listed.contains(*id))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            (tokens(4), tokens(14)),
            (vec![true, true], vec![false, true])
        );
        for index in [4, 14] {
            delete(&mut store, &entries[index].1).await;
        }
        assert_eq!(
            paused
                .iter()
                .map(|(_, id, ..)| count(&store, *id))
                .collect::<Vec<_>>(),
            [9, 18]
        );

        // The driver finishes the walks in the order they started.
        run_walks(&mut store).await;
        for definition in [&COUNT, &RESUMED, &ADOPTED] {
            let index = registry_after(&store, definition.name);
            assert_eq!(index.state, IndexStatus::Ready);
            assert_eq!(count(&store, index.id), SEEDED - 2, "{}", definition.name);
        }
        assert!(self::walks(&store).entries().is_empty());
    }

    #[restate_core::test]
    async fn failed_slices_commit_nothing_and_are_recorded() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain(&COUNT)).await;
        let id = registry(&store).maintained(COUNT.name).unwrap().id;
        let entries = read_chunk(&store, None, usize::MAX).unwrap();

        // The target of the fifth entry cannot resolve, so the second slice of
        // three fails after staging the fourth entry's contribution.
        let invocation = entries[4].1.to_invocation_id().unwrap();
        let mut tx = store.transaction();
        let status = tx.get_invocation_status(&invocation).await.unwrap();
        tx.delete_invocation_status(&invocation).unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        let error = engine::run_all(&mut store, 3).await.unwrap_err();
        let StorageError::Generic(cause) = error else {
            panic!("unexpected failure: {error}");
        };
        assert!(format!("{cause:#}").contains("missing invocation status"));
        assert_eq!(count(&store, id), 3, "only_the_first_slice_committed");
        let walks = self::walks(&store);
        let [walk] = walks.entries() else {
            panic!("one walk: {walks:?}");
        };
        assert_eq!(walk.cursor.as_deref(), Some(entries[2].0.as_slice()));
        let job = committed_job(&store, id);
        assert_eq!((job.rows_examined, job.slices, job.failures), (3, 1, 1));
        assert!(
            job.last_error
                .as_deref()
                .is_some_and(|error| error.contains("missing invocation status"))
        );
        let reason = reason(&store, &COUNT);
        assert!(
            reason.contains(&format!(
                "backfill walk {} has examined 3 entries; failed attempts: 1, the last with: ",
                walk.id
            )) && reason.contains("missing invocation status"),
            "{reason}"
        );

        // Once the target resolves again, the walk resumes after its cursor.
        let mut tx = store.transaction();
        tx.put_invocation_status(&invocation, &status).unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        run_walks(&mut store).await;
        assert_eq!(count(&store, id), SEEDED);
        assert_eq!(registry(&store).get(id).unwrap().state, IndexStatus::Ready);
        assert!(self::walks(&store).entries().is_empty());
    }

    /// The rows of every index, by ID, without the replica-local key prefix. A
    /// statistic row whose counts all returned to zero is left out, like one never
    /// written.
    fn index_rows(store: &PartitionStore) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
        let partition = store.partition_id();
        let db = store.partition_db().rocksdb().inner().as_raw_db();
        let cf = db.cf_handle(store.partition().cf_name().as_ref()).unwrap();
        let mut rows = Vec::new();
        for index in registry(store).indexes() {
            let prefix = match index.kind {
                IndexKind::Secondary => {
                    IndexKeyPrefix::new(partition, index.id).as_bytes().to_vec()
                }
                IndexKind::Aggregate => AggregateKeyPrefix::new(partition, index.id)
                    .as_bytes()
                    .to_vec(),
                IndexKind::Unknown => unreachable!(),
            };
            let mut options = rocksdb::ReadOptions::default();
            options.set_total_order_seek(true);
            let mut iterator = db.raw_iterator_cf_opt(&cf, options);
            iterator.seek(&prefix);
            while let Some(key) = iterator.key().filter(|key| key.starts_with(&prefix)) {
                let value = iterator.value().unwrap().to_vec();
                if index.kind == IndexKind::Secondary || !value.is_empty() {
                    rows.push((index.id, key[prefix.len()..].to_vec(), value));
                }
                iterator.next();
            }
            iterator.status().unwrap();
        }
        rows
    }

    #[restate_core::test]
    async fn byte_limits_end_slices_and_walks_resume_to_parity() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let mut reference = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();
        // Every released index and statistic, and a passive one: one walk, every
        // member.
        let all: Vec<&'static IndexDefinition> = crate::registry::INDEX_DEFINITIONS
            .iter()
            .copied()
            .chain([&PASSIVE])
            .collect();
        let all = all.as_slice();
        seed(&mut store).await;
        seed(&mut reference).await;
        try_reconcile(&mut reference, all, maintain_all(all))
            .await
            .unwrap();
        run_walks(&mut reference).await;
        // Earlier builds left finished jobs that failed with long errors, larger
        // than 2 KiB per member. They stay on disk: no slice counts them against
        // its write budget, nor writes them.
        let mut earlier =
            MigrationJobRecords::new(MigrationJobs::default(), Duration::from_secs(3600));
        for id in 1_000..1_030 {
            let mut job =
                MigrationJob::index_backfill(id, ReString::from_static("old/1"), None, None, None);
            job.record_failure(0, &"x".repeat(crate::job::MAX_ERROR_LEN), 0);
            earlier.unfinished.insert(job);
            let now = WallClock::now_ms().as_u64();
            earlier.finish(
                MigrationJobId::for_entry(id),
                MigrationJobStatus::Cancelled,
                None,
                now,
            );
        }
        let earlier_bytes: usize = earlier.finished.iter().map(|job| job.encode().len()).sum();
        assert!(earlier_bytes > all.len() * (2 << 10), "{earlier_bytes}");
        testing::put_jobs(store.partition_db(), &earlier.finished);
        try_reconcile(&mut store, all, maintain_all(all))
            .await
            .unwrap();
        let registered = registry(&store);
        assert_eq!(registered.indexes().len(), all.len());
        let walk = registered.indexes()[0].walk.unwrap();
        assert!(
            registered
                .indexes()
                .iter()
                .all(|index| index.walk == Some(walk))
        );
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let db = store.partition_db().clone();
        let mut tx = store.transaction();
        // Every member is charged the walk's rows, so any member's job tells.
        let member = registered.indexes()[0].id;
        let passive = registered.maintained(PASSIVE.name).unwrap().id;
        let progress = |tx: &PartitionStoreTransaction<'_>| {
            let walk = tx.published_registry().walks().get(walk).unwrap().clone();
            let rows = published_job(tx, member).rows_examined;
            // PASSIVE's gaps count exactly the entries slices took: a candidate
            // rolled back at the byte limit leaves them unchanged too.
            assert_eq!(walk.uncovered(passive), rows);
            (walk.cursor.unwrap(), rows as usize)
        };
        let unbounded = SliceBudget {
            keys: usize::MAX,
            read_bytes: usize::MAX,
            write_bytes: usize::MAX,
        };

        // Without a byte allowance, each entry is oversized: a slice takes its
        // first entry alone, so the walk still progresses, and stages nothing for
        // the next one, whose target does not resolve for now. Having read up to
        // the walk's end does not finish it while work is left.
        let invocation = entries[2].1.to_invocation_id().unwrap();
        let status = tx.get_invocation_status(&invocation).await.unwrap();
        tx.delete_invocation_status(&invocation).unwrap();
        tx.commit().await.unwrap();
        let oversized = |limit: fn(&mut SliceBudget)| {
            let mut budget = unbounded;
            limit(&mut budget);
            budget
        };
        let no_read = oversized(|budget| budget.read_bytes = 0);
        let no_write = oversized(|budget| budget.write_bytes = 0);
        for (i, budget) in [no_read, no_write].into_iter().enumerate() {
            assert_eq!(
                engine::run_slice(&mut tx, &db, walk, budget).await.unwrap(),
                Slice::Progressed
            );
            assert_eq!(progress(&tx), (Bytes::from(entries[i].0.clone()), i + 1));
        }
        // The next slice fails on that entry alone, keeping the progress.
        assert!(matches!(
            engine::run_slice(&mut tx, &db, walk, no_write).await,
            Err(engine::SliceError::Staging(_))
        ));
        assert_eq!(progress(&tx), (Bytes::from(entries[1].0.clone()), 2));
        tx.put_invocation_status(&invocation, &status).unwrap();
        tx.commit().await.unwrap();

        // An allowance for the records the slice commits with, the registry,
        // its walk and its jobs, and no more, still takes the first entry, and
        // nothing more: those records count against its budget. They are much
        // smaller than the finished jobs on disk, which don't count.
        let records = {
            let active = tx.published_registry();
            let members: Vec<_> = active
                .registry()
                .members(walk)
                .map(|index| index.id)
                .collect();
            engine::records_estimate(&active, active.walks().get(walk).unwrap(), &members)
        };
        assert!(records < earlier_bytes, "{records}");
        let tight = SliceBudget {
            keys: 10,
            write_bytes: records,
            ..unbounded
        };
        assert_eq!(
            engine::run_slice(&mut tx, &db, walk, tight).await.unwrap(),
            Slice::Progressed
        );
        assert_eq!(progress(&tx), (Bytes::from(entries[2].0.clone()), 3));

        // An allowance on top of the records (scaled to the members, each of
        // which stages about one record per entry), smaller than the finished
        // jobs on disk, ends each slice before its key limit, here all but the
        // last entry, and the cursor covers exactly the work committed.
        let mut taken = Vec::new();
        let mut scanned = 3;
        while scanned < entries.len() - 1 {
            let budget = SliceBudget {
                keys: entries.len() - 1 - scanned,
                write_bytes: records + all.len() * (2 << 10),
                ..unbounded
            };
            assert_eq!(
                engine::run_slice(&mut tx, &db, walk, budget).await.unwrap(),
                Slice::Progressed
            );
            let (cursor, now) = progress(&tx);
            assert_eq!(cursor, entries[now - 1].0);
            if now - scanned < budget.keys {
                taken.push(now - scanned);
            }
            scanned = now;
        }
        assert!(
            taken.len() > 1 && taken.iter().all(|taken| *taken > 1),
            "several byte-ended, multi-entry slices: {taken:?}"
        );
        // An oversized last entry still finishes the walk.
        assert_eq!(
            engine::run_slice(&mut tx, &db, walk, no_write)
                .await
                .unwrap(),
            Slice::Exhausted
        );
        drop(tx);

        assert!(self::walks(&store).entries().is_empty());
        assert!(
            registry(&store)
                .indexes()
                .iter()
                .all(|index| (index.state == IndexStatus::Ready) != (index.id == passive))
        );
        assert_eq!(index_rows(&store), index_rows(&reference));
        // The finishing slice finished every active member's job and left the
        // earlier ones alone; PASSIVE waits with a gap at every entry.
        for index in registry(&store).indexes() {
            if index.id != passive {
                assert_eq!(
                    finished_job(&store, index.id).status,
                    MigrationJobStatus::Completed
                );
            }
        }
        for job in &earlier.finished {
            assert_eq!(
                get_job(store.partition_db(), job.id().as_u32()).as_ref(),
                Some(job)
            );
        }
        assert_eq!(
            committed_job(&store, passive).rows_uncovered,
            Some(entries.len() as u64)
        );
    }

    #[restate_core::test]
    async fn resetting_some_members_keeps_the_walk_for_the_others() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain_all(KNOWN)).await;
        // COUNT and ADOPTED's walk covered ten entries; RESUMED walks on its own.
        let mut persisted = registry(&store);
        let [count_id, adopted_id, resumed_id] = [&COUNT, &ADOPTED, &RESUMED]
            .map(|definition| persisted.maintained(definition.name).unwrap().id);
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let mut walks = self::walks(&store);
        let shared = persisted.get(count_id).unwrap().walk.unwrap();
        walks
            .advance(shared, Bytes::from(entries[9].0.clone()))
            .unwrap();
        let end = walks.get(shared).unwrap().end.clone();
        // ADOPTED has evidence on the shared walk, which goes with it.
        walks.add_uncovered(shared, adopted_id, 3).unwrap();
        let own = walks
            .start(
                &mut persisted,
                WalkKind::index_backfill(),
                Source::EntryStatus,
                end,
            )
            .unwrap();
        persisted.link(resumed_id, own).unwrap();
        for id in [count_id, adopted_id] {
            add_counts(&store, id, 10);
        }
        write_state(&store, &persisted, &walks);

        // Resetting ADOPTED deletes its counts; COUNT's walk keeps its cursor
        // rather than restarting.
        reconcile(
            &mut store,
            Requests {
                reset: vec![&ADOPTED],
                ..Default::default()
            },
        )
        .await;
        let after = registry(&store);
        assert_eq!(after.get(adopted_id).unwrap().state, IndexStatus::Dropped);
        assert_eq!(count(&store, adopted_id), 0);
        assert_eq!(after.get(count_id).unwrap().walk, Some(shared));
        let kept = self::walks(&store).get(shared).unwrap().clone();
        assert_eq!(kept.cursor.as_deref(), Some(entries[9].0.as_slice()));
        assert_eq!(kept.member(adopted_id), None, "the reset member's evidence");
        // The next open accepts what this one committed.
        reconcile(&mut store, Requests::default()).await;
        run_walks(&mut store).await;
        let after = registry(&store);
        assert_eq!(
            (after.get(count_id).unwrap().state, count(&store, count_id)),
            (IndexStatus::Ready, SEEDED)
        );
        assert_eq!(
            (
                after.get(resumed_id).unwrap().state,
                count(&store, resumed_id)
            ),
            (IndexStatus::Ready, SEEDED)
        );
        assert!(self::walks(&store).entries().is_empty());
    }

    #[restate_core::test]
    async fn opens_reject_inconsistent_walks_before_any_change() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        // A store with the registry's storage feature, so each case meets its own
        // check rather than the feature's.
        reconcile(&mut store, maintain(&COUNT)).await;
        let mut base = Registry::default();
        let id = base.register(COUNT.name, COUNT.kind).unwrap();
        // The counter issued walk 1, which each case starts.
        assert_eq!(base.issue_walk_id(), 1);
        let end = Bytes::from(read_chunk(&store, None, 1).unwrap()[0].0.clone());
        let started = |end: Bytes| {
            let mut walks = Walks::default();
            walks
                .start(
                    &mut Registry::default(),
                    WalkKind::index_backfill(),
                    Source::EntryStatus,
                    end,
                )
                .unwrap();
            walks
        };
        let linked = {
            let mut registry = base.clone();
            registry.link(id, 1).unwrap();
            registry
        };
        let cases: [(&str, &str, Registry, Walks); 7] = [
            (
                "dangling link",
                "walk link of",
                {
                    let mut registry = base.clone();
                    registry.link(id, 7).unwrap();
                    registry
                },
                Walks::default(),
            ),
            (
                "walk without members",
                "without members",
                base.clone(),
                started(end.clone()),
            ),
            (
                "position outside the entry-status schema",
                "position of walk 1",
                linked.clone(),
                started(Bytes::from_static(b"not a key")),
            ),
            (
                "an entry-status key cut short",
                "position of walk 1",
                linked.clone(),
                started(end.slice(..end.len() - 1)),
            ),
            (
                "evidence for an index the walk doesn't link",
                "evidence of walk 1 for index 65",
                linked.clone(),
                {
                    let mut walks = started(end.clone());
                    walks.add_uncovered(1, id + 1, 1).unwrap();
                    walks
                },
            ),
            (
                "a join position outside the entry-status schema",
                "position of walk 1",
                linked.clone(),
                {
                    let mut walks = started(end.clone());
                    walks.advance(1, end.clone()).unwrap();
                    walks.entries_mut()[0].members.push(WalkMember {
                        index: id,
                        joined_at: Some(Bytes::from_static(b"\x00")),
                        uncovered: 0,
                    });
                    walks
                },
            ),
            (
                "a walk this binary doesn't know and must not discard",
                "must not discard",
                linked,
                {
                    let mut walks = started(end.clone());
                    let walk = &mut walks.entries_mut()[0];
                    (walk.kind.version, walk.on_unknown) = (2, OnUnknown::Refuse as u32);
                    walks
                },
            ),
        ];
        for (case, expected, registry, walks) in cases {
            write_state(&store, &registry, &walks);
            assert!(
                matches!(
                    store.validate_registry(&store.storage_features()),
                    Err(MigrationError::MigrationBarrier(reason)) if reason.contains(expected)
                ),
                "{case}"
            );
        }

        // A built store meets walks of a later release. Any refusing one refuses
        // the open, which changes nothing. Without it, the discardable one, with
        // opaque params and positions, is removed: COUNT restarts under this
        // binary's kind and a definition this binary doesn't know is retired.
        let mut later = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut later).await;
        reconcile(&mut later, maintain(&COUNT)).await;
        let count = registry(&later).maintained(COUNT.name).unwrap().id;
        let walk = registry(&later).get(count).unwrap().walk.unwrap();
        fill(&mut later, walk, 10).await;
        let (mut registry_later, mut walks_later) = (registry(&later), walks(&later));
        let gone = registry_later
            .register("gone/1", IndexKind::Aggregate)
            .unwrap();
        registry_later.link(gone, walk).unwrap();
        let unknown = |walk: &mut Walk, version, on_unknown: OnUnknown| {
            walk.kind.version = version;
            walk.on_unknown = on_unknown as u32;
            walk.params = Bytes::from_static(b"opaque");
            walk.end = Bytes::from_static(b"not a key");
            walk.cursor = Some(Bytes::from_static(b"not a key"));
        };
        unknown(&mut walks_later.entries_mut()[0], 2, OnUnknown::Discard);
        let (mut refusing, mut registry_refusing) = (walks_later.clone(), registry_later.clone());
        let refused = refusing
            .start(
                &mut registry_refusing,
                WalkKind::index_backfill(),
                Source::EntryStatus,
                end.clone(),
            )
            .unwrap();
        unknown(&mut refusing.entries_mut()[1], 3, OnUnknown::Refuse);
        write_state(&later, &registry_refusing, &refusing);
        let raw = |store: &PartitionStore| testing::raw(store.partition_db(), false);
        let before = raw(&later);
        assert!(matches!(
            try_reconcile(&mut later, KNOWN, Requests::default()).await,
            Err(MigrationError::MigrationBarrier(reason))
                if reason.contains(&format!("walk {refused}"))
                    && reason.contains(&format!(
                        "minimum Restate version is {}",
                        get_min_restate_version_from_partition_db(later.partition_db()).unwrap()
                    ))
        ));
        assert_eq!(raw(&later), before, "a refused open changes nothing");

        write_state(&later, &registry_later, &walks_later);
        reconcile(&mut later, Requests::default()).await;
        let reopened = registry(&later);
        assert_eq!(reopened.get(gone).unwrap().state, IndexStatus::Dropped);
        let restarted = reopened.get(count).unwrap().walk.unwrap();
        assert!(restarted > walk && walks(&later).get(walk).is_none());
        run_walks(&mut later).await;
        assert_eq!(
            self::count(&later, count),
            SEEDED,
            "the restart skips what the discarded walk filled"
        );
    }

    /// Awaits and runs `driver`'s steps through `tx` while it has any, as the
    /// processor does between its turns, for at most `steps`. Tests driving it run
    /// on paused time, so its rests and backoffs pass without waiting.
    async fn drive(driver: &mut WalkDriver, tx: &mut PartitionStoreTransaction<'_>, steps: usize) {
        for _ in 0..steps {
            if driver.due().is_none() {
                return;
            }
            driver.ready().await;
            let write_bytes = WorkerOptions::default().max_command_batch_bytes.as_usize();
            driver.step(tx, write_bytes).await.unwrap();
        }
    }

    /// Why the job of index `id` isn't progressing at `now`, as `migration_jobs`
    /// shows it: from `tx`'s records and what the driver of `db` published.
    fn blocked_by(
        db: &PartitionDb,
        tx: &PartitionStoreTransaction<'_>,
        id: u32,
        now: Instant,
    ) -> Option<&'static str> {
        let active = tx.published_registry();
        let replica = ReplicaMigrations {
            partition_id: db.partition().id(),
            key_range: db.partition().key_range,
            registry: active.registry().clone(),
            walks: active.walks().clone(),
            jobs: active.jobs().clone(),
            index_sizes: Default::default(),
            driver: db.driver_state().get(),
        };
        let job = active.jobs().get(MigrationJobId::for_entry(id))?;
        replica.blocked_by(job, now)
    }

    /// Deletes entry `id` through `tx`, as a command does.
    async fn delete_entry(
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        id: BaseEntryId,
    ) {
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        with_entry_target(db, &id, header.vqueue_id(), header.seq(), |target| {
            tx.delete_vqueue_entry_status(
                &EntryContext {
                    qid: header.vqueue_id(),
                    target,
                },
                StoredEntryRef::from_header(&header),
            );
            Ok(())
        })
        .unwrap();
        tx.commit().await.unwrap();
    }

    /// Rewrites entry `id` unchanged through `tx`, as a command does.
    async fn rewrite_entry(
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        id: BaseEntryId,
    ) {
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        with_entry_target(db, &id, header.vqueue_id(), header.seq(), |target| {
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: header.vqueue_id(),
                    target,
                },
                StoredEntryRef::from_header(&header),
                EntryStateRef::from_header(&header),
            );
            Ok(())
        })
        .unwrap();
        tx.commit().await.unwrap();
    }

    #[restate_core::test(start_paused = true)]
    async fn the_driver_gives_walks_turns_and_starts_passes_when_due() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        // COUNT's walk starts at one open; ADOPTED's and PASSIVE's at the next.
        let known: &[&'static IndexDefinition] = &[&COUNT, &ADOPTED, &PASSIVE];
        try_reconcile(&mut store, known, maintain(&COUNT))
            .await
            .unwrap();
        try_reconcile(&mut store, known, maintain_all(known))
            .await
            .unwrap();
        let [count, adopted, passive] = [&COUNT, &ADOPTED, &PASSIVE]
            .map(|definition| registry(&store).maintained(definition.name).unwrap().id);
        let second = registry(&store).get(adopted).unwrap().walk.unwrap();
        assert!(registry(&store).get(count).unwrap().walk < Some(second));

        // The second walk passes entry 5; then the entry's target goes missing,
        // so only the first walk, which still has to fill it, fails there.
        fill(&mut store, second, 6).await;
        let invocation = entries[5].1.to_invocation_id().unwrap();
        let status = {
            let mut tx = store.transaction();
            let status = tx.get_invocation_status(&invocation).await.unwrap();
            tx.delete_invocation_status(&invocation).unwrap();
            tx.commit().await.unwrap();
            status
        };
        let mut driver = store.walk_driver(&StorageOptions::default());
        // Paused time makes every key look instant; small slices keep turns.
        driver.tuning_mut().max_keys = 16;
        let db = store.partition_db().clone();
        let mut tx = store.transaction();
        let state = |tx: &PartitionStoreTransaction<'_>, id| {
            tx.published_registry().registry().get(id).unwrap().state
        };

        // The walks take turns. The failing one waits out its backoff while the
        // other finishes: ADOPTED is Ready and PASSIVE waits with gaps, while
        // COUNT keeps failing.
        for _ in 0..1_000 {
            if state(&tx, adopted) == IndexStatus::Ready {
                break;
            }
            drive(&mut driver, &mut tx, 1).await;
        }
        assert_eq!(state(&tx, adopted), IndexStatus::Ready);
        assert!(published_job(&tx, count).failures > 0);
        assert_eq!(state(&tx, count), IndexStatus::Backfilling);
        assert_eq!(
            published_job(&tx, passive).status,
            MigrationJobStatus::Waiting
        );

        // The target comes back. Commands rewrite half the entries and delete
        // the others, covering PASSIVE. With no reopen, COUNT's walk recovers,
        // and once PASSIVE's wait is over the driver starts its next pass, which
        // finds no gap.
        tx.put_invocation_status(&invocation, &status).unwrap();
        tx.commit().await.unwrap();
        let mut kept = 0;
        for (position, (_, id, _)) in entries.iter().enumerate() {
            if position % 2 == 0 {
                rewrite_entry(&mut tx, &db, *id).await;
                kept += 1;
            } else {
                delete_entry(&mut tx, &db, *id).await;
            }
        }
        drive(&mut driver, &mut tx, 10_000).await;
        assert_eq!(driver.due(), None, "idle once everything is Ready");
        drop(tx);
        for id in [count, passive] {
            assert_eq!(
                (
                    registry(&store).get(id).unwrap().state,
                    self::count(&store, id)
                ),
                (IndexStatus::Ready, kept),
                "{id}"
            );
        }
        let done = finished_job(&store, passive);
        assert_eq!((done.passes, done.rows_uncovered), (2, Some(0)));
    }

    #[restate_core::test(start_paused = true)]
    async fn a_throttled_walk_does_not_delay_a_due_pass() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        // PASSIVE's first pass finds gaps, so it waits the shortest re-check
        // wait. COUNT's walk starts at the next open.
        let known: &[&'static IndexDefinition] = &[&COUNT, &PASSIVE];
        try_reconcile(&mut store, known, maintain(&PASSIVE))
            .await
            .unwrap();
        run_walks(&mut store).await;
        try_reconcile(&mut store, known, maintain_all(known))
            .await
            .unwrap();
        let passive = registry(&store).maintained(PASSIVE.name).unwrap().id;
        let count = registry(&store).maintained(COUNT.name).unwrap().id;
        let db = store.partition_db().clone();
        // One key an hour after a burst of 16: COUNT's walk soon waits hours.
        let mut throttled = StorageOptions::default();
        throttled.background_migrations_throttling = Some(ThrottlingOptions {
            rate: Rate::Hour(std::num::NonZeroU32::MIN),
            capacity: std::num::NonZeroU32::new(16),
        });
        let mut driver = store.walk_driver(&throttled);
        let mut tx = store.transaction();
        let deadline = Instant::now() + Duration::from_millis(crate::job::RECHECK_FLOOR_MS);

        // Waiting for tokens, the driver still wakes when PASSIVE's wait is over,
        // and starts its pass then: a start needs no tokens.
        drive(&mut driver, &mut tx, 2).await;
        assert!(
            driver.due().is_some_and(|due| due <= deadline),
            "{:?}",
            driver.due()
        );
        // COUNT's job shows that it waits for tokens until they're due, without
        // another step. PASSIVE, waiting for its pass, doesn't inherit it.
        assert_eq!(
            blocked_by(&db, &tx, count, Instant::now()),
            Some("throttled")
        );
        assert_eq!(blocked_by(&db, &tx, passive, Instant::now()), None);
        let refill = db.driver_state().get().unwrap().throttled_until.unwrap();
        assert_eq!(blocked_by(&db, &tx, count, refill), None);
        drive(&mut driver, &mut tx, 1).await;
        assert!(
            tx.published_registry()
                .registry()
                .get(passive)
                .unwrap()
                .walk
                .is_some()
        );
        assert!(Instant::now() <= deadline);
    }

    #[restate_core::test]
    async fn migration_reports_show_each_replica_through_its_build() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        let known: &[&'static IndexDefinition] = &[&COUNT, &PASSIVE];
        try_reconcile(&mut store, known, maintain_all(known))
            .await
            .unwrap();
        let [active, passive] = [&COUNT, &PASSIVE]
            .map(|definition| registry(&store).maintained(definition.name).unwrap().id);
        let walk = registry(&store).get(active).unwrap().walk.unwrap();
        let executor = QueryIoExecutor::new(std::num::NonZeroUsize::new(1).unwrap());
        let report = || async {
            manager
                .migration_report(&executor, IndexSizes::Estimate)
                .unwrap()
                .await
                .unwrap()
        };

        // Mid-build: both indexes Backfilling on the walk, their jobs running, and
        // the walk part of the way through the replica's key range.
        fill(&mut store, walk, 10).await;
        let mid = report().await;
        let [replica] = mid.replicas.as_slice() else {
            panic!("one open replica: {mid:?}");
        };
        assert_eq!(replica.partition_id, PartitionId::MIN);
        assert!(replica.registry.members(walk).count() == 2);
        let running = replica.walks.get(walk).unwrap();
        assert!(replica.progress_percent(running).is_some_and(|p| p <= 100));
        assert_eq!(replica.walks.get(walk).unwrap().uncovered(passive), 10);
        // Before the first slice, or over a single key, progress is unknown.
        let unstarted = Walk {
            cursor: None,
            ..running.clone()
        };
        assert_eq!(replica.progress_percent(&unstarted), None);
        let single = ReplicaMigrations {
            key_range: KeyRange::new(5, 5),
            ..replica.clone()
        };
        assert_eq!(single.progress_percent(running), None);

        // `eta` stretches the time a pass has run by the key range still ahead: a
        // pass 25% through after 15 minutes ends after an hour. It's unknown with
        // unknown or zero progress, or with a clock before the pass started.
        let mut cursor = running.cursor.as_deref().unwrap();
        let key = EntryStatusKey::deserialize_from(&mut cursor)
            .unwrap()
            .id
            .partition_key();
        let quarter = ReplicaMigrations {
            key_range: KeyRange::new(key - 5, key + 15),
            ..replica.clone()
        };
        let (start, minute) = (1_700_000_000_000, 60_000);
        assert_eq!(quarter.progress_percent(running), Some(25));
        assert_eq!(
            quarter.eta(running, start, start + 15 * minute),
            Some(start + 60 * minute)
        );
        assert_eq!(quarter.eta(&unstarted, start, start + 15 * minute), None);
        assert_eq!(quarter.eta(running, start, start - 1), None);
        let at_start = ReplicaMigrations {
            key_range: KeyRange::new(key, key + 20),
            ..replica.clone()
        };
        assert_eq!(at_start.eta(running, start, start + 15 * minute), None);
        // 1 of 200 keys: the percent rounds down to 0, but the estimate uses the
        // exact position.
        let early = ReplicaMigrations {
            key_range: KeyRange::new(key - 1, key + 199),
            ..replica.clone()
        };
        assert_eq!(early.progress_percent(running), Some(0));
        assert_eq!(
            early.eta(running, start, start + 15 * minute),
            Some(start + 200 * 15 * minute)
        );

        // Cursor bytes alone do not identify their source's key format.
        let unknown_source = Walk {
            source: u32::MAX,
            ..running.clone()
        };
        assert_eq!(replica.progress_percent(&unknown_source), None);

        // After the pass: COUNT's job is finished, and listed until it expires;
        // PASSIVE's waits.
        run_walks(&mut store).await;
        let after = report().await;
        let replica = &after.replicas[0];
        // Each index has RocksDB's estimate of its keys: COUNT's grows with the
        // rows it counted, and PASSIVE, which writes no keys, takes nothing.
        let (sizes_mid, sizes) = (&mid.replicas[0].index_sizes, &replica.index_sizes);
        assert!(sizes_mid[&active] > 0, "{sizes_mid:?}");
        assert!(sizes[&active] > sizes_mid[&active], "{sizes:?}");
        assert_eq!(sizes[&passive], 0);
        assert!(replica.walks.entries().is_empty());
        assert_eq!(
            replica
                .jobs
                .get(MigrationJobId::for_entry(active))
                .unwrap()
                .status,
            MigrationJobStatus::Completed
        );
        assert_eq!(
            replica
                .jobs
                .get(MigrationJobId::for_entry(passive))
                .unwrap()
                .status,
            MigrationJobStatus::Waiting
        );
        // Once its record expired, a report hides it, compacted away or not.
        let mut expired = get_job(store.partition_db(), active).unwrap();
        expired.expires_at = Some(WallClock::now_ms().as_u64());
        testing::put_jobs(store.partition_db(), [&expired]);
        let hidden = report().await;
        let ids: Vec<_> = hidden.replicas[0]
            .jobs
            .entries()
            .iter()
            .map(|job| job.id().as_u32())
            .collect();
        assert_eq!(ids, [passive]);

        // A flushed index is estimated from its table files: 1 MiB of
        // incompressible values under COUNT's prefix, once flushed, counts at
        // least half of that. A report that skips sizes estimates none.
        let db = store.partition_db().clone();
        let mut batch = WriteBatch::default();
        let mut random = 0x9E37_79B9_7F4A_7C15_u64;
        for i in 0..1024_u32 {
            let mut key = count_key(db.partition().id(), active);
            key.extend_from_slice(&i.to_be_bytes());
            let value: Vec<u8> = (0..1024)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    random as u8
                })
                .collect();
            batch.put_cf(db.cf_handle(), key, value);
        }
        db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
        db.flush_memtables(true).await.unwrap();
        let flushed = report().await;
        let sizes = &flushed.replicas[0].index_sizes;
        assert!(sizes[&active] >= 512 * 1024, "{sizes:?}");
        assert_eq!(sizes[&passive], 0);
        let skipped = manager
            .migration_report(&executor, IndexSizes::Skip)
            .unwrap()
            .await
            .unwrap();
        assert!(skipped.replicas[0].index_sizes.is_empty());

        // An unreadable record is reported with its replica, never as a replica
        // without indexes.
        let db = store.partition_db();
        let mut batch = WriteBatch::default();
        batch.put_cf(
            db.cf_handle(),
            RecordKey::job(db.partition().id(), MigrationJobId::for_entry(active)),
            b"not a job",
        );
        db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
        let broken = report().await;
        assert!(broken.replicas.is_empty());
        assert_eq!(
            broken
                .unreadable
                .iter()
                .map(|replica| (replica.partition_id, replica.record))
                .collect::<Vec<_>>(),
            [(PartitionId::MIN, "job")]
        );
        assert!(
            broken.unreadable[0]
                .error
                .starts_with(&format!("job {active}: malformed job record")),
            "{:?}",
            broken.unreadable
        );
    }

    #[restate_core::test(start_paused = true)]
    async fn slices_follow_the_entry_cap_and_the_write_budget() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain(&COUNT)).await;
        let count = registry(&store).maintained(COUNT.name).unwrap().id;
        let mut options = StorageOptions::default();
        options.background_migrations_max_slice_entries = NonZeroUsize::new(4).unwrap();
        let mut driver = store.walk_driver(&options);
        let mut tx = store.transaction();

        // Slices take at most four entries, below the usual lower bound of 16.
        // Without any write budget, a slice still takes its first entry. The
        // budget can change between slices: the processor reads it live.
        let mut taken = Vec::new();
        for write_bytes in [usize::MAX, usize::MAX, 0, usize::MAX] {
            driver.ready().await;
            driver.step(&mut tx, write_bytes).await.unwrap();
            taken.push(published_job(&tx, count).rows_examined);
        }
        assert_eq!(taken, [4, 8, 9, 13]);
    }

    /// Two passive indexes waiting for their next pass after a real first one:
    /// the first may start now, the second 10 seconds later.
    async fn waiting_pair(store: &mut PartitionStore) -> [u32; 2] {
        static PEER: IndexDefinition = IndexDefinition {
            name: "passive_peer/1",
            kind: IndexKind::Aggregate,
            apply: apply_count,
            can_fill: false,
            introduced: crate::registry::TEST_INTRODUCED,
        };
        let known: &[&'static IndexDefinition] = &[&PASSIVE, &PEER];
        seed(store).await;
        try_reconcile(store, known, maintain_all(known))
            .await
            .unwrap();
        run_walks(store).await;
        let active = store.published_registry();
        let ids = [&PASSIVE, &PEER]
            .map(|definition| active.registry().maintained(definition.name).unwrap().id);
        let mut jobs = active.jobs().clone();
        let later = WallClock::now_ms().as_u64() + 10_000;
        for (id, at) in [(ids[0], None), (ids[1], Some(later))] {
            let job = jobs.get_mut(MigrationJobId::for_entry(id)).unwrap();
            assert_eq!(job.status, MigrationJobStatus::Waiting);
            job.wait(None, at);
        }
        let db = store.partition_db();
        testing::put_jobs(db, jobs.entries());
        db.install_published_registry(Arc::new(active.successor(
            None,
            active.walks().clone(),
            jobs,
        )));
        ids
    }

    #[restate_core::test(start_paused = true)]
    async fn waiting_jobs_report_their_own_start_backoff_and_held_exports() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();

        // The first job's start fails and retries in a minute. Before that, the
        // second becomes eligible and starts, or fails, on its own: the first
        // keeps its reason either way.
        for (partition, second_fails) in [(1, false), (2, true)] {
            let partition = Partition::new(PartitionId::from(partition), KeyRange::FULL);
            let mut store = manager.open(&partition, None).await.unwrap();
            let [first, second] = waiting_pair(&mut store).await;
            let db = store.partition_db().clone();
            let mut driver = store.walk_driver(&StorageOptions::default());
            driver.tuning_mut().backoff = Duration::from_secs(60);
            let mut tx = store.transaction();
            let faults = manager.faults(&partition);
            faults.fail(FaultPoint::PassStart, RocksError::ReadOnly);
            drive(&mut driver, &mut tx, 1).await;
            let state = db.driver_state().get().unwrap();
            let retry = state
                .backoff_starts
                .iter()
                .find(|(id, _)| *id == first)
                .unwrap()
                .1;
            if second_fails {
                faults.fail(FaultPoint::PassStart, RocksError::ReadOnly);
            }
            drive(&mut driver, &mut tx, 2).await;
            assert!(
                Instant::now() < retry,
                "the first job's retry is still ahead"
            );
            let started = tx.published_registry().registry().get(second).unwrap().walk;
            assert_eq!(started.is_some(), !second_fails);
            assert_eq!(
                [first, second].map(|id| blocked_by(&db, &tx, id, Instant::now())),
                [
                    Some("failure-backoff"),
                    second_fails.then_some("failure-backoff")
                ],
                "second fails: {second_fails}"
            );
        }

        // A snapshot export holds a fresh driver from its first step: the job
        // that may start now shows it at once, the other once it's due, while
        // the export still holds. The report shows it too.
        let partition = Partition::new(PartitionId::from(3), KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        let [first, second] = waiting_pair(&mut store).await;
        let db = store.partition_db().clone();
        let mut driver = store.walk_driver(&StorageOptions::default());
        let mut tx = store.transaction();
        let fence = tx.fence_commits().unwrap();
        drive(&mut driver, &mut tx, 1).await;
        assert_eq!(
            [first, second].map(|id| blocked_by(&db, &tx, id, Instant::now())),
            [Some("snapshot-export"), None]
        );
        let state = db.driver_state().get().unwrap();
        let due = state
            .eligible_at
            .iter()
            .find(|(id, _)| *id == second)
            .unwrap()
            .1;
        while Instant::now() < due {
            drive(&mut driver, &mut tx, 1).await;
        }
        assert_eq!(
            blocked_by(&db, &tx, second, Instant::now()),
            Some("snapshot-export")
        );
        let report = manager
            .migration_report(&QueryIoExecutor::new(NonZeroUsize::MIN), IndexSizes::Skip)
            .unwrap()
            .await
            .unwrap();
        let replica = report
            .replicas
            .iter()
            .find(|replica| replica.partition_id == partition.id())
            .unwrap();
        let job = replica.jobs.get(MigrationJobId::for_entry(first)).unwrap();
        assert_eq!(
            replica.blocked_by(job, Instant::now()),
            Some("snapshot-export")
        );
        drop(fence);
    }

    #[restate_core::test(start_paused = true)]
    async fn a_failed_pass_start_leaves_started_walks_their_turns() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        seed(&mut store).await;
        // RESUMED's walk is near its end when ADOPTED joins it late; COUNT's walk,
        // started at the next open, has all its work ahead.
        reconcile(&mut store, maintain(&RESUMED)).await;
        let resumed = registry(&store).maintained(RESUMED.name).unwrap().id;
        let near = registry(&store).get(resumed).unwrap().walk.unwrap();
        fill(&mut store, near, SEEDED as usize - 7).await;
        let (mut joined, mut walks_now) = (registry(&store), walks(&store));
        let adopted = joined.register(ADOPTED.name, ADOPTED.kind).unwrap();
        joined.link(adopted, near).unwrap();
        walks_now.join(near, adopted).unwrap();
        write_state(&store, &joined, &walks_now);
        reconcile(&mut store, maintain(&COUNT)).await;
        let count = registry(&store).maintained(COUNT.name).unwrap().id;

        let mut driver = store.walk_driver(&StorageOptions::default());
        driver.tuning_mut().max_keys = 16;
        let db = store.partition_db().clone();
        let mut tx = store.transaction();
        let rows = |tx: &PartitionStoreTransaction<'_>| published_job(tx, count).rows_examined;
        let linked = |tx: &PartitionStoreTransaction<'_>| {
            tx.published_registry()
                .registry()
                .get(adopted)
                .unwrap()
                .walk
        };

        // The near walk ends: RESUMED is Ready and the late joiner ADOPTED waits,
        // eligible at once. Starting its pass fails once.
        manager
            .faults(&partition)
            .fail(FaultPoint::PassStart, RocksError::ReadOnly);
        drive(&mut driver, &mut tx, 1).await;
        assert_eq!(
            published_job(&tx, adopted).status,
            MigrationJobStatus::Waiting
        );
        drive(&mut driver, &mut tx, 1).await;
        assert_eq!((linked(&tx), rows(&tx)), (None, 0), "the start failed");
        let failed = published_job(&tx, adopted);
        assert_eq!(
            failed.failures, 1,
            "recorded on the job, like a failed slice"
        );
        assert!(failed.last_error.is_some());
        // ADOPTED shows its start's backoff until it ends, without another step;
        // COUNT, runnable, shows nothing.
        assert_eq!(
            blocked_by(&db, &tx, adopted, Instant::now()),
            Some("failure-backoff")
        );
        assert_eq!(blocked_by(&db, &tx, count, Instant::now()), None);
        let (_, retry) = db.driver_state().get().unwrap().backoff_starts[0];
        assert_eq!(blocked_by(&db, &tx, adopted, retry), None);
        // COUNT's walk takes the next turn while the start waits out its backoff;
        // then the start succeeds, with no reopen.
        drive(&mut driver, &mut tx, 1).await;
        assert_eq!(
            (linked(&tx), rows(&tx) > 0),
            (None, true),
            "a turn for COUNT"
        );
        drive(&mut driver, &mut tx, 10_000).await;
        assert!(db.driver_state().get().unwrap().backoff_starts.is_empty());
        drop(tx);
        for id in [resumed, adopted, count] {
            assert_eq!(
                (
                    registry(&store).get(id).unwrap().state,
                    self::count(&store, id)
                ),
                (IndexStatus::Ready, SEEDED),
                "{id}"
            );
        }
    }

    #[restate_core::test]
    async fn background_walks_resume_after_a_restart_to_parity() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
        let mut store = manager.open(&partition, None).await.unwrap();
        let mut reference = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();
        let all = crate::registry::INDEX_DEFINITIONS;
        seed(&mut store).await;
        seed(&mut reference).await;
        try_reconcile(&mut reference, all, maintain_all(all))
            .await
            .unwrap();
        run_walks(&mut reference).await;
        try_reconcile(&mut store, all, maintain_all(all))
            .await
            .unwrap();
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let db = store.partition_db().clone();
        let mut driver = store.walk_driver(&StorageOptions::default());
        // Small slices, so that the walk is still going after a few.
        driver.tuning_mut().max_keys = 32;
        let mut tx = store.transaction();
        let walks = tx.published_registry().walks().clone();
        let [walk] = walks.entries() else {
            panic!("one walk: {walks:?}");
        };
        let walk = walk.id;
        assert_eq!(
            tx.published_registry().registry().members(walk).count(),
            all.len()
        );

        // A few slices later, commands delete an entry the walk covered and one it
        // did not reach, through the same transaction.
        drive(&mut driver, &mut tx, 4).await;
        let member = tx
            .published_registry()
            .registry()
            .members(walk)
            .next()
            .unwrap()
            .id;
        let covered = published_job(&tx, member).rows_examined as usize;
        assert!((1..entries.len() - 1).contains(&covered), "{covered}");
        for index in [0, entries.len() - 1] {
            delete_entry(&mut tx, &db, entries[index].1).await;
            let reference_db = reference.partition_db().clone();
            delete_entry(
                &mut reference.transaction(),
                &reference_db,
                entries[index].1,
            )
            .await;
        }
        assert!(
            Arc::ptr_eq(&tx.published_registry(), &db.published_registry().unwrap()),
            "the processor's transaction and the partition share the committed set"
        );

        // A restart from the flushed state, here a snapshot, resumes the walk
        // without double-counting.
        drop(tx);
        let snapshots = restate_test_util::tempdir().unwrap();
        let snapshot = store
            .create_local_snapshot(
                manager.test_capture_guard(partition.partition_id).await,
                snapshots.path(),
                None,
                SnapshotId::new(),
            )
            .await
            .unwrap();
        drop((driver, db, store));
        let restored = LocalPartitionSnapshot {
            base_dir: SnapshotDir::new(snapshots.path().into()),
            log_id: LogId::from(partition.partition_id),
            min_applied_lsn: snapshot.min_applied_lsn,
            db_comparator_name: snapshot.db_comparator_name.clone(),
            files: snapshot.files.clone(),
            key_range: partition.key_range,
            compatibility: snapshot.compatibility.clone(),
            byte_offsets_base_lsn: snapshot.byte_offsets_base_lsn,
            log_byte_offset: snapshot.log_byte_offset,
            partition_store_byte_offset: snapshot.partition_store_byte_offset,
        };
        let _exported = snapshot.base_dir.into_path();
        manager.drop_partition_store(&partition).await.unwrap();
        let mut store = manager
            .open_from_snapshot(&partition, restored)
            .await
            .unwrap();
        try_reconcile(&mut store, all, maintain_all(all))
            .await
            .unwrap();
        assert_eq!(
            committed_job(&store, member).rows_examined as usize,
            covered
        );
        let mut driver = store.walk_driver(&StorageOptions::default());
        let mut tx = store.transaction();
        drive(&mut driver, &mut tx, 1_000).await;
        assert_eq!(driver.due(), None, "nothing left");
        drop(tx);

        assert!(self::walks(&store).entries().is_empty());
        assert!(
            registry(&store)
                .indexes()
                .iter()
                .all(|index| index.state == IndexStatus::Ready)
        );
        assert_eq!(index_rows(&store), index_rows(&reference));
        let snapshot = store.query_read(CancellationToken::new()).unwrap().0;
        for definition in all {
            snapshot
                .ready_index(definition.name, definition.kind)
                .unwrap();
        }
    }

    #[restate_core::test(start_paused = true)]
    async fn background_walks_wait_out_fences_failures_and_disabling() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        let entries = read_chunk(&store, None, usize::MAX).unwrap();

        // With background migrations disabled, the driver leaves the started walk
        // waiting: nothing is due, ever.
        reconcile(&mut store, maintain(&COUNT)).await;
        let mut disabled = StorageOptions::default();
        disabled.disable_background_migrations = true;
        let mut idle = store.walk_driver(&disabled);
        assert_eq!(idle.due(), None);
        assert!(idle.ready().now_or_never().is_none());
        let id = registry(&store).maintained(COUNT.name).unwrap().id;
        let walk = registry(&store).get(id).unwrap().walk.unwrap();
        assert_eq!(walks(&store).get(walk).unwrap().cursor, None);
        let db = store.partition_db().clone();
        assert_eq!(
            blocked_by(&db, &store.transaction(), id, Instant::now()),
            Some("background-migrations-disabled")
        );

        // Enabled, it runs the walk. It replaces the disabled driver's state, and
        // dropping that driver withdraws only its own. While a snapshot export
        // holds commits back, it does nothing and checks again shortly.
        let mut driver = store.walk_driver(&StorageOptions::default());
        drop(idle);
        assert_eq!(
            db.driver_state().get().map(|state| state.disabled),
            Some(false)
        );
        let recheck = driver.tuning_mut().fence_recheck;
        let mut tx = store.transaction();
        let progress = |tx: &PartitionStoreTransaction<'_>| {
            let cursor = tx
                .published_registry()
                .walks()
                .get(walk)
                .unwrap()
                .cursor
                .clone();
            (cursor, published_job(tx, id))
        };
        let fence = tx.fence_commits().unwrap();
        let before = Instant::now();
        drive(&mut driver, &mut tx, 1).await;
        assert_eq!(progress(&tx).0, None);
        assert!(driver.due().unwrap() >= before + recheck);
        assert_eq!(
            blocked_by(&db, &tx, id, Instant::now()),
            Some("snapshot-export")
        );
        drop(fence);
        drive(&mut driver, &mut tx, 1).await;
        assert_eq!(blocked_by(&db, &tx, id, Instant::now()), None);
        assert_eq!(
            progress(&tx).1.rows_examined,
            16,
            "a first slice of the lower bound"
        );

        // A slice that fails is recorded, and retried after a backoff while the
        // index stays Backfilling.
        let invocation = entries[40].1.to_invocation_id().unwrap();
        let status = tx.get_invocation_status(&invocation).await.unwrap();
        tx.delete_invocation_status(&invocation).unwrap();
        tx.commit().await.unwrap();
        let committed = progress(&tx);
        while progress(&tx).1.failures < 2 {
            drive(&mut driver, &mut tx, 1).await;
        }
        let failed = progress(&tx);
        assert_eq!(
            (failed.0.clone(), failed.1.rows_examined),
            (committed.0, committed.1.rows_examined),
            "failed slices commit no progress"
        );
        assert!(
            failed
                .1
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("missing invocation status"))
        );
        assert_eq!(
            tx.published_registry().registry().get(id).unwrap().state,
            IndexStatus::Backfilling
        );
        // Its job shows the walk's backoff until it ends, without another step.
        assert_eq!(
            blocked_by(&db, &tx, id, Instant::now()),
            Some("failure-backoff")
        );
        let (_, retry) = db.driver_state().get().unwrap().backoff_walks[0];
        assert_eq!(blocked_by(&db, &tx, id, retry), None);
        tx.put_invocation_status(&invocation, &status).unwrap();
        tx.commit().await.unwrap();
        drive(&mut driver, &mut tx, 1_000).await;
        // Recovered, nothing is reported; without a driver, nothing is either.
        assert!(db.driver_state().get().unwrap().backoff_walks.is_empty());
        drop(driver);
        assert_eq!(db.driver_state().get(), None);
        drop(tx);
        assert_eq!(count(&store, id), SEEDED);
        assert_eq!(registry(&store).get(id).unwrap().state, IndexStatus::Ready);
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_closed_partition_takes_no_slice_and_a_held_publication_delays_its_close() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::from(32), KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain(&COUNT)).await;
        let id = registry(&store).maintained(COUNT.name).unwrap().id;
        let walk = registry(&store).get(id).unwrap().walk.unwrap();
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let faults = manager.faults(&partition);
        let db = store.partition_db().clone();

        // A slice held at its publication, after its commit, keeps its session:
        // closing the partition waits for it.
        let (published, resume) = faults.pause(FaultPoint::WalkPublication);
        let mut tx = store.transaction();
        {
            let mut slice = std::pin::pin!(engine::run_slice(&mut tx, &db, walk, 3));
            tokio::select! {
                biased;
                result = &mut slice => panic!("the slice finished without publishing: {result:?}"),
                published = published => published.unwrap(),
            }
            let mut closing = std::pin::pin!(manager.close(partition.id()));
            assert!(
                futures::poll!(closing.as_mut()).is_pending(),
                "the_close_waits"
            );
            drop(resume);
            assert_eq!(slice.await.unwrap(), Slice::Progressed);
            closing.await;
        }
        assert_eq!(
            tx.published_registry()
                .walks()
                .get(walk)
                .unwrap()
                .cursor
                .as_deref(),
            Some(entries[2].0.as_slice()),
            "published_before_the_close"
        );

        // Once closed, the old handle commits neither a slice nor a failure record.
        assert!(matches!(
            engine::run_slice(&mut tx, &db, walk, 3).await,
            Err(engine::SliceError::Staging(StorageError::OperationalError))
        ));
        assert!(tx.is_empty(), "nothing_stays_staged");
        assert!(matches!(
            engine::record_failure(&mut tx, &db, walk, "late", Duration::ZERO).await,
            Err(engine::SliceError::Staging(StorageError::OperationalError))
        ));
        drop(tx);

        // A fresh open installs the committed records and continues after them.
        let mut store = manager.open(&partition, None).await.unwrap();
        reconcile(&mut store, maintain(&COUNT)).await;
        let resumed = walks(&store).get(walk).unwrap().clone();
        let job = committed_job(&store, id);
        assert_eq!(
            (resumed.cursor.as_deref(), job.rows_examined, job.failures),
            (Some(entries[2].0.as_slice()), 3, 0)
        );
        run_walks(&mut store).await;
        assert_eq!(count(&store, id), SEEDED);
    }

    /// `COUNT`'s index read as a table: its header and no suffix.
    enum CountTable {}
    enum CountHeader {}
    struct CountKey;

    impl KeyDecode for CountKey {
        fn decode(_: &mut &[u8]) -> crate::Result<Self> {
            Ok(Self)
        }
    }

    impl KeySchema for CountKey {
        type Header = CountHeader;
    }

    impl KeyHeader for CountHeader {
        type Scope = IndexScope;
        const KEY_KIND: KeyKind = KeyKind::Stats;
        const LENGTH: usize = AggregateKeyPrefix::SERIALIZED_LENGTH + 1;

        fn scope(metadata: &ReadMetadata<'_>) -> crate::Result<IndexScope> {
            Ok(IndexScope {
                partition: metadata.partition.partition_id,
                index: metadata.ready_index(COUNT.name, COUNT.kind)?,
            })
        }

        fn bounds(scope: IndexScope) -> Option<HeaderBounds> {
            Some(HeaderBounds::prefix(
                Self::KEY_KIND,
                count_key(scope.partition, scope.index).into(),
            ))
        }
    }

    impl Table for CountTable {
        type Record<'a> = (KeyDecoder<'a, CountKey>, &'a [u8]);
        type Scan = KeySelection<CountKey>;
    }

    impl PhysicalTable for CountTable {
        type Key = CountKey;
    }

    /// `COUNT` as a snapshot read sees it.
    fn snapshot_count(snapshot: &ReadSnapshot) -> crate::Result<u64> {
        let mut total = 0;
        <ReadSnapshot as BlockingScan<CountTable>>::scan_blocking(
            snapshot,
            Arc::new(KeySelection::all()),
            |(_, payload)| {
                total += <u64 as StatValueCodec>::deserialize_from(payload).unwrap();
                ControlFlow::Continue(())
            },
        )?;
        Ok(total)
    }

    fn not_ready<T: std::fmt::Debug>(result: crate::Result<T>) -> bool {
        match result {
            Err(StorageError::Generic(err)) => {
                err.downcast_ref::<super::super::IndexNotReady>().is_some()
            }
            other => panic!("expected IndexNotReady, got {other:?}"),
        }
    }

    #[restate_core::test]
    async fn queries_admit_only_ready_indexes_of_their_snapshot() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        let snapshot =
            |store: &PartitionStore| store.query_read(CancellationToken::new()).unwrap().0;

        let unregistered = snapshot(&store);
        assert!(not_ready(
            unregistered.ready_index("count/1", IndexKind::Aggregate)
        ));
        assert!(not_ready(snapshot_count(&unregistered)), "table_reads_fail");
        reconcile(&mut store, maintain(&COUNT)).await;
        let backfilling = snapshot(&store);
        assert!(
            not_ready(backfilling.ready_index("count/1", IndexKind::Aggregate)),
            "backfilling_indexes_are_not_ready"
        );
        assert!(not_ready(snapshot_count(&backfilling)));

        run_walks(&mut store).await;
        let id = registry(&store).maintained("count/1").unwrap().id;
        assert!(
            not_ready(backfilling.ready_index("count/1", IndexKind::Aggregate)),
            "readiness_is_that_of_the_snapshot"
        );
        assert!(not_ready(snapshot_count(&backfilling)));
        let ready = snapshot(&store);
        assert_eq!(
            ready.ready_index("count/1", IndexKind::Aggregate).unwrap(),
            id
        );
        assert_eq!(snapshot_count(&ready).unwrap(), SEEDED);
        assert!(
            not_ready(ready.ready_index("count/1", IndexKind::Secondary)),
            "kinds_must_match"
        );
        assert!(not_ready(
            ready.ready_index("absent/1", IndexKind::Aggregate)
        ));

        reconcile(
            &mut store,
            Requests {
                reset: vec![&COUNT],
                ..Default::default()
            },
        )
        .await;
        assert!(
            not_ready(snapshot(&store).ready_index("count/1", IndexKind::Aggregate)),
            "dropped_indexes_are_not_ready"
        );
        assert!(not_ready(snapshot_count(&snapshot(&store))));
        assert_eq!(
            ready.ready_index("count/1", IndexKind::Aggregate).unwrap(),
            id,
            "an_admitted_snapshot_keeps_its_view"
        );
        assert_eq!(
            snapshot_count(&ready).unwrap(),
            SEEDED,
            "and_reads_its_data"
        );
    }
}

#[cfg(test)]
mod open_tests {
    use bytes::Bytes;
    use tokio_util::sync::CancellationToken;

    use restate_rocksdb::{RocksDbManager, RocksError};
    use restate_types::identifiers::{BaseEntryId, PartitionId};
    use restate_types::partition_table::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::{EntryId, EntryKind};

    use super::*;
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::keys::EncodeTableKeyPrefix;
    use crate::registry::records::{get_registry, testing};
    use crate::stats::StatKind;
    use crate::stats::aggregated::backfill::tests::seed;
    use crate::vqueue_table::EntryStatusKey;
    use crate::walk::{WalkKind, Walks, engine};
    use crate::{PartitionStoreManager, PartitionStoreTransaction};

    use super::super::backfill::read_chunk;

    fn ignore(
        _: &mut PartitionStoreTransaction<'_>,
        _: u32,
        _: &restate_storage_api::vqueue_table::EntryContext<'_>,
        _: Option<&restate_storage_api::vqueue_table::EntryStateRef<'_>>,
        _: Option<&restate_storage_api::vqueue_table::EntryStateRef<'_>>,
    ) {
    }

    static STAT: IndexDefinition = IndexDefinition {
        name: "stat/1",
        kind: IndexKind::Aggregate,
        apply: ignore,
        can_fill: true,
        introduced: crate::registry::TEST_INTRODUCED,
    };

    async fn reconcile(
        store: &mut PartitionStore,
        requests: Requests,
    ) -> Result<(), MigrationError> {
        let db = store.partition_db().clone();
        let mut features = LoadedStorageFeatures::load(
            get_storage_features_from_partition_db(&db).unwrap(),
            get_storage_version_from_partition_db(&db).unwrap(),
            SemanticRestateVersion::current(),
        )
        .unwrap();
        let mut min = get_min_restate_version_from_partition_db(&db).unwrap();
        store
            .reconcile_registry(
                &[&STAT],
                &requests,
                FenceState {
                    features: &mut features,
                    current_version: SemanticRestateVersion::current(),
                    min_restate_version: &mut min,
                },
            )
            .await
    }

    fn maintain() -> Requests {
        Requests {
            maintain: vec![&STAT],
            ..Default::default()
        }
    }

    /// Writes `registry` and `walks` without flushing, as an interrupted open
    /// might have left them.
    fn write_unflushed(db: &PartitionDb, registry: &Registry, walks: &Walks) {
        let mut batch = WriteBatch::default();
        testing::stage_overwrite(db, &mut batch, registry, walks);
        let mut options = rocksdb::WriteOptions::default();
        options.disable_wal(true);
        db.rocksdb()
            .inner()
            .as_raw_db()
            .write_opt(&batch, &options)
            .unwrap();
    }

    fn unflushed(db: &PartitionDb) -> u64 {
        let raw = db.rocksdb().inner().as_raw_db();
        [
            rocksdb::properties::NUM_ENTRIES_ACTIVE_MEM_TABLE,
            rocksdb::properties::NUM_ENTRIES_IMM_MEM_TABLES,
        ]
        .into_iter()
        .map(|property| {
            raw.property_int_value_cf(db.cf_handle(), property)
                .unwrap()
                .unwrap()
        })
        .sum()
    }

    fn cursor(partition_key: u64, byte: u8) -> Bytes {
        let id = BaseEntryId::new(
            partition_key,
            EntryId::new(EntryKind::Invocation, [byte; 16]),
        );
        Bytes::from(EntryStatusKey::from(id).serialize().to_vec())
    }

    #[restate_core::test]
    async fn cursors_are_validated_and_retries_flush_before_publication() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        // The seeded entries live under partition keys 10 to 12.
        let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, 100));
        let mut store = manager.open(&partition, None).await.unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain()).await.unwrap();
        let db = store.partition_db().clone();
        let registered = get_registry(&db).unwrap().unwrap();
        let id = registered.maintained("stat/1").unwrap().id;
        assert_eq!(unflushed(&db), 0, "registration_is_flushed");

        let started = records::load(&db).unwrap().0.walks;

        // A walk positioned at an entry-status key of another partition is not
        // a walk of this one.
        let mut outside = Walks::default();
        let mut linked = Registry::default();
        assert_eq!(linked.register("stat/1", IndexKind::Aggregate).unwrap(), id);
        let walk = outside
            .start(
                &mut linked,
                WalkKind::index_backfill(),
                Source::EntryStatus,
                cursor(500, 1),
            )
            .unwrap();
        outside.advance(walk, cursor(500, 1)).unwrap();
        linked.link(id, walk).unwrap();
        write_unflushed(&db, &linked, &outside);
        assert!(
            reconcile(&mut store, maintain()).await.is_err(),
            "out_of_range"
        );

        // With the registered state back, its walk runs to completion.
        write_unflushed(&db, &registered, &started);
        reconcile(&mut store, maintain()).await.unwrap();
        engine::run_all(&mut store, 3).await.unwrap();
        let completed = get_registry(&db).unwrap().unwrap();
        assert_eq!(completed.get(id).unwrap().state, IndexStatus::Ready);

        // An attempt that wrote registry state but did not flush it: a retry on
        // the same physical store, with nothing left to change, still flushes.
        let mut later = completed.clone();
        later.register("other/1", IndexKind::Secondary).unwrap();
        later
            .drop_index(later.maintained("other/1").unwrap().id)
            .unwrap();
        write_unflushed(&db, &later, &Walks::default());
        drop(store);
        let mut retry = manager.open(&partition, None).await.unwrap();
        assert!(unflushed(retry.partition_db()) > 0);
        reconcile(&mut retry, maintain()).await.unwrap();
        assert_eq!(
            get_registry(retry.partition_db()).unwrap().unwrap(),
            later,
            "the retry changes nothing"
        );
        assert_eq!(
            unflushed(retry.partition_db()),
            0,
            "but flushes before publishing"
        );
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconciliation_through_a_sealed_partition_changes_nothing() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::from(31), KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain()).await.unwrap();
        let db = store.partition_db().clone();
        let registered = get_registry(&db).unwrap().unwrap();
        let walks = records::load(&db).unwrap().0.walks;
        let id = registered.maintained("stat/1").unwrap().id;
        // A row of the index.
        let mut key = AggregateKeyPrefix::new(partition.id(), id)
            .as_bytes()
            .to_vec();
        key.push(StatKind::AggregatedGauge as u8);
        key.extend_from_slice(b"payload");
        let raw = db.rocksdb().inner().as_raw_db();
        raw.put_cf(db.cf_handle(), &key, b"value").unwrap();
        let installed = db.published_registry().unwrap();
        let ledger = |db: &PartitionDb| {
            serde_json::to_value(get_storage_features_from_partition_db(db).unwrap()).unwrap()
        };
        let fenced = ledger(&db);

        // Closing seals the partition's session: the old handle can neither reset
        // the index nor publish, whether it has a change to make or not.
        manager.close(partition.id()).await;
        let reset = Requests {
            reset: vec![&STAT],
            ..Default::default()
        };
        for requests in [reset, maintain()] {
            assert!(matches!(
                reconcile(&mut store, requests).await,
                Err(MigrationError::StorageError(StorageError::OperationalError))
            ));
        }
        assert!(
            raw.get_cf(db.cf_handle(), &key).unwrap().is_some(),
            "the_index_keys_stay"
        );
        assert_eq!(get_registry(&db).unwrap().unwrap(), registered);
        assert_eq!(records::load(&db).unwrap().0.walks, walks);
        assert_eq!(ledger(&db), fenced, "the_fence_stays");
        assert!(
            Arc::ptr_eq(&db.published_registry().unwrap(), &installed),
            "nothing_was_published"
        );

        // A fresh open reconciles as usual.
        let mut store = manager.open(&partition, None).await.unwrap();
        reconcile(&mut store, maintain()).await.unwrap();
        assert_eq!(
            get_registry(store.partition_db()).unwrap().unwrap(),
            registered
        );
    }

    #[restate_core::test]
    async fn reconciled_indexes_are_published_only_once_flushed() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        seed(&mut store).await;
        let db = store.partition_db().clone();
        let faults = manager.faults(&partition);
        // Seeding installed the empty set a store without reconciliation uses.
        let empty = db.published_registry().unwrap();
        assert!(empty.registry().indexes().is_empty());
        let unpublished = |db: &PartitionDb| Arc::ptr_eq(&db.published_registry().unwrap(), &empty);

        // A failed flush fails the open and publishes nothing: the registration
        // is written, but not installed.
        faults.fail(FaultPoint::RegistryFlush, RocksError::ReadOnly);
        assert!(matches!(
            reconcile(&mut store, maintain()).await,
            Err(MigrationError::StorageError(StorageError::Generic(_)))
        ));
        assert!(unpublished(&db), "nothing_was_published");
        let written = get_registry(&db).unwrap().unwrap();
        assert_eq!(
            written.maintained("stat/1").map(|index| index.state),
            Some(IndexStatus::Backfilling)
        );

        // A retry changes nothing but still flushes; while that flush is pending,
        // nothing is published either. Once it succeeds, the written registration
        // is installed.
        let (reached, resume) = faults.pause(FaultPoint::RegistryFlush);
        let mut retry = std::pin::pin!(reconcile(&mut store, maintain()));
        tokio::select! {
            biased;
            result = &mut retry => panic!("reconciliation finished without flushing: {result:?}"),
            reached = reached => reached.unwrap(),
        }
        assert!(unpublished(&db), "unpublished_while_the_flush_is_pending");
        drop(resume);
        retry.await.unwrap();
        let installed = db.published_registry().unwrap();
        assert_eq!(installed.registry(), &written);
        assert!(!installed.touched_by_on_write().is_empty());
    }

    #[restate_core::test]
    async fn production_open_rejects_an_invalid_registry_before_migrating() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        // Initialize the store first, so only the registry can fail its next open.
        store
            .verify_and_run_migrations(CancellationToken::new(), &Configuration::default())
            .await
            .unwrap();
        seed(&mut store).await;
        let db = store.partition_db().clone();
        let mut invalid = Registry::default();
        invalid.register("stat/1", IndexKind::Aggregate).unwrap();
        invalid.next_id = 10;
        write_unflushed(&db, &invalid, &Walks::default());
        // A legacy gauge, which a successful open would delete first.
        let legacy = crate::features::legacy_status_gauge_key(db.partition().id(), 1);
        let raw = db.rocksdb().inner().as_raw_db();
        raw.put_cf(db.cf_handle(), &legacy, b"value").unwrap();
        let ledger = |db: &PartitionDb| {
            serde_json::to_value(get_storage_features_from_partition_db(db).unwrap()).unwrap()
        };
        let before = ledger(&db);
        let entries = read_chunk(&store, None, usize::MAX).unwrap().len();
        let mut config = Configuration::default();
        config.common.experimental.set_track_status_gauges(true);
        let error = store
            .verify_and_run_migrations(CancellationToken::new(), &config)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("invalid registry"),
            "the_registry_fails_the_open: {error}"
        );
        assert_eq!(ledger(&db), before, "no_feature_was_enabled");
        assert!(
            raw.get_cf(db.cf_handle(), &legacy).unwrap().is_some(),
            "no_legacy_gauge_was_deleted"
        );
        assert!(
            get_registry(&db).is_err(),
            "the_invalid_registry_is_left_as_is"
        );
        assert_eq!(read_chunk(&store, None, usize::MAX).unwrap().len(), entries);
    }
}
