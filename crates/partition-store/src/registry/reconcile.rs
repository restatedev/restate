// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
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
//! 1. **Retire** maintained indexes of definitions this binary does not know (a
//!    rollback, or another build): their keys are range-deleted and they become
//!    Dropped, keeping their IDs reserved.
//! 2. **Reset** the indexes of families whose reset is requested, the same way.
//! 3. **Register** each requested definition that has no maintained index, under a
//!    fresh ID. Running out of IDs skips a maintenance-only request with an error
//!    log; a request for completeness fails the open.
//! 4. **Fence:** once anything was ever registered, the `query-registry` storage
//!    feature is in the store's ledger, so older binaries cannot open it.
//!
//! The changes, the walks (see [`crate::walk`]) and the fence are written in one
//! batch, which also upgrades a format-1 registry: its passes in progress become
//! walks. The reconciled indexes are then installed for writes, and the
//! startup driver runs the walks one at a time, a slice per commit, through the
//! walk engine (see [`crate::walk::engine`] and `complete`): every started walk,
//! in the order they started, even if its request was cleared. The store is
//! flushed before they run, so the indexes, walks and fence are durable
//! before any effect depends on them. Only then do requested indexes that have
//! not started form one new walk; its start is committed and flushed before its
//! first effect, and until then they stay unstarted.
//!
//! With `enable-eager-migrations-on-startup` off, walks wait for a later driver
//! instead: started walks keep their cursors, which writes keep honoring, and
//! requested instances stay unstarted, even on an empty partition. Either way,
//! the store is flushed before the processor starts whenever it has any
//! registration history, even when this open changed nothing: an earlier attempt
//! may have written registry state, effects or Ready without finishing its flush.
//!
//! The registry, the walks, their positions, their links and the fence walks
//! require are validated before any of this, and before any cleanup or
//! storage-feature migration of the same open (`validate_registry`).

use std::ops::RangeBounds;
use std::sync::Arc;

use rocksdb::WriteBatch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use zerocopy::IntoBytes;

use restate_rocksdb::{IoMode, Priority};
use restate_storage_api::StorageError;
use restate_types::SemanticRestateVersion;
use restate_types::config::Configuration;
use restate_types::vqueues::EntryKind;

use crate::features::{LoadedStorageFeatures, StorageFeatures};
use crate::fsm_table::{
    append_registry_to_wb, append_walks_to_wb, get_registry_from_partition_db,
    get_walks_from_partition_db,
};
use crate::keys::{DecodeTableKey, IndexKeyPrefix};
use crate::stats::AggregateKeyPrefix;
use crate::vqueue_table::EntryStatusKey;
use crate::walk::engine::{self, SliceBudget};
use crate::walk::{Source, WalkKind, Walks};
use crate::{MigrationError, PartitionDb, PartitionStore, PartitionStoreTransaction};

use super::{
    IndexDefinition, IndexKind, IndexRegistry, IndexStatus, MaintainedIndexes, RegistryError,
    complete,
};

/// Definitions released together, controlled by the same flags. The registry
/// still tracks each definition individually.
pub(crate) struct Family {
    pub(crate) definitions: &'static [&'static IndexDefinition],
    /// Register and maintain the definitions.
    pub(crate) maintain: fn(&Configuration) -> bool,
    /// Also complete them at open, before the processor starts. Implies maintain.
    pub(crate) complete: fn(&Configuration) -> bool,
    /// Disable their indexes, overriding the other requests.
    pub(crate) reset: fn(&Configuration) -> bool,
}

/// The released families.
pub(crate) const FAMILIES: &[Family] = &[
    crate::vqueue_table::STATUS_GAUGES,
    crate::vqueue_table::ENTRY_INDEXES,
];

/// What opening a partition asks of its registry.
#[derive(Default)]
pub(crate) struct Requests {
    /// Definitions to register when they have no maintained index.
    pub(crate) maintain: Vec<&'static IndexDefinition>,
    /// Definitions to complete; also in `maintain`.
    pub(crate) complete: Vec<&'static IndexDefinition>,
    /// Definitions whose maintained index is dropped.
    pub(crate) reset: Vec<&'static IndexDefinition>,
    /// Leave started and requested walks waiting instead of running them at open,
    /// when `enable-eager-migrations-on-startup` is off.
    pub(crate) defer_walks: bool,
}

impl Requests {
    pub(crate) fn from_config(families: &[Family], config: &Configuration) -> Self {
        let mut requests = Self {
            defer_walks: !config.worker.storage.enable_eager_migrations_on_startup,
            ..Self::default()
        };
        for family in families {
            if (family.reset)(config) {
                requests.reset.extend(family.definitions);
            } else if (family.complete)(config) {
                requests.maintain.extend(family.definitions);
                requests.complete.extend(family.definitions);
            } else if (family.maintain)(config) {
                requests.maintain.extend(family.definitions);
            }
        }
        requests
    }

    fn completes(&self, definition: &IndexDefinition) -> bool {
        self.complete
            .iter()
            .any(|requested| std::ptr::eq(*requested, definition))
    }
}

/// The storage-feature state of the open in progress, which the fence updates.
pub(crate) struct FenceState<'a> {
    pub(crate) features: &'a mut LoadedStorageFeatures,
    pub(crate) current_version: &'a SemanticRestateVersion,
    pub(crate) min_restate_version: &'a mut SemanticRestateVersion,
}

impl PartitionStore {
    /// Reads and validates the registry and the walks for opening this partition:
    /// their formats and invariants, every walk endpoint and cursor (and format-1
    /// cursor) against the entry-status key schema and this partition's key range,
    /// and the links between them. Walks also require the `query-registry`
    /// fence in `features`, the loaded storage features, which keeps binaries
    /// predating the registry out of a store with walks. Runs before any cleanup
    /// or migration mutates the store; an invalid state fails the open with the
    /// store unchanged.
    pub(crate) fn validate_registry(
        &self,
        features: &StorageFeatures,
    ) -> Result<(Option<IndexRegistry>, Walks), MigrationError> {
        let db = self.partition_db();
        let registry = get_registry_from_partition_db(db)?;
        let walks = get_walks_from_partition_db(db)?.unwrap_or_default();
        let barrier = |what: String, err: StorageError| {
            MigrationError::MigrationBarrier(format!("invalid {what}: {err}"))
        };
        if !walks.entries().is_empty() && !features.is_query_registry {
            return Err(MigrationError::MigrationBarrier(
                "walks exist without the query-registry storage feature".to_owned(),
            ));
        }
        for index in registry.iter().flat_map(IndexRegistry::entries) {
            if let Some(cursor) = &index.cursor {
                validate_cursor(db, cursor).map_err(|err| {
                    barrier(
                        format!(
                            "completeness cursor of '{}' ({})",
                            index.definition, index.id
                        ),
                        err,
                    )
                })?;
            }
            if let Some(walk) = index.walk
                && walks.get(walk).is_none()
            {
                return Err(barrier(
                    format!("walk link of '{}' ({})", index.definition, index.id),
                    StorageError::DataIntegrityError,
                ));
            }
        }
        for walk in walks.entries() {
            for key in std::iter::once(&walk.end).chain(walk.cursor.as_ref()) {
                validate_cursor(db, key)
                    .map_err(|err| barrier(format!("position of walk {}", walk.id), err))?;
            }
            if registry
                .as_ref()
                .is_none_or(|registry| registry.members(walk.id).next().is_none())
            {
                return Err(barrier(
                    format!("walk {} without members", walk.id),
                    StorageError::DataIntegrityError,
                ));
            }
        }
        Ok((registry, walks))
    }

    /// Reconciles the registry with `known` definitions and `requests`; see the
    /// module documentation. Fails the open on an unreadable or invalid registry.
    pub(crate) async fn reconcile_registry(
        &mut self,
        known: &[&'static IndexDefinition],
        requests: &Requests,
        fence: FenceState<'_>,
        budget: impl Into<SliceBudget>,
        cancel: &CancellationToken,
    ) -> Result<(), MigrationError> {
        let budget = budget.into();
        let FenceState {
            features,
            current_version,
            min_restate_version: current_min_restate_version,
        } = fence;
        let db = self.partition_db().clone();
        let (persisted, persisted_walks) = self.validate_registry(features.enabled())?;
        let mut registry = persisted.clone().unwrap_or_default();
        let mut walks = persisted_walks.clone();
        let mut batch = WriteBatch::default();

        upgrade_format_1(&db, &mut registry, &mut walks)?;

        let unknown: Vec<_> = registry
            .entries()
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
            registry.drop_index(id).map_err(registry_error)?;
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
                registry.drop_index(id).map_err(registry_error)?;
                info!(definition = definition.name, id, "Reset the index");
            }
        }
        // A walk whose members were all dropped has nothing left to backfill;
        // surviving members keep their walk and its position.
        let abandoned: Vec<_> = walks
            .entries()
            .iter()
            .map(|walk| walk.id)
            .filter(|walk| registry.members(*walk).next().is_none())
            .collect();
        for walk in abandoned {
            walks.remove(walk).map_err(walk_error)?;
        }

        for definition in &requests.maintain {
            match registry.register(definition.name, definition.kind) {
                Ok(_) => {}
                Err(RegistryError::Exhausted) if !requests.completes(definition) => {
                    error!(
                        definition = definition.name,
                        "Cannot register: all index IDs of this partition are allocated"
                    );
                }
                Err(err) => return Err(registry_error(err)),
            }
        }

        let changed = persisted.as_ref() != Some(&registry) && registry != IndexRegistry::default();
        let walks_changed = walks != persisted_walks;
        let fenced = !registry.entries().is_empty()
            && features.append_registry_fence(
                current_version,
                current_min_restate_version,
                db.cf_handle(),
                &mut batch,
                db.partition().id(),
            )?;
        if changed || walks_changed || fenced {
            append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), &registry);
            if walks_changed {
                append_walks_to_wb(db.cf_handle(), &mut batch, db.partition().id(), &walks);
            }
            write(&db, batch).await?;
            features.mark_persisted();
            self.set_storage_features(*features.enabled());
        }

        // Writes maintain the reconciled indexes from here on, and walks run on
        // the records as committed.
        db.install_maintained_indexes(Arc::new(MaintainedIndexes::new(&registry, &walks, known)));

        let mut started: Vec<_> = walks.entries().iter().map(|walk| walk.id).collect();
        started.sort_unstable();
        let requested: Vec<_> = known
            .iter()
            .filter(|definition| requests.completes(definition))
            .filter_map(|definition| registry.maintained(definition.name))
            .filter(|active| active.state == IndexStatus::Backfilling && active.walk.is_none())
            .map(|active| active.id)
            .collect();
        if !requests.defer_walks {
            self.run_walks(&started, &requested, budget, cancel).await?;
        } else if !started.is_empty() || !requested.is_empty() {
            warn!(
                started = started.len(),
                requested = requested.len(),
                "Index and statistics backfill waits: enable-eager-migrations-on-startup is \
                 off, and this build backfills on startup only"
            );
        }
        // Whatever this or an earlier attempt wrote becomes durable before the
        // processor starts: registry changes, walks, fence, deletions, effects.
        if !registry.entries().is_empty() {
            flush(&db).await?;
        }
        Ok(())
    }

    /// The startup driver: runs the `started` walks to their ends, in the order
    /// they started, even if their request was cleared, then starts one walk for
    /// the `requested` indexes and runs it. Until the earlier walks finished,
    /// those stay unstarted, so clearing their request before it starts leaves
    /// them so.
    async fn run_walks(
        &mut self,
        started: &[u64],
        requested: &[u32],
        budget: SliceBudget,
        cancel: &CancellationToken,
    ) -> Result<(), MigrationError> {
        let db = self.partition_db().clone();
        // Registrations, walks and the fence are durable before any walk commits
        // effects that depend on them, including state an earlier attempt left
        // unflushed.
        if !started.is_empty() {
            flush(&db).await?;
        }
        let mut tx = self.transaction();
        for walk in started {
            run_walk(&mut tx, &db, *walk, budget, cancel).await?;
        }
        if !requested.is_empty()
            && let Some(walk) = complete::start(&mut tx, &db, requested)
                .await
                .map_err(StorageError::from)?
        {
            // The start is durable before its first effect.
            flush(&db).await?;
            run_walk(&mut tx, &db, walk, budget, cancel).await?;
        }
        Ok(())
    }
}

async fn run_walk(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    walk: u64,
    budget: SliceBudget,
    cancel: &CancellationToken,
) -> Result<(), MigrationError> {
    let names: Vec<_> = tx
        .maintained_indexes()
        .registry()
        .members(walk)
        .map(|member| member.definition.clone())
        .collect();
    info!(walk, definitions = ?names, "Backfilling indexes");
    engine::run_to_end(tx, db, walk, budget, cancel).await?;
    info!(walk, definitions = ?names, "The indexes are Ready");
    Ok(())
}

async fn flush(db: &PartitionDb) -> Result<(), StorageError> {
    db.flush_memtables(true)
        .await
        .map_err(|err| StorageError::Generic(err.into()))
}

/// Upgrades a format-1 registry: each pass in progress becomes a walk that
/// resumes after its cursor and ends at the last entry now, or at its cursor if
/// that is later, so it never revisits a processed entry. Indexes with equal
/// cursors share a walk.
pub(super) fn upgrade_format_1(
    db: &PartitionDb,
    registry: &mut IndexRegistry,
    walks: &mut Walks,
) -> Result<(), MigrationError> {
    if registry.format() != 1 {
        return Ok(());
    }
    let last = engine::last_key(db)?;
    for (cursor, ids) in registry.take_legacy_cursors() {
        let end = last
            .clone()
            .filter(|last| *last > cursor)
            .unwrap_or_else(|| cursor.clone());
        let walk = walks
            .start(
                WalkKind::index_backfill(),
                Source::EntryStatus,
                end,
                Some(cursor),
            )
            .map_err(walk_error)?;
        for id in ids {
            registry.link(id, walk).map_err(registry_error)?;
        }
    }
    Ok(())
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

/// A resume cursor must be the exact key of a usable entry status of this
/// partition. The raw key codec also decodes the `Unknown` entry-kind sentinel,
/// which no stored entry has.
fn validate_cursor(db: &PartitionDb, cursor: &[u8]) -> Result<(), StorageError> {
    if cursor.len() != EntryStatusKey::serialized_length_fixed() {
        return Err(StorageError::DataIntegrityError);
    }
    let mut rest = cursor;
    let key = EntryStatusKey::deserialize_from(&mut rest)?;
    if !rest.is_empty()
        || key.id.kind() == EntryKind::Unknown
        || !db.partition().key_range.contains(&key.id.partition_key())
    {
        return Err(StorageError::DataIntegrityError);
    }
    Ok(())
}

/// Commits registry changes; the open flushes them before publishing.
async fn write(db: &PartitionDb, batch: WriteBatch) -> Result<(), StorageError> {
    let mut options = rocksdb::WriteOptions::default();
    options.disable_wal(true);
    db.rocksdb()
        .write_batch(
            "registry-reconciliation",
            Priority::High,
            IoMode::Default,
            options,
            batch,
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
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::{RESTATE_VERSION_1_8_2, SemanticRestateVersion};

    use super::*;
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::stats::StatKind;
    use crate::{PartitionStoreManager, PartitionStoreTransaction};

    #[test]
    fn released_families_consist_of_known_definitions() {
        // An unknown definition would be retired and registered again at every open.
        for family in FAMILIES {
            for definition in family.definitions {
                assert!(
                    crate::registry::DEFINITIONS
                        .iter()
                        .any(|known| std::ptr::eq(*known, *definition)),
                    "{}",
                    definition.name
                );
            }
        }
        // Walks wait by default; running them at open requires an explicit opt-in.
        let mut config = Configuration::default();
        assert!(Requests::from_config(FAMILIES, &config).defer_walks);
        config.worker.storage.enable_eager_migrations_on_startup = true;
        assert!(!Requests::from_config(FAMILIES, &config).defer_walks);
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
    };
    static STAT: IndexDefinition = IndexDefinition {
        name: "stat/1",
        kind: IndexKind::Aggregate,
        apply: ignore,
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
                3,
                &CancellationToken::new(),
            )
            .await
    }

    fn persisted(store: &PartitionStore) -> IndexRegistry {
        get_registry_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap()
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
    async fn opens_fence_register_retire_and_reset_instances() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let db = store.partition_db().clone();

        // Without requests nothing is written and the store stays unfenced.
        reconcile(&mut store, &[&INDEX, &STAT], Requests::default())
            .await
            .unwrap();
        assert!(get_registry_from_partition_db(&db).unwrap().is_none());
        assert!(!store.storage_features().is_query_registry);

        // The first registration persists the fence and the minimum version with it.
        reconcile(&mut store, &[&INDEX, &STAT], maintain(&[&INDEX, &STAT]))
            .await
            .unwrap();
        let registry = persisted(&store);
        assert_eq!(registry.maintained("index/1").unwrap().id, 64);
        assert_eq!(registry.maintained("stat/1").unwrap().id, 65);
        let ledger = LoadedStorageFeatures::load(
            get_storage_features_from_partition_db(&db).unwrap(),
            get_storage_version_from_partition_db(&db).unwrap(),
            SemanticRestateVersion::current(),
        )
        .unwrap();
        assert!(ledger.enabled().is_query_registry, "fenced_on_disk");
        assert!(
            get_min_restate_version_from_partition_db(&db)
                .unwrap()
                .is_equal_or_newer_than(&RESTATE_VERSION_1_8_2)
        );
        assert_eq!(
            store.maintained_indexes().touched_by_on_write(),
            [0, 0, 0, 64, 0, 0, 0, 65],
            "reconciled_instances_are_installed"
        );

        // Reopening with the same requests changes nothing and allocates nothing.
        reconcile(&mut store, &[&INDEX, &STAT], maintain(&[&INDEX, &STAT]))
            .await
            .unwrap();
        assert_eq!(persisted(&store), registry);

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

        // A binary that does not know the definition retires its index.
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
            "retirement_deletes_exactly_the_instance"
        );
        assert_eq!(
            store.maintained_indexes().touched_by_on_write(),
            [0, 0, 0, 65]
        );

        // Resetting the statistic disables it and deletes its keys only.
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
            "reset_deletes_exactly_the_instance"
        );
        assert!(store.storage_features().is_query_registry, "fence_is_kept");

        // Enabling again builds fresh indexes; old IDs stay reserved.
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
    }

    #[restate_core::test]
    async fn exhaustion_fails_only_required_completeness_and_invalid_registries_fail() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let db = store.partition_db().clone();
        let mut registry = IndexRegistry::default();
        registry.register("stat/1", IndexKind::Aggregate).unwrap();
        registry.next_id = u64::from(u32::MAX) + 1;
        let write = |registry: &IndexRegistry| {
            let mut batch = WriteBatch::default();
            append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), registry);
            db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
        };
        write(&registry);

        // A maintenance-only request is skipped; the existing index stays active.
        reconcile(&mut store, &[&INDEX, &STAT], maintain(&[&INDEX, &STAT]))
            .await
            .unwrap();
        assert!(persisted(&store).maintained("index/1").is_none());
        assert_eq!(
            store.maintained_indexes().touched_by_on_write(),
            [0, 0, 0, 64]
        );

        // A completeness request that cannot be registered fails the open.
        let required = Requests {
            maintain: vec![&INDEX],
            complete: vec![&INDEX],
            ..Default::default()
        };
        assert!(
            reconcile(&mut store, &[&INDEX, &STAT], required)
                .await
                .is_err()
        );

        // So does a registry this binary cannot validate.
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
mod completeness_tests {
    use std::ops::ControlFlow;

    use bytes::Bytes;
    use tokio_util::sync::CancellationToken;
    use zerocopy::IntoBytes;

    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::invocation_status_table::{
        ReadInvocationStatusTable, WriteInvocationStatusTable,
    };
    use restate_storage_api::vqueue_table::{
        EntryContext, EntryStateRef, EntryStatusHeader, ReadVQueueTable, StoredEntryRef,
        WriteVQueueTable,
    };
    use restate_storage_api::{StorageError, Transaction};
    use restate_storage_query_api::scan::{BlockingScan, Scan, Table};
    use restate_types::identifiers::PartitionId;
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::{SemanticRestateVersion, vqueues::EntryTargetRef};

    use super::*;
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::keys::header::{HeaderBounds, IndexScope, KeyHeader, KeySchema};
    use crate::keys::selection::KeySelection;
    use crate::keys::{KeyDecode, KeyDecoder, KeyKind};
    use crate::stats::aggregated::Gauge;
    use crate::stats::aggregated::backfill::tests::seed;
    use crate::stats::{StatKind, StatValueCodec};
    use crate::table::{PhysicalTable, ReadMetadata};
    use crate::walk::engine::{Slice, SliceBudget};
    use crate::walk::{Source, WalkKind, Walks};
    use crate::{PartitionStoreManager, PartitionStoreTransaction, ReadSnapshot};

    use super::super::complete::read_chunk;

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
    };
    static ADOPTED: IndexDefinition = IndexDefinition {
        name: "adopted/1",
        kind: IndexKind::Aggregate,
        apply: apply_count,
    };
    static RESUMED: IndexDefinition = IndexDefinition {
        name: "resumed/1",
        kind: IndexKind::Aggregate,
        apply: apply_count,
    };
    const KNOWN: &[&IndexDefinition] = &[&COUNT, &ADOPTED, &RESUMED];

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
                3,
                &CancellationToken::new(),
            )
            .await
    }

    fn complete(definition: &'static IndexDefinition) -> Requests {
        Requests {
            maintain: vec![definition],
            complete: vec![definition],
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

    fn registry(store: &PartitionStore) -> IndexRegistry {
        get_registry_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap()
    }

    #[restate_core::test]
    async fn passes_reflect_every_entry_exactly_once() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;

        // A requested pass over untouched entries, in chunks of three.
        reconcile(&mut store, complete(&COUNT)).await;
        let index = registry(&store).maintained("count/1").unwrap().clone();
        assert_eq!(index.state, super::IndexStatus::Ready);
        assert_eq!(index.cursor, None);
        assert_eq!(count(&store, index.id), SEEDED, "every_entry_once");
        assert!(
            store.maintained_indexes().touched_by_on_write().is_empty(),
            "ready_instances_are_not_listed"
        );

        // Entries adopted while Backfilling are skipped by the later pass.
        reconcile(&mut store, maintain(&ADOPTED)).await;
        let adopted = registry(&store).maintained("adopted/1").unwrap().id;
        let entries = read_chunk(&store, None, 2).unwrap();
        let mut tx = store.transaction();
        for (_, entry, _) in &entries {
            let header = tx.get_vqueue_entry_status(entry).await.unwrap().unwrap();
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: header.vqueue_id(),
                    target: &TARGET,
                },
                StoredEntryRef::from_header(&header),
                EntryStateRef::from_header(&header),
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(count(&store, adopted), 2, "writes_adopt");
        reconcile(&mut store, complete(&ADOPTED)).await;
        assert_eq!(
            count(&store, adopted),
            SEEDED,
            "tracked_entries_are_skipped"
        );

        // A started pass resumes after its cursor, even with its request cleared.
        reconcile(&mut store, maintain(&RESUMED)).await;
        let mut registry = registry(&store);
        let resumed = registry.maintained("resumed/1").unwrap().id;
        // As if a first chunk of ten had committed: its counts and cursor.
        let done = read_chunk(&store, None, 10).unwrap();
        let cursor = done.last().unwrap().0.clone();
        let done = done.len() as u32;
        registry.set_cursor(resumed, Bytes::from(cursor)).unwrap();
        let db = store.partition_db().clone();
        let mut batch = WriteBatch::default();
        append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), &registry);
        db.rocksdb()
            .inner()
            .as_raw_db()
            .merge_cf(
                db.cf_handle(),
                count_key(db.partition().id(), resumed),
                Gauge::add(done),
            )
            .unwrap();
        db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
        reconcile(&mut store, Requests::default()).await;
        let index = registry_after(&store, "resumed/1");
        assert_eq!(
            (index.state, count(&store, resumed)),
            (super::IndexStatus::Ready, SEEDED),
            "resumed_passes_finish_without_double_counting"
        );
    }

    /// Backfilling indexes of [`KNOWN`] over the seed: every index reflects two
    /// adopted entries, and RESUMED has a cursor after ten entries, as if its own
    /// pass had committed a first chunk. Returns their IDs.
    async fn started(store: &mut PartitionStore) -> [u32; 3] {
        seed(store).await;
        reconcile(
            store,
            Requests {
                maintain: KNOWN.to_vec(),
                ..Default::default()
            },
        )
        .await;
        let mut persisted = registry(store);
        let ids = [&COUNT, &ADOPTED, &RESUMED]
            .map(|definition| persisted.maintained(definition.name).unwrap().id);
        let entries = read_chunk(store, None, 10).unwrap();
        let mut tx = store.transaction();
        for (_, entry, _) in &entries[..2] {
            let header = tx.get_vqueue_entry_status(entry).await.unwrap().unwrap();
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: header.vqueue_id(),
                    target: &TARGET,
                },
                StoredEntryRef::from_header(&header),
                EntryStateRef::from_header(&header),
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
        persisted
            .set_cursor(ids[2], Bytes::from(entries.last().unwrap().0.clone()))
            .unwrap();
        let db = store.partition_db().clone();
        let mut batch = WriteBatch::default();
        append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), &persisted);
        // The first chunk counted its ten entries but the two adopted ones.
        batch.merge_cf(
            db.cf_handle(),
            count_key(db.partition().id(), ids[2]),
            Gauge::add(8),
        );
        db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
        ids
    }

    #[restate_core::test]
    async fn instances_at_one_position_share_a_pass() {
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

        // COUNT and ADOPTED start together, RESUMED continues on its own.
        let ids = started(&mut shared).await;
        reconcile(
            &mut shared,
            Requests {
                complete: vec![&COUNT, &ADOPTED],
                ..maintain(&COUNT)
            },
        )
        .await;
        // The same indexes, each completed on its own.
        let single_ids = started(&mut single).await;
        let mut persisted = registry(&single);
        for (definition, id) in [&COUNT, &ADOPTED, &RESUMED].into_iter().zip(single_ids) {
            single
                .complete_instance(&mut persisted, definition, id, 3, &CancellationToken::new())
                .await
                .unwrap();
        }

        for (store, ids) in [(&shared, ids), (&single, single_ids)] {
            for (definition, id) in [&COUNT, &ADOPTED, &RESUMED].into_iter().zip(ids) {
                let index = registry_after(store, definition.name);
                assert_eq!(
                    (index.state, index.cursor, count(store, id)),
                    (super::IndexStatus::Ready, None, SEEDED),
                    "{} reflects every entry once",
                    definition.name
                );
            }
        }
    }

    /// Writes `registry` and `walks` as a binary would have committed them.
    fn write_state(store: &PartitionStore, registry: &IndexRegistry, walks: Option<&Walks>) {
        let db = store.partition_db();
        let mut batch = WriteBatch::default();
        append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), registry);
        if let Some(walks) = walks {
            append_walks_to_wb(db.cf_handle(), &mut batch, db.partition().id(), walks);
        }
        db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
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

    fn walks(store: &PartitionStore) -> Walks {
        get_walks_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap_or_default()
    }

    #[restate_core::test]
    async fn format_1_passes_resume_as_walks_grouped_by_position() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(
            &mut store,
            Requests {
                maintain: KNOWN.to_vec(),
                ..Default::default()
            },
        )
        .await;
        // A binary predating walks stopped COUNT and ADOPTED after ten entries,
        // and RESUMED after twenty, counting the entries they covered.
        let mut legacy = registry(&store);
        let ids = [&COUNT, &ADOPTED, &RESUMED]
            .map(|definition| legacy.maintained(definition.name).unwrap().id);
        let entries = read_chunk(&store, None, 20).unwrap();
        for (id, covered) in ids.into_iter().zip([10, 10, 20]) {
            legacy
                .set_cursor(id, Bytes::from(entries[covered - 1].0.clone()))
                .unwrap();
            add_counts(&store, id, covered as u32);
        }
        assert_eq!(legacy.format(), 1);
        write_state(&store, &legacy, None);
        let before = store.query_read(CancellationToken::new()).unwrap().0;

        // Reopening with every request cleared resumes both positions, the shared
        // one first, without counting a covered entry twice.
        reconcile(&mut store, Requests::default()).await;
        let upgraded = registry(&store);
        assert_eq!(upgraded.format(), 2);
        for id in ids {
            let index = upgraded.get(id).unwrap();
            assert_eq!(
                (index.state, index.walk, &index.cursor),
                (super::IndexStatus::Ready, None, &None)
            );
            assert_eq!(count(&store, id), SEEDED, "{id} reflects every entry once");
        }
        let mut next = walks(&store);
        assert!(next.entries().is_empty());
        assert_eq!(
            next.start(
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::new(),
                None
            )
            .unwrap(),
            3,
            "two walks were issued and their IDs stay reserved"
        );
        // A snapshot admitted before the upgrade still reads its own format-1
        // registry, in which every index is being completed.
        assert!(
            before
                .ready_index(COUNT.name, COUNT.kind)
                .is_err_and(|err| err.to_string().contains("still being completed"))
        );
    }

    #[restate_core::test]
    async fn writes_treat_entries_a_walk_covered_as_tracked() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain(&COUNT)).await;
        // COUNT's walk covered the first ten entries without listing them.
        let mut persisted = registry(&store);
        let id = persisted.maintained(COUNT.name).unwrap().id;
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let mut walks = Walks::default();
        let walk = walks
            .start(
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::from(entries.last().unwrap().0.clone()),
                None,
            )
            .unwrap();
        walks
            .advance(walk, Bytes::from(entries[9].0.clone()), 10)
            .unwrap();
        persisted.link(id, walk).unwrap();
        write_state(&store, &persisted, Some(&walks));
        add_counts(&store, id, 10);
        store.set_maintained_indexes(MaintainedIndexes::new(&persisted, &walks, KNOWN));

        let delete = async |store: &mut PartitionStore, index: usize| {
            let mut tx = store.transaction();
            let header = tx
                .get_vqueue_entry_status(&entries[index].1)
                .await
                .unwrap()
                .unwrap();
            tx.delete_vqueue_entry_status(
                &EntryContext {
                    qid: header.vqueue_id(),
                    target: &TARGET,
                },
                StoredEntryRef::from_header(&header),
            );
            tx.commit().await.unwrap();
        };
        let update = async |store: &mut PartitionStore, index: usize| {
            let mut tx = store.transaction();
            let header = tx
                .get_vqueue_entry_status(&entries[index].1)
                .await
                .unwrap()
                .unwrap();
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: header.vqueue_id(),
                    target: &TARGET,
                },
                StoredEntryRef::from_header(&header),
                EntryStateRef::from_header(&header),
            );
            tx.commit().await.unwrap();
        };
        // Covered and unlisted: touched, so its contribution is removed.
        delete(&mut store, 4).await;
        assert_eq!(count(&store, id), 9);
        // Covered: touched, an update keeps it counted once.
        update(&mut store, 5).await;
        assert_eq!(count(&store, id), 9);
        // Not covered and unlisted: nothing to remove.
        delete(&mut store, 50).await;
        assert_eq!(count(&store, id), 9);
        // Not covered and unlisted: adopted, so it is counted and listed.
        update(&mut store, 60).await;
        assert_eq!(count(&store, id), 10);

        // Resuming the walk counts each remaining entry exactly once.
        reconcile(&mut store, Requests::default()).await;
        assert_eq!(count(&store, id), SEEDED - 2);
        assert_eq!(
            registry(&store).get(id).unwrap().state,
            super::IndexStatus::Ready
        );
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

    #[restate_core::test]
    async fn deferred_walks_wait_with_their_watermarks() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let deferred = |definitions: &[&'static IndexDefinition]| Requests {
            maintain: definitions.to_vec(),
            complete: definitions.to_vec(),
            defer_walks: true,
            ..Default::default()
        };

        // An empty partition has nothing to walk, but deferring leaves even that
        // to a driver: no index becomes Ready on its own.
        let mut empty = manager
            .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
            .await
            .unwrap();
        reconcile(&mut empty, deferred(&[&COUNT])).await;
        let unstarted = registry(&empty).maintained(COUNT.name).unwrap().clone();
        assert_eq!(unstarted.state, super::IndexStatus::Backfilling);
        assert_eq!(unstarted.walk, None);
        assert!(reason(&empty, &COUNT).contains("its backfill has not started"));

        // Walks of COUNT and RESUMED covered the first ten and twenty entries when
        // the partition reopens with ADOPTED requested too.
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(
            &mut store,
            Requests {
                maintain: vec![&COUNT, &RESUMED],
                ..Default::default()
            },
        )
        .await;
        let mut persisted = registry(&store);
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let end = Bytes::from(entries.last().unwrap().0.clone());
        let mut walks = Walks::default();
        let mut paused = Vec::new();
        for (definition, covered) in [(&COUNT, 10), (&RESUMED, 20)] {
            let id = persisted.maintained(definition.name).unwrap().id;
            let walk = walks
                .start(
                    WalkKind::index_backfill(),
                    Source::EntryStatus,
                    end.clone(),
                    None,
                )
                .unwrap();
            let cursor = Bytes::from(entries[covered - 1].0.clone());
            walks.advance(walk, cursor, covered as u64).unwrap();
            persisted.link(id, walk).unwrap();
            add_counts(&store, id, covered as u32);
            paused.push((definition, id, walk, covered));
        }
        write_state(&store, &persisted, Some(&walks));
        reconcile(&mut store, deferred(&[&COUNT, &RESUMED, &ADOPTED])).await;
        let adopted = registry(&store).maintained(ADOPTED.name).unwrap().clone();
        assert_eq!(
            (adopted.state, adopted.walk),
            (super::IndexStatus::Backfilling, None),
            "requested_instances_stay_unstarted"
        );
        assert_eq!(self::walks(&store), walks, "started_walks_wait");
        for (definition, _, walk, covered) in &paused {
            assert!(reason(&store, definition).contains(&format!(
                "is still being completed: backfill walk {walk} has examined {covered} entries"
            )));
        }
        assert!(reason(&store, &ADOPTED).contains("its backfill has not started"));

        // Every waiting walk's cursor still applies: an entry it covered is
        // touched without being listed, so deleting it removes its contribution.
        // The fifth entry is covered by both walks, the fifteenth by RESUMED's.
        for index in [4, 14] {
            let mut tx = store.transaction();
            let header = tx
                .get_vqueue_entry_status(&entries[index].1)
                .await
                .unwrap()
                .unwrap();
            tx.delete_vqueue_entry_status(
                &EntryContext {
                    qid: header.vqueue_id(),
                    target: &TARGET,
                },
                StoredEntryRef::from_header(&header),
            );
            tx.commit().await.unwrap();
        }
        assert_eq!(
            paused
                .iter()
                .map(|(_, id, ..)| count(&store, *id))
                .collect::<Vec<_>>(),
            [9, 18]
        );

        // Once walks run at open, the started walks finish first, then the
        // requested index gets its own.
        reconcile(&mut store, complete(&ADOPTED)).await;
        for definition in [&COUNT, &RESUMED, &ADOPTED] {
            let index = registry(&store)
                .maintained(definition.name)
                .unwrap()
                .clone();
            assert_eq!(index.state, super::IndexStatus::Ready);
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
        let error = try_reconcile(&mut store, KNOWN, complete(&COUNT))
            .await
            .unwrap_err();
        let MigrationError::StorageError(StorageError::Generic(cause)) = error else {
            panic!("unexpected failure: {error}");
        };
        assert!(format!("{cause:#}").contains("missing invocation status"));
        assert_eq!(count(&store, id), 3, "only_the_first_slice_committed");
        let walks = self::walks(&store);
        let [walk] = walks.entries() else {
            panic!("one walk: {walks:?}");
        };
        assert_eq!(walk.cursor.as_deref(), Some(entries[2].0.as_slice()));
        assert_eq!((walk.scanned, walk.failures), (3, 1));
        assert!(
            walk.last_error
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
        reconcile(&mut store, Requests::default()).await;
        assert_eq!(count(&store, id), SEEDED);
        assert_eq!(
            registry(&store).get(id).unwrap().state,
            super::IndexStatus::Ready
        );
        assert!(self::walks(&store).entries().is_empty());
    }

    /// The rows of every index, by ID, without the replica-local key prefix.
    fn instance_rows(store: &PartitionStore) -> Vec<(u32, Vec<u8>, Vec<u8>)> {
        let partition = store.partition_id();
        let db = store.partition_db().rocksdb().inner().as_raw_db();
        let cf = db.cf_handle(store.partition().cf_name().as_ref()).unwrap();
        let mut rows = Vec::new();
        for index in registry(store).entries() {
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
                rows.push((index.id, key[prefix.len()..].to_vec(), value));
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
        // Every released index and statistic: one walk, nine members.
        let all = crate::registry::DEFINITIONS;
        seed(&mut store).await;
        seed(&mut reference).await;
        let requests = |complete: bool| Requests {
            maintain: all.to_vec(),
            complete: if complete { all.to_vec() } else { Vec::new() },
            ..Default::default()
        };
        try_reconcile(&mut reference, all, requests(true))
            .await
            .unwrap();
        try_reconcile(&mut store, all, requests(false))
            .await
            .unwrap();
        let ids: Vec<_> = registry(&store).entries().iter().map(|r| r.id).collect();
        assert_eq!(ids.len(), all.len());
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let db = store.partition_db().clone();
        let mut tx = store.transaction();
        let walk = super::super::complete::start(&mut tx, &db, &ids)
            .await
            .unwrap()
            .unwrap();
        let progress = |tx: &PartitionStoreTransaction<'_>| {
            let walk = tx.maintained_indexes().walks().get(walk).unwrap().clone();
            (walk.cursor.unwrap(), walk.scanned as usize)
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

        // An allowance of a few entries' work ends each slice before its key
        // limit, here all but the last entry, and the cursor covers exactly the
        // work committed.
        let mut taken = Vec::new();
        let mut scanned = 2;
        while scanned < entries.len() - 1 {
            let budget = SliceBudget {
                keys: entries.len() - 1 - scanned,
                write_bytes: 16 << 10,
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
                .entries()
                .iter()
                .all(|index| index.state == super::IndexStatus::Ready)
        );
        assert_eq!(instance_rows(&store), instance_rows(&reference));
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
        reconcile(
            &mut store,
            Requests {
                maintain: KNOWN.to_vec(),
                ..Default::default()
            },
        )
        .await;
        // COUNT and ADOPTED share a walk that covered ten entries.
        let mut persisted = registry(&store);
        let [count_id, adopted_id, _] = [&COUNT, &ADOPTED, &RESUMED]
            .map(|definition| persisted.maintained(definition.name).unwrap().id);
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let mut shared = Walks::default();
        let walk = shared
            .start(
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::from(entries.last().unwrap().0.clone()),
                Some(Bytes::from(entries[9].0.clone())),
            )
            .unwrap();
        for id in [count_id, adopted_id] {
            persisted.link(id, walk).unwrap();
            add_counts(&store, id, 10);
        }
        write_state(&store, &persisted, Some(&shared));

        // Resetting ADOPTED deletes its counts; COUNT's walk resumes after its
        // cursor rather than restarting.
        reconcile(
            &mut store,
            Requests {
                reset: vec![&ADOPTED],
                ..Default::default()
            },
        )
        .await;
        let after = registry(&store);
        assert_eq!(
            after.get(adopted_id).unwrap().state,
            super::IndexStatus::Dropped
        );
        assert_eq!(count(&store, adopted_id), 0);
        assert_eq!(
            (after.get(count_id).unwrap().state, count(&store, count_id)),
            (super::IndexStatus::Ready, SEEDED)
        );
        assert!(walks(&store).entries().is_empty());
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
        reconcile(&mut store, maintain(&COUNT)).await;
        let base = registry(&store);
        let id = base.maintained(COUNT.name).unwrap().id;
        let end = Bytes::from(read_chunk(&store, None, 1).unwrap()[0].0.clone());
        let started = |end: Bytes| {
            let mut walks = Walks::default();
            let walk = walks
                .start(WalkKind::index_backfill(), Source::EntryStatus, end, None)
                .unwrap();
            (walks, walk)
        };
        let cases: [(&str, IndexRegistry, Option<Walks>); 3] = [
            (
                "dangling link",
                {
                    let mut registry = base.clone();
                    registry.link(id, 7).unwrap();
                    registry
                },
                None,
            ),
            ("walk without members", base.clone(), Some(started(end).0)),
            (
                "position outside the entry-status schema",
                {
                    let mut registry = base;
                    registry.link(id, 1).unwrap();
                    registry
                },
                Some(started(Bytes::from_static(b"not a key")).0),
            ),
        ];
        for (case, registry, walks) in cases {
            write_state(&store, &registry, walks.as_ref());
            assert!(
                matches!(
                    store.validate_registry(&store.storage_features()),
                    Err(MigrationError::MigrationBarrier(_))
                ),
                "{case}"
            );
        }
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

    fn decode_count(payload: &[u8]) -> ControlFlow<crate::Result<()>, u64> {
        ControlFlow::Continue(<u64 as StatValueCodec>::deserialize_from(payload).unwrap())
    }

    /// `COUNT` as a snapshot read sees it.
    fn snapshot_count(snapshot: &ReadSnapshot) -> crate::Result<u64> {
        let mut total = 0;
        <ReadSnapshot as BlockingScan<CountTable>>::scan_blocking(
            snapshot,
            Arc::new(KeySelection::all()),
            |(_, payload)| decode_count(payload).map_continue(|count| total += count),
        )?;
        Ok(total)
    }

    /// `COUNT` as a direct store read sees it.
    async fn store_count(store: &PartitionStore) -> crate::Result<u64> {
        let (tx, rx) = std::sync::mpsc::channel();
        <PartitionStore as Scan<CountTable>>::scan(
            store,
            Arc::new(KeySelection::all()),
            move |(_, payload)| decode_count(payload).map_continue(|count| tx.send(count).unwrap()),
        )?
        .await?;
        Ok(rx.into_iter().sum())
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
    async fn queries_admit_only_ready_instances_of_their_snapshot() {
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
        assert!(not_ready(store_count(&store).await), "store_reads_fail");
        reconcile(&mut store, maintain(&COUNT)).await;
        let maintaining = snapshot(&store);
        assert!(
            not_ready(maintaining.ready_index("count/1", IndexKind::Aggregate)),
            "maintaining_instances_are_not_ready"
        );
        assert!(not_ready(snapshot_count(&maintaining)));
        assert!(not_ready(store_count(&store).await));

        reconcile(&mut store, complete(&COUNT)).await;
        let id = registry(&store).maintained("count/1").unwrap().id;
        assert!(
            not_ready(maintaining.ready_index("count/1", IndexKind::Aggregate)),
            "readiness_is_that_of_the_snapshot"
        );
        assert!(not_ready(snapshot_count(&maintaining)));
        let ready = snapshot(&store);
        assert_eq!(
            ready.ready_index("count/1", IndexKind::Aggregate).unwrap(),
            id
        );
        assert_eq!(snapshot_count(&ready).unwrap(), SEEDED);
        assert_eq!(store_count(&store).await.unwrap(), SEEDED);
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
            "disabled_instances_are_not_ready"
        );
        assert!(not_ready(snapshot_count(&snapshot(&store))));
        assert!(not_ready(store_count(&store).await));
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

    fn registry_after(store: &PartitionStore, name: &str) -> super::super::Index {
        get_registry_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap()
            .entries()
            .iter()
            .find(|index| index.definition == name)
            .unwrap()
            .clone()
    }

    fn maintain(definition: &'static IndexDefinition) -> Requests {
        Requests {
            maintain: vec![definition],
            ..Default::default()
        }
    }

    /// Statistics here ignore the target, so updates can use any.
    const TARGET: EntryTargetRef<'static> = EntryTargetRef::Service {
        scope: None,
        service: "svc",
        handler: "run",
    };
}

#[cfg(test)]
mod open_tests {
    use bytes::Bytes;
    use tokio_util::sync::CancellationToken;

    use restate_rocksdb::RocksDbManager;
    use restate_types::identifiers::{BaseEntryId, PartitionId};
    use restate_types::partitions::Partition;
    use restate_types::sharding::KeyRange;
    use restate_types::vqueues::{EntryId, EntryKind};

    use super::*;
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::keys::EncodeTableKeyPrefix;
    use crate::stats::aggregated::backfill::tests::seed;
    use crate::{PartitionStoreManager, PartitionStoreTransaction};

    use super::super::complete::read_chunk;

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
                3,
                &CancellationToken::new(),
            )
            .await
    }

    fn maintain() -> Requests {
        Requests {
            maintain: vec![&STAT],
            ..Default::default()
        }
    }

    fn write_registry_unflushed(db: &PartitionDb, registry: &IndexRegistry) {
        let mut batch = WriteBatch::default();
        append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), registry);
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
        let registered = get_registry_from_partition_db(&db).unwrap().unwrap();
        let id = registered.maintained("stat/1").unwrap().id;

        // An entry-status key of another partition is not a cursor of this one.
        let mut outside = registered.clone();
        outside.set_cursor(id, cursor(500, 1)).unwrap();
        write_registry_unflushed(&db, &outside);
        assert!(
            reconcile(&mut store, maintain()).await.is_err(),
            "out_of_range"
        );

        // A well-formed cursor whose entry is gone resumes strictly after it.
        let mut gone = registered.clone();
        let resume = cursor(11, 0xEE);
        gone.set_cursor(id, resume.clone()).unwrap();
        write_registry_unflushed(&db, &gone);
        let remaining = read_chunk(&store, Some(&resume), usize::MAX).unwrap().len();
        assert!(remaining > 0 && remaining < 147);
        reconcile(&mut store, maintain()).await.unwrap();
        let completed = get_registry_from_partition_db(&db).unwrap().unwrap();
        assert_eq!(completed.get(id).unwrap().state, IndexStatus::Ready);
        assert_eq!(unflushed(&db), 0, "completion_is_flushed");

        // An attempt that wrote registry state but did not flush it: a retry on
        // the same physical store, with nothing left to change, still flushes.
        let mut later = completed.clone();
        later.register("other/1", IndexKind::Secondary).unwrap();
        later
            .drop_index(later.maintained("other/1").unwrap().id)
            .unwrap();
        write_registry_unflushed(&db, &later);
        drop(store);
        let mut retry = manager.open(&partition, None).await.unwrap();
        assert!(unflushed(retry.partition_db()) > 0);
        reconcile(&mut retry, maintain()).await.unwrap();
        assert_eq!(
            get_registry_from_partition_db(retry.partition_db())
                .unwrap()
                .unwrap(),
            later,
            "the retry changes nothing"
        );
        assert_eq!(
            unflushed(retry.partition_db()),
            0,
            "but flushes before publishing"
        );
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
        let mut invalid = IndexRegistry::default();
        invalid.register("stat/1", IndexKind::Aggregate).unwrap();
        invalid.next_id = 10;
        write_registry_unflushed(&db, &invalid);
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
        config.common.experimental.set_complete_status_gauges(true);
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
            get_registry_from_partition_db(&db).is_err(),
            "the_invalid_registry_is_left_as_is"
        );
        assert_eq!(read_chunk(&store, None, usize::MAX).unwrap().len(), entries);
    }
}
