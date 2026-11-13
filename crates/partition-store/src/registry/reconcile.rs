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
//! 1. **Retire** maintained indexes of definitions this binary does not know (a
//!    rollback, or another build): their keys are range-deleted and they become
//!    Dropped, keeping their IDs reserved.
//! 2. **Reset** the indexes of families whose reset is requested, the same way.
//! 3. **Register** each requested definition that has no maintained index, under
//!    a fresh ID. Running out of IDs skips the request with an error log.
//! 4. **Start** one walk for the Backfilling indexes that have none, over the
//!    entries that exist now (see [`super::backfill`]). A partition without
//!    entries has nothing to walk: those indexes become Ready instead.
//! 5. **Fence:** once anything was ever registered, the `index-registry` storage
//!    feature is in the store's ledger, so older binaries cannot open it.
//!
//! ```text
//! open ─▶ validate registry, walks, fence ─▶ retire · reset · register · start walks
//!          (an invalid state fails the open)                 │
//!                                             one batch: registry + walks + fence
//!                                                            │
//!                                              flush ─▶ install for writes ─▶ processor runs,
//!                                                                             walks run in slices
//! ```
//!
//! The changes, the walks (see [`crate::walk`]) and the fence are written in one
//! batch. The store is then flushed whenever it has any registration history,
//! even when this open changed nothing: an earlier attempt may have written
//! registry state or effects without finishing its flush. Only once the flush
//! succeeded are the reconciled indexes installed for writes, so the registry,
//! the walks and the fence are durable before any effect depends on them, and a
//! failed flush publishes nothing.
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
//! across restarts, and writes keep honoring it.
//!
//! The registry, the walks, their positions, their links and the fence walks
//! require are validated before any of this, and before any cleanup or
//! storage-feature migration of the same open (`validate_registry`).

use std::ops::RangeBounds;
use std::sync::Arc;

use rocksdb::WriteBatch;
use tracing::{error, info, warn};
use zerocopy::IntoBytes;

use restate_rocksdb::{IoMode, Priority};
use restate_storage_api::StorageError;
use restate_types::SemanticRestateVersion;
use restate_types::config::Configuration;
use restate_types::vqueues::EntryKind;

use crate::faults::{self, FaultPoint};
use crate::features::{LoadedStorageFeatures, StorageFeatures};
use crate::fsm_table::{
    append_registry_to_wb, append_walks_to_wb, get_registry_from_partition_db,
    get_walks_from_partition_db,
};
use crate::keys::{DecodeTableKey, IndexKeyPrefix};
use crate::stats::AggregateKeyPrefix;
use crate::vqueue_table::EntryStatusKey;
use crate::walk::Walks;
use crate::{MigrationError, PartitionDb, PartitionStore};

use super::{
    Family, IndexDefinition, IndexKind, IndexRegistry, IndexStatus, MaintainedIndexes,
    RegistryError, backfill,
};

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
    /// Definitions whose maintained index is dropped.
    pub(crate) reset: Vec<&'static IndexDefinition>,
}

impl Requests {
    pub(crate) fn from_config(families: &[Family], config: &Configuration) -> Self {
        let mut requests = Self::default();
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
    /// Reads and validates the registry and the walks for opening this partition:
    /// their formats and invariants, every walk endpoint and cursor against the
    /// entry-status key schema and this partition's key range,
    /// and the links between them. Walks also require the `index-registry`
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
        if !walks.entries().is_empty() && !features.is_index_registry {
            return Err(MigrationError::MigrationBarrier(
                "walks exist without the index-registry storage feature".to_owned(),
            ));
        }
        for index in registry.iter().flat_map(IndexRegistry::entries) {
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
        let (persisted, persisted_walks) = self.validate_registry(features.enabled())?;
        let mut registry = persisted.clone().unwrap_or_default();
        let mut walks = persisted_walks.clone();
        let mut batch = WriteBatch::default();

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
                Err(RegistryError::Exhausted) => {
                    error!(
                        definition = definition.name,
                        "Cannot register: all index IDs of this partition are allocated"
                    );
                }
                Err(err) => return Err(registry_error(err)),
            }
        }

        // The indexes registered just now, and any an earlier attempt left
        // without a walk, start one over the entries that exist now.
        let unstarted: Vec<_> = registry
            .entries()
            .iter()
            .filter(|index| index.state == IndexStatus::Backfilling && index.walk.is_none())
            .map(|index| index.id)
            .collect();
        if !unstarted.is_empty()
            && let Some(walk) = backfill::start(&db, &mut registry, &mut walks, &unstarted)?
        {
            info!(walk, indexes = ?unstarted, "Started the backfill of indexes");
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

        // Whatever this or an earlier attempt wrote becomes durable before
        // anything depends on it: registry changes, walks, fence, deletions,
        // effects.
        if !registry.entries().is_empty() {
            flush(&db).await?;
        }

        // Writes maintain the reconciled indexes from here on, and walks run on
        // the records as committed.
        db.install_maintained_indexes(Arc::new(MaintainedIndexes::new(&registry, &walks, known)));
        Ok(())
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
    use crate::stats::StatKind;
    use crate::stats::aggregated::backfill::tests::seed;
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
            )
            .await
    }

    fn persisted(store: &PartitionStore) -> IndexRegistry {
        get_registry_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap()
    }

    fn walks(store: &PartitionStore) -> Walks {
        get_walks_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap_or_default()
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
        assert!(get_registry_from_partition_db(&db).unwrap().is_none());
        assert!(!store.storage_features().is_index_registry);

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
        assert!(ledger.enabled().is_index_registry, "fenced_on_disk");
        assert!(
            get_min_restate_version_from_partition_db(&db)
                .unwrap()
                .is_equal_or_newer_than(&RESTATE_VERSION_1_8_2)
        );
        assert_eq!(
            store.maintained_indexes().touched_by_on_write(),
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
            store.maintained_indexes().touched_by_on_write(),
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
        assert!(store.storage_features().is_index_registry, "fence_is_kept");
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
        let mut registry = IndexRegistry::default();
        registry.register("stat/1", IndexKind::Aggregate).unwrap();
        registry.next_id = u64::from(u32::MAX) + 1;
        let write = |registry: &IndexRegistry| {
            let mut batch = WriteBatch::default();
            append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), registry);
            db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
        };
        write(&registry);

        // A request that cannot be registered is skipped; the existing index
        // stays maintained and starts its walk.
        reconcile(&mut store, &[&INDEX, &STAT], maintain(&[&INDEX, &STAT]))
            .await
            .unwrap();
        assert!(persisted(&store).maintained("index/1").is_none());
        assert_eq!(
            store.maintained_indexes().touched_by_on_write(),
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
    use restate_storage_query_api::scan::{BlockingScan, Table};
    use restate_types::identifiers::PartitionId;
    use restate_types::partition_table::Partition;
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
    use crate::walk::{Source, WalkKind, engine};
    use crate::{PartitionStoreManager, PartitionStoreTransaction, ReadSnapshot};

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

    fn registry(store: &PartitionStore) -> IndexRegistry {
        get_registry_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap()
    }

    fn walks(store: &PartitionStore) -> Walks {
        get_walks_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap_or_default()
    }

    fn registry_after(store: &PartitionStore, name: &str) -> super::super::Index {
        registry(store)
            .entries()
            .iter()
            .find(|index| index.definition == name)
            .unwrap()
            .clone()
    }

    /// Writes `registry` and `walks` as a binary would have committed them.
    fn write_state(store: &PartitionStore, registry: &IndexRegistry, walks: &Walks) {
        let db = store.partition_db();
        let mut batch = WriteBatch::default();
        append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), registry);
        append_walks_to_wb(db.cf_handle(), &mut batch, db.partition().id(), walks);
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
            store.maintained_indexes().touched_by_on_write().is_empty(),
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
                done.len() as u64,
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
            .start(WalkKind::index_backfill(), Source::EntryStatus, end)
            .unwrap();
        walks
            .advance(own, Bytes::from(entries.last().unwrap().0.clone()), 10)
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
        let persisted = registry(&store);
        let index = persisted.maintained(COUNT.name).unwrap().clone();
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let mut walks = self::walks(&store);
        walks
            .advance(index.walk.unwrap(), Bytes::from(entries[9].0.clone()), 10)
            .unwrap();
        write_state(&store, &persisted, &walks);
        add_counts(&store, index.id, 10);
        store.set_maintained_indexes(MaintainedIndexes::new(&persisted, &walks, KNOWN));

        // Covered and unlisted: touched, so its contribution is removed.
        delete(&mut store, &entries[4].1).await;
        assert_eq!(count(&store, index.id), 9);
        // Covered: touched, an update keeps it counted once.
        update(&mut store, &entries[5].1).await;
        assert_eq!(count(&store, index.id), 9);
        // Not covered and unlisted: nothing to remove.
        delete(&mut store, &entries[50].1).await;
        assert_eq!(count(&store, index.id), 9);
        // Not covered and unlisted: adopted, so it is counted and listed.
        update(&mut store, &entries[60].1).await;
        assert_eq!(count(&store, index.id), 10);

        // Resuming the walk counts each remaining entry exactly once.
        reconcile(&mut store, Requests::default()).await;
        run_walks(&mut store).await;
        assert_eq!(count(&store, index.id), SEEDED - 2);
        assert_eq!(
            registry(&store).get(index.id).unwrap().state,
            IndexStatus::Ready
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

        // Walks of COUNT and RESUMED covered the first ten and twenty entries when
        // the partition reopens with ADOPTED requested too.
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain_all(&[&COUNT, &RESUMED])).await;
        let mut persisted = registry(&store);
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let end = Bytes::from(entries.last().unwrap().0.clone());
        let mut walks = Walks::default();
        let mut paused = Vec::new();
        for (definition, covered) in [(&COUNT, 10), (&RESUMED, 20)] {
            let id = persisted.maintained(definition.name).unwrap().id;
            let walk = walks
                .start(WalkKind::index_backfill(), Source::EntryStatus, end.clone())
                .unwrap();
            let cursor = Bytes::from(entries[covered - 1].0.clone());
            walks.advance(walk, cursor, covered as u64).unwrap();
            persisted.link(id, walk).unwrap();
            add_counts(&store, id, covered as u32);
            paused.push((definition, id, walk, covered));
        }
        write_state(&store, &persisted, &walks);
        reconcile(&mut store, maintain_all(&[&COUNT, &RESUMED, &ADOPTED])).await;
        let adopted = registry(&store).maintained(ADOPTED.name).unwrap().clone();
        assert_eq!(
            (adopted.state, adopted.walk),
            (IndexStatus::Backfilling, Some(3)),
            "a_requested_index_starts_its_own_walk"
        );
        for (definition, _, walk, covered) in &paused {
            assert!(reason(&store, definition).contains(&format!(
                "is still being completed: backfill walk {walk} has examined {covered} entries"
            )));
        }
        assert!(reason(&store, &ADOPTED).contains("backfill walk 3 has examined 0 entries"));
        let started = self::walks(&store);
        assert_eq!(
            &started.entries()[..2],
            walks.entries(),
            "started_walks_wait"
        );

        // Every waiting walk's cursor still applies: an entry it covered is
        // touched without being listed, so deleting it removes its contribution.
        // The fifth entry is covered by both walks, the fifteenth by RESUMED's.
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
        try_reconcile(&mut reference, all, maintain_all(all))
            .await
            .unwrap();
        run_walks(&mut reference).await;
        try_reconcile(&mut store, all, maintain_all(all))
            .await
            .unwrap();
        let registered = registry(&store);
        assert_eq!(registered.entries().len(), all.len());
        let walk = registered.entries()[0].walk.unwrap();
        assert!(
            registered
                .entries()
                .iter()
                .all(|index| index.walk == Some(walk))
        );
        let entries = read_chunk(&store, None, usize::MAX).unwrap();
        let db = store.partition_db().clone();
        let mut tx = store.transaction();
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
                .all(|index| index.state == IndexStatus::Ready)
        );
        assert_eq!(index_rows(&store), index_rows(&reference));
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
            .advance(shared, Bytes::from(entries[9].0.clone()), 10)
            .unwrap();
        let end = walks.get(shared).unwrap().end.clone();
        let own = walks
            .start(WalkKind::index_backfill(), Source::EntryStatus, end)
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
        assert_eq!(
            self::walks(&store).get(shared).unwrap().cursor.as_deref(),
            Some(entries[9].0.as_slice())
        );
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
        let mut base = IndexRegistry::default();
        let id = base.register(COUNT.name, COUNT.kind).unwrap();
        let end = Bytes::from(read_chunk(&store, None, 1).unwrap()[0].0.clone());
        let started = |end: Bytes| {
            let mut walks = Walks::default();
            walks
                .start(WalkKind::index_backfill(), Source::EntryStatus, end)
                .unwrap();
            walks
        };
        let cases: [(&str, IndexRegistry, Walks); 3] = [
            (
                "dangling link",
                {
                    let mut registry = base.clone();
                    registry.link(id, 7).unwrap();
                    registry
                },
                Walks::default(),
            ),
            ("walk without members", base.clone(), started(end)),
            (
                "position outside the entry-status schema",
                {
                    let mut registry = base;
                    registry.link(id, 1).unwrap();
                    registry
                },
                started(Bytes::from_static(b"not a key")),
            ),
        ];
        for (case, registry, walks) in cases {
            write_state(&store, &registry, &walks);
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
    use crate::stats::StatKind;
    use crate::stats::aggregated::backfill::tests::seed;
    use crate::walk::{Source, WalkKind, engine};
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
    fn write_unflushed(db: &PartitionDb, registry: &IndexRegistry, walks: &Walks) {
        let mut batch = WriteBatch::default();
        append_registry_to_wb(db.cf_handle(), &mut batch, db.partition().id(), registry);
        append_walks_to_wb(db.cf_handle(), &mut batch, db.partition().id(), walks);
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
        assert_eq!(unflushed(&db), 0, "registration_is_flushed");

        let started = get_walks_from_partition_db(&db).unwrap().unwrap();

        // A walk positioned at an entry-status key of another partition is not
        // a walk of this one.
        let mut outside = Walks::default();
        let walk = outside
            .start(
                WalkKind::index_backfill(),
                Source::EntryStatus,
                cursor(500, 1),
            )
            .unwrap();
        outside.advance(walk, cursor(500, 1), 1).unwrap();
        let mut linked = IndexRegistry::default();
        assert_eq!(linked.register("stat/1", IndexKind::Aggregate).unwrap(), id);
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
        let completed = get_registry_from_partition_db(&db).unwrap().unwrap();
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

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconciliation_through_a_sealed_partition_changes_nothing() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::from(31), KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        seed(&mut store).await;
        reconcile(&mut store, maintain()).await.unwrap();
        let db = store.partition_db().clone();
        let registered = get_registry_from_partition_db(&db).unwrap().unwrap();
        let walks = get_walks_from_partition_db(&db).unwrap().unwrap();
        let id = registered.maintained("stat/1").unwrap().id;
        // A row of the index.
        let mut key = AggregateKeyPrefix::new(partition.id(), id)
            .as_bytes()
            .to_vec();
        key.push(StatKind::AggregatedGauge as u8);
        key.extend_from_slice(b"payload");
        let raw = db.rocksdb().inner().as_raw_db();
        raw.put_cf(db.cf_handle(), &key, b"value").unwrap();
        let installed = db.maintained_indexes().unwrap();
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
        assert_eq!(
            get_registry_from_partition_db(&db).unwrap().unwrap(),
            registered
        );
        assert_eq!(get_walks_from_partition_db(&db).unwrap().unwrap(), walks);
        assert_eq!(ledger(&db), fenced, "the_fence_stays");
        assert!(
            Arc::ptr_eq(&db.maintained_indexes().unwrap(), &installed),
            "nothing_was_published"
        );

        // A fresh open reconciles as usual.
        let mut store = manager.open(&partition, None).await.unwrap();
        reconcile(&mut store, maintain()).await.unwrap();
        assert_eq!(
            get_registry_from_partition_db(store.partition_db())
                .unwrap()
                .unwrap(),
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
        let empty = db.maintained_indexes().unwrap();
        assert!(empty.registry().entries().is_empty());
        let unpublished = |db: &PartitionDb| Arc::ptr_eq(&db.maintained_indexes().unwrap(), &empty);

        // A failed flush fails the open and publishes nothing: the registration
        // is written, but not installed.
        faults.fail(FaultPoint::RegistryFlush, RocksError::ReadOnly);
        assert!(matches!(
            reconcile(&mut store, maintain()).await,
            Err(MigrationError::StorageError(StorageError::Generic(_)))
        ));
        assert!(unpublished(&db), "nothing_was_published");
        let written = get_registry_from_partition_db(&db).unwrap().unwrap();
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
        let installed = db.maintained_indexes().unwrap();
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
        let mut invalid = IndexRegistry::default();
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
            get_registry_from_partition_db(&db).is_err(),
            "the_invalid_registry_is_left_as_is"
        );
        assert_eq!(read_chunk(&store, None, usize::MAX).unwrap().len(), entries);
    }
}
