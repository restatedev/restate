// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;

use ahash::HashMap;
use rocksdb::ExportImportFilesMetaData;
use rocksdb::event_listener::EventListenerExt;
use tokio::sync::{RwLock, RwLockWriteGuard, Semaphore};
use tracing::{debug, error, info, warn};

use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, RocksDb, RocksDbManager, RocksError,
};
use restate_storage_api::fsm_table::ReadOnlyFsmTable;
use restate_types::config::{RocksDbOptions, StorageOptions};
use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
use restate_types::live::LiveLoad;
use restate_types::live::LiveLoadExt;
use restate_types::logs::Lsn;
use restate_types::logs::SequenceNumber;

use crate::PartitionStore;
use crate::cf_options;
use crate::durable_lsn_tracking::{DurableLsnEventListener, DurableLsnLookup};
use crate::snapshots::{LocalPartitionSnapshot, SnapshotError, SnapshotErrorKind};

const DB_NAME: &str = "db";
const PARTITION_CF_PREFIX: &str = "data-";

/// Controls how a partition store is opened
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenMode {
    CreateIfMissing,
    OpenExisting,
}

#[derive(Clone, Debug)]
pub struct PartitionStoreManager {
    lookup: Arc<RwLock<PartitionLookup>>,
    durable_lsns: Arc<DurableLsnLookup>,
    rocksdb: Arc<RocksDb>,
    snapshot_limiter: Arc<Semaphore>,
}

type PartitionLookup = HashMap<PartitionId, PartitionStore>;

impl PartitionStoreManager {
    pub async fn create(
        mut storage_opts: impl LiveLoad<Live = StorageOptions> + 'static,
        initial_partition_set: &[(PartitionId, RangeInclusive<PartitionKey>)],
    ) -> Result<Self, RocksError> {
        let options = storage_opts.live_load();

        let per_partition_memory_budget = options.rocksdb_memory_budget()
            / options.num_partitions_to_share_memory_budget() as usize;

        let snapshot_limiter =
            Semaphore::new(options.rocksdb.rocksdb_max_background_jobs().get() as usize);

        let mut db_opts = db_options();

        let event_listener = DurableLsnEventListener::default();
        let durable_lsns = event_listener.durable_lsns.clone();
        db_opts.add_event_listener(event_listener);

        let db_spec = DbSpecBuilder::new(DbName::new(DB_NAME), options.data_dir(), db_opts)
            .add_cf_pattern(
                CfPrefixPattern::new(PARTITION_CF_PREFIX),
                cf_options(per_partition_memory_budget),
            )
            .ensure_column_families(partition_ids_to_cfs(initial_partition_set))
            // This is added as an experiment. We might make this configurable to let users decide
            // on the trade-off between shutdown time and startup catchup time.
            .add_to_flush_on_shutdown(CfPrefixPattern::ANY)
            .build()
            .expect("valid spec");

        let manager = RocksDbManager::get();
        let rocksdb = manager
            .open_db(storage_opts.map(|opts| &opts.rocksdb), db_spec)
            .await?;

        Ok(Self {
            lookup: Default::default(),
            durable_lsns,
            rocksdb,
            snapshot_limiter: Arc::new(snapshot_limiter),
        })
    }

    /// Check whether we have a partition store for the given partition id, irrespective of whether
    /// the store is open or not.
    pub async fn has_partition_store(&self, partition_id: PartitionId) -> bool {
        // Serializes with open/close operations; the lookup table is the source of truth for PSM,
        // not just the underlying column family.
        let _guard = self.lookup.read().await;
        let cf_name = cf_for_partition(partition_id);
        self.rocksdb.inner().cf_handle(&cf_name).is_some()
    }

    pub async fn get_partition_store(&self, partition_id: PartitionId) -> Option<PartitionStore> {
        self.lookup.read().await.get(&partition_id).cloned()
    }

    pub async fn open_partition_store(
        &self,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        open_mode: OpenMode,
        opts: &RocksDbOptions,
    ) -> Result<PartitionStore, RocksError> {
        let guard = self.lookup.write().await;
        if let Some(store) = guard.get(&partition_id) {
            return Ok(store.clone());
        }
        let cf_name = cf_for_partition(partition_id);
        let already_exists = self.rocksdb.inner().cf_handle(&cf_name).is_some();
        if !already_exists {
            if open_mode == OpenMode::CreateIfMissing {
                debug!("Initializing storage for partition {}", partition_id);
                self.rocksdb.open_cf(cf_name.clone(), opts).await?;
            } else {
                return Err(RocksError::AlreadyOpen);
            }
        }

        self.create_partition_store(guard, partition_id, partition_key_range, cf_name)
            .await
    }

    /// Imports a partition snapshot and opens it as a partition store.
    /// The database must not have an existing column family for the partition id;
    /// it will be created based on the supplied snapshot.
    pub async fn open_partition_store_from_snapshot(
        &self,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        snapshot: LocalPartitionSnapshot,
        opts: &RocksDbOptions,
    ) -> Result<PartitionStore, RocksError> {
        let guard = self.lookup.write().await;
        if guard.contains_key(&partition_id) {
            warn!(
                %partition_id,
                "The partition store is already open, refusing to import snapshot"
            );
            return Err(RocksError::AlreadyOpen);
        }

        let cf_name = cf_for_partition(partition_id);
        let cf_exists = self.rocksdb.inner().cf_handle(&cf_name).is_some();
        if cf_exists {
            warn!(
                %partition_id,
                %cf_name,
                "The column family for partition already exists in the database, cannot import snapshot"
            );
            return Err(RocksError::ColumnFamilyExists);
        }

        if snapshot.key_range.start() > partition_key_range.start()
            || snapshot.key_range.end() < partition_key_range.end()
        {
            warn!(
                %partition_id,
                snapshot_range = ?snapshot.key_range,
                partition_range = ?partition_key_range,
                "The snapshot key range does not fully cover the partition key range"
            );
            return Err(RocksError::SnapshotKeyRangeMismatch);
        }

        let mut import_metadata = ExportImportFilesMetaData::default();
        import_metadata.set_db_comparator_name(snapshot.db_comparator_name.as_str());
        import_metadata.set_files(&snapshot.files);

        info!(
            %partition_id,
            min_lsn = %snapshot.min_applied_lsn,
            path = ?snapshot.base_dir,
            "Importing partition store snapshot"
        );

        self.rocksdb
            .import_cf(cf_name.clone(), opts, import_metadata)
            .await?;

        assert!(self.rocksdb.inner().cf_handle(&cf_name).is_some());
        self.create_partition_store(guard, partition_id, partition_key_range, cf_name)
            .await
    }

    /// Creates and registers a new partition store
    async fn create_partition_store(
        &self,
        mut guard: RwLockWriteGuard<'_, PartitionLookup>,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        cf_name: CfName,
    ) -> Result<PartitionStore, RocksError> {
        let mut partition_store = PartitionStore::new(
            self.rocksdb.clone(),
            cf_name,
            partition_id,
            partition_key_range,
        );

        let live_store = guard.insert(partition_id, partition_store.clone());
        assert!(
            live_store.is_none(),
            "create_partition_store found an open partition"
        );

        let durable_lsn = match partition_store.get_locally_durable_lsn().await {
            Ok(Some(durable_lsn)) => durable_lsn,
            Ok(None) => Lsn::INVALID,
            Err(err) => {
                error!(
                    %partition_id,
                    "Failed reading the (durable) applied LSN for partition store: {}", err
                );
                Lsn::INVALID
            }
        };
        self.durable_lsns.insert(partition_id, durable_lsn);

        Ok(partition_store)
    }

    pub fn get_durable_lsn(&self, partition_id: PartitionId) -> Option<Lsn> {
        self.durable_lsns.get(&partition_id).map(|lsn| *lsn)
    }

    pub async fn export_partition_snapshot(
        &self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
        snapshot_base_path: &Path,
    ) -> Result<LocalPartitionSnapshot, SnapshotError> {
        // Require a lock to prevent closing/reopening stores while a snapshot is ongoing. Failure
        // to do so can lead to exporting a partially-initialized store.
        let guard = self.lookup.read().await;
        let mut partition_store = guard.get(&partition_id).cloned().ok_or(SnapshotError {
            partition_id,
            kind: SnapshotErrorKind::PartitionNotFound,
        })?;

        let _permit = self
            .snapshot_limiter
            .acquire()
            .await
            .expect("we never close the semaphore");
        partition_store
            .create_snapshot(snapshot_base_path, min_target_lsn, snapshot_id)
            .await
            .map_err(|err| SnapshotError {
                partition_id,
                kind: SnapshotErrorKind::Export(err.into()),
            })
    }

    pub async fn drop_partition(&self, partition_id: PartitionId) {
        let mut guard = self.lookup.write().await;
        self.rocksdb
            .inner()
            .as_raw_db()
            .drop_cf(&cf_for_partition(partition_id))
            .unwrap();

        guard.remove(&partition_id);
        self.durable_lsns.remove(&partition_id);
    }

    #[cfg(test)]
    pub async fn close_partition_store(
        &self,
        partition_id: PartitionId,
    ) -> Result<(), restate_storage_api::StorageError> {
        let mut guard = self.lookup.write().await;
        self.durable_lsns.remove(&partition_id);
        guard.remove(&partition_id);
        Ok(())
    }
}

fn cf_for_partition(partition_id: PartitionId) -> CfName {
    CfName::from(format!("{PARTITION_CF_PREFIX}{partition_id}"))
}

#[inline]
fn partition_ids_to_cfs<T>(partition_ids: &[(PartitionId, T)]) -> Vec<CfName> {
    partition_ids
        .iter()
        .map(|(partition, _)| cf_for_partition(*partition))
        .collect()
}

fn db_options() -> rocksdb::Options {
    let mut db_options = rocksdb::Options::default();
    // we always enable manual wal flushing in case that the user enables wal at runtime
    db_options.set_manual_wal_flush(true);

    db_options
}
