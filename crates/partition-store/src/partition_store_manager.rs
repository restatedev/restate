// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::Arc;

use rocksdb::ExportImportFilesMetaData;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::PartitionStore;
use crate::cf_options;
use crate::snapshots::LocalPartitionSnapshot;
use restate_core::worker_api::SnapshotError;
use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, RocksDb, RocksDbManager, RocksError,
};
use restate_types::config::{RocksDbOptions, StorageOptions};
use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
use restate_types::live::{BoxedLiveLoad, LiveLoad};
use restate_types::logs::Lsn;

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
    lookup: Arc<Mutex<PartitionLookup>>,
    rocksdb: Arc<RocksDb>,
}

#[derive(Default, Debug)]
struct PartitionLookup {
    live: BTreeMap<PartitionId, PartitionStore>,
}

impl PartitionStoreManager {
    pub async fn create(
        mut storage_opts: impl LiveLoad<StorageOptions> + Send + 'static,
        updatable_opts: BoxedLiveLoad<RocksDbOptions>,
        initial_partition_set: &[(PartitionId, RangeInclusive<PartitionKey>)],
    ) -> Result<Self, RocksError> {
        let options = storage_opts.live_load();

        // let overall_memtables_budget = options.rocksdb_memory_budget();

        // Flush Controller respects the overall budget with no need to set per-partition budgets
        // let per_partition_memtable_budget =
        //     overall_memtables_budget / options.num_partitions_to_share_memory_budget() as usize;

        let db_spec = DbSpecBuilder::new(DbName::new(DB_NAME), options.data_dir(), db_options())
            .add_cf_pattern(
                CfPrefixPattern::new(PARTITION_CF_PREFIX),
                cf_options(), // Flush Controller will take care of adhering to memory budgets
            )
            .ensure_column_families(partition_ids_to_cfs(initial_partition_set))
            // This is added as an experiment. We might make this configurable to let users decide
            // on the trade-off between shutdown time and startup catchup time.
            .add_to_flush_on_shutdown(CfPrefixPattern::ANY)
            .build()
            .expect("valid spec");

        let manager = RocksDbManager::get();
        let rocksdb = manager.open_db(updatable_opts, db_spec).await?;

        Ok(Self {
            rocksdb,
            lookup: Arc::default(),
        })
    }

    /// Check whether we have a partition store for the given partition id, irrespective of whether
    /// the store is open or not.
    pub async fn has_partition_store(&self, partition_id: PartitionId) -> bool {
        let _guard = self.lookup.lock().await;
        let cf_name = cf_for_partition(partition_id);
        self.rocksdb.inner().cf_handle(&cf_name).is_some()
    }

    pub async fn get_partition_store(&self, partition_id: PartitionId) -> Option<PartitionStore> {
        self.lookup.lock().await.live.get(&partition_id).cloned()
    }

    pub async fn get_all_partition_stores(&self) -> Vec<PartitionStore> {
        self.lookup.lock().await.live.values().cloned().collect()
    }

    pub async fn open_partition_store(
        &self,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        open_mode: OpenMode,
        opts: &RocksDbOptions,
    ) -> Result<PartitionStore, RocksError> {
        let mut guard = self.lookup.lock().await;
        if let Some(store) = guard.live.get(&partition_id) {
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

        let partition_store = PartitionStore::new(
            self.rocksdb.clone(),
            cf_name,
            partition_id,
            partition_key_range,
        );
        guard.live.insert(partition_id, partition_store.clone());

        Ok(partition_store)
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
        let mut guard = self.lookup.lock().await;
        if guard.live.contains_key(&partition_id) {
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
        let partition_store = PartitionStore::new(
            self.rocksdb.clone(),
            cf_name,
            partition_id,
            partition_key_range,
        );
        guard.live.insert(partition_id, partition_store.clone());

        Ok(partition_store)
    }

    pub async fn export_partition_snapshot(
        &self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
        snapshot_base_path: &Path,
    ) -> Result<LocalPartitionSnapshot, SnapshotError> {
        let guard = self.lookup.lock().await;
        let mut partition_store = guard
            .live
            .get(&partition_id)
            .cloned()
            .ok_or(SnapshotError::PartitionNotFound(partition_id))?;

        partition_store
            .create_snapshot(snapshot_base_path, min_target_lsn, snapshot_id)
            .await
    }

    pub async fn drop_partition(&self, partition_id: PartitionId) {
        let mut guard = self.lookup.lock().await;
        self.rocksdb
            .inner()
            .as_raw_db()
            .drop_cf(&cf_for_partition(partition_id))
            .unwrap();

        guard.live.remove(&partition_id);
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
