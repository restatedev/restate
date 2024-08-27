// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::live::BoxedLiveLoad;
use rocksdb::metadata::ExportImportFilesMetaData;
use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, RocksDb, RocksDbManager, RocksError,
};
use restate_types::config::RocksDbOptions;
use restate_types::config::StorageOptions;
use restate_types::identifiers::PartitionId;
use restate_types::identifiers::PartitionKey;
use restate_types::live::LiveLoad;

use crate::cf_options;
use crate::snapshots::LocalPartitionSnapshot;
use crate::PartitionStore;
use crate::DB;

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
    raw_db: Arc<DB>,
}

#[derive(Default, Debug)]
struct PartitionLookup {
    live: BTreeMap<PartitionId, PartitionStore>,
}

impl PartitionStoreManager {
    pub async fn create(
        mut storage_opts: impl LiveLoad<StorageOptions> + Send + 'static,
        updateable_opts: BoxedLiveLoad<RocksDbOptions>,
        initial_partition_set: &[(PartitionId, RangeInclusive<PartitionKey>)],
    ) -> Result<Self, RocksError> {
        let options = storage_opts.live_load();

        let per_partition_memory_budget = options.rocksdb_memory_budget()
            / options.num_partitions_to_share_memory_budget() as usize;

        let db_spec = DbSpecBuilder::new(DbName::new(DB_NAME), options.data_dir(), db_options())
            .add_cf_pattern(
                CfPrefixPattern::new(PARTITION_CF_PREFIX),
                cf_options(per_partition_memory_budget),
            )
            .ensure_column_families(partition_ids_to_cfs(initial_partition_set))
            .build()
            .expect("valid spec");

        let manager = RocksDbManager::get();
        let raw_db = manager.open_db(updateable_opts, db_spec).await?;

        let rocksdb = manager.get_db(DbName::new(DB_NAME)).unwrap();

        Ok(Self {
            raw_db,
            rocksdb,
            lookup: Arc::default(),
        })
    }

    pub async fn has_partition(&self, partition_id: PartitionId) -> bool {
        let guard = self.lookup.lock().await;
        guard.live.contains_key(&partition_id)
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
            self.raw_db.clone(),
            self.rocksdb.clone(),
            cf_name,
            partition_id,
            partition_key_range,
        );
        guard.live.insert(partition_id, partition_store.clone());

        Ok(partition_store)
    }

    pub async fn create_partition_store_from_snapshot(
        &self,
        partition_id: PartitionId,
        partition_key_range: RangeInclusive<PartitionKey>,
        snapshot: LocalPartitionSnapshot,
        opts: &RocksDbOptions,
    ) -> Result<PartitionStore, RocksError> {
        let mut guard = self.lookup.lock().await;
        if let Some(store) = guard.live.get(&partition_id) {
            return Ok(store.clone());
        }
        let cf_name = cf_for_partition(partition_id);
        let already_exists = self.rocksdb.inner().cf_handle(&cf_name).is_some();
        if already_exists {
            warn!(?partition_id, "already exists, refusing to import snapshot");
            return Err(RocksError::AlreadyOpen);
        }

        let mut import_metadata = ExportImportFilesMetaData::default();
        import_metadata.set_db_comparator_name(snapshot.db_comparator_name.as_str());
        import_metadata.set_files(&snapshot.files);

        info!(?partition_id, ?snapshot.minimum_lsn, "importing partition from snapshot");
        let result = self
            .rocksdb
            .import_cf(cf_name.clone(), opts, import_metadata)
            .await;

        if result.is_err() {
            error!(?partition_id, "failed to import snapshot");
            return Err(result.unwrap_err());
        }

        assert!(self.rocksdb.inner().cf_handle(&cf_name).is_some());

        let partition_store = PartitionStore::new(
            self.raw_db.clone(),
            self.rocksdb.clone(),
            cf_name,
            partition_id,
            partition_key_range,
        );
        guard.live.insert(partition_id, partition_store.clone());

        Ok(partition_store)
    }

    // #[cfg(test)]
    pub async fn drop_partition(&self, partition_id: PartitionId) {
        let mut guard = self.lookup.lock().await;
        self.raw_db
            .drop_cf(&cf_for_partition(partition_id))
            .unwrap();
        guard.live.remove(&partition_id);
    }
}

fn cf_for_partition(partition_id: PartitionId) -> CfName {
    CfName::from(format!("{}{}", PARTITION_CF_PREFIX, partition_id))
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
