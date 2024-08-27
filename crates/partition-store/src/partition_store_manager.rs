// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use std::sync::Arc;

use restate_types::live::BoxedLiveLoad;
use tokio::sync::Mutex;
use tracing::debug;

use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, RocksDb, RocksDbManager, RocksError,
};
use restate_types::config::{data_dir, RocksDbOptions};
use restate_types::config::StorageOptions;
use restate_types::identifiers::PartitionId;
use restate_types::identifiers::PartitionKey;
use restate_types::live::LiveLoad;

use crate::cf_options;
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

#[derive(Clone, derive_more::Debug)]
pub struct PartitionStoreManager {
    lookup: Arc<Mutex<PartitionLookup>>,
    // rocksdb: Arc<RocksDb>,
    // raw_db: Arc<DB>,
    storage_opts: StorageOptions,
    #[debug(skip)]
    updateable_opts: BoxedLiveLoad<RocksDbOptions>,
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
    ) -> std::result::Result<Self, RocksError> {
        // let options = storage_opts.live_load();
        //
        // let per_partition_memory_budget = options.rocksdb_memory_budget()
        //     / options.num_partitions_to_share_memory_budget() as usize;
        //
        // let db_spec = DbSpecBuilder::new(DbName::new(DB_NAME), options.data_dir(), db_options())
        //     .add_cf_pattern(
        //         CfPrefixPattern::new(PARTITION_CF_PREFIX),
        //         cf_options(per_partition_memory_budget),
        //     )
        //     .ensure_column_families(partition_ids_to_cfs(initial_partition_set))
        //     .build()
        //     .expect("valid spec");
        //
        // let manager = RocksDbManager::get();
        // let raw_db = manager.open_db(updateable_opts.clone(), db_spec).await?;
        //
        // let rocksdb = manager.get_db(DbName::new(DB_NAME)).unwrap();

        Ok(Self {
            // raw_db,
            // rocksdb,
            lookup: Arc::default(),
            updateable_opts,
            storage_opts: storage_opts.live_load().clone(),
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
    ) -> std::result::Result<PartitionStore, RocksError> {
        let mut guard = self.lookup.lock().await;
        if let Some(store) = guard.live.get(&partition_id) {
            return Ok(store.clone());
        }

        let db_manager = RocksDbManager::get();

        let per_partition_memory_budget = self.storage_opts.rocksdb_memory_budget()
            / self.storage_opts.num_partitions_to_share_memory_budget() as usize;

        let path = data_dir(&format!("db-{partition_id}"));

        let db_name = DbName::from_string(format!("db-{partition_id}"));
        let db_spec = DbSpecBuilder::new(db_name.clone(), path, db_options())
            .add_cf_pattern(
                CfPrefixPattern::new(PARTITION_CF_PREFIX),
                cf_options(per_partition_memory_budget),
            )
            .ensure_column_families(partition_ids_to_cfs::<RangeInclusive<PartitionKey>>(&[]))
            .build()
            .expect("valid spec");

        let raw_db = db_manager.open_db(self.updateable_opts.clone(), db_spec).await?;
        let rocksdb = db_manager.get_db(db_name).unwrap();

        let cf_name = cf_for_partition(partition_id);
        let already_exists = rocksdb.inner().cf_handle(&cf_name).is_some();

        if !already_exists {
            if open_mode == OpenMode::CreateIfMissing {
                debug!("Initializing storage for partition {}", partition_id);
                rocksdb.open_cf(cf_name.clone(), opts).await?;
            } else {
                return Err(RocksError::AlreadyOpen);
            }
        }

        let partition_store = PartitionStore::new(
            raw_db,
            rocksdb,
            cf_name,
            partition_id,
            partition_key_range,
        );
        guard.live.insert(partition_id, partition_store.clone());

        Ok(partition_store)
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
