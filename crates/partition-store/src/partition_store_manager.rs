// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ahash::HashMap;
use rocksdb::ExportImportFilesMetaData;
use rocksdb::event_listener::EventListenerExt;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tracing::{debug, info, instrument, warn};

use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, RocksDb, RocksDbManager, RocksError,
};
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::live::LiveLoadExt;
use restate_types::logs::Lsn;
use restate_types::partitions::Partition;

use crate::cf_options;
use crate::durable_lsn_tracking::{DurableLsnEventListener, DurableLsnTracker};
use crate::snapshots::{LocalPartitionSnapshot, SnapshotError, SnapshotErrorKind, Snapshots};
use crate::{BuildError, PartitionStore};

const DB_NAME: &str = "db";
const PARTITION_CF_PREFIX: &str = "data-";

type PartitionLookup = HashMap<PartitionId, PartitionStore>;

/// Controls how a partition store is opened
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenMode {
    CreateIfMissing,
    OpenExisting,
}

#[derive(Clone, derive_more::Debug)]
pub struct PartitionStoreManager {
    lookup: Arc<RwLock<PartitionLookup>>,
    #[debug(skip)]
    durable_lsns: DurableLsnTracker,
    rocksdb: Arc<RocksDb>,
    #[debug(skip)]
    snapshots: Snapshots,
}

impl PartitionStoreManager {
    pub async fn create() -> Result<Self, BuildError> {
        let mut live_config = Configuration::live();

        let config = live_config.live_load();
        let snapshots = Snapshots::create(config)
            .await
            .map_err(BuildError::Snapshots)?;

        let per_partition_memory_budget = config.worker.storage.rocksdb_memory_budget()
            / config
                .worker
                .storage
                .num_partitions_to_share_memory_budget() as usize;

        let mut db_opts = db_options();

        let event_listener = DurableLsnEventListener::default();
        let durable_lsns = event_listener.durable_lsns.clone();
        db_opts.add_event_listener(event_listener);

        let db_spec = DbSpecBuilder::new(
            DbName::new(DB_NAME),
            config.worker.storage.data_dir(),
            db_opts,
        )
        .add_cf_pattern(
            CfPrefixPattern::new(PARTITION_CF_PREFIX),
            cf_options(per_partition_memory_budget),
        )
        // This is added as an experiment. We might make this configurable to let users decide
        // on the trade-off between shutdown time and startup catchup time.
        .add_to_flush_on_shutdown(CfPrefixPattern::ANY)
        .build()
        .expect("valid spec");

        let manager = RocksDbManager::get();
        let rocksdb = manager
            .open_db(
                live_config.map(|opts| &opts.worker.storage.rocksdb),
                db_spec,
            )
            .await?;

        Ok(Self {
            lookup: Default::default(),
            durable_lsns,
            rocksdb,
            snapshots,
        })
    }

    pub fn is_repository_configured(&self) -> bool {
        self.snapshots.is_repository_configured()
    }

    pub async fn refresh_latest_archived_lsn(&self, partition_id: PartitionId) -> Option<Lsn> {
        self.snapshots
            .refresh_latest_archived_lsn(partition_id)
            .await
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

    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.cf_name()))]
    pub async fn open(
        &self,
        partition: &Partition,
        min_applied_lsn: Option<Lsn>,
    ) -> anyhow::Result<PartitionStore> {
        // todo: acquire a lock on the partition id while doing this
        let partition_store_exists = self.has_partition_store(partition.partition_id).await;

        if partition_store_exists && min_applied_lsn.is_none() {
            // We have an initialized partition store, and no fast-forward target - go on and open it.
            return Ok(self
                .open_local_partition_store(partition, OpenMode::OpenExisting)
                .await?);
        };

        // We either don't have an existing local partition store initialized - or we have a
        // fast-forward LSN target for the local state (probably due to seeing a log trim-gap).

        // Attempt to get the latest available snapshot from the snapshot repository:
        let snapshot = self
            .snapshots
            .download_latest_snapshot(partition.partition_id)
            .await?;

        Ok(match (snapshot, min_applied_lsn) {
            (None, None) => {
                debug!("No snapshot found to bootstrap partition, creating new partition store");
                self.open_local_partition_store(partition, OpenMode::CreateIfMissing)
                    .await?
            }
            (Some(snapshot), None) => {
                // Based on the assumptions for calling this method, we should only reach this point if
                // there is no existing store - we can import without first dropping the column family.
                info!("Found partition snapshot, restoring it");
                self.open_partition_store_from_snapshot(partition, snapshot)
                    .await?
            }
            (Some(snapshot), Some(fast_forward_lsn))
                if snapshot.min_applied_lsn >= fast_forward_lsn =>
            {
                // We trust that the fast_forward_lsn is greater than the locally applied LSN.
                info!(
                    latest_snapshot_lsn = %snapshot.min_applied_lsn,
                    %fast_forward_lsn,
                    "Found snapshot with LSN >= target LSN, dropping local partition store state",
                );
                self.drop_partition(partition.partition_id).await;
                self.open_partition_store_from_snapshot(partition, snapshot)
                    .await?
            }
            (maybe_snapshot, Some(fast_forward_lsn)) => {
                // Play it safe and keep the partition store intact; we can't do much else at this
                // point. We'll likely halt again as soon as the processor starts up.
                let recovery_guide_msg = "The partition's log is trimmed to a point from which this processor can not resume. \
                Visit https://docs.restate.dev/operate/clusters#handling-missing-snapshots \
                to learn more about how to recover this processor.";

                if let Some(snapshot) = maybe_snapshot {
                    warn!(
                        %snapshot.min_applied_lsn,
                        %fast_forward_lsn,
                        "The latest available snapshot is from an LSN before the target LSN! {}",
                        recovery_guide_msg,
                    );
                } else if !self.snapshots.is_repository_configured() {
                    warn!(
                        %fast_forward_lsn,
                        "A log trim gap was encountered, but no snapshot repository is configured! {}",
                        recovery_guide_msg,
                    );
                } else {
                    warn!(
                        %fast_forward_lsn,
                        "A log trim gap was encountered, but no snapshot is available for this partition! {}",
                        recovery_guide_msg,
                    );
                }

                // We expect the processor startup attempt will fail, avoid spinning too fast.
                // todo(pavel): replace this with RetryPolicy
                tokio::time::sleep(Duration::from_millis(
                    10_000 + rand::random::<u64>() % 10_000,
                ))
                .await;

                self.open_local_partition_store(partition, OpenMode::OpenExisting)
                    .await?
            }
        })
    }

    pub async fn open_local_partition_store(
        &self,
        partition: &Partition,
        open_mode: OpenMode,
    ) -> Result<PartitionStore, RocksError> {
        let guard = self.lookup.write().await;
        if let Some(store) = guard.get(&partition.partition_id) {
            return Ok(store.clone());
        }
        let cf_name = restate_rocksdb::CfName::from(partition.cf_name().as_ref());
        let already_exists = self.rocksdb.inner().cf_handle(&cf_name).is_some();
        if !already_exists {
            if open_mode == OpenMode::CreateIfMissing {
                debug!(
                    "Initializing storage for partition {}",
                    partition.partition_id
                );
                self.rocksdb
                    .clone()
                    .open_cf(
                        cf_name.clone(),
                        &Configuration::pinned().worker.storage.rocksdb,
                    )
                    .await?;
            } else {
                return Err(RocksError::AlreadyOpen);
            }
        }

        self.create_partition_store(guard, partition).await
    }

    /// Imports a partition snapshot and opens it as a partition store.
    /// The database must not have an existing column family for the partition id;
    /// it will be created based on the supplied snapshot.
    #[instrument(level = "error", skip_all, fields(partition_id = %partition.partition_id, cf_name = %partition.cf_name()))]
    pub(crate) async fn open_partition_store_from_snapshot(
        &self,
        partition: &Partition,
        snapshot: LocalPartitionSnapshot,
    ) -> Result<PartitionStore, RocksError> {
        let partition_store = {
            let guard = self.lookup.write().await;
            if guard.contains_key(&partition.partition_id) {
                warn!("The partition store is already open, refusing to import snapshot");
                return Err(RocksError::AlreadyOpen);
            }

            let cf_name = restate_rocksdb::CfName::from(partition.cf_name().as_ref());
            let cf_exists = self.rocksdb.inner().cf_handle(cf_name.as_ref()).is_some();
            if cf_exists {
                warn!(
                    "The column family for partition already exists in the database, cannot import snapshot"
                );
                return Err(RocksError::ColumnFamilyExists);
            }

            if snapshot.key_range.start() > partition.key_range.start()
                || snapshot.key_range.end() < partition.key_range.end()
            {
                warn!(
                    snapshot_range = ?snapshot.key_range,
                    partition_range = ?partition.key_range,
                    "The snapshot key range does not fully cover the partition key range"
                );
                return Err(RocksError::SnapshotKeyRangeMismatch);
            }

            let mut import_metadata = ExportImportFilesMetaData::default();
            import_metadata.set_db_comparator_name(snapshot.db_comparator_name.as_str());
            import_metadata.set_files(&snapshot.files);

            info!(
                min_lsn = %snapshot.min_applied_lsn,
                path = ?snapshot.base_dir,
                "Importing partition store snapshot"
            );

            self.rocksdb
                .clone()
                .import_cf(
                    cf_name.clone(),
                    &Configuration::pinned().worker.storage.rocksdb,
                    import_metadata,
                )
                .await?;

            assert!(self.rocksdb.inner().cf_handle(&cf_name).is_some());

            self.create_partition_store(guard, partition).await
        };

        match partition_store {
            Ok(partition_store) => {
                let res = tokio::fs::remove_dir_all(&snapshot.base_dir).await;
                if let Err(err) = res {
                    // This is not critical; since we move the SST files into RocksDB on import,
                    // at worst only the snapshot metadata file will remain in the staging dir
                    warn!(
                        partition_id = %partition.partition_id,
                        snapshot_path = %snapshot.base_dir.display(),
                        %err,
                        "Failed to remove local snapshot directory, continuing with startup",
                    );
                }
                Ok(partition_store)
            }
            Err(err) => {
                warn!(
                    partition_id = %partition.partition_id,
                    snapshot_path = %snapshot.base_dir.display(),
                    %err,
                    "Failed to import snapshot, local snapshot data retained"
                );
                Err(err)
            }
        }
    }

    /// Creates and registers a new partition store
    async fn create_partition_store(
        &self,
        mut guard: RwLockWriteGuard<'_, PartitionLookup>,
        partition: &Partition,
    ) -> Result<PartitionStore, RocksError> {
        let partition_store = PartitionStore::new(
            self.rocksdb.clone(),
            self.durable_lsns.clone(),
            restate_rocksdb::CfName::from(partition.cf_name().as_ref()),
            partition.partition_id,
            partition.key_range.clone(),
        );

        let live_store = guard.insert(partition.partition_id, partition_store.clone());
        assert!(
            live_store.is_none(),
            "create_partition_store found an open partition"
        );

        // Make sure we have a `None` LSN entry in the lookup table to allow the
        // event listener to update the durable LSN when it sees a flush.
        self.durable_lsns.insert_partition(partition.partition_id);

        Ok(partition_store)
    }

    pub async fn export_partition(
        &self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
        snapshot_base_path: &Path,
    ) -> Result<LocalPartitionSnapshot, SnapshotError> {
        // Require a lock to prevent closing/reopening stores while a snapshot is ongoing. Failure
        // to do so can lead to exporting a partially-initialized store.
        let guard = self.lookup.read().await;
        let partition_store = guard.get(&partition_id).cloned().ok_or(SnapshotError {
            partition_id,
            kind: SnapshotErrorKind::PartitionNotFound,
        })?;

        self.snapshots
            .create_local_snapshot(
                partition_store,
                min_target_lsn,
                snapshot_id,
                snapshot_base_path,
            )
            .await
    }

    pub async fn drop_partition(&self, partition_id: PartitionId) {
        let mut guard = self.lookup.write().await;
        self.rocksdb
            .inner()
            .as_raw_db()
            .drop_cf(&cf_for_partition(partition_id))
            .unwrap();

        self.durable_lsns.close_partition(partition_id);
        guard.remove(&partition_id);
    }

    #[cfg(test)]
    pub async fn close_partition_store(
        &self,
        partition_id: PartitionId,
    ) -> Result<(), restate_storage_api::StorageError> {
        let mut guard = self.lookup.write().await;
        self.durable_lsns.close_partition(partition_id);
        guard.remove(&partition_id);
        Ok(())
    }
}

fn cf_for_partition(partition_id: PartitionId) -> CfName {
    CfName::from(format!("{PARTITION_CF_PREFIX}{partition_id}"))
}

fn db_options() -> rocksdb::Options {
    let mut db_options = rocksdb::Options::default();
    // we always enable manual wal flushing in case that the user enables wal at runtime
    db_options.set_manual_wal_flush(true);

    db_options
}
