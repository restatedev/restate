// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::DerefMut;
use std::sync::Arc;

use parking_lot::RwLock;
use rocksdb::{BoundColumnFamily, ExportImportFilesMetaData};
use tokio::sync::{RwLock as AsyncRwLock, watch};
use tracing::{debug, info, instrument, warn};

use restate_core::ShutdownError;
use restate_rocksdb::{RocksDb, RocksError};
use restate_types::config::Configuration;
use restate_types::logs::Lsn;
use restate_types::partitions::{CfName, Partition};

use crate::snapshots::LocalPartitionSnapshot;

#[derive(Clone)]
pub struct PartitionDb {
    meta: Arc<Partition>,
    durable_lsn: watch::Sender<Option<Lsn>>,
    archived_lsn: watch::Sender<Option<Lsn>>,
    // Note: Rust will drop the fields in the order they are declared in the struct.
    // It's crucial to keep the column family and the database in this exact order.
    cf: PartitionBoundCfHandle,
    rocksdb: Arc<RocksDb>,
}

impl PartitionDb {
    pub(crate) fn new(
        meta: Arc<Partition>,
        archived_lsn: watch::Sender<Option<Lsn>>,
        rocksdb: Arc<RocksDb>,
        cf: Arc<BoundColumnFamily<'_>>,
    ) -> Self {
        Self {
            meta,
            durable_lsn: watch::Sender::new(None),
            archived_lsn,
            cf: PartitionBoundCfHandle::new(cf),
            rocksdb,
        }
    }

    pub fn partition(&self) -> &Arc<Partition> {
        &self.meta
    }

    pub fn rocksdb(&self) -> &Arc<RocksDb> {
        &self.rocksdb
    }

    pub fn cf_handle(&self) -> &Arc<BoundColumnFamily> {
        &self.cf.0
    }

    pub(crate) fn note_archived_lsn(&self, lsn: Lsn) -> bool {
        self.archived_lsn.send_if_modified(|current| {
            if current.is_none_or(|c| lsn > c) {
                *current = Some(lsn);
                true
            } else {
                false
            }
        })
    }

    /// The last (locally) known archived LSN for this partition
    pub fn get_archived_lsn(&self) -> Option<Lsn> {
        *self.archived_lsn.borrow()
    }

    pub fn watch_archived_lsn(&self) -> watch::Receiver<Option<Lsn>> {
        self.archived_lsn.subscribe()
    }

    pub(crate) fn durable_lsn_sender(&self) -> &watch::Sender<Option<Lsn>> {
        &self.durable_lsn
    }
}

#[derive(Clone)]
pub(crate) struct PartitionBoundCfHandle(Arc<BoundColumnFamily<'static>>);

impl PartitionBoundCfHandle {
    pub fn new(cf: Arc<BoundColumnFamily<'_>>) -> Self {
        // SAFETY: the new BoundColumnFamily here just expanding lifetime to static,
        // so that we can re-bind the lifetime to PartitionDb/PartitionStore instead of
        // the database.
        let static_cf = unsafe { Arc::from_raw(Arc::into_raw(cf).cast()) };
        Self(static_cf)
    }
}

pub(crate) struct PartitionCell {
    meta: Arc<Partition>,
    #[allow(dead_code)]
    archived_lsn: watch::Sender<Option<Lsn>>,
    durable_lsn: RwLock<Option<watch::Sender<Option<Lsn>>>>,
    pub(crate) inner: AsyncRwLock<State>,
}

impl PartitionCell {
    pub(crate) fn new(partition: Partition) -> Self {
        Self {
            meta: Arc::new(partition),
            archived_lsn: Default::default(),
            durable_lsn: Default::default(),
            inner: AsyncRwLock::new(State::Unknown),
        }
    }

    pub(crate) fn cf_name(&self) -> CfName {
        self.meta.cf_name()
    }

    fn open_local_cf(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>, db: PartitionDb) {
        let mut durable_lsn_guard = self.durable_lsn.write();
        *durable_lsn_guard = Some(db.durable_lsn_sender().clone());
        **guard = State::Open { db };
    }

    pub fn set_cf_missing(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>) {
        let mut durable_lsn_guard = self.durable_lsn.write();
        **guard = State::CfMissing;
        if let Some(durable_lsn) = durable_lsn_guard.take() {
            durable_lsn.send_replace(None);
        }
    }

    pub fn open_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        rocksdb: &Arc<RocksDb>,
    ) {
        let cf_name = self.cf_name();
        match rocksdb.inner().cf_handle(cf_name.as_ref()) {
            Some(handle) => {
                let db = PartitionDb::new(
                    self.meta.clone(),
                    self.archived_lsn.clone(),
                    rocksdb.clone(),
                    handle,
                );
                self.open_local_cf(guard, db);
            }
            None => {
                self.set_cf_missing(guard);
            }
        }
    }

    // low-level opening of a column family.
    //
    // Note: This doesn't check whether the column family exists or not
    #[instrument(level = "error", skip_all, fields(partition_id = %self.meta.partition_id, cf_name = %self.meta.cf_name()))]
    pub async fn create_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        rocksdb: Arc<RocksDb>,
    ) -> Result<PartitionDb, RocksError> {
        let cf_name = self.meta.cf_name();
        debug!("Creating new column family {}", cf_name);
        rocksdb
            .clone()
            .open_cf(
                self.meta.cf_name().into(),
                &Configuration::pinned().worker.storage.rocksdb,
            )
            .await?;
        let handle = rocksdb
            .inner()
            .cf_handle(cf_name.as_ref())
            .expect("cf must be open");
        let db = PartitionDb::new(
            self.meta.clone(),
            self.archived_lsn.clone(),
            rocksdb.clone(),
            handle,
        );
        self.open_local_cf(guard, db.clone());
        Ok(db)
    }

    // low-level importing a column family from a locally downloaded a snapshot
    //
    // Note: This doesn't check whether the column family exists or not
    #[instrument(level = "error", skip_all, fields(partition_id = %self.meta.partition_id, cf_name = %self.meta.cf_name(), path = %snapshot.base_dir.display()))]
    pub async fn import_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
        snapshot: LocalPartitionSnapshot,
        rocksdb: Arc<RocksDb>,
    ) -> Result<PartitionDb, RocksError> {
        // Sanity check
        if snapshot.key_range.start() > self.meta.key_range.start()
            || snapshot.key_range.end() < self.meta.key_range.end()
        {
            warn!(
                snapshot_range = ?snapshot.key_range,
                partition_range = ?self.meta.key_range,
                "The snapshot key range does fully cover the partition key range"
            );
            return Err(RocksError::SnapshotKeyRangeMismatch);
        }

        let mut import_metadata = ExportImportFilesMetaData::default();
        import_metadata.set_db_comparator_name(snapshot.db_comparator_name.as_str());
        import_metadata.set_files(&snapshot.files);

        info!(
            snapshot_applied_lsn = %snapshot.min_applied_lsn,
            path = ?snapshot.base_dir,
            "Importing partition store snapshot"
        );

        rocksdb
            .clone()
            .import_cf(
                self.meta.cf_name().into(),
                &Configuration::pinned().worker.storage.rocksdb,
                import_metadata,
            )
            .await?;

        if let Err(err) = tokio::fs::remove_dir_all(&snapshot.base_dir).await {
            // This is not critical; since we move the SST files into RocksDB on import,
            // at worst only the snapshot metadata file will remain in the staging dir
            warn!(
                %err,
                "Failed to remove local snapshot directory, continuing with startup",
            );
        };

        let db = PartitionDb::new(
            self.meta.clone(),
            self.archived_lsn.clone(),
            rocksdb.clone(),
            rocksdb
                .inner()
                .cf_handle(self.meta.cf_name().as_ref())
                .expect("cf must exist after import"),
        );

        self.open_local_cf(guard, db.clone());
        Ok(db)
    }

    /// Deletes the underlying column famil(ies) and closes the [`PartitionDb`].
    pub async fn drop_cf(
        &self,
        guard: &mut tokio::sync::RwLockWriteGuard<'_, State>,
    ) -> Result<(), RocksError> {
        // We set the state to Unknown in case we returned an error during the drop process.
        let state = std::mem::replace(guard.deref_mut(), State::Unknown);
        match state {
            State::Unknown => return Ok(()),
            State::CfMissing => { /* nothing to do.*/ }
            State::Open { db } => {
                let db = Arc::clone(&db.rocksdb);
                let cf_name = self.meta.cf_name().clone();

                // if dropping failed. We leave the column family closed but mark it as "unknown"
                tokio::task::spawn_blocking(move || {
                    db.inner().as_raw_db().drop_cf(cf_name.as_ref())
                })
                .await
                .map_err(|_| RocksError::Shutdown(ShutdownError))??;
                debug!("Column family {} dropped", self.meta.cf_name());
            }
        }
        self.set_cf_missing(guard);
        Ok(())
    }

    /// Clone the underlying [`PartitionDb`], if it is open.
    pub async fn clone_db(&self) -> Option<PartitionDb> {
        let guard = self.inner.read().await;
        match &*guard {
            State::Unknown | State::CfMissing => None,
            State::Open { db } => Some(db.clone()),
        }
    }

    /// Updates the durable Lsn of the local partition store.
    pub fn note_durable_lsn(&self, lsn: Lsn) {
        let durable_lsn_guard = self.durable_lsn.read();
        if let Some(durable_lsn) = durable_lsn_guard.as_ref() {
            durable_lsn.send_replace(Some(lsn));
        }
    }

    #[cfg(test)]
    pub async fn reset_to_unknown(&self, guard: &mut tokio::sync::RwLockWriteGuard<'_, State>) {
        let mut durable_lsn_guard = self.durable_lsn.write();
        **guard = State::Unknown;
        if let Some(durable_lsn) = durable_lsn_guard.take() {
            durable_lsn.send_replace(None);
        }
    }
}

#[derive(Default)]
pub(crate) enum State {
    /// The state of local column family is still unknown.
    ///
    /// The column family may or may not exist locally and we will only know
    /// this after an attempt to opening it.
    #[default]
    Unknown,
    /// The column famil(ies) for this partition does not exist locally
    CfMissing,
    Open {
        db: PartitionDb,
    },
}

impl State {
    pub fn is_unknown(&self) -> bool {
        matches!(self, State::Unknown)
    }

    pub fn get_db(&self) -> Option<&PartitionDb> {
        match self {
            State::Unknown | State::CfMissing => None,
            State::Open { db } => Some(db),
        }
    }
}
