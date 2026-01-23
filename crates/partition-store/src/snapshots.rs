// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod metadata;
mod repository;
mod snapshot_task;

use std::path::Path;
use std::sync::Arc;

use crate::{PartitionDb, PartitionStore, SnapshotError, SnapshotErrorKind};

pub use self::metadata::*;
pub use self::repository::{PartitionSnapshotStatus, SnapshotRepository};
pub use self::snapshot_task::*;

use tokio::sync::Semaphore;
use tracing::{debug, instrument};

use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::Lsn;

#[derive(Clone)]
pub struct Snapshots {
    repository: Option<SnapshotRepository>,
    concurrency_limit: Arc<Semaphore>,
}

impl Snapshots {
    pub async fn create(config: &Configuration) -> anyhow::Result<Self> {
        let repository = SnapshotRepository::new_from_config(
            &config.worker.snapshots,
            config.worker.storage.snapshots_staging_dir(),
        )
        .await?;

        let concurrency_limit = Arc::new(Semaphore::new(
            config
                .worker
                .storage
                .rocksdb
                .rocksdb_max_background_jobs()
                .get() as usize,
        ));

        Ok(Self {
            repository,
            concurrency_limit,
        })
    }

    pub fn is_repository_configured(&self) -> bool {
        self.repository.is_some()
    }

    pub async fn create_local_snapshot(
        &self,
        mut partition_store: PartitionStore,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
        snapshot_base_path: &Path,
    ) -> Result<LocalPartitionSnapshot, SnapshotError> {
        let partition_id = partition_store.partition_id();

        let _permit = self
            .concurrency_limit
            .acquire()
            .await
            .expect("we never close the semaphore");

        partition_store
            .create_local_snapshot(snapshot_base_path, min_target_lsn, snapshot_id)
            .await
            .map_err(|err| SnapshotError {
                partition_id,
                kind: SnapshotErrorKind::Export(err.into()),
            })
    }

    pub async fn refresh_latest_partition_snapshot_status(
        &self,
        db: PartitionDb,
    ) -> anyhow::Result<Option<PartitionSnapshotStatus>> {
        let Some(repository) = &self.repository else {
            return Ok(None);
        };

        let partition_id = db.partition().partition_id;
        let log_id = db.partition().log_id();

        let status = match repository
            .get_latest_partition_snapshot_status(partition_id)
            .await?
        {
            Some(status) => status,
            None => PartitionSnapshotStatus::none(log_id),
        };

        let _ = db.note_archived_lsn(status.archived_lsn);
        Ok(Some(status))
    }

    #[instrument(level = "error", skip_all, fields(partition_id = %partition_id))]
    pub async fn download_latest_snapshot(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        // Attempt to get the latest available snapshot from the snapshot repository:
        let snapshot = match &self.repository {
            Some(repository) => {
                debug!("Looking for partition snapshot from which to bootstrap partition store");
                // todo(pavel): pass target LSN to repository
                repository.get_latest(partition_id).await?
            }
            None => {
                debug!("No snapshot repository configured");
                None
            }
        };
        Ok(snapshot)
    }
}
