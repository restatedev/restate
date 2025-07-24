// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use crate::PartitionStore;

pub use self::metadata::*;
pub use self::repository::SnapshotRepository;
pub use self::snapshot_task::*;

use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::{Lsn, SequenceNumber};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

#[derive(Debug, derive_more::Display)]
#[display("{kind} for partition {partition_id}")]
pub struct SnapshotError {
    pub partition_id: PartitionId,
    pub kind: SnapshotErrorKind,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotErrorKind {
    #[error("Partition not found")]
    PartitionNotFound,
    #[error("Snapshot export in progress")]
    SnapshotInProgress,
    #[error("Partition Processor state does not permit snapshotting")]
    InvalidState,
    #[error("Snapshot repository is not configured")]
    RepositoryNotConfigured,
    #[error("Snapshot export failed for partition")]
    Export(#[source] anyhow::Error),
    #[error("Snapshot repository IO error")]
    RepositoryIo(#[source] anyhow::Error),
    #[error("Internal error")]
    Internal(anyhow::Error),
}

#[derive(Clone)]
pub struct Snapshots {
    repository: Option<SnapshotRepository>,
    concurrency_limit: Arc<Semaphore>,
}

impl Snapshots {
    pub async fn create(config: &Configuration) -> anyhow::Result<Self> {
        let repository = SnapshotRepository::create_if_configured(
            &config.worker.snapshots,
            config.worker.storage.snapshots_staging_dir(),
            config.common.cluster_name().to_owned(),
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

    pub async fn refresh_latest_archived_lsn(&self, partition_id: PartitionId) -> Option<Lsn> {
        let Some(repository) = &self.repository else {
            return None;
        };

        let archived_lsn = repository
            .get_latest_archived_lsn(partition_id)
            .await
            .inspect(|lsn| debug!(?partition_id, "Latest archived LSN: {}", lsn))
            .inspect_err(|err| warn!(?partition_id, "Unable to get latest archived LSN: {}", err))
            .ok()
            .unwrap_or(Lsn::INVALID);
        // todo: update partition_store archived lsn
        Some(archived_lsn)
    }

    pub async fn download_latest_snapshot(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        // Attempt to get the latest available snapshot from the snapshot repository:
        let snapshot = match &self.repository {
            Some(repository) => {
                debug!(
                    partition_id = %partition_id,
                    "Looking for partition snapshot from which to bootstrap partition store"
                );
                // todo(pavel): pass target LSN to repository
                repository.get_latest(partition_id).await?
            }
            None => {
                debug!(
                    %partition_id,
                    "No snapshot repository configured"
                );
                None
            }
        };
        Ok(snapshot)
    }
}
