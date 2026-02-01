// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod leases;
mod metadata;
mod repository;
mod snapshot_task;

use std::path::Path;
use std::sync::Arc;

use crate::{PartitionDb, PartitionStore, SnapshotError, SnapshotErrorKind};

#[cfg(any(test, feature = "test-util"))]
pub use self::leases::NoOpLeaseManager;
pub use self::leases::{
    LEASE_DURATION, LEASE_RENEWAL_INTERVAL, LEASE_SAFETY_MARGIN, LeaseError, SnapshotLeaseGuard,
    SnapshotLeaseManager, SnapshotLeaseValue,
};
pub use self::metadata::*;
pub use self::repository::{
    LeaseProvider, PartitionSnapshotStatus, SnapshotReference, SnapshotRepository,
};
pub use self::snapshot_task::*;

use tokio::sync::Semaphore;
use tracing::{info, instrument, warn};

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
        let repository = SnapshotRepository::new_read_only_from_config(
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

    #[cfg(any(test, feature = "test-util"))]
    pub async fn create_with_stub_leases(config: &Configuration) -> anyhow::Result<Self> {
        let repository = SnapshotRepository::new_from_config_with_stub_leases(
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
    pub async fn download_snapshot(
        &self,
        partition_id: PartitionId,
        target_lsn: Option<Lsn>,
    ) -> anyhow::Result<LocalPartitionSnapshot> {
        use crate::metric_definitions::{
            SNAPSHOT_DOWNLOAD_DURATION, SNAPSHOT_DOWNLOAD_FAILED, SNAPSHOT_DOWNLOAD_FALLBACK,
        };

        let Some(repository) = &self.repository else {
            anyhow::bail!("No snapshot repository configured");
        };

        let start = std::time::Instant::now();
        let candidates = repository
            .get_snapshot_candidates(partition_id, target_lsn)
            .await?;

        if candidates.is_empty() {
            anyhow::bail!("No snapshot candidates found");
        }

        let mut download_error: Option<anyhow::Error> = None;
        for (i, snapshot_ref) in candidates.iter().enumerate() {
            match repository.get_snapshot(partition_id, snapshot_ref).await {
                Ok(snapshot) => {
                    metrics::histogram!(SNAPSHOT_DOWNLOAD_DURATION)
                        .record(start.elapsed().as_secs_f64());
                    if i > 0 {
                        metrics::counter!(SNAPSHOT_DOWNLOAD_FALLBACK).increment(1);
                        info!(
                            snapshot_id = %snapshot_ref.snapshot_id,
                            attempt = i + 1,
                            "Restored from fallback snapshot"
                        );
                    }
                    return Ok(snapshot);
                }
                Err(err) => {
                    warn!(
                        snapshot_id = %snapshot_ref.snapshot_id,
                        %err,
                        remaining = candidates.len() - i - 1,
                        "Snapshot download failed, trying next"
                    );
                    download_error = Some(err);
                }
            }
        }

        metrics::histogram!(SNAPSHOT_DOWNLOAD_DURATION).record(start.elapsed().as_secs_f64());
        metrics::counter!(SNAPSHOT_DOWNLOAD_FAILED).increment(1);
        Err(download_error
            .unwrap_or_else(|| anyhow::anyhow!("No snapshot available"))
            .context("all available snapshots failed to restore"))
    }
}
