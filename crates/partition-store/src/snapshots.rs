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
pub use self::repository::{PartitionSnapshotStatus, SnapshotReference, SnapshotRepository};
pub use self::snapshot_task::*;

use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument, warn};

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
            config.worker.snapshots.export_concurrency_limit() as usize,
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
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        use crate::metric_definitions::{
            SNAPSHOT_DOWNLOAD_DURATION, SNAPSHOT_DOWNLOAD_FAILED, SNAPSHOT_DOWNLOAD_FALLBACK,
        };

        let Some(repository) = &self.repository else {
            debug!("No snapshot repository configured");
            return Ok(None);
        };

        let start = std::time::Instant::now();
        // A transient error reading the latest-snapshot pointer is propagated as `Err` so the
        // caller fails the open and retries, rather than mistaking it for "no snapshot exists".
        let candidates = repository
            .get_snapshot_candidates(partition_id, target_lsn)
            .await?;

        if candidates.is_empty() {
            debug!("No snapshot candidates found in repository");
            return Ok(None);
        }

        let mut last_error: Option<anyhow::Error> = None;
        let mut tried_snapshot_ids: Vec<SnapshotId> = Vec::with_capacity(candidates.len());
        for (i, snapshot_ref) in candidates.iter().enumerate() {
            tried_snapshot_ids.push(snapshot_ref.snapshot_id);
            match repository.get_snapshot(partition_id, snapshot_ref).await {
                Ok(snapshot) => {
                    // Defense-in-depth: candidates were filtered using the LSN recorded in the
                    // `latest.json` index. Re-verify against the downloaded snapshot's own metadata
                    // in case the index disagrees (corruption / manual edit), and treat a mismatch
                    // as a failed candidate so we fall back to an older snapshot.
                    if let Some(min_lsn) = target_lsn
                        && snapshot.min_applied_lsn < min_lsn
                    {
                        warn!(
                            snapshot_id = %snapshot_ref.snapshot_id,
                            snapshot_lsn = %snapshot.min_applied_lsn,
                            target_lsn = %min_lsn,
                            "Downloaded snapshot LSN is below the required target; trying next candidate"
                        );
                        last_error = Some(anyhow::anyhow!(
                            "snapshot {} has min_applied_lsn {} below required target {}",
                            snapshot_ref.snapshot_id,
                            snapshot.min_applied_lsn,
                            min_lsn,
                        ));
                        continue;
                    }
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
                    return Ok(Some(snapshot));
                }
                Err(err) => {
                    warn!(
                        snapshot_id = %snapshot_ref.snapshot_id,
                        %err,
                        remaining = candidates.len() - i - 1,
                        "Snapshot download failed, trying next"
                    );
                    last_error = Some(err);
                }
            }
        }

        metrics::histogram!(SNAPSHOT_DOWNLOAD_DURATION).record(start.elapsed().as_secs_f64());
        metrics::counter!(SNAPSHOT_DOWNLOAD_FAILED).increment(1);
        let tried = tried_snapshot_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let final_err = last_error
            .unwrap_or_else(|| anyhow::anyhow!("No snapshot available"))
            .context(format!(
                "all {} available snapshot(s) failed to restore (tried: [{}])",
                candidates.len(),
                tried,
            ));
        error!(%final_err, "Exhausted all snapshot candidates");
        Err(final_err)
    }
}
