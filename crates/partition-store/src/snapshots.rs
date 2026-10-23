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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{PartitionDb, PartitionStore, SnapshotError, SnapshotErrorKind};

pub use self::metadata::*;
pub use self::repository::{PartitionSnapshotStatus, SnapshotRepository};
pub use self::snapshot_task::*;

use tokio::sync::Semaphore;
use tracing::{Instrument, debug, info, instrument, warn};

use restate_types::config::SnapshotsOptions;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::Lsn;

/// Removes local snapshot exports left under `export_dir` by a previous run, e.g. after a crash
/// mid-upload. Exports are hard links that pin SSTs on disk until deleted.
///
/// Must only be called at startup, before any export can be in flight. The leftovers found now are
/// deleted in the background, so that startup does not wait on unlinking thousands of files; new
/// exports cannot collide with them because snapshot ids are never reused. Only directories at the
/// export location are removed; anything else is reported and left alone. Best-effort: failures are
/// logged.
async fn sweep_export_dir(export_dir: &Path) {
    // Exports live at `<export_dir>/<partition_id>/<snapshot_id>`.
    let mut leftovers = Vec::new();
    for partition_dir in list_subdirs(export_dir).await {
        leftovers.extend(list_subdirs(&partition_dir).await);
    }
    if leftovers.is_empty() {
        return;
    }

    info!(
        count = leftovers.len(),
        path = %export_dir.display(),
        "Deleting local snapshot exports left over from a previous run"
    );
    tokio::spawn(
        async move {
            for leftover in leftovers {
                if let Err(err) = tokio::fs::remove_dir_all(&leftover).await {
                    warn!(%err, path = %leftover.display(), "Failed to delete leftover local snapshot export");
                }
            }
        }
        .in_current_span(),
    );
}

/// Lists the subdirectories of `dir`, treating a missing directory as empty. Other entries,
/// including symlinks, are reported and skipped.
async fn list_subdirs(dir: &Path) -> Vec<PathBuf> {
    let list = async {
        let mut subdirs = Vec::new();
        let mut entries = tokio::fs::read_dir(dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                subdirs.push(entry.path());
            } else {
                warn!(path = %entry.path().display(), "Ignoring unexpected entry in the local snapshot export directory");
            }
        }
        Ok::<_, std::io::Error>(subdirs)
    };
    match list.await {
        Ok(subdirs) => subdirs,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => {
            warn!(%err, path = %dir.display(), "Failed to scan local snapshot export directory");
            Vec::new()
        }
    }
}

#[derive(Clone)]
pub struct Snapshots {
    repository: Option<SnapshotRepository>,
    concurrency_limit: Arc<Semaphore>,
}

impl Snapshots {
    pub async fn create(options: &SnapshotsOptions, staging_dir: PathBuf) -> anyhow::Result<Self> {
        // No export or upload can be in flight yet; the restore staging directory is a sibling of
        // the export root and is not touched.
        sweep_export_dir(&options.snapshots_base_dir()).await;

        let repository = SnapshotRepository::new_from_config(options, staging_dir).await?;

        let concurrency_limit =
            Arc::new(Semaphore::new(options.export_concurrency_limit() as usize));

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

        let permit = Arc::clone(&self.concurrency_limit)
            .acquire_owned()
            .await
            .expect("we never close the semaphore");

        // RocksDB's export runs on a blocking pool and keeps writing into the snapshot directory
        // after the future awaiting it is dropped. The export therefore runs in its own task,
        // which holds the export permit and the directory guard until RocksDB has finished, so
        // cancelling this call neither exceeds the export limit nor leaks the directory. If the
        // caller is gone by then, the finished snapshot is dropped and its guard removes it.
        let snapshot_base_path = snapshot_base_path.to_path_buf();
        tokio::spawn(
            async move {
                let _permit = permit;
                partition_store
                    .create_local_snapshot(&snapshot_base_path, min_target_lsn, snapshot_id)
                    .await
            }
            .in_current_span(),
        )
        .await
        .map_err(anyhow::Error::from)
        .and_then(|export| export.map_err(anyhow::Error::from))
        .map_err(|err| SnapshotError {
            partition_id,
            kind: SnapshotErrorKind::Export(err),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn sweep_export_dir_removes_leftovers_in_background() {
        let export_dir = tempfile::tempdir().unwrap();
        let partition_dir = export_dir.path().join("0");
        let leftover = partition_dir.join(SnapshotId::new().to_string());
        std::fs::create_dir_all(&leftover).unwrap();
        std::fs::write(leftover.join("file.sst"), b"x").unwrap();

        // Not an export location: left alone.
        let stray = export_dir.path().join("stray");
        std::fs::write(&stray, b"x").unwrap();

        sweep_export_dir(export_dir.path()).await;

        // Deletion is asynchronous.
        tokio::time::timeout(Duration::from_secs(10), async {
            while leftover.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the leftover export is deleted by the background task");
        assert!(partition_dir.exists() && stray.exists());

        // A missing export directory is a no-op.
        sweep_export_dir(&export_dir.path().join("does-not-exist")).await;
    }
}
