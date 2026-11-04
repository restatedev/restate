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

use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::partition_db::State;
use crate::{CommitFence, PartitionStore, SnapshotError, SnapshotErrorKind};

pub use self::metadata::*;
pub use self::repository::{PartitionSnapshotStatus, SnapshotRepository};
pub use self::snapshot_task::*;

use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, warn};

use restate_types::config::{Configuration, SnapshotsOptions};
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::Lsn;

/// Asks a partition processor to hold back its commits during a snapshot export. It replies with
/// the fence, or `None` if another export already holds its commits back.
pub type CommitFenceRequest = oneshot::Sender<Option<CommitFence>>;

/// What a snapshot export holds until RocksDB has captured the partition, even if whoever asked
/// for the snapshot stops waiting: its export slot, so the export concurrency limit holds; the
/// partition's lock, so it can't be closed, reopened or deleted; and, for an open partition, its
/// processor's commit fence, so the data doesn't change.
pub(crate) struct CaptureGuard {
    _permit: OwnedSemaphorePermit,
    _cell: OwnedRwLockReadGuard<State>,
    _fence: Option<CommitFence>,
}

impl CaptureGuard {
    pub(crate) fn new(
        permit: OwnedSemaphorePermit,
        cell: OwnedRwLockReadGuard<State>,
        fence: Option<CommitFence>,
    ) -> Self {
        Self {
            _permit: permit,
            _cell: cell,
            _fence: fence,
        }
    }
}

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

    /// Exports the partition while nothing writes to it, so that the snapshot records exactly
    /// the applied LSN and compatibility of the exported data. `cell` keeps the partition from
    /// being closed or reopened. `commit_fence_tx` reaches the processor of an open partition,
    /// which holds back its commits; it's `None` for a closed partition.
    pub(crate) async fn create_local_snapshot(
        &self,
        mut partition_store: PartitionStore,
        cell: OwnedRwLockReadGuard<State>,
        commit_fence_tx: Option<&mpsc::Sender<CommitFenceRequest>>,
        min_target_lsn: Option<Lsn>,
        snapshot_id: SnapshotId,
        snapshot_base_path: &Path,
    ) -> Result<LocalPartitionSnapshot, SnapshotError> {
        let partition_id = partition_store.partition_id();
        let error = |kind| SnapshotError { partition_id, kind };

        let permit = Arc::clone(&self.concurrency_limit)
            .acquire_owned()
            .await
            .expect("we never close the semaphore");

        let fence = match commit_fence_tx {
            Some(commit_fence_tx) => {
                let (reply_tx, reply_rx) = oneshot::channel();
                commit_fence_tx
                    .send(reply_tx)
                    .await
                    .map_err(|_| error(SnapshotErrorKind::InvalidState))?;
                let fence = reply_rx
                    .await
                    .map_err(|_| error(SnapshotErrorKind::InvalidState))?
                    .ok_or_else(|| error(SnapshotErrorKind::SnapshotInProgress))?;
                Some(fence)
            }
            None => None,
        };

        partition_store
            .create_local_snapshot(
                CaptureGuard::new(permit, cell, fence),
                snapshot_base_path,
                min_target_lsn,
                snapshot_id,
            )
            .await
            .map_err(|err| error(SnapshotErrorKind::Export(err.into())))
    }

    /// Asks the repository for the partition's latest snapshot status. Returns `None` if
    /// no repository is configured or it has no snapshot of the partition.
    pub async fn get_latest_snapshot_status(
        &self,
        partition_id: PartitionId,
    ) -> anyhow::Result<Option<PartitionSnapshotStatus>> {
        let Some(repository) = &self.repository else {
            return Ok(None);
        };
        repository
            .get_latest_partition_snapshot_status(partition_id)
            .await
    }

    #[instrument(level = "error", skip_all, fields(partition_id = %partition_id))]
    pub async fn download_latest_snapshot(
        &self,
        partition_id: PartitionId,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<Option<LocalPartitionSnapshot>> {
        // Attempt to get the latest available snapshot from the snapshot repository:
        let snapshot = match &self.repository {
            Some(repository) => {
                debug!("Looking for partition snapshot from which to bootstrap partition store");
                // todo(pavel): pass target LSN to repository
                repository.get_latest(partition_id, cancellation).await?
            }
            None => {
                debug!("No snapshot repository configured");
                None
            }
        };
        Ok(snapshot)
    }
}

/// The directory that an export of `snapshot_id` is written to, inside `snapshot_base_path`.
pub(crate) fn export_dir(snapshot_base_path: &Path, snapshot_id: SnapshotId) -> PathBuf {
    snapshot_base_path.join(snapshot_id.to_string())
}

/// Removes snapshot exports left over from a previous run, e.g. one that crashed while
/// exporting. Only call it while no export can be in progress, such as at startup.
///
/// It removes only what exports create: the `<partition id>/<snapshot id>` directories
/// under [`SnapshotsOptions::snapshots_base_dir`], and the `<snapshot id>.tmp` directories
/// that RocksDB stages them in. Anything else, symbolic links included, is left alone.
/// Logs only if it removed something or failed to.
pub fn remove_leftover_exports(options: &SnapshotsOptions) {
    let base_dir = options.snapshots_base_dir();
    let mut removed = 0;
    for partition_dir in real_subdirs(&base_dir) {
        let Some(partition_id) = parse_dir_name::<PartitionId>(&partition_dir) else {
            continue;
        };
        // Compare full paths, so only names that exports would produce match.
        if partition_dir != options.snapshots_dir(partition_id) {
            continue;
        }
        for dir in real_subdirs(&partition_dir) {
            let Some(name) = dir.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Ok(snapshot_id) = name.strip_suffix(".tmp").unwrap_or(name).parse() else {
                continue;
            };
            let export = export_dir(&partition_dir, snapshot_id);
            let mut staging = OsString::from(export.as_os_str());
            staging.push(".tmp");
            if dir != export && dir != staging {
                continue;
            }
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {
                    debug!(path = %dir.display(), "Removed leftover snapshot export");
                    removed += 1;
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => warn!(
                    %err,
                    path = %dir.display(),
                    "Failed to remove leftover snapshot export",
                ),
            }
        }
    }
    if removed > 0 {
        info!(
            removed,
            path = %base_dir.display(),
            "Removed snapshot exports left over from a previous run",
        );
    }
}

/// The directories directly inside `dir`, without following symbolic links.
fn real_subdirs(dir: &Path) -> Vec<PathBuf> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(err) => {
            warn!(%err, path = %dir.display(), "Failed to list snapshot exports");
            return Vec::new();
        }
    };
    entries
        .filter_map(Result::ok)
        // `DirEntry::file_type` doesn't follow symbolic links, so they don't count as
        // directories here.
        .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_dir()))
        .map(|entry| entry.path())
        .collect()
}

fn parse_dir_name<T: std::str::FromStr>(dir: &Path) -> Option<T> {
    dir.file_name()?.to_str()?.parse().ok()
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;

    use restate_types::config::SnapshotsOptions;
    use restate_types::identifiers::{PartitionId, SnapshotId};

    use super::{export_dir, remove_leftover_exports};

    #[test]
    fn remove_leftover_exports_removes_only_what_exports_create() -> anyhow::Result<()> {
        let options = SnapshotsOptions::default();
        let base_dir = options.snapshots_base_dir();
        // Nothing to do before any export created the directory.
        remove_leftover_exports(&options);
        assert!(!base_dir.exists());

        let partition_dir = options.snapshots_dir(PartitionId::from(7));
        let export = export_dir(&partition_dir, SnapshotId::new());
        let staging = PathBuf::from(format!("{}.tmp", export.display()));
        std::fs::create_dir_all(export.join("nested"))?;
        std::fs::write(export.join("000001.sst"), b"data")?;
        std::fs::create_dir_all(&staging)?;

        // Paths that exports don't create, including ones named like theirs.
        let non_canonical = base_dir.join("007").join(SnapshotId::new().to_string());
        let unknown_dir = partition_dir.join("not-a-snapshot");
        let file = partition_dir.join(SnapshotId::new().to_string());
        std::fs::create_dir_all(&non_canonical)?;
        std::fs::create_dir_all(&unknown_dir)?;
        std::fs::write(&file, b"data")?;

        // Symbolic links named like a snapshot and like a partition, both pointing outside
        // at a directory that holds an export-shaped directory. Following either one would
        // delete something there.
        let outside = restate_test_util::tempdir()?;
        let outside_export = export_dir(outside.path(), SnapshotId::new());
        std::fs::create_dir_all(&outside_export)?;
        let snapshot_link = partition_dir.join(SnapshotId::new().to_string());
        let partition_link = options.snapshots_dir(PartitionId::from(8));
        symlink(outside.path(), &snapshot_link)?;
        symlink(outside.path(), &partition_link)?;

        remove_leftover_exports(&options);

        assert!(!export.exists());
        assert!(!staging.exists());
        assert!(base_dir.is_dir() && partition_dir.is_dir());
        assert!(non_canonical.is_dir());
        assert!(unknown_dir.is_dir());
        assert!(file.is_file());
        assert!(snapshot_link.is_symlink() && partition_link.is_symlink());
        assert!(outside_export.is_dir());
        Ok(())
    }
}
