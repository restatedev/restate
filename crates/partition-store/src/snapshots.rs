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

use crate::{PartitionStore, SnapshotError, SnapshotErrorKind};

pub use self::metadata::*;
pub use self::repository::{PartitionSnapshotStatus, SnapshotRepository};
pub use self::snapshot_task::*;

use tokio::sync::Semaphore;
use tracing::{debug, info, instrument, warn};

use restate_types::config::{Configuration, SnapshotsOptions};
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
