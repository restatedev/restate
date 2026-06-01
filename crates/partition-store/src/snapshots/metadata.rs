// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use rocksdb::LiveFile;
use serde::{Deserialize, Serialize};
use serde_with::hex::Hex;
use serde_with::{DeserializeAs, SerializeAs, serde_as};
use tracing::warn;

use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::nodes_config::ClusterFingerprint;
use restate_types::sharding::KeyRange;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum SnapshotFormatVersion {
    #[default]
    V1,
}

/// A partition store snapshot. Metadata object which is published alongside with the partition
/// store RocksDB SST files that make up a particular checkpoint for a given partition. Note that
/// the applied LSN is read from the store itself, before the snapshot checkpoint is created. It
/// could therefore be smaller than the actual LSN of the snapshot, which can only be determined
/// when the snapshot is imported and opened for reading.
#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(from = "PartitionSnapshotMetadataShadow")]
pub struct PartitionSnapshotMetadata {
    pub version: SnapshotFormatVersion,

    /// Restate cluster name which produced the snapshot.
    pub cluster_name: String,

    // a unique fingerprint for this cluster.
    #[serde(default)]
    pub cluster_fingerprint: Option<ClusterFingerprint>,

    /// Restate partition id.
    pub partition_id: PartitionId,

    /// Node that produced this snapshot.
    pub node_name: String,

    /// Local node time when the snapshot was created.
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub created_at: jiff::Timestamp,

    /// Snapshot id.
    pub snapshot_id: SnapshotId,

    /// The partition key range that the partition processor which generated this snapshot was
    /// responsible for, at the time the snapshot was generated.
    pub key_range: KeyRange,

    /// The log id backing the partition.
    pub log_id: LogId,

    /// The minimum LSN guaranteed to be applied in this snapshot. The actual
    /// LSN may be >= [minimum_lsn].
    pub min_applied_lsn: Lsn,

    /// The RocksDB comparator name used by the partition processor which generated this snapshot.
    pub db_comparator_name: String,

    /// The RocksDB SST files comprising the snapshot.
    #[serde_as(as = "Vec<SnapshotSstFile>")]
    pub files: Vec<LiveFile>,
}

impl PartitionSnapshotMetadata {
    pub fn validate(
        &self,
        cluster_name: &str,
        cluster_fingerprint: Option<ClusterFingerprint>,
    ) -> anyhow::Result<()> {
        if cluster_name != self.cluster_name {
            anyhow::bail!(
                "Snapshot does not match the cluster name of latest snapshot at destination!. Expected: {cluster_name}, got: {}",
                self.cluster_name
            );
        }

        // If either the snapshot or the nodes config doesn't have a fingerprint, skip the check.
        // Eventually all new snapshots will have a fingerprint and this check will be kept for
        // very old snapshots only.
        if let (Some(snapshot_cluster_fingerprint), Some(expected_fingerprint)) =
            (self.cluster_fingerprint, cluster_fingerprint)
            && snapshot_cluster_fingerprint != expected_fingerprint
        {
            // If nodes_config and snapshot both contain a fingerprint, they must match.
            anyhow::bail!(
                "Snapshot {} does not match the cluster fingerprint. Expected: '{expected_fingerprint}' {expected_fingerprint:?}, got: '{snapshot_cluster_fingerprint}' {snapshot_cluster_fingerprint:?}",
                self.snapshot_id,
            );
        }
        Ok(())
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
struct PartitionSnapshotMetadataShadow {
    pub version: SnapshotFormatVersion,
    pub cluster_name: String,
    #[serde(default)]
    pub cluster_fingerprint: Option<ClusterFingerprint>,
    pub partition_id: PartitionId,
    pub node_name: String,
    #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
    pub created_at: jiff::Timestamp,
    pub snapshot_id: SnapshotId,
    pub key_range: KeyRange,
    pub log_id: Option<LogId>,
    pub min_applied_lsn: Lsn,
    pub db_comparator_name: String,
    #[serde_as(as = "Vec<SnapshotSstFile>")]
    pub files: Vec<LiveFile>,
}

impl From<PartitionSnapshotMetadataShadow> for PartitionSnapshotMetadata {
    fn from(value: PartitionSnapshotMetadataShadow) -> Self {
        PartitionSnapshotMetadata {
            version: value.version,
            cluster_name: value.cluster_name,
            cluster_fingerprint: value.cluster_fingerprint,
            partition_id: value.partition_id,
            node_name: value.node_name,
            created_at: value.created_at,
            snapshot_id: value.snapshot_id,
            key_range: value.key_range,
            // if a log id was not written into the snapshot metadata, then it wasn't available at
            // the time; the current log id stored in the partition table might be different now
            // todo: delete this in 1.5
            log_id: value
                .log_id
                .unwrap_or_else(|| LogId::default_for_partition(value.partition_id)),
            min_applied_lsn: value.min_applied_lsn,
            db_comparator_name: value.db_comparator_name,
            files: value.files,
        }
    }
}

/// Owns a snapshot's local on-disk directory and removes it when dropped.
///
/// This makes [`LocalPartitionSnapshot`] the sole owner of its staging directory: the files
/// never outlive the snapshot, so a failed restore or upload cannot leak them (see
/// <https://github.com/restatedev/restate/issues/4838>). A caller that genuinely needs the
/// directory to outlive the snapshot must explicitly take ownership via [`SnapshotDir::into_path`].
#[derive(Debug)]
#[must_use = "Dropping will immediately delete the underlying snapshot dir"]
pub struct SnapshotDir {
    // `None` only after `into_path` has relinquished ownership; the value is taken in both
    // `into_path` and `Drop`, so the directory is removed at most once.
    path: Option<PathBuf>,
}

impl SnapshotDir {
    pub fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    pub fn path(&self) -> &Path {
        self.path
            .as_deref()
            .expect("path is present until into_path() or drop")
    }

    /// Relinquishes ownership of the directory: returns its path and disables the drop-time
    /// cleanup. Use when the directory must outlive the snapshot.
    pub fn into_path(mut self) -> PathBuf {
        self.path
            .take()
            .expect("path is present until into_path() or drop")
    }

    /// Asynchronously removes the directory, consuming the guard. Prefer this over the
    /// synchronous drop-time cleanup when running on an async runtime and the directory may hold
    /// large files (e.g. a freshly exported snapshot before upload).
    pub async fn remove(self) {
        let path = self.into_path();
        if let Err(err) = tokio::fs::remove_dir_all(&path).await
            && err.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                %err,
                path = %path.display(),
                "Failed to remove local snapshot directory",
            );
        }
    }
}

impl Drop for SnapshotDir {
    fn drop(&mut self) {
        if let Some(path) = self.path.take()
            && let Err(err) = std::fs::remove_dir_all(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            warn!(
                %err,
                path = %path.display(),
                "Failed to remove local snapshot directory",
            );
        }
    }
}

/// A locally-stored partition snapshot.
#[derive(Debug)]
pub struct LocalPartitionSnapshot {
    /// The snapshot's local directory. Owned by the snapshot and removed on drop unless
    /// explicitly relinquished via [`SnapshotDir::into_path`].
    pub base_dir: SnapshotDir,
    pub log_id: LogId,
    pub min_applied_lsn: Lsn,
    pub db_comparator_name: String,
    pub files: Vec<LiveFile>,
    pub key_range: KeyRange,
}
/// RocksDB SST file that is part of a snapshot. Serialization wrapper around [LiveFile].
#[serde_as]
#[derive(Serialize, Deserialize)]
#[serde(remote = "LiveFile")]
pub struct SnapshotSstFile {
    pub column_family_name: String,
    pub name: String,
    pub directory: String,
    pub size: usize,
    pub level: i32,
    #[serde_as(as = "Option<Hex>")]
    pub start_key: Option<Vec<u8>>,
    #[serde_as(as = "Option<Hex>")]
    pub end_key: Option<Vec<u8>>,
    pub smallest_seqno: u64,
    pub largest_seqno: u64,
    pub num_entries: u64,
    pub num_deletions: u64,
}

impl SerializeAs<LiveFile> for SnapshotSstFile {
    fn serialize_as<S>(value: &LiveFile, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SnapshotSstFile::serialize(value, serializer)
    }
}

impl<'de> DeserializeAs<'de, LiveFile> for SnapshotSstFile {
    fn deserialize_as<D>(deserializer: D) -> Result<LiveFile, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        SnapshotSstFile::deserialize(deserializer)
    }
}
