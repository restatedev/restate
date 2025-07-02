// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeInclusive;
use std::path::PathBuf;

use rocksdb::LiveFile;
use serde::{Deserialize, Serialize};
use serde_with::hex::Hex;
use serde_with::{DeserializeAs, SerializeAs, serde_as};

use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::nodes_config::ClusterFingerprint;

#[derive(Debug, Clone, derive_more::Display)]
#[display("{}", snapshot_id)]
pub struct SnapshotCreated {
    pub snapshot_id: SnapshotId,
    pub log_id: LogId,
    pub min_applied_lsn: Lsn,
}

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
    pub created_at: humantime::Timestamp,

    /// Snapshot id.
    pub snapshot_id: SnapshotId,

    /// The partition key range that the partition processor which generated this snapshot was
    /// responsible for, at the time the snapshot was generated.
    pub key_range: RangeInclusive<PartitionKey>,

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
    pub created_at: humantime::Timestamp,
    pub snapshot_id: SnapshotId,
    pub key_range: RangeInclusive<PartitionKey>,
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
                .unwrap_or(LogId::default_for_partition(value.partition_id)),
            min_applied_lsn: value.min_applied_lsn,
            db_comparator_name: value.db_comparator_name,
            files: value.files,
        }
    }
}

impl From<&PartitionSnapshotMetadata> for SnapshotCreated {
    fn from(metadata: &PartitionSnapshotMetadata) -> SnapshotCreated {
        SnapshotCreated {
            snapshot_id: metadata.snapshot_id,
            log_id: metadata.log_id,
            min_applied_lsn: metadata.min_applied_lsn,
        }
    }
}

/// A locally-stored partition snapshot.
#[derive(Debug)]
pub struct LocalPartitionSnapshot {
    pub base_dir: PathBuf,
    pub log_id: LogId,
    pub min_applied_lsn: Lsn,
    pub db_comparator_name: String,
    pub files: Vec<LiveFile>,
    pub key_range: RangeInclusive<PartitionKey>,
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
