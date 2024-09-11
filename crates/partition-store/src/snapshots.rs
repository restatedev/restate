use restate_types::identifiers::{PartitionId, PartitionKey};
use restate_types::logs::Lsn;
use rocksdb::LiveFile;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DeserializeAs, SerializeAs};
use std::ops::RangeInclusive;
use std::path::PathBuf;

/// Partition snapshot description.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionSnapshotMetadata {
    /// Restate cluster name which produced the snapshot.
    pub cluster_name: String,

    // todo(pavel): the partition table version may be useful for sanity check on restore
    /// Restate partition id.
    pub partition_id: PartitionId,

    /// The partition key range that the partition processor which generated this snapshot was
    /// responsible for, at the time the snapshot was generated.
    pub key_range: RangeInclusive<PartitionKey>,

    /// The minimum LSN guaranteed to be applied in this snapshot. The actual
    /// LSN may be >= [minimum_lsn].
    pub min_applied_lsn: Lsn,

    /// The RocksDB SST files comprising the snapshot.
    #[serde_as(as = "Vec<SnapshotSstFile>")]
    pub files: Vec<LiveFile>,
}

/// A locally-stored partition snapshot.
#[derive(Debug)]
pub struct LocalPartitionSnapshot {
    pub base_dir: PathBuf,
    pub min_applied_lsn: Lsn,
    pub db_comparator_name: String,
    pub files: Vec<LiveFile>,
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
    #[serde_as(as = "Option<serde_with::hex::Hex>")]
    pub start_key: Option<Vec<u8>>,
    #[serde_as(as = "Option<serde_with::hex::Hex>")]
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
