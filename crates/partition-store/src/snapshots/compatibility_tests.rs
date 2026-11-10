// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compatibility of the snapshot manifests with the servers that can share a snapshot repository
//! with this one: v1.8.0 allows rolling back to v1.7.3 or newer (`release-notes/v1.8.0.md`).
//! The manifest types are the same in every release from v1.7.0 through v1.8.1. The fixtures and
//! the `legacy` module freeze them as of v1.8.1.

use std::num::NonZeroU8;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use tokio::io::AsyncWriteExt;
use url::Url;

use restate_core::{Metadata, TestCoreEnv};
use restate_memory::ByteCount;
use restate_test_util::tempdir;
use restate_types::config::SnapshotsOptions;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::sharding::KeyRange;
use restate_types::{GenerationalNodeId, SemanticRestateVersion};

use super::{LatestSnapshot, PartitionSnapshotStatus, SnapshotReference, SnapshotRepository};
use crate::snapshots::{
    PartitionSnapshotMetadata, SnapshotCompatibility, SnapshotDir, SnapshotFormatVersion,
};

/// `metadata.json` as v1.8.1 writes it.
const V1_8_1_METADATA: &str = r#"{
  "version": "V1",
  "cluster_name": "cluster",
  "cluster_fingerprint": 42,
  "partition_id": 3,
  "node_name": "node-1",
  "created_at": "2026-01-02T00:00:00Z",
  "snapshot_id": "snap_103LZ30BX8sU4IDCl1LV7zz",
  "key_range": {
    "start": 0,
    "end": 1000
  },
  "log_id": 3,
  "min_applied_lsn": 2000,
  "db_comparator_name": "leveldb.BytewiseComparator",
  "files": [
    {
      "column_family_name": "data-3",
      "name": "/000042.sst",
      "directory": "/tmp/snap",
      "size": 4096,
      "level": 6,
      "start_key": "00",
      "end_key": "ff",
      "smallest_seqno": 1,
      "largest_seqno": 9,
      "num_entries": 10,
      "num_deletions": 1
    }
  ]
}"#;

/// `latest.json` as v1.8.1 writes it, with two retained snapshots.
const V1_8_1_LATEST: &str = r#"{
  "version": "V2",
  "partition_id": 3,
  "log_id": 3,
  "cluster_name": "cluster",
  "cluster_fingerprint": 42,
  "node_name": "node-1",
  "created_at": "2026-01-02T00:00:00Z",
  "snapshot_id": "snap_103LZ30BX8sU4IDCl1LV7zz",
  "min_applied_lsn": 2000,
  "path": "lsn_00000000000000002000-snap_103LZ30BX8sU4IDCl1LV7zz",
  "retained_snapshots": [
    {
      "snapshot_id": "snap_103LZ30BX8sU4IDCl1LV7zz",
      "min_applied_lsn": 2000,
      "created_at": "2026-01-02T00:00:00Z",
      "path": "lsn_00000000000000002000-snap_103LZ30BX8sU4IDCl1LV7zz"
    },
    {
      "snapshot_id": "snap_101SZwviYzes2mjOn86AK9r",
      "min_applied_lsn": 1000,
      "created_at": "2026-01-01T00:00:00Z",
      "path": "lsn_00000000000000001000-snap_101SZwviYzes2mjOn86AK9r"
    }
  ]
}"#;

/// The manifest types of v1.8.1, `crates/partition-store/src/snapshots/{metadata,repository}.rs`.
mod legacy {
    use serde::{Deserialize, Serialize};
    use serde_with::hex::Hex;
    use serde_with::serde_as;

    use restate_types::identifiers::{PartitionId, SnapshotId};
    use restate_types::logs::{LogId, Lsn};
    use restate_types::nodes_config::ClusterFingerprint;
    use restate_types::sharding::KeyRange;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum LatestSnapshotVersion {
        V1,
        V2,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct LatestSnapshot {
        pub version: LatestSnapshotVersion,
        pub partition_id: PartitionId,
        pub log_id: Option<LogId>,
        pub cluster_name: String,
        #[serde(default)]
        pub cluster_fingerprint: Option<ClusterFingerprint>,
        pub node_name: String,
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        pub created_at: jiff::Timestamp,
        pub snapshot_id: SnapshotId,
        pub min_applied_lsn: Lsn,
        pub path: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        pub retained_snapshots: Vec<SnapshotReference>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SnapshotReference {
        pub snapshot_id: SnapshotId,
        pub min_applied_lsn: Lsn,
        #[serde(with = "serde_with::As::<serde_with::DisplayFromStr>")]
        pub created_at: jiff::Timestamp,
        pub path: String,
    }

    #[derive(Debug, Deserialize)]
    pub enum SnapshotFormatVersion {
        V1,
    }

    /// `PartitionSnapshotMetadataShadow`, through which v1.8.1 reads `metadata.json`.
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    pub struct PartitionSnapshotMetadata {
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
        pub files: Vec<SnapshotSstFile>,
    }

    #[serde_as]
    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
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

    impl LatestSnapshot {
        /// What v1.8.1 stores when it publishes `snapshot` over the stored `current`
        /// (`effective_retained_snapshots` and `build_latest_v2`). It reads only the fields of
        /// `snapshot` that v1.8.1 has.
        pub fn publish(
            current: &Self,
            snapshot: &crate::snapshots::PartitionSnapshotMetadata,
            num_retained: usize,
        ) -> Self {
            let new_ref = SnapshotReference {
                snapshot_id: snapshot.snapshot_id,
                min_applied_lsn: snapshot.min_applied_lsn,
                created_at: snapshot.created_at,
                path: format!(
                    "lsn_{:020}-{}",
                    snapshot.min_applied_lsn, snapshot.snapshot_id
                ),
            };
            let mut retained_snapshots = if current.retained_snapshots.is_empty() {
                vec![SnapshotReference {
                    snapshot_id: current.snapshot_id,
                    min_applied_lsn: current.min_applied_lsn,
                    created_at: current.created_at,
                    path: current.path.clone(),
                }]
            } else {
                current.retained_snapshots.clone()
            };
            retained_snapshots.insert(0, new_ref.clone());
            retained_snapshots.truncate(num_retained);
            Self {
                version: LatestSnapshotVersion::V2,
                partition_id: snapshot.partition_id,
                log_id: Some(snapshot.log_id),
                cluster_name: snapshot.cluster_name.clone(),
                cluster_fingerprint: snapshot.cluster_fingerprint,
                node_name: snapshot.node_name.clone(),
                created_at: snapshot.created_at,
                snapshot_id: snapshot.snapshot_id,
                min_applied_lsn: snapshot.min_applied_lsn,
                path: new_ref.path,
                retained_snapshots,
            }
        }
    }
}

#[test]
fn old_manifests_read_as_unknown() -> anyhow::Result<()> {
    let metadata: PartitionSnapshotMetadata = serde_json::from_str(V1_8_1_METADATA)?;
    assert_eq!(metadata.node_id, None);
    assert_eq!(metadata.compatibility, None);
    assert!(has_no_byte_offsets(&metadata));
    assert_eq!(metadata.data_size_in_bytes(), ByteCount::from(4096_u64));

    let latest: LatestSnapshot = serde_json::from_str(V1_8_1_LATEST)?;
    let status = PartitionSnapshotStatus::try_from(&latest)?;
    assert_eq!(status.archived_lsn, Lsn::new(1000));
    assert_eq!(status.latest_snapshot_lsn, Lsn::new(2000));
    assert_eq!(status.latest_snapshot_compatibility, None);
    assert_eq!(status.latest_snapshot_data_size_in_bytes, None);
    assert!(latest.retained_snapshots.iter().all(is_unknown));

    // Values this server can't decode, e.g. written in another shape by a future server, also
    // read as unknown, instead of making the manifest unreadable.
    let mut latest: serde_json::Value = serde_json::from_str(V1_8_1_LATEST)?;
    latest["node_id"] = "not a node".into();
    latest["compatibility"] = serde_json::json!({ "min_restate_version": 2 });
    latest["data_size_in_bytes"] = "large".into();
    latest["retained_snapshots"][0]["node_id"] = 7.into();
    latest["retained_snapshots"][0]["compatibility"] = serde_json::json!([]);
    latest["retained_snapshots"][0]["data_size_in_bytes"] = (-1).into();
    let latest: LatestSnapshot = serde_json::from_str(&latest.to_string())?;
    assert_eq!(latest.min_applied_lsn, Lsn::new(2000));
    assert_eq!(latest.retained_snapshots.len(), 2);
    assert_eq!(latest.node_id, None);
    assert_eq!(latest.compatibility, None);
    assert_eq!(latest.data_size_in_bytes, None);
    assert!(latest.retained_snapshots.iter().all(is_unknown));

    let mut metadata: serde_json::Value = serde_json::from_str(V1_8_1_METADATA)?;
    metadata["node_id"] = serde_json::json!([]);
    metadata["compatibility"] = serde_json::json!({ "storage_features": "all" });
    metadata["byte_offsets_base_lsn"] = "first".into();
    metadata["log_byte_offset"] = (-1).into();
    metadata["partition_store_byte_offset"] = serde_json::json!({ "bytes": 1 });
    let metadata: PartitionSnapshotMetadata = serde_json::from_str(&metadata.to_string())?;
    assert_eq!(metadata.files.len(), 1);
    assert_eq!(metadata.node_id, None);
    assert_eq!(metadata.compatibility, None);
    assert!(has_no_byte_offsets(&metadata));
    Ok(())
}

fn has_no_byte_offsets(metadata: &PartitionSnapshotMetadata) -> bool {
    metadata.byte_offsets_base_lsn.is_none()
        && metadata.log_byte_offset.is_none()
        && metadata.partition_store_byte_offset.is_none()
}

fn is_unknown(snapshot: &SnapshotReference) -> bool {
    snapshot.node_id.is_none()
        && snapshot.compatibility.is_none()
        && snapshot.data_size_in_bytes.is_none()
}

/// Old and new servers take turns publishing snapshots of the same partition. Each reads what the
/// other wrote, and the new fields always describe their own snapshot or are absent.
#[restate_core::test]
async fn old_and_new_servers_alternate_publishing() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let destination = tempdir()?;
    let opts = SnapshotsOptions {
        destination: Some(Url::from_file_path(destination.path()).unwrap().to_string()),
        num_retained: NonZeroU8::new(4).unwrap(),
        ..SnapshotsOptions::default()
    };
    let repository = SnapshotRepository::new_from_config(&opts, tempdir()?.keep())
        .await?
        .unwrap();
    let latest_path = repository.latest_snapshot_pointer_path(PartitionId::MIN);
    let latest_status = async || {
        repository
            .get_latest_partition_snapshot_status(PartitionId::MIN)
            .await
            .map(Option::unwrap)
    };
    let latest = async || -> anyhow::Result<LatestSnapshot> {
        Ok(serde_json::from_slice(
            &read(&repository, &latest_path).await?,
        )?)
    };
    // The new fields of each retained snapshot, and what they are when known or unknown.
    let retained = |latest: &LatestSnapshot| -> Vec<_> {
        latest
            .retained_snapshots
            .iter()
            .map(|s| {
                (
                    s.snapshot_id,
                    s.node_id,
                    s.compatibility.clone(),
                    s.data_size_in_bytes,
                )
            })
            .collect()
    };
    let known = |s: &PartitionSnapshotMetadata| {
        (
            s.snapshot_id,
            s.node_id,
            s.compatibility.clone(),
            Some(s.data_size_in_bytes()),
        )
    };
    let unknown = |s: &PartitionSnapshotMetadata| (s.snapshot_id, None, None, None);

    // A new server publishes A, and old servers read its manifests.
    let (a, a_dir) = snapshot(Lsn::new(1000), b"a-data").await?;
    repository.put(&a, SnapshotDir::new(a_dir)).await?;
    let status = latest_status().await?;
    assert_eq!(status.latest_snapshot_compatibility, a.compatibility);
    assert_eq!(
        status.latest_snapshot_data_size_in_bytes,
        Some(ByteCount::from(6_u64))
    );
    let stored = latest().await?;
    assert_eq!(stored.node_id, a.node_id);
    assert_eq!(retained(&stored), [known(&a)]);
    // Sizes are written as plain numbers of bytes.
    let raw: serde_json::Value = serde_json::from_slice(&read(&repository, &latest_path).await?)?;
    assert_eq!(raw["data_size_in_bytes"], 6);
    assert_eq!(raw["retained_snapshots"][0]["data_size_in_bytes"], 6);
    let metadata_path = repository.snapshot_file_path(&a, "metadata.json");
    let metadata_json = read(&repository, &metadata_path).await?;
    let old_metadata: legacy::PartitionSnapshotMetadata = serde_json::from_slice(&metadata_json)?;
    assert_eq!(old_metadata.snapshot_id, a.snapshot_id);
    assert_eq!(old_metadata.files.len(), 1);
    // The byte offsets are written as plain numbers, and read back by new servers.
    let raw: serde_json::Value = serde_json::from_slice(&metadata_json)?;
    assert_eq!(raw["byte_offsets_base_lsn"], 10);
    assert_eq!(raw["log_byte_offset"], 100_000);
    assert_eq!(raw["partition_store_byte_offset"], u64::MAX);
    let metadata: PartitionSnapshotMetadata = serde_json::from_slice(&metadata_json)?;
    assert_eq!(
        (
            metadata.byte_offsets_base_lsn,
            metadata.log_byte_offset,
            metadata.partition_store_byte_offset
        ),
        (
            a.byte_offsets_base_lsn,
            a.log_byte_offset,
            a.partition_store_byte_offset
        )
    );

    // A new server publishes B, whose compatibility couldn't be captured: A's isn't carried over.
    let (mut b, b_dir) = snapshot(Lsn::new(2000), b"b-data-longer").await?;
    b.compatibility = None;
    repository.put(&b, SnapshotDir::new(b_dir)).await?;
    let status = latest_status().await?;
    assert_eq!(status.latest_snapshot_id, b.snapshot_id);
    assert_eq!(status.latest_snapshot_compatibility, None);
    assert_eq!(
        status.latest_snapshot_data_size_in_bytes,
        Some(ByteCount::from(13_u64))
    );
    assert_eq!(retained(&latest().await?), [known(&b), known(&a)]);

    // An old server publishes C, dropping the fields it doesn't know, also from A and B.
    let old_latest: legacy::LatestSnapshot =
        serde_json::from_slice(&read(&repository, &latest_path).await?)?;
    assert_eq!(old_latest.version, legacy::LatestSnapshotVersion::V2);
    let (c, _) = snapshot(Lsn::new(3000), b"c-data").await?;
    let old_latest = legacy::LatestSnapshot::publish(&old_latest, &c, 4);
    repository
        .object_store
        .put(
            &latest_path,
            PutPayload::from(serde_json::to_string_pretty(&old_latest)?),
        )
        .await?;
    let status = latest_status().await?;
    assert_eq!(status.latest_snapshot_id, c.snapshot_id);
    assert_eq!(status.archived_lsn, Lsn::new(1000));
    assert_eq!(status.latest_snapshot_compatibility, None);
    assert_eq!(status.latest_snapshot_data_size_in_bytes, None);
    let stored = latest().await?;
    assert_eq!(stored.node_id, None);
    assert_eq!(retained(&stored), [unknown(&c), unknown(&b), unknown(&a)]);

    // A new server publishes D over what the old one wrote, and old servers still read it. The
    // older snapshots stay unknown: nothing fills them in from another snapshot.
    let (d, d_dir) = snapshot(Lsn::new(4000), b"d-data").await?;
    repository.put(&d, SnapshotDir::new(d_dir)).await?;
    let status = latest_status().await?;
    assert_eq!(status.latest_snapshot_id, d.snapshot_id);
    assert_eq!(status.archived_lsn, Lsn::new(1000));
    assert_eq!(status.latest_snapshot_compatibility, d.compatibility);
    assert_eq!(
        status.latest_snapshot_data_size_in_bytes,
        Some(ByteCount::from(6_u64))
    );
    assert_eq!(
        retained(&latest().await?),
        [known(&d), unknown(&c), unknown(&b), unknown(&a)]
    );

    let latest_json = read(&repository, &latest_path).await?;
    let old_latest: legacy::LatestSnapshot = serde_json::from_slice(&latest_json)?;
    assert_eq!(old_latest.snapshot_id, d.snapshot_id);
    assert_eq!(old_latest.retained_snapshots.len(), 4);
    Ok(())
}

async fn read(repository: &SnapshotRepository, path: &ObjectPath) -> anyhow::Result<Bytes> {
    Ok(repository.object_store.get(path).await?.bytes().await?)
}

/// A snapshot as a new server creates it, with its data file in a new local directory.
async fn snapshot(
    lsn: Lsn,
    data: &[u8],
) -> anyhow::Result<(PartitionSnapshotMetadata, std::path::PathBuf)> {
    let dir = tempdir()?.keep();
    let mut file = tokio::fs::File::create(dir.join("data.sst")).await?;
    file.write_all(data).await?;
    file.shutdown().await?;

    let (cluster_name, cluster_fingerprint) = Metadata::with_current(|m| {
        let nodes_config = m.nodes_config_ref();
        (
            nodes_config.cluster_name().to_owned(),
            nodes_config.cluster_fingerprint(),
        )
    });
    let metadata = PartitionSnapshotMetadata {
        version: SnapshotFormatVersion::V1,
        cluster_name,
        cluster_fingerprint,
        partition_id: PartitionId::MIN,
        node_name: "node".to_owned(),
        node_id: Some(GenerationalNodeId::new(1, 2)),
        created_at: jiff::Timestamp::now(),
        snapshot_id: SnapshotId::new(),
        key_range: KeyRange::FULL,
        log_id: LogId::MIN,
        min_applied_lsn: lsn,
        db_comparator_name: "leveldb.BytewiseComparator".to_owned(),
        compatibility: Some(SnapshotCompatibility {
            min_restate_version: SemanticRestateVersion::new(1, 8, 0),
            storage_features: vec!["scoped-state-table".into(), "future-feature".into()],
            partition_features: vec!["vqueues".into()],
        }),
        byte_offsets_base_lsn: Some(Lsn::new(10)),
        log_byte_offset: Some(lsn.as_u64() * 100),
        partition_store_byte_offset: Some(u64::MAX),
        files: vec![rocksdb::LiveFile {
            column_family_name: "data-0".to_owned(),
            name: "/data.sst".to_owned(),
            directory: dir.to_string_lossy().into_owned(),
            size: data.len(),
            level: 0,
            start_key: Some(vec![0]),
            end_key: Some(vec![0xff]),
            num_entries: 0,
            num_deletions: 0,
            smallest_seqno: 0,
            largest_seqno: 0,
        }],
    };
    Ok((metadata, dir))
}
