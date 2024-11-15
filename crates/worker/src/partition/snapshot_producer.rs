// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::bail;
use tracing::{info, warn};

use restate_partition_store::snapshots::{PartitionSnapshotMetadata, SnapshotFormatVersion};
use restate_partition_store::PartitionStore;
use restate_types::identifiers::SnapshotId;

/// Encapsulates producing a Restate partition snapshot out of a partition store.
pub struct SnapshotProducer {}

pub struct SnapshotSource {
    pub cluster_name: String,
    pub node_name: String,
}

impl SnapshotProducer {
    pub async fn create(
        snapshot_source: SnapshotSource,
        mut partition_store: PartitionStore,
        partition_snapshots_path: PathBuf,
    ) -> anyhow::Result<PartitionSnapshotMetadata> {
        if let Err(e) = tokio::fs::create_dir_all(&partition_snapshots_path).await {
            warn!(
                path = ?partition_snapshots_path,
                error = ?e,
                "Failed to create partition snapshot directory"
            );
            bail!("Failed to create partition snapshot directory: {:?}", e);
        }

        let snapshot_id = SnapshotId::new();
        let snapshot_path = partition_snapshots_path.join(snapshot_id.to_string());
        let snapshot = partition_store
            .create_snapshot(snapshot_path.clone())
            .await?;

        let snapshot_meta = PartitionSnapshotMetadata {
            version: SnapshotFormatVersion::V1,
            cluster_name: snapshot_source.cluster_name,
            node_name: snapshot_source.node_name,
            partition_id: partition_store.partition_id(),
            created_at: humantime::Timestamp::from(SystemTime::now()),
            snapshot_id,
            key_range: partition_store.partition_key_range().clone(),
            min_applied_lsn: snapshot.min_applied_lsn,
            db_comparator_name: snapshot.db_comparator_name.clone(),
            files: snapshot.files.clone(),
        };
        let metadata_json = serde_json::to_string_pretty(&snapshot_meta)?;

        let metadata_path = snapshot_path.join("metadata.json");
        tokio::fs::write(metadata_path.clone(), metadata_json).await?;
        info!(
            lsn = %snapshot.min_applied_lsn,
            metadata = ?metadata_path,
            "Partition snapshot written"
        );

        Ok(snapshot_meta)
    }
}
