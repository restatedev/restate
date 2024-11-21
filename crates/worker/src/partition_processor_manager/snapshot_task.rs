// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use tracing::{debug, instrument, warn};

use restate_core::worker_api::SnapshotError;
use restate_partition_store::snapshots::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotFormatVersion,
};
use restate_partition_store::PartitionStoreManager;
use restate_types::identifiers::{PartitionId, SnapshotId};

/// Creates a partition store snapshot along with Restate snapshot metadata.
pub struct SnapshotPartitionTask {
    pub snapshot_id: SnapshotId,
    pub partition_id: PartitionId,
    pub snapshot_base_path: PathBuf,
    pub partition_store_manager: PartitionStoreManager,
    pub cluster_name: String,
    pub node_name: String,
}

impl SnapshotPartitionTask {
    #[instrument(level = "info", skip_all, fields(snapshot_id = %self.snapshot_id, partition_id = %self.partition_id))]
    pub async fn run(self) -> Result<PartitionSnapshotMetadata, SnapshotError> {
        debug!("Creating partition snapshot");

        let result = self.create_snapshot_inner().await;

        result
            .inspect(|metadata| {
                debug!(
                    archived_lsn = %metadata.min_applied_lsn,
                    "Partition snapshot created"
                );
            })
            .inspect_err(|err| {
                warn!("Failed to create partition snapshot: {}", err);
            })
    }

    async fn create_snapshot_inner(&self) -> Result<PartitionSnapshotMetadata, SnapshotError> {
        let snapshot = self
            .partition_store_manager
            .export_partition_snapshot(
                self.partition_id,
                self.snapshot_id,
                self.snapshot_base_path.as_path(),
            )
            .await?;

        let metadata = self.write_snapshot_metadata_header(snapshot).await?;

        // todo(pavel): SnapshotRepository integration will go in here in a future PR

        Ok(metadata)
    }

    async fn write_snapshot_metadata_header(
        &self,
        snapshot: LocalPartitionSnapshot,
    ) -> Result<PartitionSnapshotMetadata, SnapshotError> {
        let snapshot_meta = PartitionSnapshotMetadata {
            version: SnapshotFormatVersion::V1,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            partition_id: self.partition_id,
            created_at: humantime::Timestamp::from(SystemTime::now()),
            snapshot_id: self.snapshot_id,
            key_range: snapshot.key_range,
            min_applied_lsn: snapshot.min_applied_lsn,
            db_comparator_name: snapshot.db_comparator_name.clone(),
            files: snapshot.files.clone(),
        };
        let metadata_json =
            serde_json::to_string_pretty(&snapshot_meta).expect("Can always serialize JSON");

        let metadata_path = snapshot.base_dir.join("metadata.json");
        tokio::fs::write(metadata_path.clone(), metadata_json)
            .await
            .map_err(|e| SnapshotError::SnapshotMetadataHeaderError(self.partition_id, e))?;

        debug!(
            lsn = %snapshot.min_applied_lsn,
            "Partition snapshot metadata written to {:?}",
            metadata_path
        );

        Ok(snapshot_meta)
    }
}
