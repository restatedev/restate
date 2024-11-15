// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
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
use tokio::sync::{oneshot, watch};
use tracing::{debug, warn};

use restate_partition_store::snapshots::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotFormatVersion,
};
use restate_partition_store::PartitionStoreManager;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::Lsn;

/// Creates a partition store snapshot along with Restate snapshot metadata.
pub struct SnapshotPartitionTask {
    pub cluster_name: String,
    pub node_name: String,
    pub partition_id: PartitionId,
    pub snapshot_base_path: PathBuf,
    pub partition_store_manager: PartitionStoreManager,
    pub archived_lsn_sender: watch::Sender<Option<Lsn>>,
    pub result_sender: oneshot::Sender<anyhow::Result<SnapshotId>>,
}

impl SnapshotPartitionTask {
    pub async fn create_snapshot(self) -> anyhow::Result<SnapshotId> {
        debug!(
            partition_id = %self.partition_id,
            "Creating partition snapshot"
        );

        let result = create_snapshot_inner(
            self.partition_store_manager,
            self.cluster_name,
            self.node_name,
            self.partition_id,
            self.snapshot_base_path,
            self.archived_lsn_sender,
        )
        .await;

        match result {
            Ok(metadata) => {
                let _ = self
                    .result_sender
                    .send(Ok(metadata.snapshot_id))
                    .inspect_err(|err| {
                        warn!(
                        "Failed to send snapshot acknowledgement after snapshot {} was successfully created: {:?}",
                        metadata.snapshot_id,
                        err
                    )
                    });

                debug!(
                    partition_id = %self.partition_id,
                    snapshot_id = %metadata.snapshot_id,
                    archived_lsn = %metadata.min_applied_lsn,
                    "Partition snapshot created"
                );
                Ok(metadata.snapshot_id)
            }
            Err(err) => {
                warn!(
                    partition_id = %self.partition_id,
                    "Failed to create partition snapshot: {}",
                    err
                );

                Err(err)
            }
        }
    }
}

async fn create_snapshot_inner(
    partition_store_manager: PartitionStoreManager,
    cluster_name: String,
    node_name: String,
    partition_id: PartitionId,
    snapshot_base_path: PathBuf,
    archived_lsn_sender: watch::Sender<Option<Lsn>>,
) -> anyhow::Result<PartitionSnapshotMetadata> {
    let snapshot_id = SnapshotId::new();
    let snapshot = partition_store_manager
        .export_partition_snapshot(partition_id, snapshot_id, snapshot_base_path.clone())
        .await?;

    let metadata = write_snapshot_metadata_header(
        snapshot_id,
        cluster_name,
        node_name,
        partition_id,
        snapshot,
    )
    .await?;

    // todo(pavel): SnapshotRepository integration will go in here in a future PR

    archived_lsn_sender.send(Some(metadata.min_applied_lsn))?;

    Ok(metadata)
}

async fn write_snapshot_metadata_header(
    snapshot_id: SnapshotId,
    cluster_name: String,
    node_name: String,
    partition_id: PartitionId,
    snapshot: LocalPartitionSnapshot,
) -> anyhow::Result<PartitionSnapshotMetadata> {
    let snapshot_meta = PartitionSnapshotMetadata {
        version: SnapshotFormatVersion::V1,
        cluster_name,
        node_name,
        partition_id,
        created_at: humantime::Timestamp::from(SystemTime::now()),
        snapshot_id,
        key_range: snapshot.key_range,
        min_applied_lsn: snapshot.min_applied_lsn,
        db_comparator_name: snapshot.db_comparator_name.clone(),
        files: snapshot.files.clone(),
    };
    let metadata_json = serde_json::to_string_pretty(&snapshot_meta)?;

    let metadata_path = snapshot.base_dir.join("metadata.json");
    tokio::fs::write(metadata_path.clone(), metadata_json).await?;
    debug!(
        %snapshot_id,
        lsn = %snapshot.min_applied_lsn,
        "Partition snapshot metadata written to {:?}",
        metadata_path
    );

    Ok(snapshot_meta)
}