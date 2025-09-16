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
use std::sync::Arc;
use std::time::SystemTime;

use tracing::{debug, info, instrument, warn};

use restate_core::cancellation_token;
use restate_types::identifiers::{PartitionId, SnapshotId};
use restate_types::logs::Lsn;
use restate_types::nodes_config::ClusterFingerprint;

use super::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotError, SnapshotErrorKind,
    SnapshotFormatVersion, SnapshotRepository,
};
use crate::PartitionStoreManager;

/// Creates a partition store snapshot along with Restate snapshot metadata.
pub struct SnapshotPartitionTask {
    pub snapshot_id: SnapshotId,
    pub partition_id: PartitionId,
    pub min_target_lsn: Option<Lsn>,
    pub snapshot_base_path: PathBuf,
    pub partition_store_manager: Arc<PartitionStoreManager>,
    pub cluster_name: String,
    pub cluster_fingerprint: Option<ClusterFingerprint>,
    pub node_name: String,
    pub snapshot_repository: SnapshotRepository,
}

impl SnapshotPartitionTask {
    #[instrument(
        name = "create-snapshot",
        level = "error",
        skip_all,
        fields(partition_id = %self.partition_id, snapshot_id = %self.snapshot_id)
    )]
    pub async fn run(self) -> Result<PartitionSnapshotMetadata, SnapshotError> {
        debug!("Creating partition snapshot");
        if let Some(result) = cancellation_token()
            .run_until_cancelled(self.create_snapshot_inner())
            .await
        {
            result
                .inspect(|metadata| {
                    info!(archived_lsn = %metadata.min_applied_lsn, "Created partition snapshot");
                })
                .inspect_err(|err| {
                    warn!("Create snapshot failed: {}", err);
                })
        } else {
            Err(SnapshotError {
                partition_id: self.partition_id,
                kind: SnapshotErrorKind::Shutdown(restate_core::ShutdownError),
            })
        }
    }

    async fn create_snapshot_inner(&self) -> Result<PartitionSnapshotMetadata, SnapshotError> {
        let snapshot = self
            .partition_store_manager
            .export_partition(
                self.partition_id,
                self.min_target_lsn,
                self.snapshot_id,
                self.snapshot_base_path.as_path(),
            )
            .await?;

        let metadata = self.metadata(&snapshot, SystemTime::now());

        self.snapshot_repository
            .put(&metadata, snapshot.base_dir)
            .await
            .map_err(|e| SnapshotError {
                partition_id: self.partition_id,
                kind: SnapshotErrorKind::RepositoryIo(e),
            })?;
        if let Some(db) = self
            .partition_store_manager
            .get_partition_db(self.partition_id)
            .await
        {
            db.note_archived_lsn(metadata.min_applied_lsn);
        }

        Ok(metadata)
    }

    fn metadata(
        &self,
        snapshot: &LocalPartitionSnapshot,
        created_at: SystemTime,
    ) -> PartitionSnapshotMetadata {
        PartitionSnapshotMetadata {
            version: SnapshotFormatVersion::V1,
            cluster_name: self.cluster_name.clone(),
            cluster_fingerprint: self.cluster_fingerprint,
            node_name: self.node_name.clone(),
            partition_id: self.partition_id,
            created_at: humantime::Timestamp::from(created_at),
            snapshot_id: self.snapshot_id,
            key_range: snapshot.key_range.clone(),
            log_id: snapshot.log_id,
            min_applied_lsn: snapshot.min_applied_lsn,
            db_comparator_name: snapshot.db_comparator_name.clone(),
            files: snapshot.files.clone(),
        }
    }
}
