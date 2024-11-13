// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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

use anyhow::{bail, Context};
use tracing::{debug, error, info, trace, warn};

use crate::partition::snapshots::repository::SnapshotRepository;
use restate_partition_store::snapshots::{PartitionSnapshotMetadata, SnapshotFormatVersion};
use restate_partition_store::PartitionStore;
use restate_storage_api::fsm_table::{FsmTable, ReadOnlyFsmTable};
use restate_storage_api::Transaction;
use restate_types::config::Configuration;
use restate_types::identifiers::SnapshotId;
use restate_types::live::Live;

/// Encapsulates exporting and publishing partition snapshots.
#[derive(Clone)]
pub struct SnapshotProducer {
    snapshot_source: SnapshotSource,
    partition_store: PartitionStore,
    partition_snapshots_path: PathBuf,
    snapshot_repository: SnapshotRepository,
}

#[derive(Clone)]
pub struct SnapshotSource {
    pub cluster_name: String,
    pub node_name: String,
}

impl SnapshotProducer {
    pub async fn create(
        partition_store: PartitionStore,
        config: Live<Configuration>,
        snapshot_repository: SnapshotRepository,
    ) -> anyhow::Result<Self> {
        let config = config.pinned();
        let partition_id = partition_store.partition_id();

        Ok(SnapshotProducer {
            snapshot_source: SnapshotSource {
                cluster_name: config.common.cluster_name().into(),
                node_name: config.common.node_name().into(),
            },
            partition_store,
            snapshot_repository,
            partition_snapshots_path: config.worker.snapshots.snapshots_dir(partition_id),
        })
    }

    /// Exports a partition store snapshot and writes it to the snapshot repository.
    ///
    /// The final snapshot key will follow the structure:
    /// `[<prefix>/]<partition_id>/<sort_key>/<snapshot_id>_<lsn>.tar`.
    pub async fn create_snapshot(&mut self) -> anyhow::Result<PartitionSnapshotMetadata> {
        if let Err(e) = tokio::fs::create_dir_all(&self.partition_snapshots_path).await {
            warn!(
                path = ?self.partition_snapshots_path,
                error = ?e,
                "Failed to create partition snapshot directory"
            );
            bail!("Failed to create partition snapshot directory: {:?}", e);
        }

        let snapshot_id = SnapshotId::new();
        let snapshot_path = self.partition_snapshots_path.join(snapshot_id.to_string());

        trace!(%snapshot_id, "Creating partition snapshot export directory: {:?}", snapshot_path);
        let snapshot = self
            .partition_store
            .export_snapshot(snapshot_path.clone())
            .await
            .context("Export partition snapshot")?;

        let snapshot_metadata = PartitionSnapshotMetadata {
            version: SnapshotFormatVersion::V1,
            cluster_name: self.snapshot_source.cluster_name.clone(),
            node_name: self.snapshot_source.node_name.clone(),
            partition_id: self.partition_store.partition_id(),
            created_at: humantime::Timestamp::from(SystemTime::now()),
            snapshot_id,
            key_range: self.partition_store.partition_key_range().clone(),
            min_applied_lsn: snapshot.min_applied_lsn,
            db_comparator_name: snapshot.db_comparator_name.clone(),
            files: snapshot.files.clone(),
        };
        let metadata_json = serde_json::to_string_pretty(&snapshot_metadata)?;

        let snapshot_lsn = snapshot_metadata.min_applied_lsn;
        let metadata_path = snapshot_path.join("metadata.json");
        tokio::fs::write(metadata_path.clone(), metadata_json)
            .await
            .context("Writing snapshot metadata failed")?;

        self.snapshot_repository
            .put(
                self.partition_store.partition_id(),
                &snapshot_metadata,
                snapshot_path.as_path(),
            )
            .await
            .context("Snapshot repository upload failed")?;

        let previous_archived_snapshot_lsn = self.partition_store.get_archived_lsn().await?;
        let mut tx = self.partition_store.transaction();
        tx.put_archived_lsn(snapshot_lsn).await;
        tx.commit()
            .await
            .context("Updating archived snapshot LSN")?;
        trace!(
            %snapshot_id,
            previous_archived_lsn = ?previous_archived_snapshot_lsn,
            updated_archived_lsn = ?snapshot_lsn,
            "Updated persisted archived snapshot LSN"
        );

        let cleanup = tokio::fs::remove_dir_all(snapshot_path.clone()).await;
        match cleanup {
            Ok(_) => {
                debug!(%snapshot_id, "Cleaned up snapshot export directory: {:?}", snapshot_path);
            }
            Err(e) => {
                error!(%snapshot_id, "Failed to clean up snapshot directory: {}", e);
            }
        }

        info!(
            %snapshot_id,
            partition_id = ?self.partition_store.partition_id(),
            ?snapshot_lsn,
            "Successfully published partition snapshot"
        );
        Ok(snapshot_metadata)
    }
}
