// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::io;

use tokio::sync::{mpsc, oneshot};

use restate_types::{
    cluster::cluster_state::PartitionProcessorStatus,
    identifiers::{PartitionId, SnapshotId},
};

use crate::ShutdownError;

#[derive(Debug)]
pub enum ProcessorsManagerCommand {
    CreateSnapshot(PartitionId, oneshot::Sender<SnapshotResult>),
    GetState(oneshot::Sender<BTreeMap<PartitionId, PartitionProcessorStatus>>),
}

#[derive(Debug, Clone)]
pub struct ProcessorsManagerHandle(mpsc::Sender<ProcessorsManagerCommand>);

impl ProcessorsManagerHandle {
    pub fn new(sender: mpsc::Sender<ProcessorsManagerCommand>) -> Self {
        Self(sender)
    }

    pub async fn create_snapshot(&self, partition_id: PartitionId) -> SnapshotResult {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ProcessorsManagerCommand::CreateSnapshot(partition_id, tx))
            .await
            .map_err(|_| {
                SnapshotError::Internal(
                    partition_id,
                    "Unable to send command to PartitionProcessorManager".to_string(),
                )
            })?;
        rx.await.map_err(|_| {
            SnapshotError::Internal(partition_id, "Unable to receive response".to_string())
        })?
    }

    pub async fn get_state(
        &self,
    ) -> Result<BTreeMap<PartitionId, PartitionProcessorStatus>, ShutdownError> {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ProcessorsManagerCommand::GetState(tx))
            .await
            .map_err(|_| ShutdownError)?;
        rx.await.map_err(|_| ShutdownError)
    }
}

pub type SnapshotResult = Result<SnapshotCreated, SnapshotError>;

#[derive(Debug, Clone, derive_more::Display)]
#[display("{}", snapshot_id)]
pub struct SnapshotCreated {
    pub snapshot_id: SnapshotId,
    pub partition_id: PartitionId,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("Partition {0} not found")]
    PartitionNotFound(PartitionId),
    #[error("Snapshot creation already in progress for partition {0}")]
    SnapshotInProgress(PartitionId),
    #[error("Partition processor state does not allow snapshot export {0}")]
    InvalidState(PartitionId),
    #[error("Snapshot failed for partition {0}: {1}")]
    SnapshotExportError(PartitionId, #[source] anyhow::Error),
    #[error("Snapshot failed for partition {0}: {1}")]
    SnapshotMetadataHeaderError(PartitionId, #[source] io::Error),
    #[error("Internal error creating snapshot for partition {0}: {1}")]
    Internal(PartitionId, String),
}

impl SnapshotError {
    pub fn partition_id(&self) -> PartitionId {
        match self {
            SnapshotError::PartitionNotFound(partition_id) => *partition_id,
            SnapshotError::SnapshotInProgress(partition_id) => *partition_id,
            SnapshotError::InvalidState(partition_id) => *partition_id,
            SnapshotError::SnapshotExportError(partition_id, _) => *partition_id,
            SnapshotError::SnapshotMetadataHeaderError(partition_id, _) => *partition_id,
            SnapshotError::Internal(partition_id, _) => *partition_id,
        }
    }
}
