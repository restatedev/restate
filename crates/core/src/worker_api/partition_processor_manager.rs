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

use tokio::sync::{mpsc, oneshot};

use restate_types::logs::{LogId, Lsn};
use restate_types::{
    cluster::cluster_state::PartitionProcessorStatus,
    identifiers::{PartitionId, SnapshotId},
};

use crate::ShutdownError;

#[derive(Debug)]
pub enum ProcessorsManagerCommand {
    CreateSnapshot {
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
        tx: oneshot::Sender<SnapshotResult>,
    },
    GetState(oneshot::Sender<BTreeMap<PartitionId, PartitionProcessorStatus>>),
}

#[derive(Debug, Clone)]
pub struct ProcessorsManagerHandle(mpsc::Sender<ProcessorsManagerCommand>);

impl ProcessorsManagerHandle {
    pub fn new(sender: mpsc::Sender<ProcessorsManagerCommand>) -> Self {
        Self(sender)
    }

    pub async fn create_snapshot(
        &self,
        partition_id: PartitionId,
        min_target_lsn: Option<Lsn>,
    ) -> SnapshotResult {
        let (tx, rx) = oneshot::channel();
        self.0
            .send(ProcessorsManagerCommand::CreateSnapshot {
                partition_id,
                min_target_lsn,
                tx,
            })
            .await
            .map_err(|e| SnapshotError::Internal(e.into()))?;
        rx.await.map_err(|e| SnapshotError::Internal(e.into()))?
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
    pub log_id: LogId,
    pub min_applied_lsn: Lsn,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("Partition {0} not found")]
    PartitionNotFound(PartitionId),
    #[error("Snapshot export in progress for partition {0}")]
    SnapshotInProgress(PartitionId),
    #[error("Partition Processor state does not permit snapshotting of partition {0}")]
    InvalidState(PartitionId),
    #[error("Snapshot repository is not configured")]
    RepositoryNotConfigured,
    #[error("Snapshot export failed for partition {0}: {1}")]
    Export(PartitionId, #[source] anyhow::Error),
    #[error("Snapshot repository IO error: {1}")]
    RepositoryIo(PartitionId, #[source] anyhow::Error),
    #[error("Internal error: {0}")]
    Internal(anyhow::Error),
}
