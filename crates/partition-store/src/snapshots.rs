// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod metadata;
mod repository;
mod snapshot_task;

pub use self::metadata::*;
pub use self::repository::SnapshotRepository;
pub use self::snapshot_task::*;

use restate_types::identifiers::PartitionId;

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
