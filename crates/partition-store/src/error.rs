// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use codederror::CodedError;

use restate_core::ShutdownError;
use restate_rocksdb::RocksError;
use restate_storage_api::StorageError;
use restate_types::identifiers::PartitionId;

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("could not fetch a partition snapshot because snapshot repository was not configured")]
    SnapshotRepositoryRequired,
    #[error("a partition snapshot is required")]
    SnapshotRequired,
    #[error("partition snapshot was found but unsuitable; it was taken before the log trim point")]
    SnapshotUnsuitable,
    #[error("partition store for partition does not exist in local database")]
    NoLocalStore,
    #[error("open failed due to snapshot-related error: {0}")]
    Snapshot(#[from] anyhow::Error),
    #[error("open failed due to rocksdb error: {0}")]
    RocksDb(#[from] RocksError),
    #[error("open failed due to partition data error: {0}")]
    Storage(#[from] StorageError),
}

#[derive(Debug, thiserror::Error, CodedError)]
pub enum BuildError {
    #[error(transparent)]
    RocksDbManager(
        #[from]
        #[code]
        RocksError,
    ),
    #[error("db contains no storage format version")]
    #[code(restate_errors::RT0009)]
    MissingStorageFormatVersion,
    #[error("snapshot repository configuration error: {0}")]
    #[code(unknown)]
    Snapshots(anyhow::Error),
    #[error(transparent)]
    #[code(unknown)]
    Other(#[from] rocksdb::Error),
    #[error(transparent)]
    #[code(unknown)]
    Shutdown(#[from] ShutdownError),
}

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
    #[error("export error: {0}")]
    Export(#[source] anyhow::Error),
    #[error("snapshot repository IO error: {0}")]
    RepositoryIo(#[source] anyhow::Error),
    #[error("Internal error: {0}")]
    Internal(#[source] anyhow::Error),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

pub(crate) fn break_on_err<T, E>(
    r: std::result::Result<T, E>,
) -> ControlFlow<std::result::Result<(), E>, T> {
    match r {
        Ok(val) => ControlFlow::Continue(val),
        Err(err) => ControlFlow::Break(Err(err)),
    }
}
