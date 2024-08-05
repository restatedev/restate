// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_core::{ShutdownError, SyncError};
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, Lsn};

use crate::loglet::{LogletError, OperationError};

/// Result type for bifrost operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("log '{0}' is sealed")]
    LogSealed(LogId),
    #[error("unknown log '{0}'")]
    UnknownLogId(LogId),
    #[error("invalid log sequence number '{0}'")]
    InvalidLsn(Lsn),
    #[error("operation failed due to an ongoing shutdown")]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    LogletError(#[from] Arc<dyn LogletError + Send + Sync>),
    #[error("failed syncing logs metadata: {0}")]
    MetadataSync(#[from] SyncError),
    /// Provider is unknown or disabled
    #[error("bifrost provider '{0}' is disabled or unrecognized")]
    Disabled(String),
    #[error("read() at {0} failed waiting on reconfiguration of log {1}")]
    ReadFailureDuringReconfiguration(LogId, Lsn),
    #[error(transparent)]
    AdminError(#[from] AdminError),
    #[error(transparent)]
    MetadataStoreError(#[from] restate_core::metadata_store::ReadWriteError),
}

#[derive(thiserror::Error, Debug)]
pub enum EnqueueError<T> {
    #[error("the operation rejected due to backpressure")]
    WouldBlock(T),
    #[error("appender is draining, closed, or crashed")]
    Closed(T),
}

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("segment conflicts with existing segment with base_lsn={0}")]
    SegmentConflict(Lsn),
    #[error("segment index found in metadata does not match expected {expected}!={found}")]
    SegmentMismatch {
        expected: SegmentIndex,
        found: SegmentIndex,
    },
}

impl From<OperationError> for Error {
    fn from(value: OperationError) -> Self {
        match value {
            OperationError::Shutdown(e) => Error::Shutdown(e),
            OperationError::Other(e) => Error::LogletError(e),
        }
    }
}
