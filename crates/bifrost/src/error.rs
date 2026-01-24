// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use restate_core::{ShutdownError, SyncError};
use restate_metadata_store::ReadWriteError;
use restate_types::clock;
use restate_types::errors::MaybeRetryableError;
use restate_types::logs::builder::BuilderError;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::{LogId, Lsn};

use crate::loglet::OperationError;

/// Result type for bifrost operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, thiserror::Error, Debug)]
pub enum Error {
    #[error("metadata store doesn't have an entry for log metadata")]
    LogsMetadataNotProvisioned,
    #[error("unknown log '{0}'")]
    UnknownLogId(LogId),
    #[error("invalid log sequence number '{0}'")]
    InvalidLsn(Lsn),
    #[error("operation failed due to an ongoing shutdown")]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    LogletError(#[from] Arc<dyn MaybeRetryableError + Send + Sync>),
    #[error("failed syncing logs metadata: {0}")]
    MetadataSync(#[from] SyncError),
    /// Provider is unknown or disabled
    #[error("bifrost provider '{0}' is disabled or unrecognized")]
    Disabled(String),
    #[error(transparent)]
    AdminError(#[from] AdminError),
    #[error(transparent)]
    MetadataStoreError(#[from] Arc<ReadWriteError>),
    #[error("record batch too large: {batch_size_bytes} bytes exceeds limit of {limit} bytes")]
    BatchTooLarge {
        batch_size_bytes: usize,
        limit: NonZeroUsize,
    },
    #[error("{0}")]
    Other(String),
}

#[derive(thiserror::Error, Debug)]
pub enum EnqueueError<T> {
    #[error("the operation rejected due to backpressure")]
    Full(T),
    #[error("appender is draining, closed, or crashed")]
    Closed(T),
    #[error("record too large: {record_size} bytes exceeds limit of {limit} bytes")]
    RecordTooLarge {
        record_size: usize,
        limit: NonZeroUsize,
    },
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum AdminError {
    #[error("log {0} is permanently sealed")]
    ChainPermanentlySealed(LogId),
    #[error("could not seal the loglet or the chain")]
    ChainSealIncomplete,
    #[error("log {0} already exists")]
    LogAlreadyExists(LogId),
    #[error("segment conflicts with existing segment with base_lsn={0}")]
    SegmentConflict(Lsn),
    #[error("segment index found in metadata does not match expected {expected}!={found}")]
    SegmentMismatch {
        expected: SegmentIndex,
        found: SegmentIndex,
    },
    #[error("loglet params could not be deserialized: {0}")]
    ParamsSerde(#[from] Arc<serde_json::Error>),
    #[error("logs HLC clock error: {0}")]
    LogsHlcClock(clock::Error),
}

impl From<OperationError> for Error {
    fn from(value: OperationError) -> Self {
        match value {
            OperationError::Shutdown(e) => Error::Shutdown(e),
            OperationError::Other(e) => Error::LogletError(e),
        }
    }
}

impl From<BuilderError> for AdminError {
    fn from(value: BuilderError) -> Self {
        match value {
            BuilderError::LogAlreadyExists(log_id) => AdminError::LogAlreadyExists(log_id),
            BuilderError::ChainPermanentlySealed(log_id) => {
                AdminError::ChainPermanentlySealed(log_id)
            }
            BuilderError::ParamsSerde(error) => AdminError::ParamsSerde(Arc::new(error)),
            BuilderError::SegmentConflict(lsn) => AdminError::SegmentConflict(lsn),
            BuilderError::HlcClock(err) => AdminError::LogsHlcClock(err),
        }
    }
}

impl From<ReadWriteError> for Error {
    fn from(value: ReadWriteError) -> Self {
        Error::MetadataStoreError(Arc::new(value))
    }
}
