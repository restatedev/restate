// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_core::ShutdownError;
use restate_types::errors::MaybeRetryableError;
use restate_types::logs::metadata::SegmentIndex;
use restate_types::logs::LogId;
use restate_types::replicated_loglet::ReplicatedLogletId;

use crate::loglet::OperationError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplicatedLogletError {
    #[error("cannot parse loglet configuration for log_id={0} at segment_index={1}: {2}")]
    LogletParamsParsingError(LogId, SegmentIndex, serde_json::Error),
    #[error("cannot find the tail of the loglet: {0}")]
    FindTailFailed(String),
    #[error("could not seal loglet_id={0}, insufficient nodes available for seal")]
    SealFailed(ReplicatedLogletId),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

impl MaybeRetryableError for ReplicatedLogletError {
    fn retryable(&self) -> bool {
        match self {
            Self::LogletParamsParsingError(..) => false,
            Self::SealFailed(..) => true,
            Self::FindTailFailed(..) => true,
            Self::Shutdown(_) => false,
        }
    }
}

impl From<ReplicatedLogletError> for OperationError {
    fn from(value: ReplicatedLogletError) -> Self {
        match value {
            ReplicatedLogletError::Shutdown(e) => OperationError::Shutdown(e),
            e => OperationError::Other(Arc::new(e)),
        }
    }
}

impl From<ReplicatedLogletError> for crate::Error {
    fn from(value: ReplicatedLogletError) -> Self {
        match value {
            ReplicatedLogletError::Shutdown(e) => crate::Error::Shutdown(e),
            e => crate::Error::LogletError(Arc::new(e)),
        }
    }
}
