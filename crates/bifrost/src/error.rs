// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::{ShutdownError, SyncError};
use std::sync::Arc;

use restate_types::logs::{LogId, Lsn};

use crate::loglets::local_loglet::LogStoreError;
use crate::types::SealReason;

/// Result type for bifrost operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    #[error("log '{0}' is sealed")]
    LogSealed(LogId, SealReason),
    #[error("unknown log '{0}'")]
    UnknownLogId(LogId),
    #[error("invalid log sequence number '{0}'")]
    InvalidLsn(Lsn),
    #[error("operation failed due to an ongoing shutdown")]
    Shutdown(#[from] ShutdownError),
    #[error(transparent)]
    LogStoreError(#[from] LogStoreError),
    #[error("failed syncing logs metadata: {0}")]
    // unfortunately, we have to use Arc here, because the SyncError is not Clone.
    MetadataSync(#[from] Arc<SyncError>),
    /// Provider is unknown or disabled
    #[error("bifrost provider '{0}' is disabled or unrecognized")]
    Disabled(String),
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ProviderError {
    Shutdown(#[from] ShutdownError),
    Other(#[from] anyhow::Error),
}
