// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::ShutdownError;
use thiserror::Error;

use restate_types::logs::{LogId, Lsn};

#[cfg(any(test, feature = "local_loglet"))]
use crate::loglets::local_loglet::LogStoreError;
use crate::types::SealReason;

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("log '{0}' is sealed")]
    LogSealed(LogId, SealReason),
    #[error("unknown log '{0}")]
    UnknownLogId(LogId),
    #[error("invalid log sequence number '{0}")]
    InvalidLsn(Lsn),
    #[error("cannot fetch log metadata")]
    MetadataSync,
    #[error("operation failed due to an ongoing shutdown")]
    Shutdown(#[from] ShutdownError),
    #[cfg(any(test, feature = "local_loglet"))]
    #[error(transparent)]
    LogStoreError(#[from] LogStoreError),
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ProviderError {
    Shutdown(#[from] ShutdownError),
    Other(#[from] anyhow::Error),
}
