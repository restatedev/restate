// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;

use crate::types::SealReason;
use crate::{LogId, Lsn};

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
}
