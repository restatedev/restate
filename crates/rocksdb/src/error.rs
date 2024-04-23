// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use codederror::CodedError;
use restate_core::ShutdownError;

use crate::CfName;

#[derive(Debug, Clone, thiserror::Error, CodedError)]
pub enum RocksError {
    #[error("db is locked: {0}")]
    #[code(restate_errors::RT0005)]
    DbLocked(rocksdb::Error),
    #[error(transparent)]
    #[code(unknown)]
    Shutdown(#[from] ShutdownError),
    #[error("unknown column family: {0}")]
    #[code(unknown)]
    UnknownColumnFamily(CfName),
    #[error(transparent)]
    #[code(unknown)]
    Other(#[from] rocksdb::Error),
}

impl RocksError {
    pub(crate) fn from_rocksdb_error(err: rocksdb::Error) -> Self {
        let err_message = err.to_string();

        if err_message.starts_with("IO error: While lock file:")
            && err_message.ends_with("Resource temporarily unavailable")
        {
            Self::DbLocked(err)
        } else {
            Self::Other(err)
        }
    }
}
