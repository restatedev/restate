// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use restate_bifrost::loglet::OperationError;
use restate_core::ShutdownError;
use restate_types::errors::MaybeRetryableError;
use restate_types::logs::LogletOffset;

use restate_rocksdb::RocksError;

use super::record_format::RecordDecodeError;

#[derive(Debug, thiserror::Error)]
pub enum RocksDbLogStoreError {
    #[error("cannot accept offset {0}")]
    InvalidOffset(LogletOffset),
    #[error(transparent)]
    Decode(#[from] RecordDecodeError),
    #[error(transparent)]
    Rocksdb(#[from] rocksdb::Error),
    #[error(transparent)]
    JsonDecode(#[from] serde_json::Error),
    #[error(transparent)]
    RocksDbManager(#[from] RocksError),
    #[error(transparent)]
    Shutdown(#[from] ShutdownError),
}

impl MaybeRetryableError for RocksDbLogStoreError {
    fn retryable(&self) -> bool {
        match self {
            Self::InvalidOffset(_) => false,
            Self::Decode(_) => false,
            Self::Rocksdb(_) => true,
            Self::RocksDbManager(_) => false,
            Self::JsonDecode(_) => false,
            Self::Shutdown(_) => false,
        }
    }
}

impl From<RocksDbLogStoreError> for OperationError {
    fn from(value: RocksDbLogStoreError) -> Self {
        match value {
            RocksDbLogStoreError::Shutdown(e) => OperationError::Shutdown(e),
            e => OperationError::Other(Arc::new(e)),
        }
    }
}
