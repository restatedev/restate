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

use restate_core::ShutdownError;
use restate_types::errors::MaybeRetryableError;
use restate_types::storage::{StorageDecodeError, StorageEncodeError};

use restate_rocksdb::RocksError;

use crate::logstore::LogStoreError;

#[derive(Debug, thiserror::Error)]
pub enum RocksDbLogStoreError {
    #[error(transparent)]
    Encode(#[from] StorageEncodeError),
    #[error(transparent)]
    Decode(#[from] StorageDecodeError),
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
            Self::Encode(_) => false,
            Self::Decode(_) => false,
            Self::Rocksdb(_) => true,
            Self::RocksDbManager(_) => false,
            Self::JsonDecode(_) => false,
            Self::Shutdown(_) => false,
        }
    }
}

impl From<RocksDbLogStoreError> for LogStoreError {
    fn from(value: RocksDbLogStoreError) -> Self {
        match value {
            RocksDbLogStoreError::Shutdown(e) => LogStoreError::Shutdown(e),
            e => LogStoreError::Other(Arc::new(e)),
        }
    }
}
