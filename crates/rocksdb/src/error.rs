// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use tracing::warn;

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
    #[error("already open")]
    #[code(unknown)]
    AlreadyOpen,
    #[error("already exists")]
    #[code(unknown)]
    ColumnFamilyExists,
    #[error("invalid key range for partition")]
    #[code(unknown)]
    SnapshotKeyRangeMismatch,
    #[error("export column family returned an empty set of SST files")]
    #[code(unknown)]
    SnapshotEmpty,
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
            if err_message.contains("Direct I/O is not supported") {
                warn!(
                    r#"""Direct I/O can be disabled by configuring rocks-db-disable-direct-io-for-reads
                        and rocksdb-disable-direct-io-for-flush-and-compactions. RocksDB is better run
                        with Direct I/O enabled, are you running on an encrypted fs?"""#
                );
            }
            Self::Other(err)
        }
    }
}
