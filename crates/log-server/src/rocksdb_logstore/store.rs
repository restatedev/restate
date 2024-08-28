// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// todo: remove after scaffolding is complete
#![allow(unused)]

use std::sync::Arc;

use rocksdb::{BoundColumnFamily, WriteBatch, WriteOptions, DB};

use restate_rocksdb::{IoMode, Priority, RocksDb};
use restate_types::config::LogServerOptions;
use restate_types::live::BoxedLiveLoad;

use super::keys::MARKER_KEY;
use super::writer::RocksDbLogWriterHandle;
use super::{RocksDbLogStoreError, DATA_CF, METADATA_CF};
use crate::logstore::{LogStore, LogStoreError};
use crate::metadata::LogStoreMarker;

#[derive(Clone)]
pub struct RocksDbLogStore {
    pub(super) updateable_options: BoxedLiveLoad<LogServerOptions>,
    pub(super) rocksdb: Arc<RocksDb>,
    pub(super) writer_handle: RocksDbLogWriterHandle,
}

impl RocksDbLogStore {
    pub fn data_cf(&self) -> Arc<BoundColumnFamily> {
        self.rocksdb
            .inner()
            .cf_handle(DATA_CF)
            .expect("DATA_CF exists")
    }

    pub fn metadata_cf(&self) -> Arc<BoundColumnFamily> {
        self.rocksdb
            .inner()
            .cf_handle(METADATA_CF)
            .expect("METADATA_CF exists")
    }

    pub fn db(&self) -> &DB {
        self.rocksdb.inner().as_raw_db()
    }
}
impl LogStore for RocksDbLogStore {
    async fn load_marker(&self) -> Result<Option<LogStoreMarker>, LogStoreError> {
        let metadata_cf = self.metadata_cf();
        let value = self
            .db()
            .get_pinned_cf(&metadata_cf, MARKER_KEY)
            .map_err(RocksDbLogStoreError::from)?;

        let marker = value
            .map(LogStoreMarker::from_slice)
            .transpose()
            .map_err(RocksDbLogStoreError::from)?;
        Ok(marker)
    }

    async fn store_marker(&self, marker: LogStoreMarker) -> Result<(), LogStoreError> {
        let mut batch = WriteBatch::default();
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false);
        write_opts.set_sync(true);
        batch.put_cf(&self.metadata_cf(), MARKER_KEY, marker.to_bytes());

        self.rocksdb
            .write_batch(
                "logstore-write-batch",
                Priority::High,
                IoMode::default(),
                write_opts,
                batch,
            )
            .await
            .map_err(RocksDbLogStoreError::from)?;
        Ok(())
    }
}
