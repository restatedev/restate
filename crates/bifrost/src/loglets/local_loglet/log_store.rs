// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use restate_rocksdb::{
    CfExactPattern, CfName, DbName, DbSpecBuilder, Owner, RocksDb, RocksDbManager, RocksError,
};
use restate_types::arc_util::Updateable;
use restate_types::config::RocksDbOptions;
use rocksdb::{BoundColumnFamily, DBCompressionType, DB};

use super::keys::{MetadataKey, MetadataKind};
use super::log_state::{log_state_full_merge, log_state_partial_merge, LogState};
use super::log_store_writer::LogStoreWriter;

// matches the default directory name
pub(crate) const DB_NAME: &str = "local-loglet";

pub(crate) const DATA_CF: &str = "logstore_data";
pub(crate) const METADATA_CF: &str = "logstore_metadata";

#[derive(Debug, Clone, thiserror::Error)]
pub enum LogStoreError {
    #[error(transparent)]
    // unfortunately, we have to use Arc here, because the bincode error is not Clone.
    Encode(#[from] Arc<bincode::error::EncodeError>),
    #[error(transparent)]
    // unfortunately, we have to use Arc here, because the bincode error is not Clone.
    Decode(#[from] Arc<bincode::error::DecodeError>),
    #[error(transparent)]
    Rocksdb(#[from] rocksdb::Error),
    #[error(transparent)]
    RocksDbManager(#[from] RocksError),
}

#[derive(Debug, Clone)]
pub struct RocksDbLogStore {
    rocksdb: Arc<RocksDb>,
}

impl RocksDbLogStore {
    pub fn new(
        data_dir: PathBuf,
        updateable_options: impl Updateable<RocksDbOptions> + Send + 'static,
    ) -> Result<Self, LogStoreError> {
        let db_manager = RocksDbManager::get();

        let cfs = vec![CfName::new(DATA_CF), CfName::new(METADATA_CF)];

        let db_spec =
            DbSpecBuilder::new(DbName::new(DB_NAME), Owner::Bifrost, data_dir, db_options())
                .add_cf_pattern(CfExactPattern::new(DATA_CF), cf_data_options)
                .add_cf_pattern(CfExactPattern::new(METADATA_CF), cf_metadata_options)
                // not very important but it's to reduce the number of merges by flushing.
                // it's also a small cf so it should be quick.
                .add_to_flush_on_shutdown(CfExactPattern::new(METADATA_CF))
                .ensure_column_families(cfs)
                .build_as_db();
        let db_name = db_spec.name().clone();
        // todo: use the returned rocksdb object when open_db returns Arc<RocksDb>
        let _ = db_manager.open_db(updateable_options, db_spec)?;
        let rocksdb = db_manager.get_db(Owner::Bifrost, db_name).unwrap();
        Ok(Self { rocksdb })
    }

    pub fn data_cf(&self) -> Arc<BoundColumnFamily> {
        self.rocksdb.cf_handle(DATA_CF).expect("DATA_CF exists")
    }

    pub fn metadata_cf(&self) -> Arc<BoundColumnFamily> {
        self.rocksdb
            .cf_handle(METADATA_CF)
            .expect("METADATA_CF exists")
    }

    pub fn get_log_state(&self, log_id: u64) -> Result<Option<LogState>, LogStoreError> {
        let metadata_cf = self.metadata_cf();
        let value = self.rocksdb.as_raw_db().get_pinned_cf(
            &metadata_cf,
            MetadataKey::new(log_id, MetadataKind::LogState).to_bytes(),
        )?;

        if let Some(value) = value {
            Ok(Some(LogState::from_slice(&value)?))
        } else {
            Ok(None)
        }
    }

    pub fn create_writer(&self, manual_wal_flush: bool) -> LogStoreWriter {
        LogStoreWriter::new(self.rocksdb.clone(), manual_wal_flush)
    }

    pub fn db(&self) -> &DB {
        self.rocksdb.as_raw_db()
    }
}

fn db_options() -> rocksdb::Options {
    let mut opts = rocksdb::Options::default();
    //
    // no need to retain 1000 log files by default.
    //
    opts.set_keep_log_file_num(10);
    opts
}

// todo: optimize
fn cf_data_options(mut opts: rocksdb::Options) -> rocksdb::Options {
    //
    // Set compactions per level
    //
    opts.set_num_levels(7);
    opts.set_compression_per_level(&[
        DBCompressionType::None,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Snappy,
        DBCompressionType::Zstd,
    ]);
    // most reads are sequential
    opts.set_advise_random_on_open(false);
    //
    opts
}

// todo: optimize
fn cf_metadata_options(mut opts: rocksdb::Options) -> rocksdb::Options {
    //
    // Set compactions per level
    //
    opts.set_num_levels(3);
    opts.set_compression_per_level(&[
        DBCompressionType::None,
        DBCompressionType::None,
        DBCompressionType::Zstd,
    ]);
    //
    // Most of the changes are highly temporal, we try to delay flushing
    // to merge metadata updates into fewer L0 files.
    opts.set_max_write_buffer_number(3);
    opts.set_min_write_buffer_number_to_merge(3);
    opts.set_max_successive_merges(10);
    // Merge operator for log state updates
    opts.set_merge_operator(
        "LogStateMerge",
        log_state_full_merge,
        log_state_partial_merge,
    );
    opts
}
