// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod db_manager;
mod error;
mod rock_access;

pub use db_manager::RocksDbManager;
pub use error::*;
pub use rock_access::*;

use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use restate_types::config::RocksDbOptions;

use rocksdb::statistics::Histogram;
use rocksdb::statistics::HistogramData;
use rocksdb::statistics::Ticker;
use rocksdb::BlockBasedOptions;
use rocksdb::Cache;

#[derive(
    Debug,
    derive_more::Display,
    strum_macros::VariantArray,
    strum_macros::IntoStaticStr,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
)]
#[strum(serialize_all = "kebab-case")]
pub enum Owner {
    PartitionProcessor,
    Bifrost,
    MetadataStore,
}

#[derive(
    Debug,
    derive_more::Deref,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
)]
pub struct DbName(String);
impl DbName {
    pub fn new(name: &str) -> Self {
        Self(name.to_owned())
    }
}

#[derive(
    Debug,
    derive_more::Deref,
    derive_more::AsRef,
    derive_more::From,
    derive_more::Into,
    derive_more::Display,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
)]
pub struct CfName(String);
impl CfName {
    pub fn new(name: &str) -> Self {
        Self(name.to_owned())
    }
}

pub struct DbSpec<T> {
    pub name: DbName,
    pub owner: Owner,
    pub path: PathBuf,
    /// Options applied to the database _before_ applying RocksDbOptions loaded from disk/env.
    pub db_options: rocksdb::Options,
    /// Options of the column family are applied _before_ the values loaded from
    /// RocksDbOptions from disk/env. Those act as default values for if the option
    /// is defined in RockDbOptions.
    ///
    /// Overriding per-column family options from config file is not supported [yet].
    pub column_families: Vec<(CfName, rocksdb::Options)>,
    _phantom: std::marker::PhantomData<T>,
}

impl DbSpec<rocksdb::DB> {
    pub fn new_db(
        name: DbName,
        owner: Owner,
        path: PathBuf,
        db_options: rocksdb::Options,
        column_families: Vec<(CfName, rocksdb::Options)>,
    ) -> DbSpec<rocksdb::DB> {
        Self {
            name,
            owner,
            path,
            db_options,
            column_families,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl DbSpec<rocksdb::OptimisticTransactionDB> {
    pub fn new_optimistic_db(
        name: DbName,
        owner: Owner,
        path: PathBuf,
        db_options: rocksdb::Options,
        column_families: Vec<(CfName, rocksdb::Options)>,
    ) -> DbSpec<rocksdb::OptimisticTransactionDB> {
        Self {
            name,
            owner,
            path,
            db_options,
            column_families,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive(derive_more::Display, Clone)]
#[display(fmt = "{}::{}", owner, name)]
pub struct RocksDb {
    pub owner: Owner,
    pub name: DbName,
    pub db_options: rocksdb::Options,
    pub column_families: BTreeMap<CfName, rocksdb::Options>,
    db: Arc<dyn RocksAccess + Send + Sync + 'static>,
}

static_assertions::assert_impl_all!(RocksDb: Send, Sync);

impl Deref for RocksDb {
    type Target = Arc<dyn RocksAccess + Send + Sync + 'static>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl RocksDb {
    pub(crate) fn new<T>(spec: DbSpec<T>, db: Arc<T>) -> Self
    where
        T: RocksAccess + Send + Sync + 'static,
    {
        Self {
            owner: spec.owner,
            name: spec.name,
            db,
            db_options: spec.db_options,
            column_families: spec.column_families.into_iter().collect(),
        }
    }

    /// Returns the raw rocksdb handle, this should only be used for server operations that
    /// require direct access to rocksdb.
    pub fn inner(&self) -> &Arc<dyn RocksAccess + Send + Sync + 'static> {
        &self.db
    }

    pub fn cfs(&self) -> Vec<&CfName> {
        self.column_families.keys().collect()
    }

    pub fn get_histogram_data(&self, histogram: Histogram) -> HistogramData {
        self.db_options.get_histogram_data(histogram)
    }

    pub fn get_ticker_count(&self, ticker: Ticker) -> u64 {
        self.db_options.get_ticker_count(ticker)
    }

    pub fn get_statistics_str(&self) -> Option<String> {
        self.db_options.get_statistics()
    }
}

pub(crate) fn amend_db_options(db_options: &mut rocksdb::Options, opts: &RocksDbOptions) {
    db_options.create_if_missing(true);
    db_options.create_missing_column_families(true);

    if opts.rocksdb_num_threads() > 0 {
        db_options.increase_parallelism(opts.rocksdb_num_threads() as i32);
        db_options.set_max_background_jobs(opts.rocksdb_num_threads() as i32);
    }

    if !opts.rocksdb_disable_statistics() {
        db_options.enable_statistics();
        // Reasonable default, but we might expose this as a config in the future.
        db_options.set_statistics_level(rocksdb::statistics::StatsLevel::ExceptDetailedTimers);
    }

    //
    // Disable WAL archiving.
    // the following two options has to be both 0 to disable WAL log archive.
    //
    db_options.set_wal_size_limit_mb(0);
    db_options.set_wal_ttl_seconds(0);
    //
    if !opts.rocksdb_disable_wal() {
        // Disable automatic WAL flushing.
        // We will call flush manually, when we commit a storage transaction.
        //
        db_options.set_manual_wal_flush(opts.rocksdb_batch_wal_flushes());
        // Once the WAL logs exceed this size, rocksdb start will start flush memtables to disk.
        db_options.set_max_total_wal_size(opts.rocksdb_max_total_wal_size());
    }
    //
    // Let rocksdb decide for level sizes.
    //
    db_options.set_level_compaction_dynamic_level_bytes(true);
    db_options.set_compaction_readahead_size(1 << 21);
    //
    // We sometimes read from rocksdb directly in tokio threads, and
    // at most we have 512 threads.
    //
    db_options.set_table_cache_num_shard_bits(9);
}

pub(crate) fn amend_cf_options(
    cf_options: &mut rocksdb::Options,
    opts: &RocksDbOptions,
    cache: &Cache,
) {
    // write buffer
    //
    cf_options.set_write_buffer_size(opts.rocksdb_write_buffer_size());
    //
    // bloom filters and block cache.
    //
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, true);
    // use the latest Rocksdb table format.
    // https://github.com/facebook/rocksdb/blob/f059c7d9b96300091e07429a60f4ad55dac84859/include/rocksdb/table.h#L275
    block_opts.set_format_version(5);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_block_cache(cache);
    cf_options.set_block_based_table_factory(&block_opts);
}
