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

use rocksdb::statistics::Histogram;
use rocksdb::statistics::HistogramData;
use rocksdb::statistics::Ticker;

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
