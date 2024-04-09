// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::perf::MemoryUsageBuilder;
use rocksdb::ColumnFamilyDescriptor;

use crate::CfName;
use crate::DbSpec;
use crate::RocksError;

pub trait RocksAccess {
    fn open_db(db_spec: &DbSpec<Self>) -> Result<Self, RocksError>
    where
        Self: Sized;

    fn flush_memtables_atomically(&self, cfs: &[&CfName], wait: bool) -> Result<(), RocksError>;
    fn flush_wal(&self, sync: bool) -> Result<(), RocksError>;
    fn cancel_all_background_work(&self, wait: bool);
    fn set_options_cf(&self, cf: &CfName, opts: &[(&str, &str)]) -> Result<(), RocksError>;
    fn get_property_int_cf(&self, cf: &CfName, property: &str) -> Result<Option<u64>, RocksError>;
    fn record_memory_stats(&self, builder: &mut MemoryUsageBuilder);
}

impl RocksAccess for rocksdb::DB {
    fn open_db(db_spec: &DbSpec<Self>) -> Result<Self, RocksError> {
        let column_families = db_spec
            .column_families
            .iter()
            .map(|(name, opts)| ColumnFamilyDescriptor::new(name.clone(), opts.clone()));
        rocksdb::DB::open_cf_descriptors(&db_spec.db_options, &db_spec.path, column_families)
            .map_err(RocksError::from_rocksdb_error)
    }

    fn flush_memtables_atomically(&self, cfs: &[&CfName], wait: bool) -> Result<(), RocksError> {
        let mut flushopts = rocksdb::FlushOptions::default();
        flushopts.set_wait(wait);
        let cfs = cfs
            .iter()
            .filter_map(|name| self.cf_handle(name))
            .collect::<Vec<_>>();
        Ok(self.flush_cfs_opt(&cfs, &flushopts)?)
    }

    fn flush_wal(&self, sync: bool) -> Result<(), RocksError> {
        Ok(self.flush_wal(sync)?)
    }

    fn cancel_all_background_work(&self, wait: bool) {
        self.cancel_all_background_work(wait)
    }

    fn set_options_cf(&self, cf: &CfName, opts: &[(&str, &str)]) -> Result<(), RocksError> {
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.clone()));
        };
        Ok(self.set_options_cf(handle, opts)?)
    }

    fn get_property_int_cf(&self, cf: &CfName, property: &str) -> Result<Option<u64>, RocksError> {
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.clone()));
        };
        Ok(self.property_int_value_cf(handle, property)?)
    }

    fn record_memory_stats(&self, builder: &mut MemoryUsageBuilder) {
        builder.add_db(self)
    }
}

impl RocksAccess for rocksdb::OptimisticTransactionDB {
    fn open_db(db_spec: &DbSpec<Self>) -> Result<Self, RocksError> {
        let column_families = db_spec
            .column_families
            .iter()
            .map(|(name, opts)| ColumnFamilyDescriptor::new(name.clone(), opts.clone()));
        rocksdb::OptimisticTransactionDB::open_cf_descriptors(
            &db_spec.db_options,
            &db_spec.path,
            column_families,
        )
        .map_err(RocksError::from_rocksdb_error)
    }

    fn flush_memtables_atomically(&self, cfs: &[&CfName], wait: bool) -> Result<(), RocksError> {
        let mut flushopts = rocksdb::FlushOptions::default();
        flushopts.set_wait(wait);
        let cfs = cfs
            .iter()
            .filter_map(|name| self.cf_handle(name))
            .collect::<Vec<_>>();
        Ok(self.flush_cfs_opt(&cfs, &flushopts)?)
    }

    fn flush_wal(&self, sync: bool) -> Result<(), RocksError> {
        Ok(self.flush_wal(sync)?)
    }

    fn cancel_all_background_work(&self, wait: bool) {
        self.cancel_all_background_work(wait)
    }

    fn set_options_cf(&self, cf: &CfName, opts: &[(&str, &str)]) -> Result<(), RocksError> {
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.clone()));
        };
        Ok(self.set_options_cf(handle, opts)?)
    }

    fn get_property_int_cf(&self, cf: &CfName, property: &str) -> Result<Option<u64>, RocksError> {
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.clone()));
        };
        Ok(self.property_int_value_cf(handle, property)?)
    }

    fn record_memory_stats(&self, builder: &mut MemoryUsageBuilder) {
        builder.add_db(self)
    }
}
