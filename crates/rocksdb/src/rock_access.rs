// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use rocksdb::perf::MemoryUsageBuilder;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::SingleThreaded;
use tracing::trace;

use crate::CfName;
use crate::DbSpec;
use crate::RocksError;

pub trait RocksAccess {
    fn open_db(
        db_spec: &DbSpec<Self>,
        default_cf_options: rocksdb::Options,
    ) -> Result<(Self, HashSet<CfName>), RocksError>
    where
        Self: Sized;

    fn flush_memtables(&self, cfs: &[&CfName], wait: bool) -> Result<(), RocksError>;
    fn flush_wal(&self, sync: bool) -> Result<(), RocksError>;
    fn cancel_all_background_work(&self, wait: bool);
    fn set_options_cf(&self, cf: &CfName, opts: &[(&str, &str)]) -> Result<(), RocksError>;
    fn get_property_int_cf(&self, cf: &CfName, property: &str) -> Result<Option<u64>, RocksError>;
    fn record_memory_stats(&self, builder: &mut MemoryUsageBuilder);
}

fn prepare_descriptors<T>(
    db_spec: &DbSpec<T>,
    default_cf_options: rocksdb::Options,
    all_cfs: &mut HashSet<CfName>,
) -> Result<Vec<ColumnFamilyDescriptor>, RocksError> {
    // Make sure default column family uses the global cache so that it doesn't create
    // its own cache (wastes ~32MB RSS per db)
    all_cfs.insert(CfName::new("default"));
    // Make sure we have all column families we were asked to open/create.
    all_cfs.extend(db_spec.ensure_column_families.iter().cloned());

    let mut descriptors = Vec::with_capacity(all_cfs.len());
    'outer: for cf in all_cfs.iter() {
        // try patterns one by one
        for (pattern, options_updater) in &db_spec.cf_patterns {
            if pattern.cf_matches(cf) {
                // Stop at first pattern match
                let cf_options = options_updater(default_cf_options.clone());
                descriptors.push(ColumnFamilyDescriptor::new(cf.as_str(), cf_options));
                continue 'outer;
            }
        }
        // default is special case
        if cf.as_str() == "default" {
            descriptors.push(ColumnFamilyDescriptor::new(
                cf.as_str(),
                default_cf_options.clone(),
            ));
            continue 'outer;
        }
        // We have no pattern for this cf
        return Err(RocksError::UnknownColumnFamily(cf.clone()));
    }

    Ok(descriptors)
}

impl RocksAccess for rocksdb::DB {
    fn open_db(
        db_spec: &DbSpec<Self>,
        default_cf_options: rocksdb::Options,
    ) -> Result<(Self, HashSet<CfName>), RocksError> {
        let mut all_cfs: HashSet<CfName> =
            match rocksdb::DB::list_cf(&db_spec.db_options, &db_spec.path) {
                Ok(existing) => existing.into_iter().map(Into::into).collect(),
                Err(e) => {
                    // Why it's okay to ignore this error? because we will attempt to open the
                    // database immediately after. If the database exists and we failed in reading
                    // the list of column families, rocksdb will fail on open (unless the list of
                    // column families we have in `ensure_column_families` exactly match what's in
                    // the database, in this case, it's okay to continue anyway)
                    trace!(
                        db = %db_spec.name,
                        owner = %db_spec.name,
                        "Couldn't list cfs: {}", e);
                    HashSet::with_capacity(
                        db_spec.ensure_column_families.len() + 1, /* +1 for default */
                    )
                }
            };

        let descriptors = prepare_descriptors(db_spec, default_cf_options, &mut all_cfs)?;

        Ok((
            rocksdb::DB::open_cf_descriptors(&db_spec.db_options, &db_spec.path, descriptors)
                .map_err(RocksError::from_rocksdb_error)?,
            all_cfs,
        ))
    }

    fn flush_memtables(&self, cfs: &[&CfName], wait: bool) -> Result<(), RocksError> {
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
    fn open_db(
        db_spec: &DbSpec<Self>,
        default_cf_options: rocksdb::Options,
    ) -> Result<(Self, HashSet<CfName>), RocksError> {
        // copy pasta from DB, this will be removed as soon we as we remove the use of
        // Optimistic Transaction DB
        let mut all_cfs: HashSet<CfName> =
            match rocksdb::OptimisticTransactionDB::<SingleThreaded>::list_cf(
                &db_spec.db_options,
                &db_spec.path,
            ) {
                Ok(existing) => existing.into_iter().map(Into::into).collect(),
                Err(e) => {
                    // Why it's okay to ignore this error? because we will attempt to open the
                    // database immediately after. If the database exists and we failed in reading
                    // the list of column families, rocksdb will fail on open (unless the list of
                    // column families we have in `ensure_column_families` exactly match what's in
                    // the database, in this case, it's okay to continue anyway)
                    trace!(
                        db = %db_spec.name,
                        owner = %db_spec.name,
                        "Couldn't list cfs: {}", e);
                    HashSet::with_capacity(
                        db_spec.ensure_column_families.len() + 1, /* +1 for default */
                    )
                }
            };

        let descriptors = prepare_descriptors(db_spec, default_cf_options, &mut all_cfs)?;

        Ok((
            rocksdb::OptimisticTransactionDB::open_cf_descriptors(
                &db_spec.db_options,
                &db_spec.path,
                descriptors,
            )
            .map_err(RocksError::from_rocksdb_error)?,
            all_cfs,
        ))
    }

    fn flush_memtables(&self, cfs: &[&CfName], wait: bool) -> Result<(), RocksError> {
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
