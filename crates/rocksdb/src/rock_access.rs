// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use rocksdb::perf::MemoryUsageBuilder;
use rocksdb::{ColumnFamilyDescriptor, ImportColumnFamilyOptions};
use rocksdb::{CompactOptions, ExportImportFilesMetaData};
use tracing::trace;

use crate::BoxedCfMatcher;
use crate::BoxedCfOptionUpdater;
use crate::CfName;
use crate::DbSpec;
use crate::RocksError;

/// Operations in this trait can be IO blocking, prefer using `RocksDb` for efficient async access
/// to the database.
pub trait RocksAccess {
    fn open_db(db_spec: &DbSpec, default_cf_options: rocksdb::Options) -> Result<Self, RocksError>
    where
        Self: Sized;
    fn cf_handle(&self, cf: &str) -> Option<Arc<rocksdb::BoundColumnFamily>>;
    // todo: remove when we no longer need access to the raw db
    fn as_raw_db(&self) -> &rocksdb::DB;
    fn flush_all(&self) -> Result<(), RocksError>;
    fn flush_memtables(&self, cfs: &[CfName], wait: bool) -> Result<(), RocksError>;
    fn flush_wal(&self, sync: bool) -> Result<(), RocksError>;
    fn compact_all(&self);
    fn cancel_all_background_work(&self, wait: bool);
    fn set_options_cf(&self, cf: &CfName, opts: &[(&str, &str)]) -> Result<(), RocksError>;
    fn get_property_int_cf(&self, cf: &CfName, property: &str) -> Result<Option<u64>, RocksError>;
    fn record_memory_stats(&self, builder: &mut MemoryUsageBuilder);
    /// This is a blocking operation and it's not meant to be called concurrently on the same
    /// database, although it's not dangerous to do so. The only impact would be the one of the
    /// callers will get an error.
    fn open_cf(
        &self,
        name: CfName,
        default_cf_options: rocksdb::Options,
        cf_patterns: &[(BoxedCfMatcher, BoxedCfOptionUpdater)],
    ) -> Result<(), RocksError>;
    /// Create a column family from a snapshot. The data files referenced by
    /// `metadata` will be moved into the RocksDB data directory.
    fn import_cf(
        &self,
        name: CfName,
        default_cf_options: rocksdb::Options,
        cf_patterns: &[(BoxedCfMatcher, BoxedCfOptionUpdater)],
        metadata: ExportImportFilesMetaData,
    ) -> Result<(), RocksError>;
    fn cfs(&self) -> Vec<CfName>;

    fn write_batch(
        &self,
        batch: &rocksdb::WriteBatch,
        write_options: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error>;

    fn write_batch_with_index(
        &self,
        batch: &rocksdb::WriteBatchWithIndex,
        write_options: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error>;
}

fn prepare_cf_options(
    cf_patterns: &[(BoxedCfMatcher, BoxedCfOptionUpdater)],
    default_cf_options: rocksdb::Options,
    cf: &CfName,
) -> Result<rocksdb::Options, RocksError> {
    // try patterns one by one
    for (pattern, options_updater) in cf_patterns {
        if pattern.cf_matches(cf) {
            // Stop at first pattern match
            return Ok(options_updater(default_cf_options));
        }
    }
    // default is special case
    if cf.as_str() == "default" {
        return Ok(default_cf_options);
    }
    // We have no pattern for this cf
    Err(RocksError::UnknownColumnFamily(cf.clone()))
}

fn prepare_descriptors(
    db_spec: &DbSpec,
    default_cf_options: rocksdb::Options,
    all_cfs: &mut HashSet<CfName>,
) -> Result<Vec<ColumnFamilyDescriptor>, RocksError> {
    // Make sure default column family uses the global cache so that it doesn't create
    // its own cache (wastes ~32MB RSS per db)
    all_cfs.insert(CfName::new("default"));
    // Make sure we have all column families we were asked to open/create.
    all_cfs.extend(db_spec.ensure_column_families.iter().cloned());

    let mut descriptors = Vec::with_capacity(all_cfs.len());
    for cf in all_cfs.iter() {
        let cf_options = prepare_cf_options(&db_spec.cf_patterns, default_cf_options.clone(), cf)?;
        descriptors.push(ColumnFamilyDescriptor::new(cf.as_str(), cf_options));
    }

    Ok(descriptors)
}

impl RocksAccess for rocksdb::DB {
    fn open_db(db_spec: &DbSpec, default_cf_options: rocksdb::Options) -> Result<Self, RocksError> {
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

        rocksdb::DB::open_cf_descriptors(&db_spec.db_options, &db_spec.path, descriptors)
            .map_err(RocksError::from_rocksdb_error)
    }

    fn cf_handle(&self, cf: &str) -> Option<Arc<rocksdb::BoundColumnFamily>> {
        self.cf_handle(cf)
    }

    fn as_raw_db(&self) -> &rocksdb::DB {
        self
    }

    fn open_cf(
        &self,
        name: CfName,
        default_cf_options: rocksdb::Options,
        cf_patterns: &[(BoxedCfMatcher, BoxedCfOptionUpdater)],
    ) -> Result<(), RocksError> {
        let options = prepare_cf_options(cf_patterns, default_cf_options, &name)?;
        Ok(Self::create_cf(self, name.as_str(), &options)?)
    }

    fn import_cf(
        &self,
        name: CfName,
        default_cf_options: rocksdb::Options,
        cf_patterns: &[(BoxedCfMatcher, BoxedCfOptionUpdater)],
        metadata: ExportImportFilesMetaData,
    ) -> Result<(), RocksError> {
        let options = prepare_cf_options(cf_patterns, default_cf_options, &name)?;

        let mut import_opts = ImportColumnFamilyOptions::default();
        import_opts.set_move_files(true);

        Ok(Self::create_column_family_with_import(
            self,
            &options,
            name.as_str(),
            &import_opts,
            &metadata,
        )?)
    }

    fn flush_memtables(&self, cfs: &[CfName], wait: bool) -> Result<(), RocksError> {
        let mut flushopts = rocksdb::FlushOptions::default();
        flushopts.set_wait(wait);
        let cfs = cfs
            .iter()
            .filter_map(|name| self.cf_handle(name))
            .collect::<Vec<_>>();
        // a side effect of the awkward rust-rocksdb interface!
        let cf_refs = cfs.iter().collect::<Vec<_>>();
        Ok(self.flush_cfs_opt(&cf_refs, &flushopts)?)
    }

    fn flush_wal(&self, sync: bool) -> Result<(), RocksError> {
        Ok(self.flush_wal(sync)?)
    }

    fn flush_all(&self) -> Result<(), RocksError> {
        self.flush_wal(true)?;

        let mut flushopts = rocksdb::FlushOptions::default();
        flushopts.set_wait(true);
        let cfs = self
            .cfs()
            .iter()
            .filter_map(|name| self.cf_handle(name))
            .collect::<Vec<_>>();
        // a side effect of the awkward rust-rocksdb interface!
        let cf_refs = cfs.iter().collect::<Vec<_>>();
        Ok(self.flush_cfs_opt(&cf_refs, &flushopts)?)
    }

    fn compact_all(&self) {
        let opts = CompactOptions::default();
        self.cfs()
            .iter()
            .filter_map(|name| self.cf_handle(name))
            .for_each(|cf| self.compact_range_cf_opt::<&str, &str>(&cf, None, None, &opts));
    }

    fn cancel_all_background_work(&self, wait: bool) {
        self.cancel_all_background_work(wait)
    }

    fn set_options_cf(&self, cf: &CfName, opts: &[(&str, &str)]) -> Result<(), RocksError> {
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.clone()));
        };
        Ok(self.set_options_cf(&handle, opts)?)
    }

    fn get_property_int_cf(&self, cf: &CfName, property: &str) -> Result<Option<u64>, RocksError> {
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.clone()));
        };
        Ok(self.property_int_value_cf(&handle, property)?)
    }

    fn record_memory_stats(&self, builder: &mut MemoryUsageBuilder) {
        builder.add_db(self)
    }

    fn cfs(&self) -> Vec<CfName> {
        self.cf_names().into_iter().map(CfName::from).collect()
    }

    fn write_batch(
        &self,
        batch: &rocksdb::WriteBatch,
        write_options: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        self.write_opt(batch, write_options)
    }

    fn write_batch_with_index(
        &self,
        batch: &rocksdb::WriteBatchWithIndex,
        write_options: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        self.write_wbwi_opt(batch, write_options)
    }
}
