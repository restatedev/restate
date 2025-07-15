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
use std::path::PathBuf;
use std::sync::Arc;

use rocksdb::perf::MemoryUsageBuilder;
use rocksdb::{ColumnFamilyDescriptor, ImportColumnFamilyOptions};
use rocksdb::{CompactOptions, ExportImportFilesMetaData};
use tokio::time::Instant;
use tracing::{debug, info, trace, warn};

use crate::DbSpec;
use crate::RocksError;
use crate::{BoxedCfMatcher, RawRocksDb};
use crate::{BoxedCfOptionUpdater, DbName};
use crate::{CfName, RocksDbPerfGuard};

/// Operations in this trait can be IO blocking, prefer using `RocksDb` for efficient async access
/// to the database.
#[derive(derive_more::Display, derive_more::Debug)]
#[display("{}", db_spec.name)]
#[debug("RocksDb({} at {}", db_spec.name, db_spec.path.display())]
pub struct RocksAccess {
    db_spec: DbSpec,
    db: RawRocksDb,
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

impl RocksAccess {
    pub(crate) fn db_options(&self) -> &rocksdb::Options {
        &self.db_spec.db_options
    }

    pub fn path(&self) -> &PathBuf {
        &self.db_spec.path
    }

    pub fn name(&self) -> &DbName {
        &self.db_spec.name
    }

    pub fn open_db(
        db_spec: DbSpec,
        default_cf_options: rocksdb::Options,
    ) -> Result<Self, RocksError> {
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

        let descriptors = prepare_descriptors(&db_spec, default_cf_options, &mut all_cfs)?;

        rocksdb::DB::open_cf_descriptors(&db_spec.db_options, &db_spec.path, descriptors)
            .map(|db| RocksAccess { db, db_spec })
            .map_err(RocksError::from_rocksdb_error)
    }

    pub fn spec(&self) -> &DbSpec {
        &self.db_spec
    }

    pub fn cf_handle(&self, cf: &str) -> Option<Arc<rocksdb::BoundColumnFamily>> {
        self.db.cf_handle(cf)
    }

    pub fn as_raw_db(&self) -> &RawRocksDb {
        &self.db
    }

    pub fn open_cf(
        &self,
        name: CfName,
        default_cf_options: rocksdb::Options,
    ) -> Result<(), RocksError> {
        let options = prepare_cf_options(&self.db_spec.cf_patterns, default_cf_options, &name)?;
        Ok(RawRocksDb::create_cf(&self.db, name.as_str(), &options)?)
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub fn shutdown(&self) {
        let _x = RocksDbPerfGuard::new("shutdown");
        if let Err(e) = self.db.flush_wal(true) {
            warn!(
                db = %self.name(),
                "Failed to flush rocksdb WAL: {}",
                e
            );
        }

        let cfs_to_flush = self
            .cfs()
            .into_iter()
            .filter(|c| {
                self.db_spec
                    .flush_on_shutdown
                    .iter()
                    .any(|matcher| matcher.cf_matches(c))
            })
            .collect::<Vec<_>>();
        if cfs_to_flush.is_empty() {
            debug!(
                db = %self.name(),
                "No column families to flush for db on shutdown"
            );
            return;
        } else {
            let start = Instant::now();

            debug!(
        db = %self.name(),
        "Number of column families to flush on shutdown: {}", cfs_to_flush.len());
            if let Err(e) = self.flush_memtables(cfs_to_flush.as_slice(), true) {
                warn!(
                    db = %self.name(),
                    "Failed to flush memtables: {}",
                    e
                );
            } else {
                info!(
                    db = %self.name(),
                    "{} column families flushed in {:?}",
                    cfs_to_flush.len(),
                    start.elapsed(),
                );
            }
        }
        self.db.cancel_all_background_work(true);
        info!("Rocksdb '{}' was gracefully closed", self.name());
    }

    pub fn import_cf(
        &self,
        name: CfName,
        default_cf_options: rocksdb::Options,
        metadata: ExportImportFilesMetaData,
    ) -> Result<(), RocksError> {
        let options = prepare_cf_options(&self.db_spec.cf_patterns, default_cf_options, &name)?;

        let mut import_opts = ImportColumnFamilyOptions::default();
        import_opts.set_move_files(true);

        Ok(RawRocksDb::create_column_family_with_import(
            &self.db,
            &options,
            name.as_str(),
            &import_opts,
            &metadata,
        )?)
    }

    pub fn flush_memtables(&self, cfs: &[CfName], wait: bool) -> Result<(), RocksError> {
        let mut flushopts = rocksdb::FlushOptions::default();
        flushopts.set_wait(wait);
        let cfs = cfs
            .iter()
            .filter_map(|name| self.cf_handle(name))
            .collect::<Vec<_>>();
        // a side effect of the awkward rust-rocksdb interface!
        let cf_refs = cfs.iter().collect::<Vec<_>>();
        Ok(self.db.flush_cfs_opt(&cf_refs, &flushopts)?)
    }

    pub fn flush_wal(&self, sync: bool) -> Result<(), RocksError> {
        Ok(self.db.flush_wal(sync)?)
    }

    pub fn flush_all(&self) -> Result<(), RocksError> {
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
        Ok(self.db.flush_cfs_opt(&cf_refs, &flushopts)?)
    }

    pub fn compact_all(&self) {
        let opts = CompactOptions::default();
        self.cfs()
            .iter()
            .filter_map(|name| self.cf_handle(name))
            .for_each(|cf| {
                self.db
                    .compact_range_cf_opt::<&str, &str>(&cf, None, None, &opts)
            });
    }

    // pub fn cancel_all_background_work(&self, wait: bool) {
    //     self.db.cancel_all_background_work(wait)
    // }

    pub fn set_options_cf(&self, cf: &CfName, opts: &[(&str, &str)]) -> Result<(), RocksError> {
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.clone()));
        };
        Ok(self.db.set_options_cf(&handle, opts)?)
    }

    pub fn get_property_int_cf(
        &self,
        cf: &CfName,
        property: &str,
    ) -> Result<Option<u64>, RocksError> {
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.clone()));
        };
        Ok(self.db.property_int_value_cf(&handle, property)?)
    }

    pub fn record_memory_stats(&self, builder: &mut MemoryUsageBuilder) {
        builder.add_db(&self.db)
    }

    pub fn cfs(&self) -> Vec<CfName> {
        self.db.cf_names().into_iter().map(CfName::from).collect()
    }

    pub fn write_batch(
        &self,
        batch: &rocksdb::WriteBatch,
        write_options: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        self.db.write_opt(batch, write_options)
    }

    pub fn write_batch_with_index(
        &self,
        batch: &rocksdb::WriteBatchWithIndex,
        write_options: &rocksdb::WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        self.db.write_wbwi_opt(batch, write_options)
    }
}
