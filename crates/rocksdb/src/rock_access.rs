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
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, ImportColumnFamilyOptions, WriteBufferManager,
};
use rocksdb::{CompactOptions, ExportImportFilesMetaData};
use tokio::time::Instant;
use tracing::{debug, info, trace, warn};

use crate::DbName;
use crate::DbSpec;
use crate::RawRocksDb;
use crate::RocksError;
use crate::configuration::create_default_cf_options;
use crate::{CfName, RocksDbPerfGuard};

/// Operations in this wrapper can be IO blocking, prefer using [`crate::RocksDb`]
/// for async access to the database.
#[derive(derive_more::Display, derive_more::Debug)]
#[display("{}", db_spec.name)]
#[debug("RocksDb({} at {}", db_spec.name, db_spec.path.display())]
pub struct RocksAccess {
    db_spec: DbSpec,
    db_options: rocksdb::Options,
    db: RawRocksDb,
}

fn prepare_cf_options(
    cf: &CfName,
    db_spec: &DbSpec,
    write_buffer_manager: &WriteBufferManager,
    global_cache: &Cache,
) -> Result<rocksdb::Options, RocksError> {
    let cf_patterns = &db_spec.cf_patterns;
    let db_name = &db_spec.name;
    // try patterns one by one
    for (pattern, options_updater) in cf_patterns {
        if pattern.cf_matches(cf) {
            // Stop at first pattern match
            return Ok(options_updater.get_cf_options(
                db_name,
                cf,
                global_cache,
                write_buffer_manager,
            ));
        }
    }
    // default is special case
    if cf.as_str() == "default" {
        // the goal here is to use the global cache for the default column family, otherwise
        // rocksdb will create a new cache, wasting ~32MB RSS per db.
        let mut cf_options = create_default_cf_options(Some(write_buffer_manager));
        cf_options.set_write_buffer_manager(write_buffer_manager);

        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(global_cache);
        cf_options.set_block_based_table_factory(&block_opts);

        return Ok(cf_options);
    }
    // We have no pattern for this cf
    Err(RocksError::UnknownColumnFamily(cf.clone()))
}

fn prepare_descriptors(
    db_spec: &DbSpec,
    write_buffer_manager: &WriteBufferManager,
    global_cache: &Cache,
    all_cfs: &mut HashSet<CfName>,
) -> Result<Vec<ColumnFamilyDescriptor>, RocksError> {
    // Make sure default column family uses the global cache so that it doesn't create
    // its own cache (wastes ~32MB RSS per db)
    all_cfs.insert(CfName::new("default"));
    // Make sure we have all column families we were asked to open/create.
    all_cfs.extend(db_spec.ensure_column_families.iter().cloned());

    let mut descriptors = Vec::with_capacity(all_cfs.len());
    for cf in all_cfs.iter() {
        let cf_options = prepare_cf_options(cf, db_spec, write_buffer_manager, global_cache)?;
        descriptors.push(ColumnFamilyDescriptor::new(cf.as_str(), cf_options));
    }

    Ok(descriptors)
}

impl RocksAccess {
    pub(crate) fn db_options(&self) -> &rocksdb::Options {
        &self.db_options
    }

    pub fn path(&self) -> &PathBuf {
        &self.db_spec.path
    }

    pub fn name(&self) -> &DbName {
        &self.db_spec.name
    }

    pub fn open_db(
        db_spec: DbSpec,
        env: &rocksdb::Env,
        write_buffer_manager: &WriteBufferManager,
        global_cache: &Cache,
    ) -> Result<Self, RocksError> {
        let db_options =
            db_spec
                .db_configurator
                .get_db_options(db_spec.name(), env, write_buffer_manager);
        let mut all_cfs: HashSet<CfName> = match rocksdb::DB::list_cf(&db_options, &db_spec.path) {
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

        let descriptors =
            prepare_descriptors(&db_spec, write_buffer_manager, global_cache, &mut all_cfs)?;

        rocksdb::DB::open_cf_descriptors(&db_options, &db_spec.path, descriptors)
            .map(|db| RocksAccess {
                db,
                db_options,
                db_spec,
            })
            .map_err(RocksError::from_rocksdb_error)
    }

    pub fn spec(&self) -> &DbSpec {
        &self.db_spec
    }

    pub fn cf_handle(&self, cf: &str) -> Option<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db.cf_handle(cf)
    }

    pub fn as_raw_db(&self) -> &RawRocksDb {
        &self.db
    }

    pub fn open_cf(
        &self,
        name: CfName,
        write_buffer_manager: &WriteBufferManager,
        global_cache: &Cache,
    ) -> Result<(), RocksError> {
        let options = prepare_cf_options(&name, &self.db_spec, write_buffer_manager, global_cache)?;
        Ok(RawRocksDb::create_cf(&self.db, name.as_str(), &options)?)
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub(crate) fn shutdown(&self) {
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

    pub(crate) fn import_cf(
        &self,
        name: CfName,
        write_buffer_manager: &WriteBufferManager,
        global_cache: &Cache,
        metadata: ExportImportFilesMetaData,
    ) -> Result<(), RocksError> {
        let options = prepare_cf_options(&name, &self.db_spec, write_buffer_manager, global_cache)?;

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

    pub fn set_options_cf(
        &self,
        cf: impl AsRef<str>,
        opts: &[(&str, &str)],
    ) -> Result<(), RocksError> {
        let cf = cf.as_ref();
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.into()));
        };
        Ok(self.db.set_options_cf(&handle, opts)?)
    }

    pub fn get_property_int_cf(
        &self,
        cf: impl AsRef<str>,
        property: &str,
    ) -> Result<Option<u64>, RocksError> {
        let cf = cf.as_ref();
        let Some(handle) = self.cf_handle(cf) else {
            return Err(RocksError::UnknownColumnFamily(cf.into()));
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
