// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use rocksdb::checkpoint::Checkpoint;
use rocksdb::{BlockBasedOptions, ColumnFamilyDescriptor, ImportColumnFamilyOptions};
use rocksdb::{CompactOptions, ExportImportFilesMetaData};
use tokio::time::Instant;
use tracing::{debug, error, info, trace, warn};

use crate::RawRocksDb;
use crate::RocksError;
use crate::configuration::create_default_cf_options;
use crate::{BottommostLevelCompaction, CfName, ManualCompactionOptions, RocksDbReadPerfGuard};
use crate::{DbEnv, DbSpec};
use crate::{DbName, OpenMode};

/// Operations in this wrapper can be IO blocking, prefer using [`crate::RocksDb`]
/// for async access to the database.
#[derive(derive_more::Display, derive_more::Debug)]
#[display("{}", db_spec.name)]
#[debug("RocksDb({} at {}", db_spec.name, db_spec.path.display())]
pub struct RocksAccess {
    db_spec: DbSpec,
    db_options: rocksdb::Options,
    db: RawRocksDb,
    // Retain the selected resources through native database destruction, including
    // when the entire RocksAccess is moved to detached cleanup.
    db_env: DbEnv,
}

/// Configuration is complete, but no native database has been opened yet.
pub(crate) struct PreparedOpen {
    db_spec: DbSpec,
    db_options: rocksdb::Options,
    descriptors: Vec<ColumnFamilyDescriptor>,
    env: DbEnv,
}

fn prepare_cf_options(
    cf: &CfName,
    db_spec: &DbSpec,
    env: &DbEnv,
) -> Result<rocksdb::Options, RocksError> {
    let cf_patterns = &db_spec.cf_patterns;
    let db_name = &db_spec.name;
    // try patterns one by one
    for (pattern, options_updater) in cf_patterns {
        if pattern.cf_matches(cf) {
            // Stop at first pattern match
            return Ok(options_updater.get_cf_options(db_name, cf, env));
        }
    }
    // default is special case
    if cf.as_str() == "default" {
        // Use the supplied cache for the default CF too; otherwise RocksDB creates
        // a private cache (~32MB RSS per db). Without one, leave native caching defaults.
        let mut cf_options = create_default_cf_options(env.write_buffer_manager.as_ref());
        cf_options.set_level_zero_slowdown_writes_trigger(1 << 30);
        cf_options.set_level_zero_stop_writes_trigger(1 << 30);

        let mut block_opts = BlockBasedOptions::default();
        if let Some(cache) = &env.cache {
            block_opts.set_block_cache(cache);
        }
        cf_options.set_block_based_table_factory(&block_opts);

        return Ok(cf_options);
    }
    // We have no pattern for this cf
    Err(RocksError::UnknownColumnFamily(cf.clone()))
}

fn prepare_descriptors(
    db_spec: &DbSpec,
    env: &DbEnv,
    all_cfs: &mut HashSet<CfName>,
) -> Result<Vec<ColumnFamilyDescriptor>, RocksError> {
    // Include the default CF so it can share the supplied cache instead of
    // creating a private cache (~32MB RSS per db).
    all_cfs.insert(CfName::new("default"));
    // Make sure we have all column families we were asked to open/create.
    all_cfs.extend(db_spec.ensure_column_families.iter().cloned());

    let mut descriptors = Vec::with_capacity(all_cfs.len());
    for cf in all_cfs.iter() {
        let cf_options = prepare_cf_options(cf, db_spec, env)?;
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

    /// Opens a database and retains its selected resources through native destruction.
    /// Absent resources are left to database-specific configuration and native defaults.
    pub fn open_db(db_spec: DbSpec, env: DbEnv) -> Result<Self, RocksError> {
        Self::prepare_open(db_spec, env)?.open()
    }

    pub(crate) fn prepare_open(db_spec: DbSpec, env: DbEnv) -> Result<PreparedOpen, RocksError> {
        let db_options = db_spec.db_configurator.get_db_options(db_spec.name(), &env);

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

        let descriptors = prepare_descriptors(&db_spec, &env, &mut all_cfs)?;
        Ok(PreparedOpen {
            db_spec,
            db_options,
            descriptors,
            env,
        })
    }
}

impl PreparedOpen {
    pub(crate) fn open(self) -> Result<RocksAccess, RocksError> {
        let Self {
            db_spec,
            db_options,
            descriptors,
            env,
        } = self;
        trace!(path = %db_spec.path.display(), "Opening rocksdb database '{}'", db_spec.name());

        match db_spec.open_mode() {
            OpenMode::ReadWrite => {
                trace!(path = %db_spec.path.display(), "Opening rocksdb database '{}'", db_spec.name());
                rocksdb::DB::open_cf_descriptors(&db_options, &db_spec.path, descriptors)
                    .map(|db| RocksAccess {
                        db,
                        db_options,
                        db_spec,
                        db_env: env,
                    })
                    .map_err(RocksError::from_rocksdb_error)
            }
            OpenMode::ReadOnly => {
                warn!(path = %db_spec.path.display(), "Opening rocksdb database '{}' in read-only mode", db_spec.name());
                rocksdb::DB::open_cf_descriptors_read_only(
                    &db_options,
                    &db_spec.path,
                    descriptors,
                    false,
                )
                .map(|db| RocksAccess {
                    db,
                    db_options,
                    db_spec,
                    db_env: env,
                })
                .map_err(RocksError::from_rocksdb_error)
            }
        }
    }
}

impl RocksAccess {
    pub fn spec(&self) -> &DbSpec {
        &self.db_spec
    }

    /// Resources selected at open, also used for later CF creation and import.
    pub fn db_env(&self) -> &DbEnv {
        &self.db_env
    }

    pub fn cf_handle(&self, cf: &str) -> Option<Arc<rocksdb::BoundColumnFamily<'_>>> {
        self.db.cf_handle(cf)
    }

    pub fn as_raw_db(&self) -> &RawRocksDb {
        &self.db
    }

    pub fn export_cf(
        &self,
        name: CfName,
        export_dir: PathBuf,
    ) -> Result<ExportImportFilesMetaData, RocksError> {
        let _perf = RocksDbReadPerfGuard::new("export-column-family");
        let checkpoint = Checkpoint::new(self.as_raw_db())?;
        let cf = self
            .cf_handle(name.as_str())
            .ok_or_else(|| RocksError::UnknownColumnFamily(name.clone()))?;
        let metadata = checkpoint.export_column_family(&cf, export_dir.as_path())?;
        if metadata.get_files().is_empty() {
            error!(
                "Refusing to create an empty snapshot! RocksDB column family export \
                returned an empty set of files. The export is retained at: {}",
                export_dir.display()
            );
            return Err(RocksError::SnapshotEmpty);
        }
        Ok(metadata)
    }

    pub fn open_cf(&self, name: CfName) -> Result<(), RocksError> {
        let options = prepare_cf_options(&name, &self.db_spec, &self.db_env)?;
        debug!("Creating column family {}/{name}", self.name());
        Ok(RawRocksDb::create_cf(&self.db, name.as_str(), &options)?)
    }

    #[tracing::instrument(skip_all, fields(db = %self.name()))]
    pub(crate) fn shutdown(&self) {
        let _x = RocksDbReadPerfGuard::new("shutdown");
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
        metadata: ExportImportFilesMetaData,
    ) -> Result<(), RocksError> {
        let options = prepare_cf_options(&name, &self.db_spec, &self.db_env)?;

        let mut import_opts = ImportColumnFamilyOptions::default();
        import_opts.set_move_files(true);

        debug!(
            "Importing column family {}/{name} from snapshot",
            self.name()
        );
        Ok(RawRocksDb::create_column_family_with_import(
            &self.db,
            &options,
            name.as_str(),
            &import_opts,
            &metadata,
        )?)
    }

    pub fn flush_memtables(&self, cfs: &[CfName], wait: bool) -> Result<(), RocksError> {
        debug!("Flushing. db: {}, memtables: {:?}", self.name(), cfs);
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
        debug!("Flushing WAL. db: {}", self.name());
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

    pub fn compact_all(&self, options: ManualCompactionOptions) {
        let mut opts = CompactOptions::default();
        opts.set_bottommost_level_compaction(match options.bottommost_level_compaction {
            BottommostLevelCompaction::Skip => rocksdb::BottommostLevelCompaction::Skip,
            BottommostLevelCompaction::IfHaveCompactionFilter => {
                rocksdb::BottommostLevelCompaction::IfHaveCompactionFilter
            }
            BottommostLevelCompaction::Force => rocksdb::BottommostLevelCompaction::Force,
            BottommostLevelCompaction::ForceOptimized => {
                rocksdb::BottommostLevelCompaction::ForceOptimized
            }
        });
        opts.set_change_level(options.recalculate_level);
        self.cfs()
            .iter()
            .filter_map(|name| self.cf_handle(name))
            .for_each(|cf| {
                self.db
                    .compact_range_cf_opt::<&str, &str>(&cf, None, None, &opts)
            });
    }

    /// Runs a manual compaction on the Range of keys given on the
    /// given column family.
    pub fn compact_cf<A, B>(
        &self,
        cf_handle: &Arc<rocksdb::BoundColumnFamily<'_>>,
        opts: &CompactOptions,
        start_key: Option<A>,
        end_key: Option<B>,
    ) where
        A: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        self.db
            .compact_range_cf_opt(cf_handle, start_key, end_key, opts)
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
