// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use bytes::BytesMut;
use bytestring::ByteString;
use itertools::Itertools;
use rocksdb::{
    BoundColumnFamily, DBCompressionType, Error, IteratorMode, ReadOptions, WriteBatch,
    WriteOptions,
};

use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, IoMode, Priority, RocksDb, RocksDbManager,
    RocksError,
};
use restate_types::Version;
use restate_types::config::{Configuration, data_dir};
use restate_types::metadata::{Precondition, VersionedValue};
use restate_types::storage::{StorageCodec, StorageDecode, StorageEncode};

use crate::local::{DATA_DIR, DB_NAME, KV_PAIRS, SEALED_KEY};
use crate::{PreconditionViolation, RequestError};

pub struct RocksDbStorage {
    rocksdb: Arc<RocksDb>,
    buffer: BytesMut,
    is_sealed: bool,
}

impl RocksDbStorage {
    pub async fn open_or_create() -> Result<Self, RocksError> {
        let db_manager = RocksDbManager::get();

        let rocksdb = if let Some(rocksdb) = db_manager.get_db(DbName::new(DB_NAME)) {
            rocksdb
        } else {
            Self::create_db().await?
        };

        let is_sealed = Self::read_sealed_flag(&rocksdb)?;
        Ok(Self {
            rocksdb,
            buffer: BytesMut::default(),
            is_sealed,
        })
    }

    async fn create_db() -> Result<Arc<RocksDb>, RocksError> {
        let data_dir = RocksDbStorage::data_dir();
        let db_name = DbName::new(DB_NAME);
        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(KV_PAIRS)];

        let db_spec = DbSpecBuilder::new(db_name.clone(), data_dir, RocksConfigurator)
            .add_cf_pattern(CfPrefixPattern::ANY, RocksConfigurator)
            .ensure_column_families(cfs)
            .add_to_flush_on_shutdown(CfPrefixPattern::ANY)
            .build()
            .expect("valid spec");

        db_manager.open_db(db_spec).await
    }

    pub async fn create() -> Result<Self, RocksError> {
        let rocksdb = Self::create_db().await?;
        let is_sealed = Self::read_sealed_flag(&rocksdb)?;

        Ok(Self {
            rocksdb,
            buffer: BytesMut::default(),
            is_sealed,
        })
    }

    fn read_sealed_flag(rocksdb: &Arc<RocksDb>) -> Result<bool, RocksError> {
        let is_sealed = {
            let cf_handle = rocksdb
                .inner()
                .as_raw_db()
                .cf_handle(KV_PAIRS)
                .expect("KV_PAIRS column family exists");
            rocksdb
                .inner()
                .as_raw_db()
                .get_pinned_cf(&cf_handle, SEALED_KEY)?
                .is_some()
        };
        Ok(is_sealed)
    }

    pub fn data_dir() -> PathBuf {
        data_dir(DATA_DIR)
    }

    /// Checks whether the data directory of the [`RocksDbStorage`] exists. This indicates that the
    /// [`RocksDbStorage`] has been used before.
    pub fn data_dir_exists() -> bool {
        Self::data_dir().exists()
    }

    fn write_options(&mut self) -> WriteOptions {
        let opts = &Configuration::pinned().metadata_server.rocksdb;
        let mut write_opts = WriteOptions::default();

        write_opts.disable_wal(opts.rocksdb_disable_wal());

        if !opts.rocksdb_disable_wal() {
            // always sync if we have wal enabled
            write_opts.set_sync(true);
        }

        write_opts
    }

    fn kv_cf_handle(&self) -> Arc<BoundColumnFamily<'_>> {
        self.rocksdb
            .inner()
            .as_raw_db()
            .cf_handle(KV_PAIRS)
            .expect("KV_PAIRS column family exists")
    }

    pub fn get(&self, key: &ByteString) -> Result<Option<VersionedValue>, RequestError> {
        let cf_handle = self.kv_cf_handle();
        let slice = self
            .rocksdb
            .inner()
            .as_raw_db()
            .get_pinned_cf(&cf_handle, key)
            .map_err(|err| RequestError::Internal(err.into()))?;

        if let Some(bytes) = slice {
            Ok(Some(Self::decode(bytes)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_version(&self, key: &ByteString) -> Result<Option<Version>, RequestError> {
        let cf_handle = self.kv_cf_handle();
        let slice = self
            .rocksdb
            .inner()
            .as_raw_db()
            .get_pinned_cf(&cf_handle, key)
            .map_err(|err| RequestError::Internal(err.into()))?;

        if let Some(bytes) = slice {
            // todo only deserialize the version part
            let versioned_value = Self::decode::<VersionedValue>(bytes)?;
            Ok(Some(versioned_value.version))
        } else {
            Ok(None)
        }
    }

    pub async fn put(
        &mut self,
        key: &ByteString,
        value: &VersionedValue,
        precondition: Precondition,
    ) -> Result<(), RequestError> {
        self.fail_if_sealed()?;
        match precondition {
            Precondition::None => Ok(self.write_versioned_kv_pair(key, value).await?),
            Precondition::DoesNotExist => {
                let current_version = self.get_version(key)?;
                if current_version.is_none() {
                    Ok(self.write_versioned_kv_pair(key, value).await?)
                } else {
                    Err(PreconditionViolation::kv_pair_exists())?
                }
            }
            Precondition::MatchesVersion(version) => {
                let current_version = self.get_version(key)?;
                if current_version == Some(version) {
                    Ok(self.write_versioned_kv_pair(key, value).await?)
                } else {
                    Err(PreconditionViolation::version_mismatch(
                        version,
                        current_version,
                    ))?
                }
            }
        }
    }

    async fn write_versioned_kv_pair(
        &mut self,
        key: &ByteString,
        value: &VersionedValue,
    ) -> Result<(), RequestError> {
        self.buffer.clear();
        Self::encode(value, &mut self.buffer)?;

        let write_options = self.write_options();
        let cf_handle = self.kv_cf_handle();
        let mut wb = WriteBatch::default();

        // safety check to respect internal/reserved keys
        if key == SEALED_KEY {
            return Err(RequestError::InvalidArgument(format!(
                "Cannot store values under key {key} as it is a reserved key"
            )));
        }

        wb.put_cf(&cf_handle, key, self.buffer.as_ref());
        self.rocksdb
            .write_batch(
                "local-metadata-write-batch",
                Priority::High,
                IoMode::default(),
                write_options,
                wb,
            )
            .await
            .map_err(|err| RequestError::Internal(err.into()))
    }

    pub fn delete(
        &mut self,
        key: &ByteString,
        precondition: Precondition,
    ) -> Result<(), RequestError> {
        self.fail_if_sealed()?;
        match precondition {
            Precondition::None => self.delete_kv_pair(key),
            // this condition does not really make sense for the delete operation
            Precondition::DoesNotExist => {
                let current_version = self.get_version(key)?;

                if current_version.is_none() {
                    // nothing to do
                    Ok(())
                } else {
                    Err(PreconditionViolation::kv_pair_exists())?
                }
            }
            Precondition::MatchesVersion(version) => {
                let current_version = self.get_version(key)?;

                if current_version == Some(version) {
                    self.delete_kv_pair(key)
                } else {
                    Err(PreconditionViolation::version_mismatch(
                        version,
                        current_version,
                    ))?
                }
            }
        }
    }

    fn delete_kv_pair(&mut self, key: &ByteString) -> Result<(), RequestError> {
        let write_options = self.write_options();
        self.rocksdb
            .inner()
            .as_raw_db()
            .delete_cf_opt(&self.kv_cf_handle(), key, &write_options)
            .map_err(|err| RequestError::Internal(err.into()))
    }

    fn encode<T: StorageEncode>(value: &T, buf: &mut BytesMut) -> Result<(), RequestError> {
        StorageCodec::encode(value, buf)?;
        Ok(())
    }

    fn decode<T: StorageDecode>(buf: impl AsRef<[u8]>) -> Result<T, RequestError> {
        let value = StorageCodec::decode(&mut buf.as_ref())?;
        Ok(value)
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = Result<(ByteString, VersionedValue), Error>> + use<'_> {
        let cf_handle = self.kv_cf_handle();
        let mut read_opts = ReadOptions::default();
        read_opts.set_async_io(true);
        self.rocksdb
            .inner()
            .as_raw_db()
            .full_iterator_cf(&cf_handle, IteratorMode::Start)
            .map_ok(|(key, value)| {
                let key = ByteString::try_from(key.as_ref()).expect("valid byte string as key");

                // filter out internal keys
                if key == SEALED_KEY {
                    return None;
                }

                let value = RocksDbStorage::decode(value.as_ref()).expect("valid versioned value");
                Some((key, value))
            })
            .flatten_ok()
    }

    pub async fn seal(&mut self) -> Result<(), RequestError> {
        if self.is_sealed {
            return Ok(());
        }

        {
            let write_options = self.write_options();
            let mut wb = WriteBatch::default();
            let cf_handle = self.kv_cf_handle();
            wb.put_cf(&cf_handle, SEALED_KEY, chrono::Utc::now().to_rfc3339());
            self.rocksdb
                .write_batch(
                    "local-metadata-seal-batch",
                    Priority::High,
                    IoMode::default(),
                    write_options,
                    wb,
                )
                .await
                .map_err(|err| RequestError::Internal(err.into()))?;
        }
        self.is_sealed = true;

        Ok(())
    }

    pub fn is_sealed(&self) -> bool {
        self.is_sealed
    }

    fn fail_if_sealed(&self) -> Result<(), RequestError> {
        if self.is_sealed {
            Err(RequestError::Internal("local metadata server has been sealed. This indicates that it has been migrated to the replicated metadata server. Please set 'metadata-server.type = \"replicated\"' in your configuration".to_owned().into()))
        } else {
            Ok(())
        }
    }
}

struct RocksConfigurator;

impl restate_rocksdb::configuration::DbConfigurator for RocksConfigurator {
    fn get_db_options(
        &self,
        _db_name: &str,
        env: &rocksdb::Env,
        write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options {
        let mut db_options = restate_rocksdb::configuration::create_default_db_options(
            env,
            true, /* create_db_if_missing */
            write_buffer_manager,
        );
        // amend default options from rocksdb_manager
        self.apply_db_opts_from_config(
            &mut db_options,
            &Configuration::pinned().metadata_server.rocksdb,
        );

        db_options
    }
}

impl restate_rocksdb::configuration::CfConfigurator for RocksConfigurator {
    fn get_cf_options(
        &self,
        _db_name: &str,
        _cf_name: &str,
        _global_cache: &rocksdb::Cache,
        _write_buffer_manager: &rocksdb::WriteBufferManager,
    ) -> rocksdb::Options {
        let config = &Configuration::pinned().metadata_server;
        // don't use WBM for the temporary use of this database since metadata-server is fully
        // deprecated.
        let mut cf_options = restate_rocksdb::configuration::create_default_cf_options(None);
        let block_options =
            restate_rocksdb::configuration::create_default_block_options(&config.rocksdb, None);

        cf_options.set_block_based_table_factory(&block_options);
        // 1MB default memory budget is enough for seal check
        let memtables_budget = 1024 * 1024;
        // We set the budget to allow 1 mutable + 3 immutable.
        cf_options.set_write_buffer_size(memtables_budget / 4);

        // merge 2 memtables when flushing to L0
        cf_options.set_min_write_buffer_number_to_merge(2);
        cf_options.set_max_write_buffer_number(4);
        // start flushing L0->L1 as soon as possible. each file on level0 is
        // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
        // memtable_memory_budget.
        cf_options.set_level_zero_file_num_compaction_trigger(2);
        // doesn't really matter much, but we don't want to create too many files
        cf_options.set_target_file_size_base(memtables_budget as u64 / 8);
        // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
        cf_options.set_max_bytes_for_level_base(memtables_budget as u64);

        cf_options.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        cf_options.set_num_levels(3);

        cf_options.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Zstd,
        ]);

        cf_options
    }
}
