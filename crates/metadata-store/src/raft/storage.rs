// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::util;
use protobuf::{Message, ProtobufError};
use raft::eraftpb::{ConfState, Entry, Snapshot};
use raft::prelude::HardState;
use raft::{GetEntriesContext, RaftState, Storage, StorageError};
use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, IoMode, Priority, RocksDb, RocksDbManager,
    RocksError,
};
use restate_types::config::{MetadataStoreOptions, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use rocksdb::{BoundColumnFamily, ReadOptions, WriteBatch, WriteOptions, DB};
use std::mem::size_of;
use std::sync::Arc;
use std::{error, mem};

const DB_NAME: &str = "raft-metadata-store";
const RAFT_CF: &str = "raft";

const FIRST_RAFT_INDEX: u64 = 1;

const RAFT_ENTRY_DISCRIMINATOR: u8 = 0x01;
const HARD_STATE_DISCRIMINATOR: u8 = 0x02;
const CONF_STATE_DISCRIMINATOR: u8 = 0x03;

const RAFT_ENTRY_KEY_LENGTH: usize = 9;

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("failed creating RocksDb: {0}")]
    RocksDb(#[from] RocksError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed reading/writing from/to RocksDb: {0}")]
    RocksDb(#[from] RocksError),
    #[error("failed reading/writing from/to raw RocksDb: {0}")]
    RocksDbRaw(#[from] rocksdb::Error),
    #[error("failed encoding value: {0}")]
    Encode(#[from] ProtobufError),
    #[error("index '{index}' is out of bounds; last index is '{last_index}'")]
    IndexOutOfBounds { index: u64, last_index: u64 },
    #[error("raft log has been compacted; first index is {0}")]
    Compacted(u64),
}

/// Map our internal error type to [`raft::Error`] to fit the [`Storage`] trait definition.
impl From<Error> for raft::Error {
    fn from(value: Error) -> Self {
        match value {
            err @ Error::RocksDb(_)
            | err @ Error::RocksDbRaw(_)
            | err @ Error::IndexOutOfBounds { .. } => storage_error(err),
            Error::Encode(err) => raft::Error::CodecError(err),
            Error::Compacted(_) => raft::Error::Store(StorageError::Compacted),
        }
    }
}

pub struct RocksDbStorage {
    db: Arc<DB>,
    rocksdb: Arc<RocksDb>,

    last_index: u64,
    buffer: Vec<u8>,
}

impl RocksDbStorage {
    pub async fn create(
        options: &MetadataStoreOptions,
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    ) -> Result<Self, BuildError> {
        let db_name = DbName::new(DB_NAME);
        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(RAFT_CF)];
        let db_spec = DbSpecBuilder::new(
            db_name.clone(),
            options.data_dir(),
            util::db_options(options),
        )
        .add_cf_pattern(
            CfPrefixPattern::ANY,
            util::cf_options(options.rocksdb_memory_budget()),
        )
        .ensure_column_families(cfs)
        .build_as_db();

        let db = db_manager.open_db(rocksdb_options, db_spec).await?;
        let rocksdb = db_manager
            .get_db(db_name)
            .expect("raft metadata store db is open");

        let last_index = Self::find_last_index(&db);

        Ok(Self {
            db,
            rocksdb,
            last_index,
            buffer: Vec::with_capacity(1024),
        })
    }
}

impl RocksDbStorage {
    fn write_options(&self) -> WriteOptions {
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false);
        // always sync to not lose data
        write_opts.set_sync(true);
        write_opts
    }

    fn find_last_index(db: &DB) -> u64 {
        let cf = db.cf_handle(RAFT_CF).expect("RAFT_CF exists");
        let start = Self::raft_entry_key(0);
        // end is exclusive so switch to the next discriminator
        let mut end = [0; 9];
        end[0] = RAFT_ENTRY_DISCRIMINATOR + 1;

        let mut options = ReadOptions::default();
        options.set_async_io(true);
        options.set_iterate_range(start..end);
        let mut iterator = db.raw_iterator_cf_opt(&cf, options);

        iterator.seek_to_last();

        if iterator.valid() {
            let key_bytes = iterator.key().expect("key should be present");
            assert_eq!(
                key_bytes.len(),
                RAFT_ENTRY_KEY_LENGTH,
                "raft entry keys must consist of '{}' bytes",
                RAFT_ENTRY_KEY_LENGTH
            );
            u64::from_be_bytes(
                key_bytes[1..(1 + size_of::<u64>())]
                    .try_into()
                    .expect("buffer should be long enough"),
            )
        } else {
            // the first valid raft index starts at 1, so 0 means there are no replicated raft entries
            0
        }
    }

    pub fn get_hard_state(&self) -> Result<HardState, Error> {
        let key = Self::hard_state_key();
        self.get_value(key)
            .map(|hard_state| hard_state.unwrap_or_default())
    }

    pub async fn store_hard_state(&mut self, hard_state: HardState) -> Result<(), Error> {
        let key = Self::hard_state_key();
        self.put_value(key, hard_state).await
    }

    pub fn get_conf_state(&self) -> Result<ConfState, Error> {
        let key = Self::conf_state_key();
        self.get_value(key)
            .map(|hard_state| hard_state.unwrap_or_default())
    }

    pub async fn store_conf_state(&mut self, conf_state: ConfState) -> Result<(), Error> {
        let key = Self::conf_state_key();
        self.put_value(key, conf_state).await
    }

    pub fn get_entry(&self, idx: u64) -> Result<Option<Entry>, Error> {
        let key = Self::raft_entry_key(idx);
        self.get_value(key)
    }

    fn get_value<T: Message + Default>(&self, key: impl AsRef<[u8]>) -> Result<Option<T>, Error> {
        let cf = self.get_cf_handle();
        let bytes = self.db.get_pinned_cf(&cf, key)?;

        if let Some(bytes) = bytes {
            let mut value = T::default();
            value.merge_from_bytes(bytes.as_ref())?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    async fn put_value<T: Message>(
        &mut self,
        key: impl AsRef<[u8]>,
        value: T,
    ) -> Result<(), Error> {
        self.buffer.clear();
        value.write_to_vec(&mut self.buffer)?;

        let cf = self.get_cf_handle();
        let mut write_batch = WriteBatch::default();
        write_batch.put_cf(&cf, key.as_ref(), &self.buffer);
        self.rocksdb
            .write_batch(
                "put_value",
                Priority::High,
                IoMode::Default,
                self.write_options(),
                write_batch,
            )
            .await
            .map_err(Into::into)
    }

    pub async fn append(&mut self, entries: &Vec<Entry>) -> Result<(), Error> {
        let mut write_batch = WriteBatch::default();
        let mut buffer = mem::take(&mut self.buffer);
        let mut last_index = self.last_index;

        {
            let cf = self.get_cf_handle();

            for entry in entries {
                assert_eq!(last_index + 1, entry.index, "Expect raft log w/o holes");
                let key = Self::raft_entry_key(entry.index);

                buffer.clear();
                entry.write_to_vec(&mut buffer)?;

                write_batch.put_cf(&cf, key, &buffer);
                last_index = entry.index;
            }
        }

        let result = self
            .rocksdb
            .write_batch(
                "append",
                Priority::High,
                IoMode::Default,
                self.write_options(),
                write_batch,
            )
            .await
            .map_err(Into::into);

        self.buffer = buffer;
        self.last_index = last_index;

        result
    }

    fn get_cf_handle(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(RAFT_CF).expect("RAFT_CF exists")
    }

    fn raft_entry_key(idx: u64) -> [u8; RAFT_ENTRY_KEY_LENGTH] {
        let mut key = [0; RAFT_ENTRY_KEY_LENGTH];
        key[0] = RAFT_ENTRY_DISCRIMINATOR;
        key[1..9].copy_from_slice(&idx.to_be_bytes());
        key
    }

    fn hard_state_key() -> [u8; 1] {
        [HARD_STATE_DISCRIMINATOR]
    }

    fn conf_state_key() -> [u8; 1] {
        [CONF_STATE_DISCRIMINATOR]
    }

    fn check_index(&self, idx: u64) -> Result<(), Error> {
        if idx < self.first_index() {
            return Err(Error::Compacted(self.first_index()));
        } else if idx > self.last_index() {
            return Err(Error::IndexOutOfBounds {
                index: idx,
                last_index: self.last_index(),
            });
        }

        Ok(())
    }

    fn check_range(&self, low: u64, high: u64) -> Result<(), Error> {
        assert!(low < high, "Low '{low}' must be smaller than high '{high}'");

        if low < self.first_index() {
            return Err(Error::Compacted(self.first_index()));
        }

        if high > self.last_index() + 1 {
            return Err(Error::IndexOutOfBounds {
                index: high,
                last_index: self.last_index(),
            });
        }

        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.last_index
    }

    fn first_index(&self) -> u64 {
        FIRST_RAFT_INDEX
    }

    pub fn apply_snapshot(&mut self, _snapshot: Snapshot) -> Result<(), Error> {
        unimplemented!("snapshots are currently not supported");
    }
}

impl Storage for RocksDbStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.get_hard_state()?;
        Ok(RaftState::new(hard_state, self.get_conf_state()?))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.check_range(low, high)?;
        let start_key = Self::raft_entry_key(low);
        let end_key = Self::raft_entry_key(high);

        let cf = self.get_cf_handle();
        let mut opts = ReadOptions::default();
        opts.set_iterate_range(start_key..end_key);
        opts.set_async_io(true);

        let mut iterator = self.db.raw_iterator_cf_opt(&cf, opts);
        iterator.seek(start_key);

        let mut result =
            Vec::with_capacity(usize::try_from(high - low).expect("u64 fits into usize"));

        let max_size =
            usize::try_from(max_size.into().unwrap_or(u64::MAX)).expect("u64 fits into usize");
        let mut size = 0;
        let mut expected_idx = low;

        while iterator.valid() {
            if size > 0 && size >= max_size {
                break;
            }

            if let Some(value) = iterator.value() {
                let mut entry = Entry::default();
                entry.merge_from_bytes(value)?;

                if expected_idx != entry.index {
                    if expected_idx == low {
                        Err(StorageError::Compacted)?;
                    } else {
                        // missing raft entries :-(
                        Err(StorageError::Unavailable)?;
                    }
                }

                result.push(entry);
                expected_idx += 1;
                size += value.len();
            }

            iterator.next();
        }

        // check for an occurred error
        iterator
            .status()
            .map_err(|err| StorageError::Other(err.into()))?;

        Ok(result)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        // todo handle first_index - 1 once truncation is supported
        if idx == 0 {
            return Ok(0);
        }

        self.check_index(idx)?;
        self.get_entry(idx)
            .map(|entry| entry.expect("should exist").term)
            .map_err(Into::into)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index())
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        // time is relative as some clever people figured out
        Err(raft::Error::Store(
            StorageError::SnapshotTemporarilyUnavailable,
        ))
    }
}

pub fn storage_error<E>(error: E) -> raft::Error
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    raft::Error::Store(StorageError::Other(error.into()))
}
