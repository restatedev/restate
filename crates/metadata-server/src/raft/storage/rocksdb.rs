// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::sync::Arc;
use std::{error, mem};

use bytes::{BufMut, BytesMut};
use flexbuffers::{DeserializationError, SerializationError};
use protobuf::{Message, ProtobufError};
use raft::{GetEntriesContext, RaftState, Storage, StorageError};
use raft_proto::eraftpb::{ConfState, Entry, HardState, Snapshot};
use rocksdb::{BoundColumnFamily, DB, DBPinnableSlice, ReadOptions, WriteBatch, WriteOptions};
use tracing::debug;

use restate_rocksdb::{IoMode, Priority, RocksDb, RocksError};
use restate_types::errors::GenericError;
use restate_types::nodes_config::NodesConfiguration;

use crate::raft::storage::keys::{
    CONF_STATE_KEY, HARD_STATE_KEY, LogEntryKey, MARKER_KEY, NODES_CONFIGURATION_KEY,
    RAFT_SERVER_STATE_KEY, SNAPSHOT_KEY,
};
use crate::raft::storage::rocksdb_builder::build_rocksdb;
use crate::raft::storage::{DATA_CF, METADATA_CF};
use crate::raft::{RaftServerState, StorageMarker};

const FIRST_RAFT_INDEX: u64 = 1;

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
    #[error("failed encoding protobuf value: {0}")]
    EncodeProto(#[from] ProtobufError),
    #[error("index '{index}' is out of bounds; last index is '{last_index}'")]
    IndexOutOfBounds { index: u64, last_index: u64 },
    #[error("raft log has been compacted; first index is {0}")]
    Compacted(u64),
    #[error("failed decoding value: {0}")]
    Decode(GenericError),
    #[error("failed encoding value: {0}")]
    Encode(GenericError),
}

/// Map our internal error type to [`raft::Error`] to fit the [`Storage`] trait definition.
impl From<Error> for raft::Error {
    fn from(value: Error) -> Self {
        match value {
            err @ Error::RocksDb(_)
            | err @ Error::RocksDbRaw(_)
            | err @ Error::IndexOutOfBounds { .. }
            | err @ Error::Decode(_)
            | err @ Error::Encode(_) => other_error(err),
            Error::EncodeProto(err) => raft::Error::CodecError(err),
            Error::Compacted(_) => raft::Error::Store(StorageError::Compacted),
        }
    }
}

pub struct RocksDbStorage {
    rocksdb: Arc<RocksDb>,

    first_index: u64,
    last_index: u64,

    // contains some value if a newer snapshot is requested
    requested_snapshot: RefCell<Option<u64>>,

    buffer: BytesMut,
}

impl RocksDbStorage {
    pub async fn create() -> Result<Self, BuildError> {
        let rocksdb = build_rocksdb().await?;
        let (first_index, last_index) = Self::find_indices(rocksdb.inner().as_raw_db());

        Ok(Self {
            rocksdb,
            first_index,
            last_index,
            requested_snapshot: RefCell::default(),
            buffer: BytesMut::with_capacity(1024),
        })
    }
}

impl RocksDbStorage {
    fn data_cf(&self) -> Arc<BoundColumnFamily<'_>> {
        self.rocksdb
            .inner()
            .cf_handle(DATA_CF)
            .expect("DATA_CF exists")
    }

    fn metadata_cf(&self) -> Arc<BoundColumnFamily<'_>> {
        self.rocksdb
            .inner()
            .cf_handle(METADATA_CF)
            .expect("METADATA_CF exists")
    }

    pub fn get_last_index(&self) -> u64 {
        self.last_index
    }

    pub fn get_first_index(&self) -> u64 {
        self.first_index
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        let is_empty = self.get_raft_server_state()? == RaftServerState::Standby
            && self.get_snapshot()?.is_empty()
            && self.get_hard_state()? == HardState::default()
            && self.get_conf_state()? == ConfState::default()
            && self.get_first_index() == 1
            && self.get_last_index() == 0
            && self.get_marker()?.is_none();

        Ok(is_empty)
    }

    pub fn requested_snapshot(&self) -> Option<u64> {
        *self.requested_snapshot.borrow()
    }

    fn write_options() -> WriteOptions {
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false);
        // always sync to not lose data
        write_opts.set_sync(true);
        write_opts
    }

    pub fn txn(&mut self) -> Transaction<'_> {
        Transaction::new(self)
    }

    pub fn get_hard_state(&self) -> Result<HardState, Error> {
        self.get_value_metadata_cf(HARD_STATE_KEY)
            .map(|hard_state| hard_state.unwrap_or_default())
    }

    pub async fn store_hard_state(&mut self, hard_state: HardState) -> Result<(), Error> {
        self.put_value_metadata_cf(HARD_STATE_KEY, hard_state).await
    }

    pub fn get_conf_state(&self) -> Result<ConfState, Error> {
        self.get_value_metadata_cf(CONF_STATE_KEY)
            .map(|hard_state| hard_state.unwrap_or_default())
    }

    pub fn get_marker(&self) -> Result<Option<StorageMarker>, Error> {
        if let Some(bytes) = self.get_bytes_metadata_cf(MARKER_KEY)? {
            Ok(Some(
                StorageMarker::from_slice(&bytes).map_err(|err| Error::Decode(err.into()))?,
            ))
        } else {
            Ok(None)
        }
    }

    pub fn get_entry(&self, idx: u64) -> Result<Option<Entry>, Error> {
        let key = LogEntryKey::new(idx).to_bytes();
        self.get_value_data_cf(key)
    }

    fn get_value_metadata_cf<T: Message + Default>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>, Error> {
        let bytes = self.get_bytes_metadata_cf(key)?;

        if let Some(bytes) = bytes {
            let mut value = T::default();
            value.merge_from_bytes(bytes.as_ref())?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn get_value_data_cf<T: Message + Default>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>, Error> {
        let bytes = self.get_bytes_data_cf(key)?;

        if let Some(bytes) = bytes {
            let mut value = T::default();
            value.merge_from_bytes(bytes.as_ref())?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    async fn put_value_metadata_cf<T: Message>(
        &mut self,
        key: impl AsRef<[u8]>,
        value: T,
    ) -> Result<(), Error> {
        self.buffer.clear();
        value.write_to_writer(&mut (&mut self.buffer).writer())?;
        let mut write_batch = WriteBatch::default();
        {
            let cf = self.metadata_cf();
            write_batch.put_cf(&cf, key.as_ref(), &self.buffer);
        }
        self.commit_write_batch(write_batch).await
    }

    fn get_bytes_data_cf(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<DBPinnableSlice<'_>>, Error> {
        block_in_place(|| {
            let cf = self.data_cf();
            self.rocksdb
                .inner()
                .as_raw_db()
                .get_pinned_cf(&cf, key)
                .map_err(Into::into)
        })
    }

    fn get_bytes_metadata_cf(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<DBPinnableSlice<'_>>, Error> {
        block_in_place(|| {
            let cf = self.metadata_cf();
            self.rocksdb
                .inner()
                .as_raw_db()
                .get_pinned_cf(&cf, key)
                .map_err(Into::into)
        })
    }

    async fn put_bytes_metadata_cf(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), Error> {
        let mut write_batch = WriteBatch::default();
        {
            let cf = self.metadata_cf();
            write_batch.put_cf(&cf, key.as_ref(), value.as_ref());
        }
        self.commit_write_batch(write_batch).await
    }

    pub async fn append(&mut self, entries: &Vec<Entry>) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }

        // sanity checks
        assert!(
            entries[0].index >= self.get_first_index(),
            "Cannot overwrite compacted raft log entries, compacted: {}, append: {}",
            self.get_first_index() - 1,
            entries[0].index
        );
        assert!(
            entries[0].index <= self.get_last_index() + 1,
            "Cannot create holes in the raft log, last: {}, append: {}",
            self.get_last_index(),
            entries[0].index
        );

        let last_entry_index = entries.last().expect("to be present").index;
        let mut write_batch = WriteBatch::default();
        let mut buffer = mem::take(&mut self.buffer);

        {
            let data_cf = self.data_cf();

            let previous_last_index = self.get_last_index();

            // delete all entries that are not being overwritten but have a higher index
            for index in last_entry_index + 1..=previous_last_index {
                let key = LogEntryKey::new(index).to_bytes();
                write_batch.delete_cf(&data_cf, key);
            }

            for entry in entries {
                buffer.clear();
                let key = LogEntryKey::new(entry.index).to_bytes();
                entry.write_to_writer(&mut (&mut buffer).writer())?;
                write_batch.put_cf(&data_cf, key, &buffer);
            }
        }

        let result = self.commit_write_batch(write_batch).await;

        self.buffer = buffer;
        self.last_index = last_entry_index;

        result
    }

    pub async fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        let mut txn = self.txn();
        txn.apply_snapshot(snapshot)?;
        txn.commit().await?;

        Ok(())
    }

    async fn commit_write_batch(&mut self, write_batch: WriteBatch) -> Result<(), Error> {
        self.rocksdb
            .write_batch(
                "commit_write_batch",
                Priority::High,
                IoMode::Default,
                Self::write_options(),
                write_batch,
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.into())
    }

    #[allow(dead_code)]
    pub async fn store_raft_server_state(
        &mut self,
        raft_server_state: &RaftServerState,
    ) -> Result<(), Error> {
        self.put_bytes_metadata_cf(
            RAFT_SERVER_STATE_KEY,
            &Self::serialize_value(raft_server_state).map_err(|err| Error::Encode(err.into()))?,
        )
        .await
    }

    pub fn get_raft_server_state(&self) -> Result<RaftServerState, Error> {
        if let Some(bytes) = self.get_bytes_metadata_cf(RAFT_SERVER_STATE_KEY)? {
            Ok(Self::deserialize_value(bytes).map_err(|err| Error::Decode(err.into()))?)
        } else {
            Ok(RaftServerState::default())
        }
    }

    pub async fn store_nodes_configuration(
        &mut self,
        nodes_configuration: &NodesConfiguration,
    ) -> Result<(), Error> {
        self.put_bytes_metadata_cf(
            NODES_CONFIGURATION_KEY,
            &Self::serialize_value(nodes_configuration).map_err(|err| Error::Encode(err.into()))?,
        )
        .await
    }

    pub fn get_nodes_configuration(&self) -> Result<Option<NodesConfiguration>, Error> {
        if let Some(bytes) = self.get_bytes_metadata_cf(NODES_CONFIGURATION_KEY)? {
            Ok(Some(
                Self::deserialize_value(bytes).map_err(|err| Error::Decode(err.into()))?,
            ))
        } else {
            Ok(None)
        }
    }

    pub fn get_snapshot(&self) -> Result<Snapshot, Error> {
        self.get_value_metadata_cf(SNAPSHOT_KEY)
            .map(|snapshot| snapshot.unwrap_or_default())
    }

    // ------------------------------
    // Utils
    // ------------------------------

    fn find_indices(db: &DB) -> (u64, u64) {
        block_in_place(|| {
            let data_cf = db.cf_handle(DATA_CF).expect("DATA_CF exists");
            let metadata_cf = db.cf_handle(METADATA_CF).expect("METADATA_CF exists");
            let start = LogEntryKey::new(0).to_bytes();
            let end = LogEntryKey::new(u64::MAX).to_bytes();

            let mut options = ReadOptions::default();
            options.set_async_io(true);
            options.set_iterate_range(start..end);
            let mut iterator = db.raw_iterator_cf_opt(&data_cf, options);

            iterator.seek_to_first();

            if iterator.valid() {
                let key_bytes = iterator.key().expect("key should be present");
                let first_index = LogEntryKey::from_slice(key_bytes).index();

                iterator.seek_to_last();

                assert!(iterator.valid(), "iterator should be valid");
                let key_bytes = iterator.key().expect("key should be present");
                let last_index = LogEntryKey::from_slice(key_bytes).index();

                (first_index, last_index)
            } else {
                let snapshot_bytes = db
                    .get_pinned_cf(&metadata_cf, SNAPSHOT_KEY)
                    .expect("snapshot key should be readable");
                if let Some(snapshot_bytes) = snapshot_bytes {
                    let snapshot = Snapshot::parse_from_bytes(snapshot_bytes.as_ref())
                        .expect("snapshot should be deserializable");
                    let last_index = snapshot.get_metadata().get_index();
                    let first_index = snapshot.get_metadata().get_index() + 1;

                    (first_index, last_index)
                } else {
                    // the first valid raft index starts at 1, so 0 means there are no replicated raft entries
                    (FIRST_RAFT_INDEX, 0)
                }
            }
        })
    }

    fn check_index(&self, idx: u64) -> Result<(), Error> {
        if idx < self.get_first_index() {
            return Err(Error::Compacted(self.get_first_index()));
        } else if idx > self.get_last_index() {
            return Err(Error::IndexOutOfBounds {
                index: idx,
                last_index: self.get_last_index(),
            });
        }

        Ok(())
    }

    /// Check if the range is valid and within the bounds of the raft log. `High` is exclusive.
    fn check_range(&self, low: u64, high: u64) -> Result<(), Error> {
        assert!(low < high, "Low '{low}' must be smaller than high '{high}'");

        if low < self.get_first_index() {
            return Err(Error::Compacted(self.get_first_index()));
        }

        // high is exclusive
        if high - 1 > self.get_last_index() {
            return Err(Error::IndexOutOfBounds {
                index: high,
                last_index: self.get_last_index(),
            });
        }

        Ok(())
    }

    fn serialize_value<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, SerializationError> {
        // todo replace with something more efficient
        flexbuffers::to_vec(value)
    }

    fn deserialize_value<T: for<'a> serde::Deserialize<'a>>(
        buf: impl AsRef<[u8]>,
    ) -> Result<T, DeserializationError> {
        // todo replace with something more efficient
        flexbuffers::from_slice(buf.as_ref())
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
        block_in_place(|| {
            self.check_range(low, high)?;
            let start_key = LogEntryKey::new(low).to_bytes();
            let end_key = LogEntryKey::new(high).to_bytes();

            let cf = self.data_cf();
            let mut opts = ReadOptions::default();
            opts.set_iterate_range(start_key..end_key);
            opts.set_async_io(true);

            let mut iterator = self
                .rocksdb
                .inner()
                .as_raw_db()
                .raw_iterator_cf_opt(&cf, opts);
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
        })
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let first_index = self.get_first_index();

        if idx < first_index {
            let snapshot = self.get_snapshot()?;

            if snapshot.get_metadata().get_index() == idx {
                return Ok(snapshot.get_metadata().get_term());
            } else {
                Err(Error::Compacted(idx))?;
            }
        }

        self.check_index(idx)?;
        self.get_entry(idx)
            .map(|entry| entry.expect("should exist").term)
            .map_err(Into::into)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.get_first_index())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.get_last_index())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        debug!("request snapshot for index {}", request_index);
        let snapshot = self.get_snapshot()?;
        if snapshot.get_metadata().get_index() >= request_index {
            return Ok(snapshot);
        }

        if request_index > self.requested_snapshot.borrow().unwrap_or_default() {
            *self.requested_snapshot.borrow_mut() = Some(request_index);
        }

        Err(raft::Error::Store(
            StorageError::SnapshotTemporarilyUnavailable,
        ))
    }
}

#[must_use = "transactions must be committed"]
pub struct Transaction<'a> {
    storage: &'a mut RocksDbStorage,
    write_batch: WriteBatch,
    first_index: u64,
    last_index: u64,
    snapshot_index: Option<u64>,
}

impl<'a> Transaction<'a> {
    fn new(storage: &'a mut RocksDbStorage) -> Self {
        let first_index = storage.get_first_index();
        let last_index = storage.get_last_index();
        Self {
            storage,
            write_batch: WriteBatch::default(),
            first_index,
            last_index,
            snapshot_index: None,
        }
    }

    pub fn apply_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        if snapshot.get_metadata().get_index() < self.first_index {
            // snapshot is outdated; ignore it
            return Ok(());
        }

        let metadata = snapshot.get_metadata();
        let snapshot_index = metadata.get_index();

        let mut hard_state = self.storage.get_hard_state()?;
        hard_state.set_term(hard_state.get_term().max(metadata.get_term()));
        hard_state.set_commit(hard_state.get_commit().max(snapshot_index));

        self.store_conf_state(metadata.get_conf_state())?;
        self.store_hard_state(&hard_state)?;
        self.store_snapshot(snapshot)?;
        // trim all entries up to the snapshot index
        self.trim(snapshot_index);

        self.snapshot_index = Some(snapshot_index);

        Ok(())
    }

    pub fn store_marker(&mut self, storage_marker: &StorageMarker) {
        self.put_bytes_metadata_cf(MARKER_KEY, storage_marker.to_bytes())
    }

    pub fn store_raft_server_state(
        &mut self,
        raft_server_state: &RaftServerState,
    ) -> Result<(), Error> {
        self.put_bytes_metadata_cf(
            RAFT_SERVER_STATE_KEY,
            &RocksDbStorage::serialize_value(raft_server_state)
                .map_err(|err| Error::Encode(err.into()))?,
        );

        Ok(())
    }

    pub fn delete_raft_server_state(&mut self) -> Result<(), Error> {
        self.delete_metadata_cf(RAFT_SERVER_STATE_KEY);
        Ok(())
    }

    pub fn store_nodes_configuration(
        &mut self,
        nodes_configuration: &NodesConfiguration,
    ) -> Result<(), Error> {
        self.put_bytes_metadata_cf(
            NODES_CONFIGURATION_KEY,
            &RocksDbStorage::serialize_value(nodes_configuration)
                .map_err(|err| Error::Encode(err.into()))?,
        );

        Ok(())
    }

    pub fn store_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), Error> {
        self.put_value_ref_metadata_cf(SNAPSHOT_KEY, snapshot)
    }

    pub fn store_conf_state(&mut self, conf_state: &ConfState) -> Result<(), Error> {
        self.put_value_ref_metadata_cf(CONF_STATE_KEY, conf_state)
    }

    pub fn store_hard_state(&mut self, hard_state: &HardState) -> Result<(), Error> {
        self.put_value_ref_metadata_cf(HARD_STATE_KEY, hard_state)
    }

    /// The `trim_point` is inclusive.
    fn trim(&mut self, trim_point: u64) {
        if trim_point < self.first_index {
            return;
        }

        let effective_trim_point = std::cmp::min(trim_point, self.last_index);

        let mut write_batch = mem::take(&mut self.write_batch);
        {
            let data_cf = self.storage.data_cf();
            for index in self.first_index..=effective_trim_point {
                // single_delete would be awesome here to avoid the tombstones
                let key = LogEntryKey::new(index).to_bytes();
                write_batch.delete_cf(&data_cf, key);
            }
        }
        self.write_batch = write_batch;

        self.first_index = trim_point + 1;
        self.last_index = self.last_index.max(trim_point);
    }

    pub async fn commit(self) -> Result<(), Error> {
        if !self.write_batch.is_empty() {
            self.storage.commit_write_batch(self.write_batch).await?;
        }
        self.storage.first_index = self.first_index;
        self.storage.last_index = self.last_index;

        if let Some(snapshot_index) = self.snapshot_index
            && self
                .storage
                .requested_snapshot
                .borrow()
                .is_some_and(|requested_snapshot| requested_snapshot <= snapshot_index)
        {
            *self.storage.requested_snapshot.get_mut() = None;
        }

        Ok(())
    }

    fn put_value_ref_metadata_cf<T: Message>(
        &mut self,
        key: impl AsRef<[u8]>,
        value: &T,
    ) -> Result<(), Error> {
        let mut buffer = mem::take(&mut self.storage.buffer);

        buffer.clear();
        value.write_to_writer(&mut (&mut buffer).writer())?;
        self.put_bytes_metadata_cf(key, &buffer);

        self.storage.buffer = buffer;

        Ok(())
    }

    fn put_bytes_metadata_cf(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        let mut write_batch = mem::take(&mut self.write_batch);
        write_batch.put_cf(&self.storage.metadata_cf(), key.as_ref(), value.as_ref());
        self.write_batch = write_batch;
    }

    fn delete_metadata_cf(&mut self, key: impl AsRef<[u8]>) {
        let mut write_batch = mem::take(&mut self.write_batch);
        write_batch.delete_cf(&self.storage.metadata_cf(), key);
        self.write_batch = write_batch;
    }
}

fn other_error<E>(error: E) -> raft::Error
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    raft::Error::Store(StorageError::Other(error.into()))
}

#[track_caller]
pub fn block_in_place<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    if cfg!(test) {
        f()
    } else {
        tokio::task::block_in_place(f)
    }
}

#[cfg(test)]
mod tests {
    use crate::raft::storage::RocksDbStorage;
    use googletest::IntoTestResult;
    use raft::{Error as RaftError, GetEntriesContext, Storage, StorageError};
    use raft_proto::eraftpb::{ConfState, Entry, Snapshot};
    use restate_rocksdb::RocksDbManager;

    #[test_log::test(restate_core::test)]
    async fn initial_values() -> googletest::Result<()> {
        RocksDbManager::init();
        let storage = RocksDbStorage::create().await?;

        assert_eq!(storage.get_last_index(), 0);
        assert_eq!(storage.get_first_index(), 1);
        assert_eq!(storage.get_snapshot()?, Snapshot::default());

        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test_log::test(restate_core::test)]
    async fn append_entries() -> googletest::Result<()> {
        RocksDbManager::init();
        let mut storage = RocksDbStorage::create().await?;

        let last_index = 10;
        let entries = (1..=last_index)
            .map(|index| Entry {
                index,
                term: 1,
                data: index.to_be_bytes().to_vec().into(),
                ..Entry::default()
            })
            .collect();

        storage.append(&entries).await?;

        assert_eq!(storage.get_last_index(), last_index);
        assert_eq!(storage.get_first_index(), 1);

        let stored_entries =
            storage.entries(1, last_index + 1, None, GetEntriesContext::empty(false))?;
        assert_eq!(stored_entries.len(), entries.len());

        for (stored_entry, entry) in stored_entries.iter().zip(entries.iter()) {
            assert_eq!(stored_entry, entry);
        }

        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test_log::test(restate_core::test)]
    async fn apply_snapshot() -> googletest::Result<()> {
        RocksDbManager::init();
        let mut storage = RocksDbStorage::create().await?;

        let last_index = 10;
        let snapshot_index = 5;
        let entries = (1..=last_index)
            .map(|index| Entry {
                index,
                term: index,
                data: index.to_be_bytes().to_vec().into(),
                ..Entry::default()
            })
            .collect();

        storage.append(&entries).await?;

        let mut conf_state = ConfState::default();
        conf_state.set_voters(vec![1, 2, 3]);
        let mut snapshot = Snapshot::default();
        snapshot.mut_metadata().set_index(snapshot_index);
        snapshot.mut_metadata().set_term(snapshot_index);
        snapshot.mut_metadata().set_conf_state(conf_state.clone());
        snapshot.set_data(vec![4, 5, 6].into());

        storage.apply_snapshot(&snapshot).await?;

        assert_eq!(storage.get_last_index(), last_index);
        assert_eq!(storage.get_first_index(), snapshot_index + 1);
        // check that we remember the term of the last compacted entry
        assert_eq!(storage.term(snapshot_index)?, snapshot_index);
        assert_eq!(storage.get_snapshot()?, snapshot);
        assert_eq!(storage.get_conf_state()?, conf_state);
        let hard_state = storage.get_hard_state()?;
        assert_eq!(hard_state.get_commit(), snapshot_index);
        assert_eq!(hard_state.get_term(), snapshot_index);

        for index in (snapshot_index + 1)..=last_index {
            assert_eq!(
                storage.get_entry(index)?.unwrap(),
                entries[index as usize - 1]
            );
        }

        drop(storage);
        // reset RocksDbManager to allow restarting the storage
        RocksDbManager::get().reset().await.into_test_result()?;

        // re-create storage to check that the information is persisted
        let storage = RocksDbStorage::create().await?;

        assert_eq!(storage.get_last_index(), last_index);
        assert_eq!(storage.get_first_index(), snapshot_index + 1);
        // check that we remember the term of the last compacted entry
        assert_eq!(storage.term(snapshot_index)?, snapshot_index);
        assert_eq!(storage.get_snapshot()?, snapshot);
        assert_eq!(storage.get_conf_state()?, conf_state);
        let hard_state = storage.get_hard_state()?;
        assert_eq!(hard_state.get_commit(), snapshot_index);
        assert_eq!(hard_state.get_term(), snapshot_index);

        let stored_entries = storage.entries(
            snapshot_index + 1,
            last_index + 1,
            None,
            GetEntriesContext::empty(false),
        )?;
        assert_eq!(stored_entries.len(), (last_index - snapshot_index) as usize);

        for (stored_entry, entry) in stored_entries
            .iter()
            .zip(&entries[snapshot_index as usize..last_index as usize])
        {
            assert_eq!(stored_entry, entry);
        }

        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test_log::test(restate_core::test)]
    async fn trim() -> googletest::Result<()> {
        RocksDbManager::init();
        let mut storage = RocksDbStorage::create().await?;

        let last_index = 10;
        let entries = (1..=last_index)
            .map(|index| Entry {
                index,
                term: index,
                data: index.to_be_bytes().to_vec().into(),
                ..Entry::default()
            })
            .collect();

        storage.append(&entries).await?;

        let trim_point = 5;
        let mut txn = storage.txn();
        txn.trim(trim_point);
        txn.commit().await?;

        assert_eq!(storage.get_first_index(), trim_point + 1);
        assert_eq!(storage.get_last_index(), last_index);

        let stored_entries = storage.entries(
            trim_point + 1,
            last_index + 1,
            None,
            GetEntriesContext::empty(false),
        )?;

        for (index, entry) in stored_entries.iter().enumerate() {
            assert_eq!(entry, &entries[index + trim_point as usize]);
        }

        // trying to access entries which are out of bounds
        assert_eq!(
            storage.entries(
                trim_point,
                trim_point + 1,
                None,
                GetEntriesContext::empty(false)
            ),
            Err(RaftError::Store(StorageError::Compacted))
        );
        assert!(
            storage
                .entries(
                    last_index + 1,
                    last_index + 2,
                    None,
                    GetEntriesContext::empty(false)
                )
                .is_err()
        );

        RocksDbManager::get().shutdown().await;
        Ok(())
    }

    #[test_log::test(restate_core::test)]
    async fn overwrite_entries() -> googletest::Result<()> {
        RocksDbManager::init();
        let mut storage = RocksDbStorage::create().await?;

        let last_index = 10;
        let entries = (1..=last_index)
            .map(|index| Entry {
                index,
                term: index,
                data: index.to_be_bytes().to_vec().into(),
                ..Entry::default()
            })
            .collect();

        storage.append(&entries).await?;

        let new_last_index = 8;
        let new_entries = (5..=new_last_index)
            .map(|index| Entry {
                index,
                term: index + 1,
                data: (index + 1).to_be_bytes().to_vec().into(),
                ..Entry::default()
            })
            .collect();

        storage.append(&new_entries).await?;

        assert_eq!(storage.get_last_index(), new_last_index);
        let stored_entries =
            storage.entries(1, new_last_index + 1, None, GetEntriesContext::empty(false))?;

        for index in 1..5 {
            assert_eq!(stored_entries[index - 1], entries[index - 1]);
        }

        for index in 5..=(new_last_index as usize) {
            assert_eq!(stored_entries[index - 1], new_entries[index - 5]);
        }

        RocksDbManager::get().shutdown().await;
        Ok(())
    }
}
