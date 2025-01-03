// Copyright (c) 2023 - 2024 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BytesMut};
use flexbuffers::{DeserializationError, SerializationError};
use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::storage::{Entry, StopSign, Storage, StorageOp, StorageResult};
use restate_rocksdb::{
    CfName, CfPrefixPattern, DbName, DbSpecBuilder, RocksAccess, RocksDbManager,
};
use restate_types::config::{data_dir, MetadataStoreOptions, RocksDbOptions};
use restate_types::live::BoxedLiveLoad;
use restate_types::storage::{
    StorageCodec, StorageDecode, StorageDecodeError, StorageEncode, StorageEncodeError,
};
use rocksdb::{BoundColumnFamily, WriteBatch, WriteOptions, DB};
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use crate::omnipaxos::{BuildError, OmniPaxosConfiguration};
use crate::util;

const DB_NAME: &str = "omni-paxos-metadata-store";
const OMNI_PAXOS_CF: &str = "omni_paxos";

const NPROM: &[u8] = b"NPROM";
const ACC: &[u8] = b"ACC";
const DECIDE: &[u8] = b"DECIDE";
const TRIM: &[u8] = b"TRIM";
const STOPSIGN: &[u8] = b"STOPSIGN";
const SNAPSHOT: &[u8] = b"SNAPSHOT";
const CONFIGURATION: &[u8] = b"CONFIG";

#[derive(Debug, thiserror::Error)]
#[error("missing bytes")]
struct MissingBytesError;

pub struct RocksDbStorage<T> {
    db: Arc<DB>,

    write_batch: WriteBatch,
    next_log_key: usize,
    buffer: BytesMut,

    _phantom: PhantomData<T>,
}

impl<T> RocksDbStorage<T>
where
    T: Entry + StorageEncode + StorageDecode,
{
    pub async fn create(
        options: &MetadataStoreOptions,
        rocksdb_options: BoxedLiveLoad<RocksDbOptions>,
    ) -> Result<Self, BuildError> {
        let db_name = DbName::new(DB_NAME);
        let db_manager = RocksDbManager::get();
        let cfs = vec![CfName::new(OMNI_PAXOS_CF)];
        let db_spec = DbSpecBuilder::new(
            db_name.clone(),
            data_dir("omni-paxos-metadata-store"),
            util::db_options(options),
        )
        .add_cf_pattern(
            CfPrefixPattern::ANY,
            util::cf_options(options.rocksdb_memory_budget()),
        )
        .ensure_column_families(cfs)
        .build()
        .expect("valid spec");

        let db = db_manager.open_db(rocksdb_options, db_spec).await?;

        let next_log_key = Self::find_next_log_key(&db);

        Ok(Self {
            db,
            write_batch: WriteBatch::default(),
            next_log_key,
            buffer: BytesMut::default(),
            _phantom: PhantomData,
        })
    }

    fn find_next_log_key(db: &Arc<DB>) -> usize {
        // Create next log key from the state of the database
        let mut log_iter =
            db.raw_iterator_cf(&db.cf_handle(OMNI_PAXOS_CF).expect("OMNI_PAXOS_CF exists"));
        log_iter.seek_to_last();
        let next_log_key =
            if log_iter.valid() {
                // There's a max key in the database. Next key is 1 greater.
                let key = log_iter.key().unwrap();
                assert_eq!(
                    key.len(),
                    8,
                    "Couldn't recover storage: Log key has unexpected format."
                );
                usize::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]) + 1
            } else {
                // No max key in the database. Either there's no entry yet added or they have been
                // trimmed away.
                match db
                    .get(TRIM)
                    .expect("Couldn't recover storage: Reading compacted_idx failed.")
                {
                    Some(bytes) => usize::from_be_bytes(bytes.try_into().expect(
                        "Couldn't recover storage: Compacted index has unexpected format.",
                    )),
                    None => 0,
                }
            };
        next_log_key
    }

    fn write_options(&self) -> WriteOptions {
        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(false);
        // always sync to not lose data
        write_opts.set_sync(true);
        write_opts
    }

    fn serialize_entry(&mut self, entry: &T) -> Result<BytesMut, StorageEncodeError> {
        StorageCodec::encode_and_split(entry, &mut self.buffer)
    }

    fn deserialize_entry(buf: &mut impl Buf) -> Result<T, StorageDecodeError> {
        StorageCodec::decode(buf)
    }

    fn serialize_omni_paxos_entity<E: serde::Serialize>(
        entity: &E,
    ) -> Result<Vec<u8>, SerializationError> {
        flexbuffers::to_vec(entity)
    }

    fn deserialize_omni_paxos_entity<E: for<'a> serde::Deserialize<'a>>(
        buf: impl AsRef<[u8]>,
    ) -> Result<E, DeserializationError> {
        flexbuffers::from_slice(buf.as_ref())
    }

    fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<(), rocksdb::Error> {
        self.db.put_opt(key, value, &self.write_options())
    }

    fn get_log_cf_handle(&self) -> Arc<BoundColumnFamily> {
        self.db
            .cf_handle(OMNI_PAXOS_CF)
            .expect("OMNI_PAXOS_CF exists")
    }

    pub fn batch_append_entry(&mut self, entry: T) -> StorageResult<()> {
        let mut write_batch = mem::take(&mut self.write_batch);
        let serialized_entry = self.serialize_entry(&entry)?;

        write_batch.put_cf(
            &self.get_log_cf_handle(),
            self.next_log_key.to_be_bytes(),
            serialized_entry,
        );
        self.next_log_key += 1;

        self.write_batch = write_batch;
        Ok(())
    }

    pub fn batch_append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        let mut write_batch = mem::take(&mut self.write_batch);

        for entry in entries {
            let serialized_entry = self.serialize_entry(&entry)?;
            write_batch.put_cf(
                &self.get_log_cf_handle(),
                self.next_log_key.to_be_bytes(),
                serialized_entry,
            );
            self.next_log_key += 1;
        }

        self.write_batch = write_batch;

        Ok(())
    }

    fn batch_append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        // Don't need to delete entries that will be overwritten.
        let delete_idx = from_idx + entries.len();
        if delete_idx < self.next_log_key {
            let from_key = delete_idx.to_be_bytes();
            let to_key = self.next_log_key.to_be_bytes();

            let mut write_batch = mem::take(&mut self.write_batch);
            write_batch.delete_range_cf(&self.get_log_cf_handle(), from_key, to_key);
            self.write_batch = write_batch;
        }
        self.next_log_key = from_idx;
        self.batch_append_entries(entries)
    }

    fn batch_set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        let prom_bytes = Self::serialize_omni_paxos_entity(&n_prom)?;
        self.write_batch.put(NPROM, prom_bytes);
        Ok(())
    }

    fn batch_set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        let ld_bytes = ld.to_be_bytes();
        self.write_batch.put(DECIDE, ld_bytes);
        Ok(())
    }

    fn batch_set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        let acc_bytes = Self::serialize_omni_paxos_entity(&na)?;
        self.write_batch.put(ACC, acc_bytes);
        Ok(())
    }

    fn batch_set_compacted_idx(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let trim_bytes = trimmed_idx.to_be_bytes();
        self.write_batch.put(TRIM, trim_bytes);
        Ok(())
    }

    fn batch_trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let from_key = 0_usize.to_be_bytes();
        let to_key = trimmed_idx.to_be_bytes();

        let mut write_batch = mem::take(&mut self.write_batch);
        write_batch.delete_range_cf(&self.get_log_cf_handle(), from_key, to_key);
        self.write_batch = write_batch;
        Ok(())
    }

    fn batch_set_stopsign(&mut self, ss: Option<StopSign>) -> StorageResult<()> {
        let stopsign = Self::serialize_omni_paxos_entity(&ss)?;
        self.write_batch.put(STOPSIGN, stopsign);
        Ok(())
    }

    fn batch_set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        let s = Self::serialize_omni_paxos_entity(&snapshot)?;
        self.write_batch.put(SNAPSHOT, s);
        Ok(())
    }

    pub fn batch_set_configuration(
        &mut self,
        omni_paxos_configuration: &OmniPaxosConfiguration,
    ) -> StorageResult<()> {
        let omni_paxos_configuration = Self::serialize_omni_paxos_entity(omni_paxos_configuration)?;
        self.write_batch
            .put(CONFIGURATION, omni_paxos_configuration);
        Ok(())
    }

    pub fn get_configuration(&self) -> StorageResult<Option<OmniPaxosConfiguration>> {
        let configuration = self.db.get_pinned(CONFIGURATION)?;
        if let Some(configuration_bytes) = configuration {
            Ok(Self::deserialize_omni_paxos_entity(
                configuration_bytes.as_ref(),
            )?)
        } else {
            Ok(None)
        }
    }

    pub fn commit_batch(&mut self) -> StorageResult<()> {
        self.db
            .write_batch(&mem::take(&mut self.write_batch), &self.write_options())?;
        Ok(())
    }
}

impl<T> Storage<T> for RocksDbStorage<T>
where
    T: Entry + StorageEncode + StorageDecode,
{
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        for op in ops {
            match op {
                StorageOp::AppendEntry(entry) => self.batch_append_entry(entry)?,
                StorageOp::AppendEntries(entries) => self.batch_append_entries(entries)?,
                StorageOp::AppendOnPrefix(from_idx, entries) => {
                    self.batch_append_on_prefix(from_idx, entries)?
                }
                StorageOp::SetPromise(bal) => self.batch_set_promise(bal)?,
                StorageOp::SetDecidedIndex(idx) => self.batch_set_decided_idx(idx)?,
                StorageOp::SetAcceptedRound(bal) => self.batch_set_accepted_round(bal)?,
                StorageOp::SetCompactedIdx(idx) => self.batch_set_compacted_idx(idx)?,
                StorageOp::Trim(idx) => self.batch_trim(idx)?,
                StorageOp::SetStopsign(ss) => self.batch_set_stopsign(ss)?,
                StorageOp::SetSnapshot(snap) => self.batch_set_snapshot(snap)?,
            }
        }
        self.commit_batch()
    }

    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        let entry_bytes = self.serialize_entry(&entry)?;
        self.db.put_cf_opt(
            &self.get_log_cf_handle(),
            self.next_log_key.to_be_bytes(),
            entry_bytes,
            &self.write_options(),
        )?;
        self.next_log_key += 1;
        Ok(())
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        let mut batch = WriteBatch::default();
        for entry in entries {
            let serialized_entry = self.serialize_entry(&entry)?;
            batch.put_cf(
                &self.get_log_cf_handle(),
                self.next_log_key.to_be_bytes(),
                serialized_entry,
            );
            self.next_log_key += 1;
        }
        self.db.write_batch(&batch, &self.write_options())?;
        Ok(())
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        // Don't need to delete entries that will be overwritten.
        let delete_idx = from_idx + entries.len();
        if delete_idx < self.next_log_key {
            let from_key = delete_idx.to_be_bytes();
            let to_key = self.next_log_key.to_be_bytes();
            self.db.delete_range_cf_opt(
                &self.get_log_cf_handle(),
                from_key,
                to_key,
                &self.write_options(),
            )?;
        }
        self.next_log_key = from_idx;
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        let prom_bytes = Self::serialize_omni_paxos_entity(&n_prom)?;
        self.put(NPROM, prom_bytes)?;
        Ok(())
    }

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        let ld_bytes = ld.to_be_bytes();
        self.put(DECIDE, ld_bytes)?;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        let decided = self.db.get_pinned(DECIDE)?;
        match decided {
            Some(ld_bytes) => Ok(usize::from_be_bytes(
                ld_bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| MissingBytesError)?,
            )),
            None => Ok(0),
        }
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        let acc_bytes = Self::serialize_omni_paxos_entity(&na)?;
        self.put(ACC, acc_bytes)?;
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        let accepted = self.db.get_pinned(ACC)?;
        match accepted {
            Some(acc_bytes) => {
                let ballot = Self::deserialize_omni_paxos_entity(&acc_bytes)?;
                Ok(Some(ballot))
            }
            None => Ok(None),
        }
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        // Check if the log has entries up to the requested endpoint.
        if to > self.next_log_key || from >= to {
            return Ok(vec![]); // Do an early return
        }

        let mut iter = self.db.raw_iterator_cf(&self.get_log_cf_handle());
        let mut entries = Vec::with_capacity(to - from);
        iter.seek(from.to_be_bytes());
        for _ in from..to {
            let mut entry_bytes = iter.value().ok_or(MissingBytesError)?;
            entries.push(Self::deserialize_entry(&mut entry_bytes)?);
            iter.next();
        }
        Ok(entries)
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        Ok(self.next_log_key - self.get_compacted_idx()?)
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        self.get_entries(from, self.next_log_key)
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        let promise = self.db.get_pinned(NPROM)?;
        match promise {
            Some(pinned_bytes) => Ok(Some(Self::deserialize_omni_paxos_entity(&pinned_bytes)?)),
            None => Ok(None),
        }
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        let stopsign = Self::serialize_omni_paxos_entity(&s)?;
        self.put(STOPSIGN, stopsign)?;
        Ok(())
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        let stopsign = self.db.get_pinned(STOPSIGN)?;
        match stopsign {
            Some(ss_bytes) => Ok(Self::deserialize_omni_paxos_entity(&ss_bytes)?),
            None => Ok(None),
        }
    }

    fn trim(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let from_key = 0_usize.to_be_bytes();
        let to_key = trimmed_idx.to_be_bytes();
        self.db.delete_range_cf_opt(
            &self.get_log_cf_handle(),
            from_key,
            to_key,
            &self.write_options(),
        )?;
        Ok(())
    }

    fn set_compacted_idx(&mut self, trimmed_idx: usize) -> StorageResult<()> {
        let trim_bytes = trimmed_idx.to_be_bytes();
        self.put(TRIM, trim_bytes)?;
        Ok(())
    }

    fn get_compacted_idx(&self) -> StorageResult<usize> {
        let trim = self.db.get(TRIM)?;
        match trim {
            Some(trim_bytes) => Ok(usize::from_be_bytes(
                trim_bytes.try_into().map_err(|_| MissingBytesError)?,
            )),
            None => Ok(0),
        }
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        let s = Self::serialize_omni_paxos_entity(&snapshot)?;
        self.put(SNAPSHOT, s)?;
        Ok(())
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        let snapshot = self.db.get_pinned(SNAPSHOT)?;
        if let Some(snapshot_bytes) = snapshot {
            Ok(Self::deserialize_omni_paxos_entity(
                snapshot_bytes.as_ref(),
            )?)
        } else {
            Ok(None)
        }
    }
}
