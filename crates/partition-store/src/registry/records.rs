// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! How the registry, its walks and its migration jobs are stored: one key per
//! record, under [`KeyKind::Registry`], so that an operation writes only the
//! records it changes.
//!
//! # Keys
//!
//! ```text
//!  0      2         4              6        7                    15
//!  ┌──────┬─────────┬──────────────┬────────┬────────────────────┐
//!  │ "rg" │ padding │ partition_id │ record │ id: u64 big-endian │
//!  └──────┴─────────┴──────────────┴────────┴────────────────────┘
//! ```
//!
//! | [`RecordKind`] | ID | Value |
//! |---|---|---|
//! | `Registry = 1` | 0 | [`Registry`]: indexes, ID counters, the family's `format` |
//! | `Walk = 2` | walk ID | [`Walk`] |
//! | `Job = 3` | job ID | [`MigrationJob`], with `expires_at` once finished |
//!
//! The key mirrors the statistics prefix (`stats::AggregateKeyPrefix`): the
//! partition ID, not a partition key, so one partition's records are one key
//! range, sorted registry first, then walks and jobs by ID. An open loads them
//! with one prefix scan ([`load`]). The ID in a key must be the ID in its value
//! (0 for the registry); a mismatch is a data-integrity error.
//!
//! # Writes
//!
//! Every operation compares the records it staged with the committed ones and
//! writes only the keys that differ ([`stage_changes`]):
//!
//! | Operation | Keys written |
//! |---|---|
//! | Open (reconcile) | the registry if it changed, walks and jobs it added, changed or removed, unreadable jobs (replaced or deleted) |
//! | Slice that progressed | its walk, its jobs |
//! | Slice that finished | the registry, its walk (deleted), its jobs (finished) |
//! | Pass start | the registry, the new walk, its jobs |
//! | Failure record | its jobs |
//!
//! # Retention
//!
//! A job keeps its key for its whole life. Finishing it rewrites it a last time,
//! with `expires_at = finished_at + worker.storage.finished-migration-jobs-retention`
//! as configured then; only unfinished jobs stay in memory. Readers hide an
//! expired job, and the compaction filter ([`compaction_filter`]) removes it.
//! RocksDB turns that removal into a tombstone, so an older, unfinished version
//! of the job can't reappear. The registry, walks and unfinished jobs never
//! expire.
//!
//! ```text
//! job 70:  Pending ─▶ Running … ─▶ Completed, expires_at = 10:30 + 24h
//!          (each slice rewrites)        │
//!                                       ├─ before expiry: listed, not loaded at open
//!                                       └─ after expiry: hidden; compaction drops it
//! ```
//!
//! # Compatibility
//!
//! The registry's `format` versions the whole family. The key layout, the
//! record kinds and the values' bilrost tags are append-only (README rule 8). A
//! key this binary can't decode, for example of a record kind it doesn't know,
//! fails the open like a newer format; an unreadable job is diagnostics and
//! replaced or deleted instead. The family has no merge operands: the merge
//! operator doesn't handle this key kind, so adding them needs a version floor.

use std::sync::Arc;

use rocksdb::{BoundColumnFamily, CompactionDecision, WriteBatch};
use tracing::warn;
use zerocopy::{IntoBytes, TryFromBytes, big_endian};

use restate_clock::WallClock;
use restate_storage_api::StorageError;
use restate_types::identifiers::PartitionId;

use crate::job::{MigrationJob, MigrationJobError, MigrationJobId, MigrationJobs};
use crate::keys::KeyKind;
use crate::scan::PhysicalScan;
use crate::walk::{Walk, WalkError, Walks};
use crate::{PartitionDb, PartitionStoreTransaction, StorageAccess, StorageIterator};

use super::{Registry, RegistryError};

/// What a registry record holds. The numbers are persisted in keys and never
/// change meaning.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::Immutable,
    zerocopy::KnownLayout,
    zerocopy::Unaligned,
)]
#[repr(u8)]
pub enum RecordKind {
    /// The [`Registry`], under ID 0.
    Registry = 1,
    /// A [`Walk`], under its walk ID.
    Walk = 2,
    /// A [`MigrationJob`], under its job ID.
    Job = 3,
}

impl RecordKind {
    /// The record's name in errors and reports: `registry`, `walk` or `job`.
    pub fn name(self) -> &'static str {
        match self {
            Self::Registry => "registry",
            Self::Walk => "walk",
            Self::Job => "job",
        }
    }
}

/// The key of one registry record; see the module documentation.
///
/// ```
/// use restate_partition_store::registry::records::{RecordKey, RecordKind};
/// use restate_types::identifiers::PartitionId;
/// use zerocopy::IntoBytes;
///
/// let key = RecordKey::new(PartitionId::from(3), RecordKind::Walk, 7);
/// assert_eq!(key.as_bytes(), b"rg\0\0\0\x03\x02\0\0\0\0\0\0\0\x07");
/// let decoded = RecordKey::decode(key.as_bytes()).unwrap();
/// assert_eq!((decoded.record(), decoded.id()), (RecordKind::Walk, 7));
/// // One partition's records, or one kind of them, share a prefix.
/// let walks = RecordKey::prefix(PartitionId::from(3), Some(RecordKind::Walk));
/// assert!(key.as_bytes().starts_with(&walks));
/// ```
#[derive(
    Clone,
    Copy,
    zerocopy::ByteEq,
    zerocopy::IntoBytes,
    zerocopy::TryFromBytes,
    zerocopy::KnownLayout,
    zerocopy::Immutable,
    zerocopy::Unaligned,
)]
#[repr(C)]
pub struct RecordKey {
    key_kind: [u8; 2],
    padding: big_endian::U16,
    partition_id: big_endian::U16,
    record: RecordKind,
    id: big_endian::U64,
}

static_assertions::const_assert_eq!(size_of::<RecordKey>(), 15);

/// The bytes of a partition's prefix: key kind, padding and partition ID.
const PARTITION_PREFIX_LENGTH: usize = 6;

impl RecordKey {
    pub const SERIALIZED_LENGTH: usize = size_of::<Self>();

    /// The key of record `id` of kind `record` in a physical partition.
    pub fn new(partition_id: PartitionId, record: RecordKind, id: u64) -> Self {
        Self {
            key_kind: *KeyKind::Registry.as_bytes(),
            padding: big_endian::U16::ZERO,
            partition_id: big_endian::U16::new(*partition_id),
            record,
            id: big_endian::U64::new(id),
        }
    }

    /// The key of the registry of a physical partition.
    pub fn registry(partition_id: PartitionId) -> Self {
        Self::new(partition_id, RecordKind::Registry, 0)
    }

    /// The key of walk `id`.
    pub fn walk(partition_id: PartitionId, id: u64) -> Self {
        Self::new(partition_id, RecordKind::Walk, id)
    }

    /// The key of job `id`.
    pub fn job(partition_id: PartitionId, id: MigrationJobId) -> Self {
        Self::new(partition_id, RecordKind::Job, u64::from(id.as_u32()))
    }

    /// Validates a whole key: its length, key kind, padding and record kind.
    pub fn decode(key: &[u8]) -> Result<&Self, StorageError> {
        let key = Self::try_ref_from_bytes(key).map_err(|_| StorageError::DataIntegrityError)?;
        if key.key_kind != *KeyKind::Registry.as_bytes() || key.padding.get() != 0 {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(key)
    }

    /// The prefix of every record of a physical partition or, with `record`, of
    /// every record of that kind: the first 6 or 7 bytes of their keys.
    pub fn prefix(partition_id: PartitionId, record: Option<RecordKind>) -> Vec<u8> {
        let key = Self::new(partition_id, RecordKind::Registry, 0);
        let length = PARTITION_PREFIX_LENGTH + usize::from(record.is_some());
        let mut prefix = key.as_bytes()[..length].to_vec();
        if let Some(record) = record {
            prefix[PARTITION_PREFIX_LENGTH] = record as u8;
        }
        prefix
    }

    pub fn partition_id(&self) -> PartitionId {
        PartitionId::from(self.partition_id.get())
    }

    pub fn record(&self) -> RecordKind {
        self.record
    }

    pub fn id(&self) -> u64 {
        self.id.get()
    }
}

impl AsRef<[u8]> for RecordKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::fmt::Debug for RecordKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RecordKey(partition={} {} {})",
            self.partition_id(),
            self.record.name(),
            self.id()
        )
    }
}

/// A scan of a physical partition's records, or of one kind of them.
pub(crate) fn scan(partition_id: PartitionId, record: Option<RecordKind>) -> PhysicalScan<Vec<u8>> {
    PhysicalScan::Prefix(KeyKind::Registry, RecordKey::prefix(partition_id, record))
}

/// A partition's records in memory: the registry, if one was ever written, the
/// walks, and the jobs the reader kept.
#[derive(Debug, Default)]
pub(crate) struct Records {
    pub(crate) registry: Option<Registry>,
    pub(crate) walks: Walks,
    pub(crate) jobs: MigrationJobs,
}

/// Why a partition's records couldn't be read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RecordsError {
    #[error(transparent)]
    Storage(StorageError),
    #[error("malformed registry record key {0:02x?}")]
    Key(Vec<u8>),
    #[error("registry: {0}")]
    Registry(RegistryError),
    #[error("walk {id}: {error}")]
    Walk { id: u64, error: WalkError },
    #[error("walks: {0}")]
    Walks(WalkError),
    #[error("job {id}: {error}")]
    Job { id: u64, error: MigrationJobError },
}

impl RecordsError {
    /// Which record couldn't be read: `records`, `key`, `registry`, `walk` or
    /// `job`.
    pub(crate) fn record(&self) -> &'static str {
        match self {
            Self::Storage(_) => "records",
            Self::Key(_) => "key",
            Self::Registry(_) => RecordKind::Registry.name(),
            Self::Walk { .. } | Self::Walks(_) => RecordKind::Walk.name(),
            Self::Job { .. } => RecordKind::Job.name(),
        }
    }
}

impl From<RecordsError> for StorageError {
    fn from(err: RecordsError) -> Self {
        match err {
            RecordsError::Storage(err) => err,
            err => StorageError::Conversion(err.into()),
        }
    }
}

/// Reads the records `iterator` yields, one partition's in key order: the
/// registry and every walk, validated together, and the decodable jobs for
/// which `keep` holds. An undecodable job goes to `unreadable`, which skips it
/// or fails the read; any other unreadable record fails it.
pub(crate) fn read(
    iterator: &mut impl StorageIterator,
    mut keep: impl FnMut(&MigrationJob) -> bool,
    mut unreadable: impl FnMut(u64, MigrationJobError) -> Result<(), RecordsError>,
) -> Result<Records, RecordsError> {
    let mut records = Records::default();
    let mut walks = Vec::new();
    while let Some((key, value)) = iterator.item().map_err(RecordsError::Storage)? {
        let key = RecordKey::decode(key).map_err(|_| RecordsError::Key(key.to_vec()))?;
        let id = key.id();
        match key.record() {
            RecordKind::Registry if id != 0 => {
                return Err(RecordsError::Registry(RegistryError::Invalid(
                    "the registry's key has an ID",
                )));
            }
            RecordKind::Registry => {
                records.registry = Some(Registry::decode(value).map_err(RecordsError::Registry)?);
            }
            RecordKind::Walk => walks
                .push(Walk::decode(id, value).map_err(|error| RecordsError::Walk { id, error })?),
            RecordKind::Job => match MigrationJob::decode(id, value) {
                Ok(job) if keep(&job) => records.jobs.insert(job),
                Ok(_) => {}
                Err(error) => unreadable(id, error)?,
            },
        }
        iterator.next();
    }
    // Walks sort after the registry, whose counter issued their IDs.
    let next_walk_id = records
        .registry
        .as_ref()
        .map_or(super::FIRST_WALK_ID, Registry::next_walk_id);
    records.walks = Walks::new(walks, next_walk_id).map_err(RecordsError::Walks)?;
    Ok(records)
}

/// Loads the records of `db`'s partition with one prefix scan, as an open
/// does: the registry, every walk and the unfinished jobs. Finished jobs stay on
/// disk. Also returns the IDs of the jobs whose records couldn't be decoded:
/// they are diagnostics, so they are reported and left for the caller to
/// replace or delete, never fail the load. A registry or walk that can't be
/// read fails it.
pub(crate) fn load(db: &PartitionDb) -> Result<(Records, Vec<u64>), StorageError> {
    let mut options = rocksdb::ReadOptions::default();
    options.fill_cache(false);
    let mut iterator = db.scan(scan(db.partition().id(), None), options)?;
    let mut unreadable = Vec::new();
    let records = read(
        &mut iterator,
        |job| !job.status.is_finished(),
        |id, err| {
            warn!(
                partition_id = %db.partition().id(),
                job = id,
                "Replacing the unreadable record of a migration job: {err}"
            );
            unreadable.push(id);
            Ok(())
        },
    )?;
    Ok((records, unreadable))
}

/// Where staged records go: the open's write batch, or the processor's
/// transaction for slices.
pub(crate) trait RecordSink {
    fn put(&mut self, key: &RecordKey, value: &impl bilrost::Message) -> Result<(), StorageError>;
    fn delete(&mut self, key: &RecordKey);
}

impl RecordSink for PartitionStoreTransaction<'_> {
    /// Encodes `value` into the transaction's reusable value buffer.
    fn put(&mut self, key: &RecordKey, value: &impl bilrost::Message) -> Result<(), StorageError> {
        let buffer = self.cleared_value_buffer_mut(value.encoded_len());
        value
            .encode(buffer)
            .map_err(|err| StorageError::Generic(err.into()))?;
        let value = buffer.split();
        self.raw_put_cf(KeyKind::Registry, key.as_bytes(), value);
        Ok(())
    }

    fn delete(&mut self, key: &RecordKey) {
        self.raw_delete_cf(KeyKind::Registry, key.as_bytes());
    }
}

/// A write batch and the partition's column family.
pub(crate) struct BatchSink<'a, 'cf> {
    pub(crate) batch: &'a mut WriteBatch,
    pub(crate) cf: &'a Arc<BoundColumnFamily<'cf>>,
}

impl RecordSink for BatchSink<'_, '_> {
    fn put(&mut self, key: &RecordKey, value: &impl bilrost::Message) -> Result<(), StorageError> {
        self.batch
            .put_cf(self.cf, key.as_bytes(), value.encode_to_vec());
        Ok(())
    }

    fn delete(&mut self, key: &RecordKey) {
        self.batch.delete_cf(self.cf, key.as_bytes());
    }
}

/// One state of a partition's records, as committed or as staged.
#[derive(Debug, Clone, Copy)]
pub(crate) struct View<'a> {
    /// `None`: nothing to write, nothing was ever registered.
    pub(crate) registry: Option<&'a Registry>,
    pub(crate) walks: &'a Walks,
    /// The unfinished jobs.
    pub(crate) jobs: &'a MigrationJobs,
}

/// Stages into `sink` what turns the committed records `before` into `after`,
/// key by key: the registry if it differs, each walk and unfinished job that is
/// new or differs (put) or is gone (deleted), and the final record of each job
/// in `finished`. Records that didn't change are not written.
///
/// For example, a slice that advanced walk 1 and updated jobs 64 and 65 of the
/// three unfinished ones writes three keys: walk 1, job 64 and job 65.
pub(crate) fn stage_changes(
    sink: &mut impl RecordSink,
    partition: PartitionId,
    before: View<'_>,
    after: View<'_>,
    finished: &[MigrationJob],
) -> Result<(), StorageError> {
    if let Some(registry) = after.registry
        && before.registry != Some(registry)
    {
        sink.put(&RecordKey::registry(partition), registry)?;
    }
    for walk in after.walks.entries() {
        if before.walks.get(walk.id) != Some(walk) {
            sink.put(&RecordKey::walk(partition, walk.id), walk)?;
        }
    }
    for walk in before.walks.entries() {
        if after.walks.get(walk.id).is_none() {
            sink.delete(&RecordKey::walk(partition, walk.id));
        }
    }
    for job in after.jobs.entries() {
        if before.jobs.get(job.id()) != Some(job) {
            sink.put(&RecordKey::job(partition, job.id()), job)?;
        }
    }
    for job in finished {
        sink.put(&RecordKey::job(partition, job.id()), job)?;
    }
    for job in before.jobs.entries() {
        let id = job.id();
        if after.jobs.get(id).is_none() && finished.iter().all(|done| done.id() != id) {
            sink.delete(&RecordKey::job(partition, id));
        }
    }
    Ok(())
}

/// The expiry of a job record, decoded without the rest of it: tag 27 of
/// [`MigrationJob`]. Decoding no other field allocates nothing, and a job of a
/// kind or status this binary doesn't know still expires.
#[derive(bilrost::Message)]
struct JobExpiry {
    #[bilrost(27)]
    expires_at: Option<u64>,
}

/// The compaction filter of registry records: removes a job whose record
/// expired by the wall clock now, and keeps everything else, including any
/// record it can't decode. RocksDB writes the removal as a tombstone, so older
/// versions of the job are deleted with it.
pub(crate) fn compaction_filter(key: &[u8], value: &[u8]) -> CompactionDecision {
    if !RecordKey::decode(key).is_ok_and(|key| key.record() == RecordKind::Job) {
        return CompactionDecision::Keep;
    }
    match <JobExpiry as bilrost::OwnedMessage>::decode(value) {
        Ok(JobExpiry {
            expires_at: Some(at),
        }) if at <= WallClock::now_ms().as_u64() => CompactionDecision::Remove,
        _ => CompactionDecision::Keep,
    }
}

/// The record of job `id` in `db`, if it has a readable one. Tests only.
#[cfg(test)]
pub(crate) fn get_job(db: &PartitionDb, id: u32) -> Option<MigrationJob> {
    let key = RecordKey::job(db.partition().id(), MigrationJobId::for_entry(id));
    let value = db
        .rocksdb()
        .inner()
        .as_raw_db()
        .get_pinned_cf(db.cf_handle(), key.as_bytes())
        .unwrap()?;
    MigrationJob::decode(u64::from(id), &value).ok()
}

/// The registry of `db`, validated; `None` when nothing was ever registered.
/// Tests only.
#[cfg(test)]
pub(crate) fn get_registry(db: &PartitionDb) -> Result<Option<Registry>, StorageError> {
    db.rocksdb()
        .inner()
        .as_raw_db()
        .get_pinned_cf(
            db.cf_handle(),
            RecordKey::registry(db.partition().id()).as_bytes(),
        )
        .map_err(|err| StorageError::Generic(err.into()))?
        .map(|value| Registry::decode(&value).map_err(|err| StorageError::Conversion(err.into())))
        .transpose()
}

/// Test helpers that plant records as a binary would have committed them, and
/// observe what a commit wrote.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// Adds to `batch` the registry and the walks as the only walks of `db`,
    /// deleting the others, whether or not they would load.
    pub(crate) fn stage_overwrite(
        db: &PartitionDb,
        batch: &mut WriteBatch,
        registry: &Registry,
        walks: &Walks,
    ) {
        let partition = db.partition().id();
        let start = RecordKey::prefix(partition, Some(RecordKind::Walk));
        let end = RecordKey::prefix(partition, Some(RecordKind::Job));
        batch.delete_range_cf(db.cf_handle(), start, end);
        let mut sink = BatchSink {
            batch,
            cf: db.cf_handle(),
        };
        sink.put(&RecordKey::registry(partition), registry).unwrap();
        for walk in walks.entries() {
            sink.put(&RecordKey::walk(partition, walk.id), walk)
                .unwrap();
        }
    }

    /// Writes the registry and the walks as the only walks of `db`.
    pub(crate) fn overwrite(db: &PartitionDb, registry: &Registry, walks: &Walks) {
        let mut batch = WriteBatch::default();
        stage_overwrite(db, &mut batch, registry, walks);
        db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
    }

    /// Writes the records of `jobs`, keeping those of other jobs.
    pub(crate) fn put_jobs<'a>(db: &PartitionDb, jobs: impl IntoIterator<Item = &'a MigrationJob>) {
        let mut batch = WriteBatch::default();
        let mut sink = BatchSink {
            batch: &mut batch,
            cf: db.cf_handle(),
        };
        for job in jobs {
            sink.put(&RecordKey::job(db.partition().id(), job.id()), job)
                .unwrap();
        }
        db.rocksdb().inner().as_raw_db().write(&batch).unwrap();
    }

    /// Every record key and value of `db`'s partition, raw. With
    /// `memtables_only`, only what was written since the last flush: after a
    /// flush, the keys the next commits put.
    pub(crate) fn raw(db: &PartitionDb, memtables_only: bool) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut options = rocksdb::ReadOptions::default();
        if memtables_only {
            options.set_read_tier(rocksdb::ReadTier::Memtable);
        }
        let mut iterator = db.scan(scan(db.partition().id(), None), options).unwrap();
        let mut records = Vec::new();
        while let Some((key, value)) = StorageIterator::item(&iterator).unwrap() {
            records.push((key.to_vec(), value.to_vec()));
            StorageIterator::next(&mut iterator);
        }
        records
    }

    /// The database's last sequence number, which every written key advances:
    /// unchanged across an operation that wrote nothing.
    pub(crate) fn sequence(db: &PartitionDb) -> u64 {
        db.rocksdb().inner().as_raw_db().latest_sequence_number()
    }

    /// The records of `db`'s partition put since its last flush, by kind and ID.
    pub(crate) fn written(db: &PartitionDb) -> Vec<(RecordKind, u64)> {
        raw(db, true)
            .iter()
            .map(|(key, _)| {
                let key = RecordKey::decode(key).unwrap();
                (key.record(), key.id())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use restate_util_string::ReString;

    use crate::job::MigrationJobStatus;
    use crate::walk::{Source, WalkKind};

    use super::*;

    /// Records in memory, in key order, as a scan yields them.
    struct Scan(
        std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
        Option<(Vec<u8>, Vec<u8>)>,
    );

    impl Scan {
        fn new(records: Vec<(RecordKey, Vec<u8>)>) -> Self {
            Self::raw(
                records
                    .into_iter()
                    .map(|(key, value)| (key.as_bytes().to_vec(), value))
                    .collect(),
            )
        }

        fn raw(mut records: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
            records.sort();
            let mut records = records.into_iter();
            let first = records.next();
            Self(records, first)
        }
    }

    impl StorageIterator for Scan {
        fn item(&self) -> crate::Result<Option<(&[u8], &[u8])>> {
            Ok(self.1.as_ref().map(|(key, value)| (&key[..], &value[..])))
        }

        fn next(&mut self) {
            self.1 = self.0.next();
        }
    }

    /// The keys a sink was asked to put (`true`) or delete (`false`).
    #[derive(Default)]
    struct Recorded(Vec<(RecordKind, u64, bool)>);

    impl RecordSink for Recorded {
        fn put(&mut self, key: &RecordKey, _: &impl bilrost::Message) -> crate::Result<()> {
            self.0.push((key.record(), key.id(), true));
            Ok(())
        }

        fn delete(&mut self, key: &RecordKey) {
            self.0.push((key.record(), key.id(), false));
        }
    }

    #[test]
    fn reads_check_ids_and_changes_write_only_what_differs() {
        let partition = PartitionId::from(1);
        let mut registry = Registry::default();
        let index = registry
            .register("stat/1", crate::registry::IndexKind::Aggregate)
            .unwrap();
        let mut walks = Walks::default();
        let walk = walks
            .start(
                &mut registry,
                WalkKind::index_backfill(),
                Source::EntryStatus,
                Bytes::from_static(b"e"),
            )
            .unwrap();
        let job = |id| {
            MigrationJob::index_backfill(id, ReString::from_static("stat/1"), None, None, None)
        };
        let mut finished = job(index + 1);
        finished.finish(MigrationJobStatus::Completed, Some(1), 2);
        let valid = || {
            vec![
                (RecordKey::registry(partition), registry.encode()),
                (
                    RecordKey::walk(partition, walk),
                    walks.get(walk).unwrap().encode(),
                ),
                (
                    RecordKey::job(partition, MigrationJobId::for_entry(index)),
                    job(index).encode(),
                ),
                (RecordKey::job(partition, finished.id()), finished.encode()),
            ]
        };
        let read_all = |records| {
            let mut unreadable = Vec::new();
            read(
                &mut Scan::new(records),
                |job| !job.status.is_finished(),
                |id, _| {
                    unreadable.push(id);
                    Ok(())
                },
            )
            .map(|read| (read, unreadable))
        };

        // A valid family loads the registry, its walks and the jobs kept.
        let (loaded, unreadable) = read_all(valid()).unwrap();
        assert_eq!(loaded.registry.as_ref(), Some(&registry));
        assert_eq!(loaded.walks, walks);
        assert_eq!(loaded.jobs.entries(), [job(index)]);
        assert!(unreadable.is_empty());

        // A record under another record's ID: a job is unreadable, a walk or the
        // registry fails the read, as do an unknown record kind and a walk the
        // registry's counter didn't issue.
        let mut moved = valid();
        moved[2].0 = RecordKey::job(partition, MigrationJobId::for_entry(index + 7));
        assert_eq!(read_all(moved).unwrap().1, [u64::from(index + 7)]);
        let failed = |change: fn(&mut Vec<(RecordKey, Vec<u8>)>)| {
            let mut records = valid();
            change(&mut records);
            read_all(records).unwrap_err().record()
        };
        assert_eq!(
            failed(|r| r[0].0 = RecordKey::new(PartitionId::from(1), RecordKind::Registry, 1)),
            "registry"
        );
        assert_eq!(
            failed(|r| r[1].0 = RecordKey::walk(PartitionId::from(1), 2)),
            "walk"
        );
        assert_eq!(failed(|r| r[0].1 = Registry::default().encode()), "walk");
        let mut unknown: Vec<_> = valid()
            .into_iter()
            .map(|(key, value)| (key.as_bytes().to_vec(), value))
            .collect();
        unknown.push((b"rg\0\0\0\x01\x04\0\0\0\0\0\0\0\x01".to_vec(), Vec::new()));
        assert!(matches!(
            read(&mut Scan::raw(unknown), |_| true, |_, _| Ok(())),
            Err(RecordsError::Key(_))
        ));

        // Only what differs is written: the advanced walk and the changed job;
        // the finished job's final record; the removed walk and the job gone
        // without finishing are deleted. The unchanged registry is not.
        let before_jobs = loaded.jobs;
        let mut after_walks = walks.clone();
        after_walks.advance(walk, Bytes::from_static(b"a")).unwrap();
        let mut after_jobs = before_jobs.clone();
        after_jobs
            .get_mut(MigrationJobId::for_entry(index))
            .unwrap()
            .slices = 1;
        let before = View {
            registry: Some(&registry),
            walks: &walks,
            jobs: &before_jobs,
        };
        let mut sink = Recorded::default();
        stage_changes(
            &mut sink,
            partition,
            before,
            View {
                walks: &after_walks,
                jobs: &after_jobs,
                ..before
            },
            &[],
        )
        .unwrap();
        assert_eq!(
            sink.0,
            [
                (RecordKind::Walk, walk, true),
                (RecordKind::Job, u64::from(index), true)
            ]
        );
        let mut sink = Recorded::default();
        let mut done = job(index);
        done.finish(MigrationJobStatus::Completed, Some(1), 2);
        stage_changes(
            &mut sink,
            partition,
            before,
            View {
                registry: Some(&Registry::default()),
                walks: &Walks::default(),
                jobs: &MigrationJobs::default(),
            },
            &[done],
        )
        .unwrap();
        assert_eq!(
            sink.0,
            [
                (RecordKind::Registry, 0, true),
                (RecordKind::Walk, walk, false),
                (RecordKind::Job, u64::from(index), true)
            ]
        );
        let mut sink = Recorded::default();
        stage_changes(
            &mut sink,
            partition,
            before,
            View {
                jobs: &MigrationJobs::default(),
                ..before
            },
            &[],
        )
        .unwrap();
        assert_eq!(sink.0, [(RecordKind::Job, u64::from(index), false)]);
    }

    #[test]
    fn keys_hold_partition_kind_and_id_and_reject_anything_else() {
        let partition = PartitionId::from(8);
        let job = RecordKey::job(partition, MigrationJobId::for_entry(70_000));
        assert_eq!(
            job.as_bytes(),
            b"rg\x00\x00\x00\x08\x03\x00\x00\x00\x00\x00\x01\x11\x70",
            "kind, padded partition, record, u64 ID"
        );
        let decoded = RecordKey::decode(job.as_bytes()).unwrap();
        assert_eq!(
            (decoded.partition_id(), decoded.record(), decoded.id()),
            (partition, RecordKind::Job, 70_000)
        );
        assert_eq!(*decoded, job);
        assert_eq!(format!("{job:?}"), "RecordKey(partition=8 job 70000)");
        let corrupt = |at: usize, byte: u8| {
            let mut key = job.as_bytes().to_vec();
            key[at] = byte;
            RecordKey::decode(&key).is_err()
        };
        assert!(corrupt(1, b'S'), "other key kinds");
        assert!(corrupt(3, 1), "nonzero padding");
        assert!(corrupt(6, 0) && corrupt(6, 4), "unknown record kinds");
        assert!(RecordKey::decode(&job.as_bytes()[..14]).is_err(), "short");
        assert!(
            RecordKey::decode(&[job.as_bytes(), b"x"].concat()).is_err(),
            "long"
        );

        // The registry sorts before the walks, the walks before the jobs, and
        // each kind by ID, within one partition's prefix.
        let keys = [
            RecordKey::registry(partition),
            RecordKey::walk(partition, 1),
            RecordKey::walk(partition, 256),
            RecordKey::job(partition, MigrationJobId::for_entry(64)),
        ];
        assert!(
            keys.windows(2)
                .all(|pair| pair[0].as_bytes() < pair[1].as_bytes())
        );
        let all = RecordKey::prefix(partition, None);
        let walks = RecordKey::prefix(partition, Some(RecordKind::Walk));
        assert!(keys.iter().all(|key| key.as_bytes().starts_with(&all)));
        assert_eq!(
            keys.map(|key| key.as_bytes().starts_with(&walks)),
            [false, true, true, false]
        );
        let other = RecordKey::registry(PartitionId::from(9));
        assert!(!other.as_bytes().starts_with(&all));
    }

    #[test]
    fn expired_jobs_are_filtered_and_everything_else_kept() {
        let now = WallClock::now_ms().as_u64();
        let mut job =
            MigrationJob::index_backfill(70, ReString::from_static("stat/1"), None, None, None);
        let key = |record| RecordKey::new(PartitionId::from(1), record, 70);
        let decide = |record, job: &MigrationJob| {
            matches!(
                compaction_filter(key(record).as_bytes(), &job.encode()),
                CompactionDecision::Remove
            )
        };
        assert!(!decide(RecordKind::Job, &job), "unfinished");
        job.finish(MigrationJobStatus::Completed, Some(now), now + 60_000);
        assert!(!decide(RecordKind::Job, &job), "not expired yet");
        job.expires_at = Some(now - 1);
        assert!(decide(RecordKind::Job, &job), "expired");
        assert!(!decide(RecordKind::Walk, &job), "only jobs expire");
        assert!(matches!(
            compaction_filter(key(RecordKind::Job).as_bytes(), b"\xff\xff"),
            CompactionDecision::Keep
        ));
        assert!(matches!(
            compaction_filter(b"rg-short", &job.encode()),
            CompactionDecision::Keep
        ));
    }
}
