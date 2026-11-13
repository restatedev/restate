// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The walk engine: runs a walk one slice at a time through a supplied
//! transaction, the same way while a partition opens and while its processor runs.
//!
//! A **slice** reads source keys after the walk's cursor through a fresh
//! iterator, so no iterator or snapshot outlives it. The walk's job stages its
//! work on them one at a time, and the engine stages the walk advanced over the
//! keys whose work it took or, once no key remains up to the walk's end, the
//! job's finish and the walk's removal. A [`SliceBudget`] bounds the keys read,
//! their bytes, and the bytes of the staged batch with the records it commits
//! with. One commit makes the work and the progress visible together. Only
//! after it does
//! the engine publish the committed records (the [`MaintainedIndexes`]) to the
//! transaction and the partition, so the next write sees the new cursor and a
//! failed commit never publishes. Starting a walk and recording a failure commit
//! and publish the same way.
//!
//! For example, a walk over `A..E` with cursor `B` and a limit of two keys reads
//! `C` and `D`, commits their work with cursor `D`, then publishes cursor `D`.
//! The next slice reads `E`, finds nothing after it, and commits its work with
//! the job's finish. Had the work on `D` taken the staged batch past its byte
//! limit, the first slice would have dropped it and committed cursor `C`: the
//! next slice starts at `D`, and a slice stopped by a limit never finishes the
//! walk, even when it read up to its end.
//!
//! ```text
//! source    A   B   C   D   E            end = E, cursor = B, budget: two keys
//! slice 1   read C, D  ─▶ stage work(C), work(D) ─▶ commit {work, cursor = D} ─▶ publish
//! slice 2   read E, none after ─▶ stage work(E), finish ─▶ commit {work, Ready, walk removed}
//! ```
//!
//! A slice always takes the work on its first key, so a key whose work alone
//! exceeds a byte limit still makes progress, in a slice of its own: the slice
//! stops before staging anything for the next key. Likewise, a slice that finds
//! no key left always commits the job's finish and the walk's removal, whatever
//! their size.
//!
//! A slice needs a transaction without pending writes. A failure before the
//! commit clears what the slice staged, so the committed records still describe
//! the store and the walk can retry ([`SliceError::Staging`]). A failed commit has
//! an unknown outcome, so the caller stops writing and reopens the partition,
//! which reloads the records ([`SliceError::Commit`]). Drivers choose which walk
//! runs, flush a start before its first slice, retry, and pace.

use std::sync::Arc;

use anyhow::anyhow;
use bytes::{Bytes, BytesMut};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use restate_storage_api::vqueue_table::OwnedEntryStatusHeader;
use restate_storage_api::{StorageError, Transaction};
use restate_types::identifiers::BaseEntryId;

use crate::fsm_table::{put_registry, put_walks};
use crate::keys::DecodeTableKey;
use crate::registry::{IndexBackfill, IndexRegistry, MaintainedIndexes};
use crate::scan::TableScan;
use crate::vqueue_table::{EntryStatusKey, decode_entry_status};
use crate::{MigrationError, PartitionDb, PartitionStoreTransaction};

use super::{Walk, WalkError, WalkKind, Walks};

/// An entry status a slice read: its key, ID and decoded header.
pub(crate) type SourceEntry = (Vec<u8>, BaseEntryId, OwnedEntryStatusHeader);

/// The bounds of one slice; see the module documentation. A key count alone
/// keeps the default byte limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SliceBudget {
    /// Source keys read.
    pub(crate) keys: usize,
    /// Bytes of the source keys and values read.
    pub(crate) read_bytes: usize,
    /// Bytes of the staged batch, including the walk and registry records it
    /// commits with.
    pub(crate) write_bytes: usize,
}

impl SliceBudget {
    pub(crate) const READ_BYTES: usize = 4 << 20;
    pub(crate) const WRITE_BYTES: usize = 8 << 20;
}

impl From<usize> for SliceBudget {
    fn from(keys: usize) -> Self {
        Self {
            keys,
            read_bytes: Self::READ_BYTES,
            write_bytes: Self::WRITE_BYTES,
        }
    }
}

/// What a committed slice did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Slice {
    /// Committed work and the advanced cursor; the walk continues.
    Progressed,
    /// Committed the last work and the job's finish; the walk is removed.
    Exhausted,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SliceError {
    /// Nothing was committed, and nothing the call staged remains; the walk can
    /// retry.
    #[error(transparent)]
    Staging(StorageError),
    /// The commit failed with an unknown outcome: stop writing until the partition
    /// reopens and reloads its records.
    #[error(transparent)]
    Commit(StorageError),
}

impl From<SliceError> for StorageError {
    fn from(err: SliceError) -> Self {
        match err {
            SliceError::Staging(err) | SliceError::Commit(err) => err,
        }
    }
}

/// A job's side of its walks. The engine reads the source, keeps the walk's
/// progress and commits; a job only stages its own work.
pub(crate) trait WalkJob {
    /// Stages the work on `entry`, the next source entry of the walk.
    fn stage(
        &self,
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        entry: &SourceEntry,
    ) -> Result<(), StorageError>;

    /// Applies what exhausting the walk publishes to `registry`, which commits
    /// with the walk's last work and removal.
    fn finish(&self, registry: &mut IndexRegistry) -> Result<(), StorageError>;
}

/// The job running `walk`.
fn job(active: &MaintainedIndexes, walk: &Walk) -> Result<impl WalkJob, StorageError> {
    if walk.kind != WalkKind::index_backfill() {
        return Err(walk_error(WalkError::UnsupportedKind(walk.kind.clone())));
    }
    IndexBackfill::new(active, walk.id)
}

/// Runs one slice of walk `id` within `budget`; see the module documentation.
pub(crate) async fn run_slice(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    id: u64,
    budget: impl Into<SliceBudget>,
) -> Result<Slice, SliceError> {
    let budget = budget.into();
    let active = tx.maintained_indexes();
    let (registry, walks, slice) = stage(tx, |tx| {
        let walk = active
            .walks()
            .get(id)
            .ok_or(WalkError::UnknownId(id))
            .map_err(walk_error)?;
        let job = job(&active, walk)?;
        let (entries, read_all) = read_source(
            db,
            walk.cursor.as_deref(),
            Some(&walk.end),
            budget.keys.max(1),
            budget.read_bytes,
        )?;
        // The records this slice commits with, generously: the advanced walk
        // differs only in its fixed-size cursor and counters, and finishing
        // shrinks the registry.
        let records = RECORD_OVERHEAD
            + bilrost::Message::encoded_len(active.walks())
            + bilrost::Message::encoded_len(active.registry());
        let mut taken = 0;
        for entry in &entries {
            if taken > 0 {
                if tx.estimated_size_in_bytes() + records > budget.write_bytes {
                    break;
                }
                tx.set_save_point();
            }
            job.stage(tx, db, entry)?;
            if taken > 0 && tx.estimated_size_in_bytes() + records > budget.write_bytes {
                tx.rollback_to_save_point()?;
                break;
            }
            taken += 1;
        }
        let mut registry = active.registry().clone();
        let mut walks = active.walks().clone();
        let slice = if read_all && taken == entries.len() {
            job.finish(&mut registry)?;
            walks.remove(id).map_err(walk_error)?;
            put_registry(tx, db.partition().id(), &registry)?;
            Slice::Exhausted
        } else {
            let (last, ..) = &entries[taken - 1];
            walks
                .advance(id, Bytes::copy_from_slice(last), taken as u64)
                .map_err(walk_error)?;
            Slice::Progressed
        };
        put_walks(tx, db.partition().id(), &walks)?;
        Ok((registry, walks, slice))
    })?;
    commit(tx, db, &active, registry, walks).await?;
    Ok(slice)
}

/// Bytes a staged record adds besides its encoded value: its key and the batch's
/// own framing, with room to spare.
const RECORD_OVERHEAD: usize = 256;

/// The startup driver: runs walk `id` to exhaustion in slices within `budget`,
/// checking `cancel` between them. A failure fails the open, after recording it
/// on the walk when nothing was committed.
pub(crate) async fn run_to_end(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    id: u64,
    budget: SliceBudget,
    cancel: &CancellationToken,
) -> Result<(), MigrationError> {
    loop {
        if cancel.is_cancelled() {
            return Err(MigrationError::MigrationCancelled);
        }
        match run_slice(tx, db, id, budget).await {
            Ok(Slice::Progressed) => {}
            Ok(Slice::Exhausted) => return Ok(()),
            Err(SliceError::Staging(err)) => {
                if let Err(record) = record_failure(tx, db, id, &describe(&err)).await {
                    warn!(walk = id, "Failed to record a failed walk slice: {record}");
                }
                return Err(err.into());
            }
            Err(SliceError::Commit(err)) => return Err(err.into()),
        }
    }
}

/// Records a failed attempt at walk `id` on the committed records, once the
/// failure cleared what it staged. A failure to record it is returned, not
/// recorded.
pub(crate) async fn record_failure(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    id: u64,
    error: &str,
) -> Result<(), SliceError> {
    let active = tx.maintained_indexes();
    let walks = stage(tx, |tx| {
        let mut walks = active.walks().clone();
        walks.record_failure(id, error).map_err(walk_error)?;
        put_walks(tx, db.partition().id(), &walks)?;
        Ok(walks)
    })?;
    commit(tx, db, &active, active.registry().clone(), walks).await
}

/// Runs `f` to stage writes into `tx`, which must have none pending, and clears
/// them if it fails.
pub(crate) fn stage<'a, T>(
    tx: &mut PartitionStoreTransaction<'a>,
    f: impl FnOnce(&mut PartitionStoreTransaction<'a>) -> Result<T, StorageError>,
) -> Result<T, SliceError> {
    if !tx.is_empty() {
        return Err(SliceError::Staging(StorageError::Generic(anyhow!(
            "a walk needs a transaction without pending writes"
        ))));
    }
    f(tx).map_err(|err| {
        tx.clear();
        SliceError::Staging(err)
    })
}

/// Commits what `tx` staged on top of `active`, then publishes `registry` and
/// `walks` as committed. Commit and publication are work of the partition's
/// session: a close waits for them, and a closed session commits nothing, so a
/// slice of a closed partition can never publish into a later session.
pub(crate) async fn commit(
    tx: &mut PartitionStoreTransaction<'_>,
    db: &PartitionDb,
    active: &MaintainedIndexes,
    registry: IndexRegistry,
    walks: Walks,
) -> Result<(), SliceError> {
    let Some(_session) = db.gate().try_acquire() else {
        tx.clear();
        return Err(SliceError::Staging(StorageError::OperationalError));
    };
    tx.commit().await.map_err(SliceError::Commit)?;
    let next = Arc::new(active.successor(registry, walks));
    db.install_maintained_indexes(Arc::clone(&next));
    tx.set_maintained_indexes(next);
    Ok(())
}

/// The last entry-status key of the partition: the end of a walk starting now.
pub(crate) fn last_key(db: &PartitionDb) -> Result<Option<Bytes>, StorageError> {
    let mut arena = BytesMut::new();
    let mut options = rocksdb::ReadOptions::default();
    options.fill_cache(false);
    let mut iterator = db.scan(
        TableScan::ScanPartitionKeyRange::<EntryStatusKey>(db.partition().key_range)
            .encode(&mut arena),
        options,
    )?;
    iterator.seek_to_last();
    let last = iterator.key().map(Bytes::copy_from_slice);
    iterator
        .status()
        .map_err(|err| StorageError::Generic(err.into()))?;
    Ok(last)
}

/// Entry statuses in `(resume, end]`, in key order, through a fresh iterator: at
/// most `keys` of them, and no more than `bytes` of keys and values unless the
/// first alone exceeds them. Also whether none remains after them: an exhausted
/// range, rather than a stop at a limit.
pub(crate) fn read_source(
    db: &PartitionDb,
    resume: Option<&[u8]>,
    end: Option<&[u8]>,
    keys: usize,
    bytes: usize,
) -> Result<(Vec<SourceEntry>, bool), StorageError> {
    const PREALLOCATED: usize = 1_000;
    let mut arena = BytesMut::new();
    let mut options = rocksdb::ReadOptions::default();
    options.fill_cache(false);
    let mut iterator = db.scan(
        TableScan::ScanPartitionKeyRange::<EntryStatusKey>(db.partition().key_range)
            .encode(&mut arena),
        options,
    )?;
    if let Some(resume) = resume {
        iterator.seek(resume);
        if iterator.key() == Some(resume) {
            iterator.next();
        }
    }
    let within = |key: &[u8]| end.is_none_or(|end| key <= end);
    let mut entries = Vec::with_capacity(keys.min(PREALLOCATED));
    let mut read = 0;
    while entries.len() < keys
        && let Some(mut key) = iterator.key().filter(|key| within(key))
    {
        let value = iterator.value().ok_or(StorageError::DataIntegrityError)?;
        read += key.len() + value.len();
        if !entries.is_empty() && read > bytes {
            break;
        }
        let raw = key.to_vec();
        let entry = EntryStatusKey::deserialize_from(&mut key)?.id;
        entries.push((raw, entry, decode_entry_status(&entry, value)?));
        iterator.next();
    }
    iterator
        .status()
        .map_err(|err| StorageError::Generic(err.into()))?;
    let exhausted = iterator.key().is_none_or(|key| !within(key));
    Ok((entries, exhausted))
}

/// `err` with its causes, as a walk records it.
pub(crate) fn describe(err: &StorageError) -> String {
    match err {
        StorageError::Generic(err) | StorageError::Conversion(err) => format!("{err:#}"),
        err => err.to_string(),
    }
}

pub(crate) fn walk_error(err: WalkError) -> StorageError {
    StorageError::Conversion(err.into())
}
