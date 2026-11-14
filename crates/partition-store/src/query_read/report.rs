// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! What each partition replica open on this node records about its indexes and
//! migration jobs, for the cluster tables `indexes` and `migration_jobs`.
//!
//! Each replica is read through one snapshot, so its registry, walks and job
//! records agree with each other; two replicas, or two reports, may be at
//! different points. The reads block, so they run on the query IO pool. A
//! replica whose records can't be read is reported as such, never as a replica
//! without indexes.

use std::sync::Arc;

use restate_storage_api::Result;
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::PartitionStoreManager;
use crate::fsm_table::{fsm_variable, migration_jobs_key, registry_key, walks_key};
use crate::job::{MigrationJobRecords, MigrationJobs};
use crate::keys::{DecodeTableKey, KeyKind};
use crate::read_snapshot::PartitionSnapshotReader;
use crate::registry::IndexRegistry;
use crate::vqueue_table::EntryStatusKey;
use crate::walk::{Source, Walk, Walks};

use super::QueryIoExecutor;

/// What one open replica records about its indexes and migration jobs, as of one
/// snapshot.
#[derive(Debug, Clone)]
pub struct ReplicaMigrations {
    pub partition_id: PartitionId,
    pub key_range: KeyRange,
    pub registry: IndexRegistry,
    pub walks: Walks,
    pub jobs: MigrationJobRecords,
}

impl ReplicaMigrations {
    /// How far `walk` is through this replica's key range, in percent: the
    /// position of its cursor's partition key. Approximate, because entries spread
    /// over the range by hashing, and never evidence of completion. `None` for a
    /// walk whose source isn't entry statuses, including a source this binary
    /// doesn't know, before the first slice, or when the range holds a single key.
    pub fn progress_percent(&self, walk: &Walk) -> Option<u32> {
        let (done, span) = self.position(walk)?;
        u32::try_from(done * 100 / span).ok()
    }

    /// When the current pass of `walk` should end if it keeps the rate it has had
    /// since it started at `pass_started_at`: the time it has run so far, stretched
    /// by how much of the key range it still has ahead. Times are milliseconds
    /// since the Unix epoch. Approximate for the same reason as
    /// [`Self::progress_percent`], but from the exact cursor position: a pass shown
    /// at 0% because the percent rounds down can already have an estimate. `None`
    /// while progress is unknown or the cursor hasn't moved past the range's first
    /// partition key, or when `now` is before `pass_started_at`.
    ///
    /// For example, a pass that started at 10:00 and is 25% through at 10:15 should
    /// end at 11:00.
    pub fn eta(&self, walk: &Walk, pass_started_at: u64, now: u64) -> Option<u64> {
        let (done, span) = self.position(walk)?;
        if done == 0 {
            return None;
        }
        let elapsed = u128::from(now.checked_sub(pass_started_at)?);
        pass_started_at.checked_add(u64::try_from(elapsed * span / done).ok()?)
    }

    /// How many partition keys of this replica's range `walk`'s cursor is past, and
    /// how many the range spans.
    fn position(&self, walk: &Walk) -> Option<(u128, u128)> {
        if walk.source() != Some(Source::EntryStatus) {
            return None;
        }
        let mut cursor = walk.cursor.as_deref()?;
        let key = EntryStatusKey::deserialize_from(&mut cursor).ok()?;
        let (start, end) = (self.key_range.start(), self.key_range.end());
        let span = u128::from(end.checked_sub(start).filter(|span| *span > 0)?);
        let done = u128::from(key.id.partition_key().checked_sub(start)?);
        Some((done.min(span), span))
    }
}

/// A replica whose records couldn't be read.
#[derive(Debug, Clone)]
pub struct UnreadableReplica {
    pub partition_id: PartitionId,
    /// Which record: `registry`, `walks`, `unfinished jobs` or `job history`.
    pub record: &'static str,
    pub error: String,
}

/// The records of every replica open on this node.
#[derive(Debug, Clone, Default)]
pub struct MigrationReport {
    pub replicas: Vec<ReplicaMigrations>,
    pub unreadable: Vec<UnreadableReplica>,
}

impl PartitionStoreManager {
    /// Reads the index and migration records of every partition replica open on
    /// this node, on `executor`'s pool. A replica in the middle of opening or
    /// closing is skipped; one that closes meanwhile is left out.
    ///
    /// Dropping the returned future before a worker starts the report releases
    /// what it captured at once. Once started, the report reads every replica to
    /// the end, with no point to stop in between; each replica's snapshot is
    /// released before the next replica is read.
    pub fn migration_report(
        &self,
        executor: &QueryIoExecutor,
    ) -> Result<impl Future<Output = Result<MigrationReport>> + Send + 'static> {
        let dbs = self.open_partition_dbs();
        executor.spawn(move || {
            let mut report = MigrationReport::default();
            for db in dbs {
                // Admitted until its reads are done; refused once the replica closes.
                let Ok(snapshot) = db.read_snapshot() else {
                    continue;
                };
                let partition = db.partition();
                match read_replica(&snapshot.reader(), partition.id(), partition.key_range) {
                    Ok(replica) => report.replicas.push(replica),
                    Err((record, error)) => report.unreadable.push(UnreadableReplica {
                        partition_id: partition.id(),
                        record,
                        error,
                    }),
                }
            }
            report
        })
    }
}

/// Reads one replica's records through `reader`, or names the record that can't
/// be read and why.
fn read_replica(
    reader: &PartitionSnapshotReader<'_>,
    partition_id: PartitionId,
    key_range: KeyRange,
) -> std::result::Result<ReplicaMigrations, (&'static str, String)> {
    let jobs = |state_id, record| {
        decode(
            reader,
            migration_jobs_key(partition_id, state_id),
            record,
            |value| MigrationJobs::decode(value).map_err(|err| err.to_string()),
        )
    };
    Ok(ReplicaMigrations {
        partition_id,
        key_range,
        registry: decode(reader, registry_key(partition_id), "registry", |value| {
            IndexRegistry::decode(value).map_err(|err| err.to_string())
        })?,
        walks: decode(reader, walks_key(partition_id), "walks", |value| {
            Walks::decode(value).map_err(|err| err.to_string())
        })?,
        jobs: MigrationJobRecords {
            unfinished: jobs(fsm_variable::MIGRATION_JOBS, "unfinished jobs")?,
            history: Arc::new(jobs(fsm_variable::MIGRATION_JOB_HISTORY, "job history")?),
        },
    })
}

/// The `record` stored at `key`, decoded, or its default when absent.
fn decode<T: Default>(
    reader: &PartitionSnapshotReader<'_>,
    key: impl AsRef<[u8]>,
    record: &'static str,
    decode: impl FnOnce(&[u8]) -> std::result::Result<T, String>,
) -> std::result::Result<T, (&'static str, String)> {
    let value = reader
        .get_pinned(KeyKind::Fsm, key.as_ref())
        .map_err(|err| (record, err.to_string()))?;
    value
        .map_or(Ok(T::default()), |value| decode(&value))
        .map_err(|err| (record, err))
}
