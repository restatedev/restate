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
//!
//! The constraints the replica's walk driver last observed come with it, read
//! from the driver's published state rather than the snapshot; see
//! [`ReplicaMigrations::blocked_by`].
//!
//! Index sizes are the exception too: when a report asks for them ([`IndexSizes`]),
//! they are RocksDB's estimate over the replica's current memtables and files,
//! not over its snapshot, so they can be a little newer than the records. The
//! estimate visits the table files whose key ranges cover the index, so a report
//! only makes it when asked.

use std::collections::HashMap;
use std::sync::Arc;

use rocksdb::{Range, SizeApproximationOptions};
use tokio::time::Instant;
use zerocopy::IntoBytes;

use restate_storage_api::Result;
use restate_types::identifiers::PartitionId;
use restate_types::sharding::KeyRange;

use crate::fsm_table::{fsm_variable, migration_jobs_key, registry_key, walks_key};
use crate::job::{MigrationJob, MigrationJobRecords, MigrationJobStatus, MigrationJobs};
use crate::keys::{DecodeTableKey, IndexKeyPrefix, KeyKind};
use crate::read_snapshot::PartitionSnapshotReader;
use crate::registry::{IndexKind, IndexRegistry};
use crate::stats::AggregateKeyPrefix;
use crate::vqueue_table::EntryStatusKey;
use crate::walk::{DriverState, Source, Walk, Walks};
use crate::{PartitionDb, PartitionStoreManager};

use super::QueryIoExecutor;

/// What one open replica records about its indexes and migration jobs, as of one
/// snapshot, and, if the report asked for them, its index sizes, estimated
/// separately.
#[derive(Debug, Clone)]
pub struct ReplicaMigrations {
    pub partition_id: PartitionId,
    pub key_range: KeyRange,
    pub registry: IndexRegistry,
    pub walks: Walks,
    pub jobs: MigrationJobRecords,
    /// RocksDB's estimate of the storage each index's keys take, including
    /// writes not flushed yet, by index ID. Empty unless the report asked for
    /// sizes; an index of unknown kind, or whose estimate returned an error, has
    /// none.
    pub index_sizes: HashMap<u32, u64>,
    /// The scheduling constraints the replica's walk driver last observed, read
    /// next to the snapshot; `None` without a running driver.
    pub driver: Option<DriverState>,
}

impl ReplicaMigrations {
    /// How far `walk` is through this replica's key range, in percent: the
    /// position of its cursor's partition key. Approximate, because entries spread
    /// over the range by hashing, and never evidence of completion. `None` for a
    /// walk whose source this binary doesn't know, before the first slice, or
    /// when the range holds a single key.
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

    /// Why `job` isn't progressing at `now`, according to the constraints the
    /// replica's walk driver last observed: `None` when it reported none that
    /// applies, which doesn't prove progress (the driver may be resting between
    /// slices, or not running). In this order:
    ///
    /// - `background-migrations-disabled`, for every unfinished job;
    /// - `snapshot-export`, for a job with a walk, or one waiting for a pass it
    ///   may start now;
    /// - `failure-backoff`, for a job whose walk retries after failed slices, or
    ///   whose next pass start retries after a failed one;
    /// - `throttled`, for a job with a walk, while the token bucket can't cover
    ///   the next slice.
    ///
    /// Backoffs and throttling end at their deadline, and a waiting job may start
    /// its pass once the driver scheduled it, both compared to `now` on the
    /// driver's monotonic clock. Finished jobs have none.
    pub fn blocked_by(&self, job: &MigrationJob, now: Instant) -> Option<&'static str> {
        use MigrationJobStatus::*;
        if !matches!(job.status, Pending | Running | Waiting) {
            return None;
        }
        let driver = self.driver.as_ref()?;
        if driver.disabled {
            return Some("background-migrations-disabled");
        }
        let id = job.id().as_u32();
        let walk = job.walk;
        let may_start = job.status == Waiting
            && driver
                .eligible_at
                .iter()
                .any(|(of, at)| *of == id && *at <= now);
        if driver.snapshot_export && (walk.is_some() || may_start) {
            return Some("snapshot-export");
        }
        let walk_backoff = walk.is_some_and(|walk| {
            driver
                .backoff_walks
                .iter()
                .any(|(of, until)| *of == walk && *until > now)
        });
        let start_backoff = job.status == Waiting
            && driver
                .backoff_starts
                .iter()
                .any(|(of, until)| *of == id && *until > now);
        if walk_backoff || start_backoff {
            return Some("failure-backoff");
        }
        if walk.is_some() && driver.throttled_until.is_some_and(|until| until > now) {
            return Some("throttled");
        }
        None
    }

    /// How many partition keys of this replica's range `walk`'s cursor is past, and
    /// how many the range spans.
    fn position(&self, walk: &Walk) -> Option<(u128, u128)> {
        let mut cursor = walk.cursor.as_deref()?;
        let partition_key = match walk.source()? {
            Source::EntryStatus => EntryStatusKey::deserialize_from(&mut cursor)
                .ok()?
                .id
                .partition_key(),
            Source::JournalCompletionId => {
                crate::journal_table_v2::JournalCompletionIdToCommandIndexKey::deserialize_from(
                    &mut cursor,
                )
                .ok()?
                .partition_key
            }
        };
        let (start, end) = (self.key_range.start(), self.key_range.end());
        let span = u128::from(end.checked_sub(start).filter(|span| *span > 0)?);
        let done = u128::from(partition_key.checked_sub(start)?);
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

/// Whether a migration report estimates the size of each index; see the module
/// documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexSizes {
    Skip,
    Estimate,
}

/// The records of every replica open on this node.
#[derive(Debug, Clone, Default)]
pub struct MigrationReport {
    pub replicas: Vec<ReplicaMigrations>,
    pub unreadable: Vec<UnreadableReplica>,
}

impl PartitionStoreManager {
    /// Reads the index and migration records of every partition replica open on
    /// this node, on `executor`'s pool, with their index sizes if `sizes` asks
    /// for them. A replica in the middle of opening or closing is skipped; one
    /// that closes meanwhile is left out.
    ///
    /// Dropping the returned future before a worker starts the report releases
    /// what it captured at once. Once started, the report reads every replica to
    /// the end, with no point to stop in between; each replica's snapshot is
    /// released before the next replica is read.
    pub fn migration_report(
        &self,
        executor: &QueryIoExecutor,
        sizes: IndexSizes,
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
                    Ok(mut replica) => {
                        replica.driver = db.driver_state().get();
                        // While the snapshot keeps the replica admitted.
                        if sizes == IndexSizes::Estimate {
                            replica.index_sizes = index_sizes(&db, &replica.registry);
                        }
                        report.replicas.push(replica);
                    }
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
        index_sizes: HashMap::new(),
        driver: None,
    })
}

/// RocksDB's estimate of the storage each index of `registry` takes in `db`:
/// the size of the key range its prefix selects, over the current memtables and
/// files. One call per key kind estimates all its indexes. A dropped index may
/// still show bytes until compaction removes its keys. Indexes of an unknown
/// kind are left out, and so are all of a key kind's if its call returns an
/// error.
fn index_sizes(db: &PartitionDb, registry: &IndexRegistry) -> HashMap<u32, u64> {
    let mut options = SizeApproximationOptions::new();
    options.set_include_memtables(true);
    options.set_include_files(true);
    let rocksdb = db.rocksdb().inner().as_raw_db();
    let ranges: Vec<_> = registry
        .entries()
        .iter()
        .filter_map(|index| {
            let (kind, start, end) = index_key_range(index.kind, db.partition().id(), index.id)?;
            Some((kind, index.id, start, end))
        })
        .collect();
    let mut sizes = HashMap::with_capacity(ranges.len());
    for kind in [KeyKind::Stats, KeyKind::SecondaryIndex] {
        let (ids, bounds): (Vec<_>, Vec<_>) = ranges
            .iter()
            .filter(|(of, ..)| *of == kind)
            .map(|(_, id, start, end)| (*id, Range::new(start, end)))
            .unzip();
        if ids.is_empty() {
            continue;
        }
        if let Ok(estimates) = rocksdb.get_approximate_sizes_cf_with_options(
            db.cf_handle_for_key_kind(kind),
            &options,
            &bounds,
        ) {
            sizes.extend(ids.into_iter().zip(estimates));
        }
    }
    sizes
}

/// The keys of index `id` in `partition`: its ten-byte prefix and everything
/// after it that starts with it, up to the prefix's upper bound. A statistic's
/// keys live under `KeyKind::Stats`, a secondary index's under
/// `KeyKind::SecondaryIndex`; both prefixes are `key kind | padding | partition |
/// index ID`. For example, index 5 of partition 3 ends where index 6 of
/// partition 3 starts, and index `u32::MAX` of partition 3 where index 0 of
/// partition 4 starts.
fn index_key_range(
    kind: IndexKind,
    partition: PartitionId,
    id: u32,
) -> Option<(KeyKind, Vec<u8>, Vec<u8>)> {
    let (kind, start) = match kind {
        IndexKind::Aggregate => (
            KeyKind::Stats,
            AggregateKeyPrefix::new(partition, id).as_bytes().to_vec(),
        ),
        IndexKind::Secondary => (
            KeyKind::SecondaryIndex,
            IndexKeyPrefix::new(partition, id).as_bytes().to_vec(),
        ),
        IndexKind::Unknown => return None,
    };
    let mut end = start.clone();
    crate::convert_to_upper_bound(&mut end).then_some((kind, start, end))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_key_ranges_hold_exactly_their_index_keys() {
        let key = |kind: IndexKind, partition: u16, id: u32| {
            let partition = PartitionId::from(partition);
            let mut key = match kind {
                IndexKind::Aggregate => AggregateKeyPrefix::new(partition, id).as_bytes().to_vec(),
                IndexKind::Secondary => IndexKeyPrefix::new(partition, id).as_bytes().to_vec(),
                IndexKind::Unknown => unreachable!(),
            };
            key.extend_from_slice(&[0xFF; 8]);
            key
        };
        let partition = PartitionId::from(3);
        for (kind, key_kind, other) in [
            (IndexKind::Aggregate, KeyKind::Stats, IndexKind::Secondary),
            (
                IndexKind::Secondary,
                KeyKind::SecondaryIndex,
                IndexKind::Aggregate,
            ),
        ] {
            for id in [0, 5, u32::MAX] {
                let (of, start, end) = index_key_range(kind, partition, id).unwrap();
                assert_eq!(of, key_kind);
                let inside = |key: &[u8]| start.as_slice() <= key && key < end.as_slice();
                // The index's own keys, from its bare prefix to its longest keys.
                assert!(inside(&start) && inside(&key(kind, 3, id)), "{kind:?} {id}");
                // Not its neighbors: other IDs, partitions, or the other kind.
                if let Some(previous) = id.checked_sub(1) {
                    assert!(!inside(&key(kind, 3, previous)));
                }
                if let Some(next) = id.checked_add(1) {
                    assert!(!inside(&key(kind, 3, next)));
                }
                assert!(!inside(&key(kind, 2, id)) && !inside(&key(kind, 4, id)));
                assert!(!inside(&key(kind, 4, 0)) && !inside(&key(other, 3, id)));
            }
        }
        assert_eq!(index_key_range(IndexKind::Unknown, partition, 5), None);
    }
}
