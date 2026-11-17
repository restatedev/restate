// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reference online cleanup: delete orphaned journal completion-ID index keys.
//!
//! The walk and the completion storage feature are authoritative; job records
//! are diagnostics and can be reconstructed. Each slice checks the journal's
//! ownership sentinel in its own processor transaction, deletes only orphans,
//! and commits deletions and cursor together. No decision survives a transaction.
//! Slices may split an invocation, so even a huge journal obeys the entry budget.
//!
//! Current journal writes create indexes with their journal, and journal deletion
//! removes the indexes in the same transaction. Thus foreground work does not
//! introduce orphans behind the cursor or beyond the captured end. A single full
//! pass suffices. Completion commits the feature ledger with the final deletions.
//!
//! This walk is discardable by older binaries: deletion of an orphan does not
//! change the live data format, and restarting the sweep is idempotent. This is
//! NOT the appropriate compatibility policy for a data-layout conversion.

use restate_storage_api::StorageError;
use restate_types::identifiers::{InvocationUuid, PartitionKey};
use restate_types::journal_v2::CompletionId;

use crate::job::{MigrationJob, MigrationJobId, MigrationJobRecords, MigrationJobStatus, Stamp};
use crate::journal_table_v2::{JournalCompletionIdToCommandIndexKey, JournalKey};
use crate::keys::{DecodeTableKey, KeyKind};
use crate::registry::IndexRegistry;
use crate::{PartitionDb, PartitionStoreTransaction, StorageAccess};

use super::engine::{self, Settlement, Staged, WalkJob, walk_error};
use super::{Source, WalkKind, Walks};

#[derive(Default)]
pub(crate) struct JcOrphanCleanup {
    // Constructed afresh for each slice: never carry an ownership decision
    // across a commit or foreground work.
    invocation: Option<(PartitionKey, InvocationUuid, bool)>,
}

impl WalkJob for JcOrphanCleanup {
    type Entry = JournalCompletionIdToCommandIndexKey;

    fn decode(raw: &[u8], _value: &[u8]) -> Result<Self::Entry, StorageError> {
        let key_len = KeyKind::SERIALIZED_LENGTH
            + size_of::<PartitionKey>()
            + InvocationUuid::RAW_BYTES_LEN
            + size_of::<CompletionId>();
        if raw.len() != key_len {
            return Err(StorageError::DataIntegrityError);
        }
        let mut rest = raw;
        let key = JournalCompletionIdToCommandIndexKey::deserialize_from(&mut rest)?;
        if !rest.is_empty() {
            return Err(StorageError::DataIntegrityError);
        }
        Ok(key)
    }

    fn job_ids(&self) -> impl Iterator<Item = u32> {
        std::iter::once(MigrationJobId::JC_ORPHAN_CLEANUP.as_u32())
    }

    fn stage(
        &mut self,
        tx: &mut PartitionStoreTransaction<'_>,
        _db: &PartitionDb,
        _raw_key: &[u8],
        key: &Self::Entry,
    ) -> Result<Staged, StorageError> {
        let mut read_bytes = 0;
        let orphan = match self.invocation {
            Some((pk, uuid, orphan)) if pk == key.partition_key && uuid == key.invocation_uuid => {
                orphan
            }
            _ => {
                let (exists, bytes) = tx.lookup_for_walk(JournalKey {
                    partition_key: key.partition_key,
                    invocation_uuid: key.invocation_uuid,
                    journal_index: 0,
                })?;
                read_bytes = bytes;
                let orphan = !exists;
                self.invocation = Some((key.partition_key, key.invocation_uuid, orphan));
                orphan
            }
        };
        if orphan {
            tx.delete_key(key)?;
        }
        Ok(Staged {
            filled: u64::from(orphan),
            uncovered: 0,
            read_bytes,
        })
    }

    fn finish(
        &self,
        tx: &mut PartitionStoreTransaction<'_>,
        db: &PartitionDb,
        _registry: &mut IndexRegistry,
        settled: &[(u32, Settlement)],
        _at: u64,
    ) -> Result<Option<crate::features::StorageFeatures>, StorageError> {
        if !matches!(settled, [(_, Settlement::Certified)]) {
            return Err(StorageError::DataIntegrityError);
        }
        crate::features::jc_orphan_cleanup::stage_completion(tx, db).map(Some)
    }
}

/// Reconstructs diagnostics from the walk, or starts a single sweep if needed.
/// Empty sources need no job; reopening them costs one last-key seek.
pub(crate) fn reconcile(
    db: &PartitionDb,
    done: bool,
    walks: &mut Walks,
    jobs: &mut MigrationJobRecords,
    stamp: &Stamp,
    history_limit: usize,
) -> Result<bool, StorageError> {
    let id = MigrationJobId::JC_ORPHAN_CLEANUP;
    if let Some(record) = jobs.unfinished.get_mut(id)
        && (record.kind != crate::job::MigrationJobKind::Cleanup
            || record.name != "jc_orphan_cleanup/1")
    {
        // Diagnostics may be stale or partly unknown; identity comes from the
        // definition, never from the record.
        *record = MigrationJob::jc_orphan_cleanup(None, None);
    }
    if done {
        let redundant: Vec<_> = walks
            .entries()
            .iter()
            .filter(|walk| walk.is_jc_orphan_cleanup())
            .map(|walk| walk.id)
            .collect();
        for walk in redundant {
            walks.remove(walk).map_err(walk_error)?;
        }
        complete_record(jobs, history_limit);
        return Ok(true);
    }
    let mut existing = walks
        .entries()
        .iter()
        .filter(|walk| walk.is_jc_orphan_cleanup());
    let existing = match (existing.next(), existing.next()) {
        (Some(walk), None) => Some((walk.id, walk.cursor.is_some())),
        (None, None) => None,
        _ => return Err(StorageError::DataIntegrityError),
    };
    let (walk, started, new) = if let Some((id, started)) = existing {
        (id, started, false)
    } else {
        let Some(end) = engine::last_source_key(db, Source::JournalCompletionId)? else {
            let unfinished = jobs.unfinished.get(id).is_some();
            complete_record(jobs, history_limit);
            return Ok(unfinished);
        };
        (
            walks
                .start(
                    WalkKind::jc_orphan_cleanup(),
                    Source::JournalCompletionId,
                    end,
                )
                .map_err(walk_error)?,
            false,
            true,
        )
    };
    if new || jobs.unfinished.get(id).is_none() {
        jobs.unfinished.insert(MigrationJob::jc_orphan_cleanup(
            Some(walk),
            new.then_some(stamp),
        ));
    }
    let record = jobs.unfinished.get_mut(id).expect("inserted above");
    if record.walk != Some(walk) {
        record.pass_started_at = None;
    }
    record.walk = Some(walk);
    record.status = if started {
        MigrationJobStatus::Running
    } else {
        MigrationJobStatus::Pending
    };
    Ok(false)
}

fn complete_record(jobs: &mut MigrationJobRecords, history_limit: usize) {
    let id = MigrationJobId::JC_ORPHAN_CLEANUP;
    if jobs.history.get(id).is_some() {
        jobs.unfinished.remove(id);
    } else {
        jobs.finish(id, MigrationJobStatus::Completed, None, history_limit);
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;

    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_types::config::Configuration;
    use restate_types::identifiers::{InvocationUuid, PartitionId};
    use restate_types::partition_table::Partition;
    use restate_types::sharding::KeyRange;

    use crate::fsm_table::{fsm_variable, put_migration_jobs};
    use crate::job::MigrationJobs;
    use crate::journal_table_v2::JournalKey;
    use crate::{PartitionStore, PartitionStoreManager, StorageRead};

    use super::*;

    fn key(pk: u64, completion_id: u32) -> JournalCompletionIdToCommandIndexKey {
        JournalCompletionIdToCommandIndexKey {
            partition_key: pk,
            invocation_uuid: InvocationUuid::from_u128(1),
            completion_id,
        }
    }

    fn sentinel(pk: u64) -> JournalKey {
        JournalKey {
            partition_key: pk,
            invocation_uuid: InvocationUuid::from_u128(1),
            journal_index: 0,
        }
    }

    async fn open_jobs(store: &mut PartitionStore) {
        store
            .verify_and_run_migrations(CancellationToken::new(), &Configuration::default())
            .await
            .unwrap();
    }

    fn exists(store: &mut PartitionStore, pk: u64, id: u32) -> bool {
        store
            .get_kv_raw(key(pk, id), |_, value| Ok(value.is_some()))
            .unwrap()
    }

    #[restate_core::test]
    async fn cleanup_resumes_rechecks_ownership_and_finishes_atomically() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        {
            let mut tx = store.transaction();
            // One invocation spans slices. Another is live; the last is orphaned.
            for (pk, count) in [(10, 3), (20, 1), (40, 1)] {
                for id in 0..count {
                    tx.put_kv_raw(key(pk, id), []).unwrap();
                }
            }
            tx.put_kv_raw(sentinel(20), []).unwrap();
            tx.commit().await.unwrap();
        }
        open_jobs(&mut store).await;
        let observer = store.clone();
        assert!(!observer.storage_features().is_jc_orphan_cleanup_v1);
        let db = store.partition_db().clone();
        let walk = store.maintained_indexes().walks().entries()[0].id;
        assert!(store.maintained_indexes().registry().entries().is_empty());
        {
            let mut tx = store.transaction();
            assert_eq!(
                engine::run_slice(&mut tx, &db, walk, 1).await.unwrap(),
                engine::Slice::Progressed
            );
        }
        assert!(!exists(&mut store, 10, 0));
        assert!(exists(&mut store, 10, 1));
        assert!(!store.storage_features().is_jc_orphan_cleanup_v1);
        let cursor = store
            .maintained_indexes()
            .walks()
            .get(walk)
            .unwrap()
            .cursor
            .clone();

        // Foreground work changes ownership between slices. Also lose diagnostics:
        // neither the old orphan decision nor a missing job record is authoritative.
        {
            let mut tx = store.transaction();
            tx.put_kv_raw(sentinel(10), []).unwrap();
            put_migration_jobs(
                &mut tx,
                partition.id(),
                fsm_variable::MIGRATION_JOBS,
                &MigrationJobs::default(),
            )
            .unwrap();
            tx.commit().await.unwrap();
        }
        db.flush_memtables(true).await.unwrap();
        drop(store);
        manager.close(partition.id()).await;
        let mut store = manager.open(&partition, None).await.unwrap();
        open_jobs(&mut store).await;
        assert_eq!(
            store.maintained_indexes().walks().get(walk).unwrap().cursor,
            cursor
        );
        let id = MigrationJobId::JC_ORPHAN_CLEANUP;
        assert_eq!(
            store
                .maintained_indexes()
                .jobs()
                .unfinished
                .get(id)
                .unwrap()
                .status,
            MigrationJobStatus::Running
        );
        engine::run_all(&mut store, 1).await.unwrap();
        assert!(exists(&mut store, 10, 1) && exists(&mut store, 10, 2));
        assert!(exists(&mut store, 20, 0));
        assert!(!exists(&mut store, 40, 0));
        assert!(store.storage_features().is_jc_orphan_cleanup_v1);
        assert!(observer.storage_features().is_jc_orphan_cleanup_v1);
        assert!(
            observer
                .get_storage_features_names()
                .iter()
                .any(|name| *name == "jc-orphan-cleanup-v1")
        );
        assert!(
            crate::fsm_table::get_storage_features_from_partition_db(store.partition_db())
                .unwrap()
                .unwrap()
                .names()
                .any(|name| *name == "jc-orphan-cleanup-v1")
        );
        assert!(!crate::fsm_table::is_jc_orphan_cleanup_done(&mut store, partition.id()).unwrap());
        let active = store.maintained_indexes();
        assert!(active.walks().entries().is_empty());
        let job = active.jobs().history.get(id).unwrap();
        assert_eq!(job.status, MigrationJobStatus::Completed);
        assert_eq!((job.rows_examined, job.rows_filled), (4, 1));
        let mut stale = MigrationJobs::default();
        let mut record = MigrationJob::jc_orphan_cleanup(Some(walk), None);
        record.kind = crate::job::MigrationJobKind::IndexBackfill;
        record.rows_examined = 99;
        record.name = restate_util_string::ReString::from_static("stale");
        stale.insert(record);

        // History retention cannot restart a completed migration.
        {
            let mut tx = store.transaction();
            put_migration_jobs(
                &mut tx,
                partition.id(),
                fsm_variable::MIGRATION_JOB_HISTORY,
                &MigrationJobs::default(),
            )
            .unwrap();
            tx.commit().await.unwrap();
        }
        open_jobs(&mut store).await;
        assert!(store.maintained_indexes().walks().entries().is_empty());
        assert!(
            store
                .maintained_indexes()
                .jobs()
                .history
                .entries()
                .is_empty()
        );
        // A completed marker also repairs stale unfinished diagnostics, rather
        // than leaving a permanently pending job or treating it as an index.
        {
            let mut tx = store.transaction();
            put_migration_jobs(
                &mut tx,
                partition.id(),
                fsm_variable::MIGRATION_JOBS,
                &stale,
            )
            .unwrap();
            tx.commit().await.unwrap();
        }
        open_jobs(&mut store).await;
        let active = store.maintained_indexes();
        assert!(active.jobs().unfinished.entries().is_empty());
        let repaired = active.jobs().history.get(id).unwrap();
        assert_eq!(repaired.status, MigrationJobStatus::Completed);
        assert_eq!(repaired.kind, crate::job::MigrationJobKind::Cleanup);
        assert_eq!(repaired.name, "jc_orphan_cleanup/1");
        assert_eq!(repaired.finished_at, None);
        assert_eq!(repaired.rows_examined, 0);

        // No source and a stale job without a walk: certify the empty sweep,
        // restore completion evidence, and don't duplicate retained history.
        {
            let mut tx = store.transaction();
            for (pk, id) in [(10, 1), (10, 2), (20, 0)] {
                tx.delete_key(&key(pk, id)).unwrap();
            }
            crate::fsm_table::delete_storage_features(&mut tx, partition.id())
                .await
                .unwrap();
            put_migration_jobs(
                &mut tx,
                partition.id(),
                fsm_variable::MIGRATION_JOBS,
                &stale,
            )
            .unwrap();
            tx.commit().await.unwrap();
        }
        open_jobs(&mut store).await;
        assert!(store.storage_features().is_jc_orphan_cleanup_v1);
        let active = store.maintained_indexes();
        assert!(active.jobs().unfinished.entries().is_empty());
        assert_eq!(active.jobs().history.entries().len(), 1);
    }

    #[restate_core::test]
    async fn cleanup_rolls_back_failed_slices_and_preserves_live_foreground_writes() {
        use crate::keys::{EncodeTableKeyPrefix, KeyKind};

        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        // Fail decoding after staging the first deletion, with a valid endpoint.
        let mut malformed = key(30, 0).serialize().to_vec();
        malformed.pop();
        {
            let mut tx = store.transaction();
            tx.put_kv_raw(key(10, 0), []).unwrap();
            tx.put_kv_raw(key(40, 0), []).unwrap();
            tx.raw_put_cf(KeyKind::JournalV2CompletionIdToCommandIndex, &malformed, []);
            tx.commit().await.unwrap();
        }
        open_jobs(&mut store).await;
        let db = store.partition_db().clone();
        let walk = store.maintained_indexes().walks().entries()[0].id;
        {
            let mut tx = store.transaction();
            assert!(matches!(
                engine::run_slice(&mut tx, &db, walk, 100).await,
                Err(engine::SliceError::Staging(_))
            ));
            assert!(tx.is_empty());
            engine::record_failure(
                &mut tx,
                &db,
                walk,
                "malformed key",
                std::time::Duration::ZERO,
            )
            .await
            .unwrap();
        }
        assert!(exists(&mut store, 10, 0));
        let active = store.maintained_indexes();
        assert!(active.walks().get(walk).unwrap().cursor.is_none());
        let job = active
            .jobs()
            .unfinished
            .get(MigrationJobId::JC_ORPHAN_CLEANUP)
            .unwrap();
        assert_eq!((job.rows_examined, job.slices, job.failures), (0, 0, 1));
        // Repair and create live indexes on both sides of the captured endpoint.
        {
            let mut tx = store.transaction();
            tx.delete_cf(crate::TableKind::Journal, &malformed).unwrap();
            tx.put_kv_raw(sentinel(1), []).unwrap();
            tx.put_kv_raw(key(1, 0), []).unwrap();
            for pk in [5, 50] {
                tx.put_kv_raw(sentinel(pk), [0; 1024]).unwrap();
                tx.put_kv_raw(key(pk, 0), []).unwrap();
            }
            // Ownership reads must also see uncommitted journal creation.
            let (exists, bytes) = tx.lookup_for_walk(sentinel(5)).unwrap();
            assert!(exists);
            assert!(bytes > 1024);
            tx.commit().await.unwrap();
        }
        // A candidate's ownership payload exceeds the remaining budget: keep
        // the cursor before it, then take it alone as the next slice's first row.
        for pk in [1, 5] {
            let mut tx = store.transaction();
            assert_eq!(
                engine::run_slice(
                    &mut tx,
                    &db,
                    walk,
                    engine::SliceBudget {
                        keys: 100,
                        read_bytes: 128,
                        write_bytes: usize::MAX,
                    }
                )
                .await
                .unwrap(),
                engine::Slice::Progressed
            );
            assert_eq!(
                tx.maintained_indexes()
                    .walks()
                    .get(walk)
                    .unwrap()
                    .cursor
                    .as_deref(),
                Some(key(pk, 0).serialize().as_ref())
            );
        }
        assert!(exists(&mut store, 10, 0) && exists(&mut store, 40, 0));
        // A tiny write budget forces completion into its own slice. The driver
        // reports feature publication, and refreshes the long-lived transaction.
        let mut driver = store.walk_driver(&restate_types::config::StorageOptions::default());
        {
            let mut tx = store.transaction();
            for _ in 0..2 {
                driver.ready().await;
                assert!(!driver.step(&mut tx, 1).await.unwrap());
            }
            let active = tx.maintained_indexes();
            let pending = active.walks().get(walk).unwrap();
            assert_eq!(pending.cursor.as_ref(), Some(&pending.end));
            assert!(!tx.storage_features().is_jc_orphan_cleanup_v1);
            assert!(!db.storage_features().unwrap().is_jc_orphan_cleanup_v1);
            assert!(!tx.lookup_for_walk(key(10, 0)).unwrap().0);
            assert!(!tx.lookup_for_walk(key(40, 0)).unwrap().0);
            assert!(
                !crate::fsm_table::get_storage_features_from_partition_db(&db)
                    .unwrap()
                    .unwrap()
                    .names()
                    .any(|name| *name == "jc-orphan-cleanup-v1")
            );
            assert!(
                active
                    .jobs()
                    .history
                    .get(MigrationJobId::JC_ORPHAN_CLEANUP)
                    .is_none()
            );
            driver.ready().await;
            assert!(driver.step(&mut tx, 1).await.unwrap());
            assert!(tx.maintained_indexes().walks().entries().is_empty());
            assert!(tx.storage_features().is_jc_orphan_cleanup_v1);
        }
        assert!(!exists(&mut store, 10, 0) && !exists(&mut store, 40, 0));
        assert!(exists(&mut store, 5, 0) && exists(&mut store, 50, 0));
        assert!(store.storage_features().is_jc_orphan_cleanup_v1);
        let active = store.maintained_indexes();
        let job = active
            .jobs()
            .history
            .get(MigrationJobId::JC_ORPHAN_CLEANUP)
            .unwrap();
        assert_eq!(
            (job.rows_filled, job.failures, job.consecutive_failures),
            (2, 1, 0)
        );
    }
}
