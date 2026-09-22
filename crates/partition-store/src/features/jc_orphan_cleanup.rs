// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, Instant};

use anyhow::Context;
use bytes::{Bytes, BytesMut};
use rocksdb::{ReadOptions, WriteBatch, WriteOptions};
use tokio_util::sync::CancellationToken;
use tracing::info;

use restate_rocksdb::{Priority, StorageTaskKind};
use restate_storage_api::StorageError;
use restate_types::config::Configuration;
use restate_types::{RESTATE_VERSION_1_7_10, RESTATE_VERSION_1_9_0, SemanticRestateVersion};
use restate_util_string::ReString;
use restate_util_time::DurationExt;

use super::{JcOrphanCleanupFeature, StorageFeature, StorageFeatures};
use crate::fsm_table::append_jc_orphan_cleanup_marker_deletion_to_wb;
use crate::journal_table_v2::{JournalCompletionIdToCommandIndexKey, JournalKey};
use crate::keys::{DecodeTableKey, EncodeTableKey, EncodeTableKeyPrefix};
use crate::scan::TableScan;
use crate::{MigrationError, PartitionStore, convert_to_upper_bound};

static JC_ORPHAN_CLEANUP: ReString = ReString::from_static("jc-orphan-cleanup");

/// A chunk ends once its deletion batch reaches this size or once this many invocations
/// were scanned, whichever comes first. The scan bound keeps orphan-sparse chunks from
/// occupying a storage-pool thread for too long.
#[cfg(not(test))]
const MAX_BATCH_SIZE_BYTES: usize = 1024 * 1024;
/// An empty `WriteBatch` already holds a 12-byte header; any range delete exceeds this.
#[cfg(test)]
const MAX_BATCH_SIZE_BYTES: usize = 13;
#[cfg(not(test))]
const MAX_INVOCATIONS_PER_CHUNK: usize = 32_768;
#[cfg(test)]
const MAX_INVOCATIONS_PER_CHUNK: usize = 3;
const ENTRIES_BEFORE_SEEK: usize = 16;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(60);

impl StorageFeature for JcOrphanCleanupFeature {
    const RUN_ON_EMPTY_STORE: bool = true;

    fn persisted_name() -> &'static ReString {
        &JC_ORPHAN_CLEANUP
    }

    fn min_required_version() -> &'static SemanticRestateVersion {
        &RESTATE_VERSION_1_7_10
    }

    fn should_enable(
        config: &Configuration,
        current_version: &SemanticRestateVersion,
        _is_store_empty: bool,
    ) -> bool {
        current_version.is_equal_or_newer_than(&RESTATE_VERSION_1_9_0)
            || config.common.experimental.is_jc_orphan_cleanup_enabled()
    }

    fn is_enabled(features: &StorageFeatures) -> bool {
        features.is_jc_orphan_cleanup
    }

    fn set_enabled(features: &mut StorageFeatures) {
        features.is_jc_orphan_cleanup = true;
    }

    async fn enable(
        storage: &mut PartitionStore,
        cancel: &CancellationToken,
        _config: &Configuration,
        finalization: &mut WriteBatch,
    ) -> Result<(), MigrationError> {
        let started = Instant::now();
        let mut last_reported = Instant::now();
        let mut scanned_invocations = 0;
        let mut deleted_invocations = 0;
        let mut next_key = None;
        let rocks = storage.partition_db().rocksdb().clone();

        info!("Starting cleanup of orphaned journal completion-id index entries");
        loop {
            // Await every chunk even during shutdown: dropping the receiver does not stop a
            // storage-pool operation. No processor may start writing until cleanup has stopped.
            let chunk = rocks
                .clone()
                .run_background_read_op(
                    "jc-orphan-cleanup",
                    StorageTaskKind::BackgroundIterator,
                    Priority::Low,
                    {
                        let mut storage = storage.clone();
                        let cancel = cancel.clone();
                        move |_| cleanup_chunk(&mut storage, next_key, || cancel.is_cancelled())
                    },
                )
                .await
                .map_err(|_| StorageError::OperationalError)??;

            scanned_invocations += chunk.scanned_invocations;
            deleted_invocations += chunk.deleted_invocations;
            if last_reported.elapsed() >= PROGRESS_INTERVAL {
                info!(
                    scanned_invocations,
                    deleted_invocations,
                    elapsed = %started.elapsed().friendly(),
                    "Cleaning up orphaned journal completion-id index entries"
                );
                last_reported = Instant::now();
            }
            let Some(key) = chunk.next_key else {
                break;
            };
            next_key = Some(key);
        }
        info!(
            scanned_invocations,
            deleted_invocations,
            elapsed = %started.elapsed().friendly(),
            "Finished scanning orphaned journal completion-id index entries"
        );
        append_jc_orphan_cleanup_marker_deletion_to_wb(
            storage.partition_db().cf_handle(),
            finalization,
            storage.partition_id(),
        );
        Ok(())
    }
}

struct CleanupChunk {
    next_key: Option<Bytes>,
    scanned_invocations: usize,
    deleted_invocations: usize,
}

/// Runs only before processor construction. With no concurrent journal initialization, checking
/// the ownership sentinel j2[0] and deleting the invocation's jc range. Deletions are idempotent;
/// interrupted sweeps restart from the beginning without a feature marker.
fn cleanup_chunk(
    storage: &mut PartitionStore,
    start_key: Option<Bytes>,
    is_cancelled: impl Fn() -> bool,
) -> Result<CleanupChunk, MigrationError> {
    let partition_db = storage.partition_db().clone();
    let cf = partition_db.cf_handle();
    let mut arena = BytesMut::new();
    // A one-time sweep must not evict hot blocks from the shared block cache.
    let mut options = ReadOptions::default();
    options.fill_cache(false);
    let mut get_options = ReadOptions::default();
    get_options.fill_cache(false);
    let mut iter = partition_db.scan(
        TableScan::ScanPartitionKeyRange::<JournalCompletionIdToCommandIndexKey>(
            storage.partition_key_range(),
        )
        .encode(&mut arena),
        options,
    )?;
    if let Some(start_key) = start_key {
        iter.seek(start_key);
    }

    let mut batch = WriteBatch::default();
    let mut prefix = BytesMut::new();
    let mut upper_bound = BytesMut::new();
    let mut journal_key = BytesMut::new();
    let mut scanned_invocations = 0;
    let mut deleted_invocations = 0;

    assert!(
        batch.size_in_bytes() < MAX_BATCH_SIZE_BYTES,
        "empty write batch must be smaller than max batch size"
    );

    while iter.valid()
        && scanned_invocations < MAX_INVOCATIONS_PER_CHUNK
        && batch.size_in_bytes() < MAX_BATCH_SIZE_BYTES
    {
        if is_cancelled() {
            return Err(MigrationError::MigrationCancelled);
        }
        let (mut key, _) = iter.item().expect("valid iterator has an item");
        let key = JournalCompletionIdToCommandIndexKey::deserialize_from(&mut key)?;
        prefix.clear();
        JournalCompletionIdToCommandIndexKey::builder()
            .partition_key(key.partition_key)
            .invocation_uuid(key.invocation_uuid)
            .serialize_to(&mut prefix);
        upper_bound.clear();
        upper_bound.extend_from_slice(&prefix);
        assert!(convert_to_upper_bound(&mut upper_bound));

        journal_key.clear();
        EncodeTableKey::serialize_to(
            &JournalKey {
                partition_key: key.partition_key,
                invocation_uuid: key.invocation_uuid,
                journal_index: 0,
            },
            &mut journal_key,
        );
        let has_journal = partition_db
            .rocksdb()
            .inner()
            .as_raw_db()
            .get_pinned_cf_opt(cf, &journal_key, &get_options)
            .context("reading journal input entry")
            .map_err(StorageError::Generic)?
            .is_some();
        if !has_journal {
            batch.delete_range_cf(cf, &prefix, &upper_bound);
            deleted_invocations += 1;
        }
        scanned_invocations += 1;

        // Advancing within a block is cheaper than a total-order seek for short invocations.
        // Seek past long invocations.
        for step in 0..ENTRIES_BEFORE_SEEK {
            iter.next();
            match iter.item() {
                Some((key, _)) if key.starts_with(&prefix) => {
                    if step + 1 == ENTRIES_BEFORE_SEEK {
                        iter.seek(&upper_bound);
                    }
                }
                _ => break,
            }
        }
    }
    iter.status()
        .context("iterating over journal completion-id index")
        .map_err(StorageError::Generic)?;
    let next_key = iter.valid().then(|| upper_bound.freeze());
    drop(iter);

    if is_cancelled() {
        return Err(MigrationError::MigrationCancelled);
    }
    if !batch.is_empty() {
        let mut options = WriteOptions::default();
        // Match feature finalization's WAL policy. FIFO memtable flush order must keep the
        // completed feature behind all these deletes, including after a crash.
        options.disable_wal(true);
        partition_db
            .rocksdb()
            .inner()
            .write_batch(&batch, &options)
            .context("deleting orphaned journal completion-id index entries")
            .map_err(StorageError::Generic)?;
    }
    Ok(CleanupChunk {
        next_key,
        scanned_invocations,
        deleted_invocations,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::fsm_table::WriteFsmTable;
    use restate_types::RESTATE_VERSION_1_8_0;
    use restate_types::identifiers::{InvocationUuid, PartitionId};
    use restate_types::logs::Lsn;
    use restate_types::partitions::{Partition, StorageVersion};
    use restate_types::sharding::KeyRange;

    use super::*;
    use crate::features::{KnownStorageFeature, LoadedStorageFeatures};
    use crate::fsm_table::{
        PartitionStateMachineKey, fsm_variable, get_min_restate_version_from_partition_db,
        get_storage_features_from_partition_db, put_min_restate_version, put_storage_version,
    };
    use crate::keys::EncodeTableKey;
    use crate::{PartitionStoreManager, StorageAccess};

    fn legacy_marker(partition_id: PartitionId) -> PartitionStateMachineKey {
        PartitionStateMachineKey {
            partition_id: partition_id.into(),
            state_id: fsm_variable::JC_ORPHAN_CLEANUP_DONE,
        }
    }

    fn contains(store: &mut PartitionStore, key: impl EncodeTableKey) -> bool {
        store
            .get_kv_raw(key, |_, value| Ok(value.is_some()))
            .unwrap()
    }

    fn index_key(pk: u64, uuid: u128, completion_id: u32) -> JournalCompletionIdToCommandIndexKey {
        JournalCompletionIdToCommandIndexKey {
            partition_key: pk,
            invocation_uuid: InvocationUuid::from_u128(uuid),
            completion_id,
        }
    }

    #[restate_core::test]
    async fn interrupted_cleanup_retries_and_replaces_legacy_marker() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::MIN, KeyRange::new(10, 20));
        let mut store = manager.open(&partition, None).await.unwrap();
        let partition_id = store.partition_id();
        // Include both key-range boundaries, adjacent invocation IDs, a UUID whose successor
        // carries into the partition key, and wide live/orphan indexes exercising the seek path.
        let invocations = [
            (9, 1, false),
            (10, 1, true),
            (10, 2, false),
            (11, 1, true),
            (12, 1, false),
            (20, u128::MAX, false),
            (21, 1, false),
        ];
        for (pk, uuid, live) in invocations {
            for completion in 0..32 {
                store
                    .put_kv_raw(index_key(pk, uuid, completion), [0; 4])
                    .unwrap();
            }
            if live {
                // Only the existence of the input entry matters to ownership detection.
                store
                    .put_kv_raw(
                        JournalKey {
                            partition_key: pk,
                            invocation_uuid: InvocationUuid::from_u128(uuid),
                            journal_index: 0,
                        },
                        b"input",
                    )
                    .unwrap();
            }
        }
        store
            .put_kv_raw(legacy_marker(partition_id), b"legacy")
            .unwrap();
        put_storage_version(&mut store, partition_id, StorageVersion::V1_5 as u16)
            .await
            .unwrap();
        let mut txn = store.transaction();
        txn.put_applied_lsn(Lsn::from(1)).unwrap();
        txn.commit().await.unwrap();
        drop(txn);

        let mut config = Configuration::default();
        store
            .verify_and_run_migrations_at_version(
                &RESTATE_VERSION_1_8_0,
                CancellationToken::new(),
                &config,
            )
            .await
            .unwrap();
        assert!(!store.storage_features().is_jc_orphan_cleanup);
        assert!(contains(&mut store, index_key(10, 2, 0)));

        // Cancelling within a chunk discards the uncommitted work.
        let checks = Cell::new(0);
        assert!(matches!(
            cleanup_chunk(&mut store, None, || {
                let check = checks.get();
                checks.set(check + 1);
                check == 1
            }),
            Err(MigrationError::MigrationCancelled)
        ));
        assert!(contains(&mut store, index_key(10, 2, 0)));

        // Persist one chunk without finalizing the feature, then interrupt migration.
        let chunk = cleanup_chunk(&mut store, None, || false).unwrap();
        assert_eq!(chunk.scanned_invocations, 2);
        assert_eq!(chunk.deleted_invocations, 1);
        assert!(chunk.next_key.is_some());
        assert!(!contains(&mut store, index_key(10, 2, 0)));
        config.common.experimental.set_jc_orphan_cleanup(true);
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            store
                .verify_and_run_migrations_at_version(&RESTATE_VERSION_1_8_0, cancel, &config,)
                .await,
            Err(MigrationError::MigrationCancelled)
        ));
        assert!(!store.storage_features().is_jc_orphan_cleanup);
        assert!(contains(&mut store, legacy_marker(partition_id)));
        store.partition_db().flush_memtables(true).await.unwrap();
        drop(store);
        manager.close_partition_store(partition_id).await;

        let mut store = manager.open(&partition, None).await.unwrap();
        // Feature finalization itself must retire the marker, without relying on
        // post-migration housekeeping in PartitionStore.
        let mut storage_version = StorageVersion::V1_5;
        let mut min_version =
            get_min_restate_version_from_partition_db(store.partition_db()).unwrap();
        let mut features = LoadedStorageFeatures::load(
            get_storage_features_from_partition_db(store.partition_db()).unwrap(),
            storage_version,
            &RESTATE_VERSION_1_8_0,
        )
        .unwrap();
        KnownStorageFeature::JcOrphanCleanup
            .enable(
                &mut store,
                &RESTATE_VERSION_1_8_0,
                &mut min_version,
                &mut storage_version,
                false,
                &CancellationToken::new(),
                &config,
                &mut features,
            )
            .await
            .unwrap();
        assert!(features.enabled().is_jc_orphan_cleanup);
        assert!(!contains(&mut store, legacy_marker(partition_id)));
        for (pk, uuid, live) in invocations {
            for completion in 0..32 {
                assert_eq!(
                    contains(&mut store, index_key(pk, uuid, completion)),
                    live || !(10..=20).contains(&pk)
                );
            }
        }
        assert_eq!(
            get_min_restate_version_from_partition_db(store.partition_db()).unwrap(),
            *RESTATE_VERSION_1_7_10
        );
        RocksDbManager::get().shutdown().await;
    }

    #[restate_core::test]
    async fn automatic_cleanup_handles_empty_stores_and_preserves_higher_barrier() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let partition = Partition::new(PartitionId::MIN, KeyRange::new(10, 20));
        let mut store = manager.open(&partition, None).await.unwrap();
        let partition_id = store.partition_id();
        store
            .put_kv_raw(legacy_marker(partition_id), b"legacy")
            .unwrap();
        let v1_8 = SemanticRestateVersion::new(1, 8, 0);
        put_min_restate_version(&mut store, partition_id, &v1_8)
            .await
            .unwrap();
        let config = Configuration::default();

        store
            .verify_and_run_migrations_at_version(
                &RESTATE_VERSION_1_9_0,
                CancellationToken::new(),
                &config,
            )
            .await
            .unwrap();
        assert!(store.storage_features().is_jc_orphan_cleanup);
        assert!(!contains(&mut store, legacy_marker(partition_id)));
        assert_eq!(
            get_min_restate_version_from_partition_db(store.partition_db()).unwrap(),
            v1_8
        );
        RocksDbManager::get().shutdown().await;
    }
}
