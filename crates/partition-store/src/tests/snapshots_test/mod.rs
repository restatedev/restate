// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use url::Url;

use restate_core::{Metadata, TestCoreEnv};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::{FsmPosition, ReadFsmTable, WriteFsmTable};
use restate_test_util::tempdir;
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::partitions::Partition;
use restate_types::partitions::features::PersistedFeatures;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, SemanticRestateVersion};

use crate::fsm_table::{put_fsm_position, put_storage_features_json};
use crate::migrations::MigrationError;
use crate::partition_store::StorageAccess;
use crate::snapshots::{
    LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotDir, SnapshotFormatVersion,
    SnapshotLeaseGuard, SnapshotRepository,
};
use crate::{PartitionStore, PartitionStoreManager};

pub(crate) async fn run_tests(
    manager: Arc<PartitionStoreManager>,
    mut partition_store: PartitionStore,
) {
    insert_test_data(&mut partition_store).await;

    let snapshots_dir = tempdir().unwrap();

    let partition_id = partition_store.partition_id();

    let snapshot = partition_store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await
        .unwrap();

    let key_range = partition_store.partition_key_range();
    let snapshot_meta = PartitionSnapshotMetadata {
        version: SnapshotFormatVersion::V1,
        cluster_name: "cluster_name".to_string(),
        cluster_fingerprint: None,
        partition_id,
        node_name: "node".to_string(),
        node_id: Some(GenerationalNodeId::new(1, 1)),
        created_at: MillisSinceEpoch::new(0).into_timestamp(),
        snapshot_id: SnapshotId::from_parts(0, 0),
        key_range,
        log_id: LogId::from(partition_id),
        min_applied_lsn: snapshot.min_applied_lsn,
        db_comparator_name: snapshot.db_comparator_name.clone(),
        compatibility: snapshot.compatibility.clone(),
        byte_offsets_base_lsn: snapshot.byte_offsets_base_lsn,
        log_byte_offset: snapshot.log_byte_offset,
        partition_store_byte_offset: snapshot.partition_store_byte_offset,
        files: snapshot.files.clone(),
        file_keys: std::collections::BTreeMap::new(),
    };
    assert!(snapshot.compatibility.is_some());
    assert_eq!(
        (
            snapshot.byte_offsets_base_lsn,
            snapshot.log_byte_offset,
            snapshot.partition_store_byte_offset
        ),
        (
            Some(POSITION.byte_offsets_base_lsn),
            Some(POSITION.log_byte_offset),
            Some(POSITION.partition_store_byte_offset)
        )
    );
    let metadata_json = serde_json::to_string_pretty(&snapshot_meta).unwrap();

    drop(partition_store);
    // Disarm the exported snapshot's directory guard so the on-disk SST files survive past this
    // drop; we restore from them below. `snapshots_dir` (a TempDir) still owns the parent dir.
    let _exported_dir = snapshot.base_dir.into_path();

    manager
        .drop_partition_store(&Partition::new(partition_id, key_range))
        .await
        .unwrap();

    let snapshot_meta: PartitionSnapshotMetadata = serde_json::from_str(&metadata_json).unwrap();
    assert_eq!(snapshot_meta.compatibility, snapshot.compatibility);
    assert_eq!(snapshot_meta.node_id, Some(GenerationalNodeId::new(1, 1)));
    assert_eq!(snapshot_meta.log_byte_offset, snapshot.log_byte_offset);

    let snapshot = LocalPartitionSnapshot {
        base_dir: SnapshotDir::new(snapshots_dir.path().into()),
        log_id: LogId::from(partition_id),
        min_applied_lsn: snapshot_meta.min_applied_lsn,
        db_comparator_name: snapshot_meta.db_comparator_name.clone(),
        files: snapshot_meta.files.clone(),
        key_range,
        compatibility: snapshot_meta.compatibility.clone(),
        byte_offsets_base_lsn: snapshot_meta.byte_offsets_base_lsn,
        log_byte_offset: snapshot_meta.log_byte_offset,
        partition_store_byte_offset: snapshot_meta.partition_store_byte_offset,
    };

    let mut new_partition_store = manager
        .open_from_snapshot(
            &Partition::new(partition_id, KeyRange::new(0, PartitionKey::MAX - 1)),
            snapshot,
        )
        .await
        .unwrap();

    verify_restored_data(&mut new_partition_store).await;
}

/// The position of the test data.
const POSITION: FsmPosition = FsmPosition {
    applied_lsn: Lsn::new(100),
    byte_offsets_base_lsn: Lsn::new(50),
    log_byte_offset: 1000,
    partition_store_byte_offset: 2000,
};

async fn insert_test_data(partition: &mut PartitionStore) {
    let mut txn = partition.transaction();
    txn.put_applied_lsn(Lsn::new(100)).unwrap();
    put_fsm_position(&mut txn, &POSITION).unwrap();
    txn.commit().await.expect("commit succeeds");
}

async fn verify_restored_data(partition: &mut PartitionStore) {
    assert_eq!(
        Lsn::new(100),
        partition.get_applied_lsn().await.unwrap().unwrap()
    );
    assert_eq!(partition.get_fsm_position().await.unwrap(), Some(POSITION));
}

/// The service gauges' total, as a query snapshot reads it.
fn registered_totals(store: &PartitionStore) -> u64 {
    use std::ops::ControlFlow;

    use restate_storage_query_api::scan::BlockingScan;

    use crate::keys::selection::KeySelection;
    use crate::stats::StatValueCodec;
    use crate::stats::aggregated::{ServiceStats, StageStatusCounts};

    let (snapshot, _) = store.query_read(CancellationToken::new()).unwrap();
    let mut counted = 0;
    <crate::ReadSnapshot as BlockingScan<ServiceStats>>::scan_blocking(
        &snapshot,
        Arc::new(KeySelection::all()),
        |(_, payload)| {
            let counts = StageStatusCounts::deserialize_from(payload).unwrap();
            counted += counts.iter().map(|(_, count)| count).sum::<u64>();
            ControlFlow::Continue(())
        },
    )
    .unwrap();
    counted
}

/// A replica restored from a snapshot of backfilled gauges opens with
/// the same Ready indexes, under the source replica's IDs, and keeps maintaining
/// them, even with the options removed. The registry and walks also preserve the
/// existing byte-offset position through the backfill and the restore.
#[restate_core::test]
async fn restored_snapshots_keep_ready_registered_instances() {
    use restate_storage_api::vqueue_table::{
        EntryContext, ReadVQueueTable, StoredEntryRef, WriteVQueueTable,
    };
    use restate_types::identifiers::{BaseEntryId, WithPartitionKey};
    use restate_types::vqueues::{EntryTargetExt, VQueueId};

    use crate::fsm_table::{fsm_variable, get_registry_from_partition_db};

    assert_eq!(fsm_variable::FSM_POSITION, 13);
    assert_eq!(fsm_variable::INDEX_REGISTRY, 14);
    assert_eq!(fsm_variable::WALKS, 15);
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let mut store = manager.open(&partition, None).await.unwrap();
    store
        .verify_and_run_migrations(CancellationToken::new(), &Configuration::default())
        .await
        .unwrap();
    let (last_id, last_status) = crate::stats::aggregated::backfill::tests::seed(&mut store).await;
    {
        let mut tx = store.transaction();
        put_fsm_position(&mut tx, &POSITION).unwrap();
        tx.commit().await.unwrap();
    }
    let mut config = Configuration::default();
    config.common.experimental.set_track_status_gauges(true);
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    crate::walk::engine::run_all(&mut store, 3).await.unwrap();
    let registry = get_registry_from_partition_db(store.partition_db())
        .unwrap()
        .unwrap();
    assert_eq!(registered_totals(&store), 147);
    assert_eq!(store.get_fsm_position().await.unwrap(), Some(POSITION));

    let snapshots_dir = tempdir().unwrap();
    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition.partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        snapshot.byte_offsets_base_lsn,
        Some(POSITION.byte_offsets_base_lsn)
    );
    assert_eq!(snapshot.log_byte_offset, Some(POSITION.log_byte_offset));
    assert_eq!(
        snapshot.partition_store_byte_offset,
        Some(POSITION.partition_store_byte_offset)
    );
    drop(store);
    let restored = LocalPartitionSnapshot {
        base_dir: SnapshotDir::new(snapshots_dir.path().into()),
        log_id: LogId::from(partition.partition_id),
        min_applied_lsn: snapshot.min_applied_lsn,
        db_comparator_name: snapshot.db_comparator_name.clone(),
        files: snapshot.files.clone(),
        key_range: partition.key_range,
        compatibility: snapshot.compatibility.clone(),
        byte_offsets_base_lsn: snapshot.byte_offsets_base_lsn,
        log_byte_offset: snapshot.log_byte_offset,
        partition_store_byte_offset: snapshot.partition_store_byte_offset,
    };
    let _exported_dir = snapshot.base_dir.into_path();
    manager.drop_partition_store(&partition).await.unwrap();

    let mut store = manager
        .open_from_snapshot(&partition, restored)
        .await
        .unwrap();
    store
        .verify_and_run_migrations(CancellationToken::new(), &Configuration::default())
        .await
        .unwrap();
    assert_eq!(
        get_registry_from_partition_db(store.partition_db())
            .unwrap()
            .unwrap(),
        registry,
        "the_source_replicas_instances_and_ids"
    );
    assert!(
        store
            .get_storage_features_names()
            .iter()
            .any(|name| name.as_str() == "index-registry")
    );
    assert_eq!(registered_totals(&store), 147);
    assert_eq!(store.get_fsm_position().await.unwrap(), Some(POSITION));

    // A command after the restore advances the position and maintains both views.
    let mut tx = store.transaction();
    tx.set_fsm_position(POSITION);
    tx.advance_fsm_position(Lsn::new(101), 50);
    let header = tx
        .get_vqueue_entry_status(&BaseEntryId::from(last_id))
        .await
        .unwrap()
        .unwrap();
    tx.delete_vqueue_entry_status(
        &EntryContext {
            qid: &VQueueId::custom(last_id.partition_key(), "backfill"),
            target: &last_status.invocation_target().unwrap().entry_target_ref(),
        },
        StoredEntryRef::from_header(&header),
    );
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(registered_totals(&store), 146);
    let position = store.get_fsm_position().await.unwrap().unwrap();
    assert_eq!(position.applied_lsn, Lsn::new(101));
    assert_eq!(
        position.byte_offsets_base_lsn,
        POSITION.byte_offsets_base_lsn
    );
    assert_eq!(position.log_byte_offset, POSITION.log_byte_offset + 50);
    assert!(position.partition_store_byte_offset > POSITION.partition_store_byte_offset);
    assert_eq!(store.get_applied_lsn().await.unwrap(), Some(Lsn::new(101)));
}

/// While a fence holds, a transaction buffers writes but commits none of them, and refuses a
/// second fence. Commits wait even with nothing to commit, and neither cancelling a waiting commit
/// nor clearing the transaction lifts the fence. Dropping the fence lets the commit through.
#[restate_core::test]
async fn commit_fence_holds_back_commits() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .unwrap();
    let mut reader = store.clone();
    let mut txn = store.transaction();

    let fence = txn.fence_commits().unwrap();
    assert!(txn.fence_commits().is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), txn.commit())
            .await
            .is_err()
    );
    txn.clear();
    txn.put_applied_lsn(Lsn::new(100)).unwrap();
    {
        let mut commit = std::pin::pin!(txn.commit());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut commit)
                .await
                .is_err()
        );
        assert_eq!(reader.get_applied_lsn().await.unwrap(), None);

        drop(fence);
        commit.await.unwrap();
    }
    assert_eq!(reader.get_applied_lsn().await.unwrap(), Some(Lsn::new(100)));
    assert!(txn.fence_commits().is_some());
}

/// A snapshot records the version that the barrier check requires of its data, counting storage
/// features this server doesn't know, and whichever of those and the version barrier is newer. It
/// also records the partition's storage and partition features.
#[restate_core::test]
async fn snapshot_records_what_the_barrier_check_requires() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .unwrap();
    let config = Configuration::pinned().clone();
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    insert_test_data(&mut store).await;
    {
        let mut txn = store.transaction();
        txn.put_state_machine_features(&PersistedFeatures {
            journal_v2: true,
            vqueues: true,
            ..PersistedFeatures::default()
        })
        .unwrap();
        txn.commit().await.unwrap();
    }
    let partition_id = store.partition_id();
    put_storage_features_json(
        &mut store,
        partition_id,
        br#"{ "features": { "future-feature": { "min_required_version": "9.0.0" } } }"#,
    )
    .await
    .unwrap();

    let snapshots_dir = tempdir().unwrap();
    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await
        .unwrap();
    let compatibility = snapshot.compatibility.clone().unwrap();
    assert_eq!(
        compatibility.min_restate_version,
        SemanticRestateVersion::new(9, 0, 0)
    );
    assert_eq!(compatibility.storage_features, ["future-feature"]);
    assert_eq!(compatibility.partition_features, ["journal_v2", "vqueues"]);
    let error = store
        .verify_and_run_migrations_at_version(
            &SemanticRestateVersion::new(8, 9, 0),
            CancellationToken::new(),
            &config,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MigrationError::StorageFeatureVersionBarrier { required_min_version, .. }
            if required_min_version == compatibility.min_restate_version
    ));

    {
        let mut txn = store.transaction();
        txn.put_min_restate_version(&SemanticRestateVersion::new(9, 1, 0))
            .unwrap();
        txn.commit().await.unwrap();
    }

    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await
        .unwrap();
    let compatibility = snapshot.compatibility.clone().unwrap();
    assert_eq!(
        compatibility.min_restate_version,
        SemanticRestateVersion::new(9, 1, 0)
    );
    let error = store
        .verify_and_run_migrations_at_version(
            &SemanticRestateVersion::new(9, 0, 0),
            CancellationToken::new(),
            &config,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        MigrationError::VersionBarrier { required_min_version }
            if required_min_version == compatibility.min_restate_version
    ));
    store
        .verify_and_run_migrations_at_version(
            &compatibility.min_restate_version,
            CancellationToken::new(),
            &config,
        )
        .await
        .unwrap();
}

/// Restoring a partition falls back to the next older retained snapshot when RocksDB rejects one
/// snapshot's files during the import, or finds corrupt data when verifying the imported blocks, and
/// falls back again when the next one fails.
#[restate_core::test]
async fn restore_falls_back_to_older_snapshot_when_import_is_rejected() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    RocksDbManager::init();

    let repository_dir = tempdir()?;
    let mut config = Configuration::pinned().clone();
    config.worker.snapshots.destination = Some(
        Url::from_file_path(repository_dir.path())
            .expect("the tempdir is an absolute path")
            .to_string(),
    );
    config.worker.snapshots.num_retained = NonZeroU8::new(3).unwrap();
    let staging_dir = config.worker.storage.snapshots_staging_dir();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let mut store = manager.open(&partition, None).await?;
    insert_test_data(&mut store).await;

    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");
    let snapshots_dir = tempdir()?;
    let set_position = |txn: &mut crate::PartitionStoreTransaction<'_>, lsn: Lsn| {
        put_fsm_position(
            txn,
            &FsmPosition {
                applied_lsn: lsn,
                ..POSITION
            },
        )
    };

    let older_id = SnapshotId::new();
    let older = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            older_id,
        )
        .await?;
    let older_meta = snapshot_metadata(&older, partition_id, older_id);
    assert_eq!(older_meta.min_applied_lsn, POSITION.applied_lsn);
    repository
        .put(
            &older_meta,
            older.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    // The middle snapshot holds enough data to fill several SST blocks. The test data tracks byte
    // offsets, so its position holds the applied LSN.
    let middle_id = SnapshotId::new();
    let mut txn = store.transaction();
    set_position(&mut txn, Lsn::new(200))?;
    for i in 0..1024u64 {
        // Pseudo-random values, so that compression leaves several blocks' worth of data.
        let mut state = 0x9e37_79b9_7f4a_7c15 ^ i;
        let value = (0..256)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        txn.put_kv_raw(format!("key-{i:04}"), value)?;
    }
    txn.commit().await?;
    drop(txn);
    let middle = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            middle_id,
        )
        .await?;
    let middle_meta = snapshot_metadata(&middle, partition_id, middle_id);
    assert_eq!(middle_meta.min_applied_lsn, Lsn::new(200));

    // The exported SSTs are hard links to the partition store's files, so copy this export before
    // corrupting it: the corruption must not reach the older snapshot or the store itself (which
    // also exports it below).
    let middle_files = tempdir()?;
    for file in &middle.files {
        let name = file.name.trim_start_matches('/');
        std::fs::copy(
            middle.base_dir.path().join(name),
            middle_files.path().join(name),
        )?;
    }
    // Corrupt a data block, keeping the file size. Data blocks occupy the bulk of the file, and the
    // bulk data above fills several of them. The import reads the first and last block (it computes
    // the file's bounds by seeking, because the exported internal keys don't survive the C API), so
    // corrupt the middle: the footer and metadata blocks stay intact, and only the block checksum
    // verification notices.
    let data_file = middle
        .files
        .iter()
        .max_by_key(|file| file.size)
        .expect("the export wrote an SST file")
        .name
        .trim_start_matches('/');
    let data_file = middle_files.path().join(data_file);
    let mut bytes = std::fs::read(&data_file)?;
    // The default block size is 64KiB, so this fills several data blocks.
    assert!(
        bytes.len() > 3 * 64 * 1024,
        "the export wrote several data blocks"
    );
    let middle_block = bytes.len() / 2;
    bytes[middle_block] ^= 0xff;
    std::fs::write(&data_file, &bytes)?;
    repository
        .put(
            &middle_meta,
            SnapshotDir::new(middle_files.path().to_path_buf()),
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;
    drop(middle);

    // The newest snapshot at LSN 300 has a corrupt footer, which the import itself rejects.
    let newest_id = SnapshotId::new();
    let mut txn = store.transaction();
    set_position(&mut txn, Lsn::new(300))?;
    txn.commit().await?;
    drop(txn);
    let newest = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            newest_id,
        )
        .await?;
    let newest_meta = snapshot_metadata(&newest, partition_id, newest_id);
    assert_eq!(newest_meta.min_applied_lsn, Lsn::new(300));
    let corrupted = newest
        .files
        .first()
        .expect("the export wrote an SST file")
        .name
        .trim_start_matches('/');
    let corrupted = newest.base_dir.path().join(corrupted);
    let mut bytes = std::fs::read(&corrupted)?;
    let footer_checksum = bytes.len() - 53 + 5;
    bytes[footer_checksum] ^= 0xff;
    std::fs::write(&corrupted, bytes)?;
    repository
        .put(
            &newest_meta,
            newest.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    // Both rejections are tried in descending LSN order before the older snapshot restores.
    assert_eq!(
        repository
            .get_snapshot_candidates(partition_id)
            .await?
            .iter()
            .map(|candidate| candidate.min_applied_lsn)
            .collect::<Vec<_>>(),
        vec![Lsn::new(300), Lsn::new(200), POSITION.applied_lsn],
    );

    drop(store);
    manager.drop_partition_store(&partition).await?;

    let mut restored = manager.open(&partition, None).await?;
    assert_eq!(
        restored.get_applied_lsn().await?,
        Some(POSITION.applied_lsn),
        "restored the older snapshot after the newer ones were rejected"
    );
    verify_restored_data(&mut restored).await;
    assert_eq!(
        restored.get_kv_raw("key-0000".to_owned(), |_, value| Ok(
            value.map(<[u8]>::to_vec)
        ))?,
        None,
        "the restored data is the older snapshot's, without the newer snapshot's writes"
    );
    // Neither rejected import left a column family behind: only the partition's own and the
    // database's default column family remain.
    let mut column_families = restored
        .partition_db()
        .rocksdb()
        .inner()
        .as_raw_db()
        .cf_names();
    column_families.sort();
    let mut expected = vec!["default".to_owned(), partition.cf_name().to_string()];
    expected.sort();
    assert_eq!(column_families, expected);
    // Nor did any download leave its staging directory behind.
    assert!(std::fs::read_dir(&staging_dir)?.next().is_none());

    Ok(())
}

fn snapshot_metadata(
    snapshot: &LocalPartitionSnapshot,
    partition_id: PartitionId,
    snapshot_id: SnapshotId,
) -> PartitionSnapshotMetadata {
    PartitionSnapshotMetadata {
        version: SnapshotFormatVersion::V1,
        cluster_name: Metadata::with_current(|m| m.nodes_config_ref().cluster_name().to_string()),
        cluster_fingerprint: Metadata::with_current(|m| m.nodes_config_ref().cluster_fingerprint()),
        partition_id,
        node_name: "node".to_string(),
        node_id: Some(GenerationalNodeId::new(1, 1)),
        created_at: MillisSinceEpoch::new(0).into_timestamp(),
        snapshot_id,
        key_range: snapshot.key_range,
        log_id: snapshot.log_id,
        min_applied_lsn: snapshot.min_applied_lsn,
        db_comparator_name: snapshot.db_comparator_name.clone(),
        file_keys: Default::default(),
        compatibility: snapshot.compatibility.clone(),
        byte_offsets_base_lsn: snapshot.byte_offsets_base_lsn,
        log_byte_offset: snapshot.log_byte_offset,
        partition_store_byte_offset: snapshot.partition_store_byte_offset,
        files: snapshot.files.clone(),
    }
}
