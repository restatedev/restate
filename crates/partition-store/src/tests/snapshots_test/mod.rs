// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::{NonZeroU8, NonZeroUsize};
use std::sync::Arc;
use std::task::{Context, Waker};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use url::Url;

use restate_core::{Metadata, TestCoreEnv};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::{FsmPosition, ReadFsmTable, WriteFsmTable};
use restate_test_util::tempdir;
use restate_types::config::{Configuration, SnapshotTrigger};
use restate_types::identifiers::{PartitionId, PartitionKey, SnapshotId};
use restate_types::logs::{LogId, Lsn};
use restate_types::partitions::Partition;
use restate_types::partitions::features::PersistedFeatures;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::{GenerationalNodeId, SemanticRestateVersion};
use restate_util_bytecount::NonZeroByteCount;

use crate::fsm_table::{put_fsm_position, put_storage_features_json};
use crate::migrations::MigrationError;
use crate::partition_store::StorageAccess;
use crate::snapshots::{
    ExportedSnapshot, LocalPartitionSnapshot, PartitionSnapshotMetadata, SnapshotDir,
    SnapshotExportMode, SnapshotFormatVersion, SnapshotLeaseGuard, SnapshotRepository,
};
use crate::{PartitionSeal, PartitionStore, PartitionStoreManager};

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
        whole_db_files: snapshot.whole_db_files.clone(),
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
        version: snapshot_meta.version,
        log_id: LogId::from(partition_id),
        min_applied_lsn: snapshot_meta.min_applied_lsn,
        db_comparator_name: snapshot_meta.db_comparator_name.clone(),
        files: snapshot_meta.files.clone(),
        key_range,
        compatibility: snapshot_meta.compatibility.clone(),
        byte_offsets_base_lsn: snapshot_meta.byte_offsets_base_lsn,
        log_byte_offset: snapshot_meta.log_byte_offset,
        partition_store_byte_offset: snapshot_meta.partition_store_byte_offset,
        whole_db_files: snapshot_meta.whole_db_files.clone(),
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

    let repository_dir = tempdir()?.keep();
    let mut config = Configuration::pinned().clone();
    config.worker.snapshots.destination = Some(
        Url::from_file_path(&repository_dir)
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

/// A large value with a deterministic, incompressible payload.
fn blob_value(seed: u64) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15 ^ seed;
    (0..2048)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// A whole-database checkpoint (V3) with blob files uploads its SSTs and blobs to the shared
/// `ssts/` prefix and restores by opening the downloaded database, without an import.
#[restate_core::test]
async fn blob_files_snapshot_and_restore_whole_db() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let config = blob_snapshot_config()?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let mut store = manager.open(&partition, None).await?;
    let values: Vec<_> = (0..4)
        .map(|i| (format!("big-{i}"), blob_value(i)))
        .collect();
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    for (key, value) in &values {
        txn.put_kv_raw(key.clone(), value.clone())?;
    }
    txn.commit().await?;
    drop(txn);

    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");

    let snapshot_id = SnapshotId::new();
    let snapshots_dir = tempdir()?;
    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            snapshot_id,
        )
        .await?;
    assert_eq!(snapshot.version, SnapshotFormatVersion::V3);
    assert!(
        snapshot
            .whole_db_files
            .iter()
            .any(|file| file.name.ends_with(".blob")),
        "the checkpoint has blob files"
    );
    let metadata = snapshot_metadata(&snapshot, partition_id, snapshot_id);
    repository
        .put(
            &metadata,
            snapshot.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    // The manifest records the checkpoint's files, and the blobs are shared objects.
    let stored = repository
        .get_snapshot_metadata(
            partition_id,
            &repository.get_snapshot_candidates(partition_id).await?[0],
        )
        .await?;
    assert_eq!(stored.version, SnapshotFormatVersion::V3);
    assert!(stored.files.iter().any(|file| file.name.ends_with(".sst")));
    let blob = stored
        .whole_db_files
        .iter()
        .find(|file| file.name.ends_with(".blob"))
        .expect("a blob file");
    assert!(
        stored
            .file_keys
            .get(&blob.name)
            .is_some_and(|key| key.starts_with("ssts/") && key.ends_with(".blob"))
    );
    assert!(
        stored
            .whole_db_files
            .iter()
            .any(|file| file.name == "CURRENT")
    );

    drop(store);
    wipe_local_database(&manager, &partition).await?;

    let mut restored = manager.open(&partition, None).await?;
    assert_eq!(restored.get_applied_lsn().await?, Some(Lsn::new(100)));
    for (key, value) in &values {
        assert_eq!(
            restored.get_kv_raw(key.clone(), |_, value| Ok(value.map(<[u8]>::to_vec)))?,
            Some(value.clone()),
            "restored value of {key}"
        );
    }
    Ok(())
}

/// The `new-files` trigger skips a round below its churn threshold without flushing, publishes
/// above it, and the published checkpoint restores without any WAL files.
#[restate_core::test]
async fn new_files_trigger_skips_below_threshold_and_restores() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let (config, _repository_dir) = new_files_snapshot_config(1)?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let cf_name = partition.cf_name().to_string();
    let mut store = manager.open(&partition, None).await?;
    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");

    let first_values: Vec<_> = (0..3)
        .map(|i| (format!("first-{i}"), blob_value(i)))
        .collect();
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    for (key, value) in &first_values {
        txn.put_kv_raw(key.clone(), value.clone())?;
    }
    txn.commit().await?;
    drop(txn);
    store.partition_db().flush_memtables(true).await?;

    // This write stays in the memtable. A checkpoint that flushed would include it.
    let unflushed_value = blob_value(99);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(200))?;
    txn.put_kv_raw("unflushed".to_owned(), unflushed_value.clone())?;
    txn.commit().await?;
    drop(txn);
    assert_eq!(store.get_applied_lsn().await?, Some(Lsn::new(200)));
    let flushes_before = manager.snapshot_triggered_flushes();

    let snapshots_dir = tempdir()?;
    let first_id = SnapshotId::new();
    let ExportedSnapshot::Snapshot(first) = export_with_fence(
        &manager,
        &mut store,
        first_id,
        snapshots_dir.path(),
        SnapshotExportMode::NewFiles,
    )
    .await?
    else {
        panic!("the first checkpoint is above the threshold");
    };
    assert_eq!(first.min_applied_lsn, Lsn::new(100));
    assert!(
        first.min_applied_lsn < store.get_applied_lsn().await?.unwrap(),
        "the checkpoint label must be below the in-memory applied LSN"
    );
    assert_eq!(
        manager.snapshot_triggered_flushes(),
        flushes_before,
        "the new-files trigger must not flush"
    );
    assert_eq!(
        max_checkpoint_applied_lsn(&first, partition_id, &cf_name).await?,
        Some(first.min_applied_lsn)
    );
    assert_eq!(
        read_checkpoint_applied_lsn(&first, partition_id, &cf_name)?,
        Some(first.min_applied_lsn)
    );
    let first_live_files = checkpoint_live_file_names(&first);
    assert_eq!(
        manager
            .churn_cache_file_names(partition_id)
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        first_live_files,
        "the cache holds exactly the checkpoint's live files"
    );
    let first_metadata = snapshot_metadata(&first, partition_id, first_id);
    assert!(
        first_metadata
            .whole_db_files
            .iter()
            .all(|file| !file.name.ends_with(".log")),
        "the checkpoint never uploads WAL files"
    );
    repository
        .put(
            &first_metadata,
            first.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;
    let first_latest = manager
        .snapshots
        .get_latest_snapshot_metadata(partition_id)
        .await?
        .expect("the first snapshot was published");

    // Nothing new is durable, so the round skips without publishing or advancing snapshot state.
    let skipped = export_with_fence(
        &manager,
        &mut store,
        SnapshotId::new(),
        snapshots_dir.path(),
        SnapshotExportMode::NewFiles,
    )
    .await?;
    assert!(matches!(skipped, ExportedSnapshot::Skipped));
    assert_eq!(manager.snapshot_triggered_flushes(), flushes_before);
    let still_latest = manager
        .snapshots
        .get_latest_snapshot_metadata(partition_id)
        .await?
        .expect("the first snapshot is still published");
    assert_eq!(still_latest.snapshot_id, first_latest.snapshot_id);
    assert_eq!(still_latest.min_applied_lsn, first_latest.min_applied_lsn);
    assert_eq!(
        manager
            .churn_cache_file_names(partition_id)
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
        first_live_files,
        "a skipped round keeps only the checkpoint's live files"
    );

    // Restoring the first snapshot must not contain the unflushed write.
    drop(store);
    wipe_local_database(&manager, &partition).await?;
    let mut store = manager.open(&partition, None).await?;
    assert!(manager.churn_cache_file_names(partition_id).is_empty());
    assert_eq!(store.get_applied_lsn().await?, Some(Lsn::new(100)));
    for (key, value) in &first_values {
        assert_eq!(
            store.get_kv_raw(key.clone(), |_, value| Ok(value.map(<[u8]>::to_vec)))?,
            Some(value.clone()),
            "restored {key}"
        );
    }
    assert_eq!(
        store.get_kv_raw("unflushed".to_owned(), |_, value| Ok(
            value.map(<[u8]>::to_vec)
        ))?,
        None,
        "the checkpoint must not include the unflushed write"
    );

    // New durable data crosses the threshold.
    let second_value = blob_value(7);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(300))?;
    txn.put_kv_raw("second".to_owned(), second_value.clone())?;
    txn.commit().await?;
    drop(txn);
    store.partition_db().flush_memtables(true).await?;

    let second_id = SnapshotId::new();
    let ExportedSnapshot::Snapshot(second) = export_with_fence(
        &manager,
        &mut store,
        second_id,
        snapshots_dir.path(),
        SnapshotExportMode::NewFiles,
    )
    .await?
    else {
        panic!("the second checkpoint is above the threshold");
    };
    assert_eq!(second.min_applied_lsn, Lsn::new(300));
    let second_metadata = snapshot_metadata(&second, partition_id, second_id);
    assert!(
        second_metadata
            .whole_db_files
            .iter()
            .all(|file| !file.name.ends_with(".log"))
    );
    repository
        .put(
            &second_metadata,
            second.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;
    let latest = manager
        .snapshots
        .get_latest_snapshot_metadata(partition_id)
        .await?
        .expect("the second snapshot was published");
    assert_eq!(latest.min_applied_lsn, Lsn::new(300));

    drop(store);
    wipe_local_database(&manager, &partition).await?;

    let mut restored = manager.open(&partition, None).await?;
    assert_eq!(restored.get_applied_lsn().await?, Some(Lsn::new(300)));
    for (key, value) in &first_values {
        assert_eq!(
            restored.get_kv_raw(key.clone(), |_, value| Ok(value.map(<[u8]>::to_vec)))?,
            Some(value.clone()),
            "restored {key}"
        );
    }
    assert_eq!(
        restored.get_kv_raw(
            "second".to_owned(),
            |_, value| Ok(value.map(<[u8]>::to_vec))
        )?,
        Some(second_value)
    );
    assert_eq!(
        restored.get_kv_raw("unflushed".to_owned(), |_, value| Ok(
            value.map(<[u8]>::to_vec)
        ))?,
        None
    );
    Ok(())
}

/// A seal written through the WAL is flushed immediately and is part of the next checkpoint.
#[restate_core::test]
async fn new_files_snapshot_includes_seal() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let (config, _repository_dir) = new_files_snapshot_config(1)?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let mut store = manager.open(&partition, None).await?;
    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");

    let value = blob_value(11);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    txn.put_kv_raw("sealed".to_owned(), value.clone())?;
    txn.commit().await?;
    drop(txn);
    store.partition_db().flush_memtables(true).await?;
    store
        .seal(&PartitionSeal::AheadOfLog {
            partition_applied_lsn: Lsn::new(100),
            log_tail_lsn: Lsn::new(50),
        })
        .await?;
    let flushes_before = manager.snapshot_triggered_flushes();

    let snapshots_dir = tempdir()?;
    let snapshot_id = SnapshotId::new();
    let ExportedSnapshot::Snapshot(snapshot) = export_with_fence(
        &manager,
        &mut store,
        snapshot_id,
        snapshots_dir.path(),
        SnapshotExportMode::NewFiles,
    )
    .await?
    else {
        panic!("the checkpoint is above the threshold");
    };
    assert_eq!(snapshot.min_applied_lsn, Lsn::new(100));
    assert_eq!(manager.snapshot_triggered_flushes(), flushes_before);
    repository
        .put(
            &snapshot_metadata(&snapshot, partition_id, snapshot_id),
            snapshot.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    drop(store);
    wipe_local_database(&manager, &partition).await?;

    let mut restored = manager.open(&partition, None).await?;
    assert_eq!(restored.get_applied_lsn().await?, Some(Lsn::new(100)));
    assert!(
        restored.get_seal_marker().await?.is_some(),
        "the restored database has the seal"
    );
    assert_eq!(
        restored.get_kv_raw(
            "sealed".to_owned(),
            |_, value| Ok(value.map(<[u8]>::to_vec))
        )?,
        Some(value)
    );
    Ok(())
}

/// The backstop flushes once and publishes even when churn is below the threshold.
#[restate_core::test]
async fn new_files_backstop_flushes_once_and_publishes() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let (config, _repository_dir) = new_files_snapshot_config(1024 * 1024 * 1024)?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let mut store = manager.open(&partition, None).await?;
    let value = blob_value(21);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    txn.put_kv_raw("unflushed".to_owned(), value.clone())?;
    txn.commit().await?;
    drop(txn);
    let flushes_before = manager.snapshot_triggered_flushes();
    let snapshots_dir = tempdir()?;

    // Nothing was ever flushed, so there is no applied LSN in the checkpoint.
    let skipped = export_with_fence(
        &manager,
        &mut store,
        SnapshotId::new(),
        snapshots_dir.path(),
        SnapshotExportMode::NewFiles,
    )
    .await?;
    assert!(matches!(skipped, ExportedSnapshot::Skipped));
    assert_eq!(manager.snapshot_triggered_flushes(), flushes_before);

    let ExportedSnapshot::Snapshot(snapshot) = export_with_fence(
        &manager,
        &mut store,
        SnapshotId::new(),
        snapshots_dir.path(),
        SnapshotExportMode::NewFilesBackstop,
    )
    .await?
    else {
        panic!("the backstop publishes regardless of churn");
    };
    assert_eq!(snapshot.min_applied_lsn, Lsn::new(100));
    assert_eq!(
        manager.snapshot_triggered_flushes(),
        flushes_before + 1,
        "the backstop flushes exactly once"
    );
    assert!(
        snapshot
            .whole_db_files
            .iter()
            .all(|file| !file.name.ends_with(".log"))
    );
    Ok(())
}

/// A corrupt blob file in the newest snapshot fails verification during restore, which falls
/// back to the next older retained snapshot.
#[restate_core::test]
async fn blob_files_restore_falls_back_when_blob_is_corrupt() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let config = blob_snapshot_config()?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let mut store = manager.open(&partition, None).await?;
    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");

    let older_value = blob_value(0);
    let newer_value = blob_value(1);
    let put = |txn: &mut crate::PartitionStoreTransaction<'_>, lsn, key: &str, value: &Vec<u8>| {
        txn.put_applied_lsn(lsn)?;
        txn.put_kv_raw(key.to_owned(), value.clone())
    };
    let mut txn = store.transaction();
    put(&mut txn, Lsn::new(100), "older", &older_value)?;
    txn.commit().await?;
    drop(txn);

    let snapshots_dir = tempdir()?;
    let older = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await?;
    let older_lsn = older.min_applied_lsn;
    repository
        .put(
            &snapshot_metadata(&older, partition_id, SnapshotId::new()),
            older.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    let mut txn = store.transaction();
    put(&mut txn, Lsn::new(200), "newer", &newer_value)?;
    txn.commit().await?;
    drop(txn);

    let newest = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            SnapshotId::new(),
        )
        .await?;
    let newest_meta = snapshot_metadata(&newest, partition_id, SnapshotId::new());
    // Copy the checkpoint before corrupting it: its files are hard links to the store's files.
    let copy = tempdir()?;
    let names = newest
        .whole_db_files
        .iter()
        .map(|file| file.name.clone())
        .chain(
            newest
                .files
                .iter()
                .map(|file| file.name.trim_start_matches('/').to_owned()),
        );
    for name in names {
        std::fs::copy(newest.base_dir.path().join(&name), copy.path().join(&name))?;
    }
    let blob_name = newest
        .whole_db_files
        .iter()
        .find(|file| file.name.ends_with(".blob"))
        .expect("the newest checkpoint has a blob file")
        .name
        .clone();
    let blob_path = copy.path().join(&blob_name);
    let mut bytes = std::fs::read(&blob_path)?;
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&blob_path, bytes)?;
    repository
        .put(
            &newest_meta,
            SnapshotDir::new(copy.path().to_path_buf()),
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    drop(store);
    wipe_local_database(&manager, &partition).await?;

    let mut restored = manager.open(&partition, None).await?;
    assert_eq!(restored.get_applied_lsn().await?, Some(older_lsn));
    assert_eq!(
        restored.get_kv_raw("older".to_owned(), |_, value| Ok(value.map(<[u8]>::to_vec)))?,
        Some(older_value)
    );
    assert_eq!(
        restored.get_kv_raw("newer".to_owned(), |_, value| Ok(value.map(<[u8]>::to_vec)))?,
        None,
        "restored the older snapshot"
    );
    Ok(())
}

/// A restore interrupted after downloading into staging leaves that directory behind. The next
/// open removes it and restores normally from the repository.
#[restate_core::test]
async fn blob_files_restore_ignores_leftover_staging_dir() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let config = blob_snapshot_config()?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let mut store = manager.open(&partition, None).await?;
    let value = blob_value(7);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    txn.put_kv_raw("large".to_owned(), value.clone())?;
    txn.commit().await?;
    drop(txn);

    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");
    let snapshot_id = SnapshotId::new();
    let snapshots_dir = tempdir()?;
    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            snapshot_id,
        )
        .await?;
    repository
        .put(
            &snapshot_metadata(&snapshot, partition_id, snapshot_id),
            snapshot.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    drop(store);
    wipe_local_database(&manager, &partition).await?;

    let db_name = restate_rocksdb::DbName::from(partition.db_name(true));
    let data_dir = Configuration::pinned().worker.storage.data_dir(&db_name);
    let staging = data_dir.with_file_name(format!(
        "{}.restoring-0123456789abcdef",
        data_dir
            .file_name()
            .expect("db directory name")
            .to_string_lossy()
    ));
    tokio::fs::create_dir_all(&staging).await?;
    tokio::fs::write(staging.join("CURRENT"), b"partial").await?;

    let mut restored = manager.open(&partition, None).await?;
    assert!(
        !staging.exists(),
        "the leftover staging directory is removed"
    );
    assert_eq!(restored.get_applied_lsn().await?, Some(Lsn::new(100)));
    assert_eq!(
        restored.get_kv_raw("large".to_owned(), |_, value| Ok(value.map(<[u8]>::to_vec)))?,
        Some(value)
    );
    Ok(())
}

/// A lagging replica fast-forwards to a whole-database checkpoint by dropping its local database
/// and restoring through the same restore-by-open path.
#[restate_core::test]
async fn blob_files_fast_forward_existing_database() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let config = blob_snapshot_config()?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let mut store = manager.open(&partition, None).await?;
    let value = blob_value(11);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(300))?;
    txn.put_kv_raw("fast-forwarded".to_owned(), value.clone())?;
    txn.commit().await?;
    drop(txn);

    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");
    let snapshot_id = SnapshotId::new();
    let snapshots_dir = tempdir()?;
    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            snapshot_id,
        )
        .await?;
    assert_eq!(snapshot.version, SnapshotFormatVersion::V3);
    repository
        .put(
            &snapshot_metadata(&snapshot, partition_id, snapshot_id),
            snapshot.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    // Make the local replica lag behind the repository, as a follower that missed a trim gap
    // would.
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    txn.commit().await?;
    drop(txn);
    drop(store);

    let mut restored = manager.open(&partition, Some(Lsn::new(300))).await?;
    assert_eq!(restored.get_applied_lsn().await?, Some(Lsn::new(300)));
    assert_eq!(
        restored.get_kv_raw("fast-forwarded".to_owned(), |_, value| Ok(
            value.map(<[u8]>::to_vec)
        ))?,
        Some(value)
    );
    Ok(())
}

/// A corrupt V3 snapshot fails verification before the old database is touched.
#[restate_core::test]
async fn blob_files_corrupt_fast_forward_keeps_old_database() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let config = blob_snapshot_config()?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let mut store = manager.open(&partition, None).await?;
    let local_value = blob_value(21);
    let remote_value = blob_value(22);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    txn.put_kv_raw("local".to_owned(), local_value.clone())?;
    txn.commit().await?;
    drop(txn);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(300))?;
    txn.put_kv_raw("remote".to_owned(), remote_value)?;
    txn.commit().await?;
    drop(txn);

    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");
    let snapshot_id = SnapshotId::new();
    let snapshots_dir = tempdir()?;
    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            snapshot_id,
        )
        .await?;
    let metadata = snapshot_metadata(&snapshot, partition_id, snapshot_id);
    let copy = tempdir()?;
    for name in snapshot
        .whole_db_files
        .iter()
        .map(|file| file.name.clone())
        .chain(
            snapshot
                .files
                .iter()
                .map(|file| file.name.trim_start_matches('/').to_owned()),
        )
    {
        std::fs::copy(
            snapshot.base_dir.path().join(&name),
            copy.path().join(&name),
        )?;
    }
    let blob_name = snapshot
        .whole_db_files
        .iter()
        .find(|file| file.name.ends_with(".blob"))
        .expect("the checkpoint has a blob file")
        .name
        .clone();
    let blob_path = copy.path().join(blob_name);
    let mut bytes = std::fs::read(&blob_path)?;
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0xff;
    std::fs::write(&blob_path, bytes)?;
    repository
        .put(
            &metadata,
            SnapshotDir::new(copy.path().to_path_buf()),
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    // Rewind so the local database lags the snapshot's LSN.
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    txn.commit().await?;
    drop(txn);
    drop(store);

    assert!(
        manager.open(&partition, Some(Lsn::new(300))).await.is_err(),
        "the corrupt snapshot must not fast-forward"
    );

    let mut unchanged = manager.open(&partition, None).await?;
    assert_eq!(unchanged.get_applied_lsn().await?, Some(Lsn::new(100)));
    assert_eq!(
        unchanged.get_kv_raw("local".to_owned(), |_, value| Ok(value.map(<[u8]>::to_vec)))?,
        Some(local_value),
        "the old database is still readable"
    );
    Ok(())
}

/// A crash between moving the old directory aside and installing the new one is recovered by
/// putting the replaced directory back.
#[restate_core::test]
async fn blob_files_recovers_replaced_directory() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let config = blob_snapshot_config()?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let mut store = manager.open(&partition, None).await?;
    let value = blob_value(31);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    txn.put_kv_raw("recovered".to_owned(), value.clone())?;
    txn.commit().await?;
    drop(txn);
    drop(store);
    manager.close_partition_store(partition.partition_id).await;

    let db_name = restate_rocksdb::DbName::from(partition.db_name(true));
    let data_dir = Configuration::pinned().worker.storage.data_dir(&db_name);
    manager.forget_cached_database(&db_name).await;
    let replaced = data_dir.with_file_name(format!(
        "{}.replaced-0123456789abcdef",
        data_dir
            .file_name()
            .expect("db directory name")
            .to_string_lossy()
    ));
    tokio::fs::rename(&data_dir, &replaced).await?;

    let mut recovered = manager.open(&partition, None).await?;
    assert!(!replaced.exists(), "the replaced directory is consumed");
    assert_eq!(recovered.get_applied_lsn().await?, Some(Lsn::new(100)));
    assert_eq!(
        recovered.get_kv_raw("recovered".to_owned(), |_, value| Ok(
            value.map(<[u8]>::to_vec)
        ))?,
        Some(value)
    );
    Ok(())
}

/// Dropping the open future mid-restore leaves the spawned restore owning the partition lock;
/// the next open waits for it and succeeds.
#[restate_core::test]
async fn blob_files_dropped_open_does_not_abandon_restore() -> anyhow::Result<()> {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    let config = blob_snapshot_config()?;
    RocksDbManager::init();
    Configuration::set(config.clone());
    let manager = PartitionStoreManager::create(true).await?;

    let partition = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1));
    let partition_id = partition.partition_id;
    let mut store = manager.open(&partition, None).await?;
    let value = blob_value(41);
    let mut txn = store.transaction();
    txn.put_applied_lsn(Lsn::new(100))?;
    txn.put_kv_raw("survives".to_owned(), value.clone())?;
    txn.commit().await?;
    drop(txn);

    let repository = SnapshotRepository::new_from_config_with_stub_leases(
        &config.worker.snapshots,
        tempdir()?.keep(),
    )
    .await?
    .expect("a snapshot destination is configured");
    let snapshot_id = SnapshotId::new();
    let snapshots_dir = tempdir()?;
    let snapshot = store
        .create_local_snapshot(
            manager.test_capture_guard(partition_id).await,
            snapshots_dir.path(),
            None,
            snapshot_id,
        )
        .await?;
    repository
        .put(
            &snapshot_metadata(&snapshot, partition_id, snapshot_id),
            snapshot.base_dir,
            Arc::new(SnapshotLeaseGuard::noop()),
        )
        .await?;

    drop(store);
    manager.close_partition_store(partition.partition_id).await;
    let db_name = restate_rocksdb::DbName::from(partition.db_name(true));
    let data_dir = Configuration::pinned().worker.storage.data_dir(&db_name);
    manager.forget_cached_database(&db_name).await;
    tokio::fs::remove_dir_all(&data_dir).await?;

    let mut opening = Box::pin(manager.open(&partition, None));
    assert!(
        opening
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "the restore is still running"
    );
    drop(opening);

    let mut restored = manager.open(&partition, None).await?;
    assert_eq!(restored.get_applied_lsn().await?, Some(Lsn::new(100)));
    assert_eq!(
        restored.get_kv_raw("survives".to_owned(), |_, value| Ok(
            value.map(<[u8]>::to_vec)
        ))?,
        Some(value)
    );
    Ok(())
}

fn checkpoint_live_file_names(
    snapshot: &LocalPartitionSnapshot,
) -> std::collections::HashSet<String> {
    snapshot
        .files
        .iter()
        .map(|file| file.name.trim_start_matches('/').to_owned())
        .chain(
            snapshot
                .whole_db_files
                .iter()
                .filter(|file| file.name.ends_with(".blob"))
                .map(|file| file.name.clone()),
        )
        .collect()
}

fn new_files_snapshot_config(
    min_new_bytes: u64,
) -> anyhow::Result<(Configuration, std::path::PathBuf)> {
    let repository_dir = tempdir()?.keep();
    let mut config = Configuration::pinned().clone();
    config.worker.storage.experimental_rocksdb_enable_blob_files = true;
    config.worker.storage.experimental_rocksdb_blob_min_size =
        Some(NonZeroByteCount::new(NonZeroUsize::new(64).unwrap()));
    config.worker.use_multi_db_layout = true;
    config.worker.snapshots.destination = Some(
        Url::from_file_path(&repository_dir)
            .expect("the tempdir is an absolute path")
            .to_string(),
    );
    config.worker.snapshots.num_retained = NonZeroU8::new(3).unwrap();
    config.worker.snapshots.experimental_snapshot_trigger = SnapshotTrigger::NewFiles;
    config.worker.snapshots.snapshot_min_new_bytes = Some(NonZeroByteCount::new(
        NonZeroUsize::new(usize::try_from(min_new_bytes)?).unwrap(),
    ));
    Ok((config, repository_dir))
}

async fn export_with_fence(
    manager: &Arc<PartitionStoreManager>,
    store: &mut PartitionStore,
    snapshot_id: SnapshotId,
    snapshot_base_path: &std::path::Path,
    mode: SnapshotExportMode,
) -> anyhow::Result<ExportedSnapshot> {
    let partition_id = store.partition_id();
    let (commit_fence_tx, mut commit_fence_rx) =
        tokio::sync::mpsc::channel::<crate::snapshots::CommitFenceRequest>(1);
    let mut txn = store.transaction();
    let processor = async move {
        let request = commit_fence_rx
            .recv()
            .await
            .expect("export asks for a fence");
        let _ = request.send(txn.fence_commits());
    };
    let export = manager.export_partition(
        partition_id,
        Some(&commit_fence_tx),
        None,
        snapshot_id,
        snapshot_base_path,
        mode,
    );
    let (result, ()) = tokio::join!(export, processor);
    result.map_err(|err| anyhow::anyhow!("{err}"))
}

fn read_checkpoint_applied_lsn(
    snapshot: &LocalPartitionSnapshot,
    partition_id: PartitionId,
    cf_name: &str,
) -> anyhow::Result<Option<Lsn>> {
    let path = snapshot.base_dir.path();
    let options = rocksdb::Options::default();
    let cfs = rocksdb::DB::list_cf(&options, path)?;
    let db = rocksdb::DB::open_cf_for_read_only(&options, path, &cfs, false)?;
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow::anyhow!("checkpoint has no column family '{cf_name}'"))?;
    Ok(crate::fsm_table::read_applied_lsn_from_db(
        &db,
        &cf,
        partition_id,
    )?)
}

async fn max_checkpoint_applied_lsn(
    snapshot: &LocalPartitionSnapshot,
    partition_id: PartitionId,
    _cf_name: &str,
) -> anyhow::Result<Option<Lsn>> {
    let key = format!("p:{partition_id}");
    let mut max = None;
    for file in &snapshot.files {
        let path = snapshot
            .base_dir
            .path()
            .join(file.name.trim_start_matches('/'));
        let Some(value) =
            crate::snapshots::sst_fingerprint::read_user_property(&path, key.as_bytes()).await?
        else {
            continue;
        };
        let value = std::str::from_utf8(&value)?.parse::<u64>()?;
        max = Some(max.map_or_else(|| Lsn::new(value), |max: Lsn| max.max(Lsn::new(value))));
    }
    Ok(max)
}

fn blob_snapshot_config() -> anyhow::Result<Configuration> {
    let repository_dir = tempdir()?.keep();
    let mut config = Configuration::pinned().clone();
    config.worker.storage.experimental_rocksdb_enable_blob_files = true;
    config.worker.storage.experimental_rocksdb_blob_min_size =
        Some(NonZeroByteCount::new(NonZeroUsize::new(64).unwrap()));
    config.worker.use_multi_db_layout = true;
    config.worker.snapshots.destination = Some(
        Url::from_file_path(&repository_dir)
            .expect("the tempdir is an absolute path")
            .to_string(),
    );
    config.worker.snapshots.num_retained = NonZeroU8::new(3).unwrap();
    Ok(config)
}

/// Mirrors local-store deletion in production: drops the partition's column family but keeps
/// the database directory.
async fn wipe_local_database(
    manager: &Arc<PartitionStoreManager>,
    partition: &Partition,
) -> anyhow::Result<()> {
    manager.drop_partition_store(partition).await?;
    Ok(())
}

fn snapshot_metadata(
    snapshot: &LocalPartitionSnapshot,
    partition_id: PartitionId,
    snapshot_id: SnapshotId,
) -> PartitionSnapshotMetadata {
    PartitionSnapshotMetadata {
        version: snapshot.version,
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
        whole_db_files: snapshot.whole_db_files.clone(),
    }
}
