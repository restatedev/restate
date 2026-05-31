// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::TestCoreEnv;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::{ReadFsmTable, WriteFsmTable};
use restate_types::identifiers::{InvocationUuid, PartitionId, PartitionKey};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

use crate::PartitionStoreManager;
use crate::journal_table_v2::JournalCompletionIdToCommandIndexKey;

/// Regression test for https://github.com/restatedev/restate/issues/4838.
///
/// The one-time orphan cleanup runs on a clone of the partition store and could outlive a
/// concurrent snapshot restore that drops and re-imports the partition's column family. Its
/// delete then targeted a dropped CF, and -- because all partitions share a single RocksDB --
/// the resulting fatal background error took the whole database read-only and crash-looped every
/// partition. This verifies that a best-effort delete to a dropped CF is silently ignored and
/// leaves the shared database fully writable.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn tolerant_delete_to_dropped_cf_does_not_poison_shared_db() {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;
    RocksDbManager::init();

    // Single shared RocksDB instance for all partitions (use_multi_db_layout = false), as in a
    // default production deployment -- this is what makes a poisoning write catastrophic.
    let manager = PartitionStoreManager::create(false)
        .await
        .expect("manager creation succeeds");

    let partition_a = Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX / 2));
    let partition_b = Partition::new(
        PartitionId::from(42),
        KeyRange::new(PartitionKey::MAX / 2 + 1, PartitionKey::MAX - 1),
    );

    let store_a = manager.open(&partition_a, None).await.expect("open a");
    let mut store_b = manager.open(&partition_b, None).await.expect("open b");

    // A stale clone of A, mirroring the cleanup task that kept a CF handle past a restore.
    let mut stale_a = store_a.clone();

    // Drop partition A's column family out from under the stale handle, as a snapshot restore
    // (drop_cf + import_cf) would.
    manager
        .drop_partition(PartitionId::MIN)
        .await
        .expect("drop a's column family");

    // The best-effort delete through the stale handle must neither fail nor poison the shared DB.
    let orphan_key = JournalCompletionIdToCommandIndexKey {
        partition_key: 1,
        invocation_uuid: InvocationUuid::from_u128(1),
        completion_id: 1,
    };
    stale_a
        .delete_key_tolerating_missing_cf(&orphan_key)
        .expect("delete to a dropped CF must be ignored, not error");

    // Partition B shares the same RocksDB; it must remain writable, proving the database was not
    // put into read-only mode by the write above.
    {
        let mut txn = store_b.transaction();
        txn.put_inbox_seq_number(42).expect("stage write to b");
        txn.commit()
            .await
            .expect("commit to b must succeed -- shared DB must not be poisoned");
    }

    let seq_number = store_b
        .get_inbox_seq_number()
        .await
        .expect("read back from b");
    assert_eq!(seq_number, 42);

    RocksDbManager::get().shutdown().await;
}
