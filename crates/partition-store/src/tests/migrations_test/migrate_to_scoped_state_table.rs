// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use rocksdb::WriteBatch;
use tokio_util::sync::CancellationToken;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_types::config::Configuration;
use restate_types::identifiers::{PartitionId, PartitionKey, ServiceId, WithPartitionKey};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

use crate::PartitionStoreManager;
use crate::keys::DecodeTableKey;
use crate::migrations::MigrationContext;
use crate::migrations::migrate_to_scoped_state_table;
use crate::migrations::tests::distinct_service_ids;
use crate::scan::{PhysicalScan, TableScan};
use crate::state_table::{ScopedStateKey, StateKey};

#[restate_core::test]
async fn migrate_to_scoped_state_table_moves_unscoped_state_to_scoped_table() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true)
        .await
        .expect("DB storage creation succeeds");
    let mut rocksdb = manager
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .expect("DB storage creation succeeds");

    let service_ids = distinct_service_ids(3);
    let entries: Vec<(ServiceId, Bytes, Bytes)> = service_ids
        .iter()
        .flat_map(|service_id| {
            [
                (
                    service_id.clone(),
                    Bytes::from_static(b"k1"),
                    Bytes::from_static(b"v1"),
                ),
                (
                    service_id.clone(),
                    Bytes::from_static(b"k2"),
                    Bytes::from_static(b"v2"),
                ),
            ]
        })
        .collect();

    let mut txn = rocksdb.transaction();
    for (service_id, state_key, value) in &entries {
        txn.put_user_state(service_id, state_key, value.as_ref())
            .expect("state write should succeed");
    }
    txn.commit().await.expect("commit should succeed");
    drop(txn);

    let config = Configuration::default();
    let cancel = CancellationToken::new();
    let mut ctx = MigrationContext::new(
        &config,
        rocksdb.partition_db(),
        rocksdb.partition_key_range(),
        cancel,
    );
    migrate_to_scoped_state_table::migrate_to_scoped_state_table(&mut ctx)
        .expect("migration should succeed");
    let mut wb = WriteBatch::default();
    migrate_to_scoped_state_table::append_delete_state_data(&ctx, &mut wb);
    let mut opts = rocksdb::WriteOptions::default();
    opts.disable_wal(true);
    rocksdb
        .partition_db()
        .rocksdb()
        .inner()
        .write_batch(&wb, &opts)
        .expect("delete-range write batch should commit");

    // The legacy `b"st"` range must be empty after cleanup.
    let mut arena = BytesMut::new();
    let mut unscoped_iter = rocksdb
        .partition_db()
        .scan(PhysicalScan::from(
            TableScan::FullScanPartitionKeyRange::<StateKey>(rocksdb.partition_key_range()),
            &mut arena,
        ))
        .expect("scan should start");
    unscoped_iter.seek_to_first();
    assert!(
        !unscoped_iter.valid(),
        "legacy unscoped state rows should have been deleted"
    );
    unscoped_iter
        .status()
        .expect("unscoped scan should not error");
    drop(unscoped_iter);
    arena.clear();

    // Every migrated row must now live under `b"sS"` with `scope == None`.
    let mut observed: HashMap<(PartitionKey, String, String, Bytes), Bytes> = HashMap::new();
    let mut scoped_iter = rocksdb
        .partition_db()
        .scan(PhysicalScan::from(
            TableScan::FullScanPartitionKeyRange::<ScopedStateKey>(rocksdb.partition_key_range()),
            &mut arena,
        ))
        .expect("scan should start");
    scoped_iter.seek_to_first();
    while scoped_iter.valid() {
        let (mut key, value) = scoped_iter.item().expect("iterator should be valid");
        let scoped_key =
            ScopedStateKey::deserialize_from(&mut key).expect("scoped key should deserialize");
        let (partition_key, scope, service_name, service_key, state_key) = scoped_key.split();
        assert_eq!(scope, None, "migrated entries must have scope=None");
        observed.insert(
            (
                partition_key,
                service_name.as_str().to_string(),
                service_key.as_str().to_string(),
                state_key,
            ),
            Bytes::copy_from_slice(value),
        );
        scoped_iter.next();
    }
    scoped_iter.status().expect("scoped scan should not error");

    assert_eq!(observed.len(), entries.len());
    for (service_id, state_key, value) in &entries {
        let lookup = (
            service_id.partition_key(),
            service_id.service_name.to_string(),
            service_id.key.to_string(),
            state_key.clone(),
        );
        assert_eq!(observed.get(&lookup), Some(value));
    }

    RocksDbManager::get().shutdown().await;
}
