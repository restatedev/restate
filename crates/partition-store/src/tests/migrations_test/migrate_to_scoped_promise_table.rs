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

use bytes::BytesMut;
use bytestring::ByteString;
use rocksdb::WriteBatch;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::promise_table::{Promise, PromiseResult, PromiseState, WritePromiseTable};
use restate_storage_api::protobuf_types::PartitionStoreProtobufValue;
use restate_types::config::Configuration;
use restate_types::errors::InvocationErrorCode;
use restate_types::identifiers::{PartitionId, PartitionKey, ServiceId, WithPartitionKey};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

use crate::PartitionStoreManager;
use crate::keys::DecodeTableKey;
use crate::migrations::MigrationContext;
use crate::migrations::migrate_to_scoped_promise_table;
use crate::migrations::tests::distinct_service_ids;
use crate::promise_table::{PromiseKey, ScopedPromiseKey};
use crate::scan::{PhysicalScan, TableScan};

#[restate_core::test]
async fn migrate_to_scoped_promise_table_moves_unscoped_promises_to_scoped_table() {
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
    let entries: Vec<(ServiceId, ByteString, Promise)> = service_ids
        .iter()
        .enumerate()
        .flat_map(|(idx, service_id)| {
            [
                (
                    service_id.clone(),
                    ByteString::from(format!("promise-a-{idx}")),
                    Promise {
                        state: PromiseState::NotCompleted(vec![]),
                    },
                ),
                (
                    service_id.clone(),
                    ByteString::from(format!("promise-b-{idx}")),
                    Promise {
                        state: PromiseState::Completed(PromiseResult::Failure(
                            InvocationErrorCode::from(500u16),
                            ByteString::from_static("nope"),
                            vec![],
                        )),
                    },
                ),
            ]
        })
        .collect();

    let mut txn = rocksdb.transaction();
    for (service_id, key, promise) in &entries {
        txn.put_promise(service_id, key, promise)
            .expect("promise write should succeed");
    }
    txn.commit().await.expect("commit should succeed");
    drop(txn);

    let config = Configuration::default();
    let mut ctx = MigrationContext::new(
        &config,
        rocksdb.partition_db(),
        rocksdb.partition_key_range(),
    );
    migrate_to_scoped_promise_table::migrate_to_scoped_promise_table(&mut ctx)
        .expect("migration should succeed");
    let mut wb = WriteBatch::default();
    migrate_to_scoped_promise_table::append_delete_promise_data(&ctx, &mut wb);
    let mut opts = rocksdb::WriteOptions::default();
    opts.disable_wal(true);
    rocksdb
        .partition_db()
        .rocksdb()
        .inner()
        .write_batch(&wb, &opts)
        .expect("delete-range write batch should commit");

    // The legacy `b"pr"` range must be empty after cleanup.
    let mut arena = BytesMut::new();
    let mut unscoped_iter = rocksdb
        .partition_db()
        .scan(PhysicalScan::from(
            TableScan::FullScanPartitionKeyRange::<PromiseKey>(rocksdb.partition_key_range()),
            &mut arena,
        ))
        .expect("scan should start");
    unscoped_iter.seek_to_first();
    assert!(
        !unscoped_iter.valid(),
        "legacy unscoped promise rows should have been deleted"
    );
    unscoped_iter
        .status()
        .expect("unscoped scan should not error");
    drop(unscoped_iter);
    arena.clear();

    // Every migrated row must now live under `b"sP"` with `scope == None`.
    let mut observed: HashMap<(PartitionKey, String, String, String), Promise> = HashMap::new();
    let mut scoped_iter = rocksdb
        .partition_db()
        .scan(PhysicalScan::from(
            TableScan::FullScanPartitionKeyRange::<ScopedPromiseKey>(rocksdb.partition_key_range()),
            &mut arena,
        ))
        .expect("scan should start");
    scoped_iter.seek_to_first();
    while scoped_iter.valid() {
        let (mut key, mut value) = scoped_iter.item().expect("iterator should be valid");
        let scoped_key =
            ScopedPromiseKey::deserialize_from(&mut key).expect("scoped key should deserialize");
        let (partition_key, scope, service_name, service_key, promise_key) = scoped_key.split();
        assert_eq!(scope, None, "migrated entries must have scope=None");
        let promise = Promise::decode(&mut value).expect("promise value should decode");
        observed.insert(
            (
                partition_key,
                service_name.as_str().to_string(),
                service_key.as_str().to_string(),
                promise_key.as_str().to_string(),
            ),
            promise,
        );
        scoped_iter.next();
    }
    scoped_iter.status().expect("scoped scan should not error");

    assert_eq!(observed.len(), entries.len());
    for (service_id, key, promise) in &entries {
        let lookup = (
            service_id.partition_key(),
            service_id.service_name.to_string(),
            service_id.key.to_string(),
            key.to_string(),
        );
        assert_eq!(observed.get(&lookup), Some(promise));
    }

    RocksDbManager::get().shutdown().await;
}
