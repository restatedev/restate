// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;

use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::state_table::WriteStateTable;
use restate_types::Scope;
use restate_types::identifiers::{PartitionId, ServiceId};
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;

use crate::mocks::{differential_sessions, rows};

/// Reads of the state of listed objects, as the UI lists them, match V1: every
/// state row of each listed object and nothing else, across both layouts,
/// whether or not the scope is constrained, with missing and repeated object
/// keys, large values, a state key that isn't UTF-8, and limits.
#[restate_core::test]
async fn object_lists_match_v1_across_layouts() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let scoped = |scope: &str| Some(Scope::try_non_interned(scope).unwrap());
    let mut tx = store.transaction();
    for object in 0..100 {
        let object = format!("{object}");
        // Unscoped objects of three services share each object's partition key.
        // Service names encode with their length first: "Other" sorts before
        // "Counter", "CounterArchive" after it.
        for service in ["Counter", "CounterArchive", "Other"] {
            let id = ServiceId::new(None, service, object.as_str());
            for state in ["count", "limits"] {
                tx.put_user_state(&id, &Bytes::from(state), format!("{service}/{object}"))
                    .unwrap();
            }
        }
        // A value too large for the UI's listing.
        let id = ServiceId::new(None, "Counter", object.as_str());
        tx.put_user_state(&id, &Bytes::from("history"), vec![b'x'; 4096])
            .unwrap();
    }
    for scope in ["t1", "t2", "t3"] {
        for object in ["5", "6", "57"] {
            let id = ServiceId::new(scoped(scope), "Counter", object);
            tx.put_user_state(&id, &Bytes::from("count"), format!("{scope}/{object}"))
                .unwrap();
        }
    }
    let raw = ServiceId::new(None, "Counter", "57");
    tx.put_user_state(&raw, &Bytes::from_static(&[0xff, 0xfe]), "raw")
        .unwrap();
    tx.commit().await.unwrap();
    drop(tx);

    let [old, new] = differential_sessions(&manager).await;
    let columns = "SELECT service_name, service_key, scope, key, value_length, value FROM state";
    let order = "ORDER BY service_name, service_key, scope, key, value_length";
    let ui = format!(
        "{columns} WHERE service_name = 'Counter' \
         AND service_key IN ('3', '57', '99', '404', '3') AND scope IS NULL \
         AND value_length <= 2048 {order}"
    );
    for sql in [
        ui.clone(),
        format!(
            "{columns} WHERE scope IN ('t1', 't3') AND service_key IN ('5', '57', '404') {order}"
        ),
        // Unconstrained scopes: the scoped layout can't list partition keys.
        format!("{columns} WHERE service_name = 'Counter' AND service_key IN ('5', '57') {order}"),
        format!("{columns} WHERE service_key IN ('3', '57') AND key IS NULL {order}"),
        format!("{columns} WHERE service_key IN ('3', '57', '99') {order} LIMIT 3 OFFSET 2"),
        "SELECT COUNT(*) FROM state WHERE service_key IN ('1', '2', '3', '4', '5')".into(),
    ] {
        assert_eq!(
            rows(new.as_ref(), &sql).await,
            rows(old.as_ref(), &sql).await,
            "{sql}"
        );
    }

    // Objects 3, 57 and 99: two small values each, and 57's raw key.
    let found = rows(new.as_ref(), &ui).await;
    assert_eq!(found.len(), 7, "{found:?}");
    // A list reads the same rows as one equality per object.
    let union = format!(
        "SELECT * FROM ({columns} WHERE service_name = 'Counter' AND service_key = '3' \
         AND scope IS NULL AND value_length <= 2048 \
         UNION ALL {columns} WHERE service_name = 'Counter' AND service_key = '57' \
         AND scope IS NULL AND value_length <= 2048 \
         UNION ALL {columns} WHERE service_name = 'Counter' AND service_key = '99' \
         AND scope IS NULL AND value_length <= 2048) {order}"
    );
    assert_eq!(rows(new.as_ref(), &union).await, found);
}
