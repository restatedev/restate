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
use restate_storage_api::promise_table::{Promise, PromiseResult, PromiseState, WritePromiseTable};
use restate_types::Scope;
use restate_types::errors::InvocationErrorCode;
use restate_types::identifiers::{PartitionId, ServiceId};
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;

use crate::mocks::{differential_sessions, rows};

#[restate_core::test]
async fn promises_match_v1_across_layouts_and_scopes() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let unscoped = ServiceId::new(None, "Signup", "alice");
    let scoped = ServiceId::new(
        Some(Scope::try_non_interned("tenant").unwrap()),
        "Signup",
        "bob",
    );
    let promises = [
        (
            &unscoped,
            "email",
            PromiseState::Completed(PromiseResult::Success(Bytes::from_static(b"ok"))),
        ),
        (
            &unscoped,
            "binary",
            PromiseState::Completed(PromiseResult::Success(Bytes::from_static(&[0xff]))),
        ),
        (
            &scoped,
            "approval",
            PromiseState::Completed(PromiseResult::Failure(
                InvocationErrorCode::from(409u16),
                "rejected".into(),
                vec![],
            )),
        ),
        (&scoped, "pending", PromiseState::NotCompleted(vec![])),
    ];
    let mut tx = store.transaction();
    for (service, key, state) in promises {
        tx.put_promise(service, &key.into(), &Promise { state })
            .unwrap();
    }
    tx.commit().await.unwrap();
    drop(tx);

    let [old, new] = differential_sessions(&manager).await;
    for sql in [
        "SELECT * FROM sys_promise ORDER BY service_key, key".to_owned(),
        "SELECT key, completed FROM sys_promise WHERE scope IS NULL AND service_key = 'alice' \
         ORDER BY key"
            .into(),
        "SELECT key, completion_failure FROM sys_promise WHERE scope = 'tenant' ORDER BY key"
            .into(),
        "SELECT key FROM sys_promise WHERE NOT completed".into(),
        "SELECT key FROM sys_promise ORDER BY key LIMIT 2 OFFSET 1".into(),
        "SELECT COUNT(*) FROM sys_promise".into(),
    ] {
        assert_eq!(
            rows(new.as_ref(), &sql).await,
            rows(old.as_ref(), &sql).await,
            "{sql}"
        );
    }
    assert_eq!(
        rows(new.as_ref(), "SELECT COUNT(*) FROM sys_promise").await,
        [["4"]]
    );
}
