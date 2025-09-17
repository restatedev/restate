// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Unfortunately we need this because of https://github.com/rust-lang/rust-clippy/issues/9801
#![allow(clippy::borrow_interior_mutable_const)]
#![allow(clippy::declare_interior_mutable_const)]

use super::storage_test_environment;

use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::idempotency_table::{
    IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable,
};
use restate_types::identifiers::{IdempotencyId, InvocationId, InvocationUuid};

const FIXTURE_INVOCATION_1: InvocationUuid = InvocationUuid::from_u128(12345678900001);
const FIXTURE_INVOCATION_2: InvocationUuid = InvocationUuid::from_u128(12345678900002);
const FIXTURE_INVOCATION_3: InvocationUuid = InvocationUuid::from_u128(12345678900003);

const IDEMPOTENCY_ID_1: IdempotencyId =
    IdempotencyId::unkeyed(10, "my-component", "my-handler", "my-key");
const IDEMPOTENCY_ID_2: IdempotencyId =
    IdempotencyId::unkeyed(10, "my-component", "my-handler", "another-key");
const IDEMPOTENCY_ID_3: IdempotencyId =
    IdempotencyId::unkeyed(10, "my-component", "my-handler-2", "my-key");

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_idempotency_key() {
    let mut rocksdb = storage_test_environment().await;

    // Fill in some data
    let mut txn = rocksdb.transaction();
    txn.put_idempotency_metadata(
        &IDEMPOTENCY_ID_1,
        &IdempotencyMetadata {
            invocation_id: InvocationId::from_parts(10, FIXTURE_INVOCATION_1),
        },
    )
    .await
    .unwrap();
    txn.put_idempotency_metadata(
        &IDEMPOTENCY_ID_2,
        &IdempotencyMetadata {
            invocation_id: InvocationId::from_parts(10, FIXTURE_INVOCATION_2),
        },
    )
    .await
    .unwrap();
    txn.put_idempotency_metadata(
        &IDEMPOTENCY_ID_3,
        &IdempotencyMetadata {
            invocation_id: InvocationId::from_parts(10, FIXTURE_INVOCATION_3),
        },
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    // Query
    assert_eq!(
        rocksdb
            .get_idempotency_metadata(&IDEMPOTENCY_ID_1)
            .await
            .unwrap(),
        Some(IdempotencyMetadata {
            invocation_id: InvocationId::from_parts(10, FIXTURE_INVOCATION_1),
        })
    );
    assert_eq!(
        rocksdb
            .get_idempotency_metadata(&IDEMPOTENCY_ID_2)
            .await
            .unwrap(),
        Some(IdempotencyMetadata {
            invocation_id: InvocationId::from_parts(10, FIXTURE_INVOCATION_2),
        })
    );
    assert_eq!(
        rocksdb
            .get_idempotency_metadata(&IDEMPOTENCY_ID_3)
            .await
            .unwrap(),
        Some(IdempotencyMetadata {
            invocation_id: InvocationId::from_parts(10, FIXTURE_INVOCATION_3),
        })
    );
    assert_eq!(
        rocksdb
            .get_idempotency_metadata(&IdempotencyId::unkeyed(
                10,
                "my-component",
                "my-handler-3",
                "my-key",
            ))
            .await
            .unwrap(),
        None
    );

    // Delete and query afterwards
    let mut txn = rocksdb.transaction();
    txn.delete_idempotency_metadata(&IDEMPOTENCY_ID_1)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        rocksdb
            .get_idempotency_metadata(&IDEMPOTENCY_ID_1)
            .await
            .unwrap(),
        None
    );

    RocksDbManager::get().shutdown().await;
}
