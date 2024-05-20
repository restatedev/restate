// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use crate::storage_test_environment;
use bytes::Bytes;
use bytestring::ByteString;
use restate_storage_api::promise_table::{
    Promise, PromiseState, PromiseTable, ReadOnlyPromiseTable,
};
use restate_storage_api::Transaction;
use restate_types::identifiers::{InvocationId, InvocationUuid, JournalEntryId, ServiceId};
use restate_types::journal::EntryResult;

const SERVICE_ID_1: ServiceId = ServiceId::from_static(10, "MySvc", "a");
const SERVICE_ID_2: ServiceId = ServiceId::from_static(11, "MySvc", "b");

const PROMISE_KEY_1: ByteString = ByteString::from_static("prom1");
const PROMISE_KEY_2: ByteString = ByteString::from_static("prom2");
const PROMISE_KEY_3: ByteString = ByteString::from_static("prom3");

const PROMISE_COMPLETED: Promise = Promise {
    state: PromiseState::Completed(EntryResult::Success(Bytes::from_static(b"{}"))),
};

#[tokio::test]
async fn test_promise_table() {
    let mut rocksdb = storage_test_environment().await;

    let promise_not_completed = Promise {
        state: PromiseState::NotCompleted(vec![
            JournalEntryId::from_parts(
                InvocationId::from_parts(
                    10,
                    InvocationUuid::from_parts(1706027034946, 12345678900001),
                ),
                1,
            ),
            JournalEntryId::from_parts(
                InvocationId::from_parts(
                    11,
                    InvocationUuid::from_parts(1706027034946, 12345678900021),
                ),
                2,
            ),
        ]),
    };

    // Fill in some data
    let mut txn = rocksdb.transaction();
    txn.put_promise(&SERVICE_ID_1, &PROMISE_KEY_1, PROMISE_COMPLETED)
        .await;
    txn.put_promise(&SERVICE_ID_1, &PROMISE_KEY_2, promise_not_completed.clone())
        .await;
    txn.put_promise(&SERVICE_ID_2, &PROMISE_KEY_3, PROMISE_COMPLETED)
        .await;
    txn.commit().await.unwrap();

    // Query
    assert_eq!(
        rocksdb
            .get_promise(&SERVICE_ID_1, &PROMISE_KEY_1,)
            .await
            .unwrap(),
        Some(PROMISE_COMPLETED)
    );
    assert_eq!(
        rocksdb
            .get_promise(&SERVICE_ID_1, &PROMISE_KEY_2,)
            .await
            .unwrap(),
        Some(promise_not_completed)
    );
    assert_eq!(
        rocksdb
            .get_promise(&SERVICE_ID_2, &PROMISE_KEY_3,)
            .await
            .unwrap(),
        Some(PROMISE_COMPLETED)
    );
    assert_eq!(
        rocksdb
            .get_promise(&SERVICE_ID_1, &PROMISE_KEY_3,)
            .await
            .unwrap(),
        None
    );

    // Delete and query afterwards
    let mut txn = rocksdb.transaction();
    txn.delete_all_promises(&SERVICE_ID_1).await;
    txn.commit().await.unwrap();

    assert_eq!(
        rocksdb
            .get_promise(&SERVICE_ID_1, &PROMISE_KEY_1,)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        rocksdb
            .get_promise(&SERVICE_ID_1, &PROMISE_KEY_2,)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        rocksdb
            .get_promise(&SERVICE_ID_2, &PROMISE_KEY_3,)
            .await
            .unwrap(),
        Some(PROMISE_COMPLETED)
    );
}
