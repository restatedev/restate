// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;

use restate_storage_api::Transaction;
use restate_storage_api::explicit_recovery_table::{
    ReadExplicitRecoveryTable, WriteExplicitRecoveryTable,
};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::journal_v2::SignalResult;

use super::storage_test_environment;

// Same partition key, adjacent uuids: the prefixes of the two invocations must not mix.
const INVOCATION_1: InvocationId = InvocationId::from_parts(1, InvocationUuid::from_u128(10));
const INVOCATION_2: InvocationId = InvocationId::from_parts(1, InvocationUuid::from_u128(11));

#[restate_core::test]
async fn state_and_signals() {
    let mut store = storage_test_environment().await;
    let (a, b) = (Bytes::from_static(b"a"), Bytes::from_static(b"b"));
    let value = |s: &'static str| SignalResult::Success(Bytes::from_static(s.as_bytes()));

    let mut txn = store.transaction();
    for id in [&INVOCATION_1, &INVOCATION_2] {
        txn.put_invocation_state(id, &a, &Bytes::from_static(b"1"))
            .unwrap();
        txn.put_invocation_state(id, &b, &Bytes::from_static(b"2"))
            .unwrap();
        assert_eq!(txn.next_signal_index(id, "approval").await.unwrap(), 0);
        txn.put_signal(id, "approval", 0, &value("first")).unwrap();
        assert_eq!(txn.next_signal_index(id, "approval").await.unwrap(), 1);
        txn.put_signal(id, "approval", 1, &SignalResult::Void)
            .unwrap();
    }
    txn.commit().await.unwrap();

    // Point and scan reads, per invocation.
    assert_eq!(
        store.get_invocation_state(&INVOCATION_1, &a).await.unwrap(),
        Some(Bytes::from_static(b"1"))
    );
    assert_eq!(
        store.get_all_invocation_state(&INVOCATION_1).await.unwrap(),
        vec![
            (a.clone(), Bytes::from_static(b"1")),
            (b.clone(), Bytes::from_static(b"2"))
        ]
    );
    assert_eq!(
        store
            .next_signal_index(&INVOCATION_1, "approval")
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        store
            .next_signal_index(&INVOCATION_1, "other")
            .await
            .unwrap(),
        0
    );
    // A name that is a prefix of another one does not see its signals.
    assert_eq!(
        store
            .next_signal_index(&INVOCATION_1, "approv")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .get_signal(&INVOCATION_1, "approval", 0)
            .await
            .unwrap(),
        Some(value("first"))
    );
    assert_eq!(
        store
            .get_signal(&INVOCATION_1, "approval", 1)
            .await
            .unwrap(),
        Some(SignalResult::Void)
    );
    assert_eq!(
        store
            .get_signal(&INVOCATION_1, "approval", 2)
            .await
            .unwrap(),
        None
    );

    // Dropping the invocation state leaves the signals.
    let mut txn = store.transaction();
    txn.delete_invocation_state(&INVOCATION_1, &b).unwrap();
    assert_eq!(
        txn.get_invocation_state(&INVOCATION_1, &b).await.unwrap(),
        None
    );
    txn.delete_all_invocation_state(&INVOCATION_1).unwrap();
    txn.commit().await.unwrap();
    assert!(
        store
            .get_all_invocation_state(&INVOCATION_1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .next_signal_index(&INVOCATION_1, "approval")
            .await
            .unwrap(),
        2
    );

    // Dropping everything of one invocation leaves the other's rows.
    let mut txn = store.transaction();
    txn.delete_all_explicit_recovery(&INVOCATION_1).unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        store
            .next_signal_index(&INVOCATION_1, "approval")
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .get_all_invocation_state(&INVOCATION_2)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        store
            .next_signal_index(&INVOCATION_2, "approval")
            .await
            .unwrap(),
        2
    );
}
