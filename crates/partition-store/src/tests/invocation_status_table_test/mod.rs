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

use super::storage_test_environment;

use crate::invocation_status_table::{InvocationStatusKey, InvocationStatusKeyV1};
use crate::partition_store::StorageAccess;
use bytestring::ByteString;
use futures_util::TryStreamExt;
use googletest::prelude::*;
use once_cell::sync::Lazy;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable, InvocationStatusV1,
    JournalMetadata, ReadOnlyInvocationStatusTable, StatusTimestamps,
};
use restate_storage_api::Transaction;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocationSpanContext, Source, VirtualObjectHandlerType,
};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;
use std::time::Duration;

const INVOCATION_TARGET_1: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("1"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_1: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_1));

const INVOCATION_TARGET_2: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("2"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_2: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_2));

const INVOCATION_TARGET_3: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("3"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_3: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_3));

const INVOCATION_TARGET_4: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("4"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_4: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_4));

const INVOCATION_TARGET_5: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("5"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_5: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_5));

fn invoked_status(invocation_target: InvocationTarget) -> InvocationStatus {
    InvocationStatus::Invoked(InFlightInvocationMetadata {
        invocation_target,
        journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
        pinned_deployment: None,
        response_sinks: HashSet::new(),
        timestamps: StatusTimestamps::init(MillisSinceEpoch::new(0)),
        source: Source::Ingress,
        completion_retention_duration: Duration::ZERO,
        idempotency_key: None,
    })
}

fn suspended_status(invocation_target: InvocationTarget) -> InvocationStatus {
    InvocationStatus::Suspended {
        metadata: InFlightInvocationMetadata {
            invocation_target,
            journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
            pinned_deployment: None,
            response_sinks: HashSet::new(),
            timestamps: StatusTimestamps::init(MillisSinceEpoch::new(0)),
            source: Source::Ingress,
            completion_retention_duration: Duration::ZERO,
            idempotency_key: None,
        },
        waiting_for_completed_entries: HashSet::default(),
    }
}

async fn populate_data<T: InvocationStatusTable>(txn: &mut T) {
    txn.put_invocation_status(
        &INVOCATION_ID_1,
        &invoked_status(INVOCATION_TARGET_1.clone()),
    )
    .await;

    txn.put_invocation_status(
        &INVOCATION_ID_2,
        &invoked_status(INVOCATION_TARGET_2.clone()),
    )
    .await;

    txn.put_invocation_status(
        &INVOCATION_ID_3,
        &suspended_status(INVOCATION_TARGET_3.clone()),
    )
    .await;

    txn.put_invocation_status(
        &INVOCATION_ID_4,
        &invoked_status(INVOCATION_TARGET_4.clone()),
    )
    .await;

    txn.put_invocation_status(
        &INVOCATION_ID_5,
        &suspended_status(INVOCATION_TARGET_5.clone()),
    )
    .await;
}

async fn verify_point_lookups<T: InvocationStatusTable>(txn: &mut T) {
    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_1)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_1.clone())
    );

    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_4)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_4.clone())
    );
}

async fn verify_all_svc_with_status_invoked<T: InvocationStatusTable>(txn: &mut T) {
    let actual = txn
        .all_invoked_invocations()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_that!(
        actual,
        unordered_elements_are![
            eq((*INVOCATION_ID_1, INVOCATION_TARGET_1.clone())),
            eq((*INVOCATION_ID_2, INVOCATION_TARGET_2.clone())),
            eq((*INVOCATION_ID_4, INVOCATION_TARGET_4.clone()))
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_invocation_status() {
    let mut rocksdb = storage_test_environment().await;
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
    verify_all_svc_with_status_invoked(&mut txn).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration() {
    let mut rocksdb = storage_test_environment().await;

    let invocation_id = InvocationId::mock_random();
    let status = InvocationStatus::Invoked(InFlightInvocationMetadata::mock());

    // Let's mock the old invocation status
    let mut txn = rocksdb.transaction();
    txn.put_kv(
        InvocationStatusKeyV1::default()
            .partition_key(invocation_id.partition_key())
            .invocation_uuid(invocation_id.invocation_uuid()),
        &InvocationStatusV1(status.clone()),
    );
    txn.commit().await.unwrap();

    // Make sure we can read without mutating
    assert_eq!(
        status,
        rocksdb.get_invocation_status(&invocation_id).await.unwrap()
    );

    // Now reading should perform the migration,
    // and result should be equal to the first inserted status
    let mut txn = rocksdb.transaction();
    assert_eq!(
        status,
        txn.get_invocation_status(&invocation_id).await.unwrap()
    );
    txn.commit().await.unwrap();

    // Let's check migration was done
    assert!(rocksdb
        .get_kv_raw(
            InvocationStatusKeyV1::default()
                .partition_key(invocation_id.partition_key())
                .invocation_uuid(invocation_id.invocation_uuid()),
            |_, v| Ok(v.is_none())
        )
        .unwrap());
    assert!(rocksdb
        .get_kv_raw(
            InvocationStatusKey::default()
                .partition_key(invocation_id.partition_key())
                .invocation_uuid(invocation_id.invocation_uuid()),
            |_, v| Ok(v.is_some())
        )
        .unwrap());

    // Make sure we can read without mutating V2
    assert_eq!(
        status,
        rocksdb.get_invocation_status(&invocation_id).await.unwrap()
    );
}
