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

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Duration;

use bytestring::ByteString;
use futures_util::TryStreamExt;
use googletest::prelude::*;

use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
    InvokedOrKilledInvocationStatusLite, JournalMetadata, ReadOnlyInvocationStatusTable,
    StatusTimestamps,
};
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId, WithPartitionKey};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocationSpanContext, Source, VirtualObjectHandlerType,
};
use restate_types::time::MillisSinceEpoch;

use super::storage_test_environment;
use crate::invocation_status_table::{
    InvocationStatusKey, InvocationStatusKeyV1, InvocationStatusV1,
};
use crate::partition_store::StorageAccess;

const INVOCATION_TARGET_1: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("1"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_1: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_1));

const INVOCATION_TARGET_2: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("2"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_2: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_2));

const INVOCATION_TARGET_3: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("3"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_3: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_3));

const INVOCATION_TARGET_4: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("4"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_4: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_4));

const INVOCATION_TARGET_5: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("5"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_5: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_5));

static RPC_REQUEST_ID: LazyLock<PartitionProcessorRpcRequestId> =
    LazyLock::new(PartitionProcessorRpcRequestId::new);

fn invoked_status(invocation_target: InvocationTarget) -> InvocationStatus {
    InvocationStatus::Invoked(InFlightInvocationMetadata {
        invocation_target,
        journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
        pinned_deployment: None,
        response_sinks: HashSet::new(),
        timestamps: StatusTimestamps::init(MillisSinceEpoch::new(0)),
        source: Source::Ingress(*RPC_REQUEST_ID),
        completion_retention_duration: Duration::ZERO,
        idempotency_key: None,
        hotfix_apply_cancellation_after_deployment_is_pinned: false,
    })
}

fn killed_status(invocation_target: InvocationTarget) -> InvocationStatus {
    InvocationStatus::Killed(InFlightInvocationMetadata {
        invocation_target,
        journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
        pinned_deployment: None,
        response_sinks: HashSet::new(),
        timestamps: StatusTimestamps::init(MillisSinceEpoch::new(0)),
        source: Source::Ingress(*RPC_REQUEST_ID),
        completion_retention_duration: Duration::ZERO,
        idempotency_key: None,
        hotfix_apply_cancellation_after_deployment_is_pinned: false,
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
            source: Source::Ingress(*RPC_REQUEST_ID),
            completion_retention_duration: Duration::ZERO,
            idempotency_key: None,
            hotfix_apply_cancellation_after_deployment_is_pinned: false,
        },
        waiting_for_notifications: HashSet::default(),
    }
}

async fn populate_data<T: InvocationStatusTable>(txn: &mut T) {
    txn.put_invocation_status(
        &INVOCATION_ID_1,
        &invoked_status(INVOCATION_TARGET_1.clone()),
    )
    .await
    .unwrap();

    txn.put_invocation_status(
        &INVOCATION_ID_2,
        &invoked_status(INVOCATION_TARGET_2.clone()),
    )
    .await
    .expect("");

    txn.put_invocation_status(
        &INVOCATION_ID_3,
        &suspended_status(INVOCATION_TARGET_3.clone()),
    )
    .await
    .unwrap();

    txn.put_invocation_status(
        &INVOCATION_ID_4,
        &killed_status(INVOCATION_TARGET_4.clone()),
    )
    .await
    .unwrap();

    txn.put_invocation_status(
        &INVOCATION_ID_5,
        &suspended_status(INVOCATION_TARGET_5.clone()),
    )
    .await
    .unwrap();
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
        killed_status(INVOCATION_TARGET_4.clone())
    );
}

async fn verify_all_svc_with_status_invoked_or_killed<T: InvocationStatusTable>(txn: &mut T) {
    let actual = txn
        .all_invoked_or_killed_invocations()
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_that!(
        actual,
        unordered_elements_are![
            eq(InvokedOrKilledInvocationStatusLite {
                invocation_id: *INVOCATION_ID_1,
                invocation_target: INVOCATION_TARGET_1.clone(),
                is_invoked: true,
            }),
            eq(InvokedOrKilledInvocationStatusLite {
                invocation_id: *INVOCATION_ID_2,
                invocation_target: INVOCATION_TARGET_2.clone(),
                is_invoked: true,
            }),
            eq(InvokedOrKilledInvocationStatusLite {
                invocation_id: *INVOCATION_ID_4,
                invocation_target: INVOCATION_TARGET_4.clone(),
                is_invoked: false,
            }),
        ]
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_invocation_status() {
    let mut rocksdb = storage_test_environment().await;
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
    verify_all_svc_with_status_invoked_or_killed(&mut txn).await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
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
    )
    .unwrap();
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
    assert!(
        rocksdb
            .get_kv_raw(
                InvocationStatusKeyV1::default()
                    .partition_key(invocation_id.partition_key())
                    .invocation_uuid(invocation_id.invocation_uuid()),
                |_, v| Ok(v.is_none())
            )
            .unwrap()
    );
    assert!(
        rocksdb
            .get_kv_raw(
                InvocationStatusKey::default()
                    .partition_key(invocation_id.partition_key())
                    .invocation_uuid(invocation_id.invocation_uuid()),
                |_, v| Ok(v.is_some())
            )
            .unwrap()
    );

    // Make sure we can read without mutating V2
    assert_eq!(
        status,
        rocksdb.get_invocation_status(&invocation_id).await.unwrap()
    );
}
