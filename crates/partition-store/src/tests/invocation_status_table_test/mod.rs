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

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Duration;

use bytestring::ByteString;

use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::{
    CompletionRangeEpochMap, InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
    InvocationStatusV1, JournalMetadata, ReadOnlyInvocationStatusTable, StatusTimestamps,
};
use restate_types::RestateVersion;
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId, WithPartitionKey};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocationSpanContext, Source, VirtualObjectHandlerType,
};
use restate_types::time::MillisSinceEpoch;

use crate::fsm_table::get_last_executed_migration;
use crate::invocation_status_table::{InvocationStatusKey, InvocationStatusKeyV1};
use crate::migrations::{LATEST_MIGRATION, LastExecutedMigration};
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

static RPC_REQUEST_ID: LazyLock<PartitionProcessorRpcRequestId> =
    LazyLock::new(PartitionProcessorRpcRequestId::new);

fn invoked_status(invocation_target: InvocationTarget) -> InvocationStatus {
    InvocationStatus::Invoked(InFlightInvocationMetadata {
        invocation_target,
        created_using_restate_version: RestateVersion::current(),
        journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
        pinned_deployment: None,
        response_sinks: HashSet::new(),
        timestamps: StatusTimestamps::new(
            MillisSinceEpoch::new(0),
            MillisSinceEpoch::new(0),
            None,
            None,
            None,
            None,
        ),
        source: Source::Ingress(*RPC_REQUEST_ID),
        execution_time: None,
        completion_retention_duration: Duration::ZERO,
        journal_retention_duration: Duration::ZERO,
        idempotency_key: None,
        hotfix_apply_cancellation_after_deployment_is_pinned: false,
        current_invocation_epoch: 1,
        completion_range_epoch_map: CompletionRangeEpochMap::from_trim_points([(5, 1)]),
    })
}

fn suspended_status(invocation_target: InvocationTarget) -> InvocationStatus {
    InvocationStatus::Suspended {
        metadata: InFlightInvocationMetadata {
            invocation_target,
            created_using_restate_version: RestateVersion::current(),
            journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
            pinned_deployment: None,
            response_sinks: HashSet::new(),
            timestamps: StatusTimestamps::new(
                MillisSinceEpoch::new(0),
                MillisSinceEpoch::new(0),
                None,
                None,
                None,
                None,
            ),
            source: Source::Ingress(*RPC_REQUEST_ID),
            execution_time: None,
            completion_retention_duration: Duration::ZERO,
            journal_retention_duration: Duration::ZERO,
            idempotency_key: None,
            hotfix_apply_cancellation_after_deployment_is_pinned: false,
            current_invocation_epoch: 1,
            completion_range_epoch_map: CompletionRangeEpochMap::from_trim_points([(5, 1)]),
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
        &suspended_status(INVOCATION_TARGET_4.clone()),
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
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_invocation_status() {
    let mut rocksdb = storage_test_environment().await;
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_1)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_1.clone())
    );
    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_2)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_2.clone())
    );
}

#[allow(unreachable_code)]
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_migration() {
    let mut rocksdb = storage_test_environment().await;

    let invocation_id = InvocationId::mock_random();
    let in_flight_invocation_status = InFlightInvocationMetadata {
        // Old data structure doesn't support created_using_restate_version
        created_using_restate_version: RestateVersion::unknown(),
        ..InFlightInvocationMetadata::mock()
    };
    let status = InvocationStatus::Invoked(in_flight_invocation_status);

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

    // Now run the migrations
    rocksdb.verify_and_run_migrations().await.unwrap();
    let partition_id = rocksdb.partition_id();
    assert_eq!(
        LastExecutedMigration::from(
            get_last_executed_migration(&mut rocksdb, partition_id)
                .await
                .unwrap()
        ),
        LATEST_MIGRATION
    );

    // --- From now on all the statuses should be migrated

    assert_eq!(
        status,
        rocksdb.get_invocation_status(&invocation_id).await.unwrap()
    );

    let mut txn = rocksdb.transaction();
    assert_eq!(
        status,
        txn.get_invocation_status(&invocation_id).await.unwrap()
    );
    txn.commit().await.unwrap();

    // Let's check nothing is left in the old key space
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
    // But invocation status is only in the new key space
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
}
