// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use std::ops::ControlFlow;
use std::sync::LazyLock;
use std::time::Duration;

use bytestring::ByteString;

use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, JournalMetadata, ReadInvocationStatusTable,
    ScanInvocationStatusTable, StatusTimestamps, WriteInvocationStatusTable,
};
use restate_storage_api::{StorageError, Transaction};
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use restate_types::invocation::{
    InvocationTarget, ServiceInvocationSpanContext, Source, VirtualObjectHandlerType,
};
use restate_types::journal_v2::UnresolvedFuture;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::Seq;
use restate_types::{LimitKey, RestateVersion};

const INVOCATION_TARGET_1: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("1"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
    scope: None,
};
static INVOCATION_ID_1: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_1));

const INVOCATION_TARGET_2: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("2"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
    scope: None,
};
static INVOCATION_ID_2: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_2));

const INVOCATION_TARGET_3: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("3"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
    scope: None,
};
static INVOCATION_ID_3: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_3));

const INVOCATION_TARGET_4: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("4"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
    scope: None,
};
static INVOCATION_ID_4: LazyLock<InvocationId> =
    LazyLock::new(|| InvocationId::mock_generate(&INVOCATION_TARGET_4));

static RPC_REQUEST_ID: LazyLock<PartitionProcessorRpcRequestId> =
    LazyLock::new(PartitionProcessorRpcRequestId::new);

fn invoked_status(invocation_target: InvocationTarget) -> InvocationStatus {
    InvocationStatus::Invoked(InFlightInvocationMetadata {
        invocation_target,
        created_using_restate_version: RestateVersion::current(),
        vqueue_id: None,
        limit_key: LimitKey::None,
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
        random_seed: None,
        creation_seq: Some(Seq::new(42)),
        explicit_recovery: None,
    })
}

fn suspended_status(invocation_target: InvocationTarget) -> InvocationStatus {
    InvocationStatus::Suspended {
        metadata: InFlightInvocationMetadata {
            invocation_target,
            vqueue_id: None,
            limit_key: LimitKey::None,
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
            random_seed: None,
            creation_seq: Some(Seq::new(42)),
            explicit_recovery: None,
        },
        awaiting_on: UnresolvedFuture::Unknown(Vec::default()),
    }
}

async fn populate_data<T: WriteInvocationStatusTable>(txn: &mut T) {
    txn.put_invocation_status(&INVOCATION_ID_1, &invoked_status(INVOCATION_TARGET_1))
        .unwrap();

    txn.put_invocation_status(&INVOCATION_ID_2, &invoked_status(INVOCATION_TARGET_2))
        .expect("");

    txn.put_invocation_status(&INVOCATION_ID_3, &suspended_status(INVOCATION_TARGET_3))
        .unwrap();

    txn.put_invocation_status(&INVOCATION_ID_4, &suspended_status(INVOCATION_TARGET_4))
        .unwrap();
}

async fn verify_point_lookups<T: ReadInvocationStatusTable>(txn: &mut T) {
    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_1)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_1)
    );
}

#[restate_core::test]
async fn invocation_status() {
    let mut rocksdb = storage_test_environment().await;
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_1)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_1)
    );
    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_2)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_2)
    );
}

#[restate_core::test]
async fn fold_invocation_status_lazy() {
    let mut rocksdb = storage_test_environment().await;
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;
    txn.commit().await.expect("should not fail");

    // Collects the visited ids, stopping once `limit` are collected.
    let collect = |from: Option<InvocationId>, limit: usize| {
        rocksdb
            .fold_invocation_status_lazy(from, Vec::new(), move |ids, (id, _)| {
                ids.push(id);
                if ids.len() == limit {
                    ControlFlow::Break(Ok::<_, anyhow::Error>(()))
                } else {
                    ControlFlow::Continue(())
                }
            })
            .expect("should not fail")
    };

    // A full scan visits every status in key order.
    let all = collect(None, usize::MAX).await.expect("should not fail");
    let mut expected = vec![
        *INVOCATION_ID_1,
        *INVOCATION_ID_2,
        *INVOCATION_ID_3,
        *INVOCATION_ID_4,
    ];
    expected.sort();
    assert_eq!(all, expected);

    // Stopping early returns the state accumulated so far.
    assert_eq!(collect(None, 2).await.expect("should not fail"), all[..2]);

    // `from` is inclusive.
    assert_eq!(
        collect(Some(all[1]), usize::MAX)
            .await
            .expect("should not fail"),
        all[1..]
    );

    // An error from the closure surfaces as a conversion error.
    let err = rocksdb
        .fold_invocation_status_lazy(None, (), |_, _| {
            ControlFlow::Break(Err(anyhow::anyhow!("boom")))
        })
        .expect("should not fail")
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Conversion(_)), "{err:?}");
}
