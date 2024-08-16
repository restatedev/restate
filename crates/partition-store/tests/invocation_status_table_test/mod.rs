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

use super::{assert_stream_eq, storage_test_environment};

use bytestring::ByteString;
use once_cell::sync::Lazy;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable, JournalMetadata,
    SourceTable, StatusTimestamps,
};
use restate_types::identifiers::InvocationId;
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
    Lazy::new(|| InvocationId::generate(&INVOCATION_TARGET_1));

const INVOCATION_TARGET_2: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("2"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_2: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::generate(&INVOCATION_TARGET_2));

const INVOCATION_TARGET_3: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("3"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_3: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::generate(&INVOCATION_TARGET_3));

const INVOCATION_TARGET_4: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("4"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_4: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::generate(&INVOCATION_TARGET_4));

const INVOCATION_TARGET_5: InvocationTarget = InvocationTarget::VirtualObject {
    name: ByteString::from_static("abc"),
    key: ByteString::from_static("5"),
    handler: ByteString::from_static("myhandler"),
    handler_ty: VirtualObjectHandlerType::Exclusive,
};
static INVOCATION_ID_5: Lazy<InvocationId> =
    Lazy::new(|| InvocationId::generate(&INVOCATION_TARGET_5));

fn invoked_status(
    invocation_target: InvocationTarget,
    source_table: SourceTable,
) -> InvocationStatus {
    InvocationStatus::Invoked(InFlightInvocationMetadata {
        invocation_target,
        journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
        pinned_deployment: None,
        response_sinks: HashSet::new(),
        timestamps: StatusTimestamps::new(MillisSinceEpoch::new(0), MillisSinceEpoch::new(0)),
        source: Source::Ingress,
        completion_retention_time: Duration::ZERO,
        idempotency_key: None,
        source_table,
    })
}

fn suspended_status(
    invocation_target: InvocationTarget,
    source_table: SourceTable,
) -> InvocationStatus {
    InvocationStatus::Suspended {
        metadata: InFlightInvocationMetadata {
            invocation_target,
            journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
            pinned_deployment: None,
            response_sinks: HashSet::new(),
            timestamps: StatusTimestamps::new(MillisSinceEpoch::new(0), MillisSinceEpoch::new(0)),
            source: Source::Ingress,
            completion_retention_time: Duration::ZERO,
            idempotency_key: None,
            source_table,
        },
        waiting_for_completed_entries: HashSet::default(),
    }
}

async fn populate_data<T: InvocationStatusTable>(txn: &mut T) {
    txn.put_invocation_status(
        &INVOCATION_ID_1,
        invoked_status(INVOCATION_TARGET_1.clone(), SourceTable::Old),
    )
    .await;

    txn.put_invocation_status(
        &INVOCATION_ID_2,
        invoked_status(INVOCATION_TARGET_2.clone(), SourceTable::Old),
    )
    .await;

    txn.put_invocation_status(
        &INVOCATION_ID_3,
        suspended_status(INVOCATION_TARGET_3.clone(), SourceTable::Old),
    )
    .await;

    txn.put_invocation_status(
        &INVOCATION_ID_4,
        invoked_status(INVOCATION_TARGET_4.clone(), SourceTable::New),
    )
    .await;

    txn.put_invocation_status(
        &INVOCATION_ID_5,
        suspended_status(INVOCATION_TARGET_5.clone(), SourceTable::New),
    )
    .await;
}

async fn verify_point_lookups<T: InvocationStatusTable>(txn: &mut T) {
    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_1)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_1.clone(), SourceTable::Old)
    );

    assert_eq!(
        txn.get_invocation_status(&INVOCATION_ID_4)
            .await
            .expect("should not fail"),
        invoked_status(INVOCATION_TARGET_4.clone(), SourceTable::New)
    );
}

async fn verify_all_svc_with_status_invoked<T: InvocationStatusTable>(txn: &mut T) {
    let stream = txn.invoked_invocations(0..=u64::MAX);

    let expected = vec![
        (*INVOCATION_ID_1, INVOCATION_TARGET_1.clone()),
        (*INVOCATION_ID_2, INVOCATION_TARGET_2.clone()),
        (*INVOCATION_ID_4, INVOCATION_TARGET_4.clone()),
    ];

    assert_stream_eq(stream, expected).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_invocation_status() {
    let mut rocksdb = storage_test_environment().await;
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
    verify_all_svc_with_status_invoked(&mut txn).await;
}
