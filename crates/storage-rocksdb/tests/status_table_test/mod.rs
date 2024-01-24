// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::assert_stream_eq;
use restate_storage_api::status_table::{
    InvocationMetadata, InvocationStatus, JournalMetadata, StatusTable, StatusTimestamps,
};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{FullInvocationId, InvocationUuid, ServiceId};
use restate_types::invocation::{ServiceInvocationSpanContext, Source};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;

const FIXTURE_INVOCATION: InvocationUuid =
    InvocationUuid::from_parts(1706027034946, 12345678900001);

fn invoked_status(invocation_id: impl Into<InvocationUuid>) -> InvocationStatus {
    InvocationStatus::Invoked(InvocationMetadata::new(
        invocation_id.into(),
        JournalMetadata::new(0, ServiceInvocationSpanContext::empty()),
        None,
        "service".into(),
        None,
        StatusTimestamps::new(MillisSinceEpoch::new(0), MillisSinceEpoch::new(0)),
        Source::Ingress,
    ))
}

fn suspended_status(invocation_id: impl Into<InvocationUuid>) -> InvocationStatus {
    InvocationStatus::Suspended {
        metadata: InvocationMetadata::new(
            invocation_id.into(),
            JournalMetadata::new(0, ServiceInvocationSpanContext::empty()),
            None,
            "service".into(),
            None,
            StatusTimestamps::new(MillisSinceEpoch::new(0), MillisSinceEpoch::new(0)),
            Source::Ingress,
        ),
        waiting_for_completed_entries: HashSet::default(),
    }
}

async fn populate_data<T: StatusTable>(txn: &mut T) {
    txn.put_invocation_status(
        &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
        invoked_status(FIXTURE_INVOCATION),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
        invoked_status(FIXTURE_INVOCATION),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(1338, "svc-1", "key-2"),
        invoked_status(FIXTURE_INVOCATION),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(1339, "svc-2", "key-0"),
        invoked_status(FIXTURE_INVOCATION),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(1339, "svc-2", "key-1"),
        suspended_status(FIXTURE_INVOCATION),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(u64::MAX, "svc-u64", "key-0"),
        invoked_status(FIXTURE_INVOCATION),
    )
    .await;
}

async fn verify_point_lookups<T: StatusTable>(txn: &mut T) {
    let status = txn
        .get_invocation_status(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .await
        .expect("should not fail");

    assert_eq!(status, Some(invoked_status(FIXTURE_INVOCATION)));
}

async fn verify_all_svc_with_status_invoked<T: StatusTable>(txn: &mut T) {
    let stream = txn.invoked_invocations(1337..=1339);

    let expected = vec![
        FullInvocationId::with_service_id(
            ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            FIXTURE_INVOCATION,
        ),
        FullInvocationId::with_service_id(
            ServiceId::with_partition_key(1337, "svc-1", "key-2"),
            FIXTURE_INVOCATION,
        ),
        FullInvocationId::with_service_id(
            ServiceId::with_partition_key(1338, "svc-1", "key-2"),
            FIXTURE_INVOCATION,
        ),
        FullInvocationId::with_service_id(
            ServiceId::with_partition_key(1339, "svc-2", "key-0"),
            FIXTURE_INVOCATION,
        ),
    ];

    assert_stream_eq(stream, expected).await;
}

async fn verify_lookup_by_invocation_id<T: StatusTable>(txn: &mut T) {
    let result = txn
        .get_invocation_status_from(1337, FIXTURE_INVOCATION)
        .await
        .expect("should not fail");

    let (id, status) = result.expect("the invocation should be present");

    assert_eq!(ServiceId::with_partition_key(1337, "svc-1", "key-1"), id);

    assert_eq!(status, invoked_status(FIXTURE_INVOCATION));
}

async fn verify_lookup_by_invocation_id_not_found<T: StatusTable>(txn: &mut T) {
    let result = txn
        .get_invocation_status_from(1337, InvocationUuid::new())
        .await
        .expect("should not fail");

    assert_eq!(result, None);
}

async fn verify_last_partition_all_svc_with_status_invoked<T: StatusTable>(txn: &mut T) {
    let stream = txn.invoked_invocations(4000..=u64::MAX);
    let expected = vec![FullInvocationId::with_service_id(
        ServiceId::with_partition_key(u64::MAX, "svc-u64", "key-0"),
        FIXTURE_INVOCATION,
    )];

    assert_stream_eq(stream, expected).await;
}

pub(crate) async fn run_tests(mut rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
    verify_lookup_by_invocation_id(&mut txn).await;
    verify_lookup_by_invocation_id_not_found(&mut txn).await;
    verify_all_svc_with_status_invoked(&mut txn).await;
    verify_last_partition_all_svc_with_status_invoked(&mut txn).await;
}
