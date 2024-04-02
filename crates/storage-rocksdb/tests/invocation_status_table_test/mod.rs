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
use once_cell::sync::Lazy;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable, JournalMetadata,
    StatusTimestamps,
};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{
    FullInvocationId, InvocationId, InvocationUuid, ServiceId, WithPartitionKey,
};
use restate_types::invocation::{ServiceInvocationSpanContext, Source};
use restate_types::time::MillisSinceEpoch;
use std::collections::HashSet;
use std::time::Duration;

static SERVICE_ID_1: Lazy<ServiceId> = Lazy::new(|| ServiceId::new("abc", "1"));
static INVOCATION_ID_1: Lazy<InvocationId> = Lazy::new(|| {
    InvocationId::new(
        SERVICE_ID_1.partition_key(),
        InvocationUuid::from_parts(1706027034946, 12345678900001),
    )
});

static SERVICE_ID_2: Lazy<ServiceId> = Lazy::new(|| ServiceId::new("abc", "2"));
static INVOCATION_ID_2: Lazy<InvocationId> = Lazy::new(|| {
    InvocationId::new(
        SERVICE_ID_2.partition_key(),
        InvocationUuid::from_parts(1706027034946, 12345678900002),
    )
});

static SERVICE_ID_3: Lazy<ServiceId> = Lazy::new(|| ServiceId::new("abc", "3"));
static INVOCATION_ID_3: Lazy<InvocationId> = Lazy::new(|| {
    InvocationId::new(
        SERVICE_ID_3.partition_key(),
        InvocationUuid::from_parts(1706027034946, 12345678900003),
    )
});

fn invoked_status(service_id: impl Into<ServiceId>) -> InvocationStatus {
    InvocationStatus::Invoked(InFlightInvocationMetadata {
        service_id: service_id.into(),
        journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
        deployment_id: None,
        method: "service".into(),
        response_sinks: HashSet::new(),
        timestamps: StatusTimestamps::new(MillisSinceEpoch::new(0), MillisSinceEpoch::new(0)),
        source: Source::Ingress,
        completion_retention_time: Duration::ZERO,
        idempotency_key: None,
    })
}

fn suspended_status(service_id: impl Into<ServiceId>) -> InvocationStatus {
    InvocationStatus::Suspended {
        metadata: InFlightInvocationMetadata {
            service_id: service_id.into(),
            journal_metadata: JournalMetadata::initialize(ServiceInvocationSpanContext::empty()),
            deployment_id: None,
            method: "service".into(),
            response_sinks: HashSet::new(),
            timestamps: StatusTimestamps::new(MillisSinceEpoch::new(0), MillisSinceEpoch::new(0)),
            source: Source::Ingress,
            completion_retention_time: Duration::ZERO,
            idempotency_key: None,
        },
        waiting_for_completed_entries: HashSet::default(),
    }
}

async fn populate_data<T: InvocationStatusTable>(txn: &mut T) {
    txn.put_invocation_status(&INVOCATION_ID_1, invoked_status(SERVICE_ID_1.clone()))
        .await;

    txn.put_invocation_status(&INVOCATION_ID_2, invoked_status(SERVICE_ID_2.clone()))
        .await;

    txn.put_invocation_status(&INVOCATION_ID_3, suspended_status(SERVICE_ID_3.clone()))
        .await;
}

async fn verify_point_lookups<T: InvocationStatusTable>(txn: &mut T) {
    let status = txn
        .get_invocation_status(&INVOCATION_ID_1)
        .await
        .expect("should not fail");

    assert_eq!(status, invoked_status(SERVICE_ID_1.clone()));
}

async fn verify_all_svc_with_status_invoked<T: InvocationStatusTable>(txn: &mut T) {
    let stream = txn.invoked_invocations(0..=u64::MAX);

    let expected = vec![
        FullInvocationId::combine(SERVICE_ID_1.clone(), INVOCATION_ID_1.clone()),
        FullInvocationId::combine(SERVICE_ID_2.clone(), INVOCATION_ID_2.clone()),
    ];

    assert_stream_eq(stream, expected).await;
}

pub(crate) async fn run_tests(mut rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
    verify_all_svc_with_status_invoked(&mut txn).await;
}
