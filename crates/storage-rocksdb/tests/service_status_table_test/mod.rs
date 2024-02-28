// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::service_status_table::{ServiceStatus, ServiceStatusTable};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{InvocationId, InvocationUuid, ServiceId};

const FIXTURE_INVOCATION: InvocationUuid =
    InvocationUuid::from_parts(1706027034946, 12345678900001);

async fn populate_data<T: ServiceStatusTable>(txn: &mut T) {
    txn.put_service_status(
        &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
        ServiceStatus::Locked(InvocationId::new(1337, FIXTURE_INVOCATION)),
    )
    .await;

    txn.put_service_status(
        &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
        ServiceStatus::Locked(InvocationId::new(1337, FIXTURE_INVOCATION)),
    )
    .await;
}

async fn verify_point_lookups<T: ServiceStatusTable>(txn: &mut T) {
    let status = txn
        .get_service_status(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .await
        .expect("should not fail");

    assert_eq!(
        status,
        ServiceStatus::Locked(InvocationId::new(1337, FIXTURE_INVOCATION))
    );
}

pub(crate) async fn run_tests(mut rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
}
