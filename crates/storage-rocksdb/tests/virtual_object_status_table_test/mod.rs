// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::service_status_table::{VirtualObjectStatus, VirtualObjectStatusTable};
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{InvocationId, InvocationUuid, ServiceId};

const FIXTURE_INVOCATION: InvocationUuid =
    InvocationUuid::from_parts(1706027034946, 12345678900001);

async fn populate_data<T: VirtualObjectStatusTable>(txn: &mut T) {
    txn.put_virtual_object_status(
        &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
        VirtualObjectStatus::Locked(InvocationId::from_parts(1337, FIXTURE_INVOCATION)),
    )
    .await;

    txn.put_virtual_object_status(
        &ServiceId::with_partition_key(1337, "svc-1", "key-2"),
        VirtualObjectStatus::Locked(InvocationId::from_parts(1337, FIXTURE_INVOCATION)),
    )
    .await;
}

async fn verify_point_lookups<T: VirtualObjectStatusTable>(txn: &mut T) {
    let status = txn
        .get_virtual_object_status(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .await
        .expect("should not fail");

    assert_eq!(
        status,
        VirtualObjectStatus::Locked(InvocationId::from_parts(1337, FIXTURE_INVOCATION))
    );
}

pub(crate) async fn run_tests(mut rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;

    verify_point_lookups(&mut txn).await;
}
