// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::pin::pin;

use futures::Stream;
use tokio_stream::StreamExt;

use restate_core::TaskCenterBuilder;
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::StorageError;
use restate_types::config::{CommonOptions, WorkerOptions};
use restate_types::identifiers::{InvocationId, PartitionId, PartitionKey, ServiceId};
use restate_types::invocation::{InvocationTarget, ServiceInvocation, Source};
use restate_types::live::Constant;
use restate_types::state_mut::ExternalStateMutation;

mod idempotency_table_test;
mod inbox_table_test;
mod invocation_status_table_test;
mod journal_table_test;
mod outbox_table_test;
mod promise_table_test;
mod state_table_test;
mod timer_table_test;
mod virtual_object_status_table_test;

async fn storage_test_environment() -> PartitionStore {
    //
    // create a rocksdb storage from options
    //
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    tc.run_in_scope_sync("db-manager-init", None, || {
        RocksDbManager::init(Constant::new(CommonOptions::default()))
    });
    let worker_options = WorkerOptions::default();
    let manager = PartitionStoreManager::create(
        Constant::new(worker_options.storage.clone()),
        Constant::new(worker_options.storage.rocksdb.clone()),
        &[],
    )
    .await
    .expect("DB storage creation succeeds");
    // A single partition store that spans all keys.
    manager
        .open_partition_store(
            PartitionId::MIN,
            RangeInclusive::new(0, PartitionKey::MAX - 1),
            OpenMode::CreateIfMissing,
            &worker_options.storage.rocksdb,
        )
        .await
        .expect("DB storage creation succeeds")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_write() {
    let rocksdb = storage_test_environment().await;

    //
    // run the tests
    //
    inbox_table_test::run_tests(rocksdb.clone()).await;
    outbox_table_test::run_tests(rocksdb.clone()).await;
    state_table_test::run_tests(rocksdb.clone()).await;
    invocation_status_table_test::run_tests(rocksdb.clone()).await;
    virtual_object_status_table_test::run_tests(rocksdb.clone()).await;
    timer_table_test::run_tests(rocksdb).await;
}

pub(crate) fn mock_service_invocation(service_id: ServiceId) -> ServiceInvocation {
    let invocation_target = InvocationTarget::mock_from_service_id(service_id);
    ServiceInvocation {
        invocation_id: InvocationId::generate(&invocation_target),
        invocation_target,
        argument: Default::default(),
        source: Source::Ingress,
        response_sink: None,
        span_context: Default::default(),
        headers: vec![],
        execution_time: None,
        completion_retention_time: None,
        idempotency_key: None,
        submit_notification_sink: None,
    }
}

pub(crate) fn mock_random_service_invocation() -> ServiceInvocation {
    mock_service_invocation(ServiceId::mock_random())
}

pub(crate) fn mock_state_mutation(service_id: ServiceId) -> ExternalStateMutation {
    ExternalStateMutation {
        service_id,
        version: None,
        state: HashMap::default(),
    }
}

pub(crate) async fn assert_stream_eq<T: Send + Debug + PartialEq + 'static>(
    actual: impl Stream<Item = Result<T, StorageError>>,
    expected: Vec<T>,
) {
    let mut actual = pin!(actual);
    let mut items = expected.into_iter();

    while let Some(item) = actual.next().await {
        let got = item.expect("Fail result");
        let expected = items.next();

        assert_eq!(Some(got), expected);
    }

    assert_eq!(None, items.next());
}
