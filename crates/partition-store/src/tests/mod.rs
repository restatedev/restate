// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_core::TestCoreEnv;
use restate_types::partitions::Partition;
use tokio_stream::StreamExt;

use crate::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::StorageError;
use restate_types::config::CommonOptions;
use restate_types::identifiers::{
    InvocationId, PartitionId, PartitionKey, PartitionProcessorRpcRequestId, ServiceId,
};
use restate_types::invocation::{InvocationTarget, ServiceInvocation, Source};
use restate_types::live::Constant;
use restate_types::state_mut::ExternalStateMutation;

mod barrier_test;
mod durable_lsn_tracking_test;
mod idempotency_table_test;
mod inbox_table_test;
mod invocation_status_table_test;
mod journal_table_test;
mod journal_table_v2_test;
mod outbox_table_test;
mod promise_table_test;
mod snapshots_test;
mod state_table_test;
mod timer_table_test;
mod virtual_object_status_table_test;

async fn storage_test_environment() -> PartitionStore {
    storage_test_environment_with_manager().await.1
}

async fn storage_test_environment_with_manager() -> (PartitionStoreManager, PartitionStore) {
    //
    // create a rocksdb storage from options
    //
    RocksDbManager::init(Constant::new(CommonOptions::default()));
    let manager = PartitionStoreManager::create()
        .await
        .expect("DB storage creation succeeds");
    // A single partition store that spans all keys.
    let store = manager
        .open_local_partition_store(
            &Partition::new(
                PartitionId::MIN,
                RangeInclusive::new(0, PartitionKey::MAX - 1),
            ),
            OpenMode::CreateIfMissing,
        )
        .await
        .expect("DB storage creation succeeds");

    (manager, store)
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_read_write() {
    let _env = TestCoreEnv::create_with_single_node(1, 1).await;

    let (manager, store) = storage_test_environment_with_manager().await;

    inbox_table_test::run_tests(store.clone()).await;
    outbox_table_test::run_tests(store.clone()).await;
    state_table_test::run_tests(store.clone()).await;
    virtual_object_status_table_test::run_tests(store.clone()).await;
    timer_table_test::run_tests(store.clone()).await;

    snapshots_test::run_tests(manager.clone(), store.clone()).await;
}

pub(crate) fn mock_service_invocation(service_id: ServiceId) -> Box<ServiceInvocation> {
    let invocation_target = InvocationTarget::mock_from_service_id(service_id);
    Box::new(ServiceInvocation::initialize(
        InvocationId::mock_generate(&invocation_target),
        invocation_target,
        Source::Ingress(PartitionProcessorRpcRequestId::new()),
    ))
}

pub(crate) fn mock_random_service_invocation() -> Box<ServiceInvocation> {
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
