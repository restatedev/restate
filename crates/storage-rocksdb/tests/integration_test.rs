// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use futures::Stream;
use restate_core::TaskCenterBuilder;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::StorageError;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::arc_util::Constant;
use restate_types::config::{CommonOptions, WorkerOptions};
use restate_types::identifiers::{InvocationId, ServiceId};
use restate_types::invocation::{InvocationTarget, ServiceInvocation, Source, SpanRelation};
use restate_types::state_mut::ExternalStateMutation;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::pin;
use tokio_stream::StreamExt;

mod idempotency_table_test;
mod inbox_table_test;
mod invocation_status_table_test;
mod journal_table_test;
mod outbox_table_test;
mod state_table_test;
mod timer_table_test;
mod virtual_object_status_table_test;

async fn storage_test_environment() -> (RocksDBStorage, impl Future<Output = ()>) {
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
    let (rocksdb, writer) = RocksDBStorage::open(
        worker_options.data_dir(),
        Constant::new(worker_options.rocksdb),
    )
    .await
    .expect("RocksDB storage creation should succeed");

    let (signal, watch) = drain::channel();
    let writer_join_handle = writer.run(watch);

    (rocksdb, async {
        signal.drain().await;
        writer_join_handle.await.unwrap().unwrap();
    })
}

#[tokio::test]
async fn test_read_write() {
    let (rocksdb, close) = storage_test_environment().await;

    //
    // run the tests
    //
    inbox_table_test::run_tests(rocksdb.clone()).await;
    journal_table_test::run_tests(rocksdb.clone()).await;
    outbox_table_test::run_tests(rocksdb.clone()).await;
    state_table_test::run_tests(rocksdb.clone()).await;
    invocation_status_table_test::run_tests(rocksdb.clone()).await;
    virtual_object_status_table_test::run_tests(rocksdb.clone()).await;
    timer_table_test::run_tests(rocksdb).await;

    close.await;
}

pub(crate) fn mock_service_invocation(service_id: ServiceId) -> ServiceInvocation {
    let invocation_target = InvocationTarget::mock_from_service_id(service_id);
    ServiceInvocation::new(
        InvocationId::generate(&invocation_target),
        invocation_target,
        Bytes::new(),
        Source::Ingress,
        None,
        SpanRelation::None,
        vec![],
        None,
        None,
    )
}

pub(crate) fn mock_random_service_invocation() -> ServiceInvocation {
    mock_service_invocation(ServiceId::mock_random())
}

pub(crate) fn mock_state_mutation(service_id: ServiceId) -> ExternalStateMutation {
    ExternalStateMutation {
        component_id: service_id,
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
