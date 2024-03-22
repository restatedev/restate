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
use bytestring::ByteString;
use futures::Stream;
use restate_storage_api::StorageError;
use restate_storage_rocksdb::RocksDBStorage;
use restate_types::identifiers::{FullInvocationId, ServiceId};
use restate_types::invocation::{ServiceInvocation, Source, SpanRelation};
use restate_types::state_mut::ExternalStateMutation;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::pin;
use tempfile::tempdir;
use tokio_stream::StreamExt;

mod inbox_table_test;
mod invocation_status_table_test;
mod journal_table_test;
mod outbox_table_test;
mod state_table_test;
mod timer_table_test;
mod virtual_object_status_table_test;

fn storage_test_environment() -> (RocksDBStorage, impl Future<Output = ()>) {
    //
    // create a rocksdb storage from options
    //
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.into_path();

    let opts = restate_storage_rocksdb::Options {
        path,
        ..Default::default()
    };
    let (rocksdb, writer) = opts
        .build()
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
    let (rocksdb, close) = storage_test_environment();

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
    ServiceInvocation::new(
        FullInvocationId::generate(service_id),
        ByteString::from_static("service"),
        Bytes::new(),
        Source::Ingress,
        None,
        SpanRelation::None,
        vec![],
    )
}

pub(crate) fn mock_state_mutation(service_id: ServiceId) -> ExternalStateMutation {
    ExternalStateMutation {
        component_id: service_id,
        version: None,
        state: HashMap::default(),
    }
}

pub(crate) fn mock_random_service_invocation() -> ServiceInvocation {
    ServiceInvocation::new(
        FullInvocationId::mock_random(),
        ByteString::from_static("service"),
        Bytes::new(),
        Source::Ingress,
        None,
        SpanRelation::None,
        vec![],
    )
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
