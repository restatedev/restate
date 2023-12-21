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
use restate_storage_api::GetStream;
use restate_types::identifiers::{FullInvocationId, InvocationUuid, ServiceId};
use restate_types::invocation::{ServiceInvocation, Source, SpanRelation};
use std::fmt::Debug;
use std::str::FromStr;
use tempfile::tempdir;
use tokio_stream::StreamExt;
use uuid::Uuid;

mod inbox_table_test;
mod journal_table_test;
mod outbox_table_test;
mod state_table_test;
mod status_table_test;
mod timer_table_test;

#[tokio::test]
async fn test_read_write() -> anyhow::Result<()> {
    //
    // create a rocksdb storage from options
    //
    let temp_dir = tempdir().unwrap();
    let path = temp_dir
        .path()
        .to_str()
        .expect("can not convert a path to string")
        .to_string();

    let opts = restate_storage_rocksdb::Options {
        path,
        ..Default::default()
    };
    let (rocksdb, writer) = opts
        .build()
        .expect("RocksDB storage creation should succeed");

    let (signal, watch) = drain::channel();
    let writer_join_handle = writer.run(watch);

    //
    // run the tests
    //
    inbox_table_test::run_tests(rocksdb.clone()).await;
    journal_table_test::run_tests(rocksdb.clone()).await;
    outbox_table_test::run_tests(rocksdb.clone()).await;
    state_table_test::run_tests(rocksdb.clone()).await;
    status_table_test::run_tests(rocksdb.clone()).await;
    timer_table_test::run_tests(rocksdb).await;

    signal.drain().await;
    writer_join_handle.await??;
    Ok(())
}

pub(crate) fn uuid_str(uuid: &str) -> Uuid {
    Uuid::from_str(uuid).expect("")
}

pub(crate) fn mock_service_invocation(service_id: ServiceId) -> ServiceInvocation {
    ServiceInvocation::new(
        FullInvocationId::with_service_id(service_id, InvocationUuid::now_v7()),
        ByteString::from_static("service"),
        Bytes::new(),
        Source::Ingress,
        None,
        SpanRelation::None,
    )
}

pub(crate) fn mock_random_service_invocation() -> ServiceInvocation {
    ServiceInvocation::new(
        FullInvocationId::mock_random(),
        ByteString::from_static("service"),
        Bytes::new(),
        Source::Ingress,
        None,
        SpanRelation::None,
    )
}

pub(crate) async fn assert_stream_eq<T: Send + Debug + PartialEq + 'static>(
    mut actual: GetStream<'_, T>,
    expected: Vec<T>,
) {
    let mut items = expected.into_iter();

    while let Some(item) = actual.next().await {
        let got = item.expect("Fail result");
        let expected = items.next();

        assert_eq!(Some(got), expected);
    }

    assert_eq!(None, items.next());
}
