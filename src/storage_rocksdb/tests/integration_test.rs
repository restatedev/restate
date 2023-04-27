use bytes::Bytes;
use bytestring::ByteString;
use restate_common::types::{ServiceInvocation, ServiceInvocationId, SpanRelation};
use restate_storage_api::GetStream;
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
async fn test_read_write() {
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
    let rocksdb = opts.build();

    //
    // run the tests
    //
    inbox_table_test::run_tests(rocksdb.clone()).await;
    journal_table_test::run_tests(rocksdb.clone()).await;
    outbox_table_test::run_tests(rocksdb.clone()).await;
    state_table_test::run_tests(rocksdb.clone()).await;
    status_table_test::run_tests(rocksdb.clone()).await;
    timer_table_test::run_tests(rocksdb).await;
}

pub(crate) fn uuid_str(uuid: &str) -> Uuid {
    Uuid::from_str(uuid).expect("")
}

pub(crate) fn mock_service_invocation() -> ServiceInvocation {
    let (service_invocation, _) = ServiceInvocation::new(
        ServiceInvocationId::new(
            ByteString::from_static("service"),
            Bytes::new(),
            uuid_str("018756fa-3f7f-7854-a76b-42c59a3d7f2d"),
        ),
        ByteString::from_static("service"),
        Bytes::new(),
        None,
        SpanRelation::None,
    );

    service_invocation
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
