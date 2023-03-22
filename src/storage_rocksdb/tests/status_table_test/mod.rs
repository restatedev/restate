use crate::{assert_stream_eq, uuid_bytes, uuid_str};
use common::types::{ServiceId, ServiceInvocationId};
use storage_api::status_table::StatusTable;
use storage_api::{Storage, Transaction};
use storage_proto::storage::v1::invocation_status::{Invoked, Status, Suspended};
use storage_proto::storage::v1::InvocationStatus;
use storage_rocksdb::RocksDBStorage;

async fn populate_data<T: StatusTable>(txn: &mut T) {
    txn.put_invocation_status(
        1337,
        &ServiceId::new("svc-1", "key-1"),
        InvocationStatus {
            status: Some(Status::Invoked(Invoked {
                invocation_id: uuid_bytes("018756fa-3f7f-7854-a76b-42c59a3d7f2d"),
                journal_meta: None,
                response_sink: None,
            })),
        },
    )
    .await;

    txn.put_invocation_status(
        1338,
        &ServiceId::new("svc-1", "key-2"),
        InvocationStatus {
            status: Some(Status::Invoked(Invoked {
                invocation_id: uuid_bytes("118756fa-3f7f-7854-a76b-42c59a3d7f2d"),
                journal_meta: None,
                response_sink: None,
            })),
        },
    )
    .await;

    txn.put_invocation_status(
        1339,
        &ServiceId::new("svc-2", "key-0"),
        InvocationStatus {
            status: Some(Status::Invoked(Invoked {
                invocation_id: uuid_bytes("118756fa-3f7f-7854-a76b-42c59a3d7f2d"),
                journal_meta: None,
                response_sink: None,
            })),
        },
    )
    .await;

    txn.put_invocation_status(
        1339,
        &ServiceId::new("svc-2", "key-1"),
        InvocationStatus {
            status: Some(Status::Suspended(Suspended {
                invocation_id: uuid_bytes("218756fa-3f7f-7854-a76b-42c59a3d7f2d"),
                journal_meta: None,
                response_sink: None,
                waiting_for_completed_entries: vec![99, 45],
            })),
        },
    )
    .await;

    txn.put_invocation_status(
        u64::MAX,
        &ServiceId::new("svc-u64", "key-0"),
        InvocationStatus {
            status: Some(Status::Invoked(Invoked {
                invocation_id: uuid_bytes("218756fa-3f7f-7854-a76b-42c59a3d7f2d"),
                journal_meta: None,
                response_sink: None,
            })),
        },
    )
    .await;
}

async fn verify_point_lookups<T: StatusTable>(txn: &mut T) {
    let status = txn
        .get_invocation_status(1337, &ServiceId::new("svc-1", "key-1"))
        .await
        .expect("should not fail");

    assert_eq!(
        status,
        Some(InvocationStatus {
            status: Some(Status::Invoked(Invoked {
                invocation_id: uuid_bytes("018756fa-3f7f-7854-a76b-42c59a3d7f2d"),
                journal_meta: None,
                response_sink: None,
            }))
        })
    );
}

async fn verify_all_svc_with_status_invoked<T: StatusTable>(txn: &mut T) {
    let stream = txn.invoked_invocations(1337..=1339);

    let expected = vec![
        ServiceInvocationId::new(
            "svc-1",
            "key-1",
            uuid_str("018756fa-3f7f-7854-a76b-42c59a3d7f2d"),
        ),
        ServiceInvocationId::new(
            "svc-1",
            "key-2",
            uuid_str("118756fa-3f7f-7854-a76b-42c59a3d7f2d"),
        ),
        ServiceInvocationId::new(
            "svc-2",
            "key-0",
            uuid_str("118756fa-3f7f-7854-a76b-42c59a3d7f2d"),
        ),
    ];

    assert_stream_eq(stream, expected).await;
}

async fn verify_last_partition_all_svc_with_status_invoked<T: StatusTable>(txn: &mut T) {
    let stream = txn.invoked_invocations(4000..=u64::MAX);
    let expected = vec![ServiceInvocationId::new(
        "svc-u64",
        "key-0",
        uuid_str("218756fa-3f7f-7854-a76b-42c59a3d7f2d"),
    )];

    assert_stream_eq(stream, expected).await;
}

pub(crate) async fn run_tests(rocksdb: RocksDBStorage) {
    let mut txn = rocksdb.transaction();
    populate_data(&mut txn).await;
    txn.commit().await.expect("should not fail");

    let mut txn = rocksdb.transaction();
    verify_point_lookups(&mut txn).await;
    verify_all_svc_with_status_invoked(&mut txn).await;
    verify_last_partition_all_svc_with_status_invoked(&mut txn).await;
}
