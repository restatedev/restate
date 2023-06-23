use crate::{assert_stream_eq, uuid_str};
use restate_common::types::{
    InvocationId, InvocationMetadata, InvocationStatus, JournalMetadata, MillisSinceEpoch,
    ServiceId, ServiceInvocationId, ServiceInvocationSpanContext,
};
use restate_storage_api::status_table::StatusTable;
use restate_storage_api::{Storage, Transaction};
use restate_storage_rocksdb::RocksDBStorage;
use std::collections::HashSet;

fn invoked_status(invocation_id: InvocationId) -> InvocationStatus {
    InvocationStatus::Invoked(InvocationMetadata::new(
        invocation_id,
        JournalMetadata::new("service", ServiceInvocationSpanContext::empty(), 0),
        None,
        MillisSinceEpoch::new(0),
        MillisSinceEpoch::new(0),
    ))
}

fn suspended_status(invocation_id: InvocationId) -> InvocationStatus {
    InvocationStatus::Suspended {
        metadata: InvocationMetadata::new(
            invocation_id,
            JournalMetadata::new("service", ServiceInvocationSpanContext::empty(), 0),
            None,
            MillisSinceEpoch::new(0),
            MillisSinceEpoch::new(0),
        ),
        waiting_for_completed_entries: HashSet::default(),
    }
}

async fn populate_data<T: StatusTable>(txn: &mut T) {
    txn.put_invocation_status(
        &ServiceId::with_partition_key(1337, "svc-1", "key-1"),
        invoked_status(uuid_str("018756fa-3f7f-7854-a76b-42c59a3d7f2d")),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(1338, "svc-1", "key-2"),
        invoked_status(uuid_str("118756fa-3f7f-7854-a76b-42c59a3d7f2d")),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(1339, "svc-2", "key-0"),
        invoked_status(uuid_str("118756fa-3f7f-7854-a76b-42c59a3d7f2d")),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(1339, "svc-2", "key-1"),
        suspended_status(uuid_str("218756fa-3f7f-7854-a76b-42c59a3d7f2d")),
    )
    .await;

    txn.put_invocation_status(
        &ServiceId::with_partition_key(u32::MAX, "svc-u64", "key-0"),
        invoked_status(uuid_str("218756fa-3f7f-7854-a76b-42c59a3d7f2d")),
    )
    .await;
}

async fn verify_point_lookups<T: StatusTable>(txn: &mut T) {
    let status = txn
        .get_invocation_status(&ServiceId::with_partition_key(1337, "svc-1", "key-1"))
        .await
        .expect("should not fail");

    assert_eq!(
        status,
        Some(invoked_status(uuid_str(
            "018756fa-3f7f-7854-a76b-42c59a3d7f2d"
        )))
    );
}

async fn verify_all_svc_with_status_invoked<T: StatusTable>(txn: &mut T) {
    let stream = txn.invoked_invocations(1337..=1339);

    let expected = vec![
        ServiceInvocationId::with_service_id(
            ServiceId::with_partition_key(1337, "svc-1", "key-1"),
            uuid_str("018756fa-3f7f-7854-a76b-42c59a3d7f2d"),
        ),
        ServiceInvocationId::with_service_id(
            ServiceId::with_partition_key(1338, "svc-1", "key-2"),
            uuid_str("118756fa-3f7f-7854-a76b-42c59a3d7f2d"),
        ),
        ServiceInvocationId::with_service_id(
            ServiceId::with_partition_key(1339, "svc-2", "key-0"),
            uuid_str("118756fa-3f7f-7854-a76b-42c59a3d7f2d"),
        ),
    ];

    assert_stream_eq(stream, expected).await;
}

async fn verify_last_partition_all_svc_with_status_invoked<T: StatusTable>(txn: &mut T) {
    let stream = txn.invoked_invocations(4000..=u32::MAX);
    let expected = vec![ServiceInvocationId::with_service_id(
        ServiceId::with_partition_key(u32::MAX, "svc-u64", "key-0"),
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
