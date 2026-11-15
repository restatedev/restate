// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_worker_api::invoker::InvocationStatusReport;
use restate_worker_api::invoker::status_handle::InvocationStatusReportInner;
use restate_worker_api::{
    CapturedPartitionRead, CapturedPartitionState, OfflinePartitionQueryAccess,
    PartitionCaptureRequirements, ProcessorsManagerCommand, ProcessorsManagerHandle,
};

use crate::UserTables;
use crate::tables::invocation_status::SysInvocationStatusTable;

use super::*;

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn invocation_view_combines_storage_and_live_admission_on_remote_leaders() {
    let manager = Arc::new(OnceLock::<Arc<PartitionStoreManager>>::new());
    let admissions = Arc::new(Mutex::new(Vec::new()));
    let fixture = setup_with_scanners(|node, sources| {
        sources.register_declared_scanner::<SysInvocationStatusTable>(Arc::new(
            SysInvocationStatusTable::create_local_scanner(),
        ));
        crate::local_scanners::register_live_scanners(sources);
        let (commands, mut receiver) = tokio::sync::mpsc::channel(32);
        sources.register_processor_reads(
            ProcessorsManagerHandle::new(
                commands,
                Arc::new(crate::distributed::tests::UnusedRuleBook),
                Arc::new(OfflinePartitionQueryAccess::new([])),
            ),
            crate::mocks::query_io(),
        );
        let manager = Arc::clone(&manager);
        let admissions = Arc::clone(&admissions);
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                let ProcessorsManagerCommand::ReadPartition(request) = command else {
                    panic!("expected capture")
                };
                assert_eq!(node, owner(request.partition));
                assert!(request.require_leader);
                assert_eq!(
                    request.capture,
                    PartitionCaptureRequirements {
                        storage: true,
                        invoker: true,
                        ..Default::default()
                    }
                );
                admissions.lock().push((node, request.partition));
                let store = manager
                    .get()
                    .unwrap()
                    .get_partition_store(request.partition)
                    .await
                    .unwrap();
                let start = store.partition_key_range().start();
                let reports: Arc<[_]> = [
                    InvocationStatusReport::new(
                        InvocationId::from_parts(start, InvocationUuid::from_u128(1)),
                        InvocationStatusReportInner {
                            in_flight: true,
                            start_count: 2,
                            last_attempt_server: Some("test-sdk".into()),
                            ..Default::default()
                        },
                    ),
                    InvocationStatusReport::new(
                        InvocationId::from_parts(start + 1, InvocationUuid::from_u128(2)),
                        InvocationStatusReportInner {
                            in_flight: false,
                            start_count: 3,
                            ..Default::default()
                        },
                    ),
                ]
                .into();
                let read = CapturedPartitionRead {
                    partition: request.partition,
                    storage: Some(
                        store
                            .query_read(tokio_util::sync::CancellationToken::new())
                            .unwrap()
                            .0,
                    ),
                    live: CapturedPartitionState {
                        invoker: Some(reports),
                        ..Default::default()
                    },
                };
                let _ = request.response.send(Ok(read));
            }
        });
    })
    .await;
    assert!(manager.set(Arc::clone(&fixture.store_manager)).is_ok());
    for mut store in fixture.stores.iter().cloned() {
        let start = store.partition_key_range().start();
        let mut tx = store.transaction();
        for index in 0..3 {
            tx.put_invocation_status(
                &InvocationId::from_parts(
                    start + index,
                    InvocationUuid::from_u128((index + 1).into()),
                ),
                &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            )
            .unwrap();
        }
        tx.commit().await.unwrap();
    }
    let env = environment(4, 1).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    assert_sql(
        session.as_ref(),
        "live_status_derivation_and_unmatched_storage_rows",
        "SELECT status, COUNT(*) FROM sys_invocation GROUP BY status ORDER BY status",
        &[&["backing-off", "8"], &["ready", "8"], &["running", "8"]],
    )
    .await;
    assert_eq!(admissions.lock().len(), fixture.partitions.0.len());
    let (partition, metadata) = &fixture.partitions.0[0];
    let id = InvocationId::from_parts(metadata.key_range.start(), InvocationUuid::from_u128(1));
    admissions.lock().clear();
    assert_sql(session.as_ref(), "id_lookup_keeps_shared_admission",
        &format!("SELECT id, status, retry_count, last_attempt_server FROM sys_invocation WHERE id = '{id}'"),
        &[&[&id.to_string(), "running", "2", "test-sdk"]]).await;
    assert_eq!(*admissions.lock(), [(owner(*partition), *partition)]);
}
