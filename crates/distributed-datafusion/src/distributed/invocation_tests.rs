// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_clock::UniqueTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
};
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
};
use restate_types::identifiers::{InvocationId, InvocationUuid, WithPartitionKey};
use restate_types::vqueues::{EntryId, EntryTargetExt, VQueueId};
use restate_worker_api::invoker::InvocationStatusReport;
use restate_worker_api::invoker::status_handle::InvocationStatusReportInner;
use restate_worker_api::{
    CapturedPartitionRead, CapturedPartitionState, PartitionCaptureRequirements,
    ProcessorsManagerCommand, ProcessorsManagerHandle,
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
                Arc::new(crate::distributed::tests::TestInvokers(Box::new(
                    invoker_reports,
                ))),
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
                let mut timing =
                    restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now());
                let snapshot = store
                    .query_read(tokio_util::sync::CancellationToken::new())
                    .unwrap()
                    .0;
                timing.snapshot = Some(restate_worker_api::CaptureSpan::since(timing.admitted));
                let read = CapturedPartitionRead {
                    partition: request.partition,
                    storage: Some(snapshot),
                    live: CapturedPartitionState::default(),
                    timing,
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
        // The entries tell the status, not the invoker: the in-flight first
        // invocation backs off, the second, which the invoker reports idle,
        // runs, and the third, without an entry, shows its stored `invoked`
        // as `ready`.
        let entries = [
            Some((Stage::Inbox, Status::BackingOff)),
            Some((Stage::Running, Status::Started)),
            None,
        ];
        for (index, entry) in (0..3).zip(entries) {
            let id = InvocationId::from_parts(
                start + index,
                InvocationUuid::from_u128((index + 1).into()),
            );
            let metadata = InFlightInvocationMetadata::mock();
            let target = metadata.invocation_target.clone();
            tx.put_invocation_status(&id, &InvocationStatus::Invoked(metadata))
                .unwrap();
            if let Some((stage, status)) = entry {
                let qid = VQueueId::custom(id.partition_key(), "q");
                let created = UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(
                    1_744_000_000_000 + index,
                ));
                let key = EntryKey::new(false, created.to_unix_millis(), index, EntryId::from(id));
                tx.create_vqueue_entry_status(
                    &EntryContext {
                        qid: &qid,
                        target: &target.entry_target_ref(),
                    },
                    EntryStateRef {
                        stage,
                        status,
                        entry_key: &key,
                        metadata: &EntryMetadata::default(),
                        stats: &EntryStatistics::new(created, key.run_at()),
                    },
                );
            }
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
        "status_from_entries_not_live_state_and_unmatched_storage_rows",
        "SELECT status, COUNT(*) FROM sys_invocation GROUP BY status ORDER BY status",
        &[&["backing-off", "8"], &["ready", "8"], &["running", "8"]],
    )
    .await;
    assert_eq!(admissions.lock().len(), fixture.partitions.0.len());

    // Under EXPLAIN ANALYZE, each owner times each of its captures once, its
    // invoker read included, with a lane per partition.
    admissions.lock().clear();
    let (_, text) = super::analyze::run(
        session.as_ref(),
        "EXPLAIN ANALYZE SELECT status, COUNT(*) FROM sys_invocation GROUP BY status",
    )
    .await;
    let mut captured = 0;
    for line in text
        .lines()
        .filter(|line| line.contains("NetworkCoalesceExec"))
    {
        let metric = |name| super::analyze::metric(line, name);
        let Some(captures) = metric("captures") else {
            continue;
        };
        captured += captures.parse::<usize>().unwrap();
        for step in [
            "capture_queued_max",
            "snapshot_create_max",
            "invoker_capture_max",
        ] {
            assert!(metric(step).is_some(), "{step} in {text}");
        }
        // A capture's way to its processor and back can't take no time.
        for step in ["capture_max", "capture_queued_max"] {
            assert_ne!(metric(step).as_deref(), Some("0ns"), "{step} in {text}");
        }
    }
    assert_eq!(captured, fixture.partitions.0.len(), "{text}");
    assert_eq!(admissions.lock().len(), captured, "{text}");
    let (partition, metadata) = &fixture.partitions.0[0];
    let id = InvocationId::from_parts(metadata.key_range.start(), InvocationUuid::from_u128(1));
    admissions.lock().clear();
    assert_sql(session.as_ref(), "id_lookup_keeps_shared_admission",
        &format!("SELECT id, status, retry_count, last_attempt_server FROM sys_invocation WHERE id = '{id}'"),
        &[&[&id.to_string(), "backing-off", "2", "test-sdk"]]).await;
    assert_eq!(*admissions.lock(), [(owner(*partition), *partition)]);
}

/// What each partition's invoker reports: two invocations on its first keys.
fn invoker_reports(partition: PartitionId) -> Vec<InvocationStatusReport> {
    let start = partition_start(partition);
    vec![
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
}
