// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::prelude::{SessionContext, col, lit};
use futures::TryStreamExt;
use tokio_util::sync::CancellationToken;

use restate_clock::UniqueTimestamp;
use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, InvocationStatus,
    PreFlightInvocationMetadata, ScheduledInvocation, StatusTimestamps, WriteInvocationStatusTable,
};
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
};
use restate_storage_query_api::tables::invocation_status::InvocationStatus as InvocationStatusSchema;
use restate_types::config::Configuration;
use restate_types::errors::KILLED_INVOCATION_ERROR;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionId, WithPartitionKey};
use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType, WorkflowHandlerType};
use restate_types::partition_table::Partition;
use restate_types::partitions::features::PersistedFeatures;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryId, EntryTargetExt, EntryTargetRef, HandlerRef, Seq, VQueueId};
use restate_worker_api::CapturedPartitionRead;

use crate::access::PrimaryRead;
use crate::mocks::{differential_sessions, rows, v2_session};
use crate::table_providers::{PartitionReadContext, ScanPartition};

use super::StatusScanner;

#[restate_core::test]
async fn persisted_invocations_match_v1_for_scans_lookups_and_projections() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut tx = store.transaction();
    let ids: Vec<_> = (0..32)
        .map(|index| InvocationId::from_parts(index, InvocationUuid::from_u128(index.into())))
        .collect();
    for (index, id) in ids.iter().enumerate() {
        let status = match index % 6 {
            0 => InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            1 => InvocationStatus::Completed(CompletedInvocation::mock_neo()),
            2 => InvocationStatus::Paused(InFlightInvocationMetadata::mock()),
            3 => InvocationStatus::Suspended {
                metadata: InFlightInvocationMetadata::mock(),
                awaiting_on: restate_types::journal_v2::UnresolvedFuture::unknown_from_iter([
                    restate_types::journal_v2::NotificationId::for_completion(1),
                    restate_types::journal_v2::NotificationId::SignalIndex(10),
                ]),
            },
            4 => InvocationStatus::Scheduled(ScheduledInvocation {
                metadata: PreFlightInvocationMetadata::mock(),
            }),
            _ => InvocationStatus::Inboxed(InboxedInvocation {
                inbox_sequence_number: 1,
                metadata: PreFlightInvocationMetadata::mock(),
            }),
        };
        tx.put_invocation_status(id, &status).unwrap();
    }
    tx.commit().await.unwrap();
    drop(tx);
    let [old, new] = differential_sessions(&manager).await;
    let mut keys = ids.iter().map(|id| format!("'{id}'")).collect::<Vec<_>>();
    keys.push(format!(
        "'{}'",
        InvocationId::from_parts(100, InvocationUuid::from_u128(100))
    ));
    for sql in [
        "SELECT * FROM sys_invocation_status ORDER BY id".to_owned(),
        "SELECT COUNT(*) FROM sys_invocation_status".into(),
        "SELECT * FROM sys_invocation ORDER BY id".into(),
        "SELECT status, COUNT(*) FROM sys_invocation GROUP BY status ORDER BY status".into(),
        format!("SELECT * FROM sys_invocation_status WHERE id IN ({}) ORDER BY id", keys.join(",")),
        format!("SELECT id, status FROM sys_invocation_status WHERE id = '{}'", ids[5]),
        "SELECT id, status FROM sys_invocation_status WHERE partition_key >= 3 ORDER BY id LIMIT 3 OFFSET 2".into(),
        "SELECT scheduled_start_at FROM sys_invocation_status ORDER BY id".into(),
    ] {
        assert_eq!(rows(new.as_ref(), &sql).await, rows(old.as_ref(), &sql).await, "{sql}");
    }
    assert_eq!(
        rows(new.as_ref(), "SELECT COUNT(*) FROM sys_invocation_status").await,
        [["32"]]
    );
    // `sys_invocation` shows each invocation's `canonical_id` as its status does.
    assert_eq!(
        rows(
            new.as_ref(),
            "SELECT COUNT(*) FROM sys_invocation i JOIN sys_invocation_status s ON i.id = s.id \
             WHERE i.canonical_id IS NOT DISTINCT FROM s.canonical_id"
        )
        .await,
        [["32"]]
    );
    assert_eq!(
        rows(
            new.as_ref(),
            "SELECT COUNT(*) FROM sys_invocation_status WHERE id = 'invalid'"
        )
        .await,
        [["0"]]
    );

    // SQL routes literal IDs to multi-get; the scan path navigates the same
    // restriction by key. Missing IDs and limits hold on that path too.
    let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
    let context = PartitionReadContext::from_capture(CapturedPartitionRead {
        partition: PartitionId::MIN,
        storage: Some(snapshot),
        live: Default::default(),
        timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
    })
    .with_query_io(crate::mocks::query_io());
    let schema = InvocationStatusSchema::arrow_schema();
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
    let missing = InvocationId::from_parts(4, InvocationUuid::from_u128(100));
    let filter = col("id").in_list(
        [ids[3], ids[5], missing]
            .iter()
            .map(|id| lit(ScalarValue::LargeUtf8(Some(id.to_string()))))
            .collect(),
        false,
    );
    let predicate = SessionContext::new()
        .create_physical_expr(filter, &df_schema)
        .unwrap();
    for (limit, expected) in [(None, vec![ids[3], ids[5]]), (Some(1), vec![ids[3]])] {
        let batches = Arc::new(StatusScanner)
            .prepare_read(
                PrimaryRead::Scan,
                schema.clone(),
                Some(Arc::clone(&predicate)),
                limit,
            )
            .unwrap()
            .execute(context.clone(), 8, Default::default())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let found: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name("id").unwrap();
                (0..batch.num_rows()).map(|row| array_value_to_string(column, row).unwrap())
            })
            .collect();
        let expected: Vec<_> = expected.iter().map(ToString::to_string).collect();
        assert_eq!(found, expected, "scan_path_ids_with_limit_{limit:?}");
    }
}

/// The workflow page and its variants, read through the entry index, match V1:
/// newest created first, the query's other conditions applied before LIMIT,
/// completed invocations included, state mutations and other services skipped.
/// Until the partition has fully migrated to vqueues, the read scans instead.
/// An invocation the index lacks (impossible once migrated) shows which path
/// a read took.
///
/// The planner reads the index for the newest or oldest rows of named
/// services, whatever the other conditions; each returned row is checked
/// against all of them. The UI's other queries scan, and match V1 too. A
/// service's newest scheduled invocations come none at first, then two behind
/// 300 others.
#[restate_core::test]
async fn invocations_read_through_the_entry_index_match_v1() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut config = Configuration::default();
    config.common.experimental.set_track_entry_indexes(true);
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    // The planner reads the index only where it's maintained.
    Configuration::set(config.clone());

    const BASE: u64 = 1_744_010_000_000;
    let targets = [
        InvocationTarget::workflow("Promises", "w", "promise", WorkflowHandlerType::Shared),
        InvocationTarget::workflow("Promises", "w", "run", WorkflowHandlerType::Workflow),
        InvocationTarget::virtual_object(
            "Promises",
            "o",
            "promise",
            VirtualObjectHandlerType::Shared,
        ),
        InvocationTarget::service("Utils", "call"),
    ];
    let qid = VQueueId::custom(7, "q");
    let mut tx = store.transaction();
    // In the index's Promises runs, but no invocation.
    let mutation = UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE + 70));
    let mutation_key = EntryKey::new(
        false,
        mutation.to_unix_millis(),
        1,
        EntryId::new(restate_types::vqueues::EntryKind::StateMutation, [9; 16]),
    );
    tx.create_vqueue_entry_status(
        &EntryContext {
            qid: &qid,
            target: &EntryTargetRef::VirtualObject {
                scope: None,
                service: "Promises",
                key: "o",
                handler: HandlerRef::StateMutation,
            },
        },
        EntryStateRef {
            stage: Stage::Inbox,
            status: Status::New,
            entry_key: &mutation_key,
            metadata: &EntryMetadata::default(),
            stats: &EntryStatistics::new(mutation, mutation_key.run_at()),
        },
    );
    // Every third invocation completed, every ninth killed; the others are
    // running. Their entries are in the stage and status that goes with that.
    let mut put = |index: u64, created: u64, target: &InvocationTarget, entry: bool| {
        let id = InvocationId::from_parts(index * 997, InvocationUuid::from_u128(index.into()));
        let created = MillisSinceEpoch::new(created);
        let (status, stage, entry_status) = if index.is_multiple_of(3) {
            let mut completed = CompletedInvocation::mock_neo();
            completed.invocation_target = target.clone();
            completed.timestamps = StatusTimestamps::init(created);
            completed.creation_seq = Some(Seq::new(index));
            let outcome = if index.is_multiple_of(9) {
                completed.response_result =
                    restate_types::invocation::ResponseResult::Failure(KILLED_INVOCATION_ERROR);
                Status::Killed
            } else {
                Status::Succeeded
            };
            (
                InvocationStatus::Completed(completed),
                Stage::Finished,
                outcome,
            )
        } else {
            let mut invoked = InFlightInvocationMetadata::mock();
            invoked.invocation_target = target.clone();
            invoked.timestamps = StatusTimestamps::init(created);
            invoked.creation_seq = Some(Seq::new(index));
            (
                InvocationStatus::Invoked(invoked),
                Stage::Running,
                Status::Started,
            )
        };
        tx.put_invocation_status(&id, &status).unwrap();
        if entry {
            // A queue shares its invocations' partition key.
            let qid = VQueueId::custom(id.partition_key(), "q");
            let created = UniqueTimestamp::from_unix_millis_unchecked(created);
            let key = EntryKey::new(false, created.to_unix_millis(), index, EntryId::from(id));
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target.entry_target_ref(),
                },
                EntryStateRef {
                    stage,
                    status: entry_status,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &EntryStatistics::new(created, key.run_at()),
                },
            );
        }
        id
    };
    // Pairs share a creation time; the workflow page's invocations don't.
    for index in 0..240 {
        let target = &targets[index as usize % targets.len()];
        put(index, BASE + (index / 2) * 7, target, true);
    }
    // More services in one hour than one read merges, stored oldest first: a
    // read of that hour scans.
    for service in 0..257 {
        let target = InvocationTarget::service(format!("Many{service}"), "h");
        put(2000 + service, BASE + 10_000 + service * 7, &target, true);
    }
    let many = |order: &str| {
        format!(
            "SELECT id, created_at FROM sys_invocation_status \
             WHERE created_at >= to_timestamp_millis({}) \
             AND created_at < to_timestamp_millis({}) \
             ORDER BY created_at {order} LIMIT 3",
            BASE + 10_000,
            BASE + 12_000
        )
    };
    // A service with many invocations, none of them scheduled yet, created in
    // pairs at the same time.
    let counter = |key: String| {
        InvocationTarget::virtual_object("Counter", key, "add", VirtualObjectHandlerType::Exclusive)
    };
    let mut created_of = HashMap::new();
    for n in 0..300u64 {
        let created = BASE + 50_000 + n.div_ceil(2) * 3;
        let id = put(3000 + n, created, &counter(format!("c{n}")), true);
        created_of.insert(id.to_string(), created);
    }
    // Stored later: two scheduled invocations, older than the others.
    let scheduled_id = |n: u64| {
        InvocationId::from_parts(
            (4000 + n) * 997,
            InvocationUuid::from_u128((4000 + n).into()),
        )
    };
    let scheduled_at = |n: u64| BASE + 49_000 + n;
    for n in 0..2 {
        created_of.insert(scheduled_id(n).to_string(), scheduled_at(n));
    }
    // The oldest of the workflow page's invocations, so that it isn't on the page.
    let orphan = put(1000, BASE - 1000, &targets[0], false);
    tx.commit().await.unwrap();
    drop(tx);

    let [old, new] = differential_sessions(&manager).await;
    // Queries the UI sends, byte for byte.
    let page = include_str!("../../ui_queries/workflow_page.sql");
    let oldest = include_str!("../../ui_queries/service_oldest.sql");
    let scheduled = include_str!("../../ui_queries/service_scheduled_newest.sql");
    // Every completed invocation of a service, by outcome: Utils' succeeded and
    // killed ones, through the Finished lanes.
    let outcomes =
        include_str!("../../ui_queries/service_outcomes_newest.sql").replace("'State'", "'Utils'");
    // Diagnostic: the same service, newest first, through the index.
    let newest_counter = oldest.replace("ASC", "DESC");
    // Every invocation of a service, newest first: the orphan is the oldest.
    let promises = "SELECT id FROM sys_invocation_status WHERE target_service_name = 'Promises' \
        ORDER BY created_at DESC NULLS LAST LIMIT 1000";
    // The newest rows of named services, or the oldest, under conditions on
    // what the read navigates, the service type and the key's presence: read
    // through the index, forward or backward, wherever NULLs go, as
    // `created_at` is never NULL. Rows that share a creation time differ only
    // in it.
    let window = |order: &str| {
        format!(
            "SELECT created_at FROM sys_invocation_status WHERE target_service_name = 'Promises' \
             AND target_handler_name IN ('promise', 'run') \
             AND created_at >= to_timestamp_millis({}) \
             AND created_at < to_timestamp_millis({}) \
             ORDER BY created_at {order} NULLS LAST LIMIT 7",
            BASE + 10,
            BASE + 400
        )
    };
    // A page of invocations of some statuses. Rows that share a creation time
    // and a status are equal.
    let status_page = |condition: &str, order: &str, limit: u32| {
        format!(
            "SELECT created_at, status, completion_result FROM sys_invocation_status \
             WHERE {condition} ORDER BY created_at {order} NULLS LAST LIMIT {limit}"
        )
    };
    // The queries the assertions below name.
    let offset_page = page.replace("LIMIT 51", "LIMIT 9 OFFSET 40");
    let many_newest = many("DESC NULLS LAST");
    let window_oldest = window("ASC");
    let many_oldest = many("ASC");
    let failed_page = status_page("completion_result = 'failure'", "DESC", 10);
    let not_completed = status_page("status != 'completed'", "DESC", 20);
    let not_completed_or_paused = status_page("status NOT IN ('completed', 'paused')", "DESC", 10);
    // Statuses whose entries can be in every lane: the creation index,
    // checking them. Only the creation times: rows of both statuses share them.
    let every_lane = "SELECT created_at FROM sys_invocation_status \
        WHERE status IN ('invoked', 'completed') ORDER BY created_at DESC NULLS LAST LIMIT 20";
    // A range of handlers, checked on the index's values.
    let handler_range = page.replace(
        "target_handler_name = 'promise'",
        "target_handler_name >= 'p' AND target_handler_name < 'q'",
    );
    let indexed = [
        page.to_owned(),
        offset_page.clone(),
        many_newest.clone(),
        window("DESC"),
        newest_counter.clone(),
        page.replace("DESC NULLS LAST", "DESC NULLS FIRST"),
        page.replace("DESC NULLS LAST", "DESC"),
        // Backwards: Counter's one run, past many windows of statuses; two
        // runs of a time window, each read back from the window's end; and
        // too many runs, which scan and sort oldest first.
        oldest.to_owned(),
        oldest.replace("LIMIT 250", "LIMIT 9 OFFSET 240"),
        window_oldest.clone(),
        many_oldest.clone(),
        // Through the status index: the service's scheduled invocations, the
        // running ones, the failed ones and the completed ones, newest or
        // oldest first.
        scheduled.to_owned(),
        status_page("status = 'invoked'", "DESC", 20),
        failed_page.clone(),
        status_page(
            "status = 'completed' AND completion_result = 'failure' \
             AND target_service_name = 'Promises'",
            "ASC",
            10,
        ),
        status_page("status = 'completed'", "DESC", 20),
        // The status's values are complete: excluding some selects the others'
        // lanes, here those of invocations that haven't completed.
        not_completed.clone(),
        not_completed_or_paused.clone(),
        // 40 of the workflow page's invocations are running: the newest 20
        // leave out the oldest, which has no entry.
        page.replace("target_service_key IS NOT NULL", "status = 'invoked'")
            .replace("LIMIT 51", "LIMIT 20"),
        // Any other condition is checked on each candidate: a column the
        // index doesn't hold, a function, the UI's outcome alternatives.
        page.replace("target_service_key IS NOT NULL", "idempotency_key IS NULL"),
        page.replace(
            "target_service_key IS NOT NULL",
            "LOWER(target_handler_name) = 'promise'",
        ),
        outcomes.clone(),
        every_lane.into(),
        handler_range.clone(),
    ];
    // Scanned: another order, a tie-breaker, no LIMIT, or a view above the
    // table.
    let scans = [
        page.replace("NULLS LAST", "NULLS LAST, id"),
        "SELECT target_service_name, COUNT(*) FROM sys_invocation_status \
         WHERE target_service_name IN ('Promises', 'Utils', 'Missing') \
         GROUP BY target_service_name ORDER BY target_service_name"
            .into(),
        "SELECT id, status, target FROM sys_invocation WHERE target_service_name = 'Promises' \
         AND target_handler_name = 'promise' ORDER BY created_at DESC NULLS LAST LIMIT 9"
            .into(),
    ];
    let sorted = |mut rows: Vec<Vec<String>>| {
        rows.sort();
        rows
    };
    // Each query's rows match V1's. Counter's pages have no tie-breaker, and
    // its invocations come in pairs created together, which a LIMIT or OFFSET
    // may split: there, both return the same creation times, each invocation
    // once, and the full pages the 250 oldest (or newest) stored.
    let matches_v1 = async |sql: &str, scheduled_stored: bool| {
        let (actual, expected) = (rows(new.as_ref(), sql).await, rows(old.as_ref(), sql).await);
        if !sql.contains("'Counter'") || sql == scheduled {
            assert_eq!(actual, expected, "{sql}");
            return;
        }
        let times = |rows: &[Vec<String>]| -> Vec<u64> {
            rows.iter().map(|row| created_of[&row[0]]).collect()
        };
        assert_eq!(times(&actual), times(&expected), "{sql}");
        for rows in [&actual, &expected] {
            assert_eq!(
                rows.iter().collect::<HashSet<_>>().len(),
                rows.len(),
                "{sql}"
            );
        }
        let newest_first = sql == newest_counter;
        if sql != oldest && !newest_first {
            return;
        }
        let mut stored: Vec<u64> = created_of.values().copied().collect();
        stored.sort_unstable();
        let stored = &stored[if scheduled_stored { 0 } else { 2 }..];
        let page: Vec<u64> = if newest_first {
            stored.iter().rev().take(250).copied().collect()
        } else {
            assert_eq!(stored[249], stored[250], "the LIMIT splits a pair");
            stored[..250].to_vec()
        };
        for rows in [&actual, &expected] {
            let times: Vec<_> = rows.iter().map(|row| created_of[&row[0]]).collect();
            assert_eq!(times, page, "{sql}");
            assert_eq!(rows.iter().collect::<HashSet<_>>().len(), 250, "{sql}");
        }
    };
    let explained = async |sql: &str| {
        rows(new.as_ref(), &format!("EXPLAIN {sql}"))
            .await
            .concat()
            .join("\n")
    };
    // EXPLAIN ANALYZE shows which path the partition's read took.
    let analyzed = async |sql: &str| {
        rows(new.as_ref(), &format!("EXPLAIN ANALYZE {sql}"))
            .await
            .concat()
            .join("\n")
    };
    let orphaned = async || {
        rows(new.as_ref(), promises)
            .await
            .concat()
            .contains(&orphan.to_string())
    };
    // The planner's choice holds whatever the partition's state.
    for sql in &indexed {
        let plan = explained(sql).await;
        assert!(plan.contains("IndexScanExec"), "{sql}: {plan}");
    }
    // The outcome alternatives all fix the status to `completed`: the Finished
    // lanes of the status index. Exclusions select the other statuses' lanes.
    for sql in [&outcomes, &not_completed, &not_completed_or_paused] {
        let plan = explained(sql).await;
        assert!(
            plan.contains("index=_idx_entry_by_status_created"),
            "{sql}: {plan}"
        );
    }
    let plan = explained(every_lane).await;
    assert!(
        plan.contains("index=_idx_entry_by_service_created"),
        "{every_lane}: {plan}"
    );
    for sql in &scans {
        let plan = explained(sql).await;
        assert!(
            plan.contains("TableScanExec") && !plan.contains("IndexScanExec"),
            "{sql}: {plan}"
        );
        assert!(!rows(new.as_ref(), sql).await.is_empty(), "{sql}");
    }
    // A partition that can't keep the index's order keeps the newest rows that
    // meet the query's conditions, OFFSET included.
    assert!(explained(page).await.contains("fallback_fetch=51"));
    assert!(explained(&offset_page).await.contains("fallback_fetch=49"));

    // Not migrated: the index path scans, and finds the invocation without an entry.
    for sql in indexed.iter().chain(&scans) {
        matches_v1(sql, false).await;
    }
    let plan = analyzed(page).await;
    assert!(
        plan.contains("entry_index_scans=1") && !plan.contains("entry_index_reads=1"),
        "{plan}"
    );
    assert_eq!(
        metric(&plan, "entry_index_scans_not_migrated"),
        Some(1),
        "{plan}"
    );
    // The scanning partition emits only the newest 51 of its 61 matches.
    let source = plan
        .lines()
        .find(|line| line.contains("IndexScanExec"))
        .unwrap();
    assert_eq!(metric(source, "output_rows"), Some(51), "{source}");
    assert!(orphaned().await);

    // Fully migrated: the index is read. Only the impossible orphan differs.
    let mut tx = store.transaction();
    tx.put_state_machine_features(&PersistedFeatures {
        vqueues: true,
        ..Default::default()
    })
    .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    for sql in indexed.iter().chain(&scans) {
        matches_v1(sql, false).await;
    }
    assert!(!orphaned().await);
    // A LIMIT without an ORDER BY, as the UI's lists send, reads an index and
    // stops: any rows meeting the conditions will do, so each page holds the
    // LIMIT's count of them, all matches.
    // The UI's outcome list as it sends it, byte for byte, without an order:
    // any 250 of Utils' completed invocations, through the Finished lanes.
    let original = include_str!("../../ui_queries/service_outcomes_any.sql");
    let original_all = original.replace("\n      LIMIT 250", "");
    assert_ne!(original_all, original);
    let unordered = [
        (
            original,
            "_idx_entry_by_status_created",
            original_all.as_str(),
        ),
        (
            "SELECT ss.id AS id FROM sys_invocation_status ss \
             WHERE ss.target_service_name IN ('Utils') LIMIT 25",
            "_idx_entry_by_service_created",
            "SELECT id FROM sys_invocation_status WHERE target_service_name = 'Utils'",
        ),
        (
            "SELECT ss.id AS id FROM sys_invocation_status ss \
             WHERE ss.status = 'invoked' AND ss.target_service_name IN ('Promises') \
             AND ss.partition_key BETWEEN 0 AND 18446744073709551615 LIMIT 10",
            "_idx_entry_by_status_created",
            "SELECT id FROM sys_invocation_status \
             WHERE status = 'invoked' AND target_service_name = 'Promises'",
        ),
        // The status's values are complete: excluding some selects the
        // others' lanes, here those of invocations that haven't completed.
        (
            "SELECT ss.id AS id FROM sys_invocation_status ss \
             WHERE ss.status != 'completed' AND ss.target_service_name IN ('Utils') LIMIT 10",
            "_idx_entry_by_status_created",
            "SELECT id FROM sys_invocation_status \
             WHERE status <> 'completed' AND target_service_name = 'Utils'",
        ),
        (
            "SELECT ss.id AS id FROM sys_invocation_status ss \
             WHERE ss.status NOT IN ('completed', 'paused') \
             AND ss.target_service_name IN ('Utils') LIMIT 10",
            "_idx_entry_by_status_created",
            "SELECT id FROM sys_invocation_status \
             WHERE status NOT IN ('completed', 'paused') AND target_service_name = 'Utils'",
        ),
        (
            "SELECT id FROM sys_invocation_status \
             WHERE target_service_name IN ('Utils') AND idempotency_key IS NULL LIMIT 25",
            "_idx_entry_by_service_created",
            "SELECT id FROM sys_invocation_status \
             WHERE target_service_name = 'Utils' AND idempotency_key IS NULL",
        ),
    ];
    for (sql, index, all) in unordered {
        let plan = explained(sql).await;
        assert!(
            plan.contains("IndexScanExec") && plan.contains(&format!("index={index}")),
            "{sql}: {plan}"
        );
        let page = rows(new.as_ref(), sql).await;
        let all: HashSet<_> = rows(old.as_ref(), all).await.into_iter().collect();
        assert!(!all.is_empty(), "{sql}");
        let limit: usize = sql.rsplit(' ').next().unwrap().parse().unwrap();
        assert_eq!(page.len(), limit.min(all.len()), "{sql}");
        assert!(page.iter().all(|row| all.contains(row)), "{sql}");
        assert_eq!(
            page.iter().collect::<HashSet<_>>().len(),
            page.len(),
            "{sql}"
        );
    }
    // Without a condition the index narrows by, it scans: the scan stops at
    // the LIMIT too.
    for sql in [
        "SELECT id FROM sys_invocation_status LIMIT 25",
        "SELECT id FROM sys_invocation_status WHERE idempotency_key IS NOT NULL LIMIT 25",
    ] {
        let plan = explained(sql).await;
        assert!(!plan.contains("IndexScanExec"), "{sql}: {plan}");
    }
    // The service's scheduled invocations, none yet, through the status index:
    // it fetches the statuses of Counter's active invocations only, not the
    // partition's every status. The newest failures read only failed keys.
    let count = async |condition: &str| -> u64 {
        rows(
            old.as_ref(),
            &format!("SELECT COUNT(*) FROM sys_invocation_status {condition}"),
        )
        .await[0][0]
            .parse()
            .unwrap()
    };
    let statuses = count("").await;
    let counter_active =
        count("WHERE target_service_name = 'Counter' AND status <> 'completed'").await;
    let status_source = |plan: &str| {
        plan.lines()
            .find(|line| line.contains("index=_idx_entry_by_status_created"))
            .unwrap()
            .to_owned()
    };
    let source = status_source(&analyzed(scheduled).await);
    assert_eq!(metric(&source, "output_rows"), Some(0), "{source}");
    assert_eq!(
        metric(&source, "entry_index_statuses"),
        Some(counter_active),
        "{source}"
    );
    assert!(
        counter_active < statuses / 3,
        "{counter_active} of {statuses}"
    );
    // Ten failures: one window of statuses, and fewer keys than the succeeded
    // invocations the read never visits.
    let succeeded = count("WHERE completion_result = 'success'").await;
    let failures = status_source(&analyzed(&failed_page).await);
    assert_eq!(
        metric(&failures, "entry_index_statuses"),
        Some(25),
        "{failures}"
    );
    assert!(
        metric(&failures, "entry_index_keys_examined").unwrap() < succeeded,
        "{failures}"
    );
    let expected = rows(old.as_ref(), promises).await;
    assert!(expected.concat().contains(&orphan.to_string()));
    assert_eq!(
        sorted(rows(new.as_ref(), promises).await),
        sorted(
            expected
                .into_iter()
                .filter(|row| !row.contains(&orphan.to_string()))
                .collect()
        )
    );
    // The page fetches only the "promise" handler's 120 statuses of the 175
    // its runs hold: the index keeps the handler in its value, and the read
    // checks it there before fetching.
    let plan = analyzed(page).await;
    assert!(
        plan.contains("entry_index_reads=1") && !plan.contains("entry_index_scans=1"),
        "{plan}"
    );
    assert_eq!(metric(&plan, "entry_index_statuses"), Some(120), "{plan}");
    // As does a range of handlers.
    let range = analyzed(&handler_range).await;
    assert_eq!(metric(&range, "entry_index_statuses"), Some(120), "{range}");
    // One run, ("Promises", "promise"), whatever the service's type; every one
    // of its keys examined at least once.
    assert_eq!(metric(&plan, "entry_index_runs"), Some(1), "{plan}");
    assert!(
        metric(&plan, "entry_index_keys_examined") >= Some(120),
        "{plan}"
    );
    // Oldest first, Counter's one run is read backwards: partitions merge
    // without a sort, and the read stops after the LIMIT, having fetched the
    // statuses of the windows it needed rather than all 300.
    let plan = explained(oldest).await;
    assert!(
        plan.contains("index=_idx_entry_by_service_created")
            && plan.contains("SortPreservingMergeExec")
            && !plan.contains("SortExec:"),
        "{plan}"
    );
    let plan = analyzed(oldest).await;
    assert!(plan.contains("entry_index_reads=1"), "{plan}");
    assert_eq!(metric(&plan, "entry_index_runs"), Some(1), "{plan}");
    assert_eq!(metric(&plan, "entry_index_statuses"), Some(275), "{plan}");
    assert!(
        metric(&plan, "entry_index_keys_examined") < Some(300),
        "{plan}"
    );
    // The time window's one run, the service in its hour, backwards: it reads
    // back from the window's end, examining none of the keys older than it, and
    // one window of statuses, of the window's two handlers, holds its 7 rows.
    let plan = analyzed(&window_oldest).await;
    assert_eq!(
        [
            "entry_index_runs",
            "entry_index_keys_examined",
            "entry_index_statuses"
        ]
        .map(|name| metric(&plan, name)),
        [Some(1), Some(33), Some(25)],
        "{plan}"
    );
    // Too many runs: the read scans and sorts, before returning anything,
    // oldest first too.
    let plan = analyzed(&many_oldest).await;
    assert_eq!(
        metric(&plan, "entry_index_scans_too_many_runs"),
        Some(1),
        "{plan}"
    );
    // Too many runs: the read scans and sorts, before returning anything.
    let plan = analyzed(&many_newest).await;
    assert!(plan.contains("entry_index_scans=1"), "{plan}");
    assert_eq!(
        metric(&plan, "entry_index_scans_too_many_runs"),
        Some(1),
        "{plan}"
    );
    // The discovery that gave up examined keys, and says so.
    assert!(
        metric(&plan, "entry_index_keys_examined") > Some(0),
        "{plan}"
    );
    // The index's order satisfies the page's ORDER BY: partitions are merged,
    // not sorted, and the merge stops after the LIMIT.
    let plan = explained(page).await;
    assert!(
        plan.contains("IndexScanExec")
            && plan.contains("index=_idx_entry_by_service_created")
            && plan.contains("SortPreservingMergeExec")
            && !plan.contains("SortExec:"),
        "{plan}"
    );
    // The service's scheduled invocations: none, then two older than the 300
    // others. Newest first, the read rejects every other one before them.
    assert!(rows(new.as_ref(), scheduled).await.is_empty());
    let mut tx = store.transaction();
    for n in 0..2 {
        let id = scheduled_id(n);
        let mut metadata = PreFlightInvocationMetadata::mock_with_vqueue(id.partition_key());
        metadata.invocation_target = counter(format!("s{n}"));
        metadata.timestamps = StatusTimestamps::init(MillisSinceEpoch::new(scheduled_at(n)));
        metadata.creation_seq = Some(Seq::new(4000 + n));
        tx.put_invocation_status(
            &id,
            &InvocationStatus::Scheduled(ScheduledInvocation { metadata }),
        )
        .unwrap();
        // With its entry, as in any migrated partition.
        let created =
            UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(scheduled_at(n)));
        let key = EntryKey::new(false, created.to_unix_millis(), 4000 + n, EntryId::from(id));
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &VQueueId::custom(id.partition_key(), "q"),
                target: &counter(format!("s{n}")).entry_target_ref(),
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::Scheduled,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created, key.run_at()),
            },
        );
    }
    tx.commit().await.unwrap();
    drop(tx);
    // Oldest first, the read now begins with them.
    matches_v1(oldest, true).await;
    matches_v1(scheduled, true).await;
    assert_eq!(
        sorted(rows(new.as_ref(), scheduled).await),
        sorted([0, 1].map(|n| vec![scheduled_id(n).to_string()]).into())
    );
    // Without a service, the index reads each hour's runs, one per service,
    // and returns V1's rows.
    let all_services = "SELECT id FROM sys_invocation_status WHERE target_handler_name = 'promise' \
        ORDER BY created_at DESC NULLS LAST LIMIT 5";
    let plan = explained(all_services).await;
    assert!(
        plan.contains("IndexScanExec") && plan.contains("index=_idx_entry_by_service_created"),
        "{plan}"
    );
    assert_eq!(
        rows(new.as_ref(), all_services).await,
        rows(old.as_ref(), all_services).await
    );
    // Where the index isn't maintained, the planner scans, with the same page.
    Configuration::set(Configuration::default());
    let plan = explained(page).await;
    assert!(
        plan.contains("TableScanExec") && !plan.contains("IndexScanExec"),
        "{plan}"
    );
    matches_v1(page, true).await;
    Configuration::set(config);

    // Broken correspondence fails the read instead of returning wrong rows: an
    // entry of another incarnation at the same time, an entry created at another
    // time than its invocation, or one without an invocation.
    let error = |sql: &'static str| {
        let new = Arc::clone(&new);
        async move {
            match new.execute(sql, Default::default()).await {
                Ok(response) => response.stream.try_collect::<Vec<_>>().await.err(),
                Err(error) => panic!("planning fails: {error}"),
            }
            .expect("the read fails")
            .to_string()
        }
    };
    // Each newer than the last, so the read meets it first.
    for (index, seq, created, expected) in [
        (
            1000,
            1001,
            BASE - 1000,
            "but its vqueue entry by record 1001",
        ),
        (1000, 1000, BASE + 20_000, "but its vqueue entry at"),
        (1001, 1001, BASE + 30_000, "but no status"),
    ] {
        let id = InvocationId::from_parts(index * 997, InvocationUuid::from_u128(index.into()));
        let qid = VQueueId::custom(id.partition_key(), "q");
        let created = UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(created));
        let key = EntryKey::new(false, created.to_unix_millis(), seq, EntryId::from(id));
        let mut tx = store.transaction();
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &targets[0].entry_target_ref(),
            },
            EntryStateRef {
                stage: Stage::Finished,
                status: Status::Succeeded,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created, key.run_at()),
            },
        );
        tx.commit().await.unwrap();
        drop(tx);
        let message = error(promises).await;
        assert!(message.contains(expected), "{message}");
    }
}

/// Two partitions behind the workflow page, read in one lane or in two. One
/// hasn't migrated to vqueues: it scans and keeps only the newest of its 61
/// matches that the query may need, 51 (55 with `OFFSET 45`). The other reads
/// the index, emitting its 30 candidates. With one lane the plan sorts what the
/// lane concatenates; either way the page is the newest of both partitions.
#[restate_core::test]
async fn partitions_sharing_a_lane_bound_their_fallback() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut config = Configuration::default();
    config.common.experimental.set_track_entry_indexes(true);
    Configuration::set(config.clone());

    const BASE: u64 = 1_744_010_000_000;
    let partitions = vec![
        (
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::new(0, 999)),
        ),
        (
            PartitionId::from(1),
            Partition::new(PartitionId::from(1), KeyRange::new(1000, u64::MAX)),
        ),
    ];
    // (id, created) of every page invocation; the second partition's are odd.
    let mut page = Vec::new();
    for ((_, partition), (count, migrated)) in partitions.iter().zip([(61u64, false), (30, true)]) {
        let mut store = manager.open(partition, None).await.unwrap();
        store
            .verify_and_run_migrations(CancellationToken::new(), &config)
            .await
            .unwrap();
        let start = partition.key_range.start();
        let mut tx = store.transaction();
        for n in 0..count {
            let id = InvocationId::from_parts(start + n, InvocationUuid::from_u128(n.into()));
            let created = MillisSinceEpoch::new(BASE + 2 * n + u64::from(migrated));
            let target = InvocationTarget::workflow(
                "Promises",
                format!("w{start}-{n}"),
                "promise",
                WorkflowHandlerType::Shared,
            );
            let mut invoked = InFlightInvocationMetadata::mock();
            invoked.invocation_target = target.clone();
            invoked.timestamps = StatusTimestamps::init(created);
            invoked.creation_seq = Some(Seq::new(n + 1));
            tx.put_invocation_status(&id, &InvocationStatus::Invoked(invoked))
                .unwrap();
            let unique = UniqueTimestamp::from_unix_millis_unchecked(created);
            let key = EntryKey::new(false, unique.to_unix_millis(), n + 1, EntryId::from(id));
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &VQueueId::custom(id.partition_key(), "q"),
                    target: &target.entry_target_ref(),
                },
                EntryStateRef {
                    stage: Stage::Finished,
                    status: Status::Succeeded,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &EntryStatistics::new(unique, key.run_at()),
                },
            );
            page.push((created, id.to_string()));
        }
        tx.put_state_machine_features(&PersistedFeatures {
            vqueues: migrated,
            ..Default::default()
        })
        .unwrap();
        tx.commit().await.unwrap();
    }
    page.sort_by(|a, b| b.cmp(a));
    let newest: Vec<_> = page.into_iter().map(|(_, id)| vec![id]).collect();

    let exact = include_str!("../../ui_queries/workflow_page.sql");
    let offset = exact.replace("LIMIT 51", "LIMIT 10 OFFSET 45");
    for parallelism in [1, 2] {
        let session = v2_session(&manager, partitions.clone(), parallelism).await;
        let plan = rows(session.as_ref(), &format!("EXPLAIN {exact}"))
            .await
            .concat()
            .join("\n");
        assert!(plan.contains("fallback_fetch=51"), "{plan}");
        assert_eq!(plan.contains("SortExec:"), parallelism == 1, "{plan}");
        assert_eq!(rows(session.as_ref(), exact).await, newest[..51]);
        assert_eq!(rows(session.as_ref(), &offset).await, newest[45..55]);
        for (sql, kept) in [(exact.to_owned(), 51), (offset.clone(), 55)] {
            let plan = rows(session.as_ref(), &format!("EXPLAIN ANALYZE {sql}"))
                .await
                .concat()
                .join("\n");
            let source = plan
                .lines()
                .find(|line| line.contains("IndexScanExec"))
                .unwrap();
            assert!(
                source.contains("entry_index_scans=1") && source.contains("entry_index_reads=1"),
                "{source}"
            );
            assert_eq!(metric(source, "output_rows"), Some(kept + 30), "{source}");
            // What each read produced, before the scanning partition's sort kept
            // its first rows.
            assert_eq!(metric(source, "read_rows"), Some(61 + 30), "{source}");
        }
    }
}

/// The value of the first counter called `name` in an EXPLAIN ANALYZE plan.
fn metric(plan: &str, name: &str) -> Option<u64> {
    let (_, value) = plan.split_once(&format!("{name}="))?;
    let digits = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    value[..digits].parse().ok()
}
