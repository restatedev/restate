// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Leader-live scans use the production catalog and scanner registration over owner tasks.

use restate_clock::{RoughTimestamp, UniqueTimestamp, time::MillisSinceEpoch};
use restate_storage_api::vqueue_table::stats::WaitStats;
use restate_storage_query_api::QueryMetadata;
use restate_types::cluster_state::NodeState;
use restate_types::logs::{Lsn, SequenceNumber};
use restate_types::partitions::state::{MemberState, ReplicaSetState};
use restate_types::vqueues::VQueueId;
use restate_util_string::ReString;
use restate_worker_api::{
    BlockedResource, CapturedPartitionRead, CapturedPartitionState, PartitionCaptureRequirements,
    PartitionReadError, ProcessorsManagerCommand, ProcessorsManagerHandle, SchedulerStatusEntry,
    SchedulingStatus, VQueueSchedulerStatus,
};

use crate::UserTables;
use crate::local_scanners::register_live_scanners;

use super::*;

#[derive(Default)]
struct LiveData {
    rows: Vec<(PartitionId, SchedulerStatusEntry)>,
    reads: Vec<(GenerationalNodeId, PartitionId, KeyRange)>,
    captured_rows: HashMap<(ReString, UniqueTimestamp), HashMap<PartitionId, usize>>,
    failure: Option<PartitionReadError>,
    captures: Vec<PartitionCaptureRequirements>,
}

struct SchedulerAccess {
    node: GenerationalNodeId,
    data: Arc<Mutex<LiveData>>,
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_key_joins_share_admission_and_preserve_outer_rows() {
    let data = Arc::new(Mutex::new(LiveData::default()));
    let fixture = setup_with_scanners(|node, scanners| {
        SchedulerAccess {
            node,
            data: Arc::clone(&data),
        }
        .register(scanners);
    })
    .await;
    for (partition, spec) in &fixture.partitions.0 {
        for index in 0..2 {
            data.lock().rows.push((
                *partition,
                (
                    VQueueId::custom(spec.key_range.start() + index, "join"),
                    VQueueSchedulerStatus {
                        waiting_inbox: index,
                        ..Default::default()
                    },
                ),
            ));
        }
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
    for (sql, expected) in [
        (
            "SELECT COUNT(*) FROM sys_scheduler a JOIN sys_scheduler b ON a.partition_key = b.partition_key AND a.id = b.id",
            "16",
        ),
        (
            "SELECT COUNT(*) FROM (SELECT partition_key AS pk, id FROM sys_scheduler WHERE num_inbox = 0) a RIGHT JOIN sys_scheduler b ON a.pk = b.partition_key AND a.id = b.id",
            "16",
        ),
        (
            "SELECT COUNT(*) FROM sys_scheduler a FULL JOIN sys_scheduler b ON a.partition_key = b.partition_key AND a.id = b.id",
            "16",
        ),
        (
            "SELECT COUNT(*) FROM sys_invocation_state a RIGHT JOIN sys_scheduler b ON a.partition_key = b.partition_key",
            "16",
        ),
        (
            "SELECT COUNT(*) FROM (SELECT partition_key, id FROM sys_scheduler WHERE partition_key = 0) a RIGHT JOIN sys_scheduler b ON a.partition_key = b.partition_key AND a.id = b.id",
            "16",
        ),
    ] {
        data.lock().reads.clear();
        data.lock().captures.clear();
        assert_sql(
            session.as_ref(),
            "partition_local_join_results",
            sql,
            &[&[expected]],
        )
        .await;
        assert_eq!(
            data.lock().reads.len(),
            fixture.partitions.0.len(),
            "both_inputs_share_one_admission: {sql}"
        );
        if sql.contains("sys_invocation_state") {
            assert!(
                data.lock()
                    .captures
                    .iter()
                    .all(|capture| capture.scheduler && capture.invoker)
            );
        }
    }
    // Without a partition-key equality, matches may cross partitions.
    data.lock().reads.clear();
    assert_sql(
        session.as_ref(),
        "cross_partition_join_stays_global",
        "SELECT COUNT(*) FROM sys_scheduler a JOIN sys_scheduler b ON a.num_inbox = b.num_inbox",
        &[&["128"]],
    )
    .await;
    assert_eq!(data.lock().reads.len(), 2 * fixture.partitions.0.len());
    assert_sql(session.as_ref(), "input_limit_remains_global",
        "SELECT COUNT(*) FROM (SELECT partition_key, id FROM sys_scheduler LIMIT 1) a JOIN sys_scheduler b ON a.partition_key = b.partition_key AND a.id = b.id", &[&["1"]]).await;
}

impl SchedulerAccess {
    fn register(self, scanners: &SourceRegistry) {
        let (commands, mut receiver) = tokio::sync::mpsc::channel(32);
        scanners.register_processor_reads(
            ProcessorsManagerHandle::new(
                commands,
                Arc::new(crate::distributed::tests::UnusedRuleBook),
                crate::distributed::tests::TestInvokers::empty(),
            ),
            crate::mocks::query_io(),
        );
        register_live_scanners(scanners);
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                let ProcessorsManagerCommand::ReadPartition(request) = command else {
                    panic!("expected capture request")
                };
                assert!(!request.capture.storage && !request.capture.user_limits);
                assert!(request.capture.scheduler || request.capture.invoker);
                assert!(request.require_leader);
                let _ = request.response.send(self.capture(
                    request.partition,
                    request.capture,
                    &request.metadata,
                ));
            }
        });
    }

    fn capture(
        &self,
        partition: PartitionId,
        requirements: PartitionCaptureRequirements,
        metadata: &QueryMetadata,
    ) -> Result<CapturedPartitionRead, PartitionReadError> {
        assert_eq!(
            self.node,
            owner(partition),
            "only_the_leader_reads_live_state"
        );
        let mut data = self.data.lock();
        data.captures.push(requirements);
        data.reads.push((self.node, partition, KeyRange::FULL));
        // Admission captures the entire partition, independently of scan bounds.
        let mut rows: Vec<_> = data
            .rows
            .iter()
            .filter(|(id, _)| *id == partition)
            .map(|(_, row)| row.clone())
            .collect();
        rows.sort_by(|(a, _), (b, _)| a.cmp(b));
        if let Some(error) = &data.failure {
            return Err(error.clone());
        }
        *data
            .captured_rows
            .entry((metadata.session_id.clone(), metadata.query_ts))
            .or_default()
            .entry(partition)
            .or_default() += rows.len();
        Ok(CapturedPartitionRead {
            partition,
            storage: None,
            live: CapturedPartitionState {
                scheduler: requirements.scheduler.then(|| rows.into()),
                invoker: requirements.invoker.then(Arc::default),
                ..Default::default()
            },
            timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
        })
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduler_limits_preserve_results_and_owner_grouping_with_partition_capture() {
    let data = Arc::new(Mutex::new(LiveData::default()));
    let fixture = setup_with_scanners(|node, scanners| {
        SchedulerAccess {
            node,
            data: Arc::clone(&data),
        }
        .register(scanners);
    })
    .await;
    for (partition, spec) in &fixture.partitions.0 {
        for row in 0..64 {
            data.lock().rows.push((
                *partition,
                (
                    VQueueId::custom(spec.key_range.start() + row, "limit"),
                    VQueueSchedulerStatus {
                        waiting_inbox: row,
                        ..Default::default()
                    },
                ),
            ));
        }
    }
    for parallelism in [1, 4] {
        for single_owner in [true, false] {
            let partitions = Partitions(
                fixture
                    .partitions
                    .0
                    .iter()
                    .filter(|(id, _)| !single_owner || owner(*id).raw_id() == 2)
                    .cloned()
                    .collect(),
            );
            let owners: BTreeSet<_> = partitions.0.iter().map(|(id, _)| owner(*id)).collect();
            let env =
                environment(parallelism, 128).with_distributed_execution(fixture.network.clone());
            env.register_tables(UserTables::new(
                partitions.clone(),
                fixture.scanners.clone(),
            ))
            .await
            .unwrap();
            let engine = DataFusionQueryEngine::from_env(env, None);
            let session = engine.create_session(SessionOptions::default()).unwrap();

            let explanation = session
                .execute(
                    "EXPLAIN VERBOSE SELECT * FROM sys_scheduler LIMIT 10",
                    QueryOptions {},
                )
                .await
                .unwrap()
                .stream
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let plan = explanation
                .iter()
                .find_map(|batch| {
                    (0..batch.num_rows()).find_map(|row| {
                        (array_value_to_string(batch.column(0), row).unwrap() == "physical_plan")
                            .then(|| array_value_to_string(batch.column(1), row).unwrap())
                    })
                })
                .unwrap();
            assert_eq!(
                plan.matches("NetworkCoalesceExec").count(),
                owners.len(),
                "limit_preserves_one_stage_per_owner: {plan}"
            );
            assert!(
                plan.matches("GlobalLimitExec").count() <= 1,
                "source_fetch_replaces_redundant_branch_limits: {plan}"
            );
            assert!(
                plan.lines()
                    .filter(|line| line.contains("TableScanExec"))
                    .all(|line| line.contains("fetch=10")),
                "source_fetch_is_visible: {plan}"
            );

            #[rustfmt::skip]
            let cases = [
                ("unfiltered_limit", "SELECT * FROM sys_scheduler LIMIT 10", 10, Some(10)),
                ("offset_uses_skip_plus_fetch", "SELECT * FROM sys_scheduler LIMIT 10 OFFSET 5", 10, Some(15)),
                ("zero_does_not_read", "SELECT * FROM sys_scheduler LIMIT 0", 0, Some(0)),
                ("nested_limits_keep_the_inner_bound", "SELECT * FROM (SELECT * FROM sys_scheduler LIMIT 12) LIMIT 10 OFFSET 5", 7, Some(12)),
                ("residual_precedes_limit", "SELECT * FROM sys_scheduler WHERE num_inbox >= 32 LIMIT 10", 10, None),
                ("sorting_precedes_limit", "SELECT * FROM sys_scheduler ORDER BY num_inbox DESC LIMIT 10 OFFSET 5", 10, None),
            ];
            for (invariant, sql, count, source_limit) in cases {
                // Remote admissions can outlive a previous LIMIT stream. Attribute
                // them to the request's execution identity, not the active test case.
                let result = session.execute(sql, QueryOptions {}).await.unwrap();
                let identity = (result.metadata.session_id.clone(), result.metadata.query_ts);
                let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
                assert_eq!(
                    batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    count,
                    "{invariant}: parallelism={parallelism}, single_owner={single_owner}"
                );
                if let Some(limit) = source_limit {
                    let captured = data
                        .lock()
                        .captured_rows
                        .remove(&identity)
                        .unwrap_or_default();
                    assert!(
                        captured.values().all(|rows| *rows == 64),
                        "admission_captures_whole_partitions: {invariant}: {captured:?}"
                    );
                    if limit == 0 {
                        assert!(captured.is_empty(), "zero_limit_does_not_poll_live_rows");
                    }
                }
                let values: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        let values = batch.column_by_name("num_inbox").unwrap();
                        (0..batch.num_rows()).map(|row| {
                            array_value_to_string(values, row)
                                .unwrap()
                                .parse::<u64>()
                                .unwrap()
                        })
                    })
                    .collect();
                if invariant == "residual_precedes_limit" {
                    assert!(
                        values.iter().all(|value| *value >= 32),
                        "late_matching_rows_survive"
                    );
                }
                if invariant == "sorting_precedes_limit" {
                    let mut expected: Vec<_> = partitions.0.iter().flat_map(|_| 0..64).collect();
                    expected.sort_unstable_by(|a, b| b.cmp(a));
                    assert_eq!(values, expected[5..15], "offset_follows_global_order");
                }
            }
        }
    }
}

async fn query_error(session: &dyn QuerySession, sql: &str) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        match session.execute(sql, QueryOptions {}).await {
            Err(error) => error.to_string(),
            Ok(result) => result
                .stream
                .try_collect::<Vec<_>>()
                .await
                .expect_err("unavailable_live_data_must_fail_the_query")
                .to_string(),
        }
    })
    .await
    .expect("failed_leader_tasks_must_terminate")
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduler_preserves_native_sql_and_required_leader_placement() {
    let data = Arc::new(Mutex::new(LiveData::default()));
    let fixture = setup_with_scanners(|node, scanners| {
        SchedulerAccess {
            node,
            data: Arc::clone(&data),
        }
        .register(scanners);
    })
    .await;
    for (partition, spec) in &fixture.partitions.0 {
        let start = spec.key_range.start();
        for (offset, status, waiting_inbox) in [
            (1, SchedulingStatus::Dormant, 0),
            (
                2,
                SchedulingStatus::Scheduled {
                    at: RoughTimestamp::from_unix_millis_clamped(MillisSinceEpoch::new(
                        1_744_010_000_000,
                    )),
                },
                3,
            ),
            (
                3,
                SchedulingStatus::BlockedOn(BlockedResource::InvokerConcurrency),
                7,
            ),
        ] {
            data.lock().rows.push((
                *partition,
                (
                    VQueueId::custom(start + offset, "scheduler"),
                    VQueueSchedulerStatus {
                        status,
                        waiting_inbox,
                        head_entry_id: (offset != 1).then_some(EntryId::new(
                            if offset == 3 {
                                EntryKind::StateMutation
                            } else {
                                EntryKind::Invocation
                            },
                            [7; 16],
                        )),
                        wait_stats: WaitStats {
                            blocked_on_invoker_concurrency_ms: 15,
                            blocked_on_throttling_rules_ms: 20,
                            blocked_on_invoker_throttling_ms: 25,
                            blocked_on_invoker_memory_ms: 30,
                            blocked_on_concurrency_rules_ms: 35,
                            blocked_on_lock_ms: 40,
                            blocked_on_deployment_concurrency_ms: 45,
                        },
                        ..Default::default()
                    },
                ),
            ));
        }
    }
    // Default replica preferences must not relax the live source's leader requirement.
    let env = environment(2, 1).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let session = session.as_ref();

    #[rustfmt::skip]
    let cases: &[(&str, &str, &[&[&str]])] = &[
        ("all_partitions_contribute_once",
         "SELECT COUNT(*), COUNT(DISTINCT id) FROM sys_scheduler",
         &[&["24", "24"]]),
        ("native_aggregation_combines_leaders",
         "SELECT status, COUNT(*) FROM sys_scheduler GROUP BY status",
         &[&["dormant", "8"], &["scheduled", "8"], &["blocked", "8"]]),
        ("nullable_head_is_not_an_empty_identifier",
         "SELECT COUNT(*) FROM sys_scheduler WHERE head_entry_id IS NULL AND blocked_on IS NULL",
         &[&["8"]]),
        ("scheduled_timestamp_is_conditional",
         "SELECT status, COUNT(*) FROM sys_scheduler WHERE scheduled_at IS NOT NULL GROUP BY status",
         &[&["scheduled", "8"]]),
        ("native_timestamps_preserve_exact_milliseconds",
         "SELECT COUNT(*) FROM sys_scheduler WHERE scheduled_at = to_timestamp_millis(1744010000000)",
         &[&["8"]]),
        ("fractional_second_does_not_round_to_a_stored_timestamp",
         "SELECT COUNT(*) FROM sys_scheduler WHERE scheduled_at = to_timestamp_millis(1744010000001)",
         &[&["0"]]),
        ("residual_dependencies_survive_projection",
         "SELECT SUM(num_inbox) FROM sys_scheduler WHERE upper(status) = 'BLOCKED'",
         &[&["56"]]),
        ("ordered_limit_and_offset_follow_global_order",
         "SELECT status FROM sys_scheduler ORDER BY partition_key LIMIT 1 OFFSET 2",
         &[&["blocked"]]),
    ];
    for &(invariant, sql, expected) in cases {
        assert_sql(session, invariant, sql, expected).await;
    }

    let (partition, spec) = fixture
        .partitions
        .0
        .iter()
        .find(|(id, _)| owner(*id).raw_id() == 2)
        .unwrap();
    let start = spec.key_range.start();
    let end = spec.key_range.end();
    let pk = start + 3;
    let predicate = format!("partition_key BETWEEN {start} AND {end}");
    data.lock().reads.clear();
    let metadata = assert_sql(
        session,
        "limit_follows_owner_residual",
        &format!("SELECT num_inbox FROM sys_scheduler WHERE {predicate} AND upper(status) = 'BLOCKED' LIMIT 1"),
        &[&["7"]],
    )
    .await;
    {
        let tasks = fixture.tasks.lock();
        let tasks: Vec<_> = tasks
            .iter()
            .filter(|task| {
                task.id.session_id == metadata.session_id && task.id.query_ts == metadata.query_ts
            })
            .collect();
        assert_eq!(tasks.len(), 1, "only_the_selected_leader_receives_work");
        assert_eq!(tasks[0].owner, owner(*partition));
        assert!(
            tasks[0].plan.contains("FilterExec"),
            "residual_is_a_native_owner_operator: {}",
            tasks[0].plan
        );
    }
    assert_eq!(
        data.lock().reads,
        [(owner(*partition), *partition, KeyRange::FULL)],
        "partition_pruning_precedes_whole_partition_capture"
    );
    assert!(
        fixture.placements.lock().iter().all(|(_, _, placement)| {
            placement.require_leader && !placement.options.require_leader
        }),
        "leader_requirement_survives_transport_without_a_session_override"
    );

    assert_sql(
        session,
        "display_columns_and_duration_units_are_preserved",
        &format!(
            "SELECT blocked_on, blocked_on_json,
            CAST(invoker_concurrency_block_duration AS BIGINT),
            CAST(throttling_rules_block_duration AS BIGINT),
            CAST(invoker_throttling_block_duration AS BIGINT),
            CAST(invoker_memory_block_duration AS BIGINT),
            CAST(concurrency_rules_block_duration AS BIGINT),
            CAST(lock_block_duration AS BIGINT),
            CAST(deployment_concurrency_block_duration AS BIGINT)
            FROM sys_scheduler WHERE partition_key = {pk}"
        ),
        &[&[
            "invoker-concurrency",
            r#"{"resource":"invoker-concurrency"}"#,
            "15",
            "20",
            "25",
            "30",
            "35",
            "40",
            "45",
        ]],
    )
    .await;

    let qid = VQueueId::custom(pk, "scheduler").to_string();
    let head = EntryId::new(EntryKind::StateMutation, [7; 16])
        .to_base_id(pk)
        .to_string();
    let other = VQueueId::custom(start + 2, "scheduler").to_string();
    #[rustfmt::skip]
    let derived_cases: &[(&str, String, &[&[&str]], usize)] = &[
        ("contradictory_identifier_branch_is_removed_before_union",
         format!("(id = '{qid}' AND partition_key = {}) OR (id = '{other}' AND partition_key = {})", start + 2, start + 2),
         &[&[&other]], 1),
        ("crossed_identifier_alternatives_do_not_dispatch",
         format!("(id = '{qid}' AND partition_key = {}) OR (id = '{other}' AND partition_key = {pk})", start + 2),
         &[], 0),
        ("malformed_alternative_does_not_widen_valid_queue_routing",
         format!("id IN ('{qid}', 'not-a-vqueue')"),
         &[&[&qid]], 1),
        ("malformed_alternative_does_not_widen_valid_head_routing",
         format!("head_entry_id IN ('{head}', 'not-an-entry')"),
         &[&[&qid]], 1),
        ("malformed_queue_equality_does_not_dispatch",
         "id = 'not-a-vqueue'".into(),
         &[], 0),
        ("malformed_queue_membership_does_not_dispatch",
         format!("id IN ('vq_1', 'vq_1💥', '{qid}_1', '{head}')"),
         &[], 0),
        ("malformed_head_membership_does_not_dispatch",
         format!("head_entry_id IN ('inv_1', '{head}_1', '{qid}')"),
         &[], 0),
        ("simplified_queue_bounds_preserve_routing",
         format!("id >= '{qid}' AND id <= '{qid}'"),
         &[&[&qid]], 1),
        ("simplified_head_bounds_preserve_routing",
         format!("head_entry_id BETWEEN '{head}' AND '{head}'"),
         &[&[&qid]], 1),
        ("lossy_exclusion_does_not_exclude_a_partition_key",
         format!("partition_key BETWEEN {} AND {pk} AND id != '{qid}'", start + 2),
         &[&[&other]], 1),
    ];
    for (invariant, predicate, expected, reads) in derived_cases {
        data.lock().reads.clear();
        let tasks_before = fixture.tasks.lock().len();
        assert_sql(
            session,
            invariant,
            &format!("SELECT id FROM sys_scheduler WHERE {predicate}"),
            expected,
        )
        .await;
        assert_eq!(data.lock().reads.len(), *reads, "{invariant}");
        if *reads == 0 {
            assert_eq!(
                fixture.tasks.lock().len(),
                tasks_before,
                "empty_targets_do_not_dispatch: {invariant}"
            );
        }
    }
    for filter in [
        format!("id = '{qid}'"),
        format!("head_entry_id = '{head}'"),
        format!("id IN ('{qid}', 'not-a-vqueue')"),
        format!("head_entry_id IN ('{head}', 'not-an-entry')"),
    ] {
        data.lock().reads.clear();
        assert_sql(
            session,
            "identifiers_derive_partition_targets",
            &format!("SELECT id, head_entry_id FROM sys_scheduler WHERE {filter}"),
            &[&[&qid, &head]],
        )
        .await;
        assert_eq!(
            data.lock().reads,
            [(owner(*partition), *partition, KeyRange::FULL)],
            "identifier_targeting_captures_only_its_partition"
        );
    }
    #[rustfmt::skip]
    let literal_cases = [
        // SQL predicate                                 count  partition reads
        ("id NOT IN ('not-a-vqueue')",                       "24", 8),
        ("head_entry_id != 'not-an-entry'",                  "16", 8),
        ("head_entry_id IS NULL",                            "8", 8),
        ("id > 'vq_'",                                      "24", 8),
        ("id < 'vq_'",                                       "0", 8),
        ("head_entry_id LIKE 'mut%'",                         "8", 8),
        ("id NOT IN ('not-a-vqueue', NULL)",                  "0", 0),
        ("scheduled_at = TIMESTAMP '1969-12-31 23:59:59.999'", "0", 0),
    ];
    for (predicate, count, reads) in literal_cases {
        data.lock().reads.clear();
        let tasks_before = fixture.tasks.lock().len();
        assert_sql(
            session,
            "malformed_literals_preserve_sql_truth_and_routing",
            &format!("SELECT COUNT(*) FROM sys_scheduler WHERE {predicate}"),
            &[&[count]],
        )
        .await;
        assert_eq!(
            data.lock().reads.len(),
            reads,
            "literal_routing_reads: {predicate}"
        );
        if reads == 0 {
            assert_eq!(
                fixture.tasks.lock().len(),
                tasks_before,
                "empty_literals_do_not_dispatch: {predicate}"
            );
        }
    }
    data.lock().reads.clear();
    assert_sql(
        session,
        "duplicate_keys_do_not_repeat_live_reads",
        &format!(
            "SELECT status FROM sys_scheduler WHERE partition_key IN ({}, {pk}, {pk})",
            start + 1
        ),
        &[&["dormant"], &["blocked"]],
    )
    .await;
    assert_eq!(
        data.lock().reads.len(),
        1,
        "disjoint_keys_share_one_partition_read"
    );
    assert_sql(session, "unrestricted_or_branch_keeps_other_leaders",
        &format!("SELECT COUNT(*) FROM sys_scheduler WHERE partition_key = {} OR upper(status) = 'BLOCKED'", start + 1),
        &[&["9"]]).await;

    data.lock().reads.clear();
    let tasks_before = fixture.tasks.lock().len();
    assert_sql(
        session,
        "contradictions_do_not_dispatch",
        "SELECT id FROM sys_scheduler WHERE partition_key = 1 AND partition_key = 2",
        &[],
    )
    .await;
    assert!(data.lock().reads.is_empty());
    assert_eq!(fixture.tasks.lock().len(), tasks_before);

    // Admission failure must fail the whole query, not yield a partial count.
    let count = format!("SELECT COUNT(*) FROM sys_scheduler WHERE {predicate}");
    for failure in [
        PartitionReadError::LiveCaptureUnavailable(*partition),
        PartitionReadError::NotLeader {
            partition: *partition,
            leader: None,
        },
    ] {
        data.lock().failure = Some(failure.clone());
        let error = query_error(session, &count).await;
        assert!(error.contains(&failure.to_string()), "{error}");
    }
    data.lock().failure = None;

    // A stale installed owner must be rejected before asking its live backend for rows.
    data.lock().reads.clear();
    fixture.move_leader_before_admission(
        owner(*partition),
        *partition,
        GenerationalNodeId::new(3, 1),
    );
    let error = query_error(session, &count).await;
    assert!(error.contains("storage ownership changed"), "{error}");
    assert!(data.lock().reads.is_empty(), "stale_leader_is_not_read");

    // A live storage replica is insufficient when no leader has been observed.
    let states = PartitionReplicaSetStates::default();
    let me = fixture.metadata.my_node_id();
    TaskCenter::current()
        .cluster_state()
        .clone()
        .updater()
        .upsert_node_state(me, NodeState::Alive);
    states.note_observed_membership(
        *partition,
        LeadershipState::default(),
        &ReplicaSetState {
            version: Version::MIN,
            members: vec![MemberState {
                node_id: me.as_plain(),
                durable_lsn: Lsn::INVALID,
            }],
        },
        &None,
    );
    let scanners = SourceRegistry::new(
        create_partition_locator(
            PartitionRouting::new(states, TaskCenter::current()),
            fixture.metadata.clone(),
        ),
        fixture.metadata.clone(),
        TaskCenter::current().cluster_state().clone(),
    );
    assert!(matches!(
        scanners
            .get_partition_target_node(*partition, PartitionPlacement::default())
            .unwrap(),
        PartitionLocation::Local
    ));
    let env = environment(2, 1).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        Partitions(vec![(*partition, spec.clone())]),
        scanners,
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let before = fixture.tasks.lock().len();
    let error = query_error(session.as_ref(), &count).await;
    assert!(error.contains("node lookup for partition"), "{error}");
    assert_eq!(
        fixture.tasks.lock().len(),
        before,
        "no_replica_fallback_for_live_state"
    );
}
