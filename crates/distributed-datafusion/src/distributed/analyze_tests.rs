// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ANALYZE` over the real task transport, with three owners.

use super::super::analyze::AnalyzeCollector;
use super::*;

/// Writes `count` single-key states, alternately 4 and 64 bytes long; returns
/// how many of each length every owner holds.
async fn write_states(
    fixture: &mut MultiFixture<impl NetworkSender>,
    count: usize,
) -> HashMap<GenerationalNodeId, [usize; 2]> {
    let mut held = HashMap::new();
    for store in &mut fixture.stores {
        let range = store.partition_key_range();
        let owner = owner(store.partition_id());
        let mut tx = store.transaction();
        for i in 0..count {
            let service = ServiceId::new(None, "analyze", format!("object-{i}"));
            if range.contains(&service.partition_key()) {
                let long = i % 2;
                tx.put_user_state(
                    &service,
                    &Bytes::from_static(b"k"),
                    Bytes::from(vec![b'x'; if long == 0 { 4 } else { 64 }]),
                )
                .unwrap();
                held.entry(owner).or_insert([0; 2])[long] += 1;
            }
        }
        tx.commit().await.unwrap();
    }
    held
}

/// The text of every row's plan column.
async fn run(session: &dyn QuerySession, sql: &str) -> (QueryMetadata, String) {
    let (metadata, batches) = tokio::time::timeout(Duration::from_secs(20), async {
        let result = session.execute(sql, QueryOptions {}).await.unwrap();
        let metadata = result.metadata;
        (
            metadata,
            result.stream.try_collect::<Vec<_>>().await.unwrap(),
        )
    })
    .await
    .expect("queries must end");
    let column = |batch: &RecordBatch| batch.num_columns() - 1;
    let text = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row| array_value_to_string(batch.column(column(batch)), row).unwrap())
        })
        .collect::<Vec<_>>()
        .join("\n");
    (metadata, text)
}

/// The metrics a line of an aggregated plan shows for `name`.
fn metric(line: &str, name: &str) -> Option<String> {
    let metrics = &line[line.find("metrics=[")? + "metrics=[".len()..];
    metrics
        .split(", ")
        .find_map(|metric| metric.strip_prefix(&format!("{name}=")))
        .map(|value| value.trim_end_matches(']').to_owned())
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_shows_what_each_owner_did() {
    let mut fixture = setup().await;
    // Below 1,000 rows per owner, so that counts display exactly.
    let held = write_states(&mut fixture, 900).await;
    assert_eq!(held.len(), 3);
    // One lane per owner: every owner reads two or three partitions in turn,
    // each with a fresh copy of the plan below its partition reads.
    let session = fixture
        .engine_with_pushdown(1, 128, true)
        .create_session(SessionOptions::default())
        .unwrap();
    let analyzed = |metadata: &QueryMetadata| {
        let tasks = fixture.tasks.lock();
        tasks
            .iter()
            .filter(|task| {
                task.id.session_id == metadata.session_id && task.id.query_ts == metadata.query_ts
            })
            .map(|task| {
                task.context
                    .session_config()
                    .get_extension::<AnalyzeCollector>()
                    .is_some()
            })
            .collect::<Vec<_>>()
    };
    // Nine values, so the compact display summarizes them.
    let sql = "SELECT key_length FROM state WHERE value_length IN (4, 5, 6, 7, 8, 9, 10, 11, 12)";

    let (metadata, text) = run(session.as_ref(), &format!("EXPLAIN ANALYZE {sql}")).await;
    assert_eq!(analyzed(&metadata), [true; 3], "{text}");
    // Each stage: its boundary's line, then its plan's.
    let stages: Vec<Vec<&str>> = text.lines().fold(Vec::new(), |mut stages, line| {
        if line.contains("NetworkCoalesceExec") {
            stages.push(vec![line]);
        } else if let Some(stage) = stages.last_mut() {
            stage.push(line);
        }
        stages
    });
    for (owner, [short, long]) in &held {
        let stage = stages
            .iter()
            .find(|stage| {
                stage
                    .iter()
                    .any(|line| line.contains(&format!("owner={owner},")))
            })
            .unwrap();
        // The task sent the rows its plan output, in batches, and took time.
        assert_eq!(
            metric(stage[0], "transport_rows"),
            Some(short.to_string()),
            "{text}"
        );
        for task in ["transport_batches", "transport_bytes", "task_elapsed"] {
            assert!(
                metric(stage[0], task).is_some_and(|value| value != "0"),
                "{text}"
            );
        }
        let line = |prefix: &str| {
            *stage
                .iter()
                .find(|line| line.trim_start().starts_with(prefix))
                .unwrap_or_else(|| panic!("{prefix} in {text}"))
        };
        let legacy = line("TableScanExec: table=state.legacy");
        // The leaf is shared by every partition's copy: read once, counted once.
        assert_eq!(
            metric(legacy, "read_rows"),
            Some((short + long).to_string()),
            "{text}"
        );
        assert_eq!(
            metric(legacy, "output_rows"),
            Some(short.to_string()),
            "{text}"
        );
        // The union below the partition reads runs as a fresh copy per partition:
        // the copies add up.
        assert_eq!(
            metric(line("UnionExec"), "output_rows"),
            Some(short.to_string()),
            "{text}"
        );
        assert_eq!(
            metric(line("FilterExec"), "output_rows"),
            Some(short.to_string()),
            "{text}"
        );
        let partitions = fixture
            .partitions
            .0
            .iter()
            .filter(|(partition, _)| super::owner(*partition) == *owner)
            .count();
        assert_eq!(
            metric(line("PartitionReadExec"), "partition_scopes"),
            Some(partitions.to_string()),
            "{text}"
        );
    }
    assert_eq!(text.matches("tasks_completed=1").count(), 3, "{text}");
    // The coordinator's own operators show the numbers of the copies the
    // distributed runtime rebuilt and ran.
    let union = text
        .lines()
        .take_while(|line| !line.contains("NetworkCoalesceExec"))
        .find(|line| line.trim_start().starts_with("UnionExec"))
        .unwrap();
    let short: usize = held.values().map(|[short, _]| short).sum();
    assert_eq!(
        metric(union, "output_rows"),
        Some(short.to_string()),
        "{text}"
    );
    assert!(
        !text.contains("metrics_"),
        "no task is missing anything: {text}"
    );
    // The compact display applies to the reported tree too.
    assert!(text.contains("<set#1: 9 values, 9 distinct"), "{text}");

    // VERBOSE and PGJSON render the same reported tree natively: the full form
    // names each metric's owner, stage and task.
    let (_, verbose) = run(session.as_ref(), &format!("EXPLAIN ANALYZE VERBOSE {sql}")).await;
    assert!(!verbose.contains("<set#"), "{verbose}");
    for owner in held.keys() {
        assert!(
            verbose.contains(&format!("tasks_completed{{owner={owner}, stage=")),
            "{verbose}"
        );
        assert!(
            verbose.contains(&format!("read_rows{{partition=0, owner={owner}, stage=")),
            "{verbose}"
        );
    }
    let (_, json) = run(
        session.as_ref(),
        &format!("EXPLAIN (ANALYZE, FORMAT PGJSON) {sql}"),
    )
    .await;
    assert_eq!(
        json.matches("\"Node Type\": \"TableScanExec\"").count(),
        6,
        "{json}"
    );
    assert!(json.contains("read_rows"), "{json}");

    // Plain EXPLAIN executes nothing; a SELECT after an ANALYZE in the same
    // session asks for no report, and its tasks collect nothing.
    let (metadata, _) = run(session.as_ref(), &format!("EXPLAIN {sql}")).await;
    assert!(analyzed(&metadata).is_empty());
    let (metadata, _) = run(session.as_ref(), sql).await;
    assert_eq!(analyzed(&metadata), [false; 3]);
}

/// On real stage plans: a report attaches only to its own stage, from the
/// stage's owner. A report of another owner's stage, of the same shape but
/// reading other partitions, doesn't describe the stage; a report from a node
/// other than the stage's owner attaches nothing at all.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_attach_only_to_their_own_stage_and_owner() {
    use super::super::analyze::{Collected, TaskReport, build_report, shape};
    use super::super::analyze_view::{Copier, Reports, attach};

    let fixture = setup().await;
    let env = environment(1, 128).with_distributed_execution(fixture.network.clone());
    let ctx = SessionContext::new_with_state(env.build_session_state().unwrap());
    ctx.register_table(
        "state",
        StateTable::create_provider(fixture.partitions.clone(), &fixture.scanners),
    )
    .unwrap();
    let plan = ctx
        .sql("SELECT key_length FROM state WHERE value_length = 4")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut stages = Vec::new();
    plan.apply(|node| {
        if let Some(boundary) = node.downcast_ref::<NetworkCoalesceExec>()
            && let Stage::Local(stage) = boundary.input_stage()
        {
            let mut owner = None;
            stage.plan.apply(|node| {
                owner = owner.or_else(|| super::super::source::source_owner(node.as_ref()));
                Ok(TreeNodeRecursion::Continue)
            })?;
            stages.push((stage.num, Arc::clone(&stage.plan), owner.unwrap()));
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    stages.sort_by_key(|(_, _, owner)| *owner);
    let [(a, a_plan, a_owner), (b, _, b_owner), (c, c_plan, c_owner)] = stages.as_slice() else {
        panic!("three owners' stages")
    };
    let received = |owner, plan: &Arc<_>| TaskReport::Received {
        owner,
        report: Box::new(build_report(
            shape(plan).as_ref(),
            Collected::default(),
            restate_types::net::distributed_query::TaskOutcome::Completed,
        )),
    };
    let reports = Reports::from([
        // Its own plan, from its own owner: attaches.
        ((*a, 0), received(*a_owner, a_plan)),
        // Another owner's stage plan: the same kinds of nodes, other sources.
        ((*b, 0), received(*b_owner, a_plan)),
        // Its own plan, from another node.
        ((*c, 0), received(*a_owner, c_plan)),
    ]);
    let shown_with_metrics =
        datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(
            attach(plan.children()[0], &reports, &mut Copier::default()).as_ref(),
        )
        .indent(true)
        .to_string();
    let stage = |owner: &GenerationalNodeId| {
        shown_with_metrics
            .split("NetworkCoalesceExec")
            .find(|stage| stage.contains(&format!("owner={owner},")))
            .unwrap_or_else(|| panic!("{shown_with_metrics}"))
            .to_owned()
    };
    assert!(
        stage(a_owner).contains("tasks_completed=1"),
        "{shown_with_metrics}"
    );
    assert!(!stage(a_owner).contains("metrics_"), "{shown_with_metrics}");
    assert!(
        stage(b_owner).contains("metrics_shape_mismatch=1"),
        "{shown_with_metrics}"
    );
    assert!(
        stage(c_owner).contains("metrics_wrong_owner=1"),
        "{shown_with_metrics}"
    );
    assert!(
        !stage(c_owner).contains("tasks_completed"),
        "{shown_with_metrics}"
    );
}
