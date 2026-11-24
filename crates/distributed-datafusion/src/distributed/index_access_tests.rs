// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Index access over the three-owner transport fixture: the same rows as a scan,
//! through the stage tables, with global limits counting only qualifying rows.
//!
//! The reference for each query is the same query over an unrestricted scan:
//! its table is a subquery under a `LIMIT`, which filters aren't pushed through.

use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryValue, Stage, Status,
    WriteVQueueTable, stats::EntryStatistics,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, VQueueId};
use restate_util_string::ToReString;

use super::*;

/// The partition holding the busy queue: owned by a remote node, not the
/// coordinator (see `owner`).
const BUSY_PARTITION: usize = 1;

/// Entries of the busy queue, spanning two fetch windows.
/// The last three carry a deployment, so a residual on it rejects every row of
/// the first window. Every partition also has a small queue in each stage.
const BUSY: u128 = 600;

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn stage_index_access_matches_scans_across_windows_and_owners() {
    let mut fixture = setup().await;
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
    let busy_key = fixture.stores[BUSY_PARTITION].partition_key_range().start();
    assert_ne!(
        owner(fixture.stores[BUSY_PARTITION].partition_id()),
        fixture.metadata.my_node_id()
    );
    let busy = VQueueId::custom(busy_key, "busy");
    let mut quiet = Vec::new();
    for (partition, store) in fixture.stores.iter_mut().enumerate() {
        let key = store.partition_key_range().start();
        let mut tx = store.transaction();
        let q = VQueueId::custom(key, "quiet");
        // Equal run times: the stage order ties on time and falls back to sequence.
        let mut put = |qid: &VQueueId, n: u128, stage: Stage, deployment: Option<&str>| {
            let entry_key = EntryKey::new(
                false,
                now,
                n as u64 + 1,
                EntryId::new(EntryKind::Invocation, n.to_be_bytes()),
            );
            let stats = EntryStatistics::new(at, entry_key.run_at());
            let metadata = EntryMetadata {
                deployment: deployment.map(|d| d.to_restring()),
                ..Default::default()
            };
            let status = match stage {
                Stage::Inbox => Status::New,
                _ => Status::Started,
            };
            tx.put_vqueue_inbox(
                qid,
                stage,
                &entry_key,
                &EntryValue {
                    status,
                    metadata: metadata.clone(),
                    stats: stats.clone(),
                },
            );
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid,
                    target: &EntryTargetRef::Service {
                        scope: None,
                        service: "svc",
                        handler: "run",
                    },
                },
                EntryStateRef {
                    stage,
                    status,
                    entry_key: &entry_key,
                    metadata: &metadata,
                    stats: &stats,
                },
            );
        };
        if partition == BUSY_PARTITION {
            for n in 0..BUSY {
                put(&busy, n, Stage::Inbox, (n >= BUSY - 3).then_some("late"));
            }
        }
        for n in 0..3 {
            put(&q, 1_000 + n, Stage::Inbox, None);
        }
        put(&q, 1_003, Stage::Running, None);
        put(&q, 1_004, Stage::Paused, None);
        put(&q, 1_005, Stage::Finished, None);
        tx.commit().await.unwrap();
        quiet.push(q.to_string());
    }

    for parallelism in [1, 4] {
        for distributed in [false, true] {
            // Both placements and both parallelisms, in two runs: this fixture is
            // large enough to slow concurrently running timing-sensitive tests.
            if distributed != (parallelism > 1) {
                continue;
            }
            let env = crate::DataFusionEnv::new(
                64 * 1024 * 1024,
                None,
                Some(parallelism),
                &HashMap::from([("datafusion.execution.batch_size".into(), "64".into())]),
            )
            .unwrap()
            .with_mock_clock(restate_clock::MockClock::new())
            .unwrap();
            let env = if distributed {
                env.with_distributed_execution(fixture.network.clone())
            } else {
                env
            };
            let scanners = if distributed {
                fixture.scanners.clone()
            } else {
                let scanners = SourceRegistry::local_only(fixture.metadata.clone());
                scanners.register_offline_reads(
                    Arc::clone(&fixture.store_manager),
                    crate::mocks::query_io(),
                );
                scanners.register_partition_scanner::<SysVqueueEntryStatusTable>(Arc::new(
                    CheckedScanner {
                        node: fixture.metadata.my_node_id(),
                        check_owner: false,
                        inner: Arc::new(SysVqueueEntryStatusTable::create_local_scanner()),
                        reads: Arc::clone(&fixture.reads),
                    },
                ));
                scanners
            };
            env.register_table::<SysVqueueEntryStatusTable>(
                "public",
                "sys_vqueue_entry_status",
                SysVqueueEntryStatusTable::create_provider(fixture.partitions.clone(), &scanners),
            )
            .unwrap();
            let engine = DataFusionQueryEngine::from_env(env, None);
            let session = engine.create_session(SessionOptions::default()).unwrap();
            // The same query over an unrestricted scan, and the accesses it planned.
            let scanned = async |sql: &str| {
                *fixture.reads.lock() = Reads::default();
                let sql = sql.replacen(
                    "FROM sys_vqueue_entry_status",
                    "FROM (SELECT * FROM sys_vqueue_entry_status LIMIT 1000000) \
                     AS sys_vqueue_entry_status",
                    1,
                );
                let rows = crate::mocks::rows(session.as_ref(), &sql).await;
                let reads = std::mem::take(&mut *fixture.reads.lock());
                assert!(
                    !reads.accesses.is_empty()
                        && reads
                            .accesses
                            .iter()
                            .all(|(_, access)| matches!(access, PrimaryRead::Scan)),
                    "the_reference_scans: {sql}: {:?}",
                    reads.accesses
                );
                rows
            };

            let late = |n: u128| {
                EntryId::new(EntryKind::Invocation, n.to_be_bytes())
                    .display(busy_key)
                    .to_string()
            };
            #[rustfmt::skip]
            let cases = [
                (format!("vqueue_id = '{busy}'"), true, 600),
                (format!("vqueue_id IN ('{}', '{}', '{busy}')", quiet[0], quiet[1]), true, 612),
                (format!("(vqueue_id = '{}' AND stage = 'running') OR (vqueue_id = '{}' AND has_lock = false)",
                    quiet[0], quiet[1]), true, 7),
                ("stage IN ('running', 'paused')".into(), true, 2 * fixture.stores.len()),
                (format!("vqueue_id = '{busy}' AND next_at >= to_timestamp_millis({})", now.as_u64()), true, 600),
                ("stage = 'finished'".into(), false, fixture.stores.len()),
                (format!("vqueue_id = '{busy}' OR num_attempts > 3"), false, 600),
                (format!("entry_id = '{}'", late(BUSY - 1)), false, 1),
            ];
            for (filter, indexed, count) in cases {
                let sql = format!(
                    "SELECT entry_id, stage, deployment FROM sys_vqueue_entry_status \
                     WHERE {filter} ORDER BY entry_id"
                );
                *fixture.reads.lock() = Reads::default();
                let rows = crate::mocks::rows(session.as_ref(), &sql).await;
                assert_eq!(rows.len(), count, "{sql}");
                let reads = std::mem::take(&mut *fixture.reads.lock());
                assert_eq!(
                    rows,
                    scanned(&sql).await,
                    "index_and_scan_agree: parallelism={parallelism} distributed={distributed}: {sql}"
                );
                assert!(!reads.accesses.is_empty(), "{sql}");
                assert_eq!(
                    reads
                        .accesses
                        .iter()
                        .all(|(_, access)| matches!(access, PrimaryRead::Index(index)
                            if index.name() == "_idx_entry_by_queue")),
                    indexed,
                    "owners_receive_the_planned_access: {sql}: {:?}",
                    reads.accesses
                );
            }

            // The residual rejects every row of the first window. Ordered and
            // unordered global limits count only qualifying rows.
            let residual = format!(
                "FROM sys_vqueue_entry_status WHERE vqueue_id = '{}' AND deployment = 'late'",
                busy
            );
            let mut qualifying: Vec<_> = (BUSY - 3..BUSY).map(late).collect();
            qualifying.sort();
            let sql = format!("SELECT entry_id {residual} ORDER BY entry_id LIMIT 2 OFFSET 1");
            assert_eq!(
                crate::mocks::rows(session.as_ref(), &sql).await,
                [[qualifying[1].clone()], [qualifying[2].clone()]],
                "{sql}"
            );
            let sql = format!("SELECT entry_id {residual} LIMIT 1 OFFSET 2");
            let rows = crate::mocks::rows(session.as_ref(), &sql).await;
            assert!(
                rows.len() == 1 && qualifying.contains(&rows[0][0]),
                "unordered_limits_count_qualifying_rows: {sql}: {rows:?}"
            );
            // The leaf reports the index work: with the deployment, which the
            // stage records don't hold exactly, every reference of the queue
            // was fetched and matched; navigation evaluated at least those
            // keys. A remote task reports it to the coordinator.
            let analyze = async |columns: &str| {
                let sql = format!(
                    "EXPLAIN ANALYZE SELECT {columns} FROM sys_vqueue_entry_status \
                     WHERE vqueue_id = '{busy}'"
                );
                crate::mocks::rows(session.as_ref(), &sql)
                    .await
                    .concat()
                    .join("\n")
            };
            let plan = analyze("entry_id, deployment").await;
            let metric = |plan: &str, name: &str| -> u64 {
                let at = plan
                    .find(&format!("{name}="))
                    .unwrap_or_else(|| panic!("{name} missing: {plan}"));
                plan[at + name.len() + 1..]
                    .split(|c: char| !c.is_ascii_digit())
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap()
            };
            assert_eq!(
                [
                    metric(&plan, "index_references"),
                    metric(&plan, "primary_keys_fetched"),
                    metric(&plan, "primary_missing"),
                    metric(&plan, "primary_mismatched"),
                ],
                [600, 600, 0, 0],
                "index_work_metrics: distributed={distributed}: {plan}"
            );
            assert!(metric(&plan, "index_keys_visited") >= 600, "{plan}");
            // Without it, the stage records alone: the same keys, no status
            // fetched, no value decoded for a key column.
            let plan = analyze("entry_id").await;
            assert!(
                metric(&plan, "stage_keys_visited") >= 600
                    && metric(&plan, "stage_values_decoded") == 0
                    && !plan.contains("primary_keys_fetched"),
                "covered_reads_fetch_nothing: distributed={distributed}: {plan}"
            );

            // An unrestricted ordered limit keeps the scan and its sort.
            let sql = "SELECT entry_id FROM sys_vqueue_entry_status ORDER BY entry_id LIMIT 5";
            *fixture.reads.lock() = Reads::default();
            let rows = crate::mocks::rows(session.as_ref(), sql).await;
            let reads = std::mem::take(&mut *fixture.reads.lock());
            assert!(
                reads
                    .accesses
                    .iter()
                    .all(|(_, access)| matches!(access, PrimaryRead::Scan)),
                "{sql}: {:?}",
                reads.accesses
            );
            assert_eq!(rows, scanned(sql).await, "{sql}");
        }
    }
}

/// The workflow page through the entry index, across eight partitions on three
/// owners: the same rows as a scan, newest created first, with the query's other
/// conditions applied before its LIMIT and OFFSET, whatever they are. Another
/// order or a tie-breaker scans every partition instead, with the same rows.
/// Each partition takes its own path, shown by invocations the index lacks:
/// - most read the index;
/// - one hasn't fully migrated to vqueues, and one has no Complete index: they
///   scan, filter, and keep the newest rows the query needs; the first holds
///   73 page matches, more than the page and its OFFSET reach;
/// - one holds a service with more handlers than one read merges: it scans
///   for that service.
///
/// With one lane per partition the owners merge their partitions; with fewer
/// lanes they concatenate them and the plan sorts.
#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_page_reads_the_entry_index_across_partitions_and_owners() {
    use restate_storage_api::fsm_table::WriteFsmTable;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InFlightInvocationMetadata, InvocationStatus, StatusTimestamps,
        WriteInvocationStatusTable,
    };
    use restate_types::config::Configuration;
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::invocation::{
        InvocationTarget, VirtualObjectHandlerType, WorkflowHandlerType,
    };
    use restate_types::partitions::features::PersistedFeatures;
    use restate_types::vqueues::{EntryTargetExt, Seq};

    use crate::tables::invocation_status::SysInvocationStatusTable;

    const INDEX: &str = "_idx_entry_by_service_created";
    /// Not migrated, and no Complete index: these partitions scan.
    const UNMIGRATED: usize = 5;
    const INCOMPLETE: usize = 6;
    /// Holds the service with more handlers than one read merges.
    const MANY: usize = 3;

    let reads = Arc::new(Mutex::new(Reads::default()));
    let mut fixture = setup_with_scanners({
        let reads = Arc::clone(&reads);
        move |node, sources| {
            sources.register_declared_scanner::<SysInvocationStatusTable>(Arc::new(
                CheckedScanner {
                    node,
                    check_owner: true,
                    inner: Arc::new(SysInvocationStatusTable::create_local_scanner()),
                    reads: Arc::clone(&reads),
                },
            ));
        }
    })
    .await;
    let mut tracking = Configuration::default();
    tracking.common.experimental.set_track_entry_indexes(true);
    let mut reset = Configuration::default();
    reset.common.experimental.set_entry_indexes_reset(true);
    Configuration::set(tracking.clone());

    const BASE: u64 = 1_744_010_000_000;
    let page = |key: &str| {
        InvocationTarget::workflow("Promises", key, "promise", WorkflowHandlerType::Shared)
    };
    let partitions = fixture.stores.len() as u64;
    for (partition, store) in fixture.stores.iter_mut().enumerate() {
        let config = if partition == INCOMPLETE {
            &reset
        } else {
            &tracking
        };
        store
            .verify_and_run_migrations(Default::default(), config)
            .await
            .unwrap();
        let start = store.partition_key_range().start();
        let mut tx = store.transaction();
        let mut sequence = 0;
        let mut put = |target: &InvocationTarget, created: u64, entry: bool| {
            sequence += 1;
            let id = InvocationId::from_parts(
                start + sequence % 7,
                InvocationUuid::from_u128(u128::from(sequence) << 8 | partition as u128),
            );
            let created = MillisSinceEpoch::new(created);
            let seq = Some(Seq::new(sequence));
            let status = if sequence.is_multiple_of(3) {
                let mut completed = CompletedInvocation::mock_neo();
                completed.invocation_target = target.clone();
                completed.timestamps = StatusTimestamps::init(created);
                completed.creation_seq = seq;
                InvocationStatus::Completed(completed)
            } else {
                let mut invoked = InFlightInvocationMetadata::mock();
                invoked.invocation_target = target.clone();
                invoked.timestamps = StatusTimestamps::init(created);
                invoked.creation_seq = seq;
                InvocationStatus::Invoked(invoked)
            };
            tx.put_invocation_status(&id, &status).unwrap();
            if entry {
                // The entry's stage follows its invocation's status.
                let (stage, status) = if sequence.is_multiple_of(3) {
                    (Stage::Finished, Status::Succeeded)
                } else if sequence.is_multiple_of(2) {
                    (Stage::Inbox, Status::New)
                } else {
                    (Stage::Running, Status::Started)
                };
                let created = UniqueTimestamp::from_unix_millis_unchecked(created);
                let key =
                    EntryKey::new(false, created.to_unix_millis(), sequence, EntryId::from(id));
                let qid = VQueueId::custom(id.partition_key(), "q");
                let stats = EntryStatistics::new(created, key.run_at());
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
                        stats: &stats,
                    },
                );
                // And its stage record, as the scheduler writes it.
                tx.put_vqueue_inbox(
                    &qid,
                    stage,
                    &key,
                    &EntryValue {
                        status,
                        metadata: EntryMetadata::default(),
                        stats,
                    },
                );
            }
        };
        let p = partition as u64;
        for n in 0..12u64 {
            // Interleaved across partitions, distinct per page invocation.
            let created = BASE + (n * partitions + p) * 10;
            put(&page(&format!("w{p}-{n}")), created, true);
            // Same service and handler, rejected by the query's other conditions,
            // some of them newer than every page invocation: a virtual object,
            // and a plain service without a key.
            let object = InvocationTarget::virtual_object(
                "Promises",
                format!("o{p}-{n}"),
                "promise",
                VirtualObjectHandlerType::Shared,
            );
            put(&object, created + 5_000, true);
            put(
                &InvocationTarget::service("Promises", "promise"),
                created,
                true,
            );
            put(
                &InvocationTarget::workflow(
                    "Promises",
                    format!("w{p}-{n}"),
                    "run",
                    WorkflowHandlerType::Workflow,
                ),
                created + 1,
                true,
            );
        }
        // More page invocations than the page shows, interleaved with the other
        // partitions': the scanning partition must keep its newest 51.
        if partition == UNMIGRATED {
            for k in 0..60u64 {
                put(&page(&format!("x{p}-{k}")), BASE + k * 15 + 3, true);
            }
        }
        // Without an entry: found only where the partition scans.
        put(&page(&format!("probe{p}")), BASE - 1_000, false);
        // The declared Doss page's service: more invocations than its LIMIT,
        // on two partitions and two handlers.
        let doss = match partition {
            0 => 300,
            1 => 20,
            _ => 0,
        };
        for k in 0..doss {
            let handler = if k % 2 == 0 { "a" } else { "b" };
            put(
                &InvocationTarget::service("Doss", handler),
                BASE + k * 7,
                true,
            );
        }
        if partition == MANY {
            for handler in 0..257u64 {
                put(
                    &InvocationTarget::service("Many", format!("h{handler}")),
                    BASE + handler * 3,
                    true,
                );
            }
        }
        tx.put_state_machine_features(&PersistedFeatures {
            vqueues: partition != UNMIGRATED,
            ..Default::default()
        })
        .unwrap();
        tx.commit().await.unwrap();
    }

    // As the UI sends it.
    let exact = include_str!("../ui_queries/workflow_page.sql");
    for parallelism in [1, 4] {
        let env = crate::DataFusionEnv::new(
            64 * 1024 * 1024,
            None,
            Some(parallelism),
            &HashMap::from([("datafusion.execution.batch_size".into(), "8".into())]),
        )
        .unwrap()
        .with_mock_clock(restate_clock::MockClock::new())
        .unwrap()
        .with_distributed_execution(fixture.network.clone());
        env.register_table::<SysInvocationStatusTable>(
            "public",
            "sys_invocation_status",
            SysInvocationStatusTable::create_provider(
                fixture.partitions.clone(),
                &fixture.scanners,
            ),
        )
        .unwrap();
        let engine = DataFusionQueryEngine::from_env(env, None);
        let session = engine.create_session(SessionOptions::default()).unwrap();
        // A query and the accesses its owners received.
        let run = async |sql: &str| {
            *reads.lock() = Reads::default();
            let rows = crate::mocks::rows(session.as_ref(), sql).await;
            (rows, std::mem::take(&mut reads.lock().accesses))
        };
        // The same query over an unrestricted scan.
        let scanned = async |sql: &str| {
            let sql = sql.replacen(
                "FROM sys_invocation_status",
                "FROM (SELECT * FROM sys_invocation_status LIMIT 1000000) AS sys_invocation_status",
                1,
            );
            let (rows, accesses) = run(&sql).await;
            assert!(
                accesses
                    .iter()
                    .all(|(_, access)| matches!(access, PrimaryRead::Scan))
            );
            rows
        };
        let through_index = |accesses: &[(PartitionId, PrimaryRead)], sql: &str| {
            assert_eq!(accesses.len(), fixture.stores.len(), "{sql}");
            assert!(
                accesses.iter().all(|(_, access)| matches!(access, PrimaryRead::Index(index) if index.name() == INDEX)),
                "every_owner_reads_the_index: {sql}: {accesses:?}"
            );
        };
        // Oldest first, the index is read backwards. Past the probes, which
        // only the partitions that scan can find (below).
        let oldest = exact.replace(
            "ORDER BY created_at DESC",
            &format!("AND created_at >= to_timestamp_millis({BASE}) ORDER BY created_at ASC"),
        );
        for sql in [
            exact.to_owned(),
            exact.replace("LIMIT 51", "LIMIT 10 OFFSET 45"),
            "SELECT id FROM sys_invocation_status WHERE target_service_name = 'Many' \
             ORDER BY created_at DESC NULLS LAST LIMIT 3"
                .to_owned(),
            oldest.clone(),
            oldest.replace("LIMIT 51", "LIMIT 10 OFFSET 45"),
            "SELECT id FROM sys_invocation_status WHERE target_service_name = 'Many' \
             ORDER BY created_at ASC NULLS LAST LIMIT 3"
                .to_owned(),
        ] {
            let (rows, accesses) = run(&sql).await;
            through_index(&accesses, &sql);
            assert!(!rows.is_empty(), "{sql}");
            assert_eq!(
                rows,
                scanned(&sql).await,
                "parallelism {parallelism}: {sql}"
            );
        }
        assert_eq!(run(exact).await.0.len(), 51);
        // A condition on the status reads the status index on every owner,
        // with the scan's rows.
        let running = exact.replace("target_service_key IS NOT NULL", "status = 'invoked'");
        let (rows, accesses) = run(&running).await;
        assert_eq!(accesses.len(), fixture.stores.len(), "{running}");
        assert!(
            accesses.iter().all(|(_, access)| matches!(
                access,
                PrimaryRead::Index(index) if index.name() == "_idx_entry_by_status_created"
            )),
            "{running}: {accesses:?}"
        );
        assert!(!rows.is_empty(), "{running}");
        assert_eq!(
            rows,
            scanned(&running).await,
            "parallelism {parallelism}: {running}"
        );
        for sql in [
            // Ties with another key, which the query orders by.
            "SELECT id, created_at FROM sys_invocation_status WHERE target_service_name = 'Promises' \
             AND target_handler_name = 'promise' ORDER BY created_at DESC NULLS LAST, id LIMIT 40"
                .to_owned(),
        ] {
            let (rows, accesses) = run(&sql).await;
            assert_eq!(accesses.len(), fixture.stores.len(), "{sql}");
            assert!(
                accesses
                    .iter()
                    .all(|(_, access)| matches!(access, PrimaryRead::Scan)),
                "{sql}: {accesses:?}"
            );
            assert!(!rows.is_empty(), "{sql}");
            assert_eq!(
                rows,
                scanned(&sql).await,
                "parallelism {parallelism}: {sql}"
            );
        }
        // Only the partitions that scan find their invocation without an entry,
        // older than every other.
        let probes = format!(
            "SELECT target_service_key FROM sys_invocation_status \
             WHERE target_service_name = 'Promises' AND target_handler_name = 'promise' \
             AND created_at < to_timestamp_millis({BASE}) \
             ORDER BY created_at DESC NULLS LAST LIMIT 20"
        );
        for probes in [probes.clone(), probes.replace("DESC", "ASC")] {
            let (mut rows, accesses) = run(&probes).await;
            through_index(&accesses, &probes);
            rows.sort();
            assert_eq!(
                rows,
                [
                    [format!("probe{UNMIGRATED}")],
                    [format!("probe{INCOMPLETE}")]
                ],
                "parallelism {parallelism}: {probes}"
            );
        }
        assert_eq!(scanned(&probes).await.len(), fixture.stores.len());
        let plan = crate::mocks::rows(session.as_ref(), &format!("EXPLAIN {exact}"))
            .await
            .concat()
            .join("\n");
        // One lane per partition: owners and the coordinator merge, nothing sorts.
        // Fewer lanes concatenate partitions, and the plan sorts.
        assert!(
            plan.contains(INDEX) && plan.contains("fallback_fetch=51"),
            "{plan}"
        );
        assert_eq!(
            !plan.contains("SortExec"),
            parallelism == 4,
            "parallelism {parallelism}: {plan}"
        );
    }

    // The repository's exact UI queries and the declared Doss query, under
    // EXPLAIN ANALYZE across the three owners: every stage's task reports, and
    // none of its numbers is missing, invalid or misattributed.
    const DOSS: &str = "SELECT\n        ss.id AS id\n      FROM sys_invocation_status ss\n      WHERE ss.target_service_name IN ('Doss')\n      LIMIT 250 ";
    let env = environment(4, 8).with_distributed_execution(fixture.network.clone());
    env.register_tables(crate::UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let session = DataFusionQueryEngine::from_env(env, None)
        .create_session(SessionOptions::default())
        .unwrap();
    // Doss without ORDER BY: any 250 of its 320 invocations, each once.
    let all: BTreeSet<_> = crate::mocks::rows(
        session.as_ref(),
        "SELECT id FROM sys_invocation_status WHERE target_service_name = 'Doss'",
    )
    .await
    .concat()
    .into_iter()
    .collect();
    assert_eq!(all.len(), 320);
    // The UI's finished page on `sys_vqueues`, read from the finished stage
    // records of every partition: the newest 250 creation times of the finished
    // entries, ties included, each entry once and a finished one.
    let finished = include_str!("../ui_queries/vqueues_finished_newest.sql");
    let found = crate::mocks::rows(
        session.as_ref(),
        &finished.replace("v.entry_id AS id", "v.entry_id AS id, v.created_at"),
    )
    .await;
    let newest = crate::mocks::rows(
        session.as_ref(),
        "SELECT entry_id, created_at FROM sys_vqueue_entry_status WHERE stage = 'finished' \
         ORDER BY created_at DESC",
    )
    .await;
    let times = |rows: &[Vec<String>]| rows.iter().map(|row| row[1].clone()).collect::<Vec<_>>();
    assert_eq!(found.len(), 250);
    assert_eq!(times(&found), times(&newest[..250]));
    assert_eq!(found.iter().collect::<BTreeSet<_>>().len(), 250);
    assert!(found.iter().all(|row| newest.contains(row)));
    let page = crate::mocks::rows(session.as_ref(), DOSS).await.concat();
    assert_eq!(page.len(), 250);
    assert_eq!(page.iter().collect::<BTreeSet<_>>().len(), 250);
    assert!(page.iter().all(|id| all.contains(id)));
    for (name, sql) in [
        (
            "service_oldest",
            include_str!("../ui_queries/service_oldest.sql"),
        ),
        (
            "service_scheduled_newest",
            include_str!("../ui_queries/service_scheduled_newest.sql"),
        ),
        (
            "vqueues_finished_newest",
            include_str!("../ui_queries/vqueues_finished_newest.sql"),
        ),
        ("workflow_page", exact),
        ("doss", DOSS),
    ] {
        let plan = crate::mocks::rows(session.as_ref(), &format!("EXPLAIN ANALYZE {sql}"))
            .await
            .concat()
            .join("\n");
        eprintln!("corpus_analyze name={name}\n{plan}");
        let stages = plan.matches("NetworkCoalesceExec").count();
        assert!(stages > 0, "{name}: {plan}");
        assert_eq!(
            plan.matches("tasks_completed=1").count() + plan.matches("tasks_incomplete=1").count(),
            stages,
            "{name}: {plan}"
        );
        for marker in [
            "metrics_missing",
            "metrics_invalid",
            "metrics_unsupported",
            "metrics_shape_mismatch",
            "metrics_wrong_owner",
            "metrics_ambiguous",
            "tasks_failed",
        ] {
            assert!(!plan.contains(marker), "{name}: {marker}: {plan}");
        }
    }

    // The newest finished entries, through `entry_by_status_transitioned/1`'s
    // Finished lanes on every owner, except the partition whose index was
    // reset, which scans and sorts: the same rows as a scan, each owner's read
    // shown. Then the newest active entries, through
    // `entry_by_status_created/1`'s lanes, the same way.
    // Neither ordered column is ever NULL, so each query reads the same with
    // NULLs placed last, first, or by default (first, for DESC): in the same
    // plan, but for the spelling.
    let finished = "SELECT entry_id, transitioned_at FROM sys_vqueue_entry_status \
        WHERE stage = 'finished' ORDER BY transitioned_at DESC NULLS LAST LIMIT 20";
    let active = "SELECT entry_id, created_at FROM sys_vqueue_entry_status \
        WHERE stage IN ('inbox', 'running') ORDER BY created_at DESC NULLS LAST LIMIT 20";
    let times = |rows: Vec<Vec<String>>| {
        rows.into_iter()
            .map(|row| row[1].clone())
            .collect::<Vec<_>>()
    };
    let unspelled = |plan: String| plan.replace(" NULLS FIRST", "").replace(" NULLS LAST", "");
    for (sql, index) in [
        (finished, "_idx_entry_status_by_status_transitioned"),
        (active, "_idx_entry_status_by_status_created"),
    ] {
        let explained = async |sql: &str| {
            let plan = crate::mocks::rows(session.as_ref(), &format!("EXPLAIN {sql}"))
                .await
                .concat()
                .join("\n");
            unspelled(plan)
        };
        let expected = explained(sql).await;
        for spelling in ["DESC NULLS LAST", "DESC", "DESC NULLS FIRST"] {
            let sql = sql.replace("DESC NULLS LAST", spelling);
            let unrestricted = sql.replacen(
                "FROM sys_vqueue_entry_status",
                "FROM (SELECT * FROM sys_vqueue_entry_status LIMIT 1000000) AS sys_vqueue_entry_status",
                1,
            );
            assert_eq!(
                times(crate::mocks::rows(session.as_ref(), &sql).await),
                times(crate::mocks::rows(session.as_ref(), &unrestricted).await),
                "{sql}"
            );
            assert_eq!(explained(&sql).await, expected, "{sql}");
            let plan = crate::mocks::rows(session.as_ref(), &format!("EXPLAIN ANALYZE {sql}"))
                .await
                .concat()
                .join("\n");
            eprintln!("{index} {spelling}\n{plan}");
            assert!(plan.contains(index), "{plan}");
            assert_eq!(
                plan.matches("tasks_completed=1").count()
                    + plan.matches("tasks_incomplete=1").count(),
                3,
                "{plan}"
            );
            assert!(plan.contains("entry_index_scans_not_complete=1"), "{plan}");
            assert!(
                !plan.contains("entry_index_reads=0,"),
                "every owner reads the index somewhere: {plan}"
            );
        }
    }

    // A count of finished entries since a time has two candidates: the
    // transition index's keys, then the stage tables. The partition whose index
    // was reset reads the second instead of scanning, over the transport; every
    // other partition reads the first. The counts equal a scan's.
    let count = format!(
        "SELECT status, COUNT(1) AS count FROM sys_vqueue_entry_status \
         WHERE stage = 'finished' AND transitioned_at >= to_timestamp_millis({}) \
         GROUP BY status ORDER BY status",
        BASE - 2_000
    );
    let counted = crate::mocks::rows(session.as_ref(), &count).await;
    let scanned = count.replacen(
        "FROM sys_vqueue_entry_status",
        "FROM (SELECT * FROM sys_vqueue_entry_status LIMIT 1000000) AS sys_vqueue_entry_status",
        1,
    );
    assert!(!counted.is_empty());
    assert_eq!(
        counted,
        crate::mocks::rows(session.as_ref(), &scanned).await
    );
    let plan = crate::mocks::rows(session.as_ref(), &format!("EXPLAIN ANALYZE {count}"))
        .await
        .concat()
        .join("\n");
    eprintln!("index candidates\n{plan}");
    assert!(
        plan.contains(
            "index=_idx_entry_status_by_status_transitioned, alternatives=[_idx_entry_by_queue]"
        ),
        "{plan}"
    );
    // Only the reset partition's owner skips a candidate, and reads that
    // partition's stage records; no partition scans.
    let skipped: Vec<_> = plan
        .lines()
        .filter(|line| line.contains("index_candidates_skipped="))
        .collect();
    assert_eq!(skipped.len(), 1, "{plan}");
    let reset_owner = format!("owner={}", owner(PartitionId::from(INCOMPLETE as u16)));
    assert!(
        skipped[0].contains(&reset_owner)
            && skipped[0].contains("index_candidates_skipped=1")
            && !skipped[0].contains("stage_keys_visited=0"),
        "{plan}"
    );
    assert!(
        plan.contains("entry_index_scans=0") && !plan.contains("entry_index_scans=1"),
        "{plan}"
    );
}
