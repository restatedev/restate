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

use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryValue, Stage, Status,
    WriteVQueueTable, stats::EntryStatistics,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::config::Configuration;
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
            // The option is sampled when a session is created.
            let session = |index_access: bool| {
                let mut config = Configuration::pinned().clone();
                config
                    .common
                    .experimental
                    .set_query_index_access(index_access);
                Configuration::set(config);
                engine.create_session(SessionOptions::default()).unwrap()
            };
            let (scan, index) = (session(false), session(true));

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
                let rows = crate::mocks::rows(index.as_ref(), &sql).await;
                assert_eq!(rows.len(), count, "{sql}");
                let reads = std::mem::take(&mut *fixture.reads.lock());
                assert_eq!(
                    rows,
                    crate::mocks::rows(scan.as_ref(), &sql).await,
                    "index_and_scan_agree: parallelism={parallelism} distributed={distributed}: {sql}"
                );
                assert!(!reads.accesses.is_empty(), "{sql}");
                assert_eq!(
                    reads
                        .accesses
                        .iter()
                        .all(|(_, access)| matches!(access, PrimaryRead::Index(index)
                            if index.as_str() == "_idx_entry_by_queue")),
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
                crate::mocks::rows(index.as_ref(), &sql).await,
                [[qualifying[1].clone()], [qualifying[2].clone()]],
                "{sql}"
            );
            let sql = format!("SELECT entry_id {residual} LIMIT 1 OFFSET 2");
            let rows = crate::mocks::rows(index.as_ref(), &sql).await;
            assert!(
                rows.len() == 1 && qualifying.contains(&rows[0][0]),
                "unordered_limits_count_qualifying_rows: {sql}: {rows:?}"
            );
            // The leaf reports the index work: every reference of the queue was
            // fetched and matched; navigation evaluated at least those keys.
            // Coordinator plans do not show remote tasks' metrics.
            if !distributed {
                let sql = format!(
                    "EXPLAIN ANALYZE SELECT entry_id FROM sys_vqueue_entry_status WHERE vqueue_id = '{}'",
                    busy
                );
                let plan = crate::mocks::rows(index.as_ref(), &sql)
                    .await
                    .concat()
                    .join("\n");
                let metric = |name: &str| -> u64 {
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
                        metric("index_references"),
                        metric("primary_keys_fetched"),
                        metric("primary_missing"),
                        metric("primary_mismatched"),
                    ],
                    [600, 600, 0, 0],
                    "index_work_metrics: distributed={distributed}: {plan}"
                );
                assert!(metric("index_keys_visited") >= 600, "{plan}");
            }

            // An unrestricted ordered limit keeps the scan and its sort.
            let sql = "SELECT entry_id FROM sys_vqueue_entry_status ORDER BY entry_id LIMIT 5";
            assert_eq!(
                crate::mocks::rows(index.as_ref(), sql).await,
                crate::mocks::rows(scan.as_ref(), sql).await,
                "{sql}"
            );
        }
    }
}
