// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Access-path evidence over the same three-owner transport fixture as coverage tests.

use std::ops::ControlFlow;

use datafusion::arrow::array::LargeStringArray;

use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, ScanVQueueEntryStatusTable, Stage,
    Status, WriteVQueueTable, filters::ScanEntryIdFilter, stats::EntryStatistics,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::identifiers::BaseEntryId;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, VQueueId};

use crate::tables::vqueue_entry_status::read::decode_keys;

use super::*;

fn list(ids: &[BaseEntryId]) -> String {
    ids.iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

struct AccessCase {
    sql: String,
    expected: Vec<BaseEntryId>,
    // None means a partition scan; Some(empty) means proven-empty access.
    requested: Option<Vec<BaseEntryId>>,
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_access_rewrite_preserves_keys_residuals_and_owner_coverage() {
    let mut fixture = setup().await;
    let mut ids = Vec::new();
    let primary = Arc::new(Mutex::new(Vec::new()));
    for store in &mut fixture.stores {
        let key = store.partition_key_range().start();
        let qid = VQueueId::custom(key, "primary-access");
        let now = MillisSinceEpoch::new(1_744_010_000_000);
        let created_at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
        let mut tx = store.transaction();
        // More than one RocksDB multi-get batch, with holes between selected IDs.
        for row in 0..40 {
            let id = BaseEntryId::new(
                key,
                EntryId::new(
                    if row == 39 {
                        EntryKind::StateMutation
                    } else {
                        EntryKind::Invocation
                    },
                    (row + 1_u128).to_be_bytes(),
                ),
            );
            let entry_key = EntryKey::new(false, now, 1, *id.as_entry_id());
            let mut stats = EntryStatistics::new(
                // Distinct HLC counters still expose the same SQL millisecond.
                UniqueTimestamp::try_from(created_at.as_u64() + row as u64).unwrap(),
                entry_key.run_at(),
            );
            stats.first_attempt_at = (row % 2 == 0).then_some(
                UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_001_000))
                    .unwrap(),
            );
            stats.latest_attempt_at = stats.first_attempt_at;
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &EntryTargetRef::Service {
                        scope: None,
                        service: "test",
                        handler: "handler",
                    },
                },
                EntryStateRef {
                    stage: Stage::Running,
                    status: Status::Started,
                    entry_key: &entry_key,
                    metadata: &EntryMetadata::default(),
                    stats: &stats,
                },
            );
            ids.push(id);
        }
        tx.commit().await.unwrap();
        drop(tx);
        let primary = Arc::clone(&primary);
        store
            .for_each_vqueue_entry_status(
                ScanEntryIdFilter::PartitionKey(KeyRange::FULL),
                move |id, _| {
                    primary.lock().push(*id);
                    ControlFlow::Continue(())
                },
            )
            .unwrap()
            .await
            .unwrap();
    }
    assert_eq!(
        *primary.lock(),
        ids,
        "unfiltered primary scan independently anchors the fixture"
    );
    let selected: Vec<_> = ids
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 40 != 1)
        .map(|(_, id)| *id)
        .collect();
    let missing = BaseEntryId::new(
        ids[0].partition_key(),
        EntryId::new(EntryKind::Invocation, 100_u128.to_be_bytes()),
    );
    let mut with_missing = selected.clone();
    with_missing.extend([missing, selected[0], selected[0]]);
    let holes: Vec<_> = ids
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 40 == 0 || i % 40 == 3)
        .map(|(_, id)| *id)
        .collect();
    let mut ordered_holes = holes.clone();
    ordered_holes.sort_by_key(ToString::to_string);
    let cases = vec![
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE canonical_id = '{}_1'",
                ids[0]
            ),
            expected: vec![ids[0]],
            requested: Some(vec![ids[0]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE canonical_id = '{}_1'",
                ids[39]
            ),
            expected: vec![ids[39]],
            requested: Some(vec![ids[39]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE canonical_id = '{}_2'",
                ids[0]
            ),
            expected: vec![],
            requested: Some(vec![ids[0]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE canonical_id IN ('{}', 'not-an-id')",
                ids[0]
            ),
            expected: vec![],
            requested: Some(vec![]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE canonical_id IN ('{}_1', '{}_2', '{}_1', 'not-an-id', NULL)",
                ids[0], ids[0], ids[40]
            ),
            expected: vec![ids[0], ids[40]],
            requested: Some(vec![ids[0], ids[40]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE canonical_id IN ('{}_2', '{}_1') ORDER BY entry_id LIMIT 1",
                ids[0], ids[3]
            ),
            expected: vec![ids[3]],
            requested: Some(vec![ids[0], ids[3]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id = '{}' OR canonical_id = '{}_1'",
                ids[0], ids[40]
            ),
            expected: vec![ids[0], ids[40]],
            requested: Some(vec![ids[0], ids[40]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id = '{}' AND canonical_id = '{}_1'",
                ids[0], ids[3]
            ),
            expected: vec![],
            requested: Some(vec![]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) AND canonical_id != '{}_2'",
                list(&[ids[0], ids[3]]), ids[0]
            ),
            expected: vec![ids[0], ids[3]],
            requested: Some(vec![ids[0], ids[3]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) AND canonical_id NOT IN ('{}_1')",
                list(&[ids[0], ids[3]]), ids[0]
            ),
            expected: vec![ids[3]],
            requested: Some(vec![ids[0], ids[3]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE canonical_id NOT IN ('{}_1', 'not-an-id')",
                ids[0]
            ),
            expected: ids[1..].to_vec(),
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id = '{}'",
                ids[0]
            ),
            expected: vec![ids[0]],
            requested: Some(vec![ids[0]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({})",
                list(&with_missing)
            ),
            expected: selected.clone(),
            requested: Some(selected.iter().copied().chain([missing]).collect()),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) AND entry_id IN ({})",
                list(&with_missing),
                list(&holes)
            ),
            expected: holes.clone(),
            requested: Some(holes.clone()),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) AND first_attempt_at IS NOT NULL",
                list(&holes)
            ),
            expected: ids.iter().step_by(40).copied().collect(),
            requested: Some(holes.clone()),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({})
                 AND created_at = to_timestamp_millis(1744010000000)
                 AND first_runnable_at = to_timestamp_millis(1744010000000)
                 AND first_attempt_at = to_timestamp_millis(1744010001000)
                 AND latest_attempt_at >= to_timestamp_millis(1744010001000)",
                list(&holes)
            ),
            expected: ids.iter().step_by(40).copied().collect(),
            requested: Some(holes.clone()),
        },
        AccessCase {
            sql: "SELECT entry_id FROM sys_vqueue_entry_status WHERE first_runnable_at = TIMESTAMP '1969-12-31 23:59:59.999'".into(),
            expected: vec![],
            requested: Some(vec![]),
        },
        AccessCase {
            sql: "SELECT entry_id FROM sys_vqueue_entry_status WHERE vqueue_id IN ('not-a-vqueue', 'vq_1💥')".into(),
            expected: vec![],
            requested: Some(vec![]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) AND vqueue_id IN ('{}', 'not-a-vqueue')",
                list(&holes), VQueueId::custom(ids[0].partition_key(), "primary-access")
            ),
            expected: vec![ids[0], ids[3]],
            requested: Some(vec![ids[0], ids[3]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) AND vqueue_id > 'vq_' AND vqueue_id != 'not-a-vqueue'",
                list(&holes)
            ),
            expected: holes.clone(),
            requested: Some(holes.clone()),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE (entry_id = '{}' OR entry_id = '{}') AND entry_id IN ('{}', '{}')",
                ids[0], ids[3], ids[3], ids[5]
            ),
            expected: vec![ids[3]],
            requested: Some(vec![ids[3]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ('{}', '{}') AND entry_id IN ('{}', '{}')",
                ids[0], ids[1], ids[2], ids[3]
            ),
            expected: vec![],
            requested: Some(vec![]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) AND partition_key = {}",
                list(&holes),
                ids[40].partition_key()
            ),
            expected: vec![ids[40], ids[43]],
            requested: Some(vec![ids[40], ids[43]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id = '{}' OR first_attempt_at IS NOT NULL",
                ids[1]
            ),
            expected: ids.iter().step_by(2).copied().chain([ids[1]]).collect(),
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id NOT IN ({})",
                list(&ids[..4])
            ),
            expected: ids[4..].to_vec(),
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) OR partition_key BETWEEN {} AND {}",
                list(&[ids[0], ids[3], ids[40]]),
                ids[80].partition_key(),
                ids[80].partition_key() + 2
            ),
            expected: [ids[0], ids[3], ids[40]]
                .into_iter()
                .chain(ids[80..120].iter().copied())
                .collect(),
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ('{}', NULL, '{}')",
                ids[0], ids[3]
            ),
            expected: vec![ids[0], ids[3]],
            requested: Some(vec![ids[0], ids[3]]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id = '{}' OR entry_id = 'invalid-id'",
                ids[0]
            ),
            expected: vec![ids[0]],
            requested: Some(vec![ids[0]]),
        },
        AccessCase {
            sql: "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id = 'invalid-id'"
                .into(),
            expected: vec![],
            requested: Some(vec![]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id = '{}_1'",
                ids[0]
            ),
            expected: vec![],
            requested: Some(vec![]),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id NOT IN ('{}', 'invalid-id', '{}_1')",
                ids[0], ids[1]
            ),
            expected: ids[1..].to_vec(),
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id < '{}'",
                ids[40]
            ),
            expected: ids
                .iter()
                .copied()
                .filter(|id| id.to_string() < ids[40].to_string())
                .collect(),
            requested: None,
        },
        AccessCase {
            sql: "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id LIKE 'mut_%'".into(),
            expected: ids.iter().skip(39).step_by(40).copied().collect(),
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) ORDER BY entry_id LIMIT 5 OFFSET 2",
                list(&holes)
            ),
            expected: ordered_holes[2..7].to_vec(),
            requested: Some(holes.clone()),
        },
        AccessCase {
            sql: format!(
                "SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({}) UNION ALL SELECT entry_id FROM sys_vqueue_entry_status WHERE entry_id IN ({})",
                list(&holes),
                list(&holes)
            ),
            expected: holes.iter().copied().chain(holes.iter().copied()).collect(),
            requested: Some(holes.iter().copied().chain(holes.iter().copied()).collect()),
        },
    ];
    for parallelism in [1, 4] {
        for distributed in [false, true] {
            let env = crate::DataFusionEnv::new(
                64 * 1024 * 1024,
                None,
                Some(parallelism),
                &HashMap::from([
                    ("datafusion.execution.batch_size".into(), "2".into()),
                    // Sort reservations are per lane, even for this tiny corpus.
                    (
                        "datafusion.execution.sort_spill_reservation_bytes".into(),
                        "65536".into(),
                    ),
                ]),
            )
            .unwrap()
            .with_mock_clock(restate_clock::MockClock::new())
            .unwrap();
            let (env, scanners) = if distributed {
                (
                    env.with_distributed_execution(fixture.network.clone()),
                    fixture.scanners.clone(),
                )
            } else {
                // Bind the same quiescent partitions locally for an independent placement path.
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
                (env, scanners)
            };
            env.register_table::<SysVqueueEntryStatusTable>(
                "public",
                "sys_vqueue_entry_status",
                SysVqueueEntryStatusTable::create_provider(fixture.partitions.clone(), &scanners),
            )
            .unwrap();
            let engine = DataFusionQueryEngine::from_env(env, None);
            let session = engine.create_session(SessionOptions::default()).unwrap();
            let projected = [ids[0], ids[39], ids[40]];
            let batches = session
                .execute(
                    &format!(
                        "SELECT entry_id, canonical_id, sequence_number FROM sys_vqueue_entry_status WHERE entry_id IN ({}) ORDER BY entry_id",
                        list(&projected)
                    ),
                    QueryOptions {},
                )
                .await
                .unwrap()
                .stream
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            let actual: Vec<_> = batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows()).map(|row| {
                        (0..3)
                            .map(|column| array_value_to_string(batch.column(column), row).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            let mut expected: Vec<_> = projected
                .iter()
                .map(|id| vec![id.to_string(), format!("{id}_1"), "1".into()])
                .collect();
            expected.sort();
            assert_eq!(
                actual, expected,
                "canonical_ids_preserve_resource_kind_and_sequence_across_owner_transport"
            );
            for case in &cases {
                let sql = if case.sql.contains("ORDER BY") {
                    case.sql.clone()
                } else {
                    format!("{} ORDER BY entry_id", case.sql)
                };
                fixture.tasks.lock().clear();
                fixture.placements.lock().clear();
                *fixture.reads.lock() = Reads::default();
                let explain = session
                    .execute(&format!("EXPLAIN VERBOSE {sql}"), QueryOptions {})
                    .await
                    .unwrap()
                    .stream
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let plan = explain
                    .iter()
                    .flat_map(|batch| {
                        (0..batch.num_rows())
                            .map(|row| array_value_to_string(batch.column(1), row).unwrap())
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                assert!(
                    fixture.reads.lock().accesses.is_empty(),
                    "EXPLAIN must not read storage"
                );
                assert!(
                    fixture.tasks.lock().is_empty(),
                    "EXPLAIN must not install tasks"
                );
                let batches = session
                    .execute(&sql, QueryOptions {})
                    .await
                    .unwrap()
                    .stream
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let actual: Vec<_> = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<LargeStringArray>()
                            .unwrap()
                            .iter()
                            .map(|id| id.unwrap().to_owned())
                    })
                    .collect();
                let mut expected: Vec<_> = case.expected.iter().map(ToString::to_string).collect();
                expected.sort();
                assert_eq!(actual, expected, "parallelism={parallelism}: {sql}\n{plan}");
                let reads = fixture.reads.lock();
                if let Some(requested) = &case.requested {
                    let mut actual_keys = Vec::new();
                    for (_, access) in &reads.accesses {
                        let PrimaryRead::MultiGet(keys) = access else {
                            panic!("expected fixed multi-get: {access:?}\n{plan}")
                        };
                        actual_keys.extend(decode_keys(keys).unwrap());
                    }
                    actual_keys.sort();
                    let mut requested = requested.clone();
                    requested.sort();
                    assert_eq!(
                        actual_keys, requested,
                        "exact sets must survive codecs, holes and residuals: {sql}"
                    );
                    if requested.is_empty() {
                        assert!(
                            fixture.tasks.lock().is_empty(),
                            "empty access dispatches no work"
                        );
                        if distributed {
                            assert!(
                                fixture.placements.lock().is_empty(),
                                "empty access needs no placement"
                            );
                        }
                    } else {
                        assert!(plan.contains("MultiGetExec"), "{plan}");
                        if distributed {
                            let tasks = fixture.tasks.lock();
                            assert!(!tasks.is_empty());
                            assert!(tasks.iter().all(|task| task.plan.contains("MultiGetExec")));
                            let expected_owners: BTreeSet<_> =
                                reads.partitions.iter().map(|(node, _)| *node).collect();
                            assert_eq!(
                                tasks.iter().map(|task| task.owner).collect::<BTreeSet<_>>(),
                                expected_owners
                            );
                            assert_eq!(
                                tasks.len(),
                                expected_owners.len(),
                                "compatible union branches share one task per owner"
                            );
                        }
                    }
                } else {
                    assert_eq!(
                        reads
                            .partitions
                            .iter()
                            .map(|(_, id)| *id)
                            .collect::<BTreeSet<_>>()
                            .len(),
                        reads.partitions.len(),
                        "scan_fallback_targets_each_partition_once: {}",
                        case.sql,
                    );
                    assert!(
                        reads
                            .accesses
                            .iter()
                            .all(|(_, access)| matches!(access, PrimaryRead::Scan))
                    );
                    assert!(plan.contains("TableScanExec"), "{plan}");
                }
            }
            // Without ORDER BY, any matching ID is valid. The limit is global,
            // and stale incarnations must not consume its rows or OFFSET.
            for (predicate, matching) in [
                (
                    format!("entry_id IN ({})", list(&[ids[0], ids[40]])),
                    [ids[0], ids[40]],
                ),
                (
                    format!(
                        "canonical_id IN ('{}_2', '{}_1', '{}_1')",
                        ids[0], ids[3], ids[6]
                    ),
                    [ids[3], ids[6]],
                ),
            ] {
                for skip in [0, 1] {
                    let sql = format!(
                        "SELECT entry_id FROM sys_vqueue_entry_status WHERE {predicate} LIMIT 1 OFFSET {skip}"
                    );
                    let batches = session
                        .execute(&sql, QueryOptions {})
                        .await
                        .unwrap()
                        .stream
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        1,
                        "global_limit_and_offset_count_only_matching_incarnations: parallelism={parallelism} distributed={distributed}: {sql}"
                    );
                    let batch = batches.iter().find(|batch| batch.num_rows() != 0).unwrap();
                    let id = array_value_to_string(batch.column(0), 0).unwrap();
                    assert!(
                        matching.iter().any(|expected| expected.to_string() == id),
                        "limit_returns_only_matching_incarnations: {sql}"
                    );
                }
            }
        }
    }
}
