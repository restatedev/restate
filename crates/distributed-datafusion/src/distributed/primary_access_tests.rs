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

use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, ScanInvocationStatusTable,
    ScanInvocationStatusTableRange, StatusTimestamps, WriteInvocationStatusTable,
};
use restate_types::identifiers::{InvocationId, InvocationUuid};
use restate_types::time::MillisSinceEpoch;

use crate::access::PrimaryKeys;

use super::*;

fn list(ids: &[InvocationId]) -> String {
    ids.iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

struct AccessCase {
    sql: String,
    expected: Vec<InvocationId>,
    // None means a range scan; Some(empty) means proven-empty access.
    requested: Option<Vec<InvocationId>>,
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn primary_access_rewrite_preserves_keys_residuals_and_owner_coverage() {
    let mut fixture = setup().await;
    let mut ids = Vec::new();
    let primary = Arc::new(Mutex::new(Vec::new()));
    for store in &mut fixture.stores {
        let key = store.partition_key_range().start();
        let mut tx = store.transaction();
        // More than one RocksDB multi-get batch, with holes between selected IDs.
        for row in 0..40 {
            let id = InvocationId::from_parts(key, InvocationUuid::from_u128(row + 1));
            let mut metadata = InFlightInvocationMetadata::mock();
            metadata.timestamps = StatusTimestamps::new(
                MillisSinceEpoch::new(1_000),
                MillisSinceEpoch::new(1_000),
                None,
                (row % 2 == 0).then_some(MillisSinceEpoch::new(2_000)),
                None,
                None,
            );
            tx.put_invocation_status(&id, &InvocationStatus::Invoked(metadata))
                .unwrap();
            ids.push(id);
        }
        tx.commit().await.unwrap();
        drop(tx);
        let primary = Arc::clone(&primary);
        store
            .for_each_invocation_status_lazy(
                ScanInvocationStatusTableRange::PartitionKey(KeyRange::FULL),
                move |(id, _)| {
                    primary.lock().push(id);
                    ControlFlow::<Result<(), anyhow::Error>>::Continue(())
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
    let missing = InvocationId::from_parts(ids[0].partition_key(), InvocationUuid::from_u128(100));
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
                "SELECT id FROM sys_invocation_status WHERE id = '{}'",
                ids[0]
            ),
            expected: vec![ids[0]],
            requested: Some(vec![ids[0]]),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id IN ({})",
                list(&with_missing)
            ),
            expected: selected.clone(),
            requested: Some(selected.iter().copied().chain([missing]).collect()),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id IN ({}) AND id IN ({})",
                list(&with_missing),
                list(&holes)
            ),
            expected: holes.clone(),
            requested: Some(holes.clone()),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id IN ({}) AND scheduled_at IS NOT NULL",
                list(&holes)
            ),
            expected: ids.iter().step_by(40).copied().collect(),
            requested: Some(holes.clone()),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE (id = '{}' OR id = '{}') AND id IN ('{}', '{}')",
                ids[0], ids[3], ids[3], ids[5]
            ),
            expected: vec![ids[3]],
            requested: Some(vec![ids[3]]),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id IN ('{}', '{}') AND id IN ('{}', '{}')",
                ids[0], ids[1], ids[2], ids[3]
            ),
            expected: vec![],
            requested: Some(vec![]),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id IN ({}) AND partition_key = {}",
                list(&holes),
                ids[40].partition_key()
            ),
            expected: vec![ids[40], ids[43]],
            requested: Some(vec![ids[40], ids[43]]),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id = '{}' OR scheduled_at IS NOT NULL",
                ids[1]
            ),
            expected: ids.iter().step_by(2).copied().chain([ids[1]]).collect(),
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id NOT IN ({})",
                list(&ids[..4])
            ),
            expected: ids[4..].to_vec(),
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id IN ('{}', NULL, '{}')",
                ids[0], ids[3]
            ),
            expected: vec![ids[0], ids[3]],
            requested: Some(vec![ids[0], ids[3]]),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id = '{}' OR id = 'invalid-id'",
                ids[0]
            ),
            expected: vec![ids[0]],
            requested: None,
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id IN ({}) ORDER BY id LIMIT 5 OFFSET 2",
                list(&holes)
            ),
            expected: ordered_holes[2..7].to_vec(),
            requested: Some(holes.clone()),
        },
        AccessCase {
            sql: format!(
                "SELECT id FROM sys_invocation_status WHERE id IN ({}) UNION ALL SELECT id FROM sys_invocation_status WHERE id IN ({})",
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
                let scanners = RemoteScannerManager::local_only(fixture.metadata.clone());
                scanners.register_partition_scanner::<SysInvocationStatusTable>(Arc::new(
                    CheckedScanner {
                        node: fixture.metadata.my_node_id(),
                        check_owner: false,
                        inner: Arc::new(SysInvocationStatusTable::create_local_scanner(
                            Arc::clone(&fixture.store_manager),
                        )),
                        reads: Arc::clone(&fixture.reads),
                    },
                ));
                (env, scanners)
            };
            env.register_table::<SysInvocationStatusTable>(
                SysInvocationStatusTable::create_provider(fixture.partitions.clone(), &scanners),
            )
            .unwrap();
            let engine = DataFusionQueryEngine::<AdminUser>::from_inventory(
                env,
                None,
                vec![SessionTable::for_table::<SysInvocationStatusTable>(
                    "sys_invocation_status",
                )],
            );
            let session = engine.create_session(SessionOptions::default()).unwrap();
            for case in &cases {
                let sql = if case.sql.contains("ORDER BY") {
                    case.sql.clone()
                } else {
                    format!("{} ORDER BY id", case.sql)
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
                    let mut actual_keys: Vec<InvocationId> = Vec::new();
                    for (_, access) in &reads.accesses {
                        let PrimaryRead::MultiGet(keys) = access else {
                            panic!("expected fixed multi-get: {access:?}\n{plan}")
                        };
                        let PrimaryKeys::Invocation(keys) = keys.as_ref() else {
                            panic!("wrong key kind")
                        };
                        actual_keys.extend_from_slice(keys);
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
                                reads.ranges.iter().map(|(node, _, _)| *node).collect();
                            assert_eq!(
                                tasks.iter().map(|task| task.owner).collect::<BTreeSet<_>>(),
                                expected_owners
                            );
                            let sources = if case.sql.contains("UNION ALL") { 2 } else { 1 };
                            assert_eq!(
                                tasks.len(),
                                expected_owners.len() * sources,
                                "one task per source-owner, not per ID"
                            );
                        }
                    }
                } else {
                    assert!(
                        reads
                            .accesses
                            .iter()
                            .all(|(_, access)| matches!(access, PrimaryRead::Range))
                    );
                    assert!(plan.contains("TableScanExec"), "{plan}");
                }
            }
        }
    }
}
