// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::RangeBounds;

use async_trait::async_trait;
use datafusion::arrow::util::display::array_value_to_string;
use futures::TryStreamExt;

use restate_clock::MockClock;
use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryId, EntryKey, EntryKind, EntryMetadata, EntryStateRef, ReadVQueueTable,
    Stage, Status, StoredEntryRef, WriteVQueueTable,
};
use restate_storage_query_api::{QueryOptions, QuerySession, SessionOptions};
use restate_types::clock::UniqueTimestamp;
use restate_types::config::{Configuration, QueryEngineOptions};
use restate_types::errors::GenericError;
use restate_types::identifiers::DeploymentId;
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryTargetRef, HandlerRef, VQueueId};

use crate::context::{DataFusionQueryEngine, SelectPartitions};
use crate::local_scanners::register_partition_scanners;
use crate::source_registry::SourceRegistry;
use crate::{DataFusionEnv, UserTables};

#[derive(Debug, Clone)]
struct Partitions(Vec<(PartitionId, Partition)>);

#[async_trait]
impl SelectPartitions for Partitions {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.clone())
    }
}

/// Checks that statistics queries fail, with an error containing `reason`, when
/// a partition they read has no Complete gauge. The summed service and
/// deployment tables must fail before they return any row, whatever they select
/// and also with a `LIMIT`: a client that already got rows couldn't tell them
/// from a complete total. `sys_virtual_object_stats` streams each partition's
/// rows, so it may return rows first; it must still fail.
pub(crate) async fn assert_queries_fail(session: &dyn QuerySession, reason: &str) {
    for table in ["sys_service_stats", "sys_deployment_stats"] {
        for sql in [
            format!("SELECT * FROM {table}"),
            format!("SELECT * FROM {table} LIMIT 1"),
            format!("SELECT * FROM {table} ORDER BY service_name LIMIT 1"),
            // Without `num_entries`, the view must still keep the sum and wait.
            format!("SELECT service_name FROM {table} LIMIT 1"),
        ] {
            let mut stream = session.execute(&sql, QueryOptions {}).await.unwrap().stream;
            let error = loop {
                match stream.try_next().await {
                    Ok(Some(batch)) => {
                        assert_eq!(batch.num_rows(), 0, "{sql}: rows before the error")
                    }
                    Ok(None) => panic!("{sql}: no error"),
                    Err(error) => break error,
                }
            };
            assert!(error.to_string().contains(reason), "{sql}: {error}");
        }
        // The sum stays in the plan, so the view waits for every partition
        // whatever the query selects; a distinct aggregate under a `LIMIT`
        // could stop early.
        let explain = format!("EXPLAIN SELECT service_name FROM {table} LIMIT 1");
        let batches: Vec<_> = session
            .execute(&explain, QueryOptions {})
            .await
            .unwrap()
            .stream
            .try_collect()
            .await
            .unwrap();
        let plan = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        assert!(plan.contains("sum("), "{explain}: {plan}");
    }
    let sql = "SELECT * FROM sys_virtual_object_stats";
    let mut stream = session.execute(sql, QueryOptions {}).await.unwrap().stream;
    let error = loop {
        match stream.try_next().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("{sql}: no error"),
            Err(error) => break error,
        }
    };
    assert!(error.to_string().contains(reason), "{sql}: {error}");
}

async fn assert_rows(session: &dyn QuerySession, sql: &str, expected: &[&[&str]]) {
    let batches: Vec<_> = session
        .execute(sql, QueryOptions {})
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap();
    let mut actual = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            actual.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).unwrap())
                    .collect::<Vec<_>>(),
            );
        }
    }
    actual.sort();
    let mut expected: Vec<Vec<_>> = expected
        .iter()
        .map(|row| row.iter().map(|value| (*value).to_owned()).collect())
        .collect();
    expected.sort();
    assert_eq!(actual, expected, "{sql}");
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_tables_aggregate_filter_and_follow_lifecycle_across_stores() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let partitions = Partitions(vec![
        (
            PartitionId::from(0),
            Partition::new(PartitionId::from(0), KeyRange::new(0, 4999)),
        ),
        (
            PartitionId::from(1),
            Partition::new(PartitionId::from(1), KeyRange::new(5000, 9999)),
        ),
    ]);
    let mut config = Configuration::default();
    // Tracked from the start, over empty partitions, the gauges are Complete at once.
    config.common.experimental.set_indexes(true);
    Configuration::set(config.clone());
    let deployment = DeploymentId::new().to_string();
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let created_at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
    let mut stores = Vec::new();
    for (_, partition) in &partitions.0 {
        let mut store = manager.open(partition, None).await.unwrap();
        store.mark_fully_migrated_to_vqueues().await;
        store
            .verify_and_run_migrations(Default::default(), &config)
            .await
            .unwrap();
        let mut tx = store.transaction();
        for (pk, kind) in [
            (3337, EntryKind::Invocation),
            (4337, EntryKind::Invocation),
            (5337, EntryKind::Invocation),
            (3337, EntryKind::StateMutation),
        ] {
            if !partition.key_range.contains(&pk) {
                continue;
            }
            let invocation = kind == EntryKind::Invocation;
            let qid = VQueueId::custom(pk, "stats");
            let key = EntryKey::new(false, now, 1, EntryId::new(kind, [1; 16]));
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &EntryTargetRef::VirtualObject {
                        scope: (pk != 5337).then_some("tenant"),
                        service: "counter",
                        key: if pk == 5337 { "b" } else { "a" },
                        handler: if invocation {
                            HandlerRef::UserHandler("increment")
                        } else {
                            HandlerRef::StateMutation
                        },
                    },
                },
                EntryStateRef {
                    stage: if invocation {
                        Stage::Running
                    } else {
                        Stage::Inbox
                    },
                    status: if invocation {
                        Status::Started
                    } else {
                        Status::New
                    },
                    entry_key: &key,
                    metadata: &EntryMetadata {
                        deployment: invocation.then(|| deployment.clone().into()),
                        ..Default::default()
                    },
                    stats: &EntryStatistics::new(created_at, key.run_at()),
                },
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
        stores.push(store);
    }

    let scanners =
        SourceRegistry::local_only(restate_core::MetadataBuilder::default().to_metadata());
    register_partition_scanners(manager.clone(), crate::mocks::query_io(), &scanners);
    let env = DataFusionEnv::from_options(&QueryEngineOptions::default())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    env.register_tables(UserTables::new(partitions.clone(), scanners.clone()))
        .await
        .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let session = session.as_ref();

    assert_rows(session, "SELECT kind, stage, status, num_entries FROM sys_service_stats WHERE service_name = 'counter'", &[
        &["invocation", "running", "running", "3"],
        &["state-mutation", "inbox", "new", "1"],
    ]).await;
    assert_rows(session, &format!("SELECT service_name, stage, status, num_entries FROM sys_deployment_stats WHERE deployment_id = '{deployment}'"), &[
        &["counter", "running", "running", "3"],
    ]).await;
    assert_rows(
        session,
        "SELECT num_entries FROM sys_service_stats WHERE num_entries > 1",
        &[&["3"]],
    )
    .await;
    assert_rows(
        session,
        "SELECT num_entries FROM sys_deployment_stats WHERE num_entries > 1",
        &[&["3"]],
    )
    .await;
    assert_rows(session, "SELECT num_entries FROM sys_service_stats WHERE handler IS NULL AND kind = 'state-mutation'", &[&["1"]]).await;
    assert_rows(session, "SELECT num_entries FROM sys_service_stats WHERE service_name IN ('counter', 'missing') AND handler LIKE 'inc%'", &[&["3"]]).await;
    assert_rows(
        session,
        "SELECT num_entries FROM sys_deployment_stats WHERE service_name != 'counter'",
        &[],
    )
    .await;
    assert_rows(
        session,
        "SELECT num_entries FROM sys_service_stats WHERE kind = 'invalid'",
        &[],
    )
    .await;
    assert_rows(
        session,
        "SELECT stage, status, SUM(num_entries) FROM sys_virtual_object_stats GROUP BY stage, status",
        &[&["running", "running", "3"], &["inbox", "new", "1"]],
    )
    .await;
    for (predicate, expected) in [
        ("partition_key IN (3337, 4337, 3337)", "3"),
        ("partition_key IN (3337, 6337)", "2"),
        ("partition_key = 3337 OR partition_key = 5337", "3"),
        ("partition_key >= 4000 AND partition_key < 6000", "2"),
    ] {
        assert_rows(
            session,
            &format!("SELECT COUNT(*) FROM sys_virtual_object_stats WHERE {predicate}"),
            &[&[expected]],
        )
        .await;
    }
    assert_rows(
        session,
        "SELECT key, num_entries FROM sys_virtual_object_stats WHERE scope IS NULL AND stage = 'running' AND status = 'running' LIMIT 1",
        &[&["b", "1"]],
    )
    .await;
    assert_rows(
        session,
        "SELECT num_entries FROM sys_virtual_object_stats WHERE partition_key = 4337",
        &[&["1"]],
    )
    .await;
    assert_rows(
        session,
        "SELECT num_entries FROM sys_virtual_object_stats WHERE handler IS NULL AND stage = 'inbox' AND status = 'new'",
        &[&["1"]],
    )
    .await;
    assert_rows(
        session,
        "SELECT num_entries FROM sys_virtual_object_stats WHERE key = 'missing' LIMIT 1",
        &[],
    )
    .await;

    let session_over = |partitions, options: QueryEngineOptions| {
        let scanners = scanners.clone();
        async move {
            let env = DataFusionEnv::from_options(&options)
                .unwrap()
                .with_mock_clock(MockClock::new())
                .unwrap();
            env.register_tables(UserTables::new(partitions, scanners))
                .await
                .unwrap();
            DataFusionQueryEngine::from_env(env, None)
                .create_session(SessionOptions::default())
                .unwrap()
        }
    };

    // Partition 0 has rows, but a partition opened without gauges has none
    // Complete: every statistics query fails. One lane reads the partitions in
    // order, so partition 0 returns its rows first, and only the summed tables
    // hold them back.
    let untracked = Partition::new(PartitionId::from(2), KeyRange::new(10000, 14999));
    let _untracked = manager.open(&untracked, None).await.unwrap();
    let mut one_lane = QueryEngineOptions::default();
    one_lane.datafusion_options.insert(
        "datafusion.execution.target_partitions".to_owned(),
        "1".to_owned(),
    );
    let incomplete = session_over(
        Partitions(vec![
            partitions.0[0].clone(),
            (PartitionId::from(2), untracked),
        ]),
        one_lane,
    )
    .await;
    assert_queries_fail(incomplete.as_ref(), "not complete").await;

    // Targeting a partition selects its identity. Narrowing within that partition
    // is an independent SQL selection consumed by physical preparation.
    let subset = session_over(
        Partitions(vec![partitions.0[0].clone()]),
        QueryEngineOptions::default(),
    )
    .await;
    assert_rows(
        subset.as_ref(),
        "SELECT stage, SUM(num_entries) FROM sys_virtual_object_stats GROUP BY stage",
        &[&["running", "2"], &["inbox", "1"]],
    )
    .await;
    assert_rows(
        subset.as_ref(),
        "SELECT stage, SUM(num_entries) FROM sys_virtual_object_stats WHERE partition_key = 3337 GROUP BY stage",
        &[&["running", "1"], &["inbox", "1"]],
    )
    .await;

    // A status-only transition must split a cross-partition deployment aggregate.
    let mut tx = stores[0].transaction();
    let qid = VQueueId::custom(4337, "stats");
    let id = EntryId::new(EntryKind::Invocation, [1; 16]).to_base_id(4337);
    let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    tx.update_vqueue_entry_status(
        &EntryContext {
            qid: &qid,
            target: &EntryTargetRef::VirtualObject {
                scope: Some("tenant"),
                service: "counter",
                key: "a",
                handler: HandlerRef::UserHandler("increment"),
            },
        },
        StoredEntryRef::from_header(&header),
        EntryStateRef {
            status: Status::Yielded,
            ..EntryStateRef::from_header(&header)
        },
    );
    tx.commit().await.unwrap();
    drop(tx);
    // Running shows one effective status whatever is stored: the yielded bucket
    // and the started ones sum into one row.
    assert_rows(
        session,
        "SELECT stage, status, num_entries FROM sys_deployment_stats WHERE stage = 'running'",
        &[&["running", "running", "3"]],
    )
    .await;
    assert_rows(session,
        "SELECT num_entries FROM sys_virtual_object_stats WHERE status = 'running' AND partition_key = 4337",
        &[&["1"]]).await;
    assert_rows(
        session,
        "SELECT num_entries FROM sys_virtual_object_stats WHERE status = 'yielded'",
        &[],
    )
    .await;

    // Updating and deleting entries must be reflected in subsequent queries in the same session.
    for (store_index, pk) in [(0, 3337), (1, 5337)] {
        let mut tx = stores[store_index].transaction();
        let qid = VQueueId::custom(pk, "stats");
        let id = EntryId::new(EntryKind::Invocation, [1; 16]).to_base_id(pk);
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        let context = EntryContext {
            qid: &qid,
            target: &EntryTargetRef::VirtualObject {
                scope: (pk != 5337).then_some("tenant"),
                service: "counter",
                key: if pk == 5337 { "b" } else { "a" },
                handler: HandlerRef::UserHandler("increment"),
            },
        };
        if store_index == 0 {
            tx.update_vqueue_entry_status(
                &context,
                StoredEntryRef::from_header(&header),
                EntryStateRef {
                    stage: Stage::Paused,
                    ..EntryStateRef::from_header(&header)
                },
            );
        } else {
            tx.delete_vqueue_entry_status(&context, StoredEntryRef::from_header(&header));
        }
        tx.commit().await.unwrap();
    }
    assert_rows(session, "SELECT stage, num_entries FROM sys_service_stats WHERE kind = 'invocation' AND num_entries > 0", &[&["running", "1"], &["paused", "1"]]).await;
    assert_rows(
        session,
        "SELECT stage, status, num_entries FROM sys_deployment_stats WHERE num_entries > 0",
        &[&["running", "running", "1"], &["paused", "paused", "1"]],
    )
    .await;
    assert_rows(
        session,
        "SELECT stage, SUM(num_entries) FROM sys_virtual_object_stats GROUP BY stage",
        &[&["running", "1"], &["paused", "1"], &["inbox", "1"]],
    )
    .await;

    // One stable object key can hold two stored statuses in the same stage. Paused
    // shows one effective status, so both buckets sum into one SQL row with count
    // two, before and after a status-only update merges them into one bucket.
    let qid = VQueueId::custom(3337, "stats");
    let target = EntryTargetRef::VirtualObject {
        scope: Some("tenant"),
        service: "counter",
        key: "a",
        handler: HandlerRef::UserHandler("increment"),
    };
    let context = EntryContext {
        qid: &qid,
        target: &target,
    };
    let key = EntryKey::new(false, now, 2, EntryId::new(EntryKind::Invocation, [2; 16]));
    let metadata = EntryMetadata {
        deployment: Some(deployment.into()),
        ..Default::default()
    };
    let stats = EntryStatistics::new(created_at, key.run_at());
    let before = EntryStateRef {
        stage: Stage::Paused,
        status: Status::New,
        entry_key: &key,
        metadata: &metadata,
        stats: &stats,
    };
    let mut tx = stores[0].transaction();
    tx.create_vqueue_entry_status(&context, before);
    tx.commit().await.unwrap();
    drop(tx);
    for (table, predicate) in [
        (
            "sys_virtual_object_stats",
            "partition_key = 3337 AND kind = 'invocation'",
        ),
        ("sys_deployment_stats", "stage = 'paused'"),
    ] {
        assert_rows(
            session,
            &format!("SELECT status, num_entries FROM {table} WHERE {predicate}"),
            &[&["paused", "2"]],
        )
        .await;
    }
    let mut tx = stores[0].transaction();
    let header = tx
        .get_vqueue_entry_status(&before.base_entry_id(&context))
        .await
        .unwrap()
        .unwrap();
    tx.update_vqueue_entry_status(
        &context,
        StoredEntryRef::from_header(&header),
        EntryStateRef {
            status: Status::Started,
            ..before
        },
    );
    tx.commit().await.unwrap();
    drop(tx);
    for (table, predicate) in [
        (
            "sys_virtual_object_stats",
            "partition_key = 3337 AND kind = 'invocation'",
        ),
        ("sys_deployment_stats", "stage = 'paused'"),
    ] {
        assert_rows(
            session,
            &format!(
                "SELECT status, num_entries FROM {table} WHERE {predicate} AND num_entries > 1"
            ),
            &[&["paused", "2"]],
        )
        .await;
        assert_rows(
            session,
            &format!("SELECT status FROM {table} WHERE {predicate} AND status = 'new'"),
            &[],
        )
        .await;
    }
}
