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
use std::sync::Arc;

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
    Stage, Status, WriteVQueueTable,
};
use restate_storage_query_api::{AdminUser, QueryOptions, QuerySession, SessionOptions};
use restate_types::clock::UniqueTimestamp;
use restate_types::config::{Configuration, QueryEngineOptions};
use restate_types::errors::GenericError;
use restate_types::identifiers::DeploymentId;
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryTargetRef, HandlerRef, VQueueId};
use restate_worker_api::OfflinePartitionQueryAccess;

use crate::context::{DataFusionQueryEngine, SelectPartitions};
use crate::local_scanners::{register_live_scanners, register_partition_scanners};
use crate::remote_query_scanner_manager::RemoteScannerManager;
use crate::{DataFusionEnv, UserTables};

#[derive(Debug, Clone)]
struct Partitions(Vec<(PartitionId, Partition)>);

#[async_trait]
impl SelectPartitions for Partitions {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.clone())
    }
}

async fn assert_rows(session: &dyn QuerySession<AdminUser>, sql: &str, expected: &[&[&str]]) {
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
    config.common.experimental.set_track_status_gauges(true);
    let deployment = DeploymentId::new().to_string();
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let created_at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
    let mut stores = Vec::new();
    for (_, partition) in &partitions.0 {
        let mut store = manager.open(partition, None).await.unwrap();
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
        RemoteScannerManager::local_only(restate_core::MetadataBuilder::default().to_metadata());
    register_partition_scanners(manager, &scanners);
    register_live_scanners(
        Arc::new(OfflinePartitionQueryAccess::new(
            partitions.0.iter().map(|(id, p)| (*id, p.key_range)),
        )),
        &scanners,
    );
    let env = DataFusionEnv::from_options(&QueryEngineOptions::default())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let engine = DataFusionQueryEngine::with_user_tables(
        env,
        None,
        UserTables::new(partitions.clone(), scanners.clone()),
    )
    .await
    .unwrap();
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let session = session.as_ref();

    assert_rows(session, "SELECT kind, stage, status, num_entries FROM sys_service_stats WHERE service_name = 'counter'", &[
        &["invocation", "running", "started", "3"],
        &["state-mutation", "inbox", "new", "1"],
    ]).await;
    assert_rows(session, &format!("SELECT service_name, stage, status, num_entries FROM sys_deployment_stats WHERE deployment_id = '{deployment}'"), &[
        &["counter", "running", "started", "3"],
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
        &[&["running", "started", "3"], &["inbox", "new", "1"]],
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
        "SELECT key, num_entries FROM sys_virtual_object_stats WHERE scope IS NULL AND stage = 'running' AND status = 'started' LIMIT 1",
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

    // A requested range must be enforced even without a SQL partition-key predicate.
    let subset = Partitions(vec![(
        PartitionId::from(0),
        Partition::new(PartitionId::from(0), KeyRange::new(3337, 3337)),
    )]);
    let env = DataFusionEnv::from_options(&QueryEngineOptions::default())
        .unwrap()
        .with_mock_clock(MockClock::new())
        .unwrap();
    let subset =
        DataFusionQueryEngine::with_user_tables(env, None, UserTables::new(subset, scanners))
            .await
            .unwrap();
    let subset = subset.create_session(SessionOptions::default()).unwrap();
    assert_rows(
        subset.as_ref(),
        "SELECT stage, SUM(num_entries) FROM sys_virtual_object_stats GROUP BY stage",
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
        EntryStateRef::from_header(&header),
        EntryStateRef {
            status: Status::Yielded,
            ..EntryStateRef::from_header(&header)
        },
    );
    tx.commit().await.unwrap();
    drop(tx);
    assert_rows(
        session,
        "SELECT stage, status, num_entries FROM sys_deployment_stats WHERE stage = 'running'",
        &[&["running", "started", "2"], &["running", "yielded", "1"]],
    )
    .await;
    assert_rows(session,
        "SELECT num_entries FROM sys_virtual_object_stats WHERE status = 'yielded' AND partition_key = 4337",
        &[&["1"]]).await;

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
                EntryStateRef::from_header(&header),
                EntryStateRef {
                    stage: Stage::Paused,
                    ..EntryStateRef::from_header(&header)
                },
            );
        } else {
            tx.delete_vqueue_entry_status(&context, EntryStateRef::from_header(&header));
        }
        tx.commit().await.unwrap();
    }
    assert_rows(session, "SELECT stage, num_entries FROM sys_service_stats WHERE kind = 'invocation' AND num_entries > 0", &[&["running", "1"], &["paused", "1"]]).await;
    assert_rows(
        session,
        "SELECT stage, status, num_entries FROM sys_deployment_stats WHERE num_entries > 0",
        &[&["running", "yielded", "1"], &["paused", "started", "1"]],
    )
    .await;
    assert_rows(
        session,
        "SELECT stage, SUM(num_entries) FROM sys_virtual_object_stats GROUP BY stage",
        &[&["running", "1"], &["paused", "1"], &["inbox", "1"]],
    )
    .await;

    // One stable object key can expand to two statuses in the same stage. A later
    // status-only update merges the buckets back into one SQL row with count two.
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
            &[&["new", "1"], &["started", "1"]],
        )
        .await;
    }
    let mut tx = stores[0].transaction();
    tx.update_vqueue_entry_status(
        &context,
        before,
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
            &[&["started", "2"]],
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
