// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{Int64Type, Schema};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use restate_core::TestCoreEnvBuilder;
use restate_partition_store::job::MigrationJobId;
use restate_partition_store::keys::KeyKind;
use restate_partition_store::registry::MigrationOnUnknown;
use restate_partition_store::registry::records::RecordKey;
use restate_partition_store::walk::DriverState;
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_types::GenerationalNodeId;
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;
use restate_util_string::ReString;

use super::*;
use crate::mocks::{AlwaysLocalPartitionLocator, NoopSvc};
use crate::node_fan_out::NodeFanOutExecutionPlan;
use crate::remote_query_scanner_manager::PartitionLocator;

#[restate_core::test]
async fn worker_nodes_list_their_replicas_indexes_and_jobs() {
    let env = TestCoreEnvBuilder::with_incoming_only_connector()
        .set_my_node_id(GenerationalNodeId::new(1, 1))
        .add_mock_nodes_config()
        .build()
        .await;
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    // Two replicas open with the status gauges: three statistics each, Ready at
    // once over empty partitions, their jobs completed without a pass.
    let mut config = Configuration::default();
    config.common.experimental.set_track_status_gauges(true);
    for id in [1, 2] {
        let mut store = manager
            .open(&Partition::new(PartitionId::from(id), KeyRange::FULL), None)
            .await
            .unwrap();
        store
            .verify_and_run_migrations(restate_core::cancellation_token(), &config)
            .await
            .unwrap();
    }

    let (registry, jobs) = create_local_scanners(
        Arc::clone(&manager),
        env.metadata.clone(),
        QueryIoExecutor::new(NonZeroUsize::MIN),
    );
    let remote = RemoteScannerManager::new(
        Arc::new(NoopSvc),
        Arc::new(AlwaysLocalPartitionLocator) as Arc<dyn PartitionLocator>,
        env.metadata.clone(),
    );
    // The worker locator finds this node, which answers with its local scanner.
    let locator = || {
        Arc::new(RoleBasedNodeLocator::new(
            Role::Worker,
            env.metadata.clone(),
        ))
    };
    let ctx = SessionContext::new();
    ctx.register_table(
        "registry",
        Arc::new(NodeFanOutTableProvider::new(
            RegistryBuilder::schema(),
            locator(),
            remote.clone(),
            Some(Arc::clone(&registry)),
            RegistryTable::identity(),
        )),
    )
    .unwrap();
    ctx.register_table(
        "migration_jobs",
        Arc::new(NodeFanOutTableProvider::new(
            MigrationJobsBuilder::schema(),
            locator(),
            remote,
            Some(Arc::clone(&jobs)),
            MigrationJobsTable::identity(),
        )),
    )
    .unwrap();
    let count = |sql: &'static str| {
        let ctx = ctx.clone();
        async move {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            batches[0].column(0).as_primitive::<Int64Type>().value(0)
        }
    };

    // Is every index readable everywhere? And is anything still being built?
    assert_eq!(
        count(
            "SELECT count(*) FROM registry
             WHERE plain_node_id = 'N1' AND gen_node_id = 'N1:1'
               AND kind = 'aggregate' AND status = 'ready' AND job_id IS NULL
               AND registered_at IS NOT NULL AND registered_by_version IS NOT NULL
               AND approx_size_bytes IS NOT NULL"
        )
        .await,
        6
    );
    assert_eq!(
        count(
            "SELECT count(*) FROM migration_jobs WHERE status IN ('pending', 'running', 'waiting')"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            "SELECT count(*) FROM migration_jobs
             WHERE kind = 'index_backfill' AND status = 'completed' AND mode = 'active'
               AND passes = 0 AND progress_percent IS NULL AND finished_at IS NOT NULL"
        )
        .await,
        6
    );
    // Without sizes, every index renders NULL: a report that skips them has
    // none, as one for a query that doesn't select the column.
    let report = manager
        .migration_report(&QueryIoExecutor::new(NonZeroUsize::MIN), IndexSizes::Skip)
        .unwrap()
        .await
        .unwrap();
    let render = |report: &MigrationReport| {
        batches::<RegistryBuilder, _>(
            RegistryBuilder::schema(),
            16,
            entries(report, None),
            |builder, (replica, entry)| {
                append_entry(builder, GenerationalNodeId::new(1, 1), replica, entry)
            },
        )
        .unwrap()
    };
    let rendered = render(&report);
    let sizes = rendered[0].column_by_name("approx_size_bytes").unwrap();
    assert_eq!((sizes.len(), sizes.null_count()), (6, 6));

    // Migrations follow a replica's indexes: one in progress, with its job, and
    // one complete, with its completion time; neither has a size.
    let mut replica = report.replicas[0].clone();
    let entries = &mut replica.registry;
    let by = || ReString::from_static("1.8.2");
    let running = entries
        .register_migration("cleanup/1", MigrationOnUnknown::Keep, 1_000, by())
        .unwrap();
    let done = entries
        .register_migration("cleanup/2", MigrationOnUnknown::Keep, 1_000, by())
        .unwrap();
    entries.complete(done, 2_000, by()).unwrap();
    replica.index_sizes.insert(done, 1);
    let rendered = render(&MigrationReport {
        replicas: vec![replica],
        unreadable: Vec::new(),
    });
    let column = |name| rendered[0].column_by_name(name).unwrap();
    let text = |name| column(name).as_string::<i32>().clone();
    let (kind, status) = (text("kind"), text("status"));
    let (job, completed) = (column("job_id"), column("completed_at"));
    assert_eq!(
        [3, 4].map(|row| (kind.value(row), status.value(row), job.is_null(row))),
        [
            ("migration", "in_progress", false),
            ("migration", "complete", true)
        ]
    );
    assert_eq!(
        job.as_primitive::<datafusion::arrow::datatypes::UInt32Type>()
            .value(3),
        running
    );
    assert_eq!(
        completed
            .as_primitive::<datafusion::arrow::datatypes::TimestampMillisecondType>()
            .value(4),
        2_000
    );
    assert!(completed.is_null(3) && column("approx_size_bytes").is_null(4));

    // `blocked_by` renders the reason the replica's driver reports for an
    // unfinished job, and NULL for a finished one.
    let mut replica = report.replicas[0].clone();
    replica.driver = Some(DriverState {
        disabled: true,
        ..DriverState::default()
    });
    let finished = replica.jobs.entries()[0].clone();
    assert!(finished.status.is_finished() && finished.expires_at.is_some());
    let mut running = finished.clone();
    running.status = MigrationJobStatus::Running;
    // A migration's job is active, whatever its name.
    let mut migration = finished.clone();
    (migration.kind, migration.name) = (MigrationJobKind::Migration, "cleanup/1".into());
    let rendered = batches::<MigrationJobsBuilder, _>(
        MigrationJobsBuilder::schema(),
        16,
        [
            (&replica, &running),
            (&replica, &finished),
            (&replica, &migration),
        ]
        .into_iter(),
        |builder, (replica, job)| {
            let now = tokio::time::Instant::now();
            append_job(builder, GenerationalNodeId::new(1, 1), replica, job, 0, now)
        },
    )
    .unwrap();
    let reasons = rendered[0]
        .column_by_name("blocked_by")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(
        (reasons.value(0), reasons.is_null(1)),
        ("background-migrations-disabled", true)
    );
    let column = |name| {
        rendered[0]
            .column_by_name(name)
            .unwrap()
            .as_string::<i32>()
            .clone()
    };
    assert_eq!(
        (column("kind").value(2), column("mode").value(2)),
        ("migration", "active")
    );

    let partitions = ctx
        .sql("SELECT DISTINCT partition_id FROM registry ORDER BY partition_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(partitions[0].column(0).len(), 2);
    // Both tables honor the row batch size and the total fetch limit, including
    // an empty projection used for counts.
    for (scanner, schema) in [
        (&registry, RegistryBuilder::schema()),
        (&jobs, MigrationJobsBuilder::schema()),
    ] {
        for (batch_size, limit, expected) in [
            (4, None, vec![4, 2]),
            (2, Some(3), vec![2, 1]),
            (4, Some(0), vec![]),
        ] {
            let rows: Vec<_> = scanner
                .scan(Arc::clone(&schema), &[], batch_size, limit)
                .map(|batch| batch.unwrap().num_rows())
                .collect()
                .await;
            assert_eq!(rows, expected);
        }
        let rows: Vec<_> = scanner
            .scan(Arc::new(Schema::empty()), &[], 4, None)
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(batch.num_columns(), 0);
                batch.num_rows()
            })
            .collect()
            .await;
        assert_eq!(rows, [4, 2]);
    }

    // A corrupt record on the second replica: a fully consumed scan of either
    // table still returns the first replica's rows, and reports one warning for
    // this node naming the replica and record.
    let mut store = manager
        .get_partition_store(PartitionId::from(2))
        .await
        .unwrap();
    let key = RecordKey::job(PartitionId::from(2), MigrationJobId::for_entry(64));
    let mut tx = store.transaction();
    tx.raw_put_cf(KeyKind::Registry, key, b"not a job");
    tx.commit().await.unwrap();
    drop(tx);
    for (table, scanner, schema) in [
        ("registry", &registry, RegistryBuilder::schema()),
        ("migration_jobs", &jobs, MigrationJobsBuilder::schema()),
    ] {
        let items: Vec<_> = scanner.scan(schema, &[], 2, None).collect().await;
        assert_eq!(items.len(), 3, "two healthy batches followed by one error");
        assert_eq!(items[0].as_ref().unwrap().num_rows(), 2);
        assert_eq!(items[1].as_ref().unwrap().num_rows(), 1);
        assert!(
            items[2]
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("partition 2 (job): job 64: malformed job record")
        );
        let plan = ctx
            .sql(&format!("SELECT * FROM {table}"))
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let mut nodes = vec![Arc::clone(&plan)];
        let mut warnings = Vec::new();
        while let Some(node) = nodes.pop() {
            if let Some(fan_out) = node.downcast_ref::<NodeFanOutExecutionPlan>() {
                warnings.push(Arc::clone(fan_out.node_warnings()));
            }
            nodes.extend(node.children().into_iter().cloned());
        }
        assert_eq!(warnings.len(), 1);
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3
        );
        let warnings = warnings[0].lock();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].node_id.as_str(), "N1");
        assert!(
            warnings[0].message.contains("partition 2 (job): job 64"),
            "{warnings:?}"
        );
    }

    // With a retention of 0, a replica keeps no finished job: its jobs expire as
    // they finish and the table hides them before compaction removes them,
    // while its indexes stay listed.
    config.worker.storage.finished_migration_jobs_retention = std::time::Duration::ZERO.into();
    let mut store = manager
        .open(&Partition::new(PartitionId::from(3), KeyRange::FULL), None)
        .await
        .unwrap();
    store
        .verify_and_run_migrations(restate_core::cancellation_token(), &config)
        .await
        .unwrap();
    assert_eq!(
        count("SELECT count(*) FROM registry WHERE partition_id = 3").await,
        3
    );
    assert_eq!(
        count("SELECT count(*) FROM migration_jobs WHERE partition_id = 3").await,
        0
    );
}
