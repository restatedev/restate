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
use restate_partition_store::fsm_table::PartitionStateMachineKey;
use restate_partition_store::keys::{EncodeTableKey, KeyKind};
use restate_partition_store::{PartitionStoreManager, QueryIoExecutor};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_types::GenerationalNodeId;
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::nodes_config::Role;
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;

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

    let (indexes, jobs) = create_local_scanners(
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
        "indexes",
        Arc::new(NodeFanOutTableProvider::new(
            IndexesBuilder::schema(),
            locator(),
            remote.clone(),
            Some(Arc::clone(&indexes)),
            IndexesTable::identity(),
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
            "SELECT count(*) FROM indexes
             WHERE plain_node_id = 'N1' AND gen_node_id = 'N1:1'
               AND kind = 'aggregate' AND status = 'ready' AND job_id IS NULL
               AND registered_at IS NOT NULL AND registered_by_version IS NOT NULL"
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
    let partitions = ctx
        .sql("SELECT DISTINCT partition_id FROM indexes ORDER BY partition_id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(partitions[0].column(0).len(), 2);
    // Both tables honor the row batch size and the total fetch limit, including
    // an empty projection used for counts.
    for (scanner, schema) in [
        (&indexes, IndexesBuilder::schema()),
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
    let mut key = Vec::new();
    PartitionStateMachineKey {
        partition_id: PartitionId::from(2).into(),
        // The job history's FSM variable.
        state_id: 17,
    }
    .serialize_to(&mut key);
    let mut tx = store.transaction();
    tx.raw_put_cf(KeyKind::Fsm, key, b"not a job list");
    tx.commit().await.unwrap();
    drop(tx);
    for (table, scanner, schema) in [
        ("indexes", &indexes, IndexesBuilder::schema()),
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
                .contains("partition 2 (job history)")
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
            warnings[0].message.contains("partition 2 (job history)"),
            "{warnings:?}"
        );
    }
}
