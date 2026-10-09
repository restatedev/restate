// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::{Array, LargeStringArray, StringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::functions::string::expr_fn::starts_with;
use datafusion::logical_expr::{Expr, col, lit};
use datafusion::physical_expr::planner::logical2physical;
use futures::TryStreamExt;

use restate_partition_store::PartitionStore;
use restate_partition_store::keys::KeyKind;
use restate_partition_store::stats::aggregated::VirtualObjectLoadKey;
use restate_storage_api::filter::Filter;
use restate_storage_api::stats::deployment_load::DeploymentLoad as DeploymentLoadTarget;
use restate_storage_api::stats::service_load::ServiceLoad;
use restate_storage_api::stats::virtual_object_load::VirtualObjectLoad as VirtualObjectLoadTarget;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryId, EntryKey, EntryKind, EntryMetadata, EntryStateRef, ReadVQueueTable,
    Stage, Status, WriteVQueueTable, stats::EntryStatistics,
};
use restate_storage_api::{StorageError, Transaction};
use restate_types::clock::UniqueTimestamp;
use restate_types::config::Configuration;
use restate_types::errors::ConversionError;
use restate_types::identifiers::DeploymentId;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryTargetRef, HandlerRef, VQueueId};

use crate::mocks::*;
use crate::partition_store_scanner::{ScanLocalPartition, ScanLocalPartitionFilter};
use crate::stats::deployment_stats::schema::SysDeploymentStatsBuilder;
use crate::stats::virtual_object_stats::schema::SysVirtualObjectStatsBuilder;

use super::schema::SysServiceStatsBuilder;
use super::table::ServiceStatsScanner;

async fn select(engine: &MockQueryEngine, query: &str) -> Vec<RecordBatch> {
    engine
        .execute(query)
        .await
        .unwrap()
        .stream
        .try_collect()
        .await
        .unwrap()
}

async fn row_count(engine: &MockQueryEngine, query: &str) -> usize {
    select(engine, query)
        .await
        .into_iter()
        .map(|batch| batch.num_rows())
        .sum()
}

/// Observe keys after storage filtering, before BatchSender/DataFusion evaluates residuals.
async fn scanned_service_names(store: &PartitionStore, predicate: Expr) -> Vec<String> {
    let predicate = logical2physical(&predicate, &SysServiceStatsBuilder::schema());
    let filter =
        <ServiceStatsScanner as ScanLocalPartition>::Filter::new(KeyRange::FULL, Some(predicate));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    ServiceStatsScanner::for_each_row(store, filter, move |(decoder, _)| {
        let key = decoder.try_full_decode::<ServiceLoad>().unwrap();
        tx.send(key.service_name.to_string()).unwrap();
        ControlFlow::Continue(())
    })
    .unwrap()
    .await
    .unwrap();

    let mut names = Vec::new();
    while let Some(name) = rx.recv().await {
        names.push(name);
    }
    names
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_tables_return_cluster_aggregates_and_partition_local_vo_counts() {
    let mut engine = MockQueryEngine::create().await;

    let mut config = Configuration::default();
    config.common.experimental.set_indexes_v1(true);
    engine
        .partition_store()
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();

    let deployment_a = DeploymentId::new().to_string();
    let deployment_b = DeploymentId::new().to_string();
    let created_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000)).unwrap();

    let mut tx = engine.partition_store().transaction();
    for (i, service_name, stage, status, deployment_id) in [
        (
            0,
            "alpha",
            Stage::Inbox,
            Status::Started,
            deployment_a.as_str(),
        ),
        (
            1,
            "alpha",
            Stage::Inbox,
            Status::Started,
            deployment_a.as_str(),
        ),
        (
            2,
            "alpha",
            Stage::Running,
            Status::Started,
            deployment_b.as_str(),
        ),
        (
            3,
            "beta",
            Stage::Paused,
            Status::Started,
            deployment_a.as_str(),
        ),
        (4, "gamma", Stage::Inbox, Status::New, deployment_a.as_str()),
    ] {
        let qid = VQueueId::custom(3337, format!("q{i}"));
        let entry_key = EntryKey::new(
            false,
            MillisSinceEpoch::new(1_744_010_001_000 + i),
            i,
            EntryId::new(EntryKind::Invocation, [i as u8 + 1; 16]),
        );
        let target = EntryTargetRef::Service {
            scope: None,
            service: service_name,
            handler: "handler",
        };
        let metadata = EntryMetadata {
            deployment: Some(deployment_id.into()),
            ..Default::default()
        };
        let stats = EntryStatistics::new(created_at, entry_key.run_at());
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &target,
            },
            EntryStateRef {
                stage,
                status,
                entry_key: &entry_key,
                metadata: &metadata,
                stats: &stats,
            },
        );

        if service_name == "gamma" {
            let header = tx
                .get_vqueue_entry_status(&entry_key.entry_id().to_base_id(qid.partition_key()))
                .await
                .unwrap()
                .unwrap();
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target,
                },
                EntryStateRef::from_header(&header),
                EntryStateRef {
                    stage: Stage::Running,
                    status: Status::Started,
                    entry_key: &entry_key,
                    metadata: &metadata,
                    stats: &stats,
                },
            );
        }
    }

    for (i, kind, target, stage, status) in [
        (
            5,
            EntryKind::Invocation,
            EntryTargetRef::VirtualObject {
                scope: Some("tenant"),
                service: "counter",
                key: "a",
                handler: HandlerRef::UserHandler("increment"),
            },
            Stage::Inbox,
            Status::New,
        ),
        (
            6,
            EntryKind::Invocation,
            EntryTargetRef::VirtualObject {
                scope: Some("tenant"),
                service: "counter",
                key: "a",
                handler: HandlerRef::UserHandler("increment"),
            },
            Stage::Inbox,
            Status::New,
        ),
        (
            7,
            EntryKind::StateMutation,
            EntryTargetRef::VirtualObject {
                scope: Some("tenant"),
                service: "counter",
                key: "a",
                handler: HandlerRef::StateMutation,
            },
            Stage::Running,
            Status::Started,
        ),
        (
            8,
            EntryKind::Invocation,
            EntryTargetRef::VirtualObject {
                scope: None,
                service: "counter",
                key: "b",
                handler: HandlerRef::UserHandler("get"),
            },
            Stage::Suspended,
            Status::Yielded,
        ),
    ] {
        // Distinct partition keys share the mock's one physical partition.
        let qid = VQueueId::custom(if i == 8 { 5337 } else { 3337 }, format!("q{i}"));
        let entry_key = EntryKey::new(
            false,
            MillisSinceEpoch::new(1_744_010_001_000 + i),
            i,
            EntryId::new(kind, [i as u8 + 1; 16]),
        );
        let metadata = EntryMetadata::default();
        let stats = EntryStatistics::new(created_at, entry_key.run_at());
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &target,
            },
            EntryStateRef {
                stage,
                status,
                entry_key: &entry_key,
                metadata: &metadata,
                stats: &stats,
            },
        );

        if i == 6 {
            let header = tx
                .get_vqueue_entry_status(&entry_key.entry_id().to_base_id(qid.partition_key()))
                .await
                .unwrap()
                .unwrap();
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target,
                },
                EntryStateRef::from_header(&header),
                EntryStateRef {
                    stage: Stage::Running,
                    status: Status::Started,
                    entry_key: &entry_key,
                    metadata: &metadata,
                    stats: &stats,
                },
            );
        }
    }
    tx.commit().await.unwrap();
    drop(tx);

    let error = ServiceStatsScanner::for_each_row(engine.partition_store(), Filter::All, |_| {
        ControlFlow::Break(Err(ConversionError::invalid_data(
            StorageError::DataIntegrityError,
        )))
    })
    .unwrap()
    .await
    .unwrap_err();
    assert!(matches!(error, StorageError::Conversion(_)));
    ServiceStatsScanner::for_each_row(engine.partition_store(), Filter::All, |_| {
        ControlFlow::Break(Ok(()))
    })
    .unwrap()
    .await
    .unwrap();

    let name = |value: &str| lit(ScalarValue::LargeUtf8(Some(value.to_owned())));
    for (predicate, expected) in [
        (col("service_name").eq(name("alpha")), vec!["alpha"]),
        (
            col("service_name").gt(name("beta")),
            vec!["counter", "counter", "counter", "gamma"],
        ),
        (
            col("service_name").gt_eq(name("beta")),
            vec!["beta", "counter", "counter", "counter", "gamma"],
        ),
        (col("service_name").lt(name("beta")), vec!["alpha"]),
        (
            col("service_name").lt_eq(name("beta")),
            vec!["alpha", "beta"],
        ),
        (
            name("beta").lt_eq(col("service_name")),
            vec!["beta", "counter", "counter", "counter", "gamma"],
        ),
        (
            col("service_name")
                .eq(name("counter"))
                .and(col("handler").lt(name("increment"))),
            vec!["counter"],
        ),
        (
            col("service_name")
                .gt_eq(name("beta"))
                .and(col("service_name").lt(name("beta"))),
            vec![],
        ),
        (col("service_name").like(name("alp%")), vec!["alpha"]),
        (starts_with(col("service_name"), name("alp")), vec!["alpha"]),
        // A suffix prefix must still discover matching keys under later services.
        (
            col("handler").like(name("h%")),
            vec!["alpha", "beta", "gamma"],
        ),
        (
            col("handler").like(name("%")),
            vec!["alpha", "beta", "counter", "counter", "gamma"],
        ),
        // The escaped percent is literal, not an early wildcard.
        (col("service_name").like(name(r"alp\%%")), vec![]),
        // Storage must include the upper endpoint and remove IN-list holes itself.
        (
            col("service_name").in_list(vec![name("alpha"), name("counter")], false),
            vec!["alpha", "counter", "counter", "counter"],
        ),
        // Even without a leading bound, storage can reject nonmatching keys.
        (col("kind").eq(name("state-mutation")), vec!["counter"]),
        (
            col("service_name")
                .eq(name("counter"))
                .and(col("kind").eq(name("state-mutation")))
                .and(col("handler").is_null()),
            vec!["counter"],
        ),
        (
            col("service_name")
                .eq(name("alpha"))
                .and(col("kind").eq(name("not-a-kind"))),
            vec![],
        ),
        (
            col("service_name")
                .eq(name("alpha"))
                .and(col("service_name").eq(name("beta"))),
            vec![],
        ),
    ] {
        assert_eq!(
            scanned_service_names(engine.partition_store(), predicate.clone()).await,
            expected,
            "{predicate}"
        );
    }

    // Count materialized storage keys, before SQL residuals or bucket expansion.
    // A suffix prefix must find matches under both deployment parents.
    for (predicate, expected) in [
        (col("service_name").like(name("alp%")), 2),
        (
            col("service_name").in_list(vec![name("alpha"), name("gamma")], false),
            3,
        ),
        (
            col("deployment_id")
                .gt_eq(name(&deployment_a))
                .and(col("deployment_id").lt_eq(name(&deployment_a))),
            3,
        ),
        (starts_with(col("deployment_id"), name(&deployment_b)), 1),
    ] {
        let filter = Filter::<DeploymentLoadTarget>::new(
            KeyRange::FULL,
            Some(logical2physical(
                &predicate,
                &SysDeploymentStatsBuilder::schema(),
            )),
        );
        let visits = Arc::new(AtomicUsize::new(0));
        let seen = visits.clone();
        engine
            .partition_store()
            .scan_deployment_load(&filter, move |_, _| {
                seen.fetch_add(1, Ordering::Relaxed);
                ControlFlow::Continue(())
            })
            .unwrap()
            .await
            .unwrap();
        assert_eq!(visits.load(Ordering::Relaxed), expected, "{predicate}");
    }
    for (predicate, expected) in [
        (col("scope").is_null(), 1),
        (col("scope").like(name("%")), 2),
        (col("scope").lt(name("zzz")), 2),
        (col("key").like(name("a%")), 2),
        (col("key").in_list(vec![name("a"), name("c")], false), 2),
        (col("handler").like(name("inc%")), 1),
        (col("handler").is_null(), 1),
        (col("kind").eq(name("invalid")), 0),
    ] {
        let filter = Filter::<VirtualObjectLoadTarget>::new(
            KeyRange::FULL,
            Some(logical2physical(
                &predicate,
                &SysVirtualObjectStatsBuilder::schema(),
            )),
        );
        let visits = Arc::new(AtomicUsize::new(0));
        let seen = visits.clone();
        engine
            .partition_store()
            .scan_virtual_object_load(&filter, move |_, _| {
                seen.fetch_add(1, Ordering::Relaxed);
                ControlFlow::Continue(())
            })
            .unwrap()
            .await
            .unwrap();
        assert_eq!(visits.load(Ordering::Relaxed), expected, "{predicate}");
    }

    let service_batches = select(
        &engine,
        "SELECT * FROM sys_service_stats
         WHERE service_name IN ('alpha', 'gamma') OR stage = 'paused'",
    )
    .await;
    let mut service_rows = Vec::new();
    for batch in service_batches {
        assert!(batch.column_by_name("partition_id").is_none());
        let services = batch
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let stages = batch
            .column_by_name("stage")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let statuses = batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let values = batch
            .column_by_name("num_entries")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        service_rows.extend((0..batch.num_rows()).map(|i| {
            (
                services.value(i).to_owned(),
                stages.value(i).to_owned(),
                statuses.value(i).to_owned(),
                values.value(i),
            )
        }));
    }
    service_rows.sort();
    assert_eq!(
        service_rows,
        [
            (
                "alpha".to_owned(),
                "inbox".to_owned(),
                "started".to_owned(),
                2,
            ),
            (
                "alpha".to_owned(),
                "running".to_owned(),
                "started".to_owned(),
                1,
            ),
            (
                "beta".to_owned(),
                "paused".to_owned(),
                "started".to_owned(),
                1,
            ),
            (
                "gamma".to_owned(),
                "running".to_owned(),
                "started".to_owned(),
                1,
            ),
        ]
    );

    let deployment_batches = select(&engine, "SELECT * FROM sys_deployment_stats").await;
    let mut deployment_rows = Vec::new();
    for batch in deployment_batches {
        assert!(batch.column_by_name("partition_id").is_none());
        let deployments = batch
            .column_by_name("deployment_id")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let services = batch
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let stages = batch
            .column_by_name("stage")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let values = batch
            .column_by_name("num_entries")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        deployment_rows.extend((0..batch.num_rows()).map(|i| {
            (
                deployments.value(i).to_owned(),
                services.value(i).to_owned(),
                stages.value(i).to_owned(),
                values.value(i),
            )
        }));
    }
    deployment_rows.sort();

    let mut expected = vec![
        (
            deployment_a.clone(),
            "alpha".to_owned(),
            "inbox".to_owned(),
            2,
        ),
        (
            deployment_a.clone(),
            "beta".to_owned(),
            "paused".to_owned(),
            1,
        ),
        (
            deployment_a.clone(),
            "gamma".to_owned(),
            "running".to_owned(),
            1,
        ),
        (deployment_b, "alpha".to_owned(), "running".to_owned(), 1),
    ];
    expected.sort();
    assert_eq!(deployment_rows, expected);

    let virtual_object_batches = select(&engine, "SELECT * FROM sys_virtual_object_stats").await;
    let mut virtual_object_rows = Vec::new();
    for batch in virtual_object_batches {
        assert!(batch.column_by_name("partition_id").is_none());
        let scopes = batch
            .column_by_name("scope")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let services = batch
            .column_by_name("service_name")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let keys = batch
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let kinds = batch
            .column_by_name("kind")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let handlers = batch
            .column_by_name("handler")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();
        let partition_keys = batch
            .column_by_name("partition_key")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(partition_keys.null_count(), 0);
        let counts = [
            "num_inbox",
            "num_running",
            "num_suspended",
            "num_paused",
            "num_finished",
        ]
        .map(|column| {
            let values = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(values.null_count(), 0, "{column}");
            values
        });
        virtual_object_rows.extend((0..batch.num_rows()).map(|i| {
            (
                (!scopes.is_null(i)).then(|| scopes.value(i).to_owned()),
                services.value(i).to_owned(),
                keys.value(i).to_owned(),
                kinds.value(i).to_owned(),
                (!handlers.is_null(i)).then(|| handlers.value(i).to_owned()),
                partition_keys.value(i),
                counts.map(|values| values.value(i)),
            )
        }));
    }
    virtual_object_rows.sort();

    let mut expected = vec![
        (
            Some("tenant".to_owned()),
            "counter".to_owned(),
            "a".to_owned(),
            "invocation".to_owned(),
            Some("increment".to_owned()),
            3337,
            [1, 1, 0, 0, 0],
        ),
        (
            Some("tenant".to_owned()),
            "counter".to_owned(),
            "a".to_owned(),
            "state-mutation".to_owned(),
            None,
            3337,
            [0, 1, 0, 0, 0],
        ),
        (
            None,
            "counter".to_owned(),
            "b".to_owned(),
            "invocation".to_owned(),
            Some("get".to_owned()),
            5337,
            [0, 0, 1, 0, 0],
        ),
    ];
    expected.sort();
    assert_eq!(virtual_object_rows, expected);

    assert_eq!(
        row_count(
            &engine,
            "SELECT * FROM sys_virtual_object_stats WHERE service_name = 'counter' LIMIT 1"
        )
        .await,
        1
    );

    assert_eq!(
        row_count(
            &engine,
            "SELECT * FROM sys_service_stats
             WHERE service_name IN ('alpha', 'gamma')
               AND kind = 'invocation'
               AND handler = 'handler'",
        )
        .await,
        3
    );
    assert_eq!(
        row_count(
            &engine,
            "SELECT * FROM sys_service_stats WHERE kind = 'invocation'",
        )
        .await,
        7
    );
    for (query, expected) in [
        (
            "SELECT * FROM sys_deployment_stats WHERE service_name LIKE 'alp%'",
            2,
        ),
        (
            "SELECT * FROM sys_deployment_stats WHERE service_name IN ('alpha', 'gamma')",
            3,
        ),
        (
            "SELECT * FROM sys_deployment_stats WHERE service_name > 'alpha'",
            2,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE scope IS NULL",
            1,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE scope LIKE '%'",
            2,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE scope < 'zzz'",
            2,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE key LIKE 'a%'",
            2,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE key IN ('a', 'c')",
            2,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE key >= 'a' AND key < 'b'",
            2,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE starts_with(handler, 'inc')",
            1,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE handler IS NULL",
            1,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE kind = 'invalid'",
            0,
        ),
        (
            "SELECT * FROM sys_virtual_object_stats WHERE scope = 'tenant' OR num_suspended > 0",
            3,
        ),
        (
            "SELECT num_running FROM sys_virtual_object_stats WHERE num_running > 0",
            2,
        ),
        (
            "SELECT num_paused, num_finished FROM sys_virtual_object_stats WHERE num_paused = 0 AND num_finished = 0",
            3,
        ),
        // Both selected keys belong to the same physical partition. Per-key fanout
        // would repeat the entire IN filter and duplicate every matching row.
        (
            "SELECT partition_key FROM sys_virtual_object_stats WHERE partition_key IN (3337, 5337)",
            3,
        ),
        (
            "SELECT partition_key FROM sys_virtual_object_stats WHERE partition_key = 3337 OR partition_key = 5337",
            3,
        ),
        // The envelope contains 5337, but the exact set must still exclude it.
        (
            "SELECT partition_key FROM sys_virtual_object_stats WHERE partition_key IN (3337, 7337)",
            2,
        ),
        (
            "SELECT partition_key FROM sys_virtual_object_stats WHERE partition_key = 5337",
            1,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name >= 'beta'",
            6,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name > 'beta'",
            5,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name < 'beta'",
            2,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name <= 'beta'",
            3,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE 'beta' <= service_name AND service_name < 'gamma'",
            5,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name = 'counter' AND handler <= 'increment'",
            3,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name LIKE 'alp%' AND service_name > 'alpha'",
            0,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name >= 'beta' AND service_name < 'beta'",
            0,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name = 'alpha' AND stage = 'running'",
            1,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name = 'counter' AND handler IS NULL",
            1,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name = 'counter' AND handler IN ('increment', NULL)",
            2,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name LIKE 'alp%' AND stage = 'running'",
            1,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE starts_with(service_name, 'alp')",
            2,
        ),
        ("SELECT * FROM sys_service_stats WHERE handler LIKE '%'", 7),
        (
            "SELECT * FROM sys_service_stats WHERE starts_with(handler, '')",
            7,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE starts_with(handler, NULL)",
            0,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name LIKE 'ALP%'",
            0,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name ILIKE 'ALP%'",
            2,
        ),
        (
            "SELECT * FROM sys_service_stats WHERE service_name LIKE 'al_h%'",
            2,
        ),
        (
            r"SELECT * FROM sys_service_stats WHERE service_name LIKE 'alp\%%'",
            0,
        ),
    ] {
        assert_eq!(row_count(&engine, query).await, expected, "{query}");
    }
    assert_eq!(
        row_count(
            &engine,
            &format!(
                "SELECT * FROM sys_deployment_stats
                 WHERE deployment_id = '{deployment_a}' AND service_name = 'alpha'"
            ),
        )
        .await,
        1
    );
    assert_eq!(
        row_count(
            &engine,
            "SELECT * FROM sys_virtual_object_stats
             WHERE service_name = 'counter' AND key = 'a'",
        )
        .await,
        2
    );
    assert_eq!(
        row_count(
            &engine,
            "SELECT * FROM sys_virtual_object_stats
             WHERE scope = 'tenant'
               AND service_name = 'counter'
               AND key = 'a'
               AND kind = 'invocation'
                AND handler = 'increment'",
        )
        .await,
        1
    );
    assert_eq!(
        row_count(
            &engine,
            "SELECT * FROM sys_virtual_object_stats
             WHERE scope IS NULL
               AND service_name = 'counter'
               AND key = 'b'
               AND kind = 'invocation'
               AND handler = 'get'",
        )
        .await,
        1
    );
    assert_eq!(
        row_count(
            &engine,
            "SELECT * FROM sys_service_stats
             WHERE service_name = 'alpha' AND kind = 'not-a-kind'",
        )
        .await,
        0
    );

    let explain = select(
        &engine,
        "EXPLAIN SELECT * FROM sys_service_stats
         WHERE service_name = 'alpha' AND kind = 'invocation'",
    )
    .await;
    let logical_plan = explain[0]
        .column_by_name("plan")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    let aggregate = logical_plan.find("Aggregate:").unwrap();
    let filter = logical_plan.find("Filter:").unwrap();
    let scan = logical_plan.find("TableScan:").unwrap();
    assert!(aggregate < filter && filter < scan, "{logical_plan}");
    assert!(
        logical_plan.contains(
            "partial_filters=[__sys_service_stats_partitioned.service_name = LargeUtf8(\"alpha\"), __sys_service_stats_partitioned.kind = LargeUtf8(\"invocation\")]"
        ),
        "{logical_plan}"
    );

    let explain = select(
        &engine,
        "EXPLAIN SELECT * FROM sys_virtual_object_stats WHERE service_name = 'counter' LIMIT 1",
    )
    .await;
    let plans = explain[0]
        .column_by_name("plan")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let logical_plan = plans.value(0);
    let physical_plan = plans.value(1);
    assert!(logical_plan.contains("Limit:"), "{logical_plan}");
    assert!(!logical_plan.contains("Aggregate:"), "{logical_plan}");
    assert!(
        physical_plan.contains("PartitionedExecutionPlan"),
        "{physical_plan}"
    );
    assert!(!physical_plan.contains("AggregateExec"), "{physical_plan}");
    assert!(
        !physical_plan.contains("partitioning=Hash("),
        "{physical_plan}"
    );

    // Run through SQL optimization, not just logical2physical. The malformed
    // leading fields detect both a full scan and a failure to seek over the gap.
    let partition = engine.partition_store().partition_id();
    let mut before_range = Vec::new();
    VirtualObjectLoadKey::prefix(partition, &mut before_range).service_name("0");
    before_range.pop();
    let mut first_gap = Vec::new();
    VirtualObjectLoadKey::prefix(partition, &mut first_gap)
        .service_name("B")
        .scope(None::<&str>)
        .key("gap")
        .handler(None::<&str>)
        .kind(EntryKind::Invocation)
        .partition_key(3337);
    let mut later_gap = Vec::new();
    VirtualObjectLoadKey::prefix(partition, &mut later_gap).service_name("C");
    later_gap.pop();
    let mut tx = engine.partition_store().transaction();
    for key in [before_range, first_gap, later_gap] {
        tx.raw_put_cf(KeyKind::Stats, key, b"malformed");
    }
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(
        row_count(
            &engine,
            "SELECT * FROM sys_virtual_object_stats WHERE service_name IN ('A', 'counter', 'z')"
        )
        .await,
        3
    );

    // Base-ID deletion must remove both the status row and the entry's stats.
    let mut tx = engine.partition_store().transaction();
    for (index, partition_key, target) in [
        (
            4,
            3337,
            EntryTargetRef::Service {
                scope: None,
                service: "gamma",
                handler: "handler",
            },
        ),
        (
            8,
            5337,
            EntryTargetRef::VirtualObject {
                scope: None,
                service: "counter",
                key: "b",
                handler: HandlerRef::UserHandler("get"),
            },
        ),
    ] {
        let qid = VQueueId::custom(partition_key, format!("q{index}"));
        let id = EntryId::new(EntryKind::Invocation, [index + 1; 16]).to_base_id(partition_key);
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        tx.delete_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &target,
            },
            EntryStateRef::from_header(&header),
        );
        assert!(tx.get_vqueue_entry_status(&id).await.unwrap().is_none());
    }
    tx.commit().await.unwrap();
    drop(tx);

    for query in [
        "SELECT * FROM sys_service_stats WHERE service_name = 'gamma' AND num_entries > 0",
        "SELECT * FROM sys_deployment_stats WHERE service_name = 'gamma' AND num_entries > 0",
        "SELECT * FROM sys_service_stats WHERE service_name = 'counter' AND handler = 'get' AND num_entries > 0",
        "SELECT * FROM sys_virtual_object_stats WHERE service_name = 'counter' AND key = 'b' AND num_suspended > 0",
    ] {
        assert_eq!(row_count(&engine, query).await, 0, "{query}");
    }
}
