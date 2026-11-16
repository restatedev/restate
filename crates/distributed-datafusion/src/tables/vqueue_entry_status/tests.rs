// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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

use datafusion::arrow::array::{
    BooleanArray, DurationMillisecondArray, LargeStringArray, TimestampMillisecondArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::prelude::{SessionContext, col, lit};
use futures::{StreamExt, TryStreamExt};
use googletest::all;
use googletest::prelude::{assert_that, eq};

use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryId, EntryKey, EntryKind, EntryMetadata, EntryStateRef, EntryValue, Stage,
    Status, WriteVQueueTable,
    stats::{EntryStatistics, WaitStats},
};
use restate_storage_query_api::QuerySession;
use restate_storage_query_api::tables::vqueue_entry_status::VqueueEntryStatus;
use restate_types::clock::UniqueTimestamp;
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{EntryTargetRef, Seq, VQueueId};
use restate_util_string::ToReString;
use restate_worker_api::CapturedPartitionRead;
use tokio_util::sync::CancellationToken;

use crate::access::PrimaryRead;
use crate::mocks::*;
use crate::row;
use crate::table_providers::{PartitionReadContext, ScanPartition};

use super::read::EntryStatusScanner;

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn vqueue_entry_status_not_in_returns_non_excluded_rows() {
    use datafusion::arrow::array::Array;

    let mut engine = MockQueryEngine::create().await;
    let qid = VQueueId::custom(3337, "df-vqueue-not-in");

    let created_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000)).unwrap();
    let metadata = EntryMetadata {
        deployment: None,
        needed_memory: None,
        retry_attempts: 0,
        retry_count_since_last_stored_command: 0,
    };

    let mut ids = Vec::new();
    let mut tx = engine.partition_store().transaction();
    for i in 1..=6u8 {
        let entry_id = EntryId::new(EntryKind::Invocation, [i; 16]);
        let key = EntryKey::new(
            false,
            MillisSinceEpoch::new(1_744_010_000_000),
            i as u64,
            entry_id,
        );
        let stats = EntryStatistics::new(created_at, key.run_at());
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
                entry_key: &key,
                metadata: &metadata,
                stats: &stats,
            },
        );
        ids.push(key.entry_id().display(qid.partition_key()).to_string());
    }
    tx.commit().await.unwrap();
    drop(tx);

    let excluded = ids[0..4]
        .iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let records = engine
        .execute(format!(
            "SELECT entry_id FROM sys_vqueue_entry_status \
             WHERE entry_id NOT IN ({excluded})"
        ))
        .await
        .unwrap()
        .stream
        .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
        .await
        .remove(0)
        .unwrap();

    let mut got: Vec<String> = records
        .column_by_name("entry_id")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .iter()
        .flatten()
        .map(str::to_string)
        .collect();
    got.sort();

    let mut expected = vec![ids[4].clone(), ids[5].clone()];
    expected.sort();

    assert_eq!(got, expected);

    // SQL routes literal IDs to multi-get; the scan path navigates the same
    // restriction by key. A canonical ID selects its base entry whatever its
    // incarnation (sequence 9 matches none here), so SQL keeps that residual.
    let (snapshot, _completed) = engine
        .partition_store()
        .query_read(CancellationToken::new())
        .unwrap();
    let context = PartitionReadContext::from_capture(CapturedPartitionRead {
        partition: PartitionId::MIN,
        storage: Some(snapshot),
        live: Default::default(),
    })
    .with_query_io(crate::mocks::query_io());
    let base = |i: u8| EntryId::new(EntryKind::Invocation, [i; 16]).to_base_id(qid.partition_key());
    let canonical =
        [(2, 2), (3, 9), (4, 4)].map(|(i, seq)| base(i).canonicalize(Seq::new(seq)).to_string());
    let schema = VqueueEntryStatus::arrow_schema();
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
    let filter = col("canonical_id").in_list(
        canonical
            .iter()
            .map(|id| lit(ScalarValue::LargeUtf8(Some(id.clone()))))
            .collect(),
        false,
    );
    let predicate = SessionContext::new()
        .create_physical_expr(filter, &df_schema)
        .unwrap();
    for (limit, expected) in [(None, vec![2, 3, 4]), (Some(1), vec![2])] {
        let batches = Arc::new(EntryStatusScanner)
            .prepare_read(
                PrimaryRead::Scan,
                schema.clone(),
                Some(Arc::clone(&predicate)),
                limit,
            )
            .unwrap()
            .execute(context.clone(), 8, Default::default())
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let found: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch.column_by_name("entry_id").unwrap();
                (0..batch.num_rows()).map(|row| array_value_to_string(column, row).unwrap())
            })
            .collect();
        let expected: Vec<_> = expected.into_iter().map(|i| base(i).to_string()).collect();
        assert_eq!(
            found, expected,
            "scan_path_canonical_ids_with_limit_{limit:?}"
        );
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_vqueue_entry_status_header_fields() {
    let mut engine = MockQueryEngine::create().await;

    let qid = VQueueId::custom(3337, "df-vqueue-entry-status");
    let entry_id = EntryId::new(EntryKind::Invocation, [9; 16]);
    let key = EntryKey::new(true, MillisSinceEpoch::new(1_744_010_001_000), 42, entry_id);

    let created_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_100)).unwrap();
    let transitioned_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_300)).unwrap();
    let first_attempt_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_400)).unwrap();
    let latest_attempt_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_500)).unwrap();

    let mut stats = EntryStatistics::new(created_at, key.run_at());
    stats.transitioned_at = transitioned_at;
    stats.num_attempts = 3;
    stats.num_errors = 1;
    stats.num_paused = 2;
    stats.num_suspensions = 4;
    stats.num_yields = 5;
    stats.first_attempt_at = Some(first_attempt_at);
    stats.latest_attempt_at = Some(latest_attempt_at);
    stats.latest_attempt_wait_stats = WaitStats {
        blocked_on_invoker_concurrency_ms: 10,
        blocked_on_throttling_rules_ms: 20,
        blocked_on_invoker_throttling_ms: 30,
        blocked_on_invoker_memory_ms: 40,
        blocked_on_concurrency_rules_ms: 50,
        blocked_on_lock_ms: 60,
        blocked_on_deployment_concurrency_ms: 70,
    };
    stats.total_wait_stats = WaitStats {
        blocked_on_invoker_concurrency_ms: 100,
        blocked_on_throttling_rules_ms: 200,
        blocked_on_invoker_throttling_ms: 300,
        blocked_on_invoker_memory_ms: 400,
        blocked_on_concurrency_rules_ms: 500,
        blocked_on_lock_ms: 600,
        blocked_on_deployment_concurrency_ms: 700,
    };

    let needed_memory = NonZeroUsize::new(4096).unwrap();
    let metadata = EntryMetadata {
        deployment: Some("dp_status".to_restring()),
        needed_memory: Some(needed_memory.into()),
        retry_attempts: 7,
        retry_count_since_last_stored_command: 2,
    };

    let mut tx = engine.partition_store().transaction();
    let first_runnable_at = stats.first_runnable_at;
    let latest_attempt_wait_stats = stats.latest_attempt_wait_stats;
    let total_wait_stats = stats.total_wait_stats;
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
            entry_key: &key,
            metadata: &metadata,
            stats: &stats,
        },
    );
    tx.commit().await.unwrap();
    drop(tx);

    let entry_id = key.entry_id().display(qid.partition_key()).to_string();
    let records = engine
        .execute(format!(
            "SELECT entry_id, canonical_id, vqueue_id, stage, status, has_lock, next_at, sequence_number, \
             entry_kind, created_at, transitioned_at, num_attempts, num_errors, num_pauses, \
             num_suspensions, num_yields, first_attempt_at, latest_attempt_at, \
             first_runnable_at, deployment, needed_memory, retry_attempts, \
             retry_count_since_last_stored_command, latest_attempt_blocked_on_invoker_concurrency, \
             latest_attempt_blocked_on_throttling_rules, latest_attempt_blocked_on_invoker_throttling, \
             latest_attempt_blocked_on_invoker_memory, latest_attempt_blocked_on_concurrency_rules, \
             latest_attempt_blocked_on_lock, latest_attempt_blocked_on_deployment_concurrency, \
             total_blocked_on_invoker_concurrency, total_blocked_on_throttling_rules, \
             total_blocked_on_invoker_throttling, total_blocked_on_invoker_memory, \
             total_blocked_on_concurrency_rules, total_blocked_on_lock, \
             total_blocked_on_deployment_concurrency FROM sys_vqueue_entry_status \
             WHERE entry_id = '{entry_id}' ORDER BY sequence_number"
        ))
        .await
        .unwrap()
        .stream
        .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
        .await
        .remove(0)
        .unwrap();

    assert_eq!(records.num_rows(), 1);
    assert_that!(
        records,
        all!(row!(
            0,
            {
                "entry_id" => LargeStringArray: eq(entry_id),
                "canonical_id" => LargeStringArray: eq(key.to_canonical_entry_id(qid.partition_key()).to_string()),
                "vqueue_id" => LargeStringArray: eq(qid.to_string()),
                "stage" => LargeStringArray: eq("running"),
                "status" => LargeStringArray: eq("started"),
                "has_lock" => BooleanArray: eq(true),
                "next_at" => TimestampMillisecondArray: eq(key.run_at().as_unix_millis().as_u64() as i64),
                "sequence_number" => UInt64Array: eq(42),
                "entry_kind" => LargeStringArray: eq("invocation"),
                "created_at" => TimestampMillisecondArray: eq(created_at.to_unix_millis().as_u64() as i64),
                "transitioned_at" => TimestampMillisecondArray: eq(transitioned_at.to_unix_millis().as_u64() as i64),
                "num_attempts" => UInt32Array: eq(3),
                "num_errors" => UInt32Array: eq(1),
                "num_pauses" => UInt32Array: eq(2),
                "num_suspensions" => UInt32Array: eq(4),
                "num_yields" => UInt32Array: eq(5),
                "first_attempt_at" => TimestampMillisecondArray: eq(first_attempt_at.to_unix_millis().as_u64() as i64),
                "latest_attempt_at" => TimestampMillisecondArray: eq(latest_attempt_at.to_unix_millis().as_u64() as i64),
                "first_runnable_at" => TimestampMillisecondArray: eq(first_runnable_at.as_u64() as i64),
                "deployment" => LargeStringArray: eq("dp_status"),
                "needed_memory" => UInt64Array: eq(needed_memory.get() as u64),
                "retry_attempts" => UInt32Array: eq(7),
                "retry_count_since_last_stored_command" => UInt32Array: eq(2),
                "latest_attempt_blocked_on_invoker_concurrency" => DurationMillisecondArray: eq(latest_attempt_wait_stats.blocked_on_invoker_concurrency_ms as i64),
                "latest_attempt_blocked_on_throttling_rules" => DurationMillisecondArray: eq(latest_attempt_wait_stats.blocked_on_throttling_rules_ms as i64),
                "latest_attempt_blocked_on_invoker_throttling" => DurationMillisecondArray: eq(latest_attempt_wait_stats.blocked_on_invoker_throttling_ms as i64),
                "latest_attempt_blocked_on_invoker_memory" => DurationMillisecondArray: eq(latest_attempt_wait_stats.blocked_on_invoker_memory_ms as i64),
                "latest_attempt_blocked_on_concurrency_rules" => DurationMillisecondArray: eq(latest_attempt_wait_stats.blocked_on_concurrency_rules_ms as i64),
                "latest_attempt_blocked_on_lock" => DurationMillisecondArray: eq(latest_attempt_wait_stats.blocked_on_lock_ms as i64),
                "latest_attempt_blocked_on_deployment_concurrency" => DurationMillisecondArray: eq(latest_attempt_wait_stats.blocked_on_deployment_concurrency_ms as i64),
                "total_blocked_on_invoker_concurrency" => DurationMillisecondArray: eq(total_wait_stats.blocked_on_invoker_concurrency_ms as i64),
                "total_blocked_on_throttling_rules" => DurationMillisecondArray: eq(total_wait_stats.blocked_on_throttling_rules_ms as i64),
                "total_blocked_on_invoker_throttling" => DurationMillisecondArray: eq(total_wait_stats.blocked_on_invoker_throttling_ms as i64),
                "total_blocked_on_invoker_memory" => DurationMillisecondArray: eq(total_wait_stats.blocked_on_invoker_memory_ms as i64),
                "total_blocked_on_concurrency_rules" => DurationMillisecondArray: eq(total_wait_stats.blocked_on_concurrency_rules_ms as i64),
                "total_blocked_on_lock" => DurationMillisecondArray: eq(total_wait_stats.blocked_on_lock_ms as i64),
                "total_blocked_on_deployment_concurrency" => DurationMillisecondArray: eq(total_wait_stats.blocked_on_deployment_concurrency_ms as i64),
            }
        ))
    );
}

/// `sys_vqueues` is a compatibility view over entry status. With stage entries and
/// statuses written consistently, as the vqueue state machine does, it matches V1,
/// which reads the stage tables.
#[restate_core::test]
async fn vqueues_view_matches_v1_stage_reads() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let created_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000)).unwrap();
    let stages = [
        (Stage::Inbox, Status::New),
        (Stage::Running, Status::Started),
        (Stage::Paused, Status::BackingOff),
        (Stage::Suspended, Status::Yielded),
        (Stage::Finished, Status::Started),
    ];
    let mut tx = store.transaction();
    let mut queues = Vec::new();
    let mut entries = Vec::new();
    for (queue, partition_key) in [(0u8, 7), (1, 9)] {
        let qid = VQueueId::custom(partition_key, format!("q{queue}"));
        for (index, (stage, status)) in stages.into_iter().enumerate() {
            let entry_id = EntryId::new(EntryKind::Invocation, [queue * 10 + index as u8; 16]);
            let entry_key = EntryKey::new(
                index % 2 == 0,
                MillisSinceEpoch::new(1_744_010_001_000 + index as u64 * 1_000),
                index as u64 + 1,
                entry_id,
            );
            let mut stats = EntryStatistics::new(created_at, entry_key.run_at());
            stats.num_attempts = index as u32;
            let metadata = EntryMetadata {
                deployment: Some("dp_1".to_restring()),
                ..Default::default()
            };
            let value = EntryValue {
                status,
                metadata: metadata.clone(),
                stats: stats.clone(),
            };
            tx.put_vqueue_inbox(&qid, stage, &entry_key, &value);
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
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
            entries.push(entry_id.display(partition_key).to_string());
        }
        queues.push(qid.to_string());
    }
    tx.commit().await.unwrap();
    drop(tx);
    // V2 reads queue- and active-stage-restricted queries through the stage
    // tables, whether or not SQL can see the index tables: showing them only adds
    // the inspection table. Either way, results match V1.
    let plan = |session: Arc<dyn QuerySession>, filter: String| async move {
        rows(
            session.as_ref(),
            &format!("EXPLAIN SELECT * FROM sys_vqueues WHERE {filter}"),
        )
        .await
        .concat()
        .join("\n")
    };
    // The stage tables, read as an index, carry the same key-derived columns.
    let columns = "stage, vqueue_id, has_lock, next_at, sequence_number, canonical_id, \
                   entry_id, partition_key";
    let inspect = |table: &str, filter: &str| {
        format!("SELECT {columns} FROM {table} WHERE {filter} ORDER BY canonical_id")
    };
    for index_tables in [false, true] {
        let mut config = Configuration::pinned().clone();
        config
            .common
            .experimental
            .set_query_index_tables(index_tables);
        Configuration::set(config);
        let [old, new] = differential_sessions(&manager).await;
        for sql in [
            "SELECT * FROM sys_vqueues ORDER BY canonical_id".to_owned(),
            "SELECT id, stage, run_at FROM sys_vqueues WHERE stage = 'inbox' ORDER BY id".into(),
            format!(
                "SELECT * FROM sys_vqueues WHERE id = '{}' ORDER BY sequence_number",
                queues[1]
            ),
            format!(
                "SELECT id, stage, status, has_lock FROM sys_vqueues WHERE entry_id IN ('{}', '{}') \
                 ORDER BY entry_id",
                entries[2], entries[8]
            ),
            "SELECT stage, COUNT(*), COUNT(run_at) FROM sys_vqueues GROUP BY stage ORDER BY stage"
                .into(),
            "SELECT id, sequence_number FROM sys_vqueues WHERE has_lock \
             ORDER BY id, sequence_number LIMIT 2 OFFSET 1"
                .into(),
            format!(
                "SELECT * FROM sys_vqueues WHERE id IN ('{}', '{}') ORDER BY canonical_id",
                queues[0], queues[1]
            ),
            format!(
                "SELECT id, stage, entry_id FROM sys_vqueues WHERE (id = '{}' AND has_lock) \
                 OR (id = '{}' AND stage = 'inbox') ORDER BY entry_id",
                queues[0], queues[1]
            ),
            "SELECT id, stage, status FROM sys_vqueues WHERE stage IN ('running', 'paused') \
             ORDER BY entry_id"
                .into(),
            format!(
                "SELECT entry_id FROM sys_vqueues WHERE id = '{}' AND num_attempts > 0 \
                 AND deployment = 'dp_1' ORDER BY sequence_number LIMIT 2 OFFSET 1",
                queues[1]
            ),
        ] {
            assert_eq!(
                rows(new.as_ref(), &sql).await,
                rows(old.as_ref(), &sql).await,
                "index_tables={index_tables}: {sql}"
            );
        }
        assert_eq!(
            rows(
                new.as_ref(),
                "SELECT COUNT(*), COUNT(run_at) FROM sys_vqueues"
            )
            .await,
            [["10", "2"]],
            "run_at_is_present_only_for_inbox_entries"
        );

        // Queue and active-stage restrictions read the stage tables; finished-only,
        // unrestricted and entry-ID reads keep their access paths.
        for (filter, access) in [
            (format!("id = '{}'", queues[1]), "index=_idx_entry_by_queue"),
            (
                "stage IN ('inbox', 'suspended')".to_owned(),
                "index=_idx_entry_by_queue",
            ),
            ("stage = 'finished'".into(), "TableScanExec"),
            (
                format!("id = '{}' OR num_attempts > 1", queues[1]),
                "TableScanExec",
            ),
            (format!("entry_id = '{}'", entries[2]), "MultiGetExec"),
        ] {
            let plan = plan(Arc::clone(&new), filter.clone()).await;
            assert!(
                plan.contains(access),
                "index_tables={index_tables}: {filter}: {plan}"
            );
            assert_eq!(
                plan.contains("IndexScanExec"),
                access.starts_with("index"),
                "{plan}"
            );
        }

        for filter in [
            "TRUE".to_owned(),
            format!("vqueue_id = '{}'", queues[1]),
            "stage IN ('inbox', 'finished') AND has_lock".into(),
        ] {
            let sql = inspect("_idx_entry_by_queue", &filter);
            if index_tables {
                assert_eq!(
                    rows(new.as_ref(), &sql).await,
                    rows(new.as_ref(), &inspect("sys_vqueue_entry_status", &filter)).await,
                    "stage_index_matches_entry_status: {filter}"
                );
            } else {
                let error = new
                    .execute(&sql, restate_storage_query_api::QueryOptions {})
                    .await
                    .err()
                    .expect("hidden index tables can't be queried")
                    .to_string();
                assert!(
                    error.contains("_idx_entry_by_queue") && error.contains("not found"),
                    "hidden_index_table: {error}"
                );
            }
        }
    }
}
