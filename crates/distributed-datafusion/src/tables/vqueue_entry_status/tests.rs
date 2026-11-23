// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
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

/// The ways to ask for a descending order: NULLs last, by default (first),
/// and first.
const SPELLINGS: [&str; 3] = ["DESC NULLS LAST", "DESC", "DESC NULLS FIRST"];

/// The ways to ask for the reverse, ascending order: NULLs last, by default
/// (last), and first.
const REVERSED: [&str; 3] = ["ASC NULLS LAST", "ASC", "ASC NULLS FIRST"];

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
        timing: restate_worker_api::CaptureTiming::admitted_at(tokio::time::Instant::now()),
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
                "status" => LargeStringArray: eq("running"),
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
        // Each row shows its effective status, in both engines: the stage's own
        // for running, suspended and paused entries whatever they stored, and
        // NULL for a Finished entry without an outcome, the `unknown` sentinel,
        // which no status name selects; a status condition selects by the rest.
        for session in [&old, &new] {
            assert_eq!(
                rows(
                    session.as_ref(),
                    "SELECT DISTINCT stage, COALESCE(status, 'NULL') FROM sys_vqueues \
                     ORDER BY stage"
                )
                .await,
                [
                    ["finished", "NULL"],
                    ["inbox", "new"],
                    ["paused", "paused"],
                    ["running", "running"],
                    ["suspended", "suspended"],
                ],
                "effective_status"
            );
            assert_eq!(
                rows(
                    session.as_ref(),
                    "SELECT COUNT(*) FROM sys_vqueue_entry_status \
                     WHERE status IN ('paused', 'yielded', 'unknown')"
                )
                .await,
                [["2"]],
                "status_conditions_select_effective_statuses"
            );
        }

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

    // The UI's finished page: more terminal entries than it shows, newest
    // created first, with newer running entries it rejects. Entries are created
    // in pairs at the same time, and one pair straddles the page's LIMIT.
    let finished = include_str!("../../ui_queries/vqueues_finished_newest.sql");
    let qid = VQueueId::custom(11, "finished");
    let terminal = [
        Status::Succeeded,
        Status::Failed,
        Status::Cancelled,
        Status::Killed,
    ];
    let mut created_of = HashMap::new();
    let mut tx = store.transaction();
    for n in 0..320u64 {
        let (stage, status) = if n < 300 {
            (Stage::Finished, terminal[n as usize % terminal.len()])
        } else {
            (Stage::Running, Status::Started)
        };
        let at = MillisSinceEpoch::new(1_744_020_000_000 + n.div_ceil(2) * 3);
        let entry_id = EntryId::new(EntryKind::Invocation, (1_000 + u128::from(n)).to_be_bytes());
        if stage == Stage::Finished {
            created_of.insert(entry_id.display(11).to_string(), at);
        }
        let entry_key = EntryKey::new(false, at, 100 + n, entry_id);
        let stats = EntryStatistics::new(
            UniqueTimestamp::try_from_unix_millis(at).unwrap(),
            entry_key.run_at(),
        );
        let metadata = EntryMetadata::default();
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
    }
    tx.commit().await.unwrap();
    drop(tx);
    let [old, new] = differential_sessions(&manager).await;
    // Both return the newest 250 creation times of the terminal entries, each
    // entry once.
    let mut terminal: Vec<_> = created_of.values().copied().collect();
    terminal.sort_by(|a, b| b.cmp(a));
    assert_eq!(terminal[249], terminal[250], "the LIMIT splits a pair");
    for session in [&old, &new] {
        let found = rows(session.as_ref(), finished).await;
        let times: Vec<_> = found.iter().map(|row| created_of[&row[0]]).collect();
        assert_eq!(times, terminal[..250]);
        assert_eq!(found.iter().collect::<HashSet<_>>().len(), 250);
    }
    // The page needs only columns the stage records hold, so V2 reads the
    // Finished stage records alone, as V1 does, and fetches no status. The read
    // checks the page's predicate before emitting: of the 302 records (2 of the
    // earlier fixture, 300 terminal), it emits the 300.
    let plan = rows(new.as_ref(), &format!("EXPLAIN ANALYZE {finished}"))
        .await
        .concat()
        .join("\n");
    let source = plan
        .lines()
        .find(|line| line.contains("IndexScanExec"))
        .unwrap_or_else(|| panic!("{plan}"));
    assert!(
        source.contains("index=_idx_entry_by_queue")
            && source.contains("stage_values_decoded=302")
            && !source.contains("primary_keys_fetched"),
        "{plan}"
    );
    let metric = |name: &str| {
        let (_, value) = source.split_once(&format!("{name}="))?;
        let digits = value
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(value.len());
        value[..digits].parse::<u64>().ok()
    };
    assert_eq!(
        (metric("read_rows"), metric("output_rows")),
        (Some(302), Some(300)),
        "{source}"
    );
}

/// Finished entries newest finished first, through the Finished lanes of
/// `entry_by_status_transitioned/1` when every alternative fixes the stage to
/// stages the index holds: the same rows as the current-status scan, ties
/// included, with the read's path and work shown. A query that doesn't fix its
/// stages, or wants another order, reads another way.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn finished_entries_read_through_the_transition_index_match_the_scan() {
    use restate_types::vqueues::HandlerRef;

    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut config = Configuration::default();
    config.common.experimental.set_track_entry_indexes(true);
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    // The planner reads the index only where it's maintained.
    Configuration::set(config.clone());

    const BASE: u64 = 1_744_010_000_000;
    let qid = VQueueId::custom(7, "q");
    let statuses = [
        (Stage::Finished, Status::Succeeded),
        (Stage::Finished, Status::Failed),
        (Stage::Finished, Status::Killed),
        (Stage::Finished, Status::Cancelled),
        // Finished, with a status that isn't an outcome: still a member.
        (Stage::Finished, Status::Started),
        (Stage::Running, Status::Started),
        (Stage::Inbox, Status::New),
    ];
    // The same entries in a partition that doesn't maintain the indexes.
    let mut unready = manager
        .open(&Partition::new(PartitionId::from(1), KeyRange::FULL), None)
        .await
        .unwrap();
    for store in [&mut store, &mut unready] {
        let mut tx = store.transaction();
        for n in 0..140u64 {
            let (stage, status) = statuses[(n % 7) as usize];
            let created =
                UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(BASE + n));
            // Pairs share their transition time: ties for the LIMIT to split.
            let transitioned = UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(
                BASE + 10_000 + (n / 2) * 10,
            ));
            let kind = if n % 5 == 0 {
                EntryKind::StateMutation
            } else {
                EntryKind::Invocation
            };
            let key = EntryKey::new(
                false,
                created.to_unix_millis(),
                n + 1,
                EntryId::new(kind, [n as u8; 16]),
            );
            let mut stats = EntryStatistics::new(created, key.run_at());
            stats.transitioned_at = transitioned;
            let service = ["Utils", "Promises", "Counter"][(n % 3) as usize];
            let handler = if kind == EntryKind::StateMutation {
                HandlerRef::StateMutation
            } else {
                HandlerRef::UserHandler(["call", "run"][(n % 2) as usize])
            };
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &EntryTargetRef::VirtualObject {
                        scope: None,
                        service,
                        key: "k",
                        handler,
                    },
                },
                EntryStateRef {
                    stage,
                    status,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &stats,
                },
            );
        }
        tx.commit().await.unwrap();
        drop(tx);
    }

    let session = crate::mocks::v2_session(
        &manager,
        vec![(
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::FULL),
        )],
        2,
    )
    .await;
    let rows = async |sql: &str| crate::mocks::rows(session.as_ref(), sql).await;
    let plan = async |sql: &str, analyze: bool| {
        let mode = if analyze {
            "EXPLAIN ANALYZE"
        } else {
            "EXPLAIN"
        };
        rows(&format!("{mode} {sql}")).await.concat().join("\n")
    };
    // The same query over a scan the index can't serve.
    let scanned = async |sql: &str| {
        rows(&sql.replacen(
            "FROM sys_vqueue_entry_status",
            "FROM (SELECT * FROM sys_vqueue_entry_status LIMIT 1000000) AS sys_vqueue_entry_status",
            1,
        ))
        .await
    };
    const INDEX: &str = "_idx_entry_status_by_status_transitioned";
    let select = "SELECT entry_id, status, transitioned_at FROM sys_vqueue_entry_status";
    let indexed = [
        format!(
            "{select} WHERE stage = 'finished' ORDER BY transitioned_at DESC NULLS LAST LIMIT 7"
        ),
        format!(
            "{select} WHERE stage = 'finished' AND status IN ('failed', 'killed') \
             ORDER BY transitioned_at DESC NULLS LAST LIMIT 5 OFFSET 2"
        ),
        format!(
            "{select} WHERE stage = 'finished' AND transitioned_at < to_timestamp_millis({}) \
             ORDER BY transitioned_at DESC NULLS LAST LIMIT 4",
            BASE + 10_300
        ),
        // Renamed above the source, the column is still never NULL.
        "SELECT entry_id, status, transitioned_at AS finished FROM sys_vqueue_entry_status \
         WHERE stage = 'finished' ORDER BY finished DESC NULLS LAST LIMIT 6"
            .into(),
        // Any other condition is checked on each candidate: a column the
        // index doesn't hold, arithmetic.
        format!(
            "{select} WHERE stage = 'finished' AND num_attempts = 0 \
             ORDER BY transitioned_at DESC NULLS LAST LIMIT 5"
        ),
        format!(
            "{select} WHERE stage = 'finished' AND num_attempts + 1 = 1 \
             ORDER BY transitioned_at DESC NULLS LAST LIMIT 5"
        ),
        // A status alone: the entries the index doesn't hold, those in a stage
        // this binary doesn't know, show no status and match none.
        format!(
            "{select} WHERE status = 'failed' ORDER BY transitioned_at DESC NULLS LAST LIMIT 5"
        ),
        // Two stages: both stages' lanes, merged.
        format!(
            "{select} WHERE stage IN ('finished', 'running') \
             ORDER BY transitioned_at DESC NULLS LAST LIMIT 5"
        ),
    ];
    let scans = [
        // Alternatives that together restrict neither column.
        format!(
            "{select} WHERE stage = 'finished' OR status = 'new' \
             ORDER BY transitioned_at DESC NULLS LAST LIMIT 5"
        ),
        // `unknown` names no status: the query selects nothing before any read.
        format!(
            "{select} WHERE stage = 'finished' AND status = 'unknown' \
             ORDER BY transitioned_at DESC NULLS LAST LIMIT 3"
        ),
        // Another order reads the creation index instead.
        format!("{select} WHERE stage = 'finished' ORDER BY created_at DESC NULLS LAST LIMIT 5"),
        // An outer join can add NULLs: nothing is asked of the source.
        "SELECT e.entry_id, e.status, e.transitioned_at FROM (SELECT 1 AS one) AS o \
         LEFT JOIN sys_vqueue_entry_status AS e ON e.stage = 'finished' \
         ORDER BY e.transitioned_at DESC LIMIT 5"
            .into(),
    ];
    // Tie-aware: the transition times in order, and every row a qualifying one.
    let same =
        |actual: &[Vec<String>], expected: &[Vec<String>], all: &[Vec<String>], sql: &str| {
            let times =
                |rows: &[Vec<String>]| rows.iter().map(|row| row[2].clone()).collect::<Vec<_>>();
            assert_eq!(times(actual), times(expected), "{sql}");
            assert!(actual.iter().all(|row| all.contains(row)), "{sql}");
            assert_eq!(
                actual.iter().collect::<HashSet<_>>().len(),
                actual.len(),
                "{sql}"
            );
        };
    // The ordered column is never NULL: wherever the query puts NULLs, by
    // default first for DESC, it reads the index in the same plan, but for the
    // spelling, and gets the same rows. Ascending, it reads the index
    // backwards, in one plan for every spelling too.
    let unspelled = |plan: String| plan.replace(" NULLS FIRST", "").replace(" NULLS LAST", "");
    for sql in &indexed {
        let all = scanned(&sql.replace(" LIMIT", " LIMIT 1000000 --")).await;
        for spellings in [SPELLINGS, REVERSED] {
            let first = sql.replace("DESC NULLS LAST", spellings[0]);
            let explained = plan(&first, false).await;
            assert!(explained.contains(INDEX), "{first}: {explained}");
            for spelling in spellings {
                let sql = sql.replace("DESC NULLS LAST", spelling);
                assert_eq!(
                    unspelled(plan(&sql, false).await),
                    unspelled(explained.clone()),
                    "{sql}"
                );
                same(&rows(&sql).await, &scanned(&sql).await, &all, &sql);
            }
        }
    }
    for sql in &scans {
        let explained = plan(sql, false).await;
        assert!(!explained.contains(INDEX), "{sql}: {explained}");
    }
    // Entries of some stages and statuses by creation time, as the UI's
    // invocation list asks, read the status index: one lane per stage and
    // status, merged into creation order. By transition time, a running page
    // reads the transition index, as finished pages do above.
    // Creation times are distinct here, as are running entries' transition
    // times, so the rows equal the scan's in order.
    let by_created = "SELECT entry_id, status, created_at FROM sys_vqueue_entry_status \
        WHERE stage = 'finished' AND status IN ('succeeded', 'failed', 'cancelled', 'killed') \
        ORDER BY created_at DESC NULLS LAST LIMIT 9";
    let running = "SELECT entry_id, status, transitioned_at FROM sys_vqueue_entry_status \
        WHERE stage = 'running' ORDER BY transitioned_at DESC NULLS LAST LIMIT 5";
    for (sql, index) in [
        (by_created, "_idx_entry_status_by_status_created"),
        (running, "_idx_entry_status_by_status_transitioned"),
    ] {
        let explained = plan(sql, false).await;
        assert!(
            explained.contains(&format!("index={index}")),
            "{sql}: {explained}"
        );
        assert_eq!(rows(sql).await, scanned(sql).await, "{sql}");
    }
    // The UI's own query, through the `sys_vqueues` view.
    let ui = include_str!("../../ui_queries/vqueues_finished_newest.sql");
    let explained = plan(ui, false).await;
    assert!(
        explained.contains("index=_idx_entry_status_by_status_created"),
        "{explained}"
    );
    let ui_scanned = ui.replacen(
        "FROM sys_vqueues v",
        "FROM (SELECT * FROM sys_vqueues LIMIT 1000000) AS v",
        1,
    );
    assert_eq!(rows(ui).await, rows(&ui_scanned).await);
    // All of the view's columns, `run_at` computed among them: the sort's
    // column is one of the entries' own, so the page reads the index too.
    let every_column = "SELECT * FROM sys_vqueues \
        WHERE stage = 'finished' AND status IN ('succeeded', 'failed', 'cancelled', 'killed') \
        ORDER BY created_at DESC NULLS LAST LIMIT 9";
    let explained = plan(every_column, false).await;
    assert!(
        explained.contains("index=_idx_entry_status_by_status_created"),
        "{explained}"
    );
    assert_eq!(
        rows(every_column).await,
        rows(&every_column.replacen(
            "FROM sys_vqueues",
            "FROM (SELECT * FROM sys_vqueues LIMIT 1000000) AS v",
            1
        ))
        .await
    );
    // A LIMIT without an ORDER BY whose stages and statuses leave fewer lanes
    // than all reads those lanes too, as the UI's failed entries of a
    // deployment, byte for byte: the Failed lane, the deployment checked on
    // each fetched status; and Finished entries alone. Any matches will do: a
    // page holds the LIMIT's count of distinct matches.
    let failed = include_str!("../../ui_queries/vqueues_failed_by_deployment.sql");
    let undeployed = "SELECT entry_id FROM sys_vqueue_entry_status \
        WHERE stage = 'finished' AND status = 'failed' AND deployment IS NULL LIMIT 3";
    let finished = "SELECT entry_id FROM sys_vqueue_entry_status WHERE stage = 'finished' LIMIT 3";
    for sql in [failed, undeployed, finished] {
        let explained = plan(sql, false).await;
        assert!(
            explained.contains("index=_idx_entry_status_by_status_created"),
            "{sql}: {explained}"
        );
    }
    assert_eq!(
        rows(failed).await,
        rows(&failed.replacen(
            "FROM sys_vqueues v",
            "FROM (SELECT * FROM sys_vqueues LIMIT 1000000) AS v",
            1,
        ))
        .await
    );
    let page = rows(undeployed).await;
    let all: HashSet<_> = scanned(&undeployed.replace(" LIMIT 3", ""))
        .await
        .into_iter()
        .collect();
    assert_eq!(page.len(), 3.min(all.len()));
    assert!(page.len() > 1 && page.iter().all(|row| all.contains(row)));
    assert_eq!(page.iter().collect::<HashSet<_>>().len(), page.len());
    // Queries that need only a status index's key columns read its keys alone,
    // fetching no status: the UI's page, in creation order, and a count of
    // finished entries by status since a transition time, in no order. Without
    // a time bound, such a count reads the stage tables as before; a column
    // the keys don't hold, the queue, fetches the statuses.
    let fetches_nothing = |analyzed: &str| {
        analyzed.contains("entry_index_reads=1") && analyzed.contains("entry_index_statuses=0")
    };
    let analyzed = plan(ui, true).await;
    assert!(fetches_nothing(&analyzed), "{analyzed}");
    let count = format!(
        "SELECT status, COUNT(1) AS count FROM sys_vqueues WHERE stage = 'finished' \
         AND transitioned_at >= to_timestamp_millis({}) GROUP BY status ORDER BY status",
        BASE + 10_300
    );
    let explained = plan(&count, false).await;
    assert!(
        explained.contains("index=_idx_entry_status_by_status_transitioned"),
        "{explained}"
    );
    let counted = rows(&count).await;
    assert!(counted.len() > 1, "{counted:?}");
    let count_scanned = count.replacen(
        "FROM sys_vqueues",
        "FROM (SELECT * FROM sys_vqueues LIMIT 1000000) AS sys_vqueues",
        1,
    );
    assert_eq!(counted, rows(&count_scanned).await);
    let analyzed = plan(&count, true).await;
    assert!(fetches_nothing(&analyzed), "{analyzed}");
    // A column the keys don't hold only in a condition: the count fetches the
    // statuses. (Neither do the stage tables hold it.)
    let conditioned = count
        .replace("FROM sys_vqueues", "FROM sys_vqueue_entry_status")
        .replace("GROUP BY", "AND needed_memory IS NULL GROUP BY");
    let explained = plan(&conditioned, false).await;
    assert!(
        !explained.contains("_idx_entry_status") && !explained.contains("_idx_entry_by_queue"),
        "{explained}"
    );
    assert_eq!(rows(&conditioned).await, counted);
    // Where the index isn't Complete, the count scans the entry statuses
    // instead, with the same counts.
    let unready = crate::mocks::v2_session(
        &manager,
        vec![(
            PartitionId::from(1),
            Partition::new(PartitionId::from(1), KeyRange::FULL),
        )],
        2,
    )
    .await;
    let unready = async |sql: &str| crate::mocks::rows(unready.as_ref(), sql).await;
    assert_eq!(unready(&count).await, counted);
    let analyzed = unready(&format!("EXPLAIN ANALYZE {count}"))
        .await
        .concat()
        .join("\n");
    assert!(
        analyzed.contains("index=_idx_entry_status_by_status_transitioned")
            && analyzed.contains("entry_index_scans_not_complete=1"),
        "{analyzed}"
    );
    // Without a time bound, or until a time only, which leaves most of the
    // history, such a read keeps the stage tables: a count, and a page with a
    // tie-breaker, which asks for no order the index has.
    let unbounded =
        "SELECT status, COUNT(1) FROM sys_vqueues WHERE stage = 'finished' GROUP BY status";
    let until = format!(
        "SELECT entry_id, created_at FROM sys_vqueue_entry_status WHERE stage = 'finished' \
         AND created_at <= to_timestamp_millis({}) ORDER BY created_at, entry_id LIMIT 9",
        BASE + 100
    );
    for sql in [unbounded, until.as_str()] {
        let explained = plan(sql, false).await;
        assert!(!explained.contains("_idx_entry_status"), "{explained}");
    }
    let fetched = by_created.replace("entry_id, status", "entry_id, vqueue_id, status");
    assert_eq!(rows(&fetched).await, scanned(&fetched).await, "{fetched}");
    let analyzed = plan(&fetched, true).await;
    assert!(
        analyzed.contains("index=_idx_entry_status_by_status_created")
            && !analyzed.contains("entry_index_statuses=0"),
        "{analyzed}"
    );
    // The read went through the index, merging its runs, and fetched only
    // Finished statuses.
    let analyzed = plan(&indexed[0], true).await;
    assert!(
        analyzed.contains("entry_index_reads=1") && !analyzed.contains("entry_index_scans=1"),
        "{analyzed}"
    );
    assert!(analyzed.contains("entry_index_skipped=0"), "{analyzed}");
    // Backwards too.
    let analyzed = plan(
        &indexed[0].replace("DESC NULLS LAST", "ASC NULLS LAST"),
        true,
    )
    .await;
    assert!(
        analyzed.contains("entry_index_reads=1") && !analyzed.contains("entry_index_scans=1"),
        "{analyzed}"
    );

    // More runs than one read merges in its first hour, 260 services in one
    // lane: the partition scans and sorts instead, before returning anything,
    // and says why and what discovery examined.
    let mut tx = store.transaction();
    for service in 0..260u64 {
        let created = UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(
            BASE + 1_000 + service,
        ));
        let key = EntryKey::new(
            false,
            created.to_unix_millis(),
            1_000 + service,
            EntryId::new(
                EntryKind::Invocation,
                [
                    200,
                    service as u8,
                    (service >> 8) as u8,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                ],
            ),
        );
        let name = format!("s{service}");
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &EntryTargetRef::Service {
                    scope: None,
                    service: &name,
                    handler: "h",
                },
            },
            EntryStateRef {
                stage: Stage::Finished,
                status: Status::Succeeded,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created, key.run_at()),
            },
        );
    }
    tx.commit().await.unwrap();
    drop(tx);
    for sql in &indexed[..2] {
        let all = scanned(&sql.replace(" LIMIT", " LIMIT 1000000 --")).await;
        for spelling in SPELLINGS {
            let sql = sql.replace("DESC NULLS LAST", spelling);
            same(&rows(&sql).await, &scanned(&sql).await, &all, &sql);
        }
    }
    let analyzed = plan(&indexed[0], true).await;
    assert!(
        analyzed.contains("entry_index_scans_too_many_runs=1"),
        "{analyzed}"
    );
    assert!(
        !analyzed.contains("entry_index_keys_examined=0"),
        "{analyzed}"
    );
}

/// Active entries newest created first, through the lanes of
/// `entry_by_status_created/1` when every alternative fixes the stage to known
/// stages: the same rows as the current-status scan, ties included, with the
/// read's path and work shown. A query that doesn't prove its stages, or wants
/// another order, reads another way; so does one on a few queues, which reads
/// the stage tables first, and one without the order.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn active_entries_read_through_the_status_index_match_the_scan() {
    use restate_types::vqueues::HandlerRef;

    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut config = Configuration::default();
    config.common.experimental.set_track_entry_indexes(true);
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    // The planner reads the index only where it's maintained.
    Configuration::set(config.clone());

    const BASE: u64 = 1_744_010_000_000;
    let qid = VQueueId::custom(7, "q");
    let stages = [
        (Stage::Inbox, Status::New),
        (Stage::Running, Status::Started),
        (Stage::Suspended, Status::Yielded),
        (Stage::Paused, Status::BackingOff),
        (Stage::Inbox, Status::BackingOff),
        (Stage::Finished, Status::Succeeded),
    ];
    let mut tx = store.transaction();
    for n in 0..144u64 {
        let (stage, status) = stages[(n % 6) as usize];
        // Pairs share a creation millisecond, apart by their logical clock:
        // ties for the LIMIT to split.
        let created = UniqueTimestamp::from_parts_unchecked(BASE + n / 2, n % 2);
        let kind = if n % 5 == 0 {
            EntryKind::StateMutation
        } else {
            EntryKind::Invocation
        };
        let key = EntryKey::new(
            false,
            created.to_unix_millis(),
            n + 1,
            EntryId::new(kind, [n as u8; 16]),
        );
        let service = ["Utils", "Promises", "Counter"][(n % 3) as usize];
        let handler = if kind == EntryKind::StateMutation {
            HandlerRef::StateMutation
        } else {
            HandlerRef::UserHandler(["call", "run"][(n % 2) as usize])
        };
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &EntryTargetRef::VirtualObject {
                    scope: None,
                    service,
                    key: "k",
                    handler,
                },
            },
            EntryStateRef {
                stage,
                status,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created, key.run_at()),
            },
        );
    }
    tx.commit().await.unwrap();
    drop(tx);

    let session = crate::mocks::v2_session(
        &manager,
        vec![(
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::FULL),
        )],
        2,
    )
    .await;
    let rows = async |sql: &str| crate::mocks::rows(session.as_ref(), sql).await;
    let plan = async |sql: &str, analyze: bool| {
        let mode = if analyze {
            "EXPLAIN ANALYZE"
        } else {
            "EXPLAIN"
        };
        rows(&format!("{mode} {sql}")).await.concat().join("\n")
    };
    // The same query over a scan the index can't serve.
    let scanned = async |sql: &str| {
        rows(&sql.replacen(
            "FROM sys_vqueue_entry_status",
            "FROM (SELECT * FROM sys_vqueue_entry_status LIMIT 1000000) AS sys_vqueue_entry_status",
            1,
        ))
        .await
    };
    let queue = rows("SELECT vqueue_id FROM sys_vqueue_entry_status LIMIT 1").await[0][0].clone();
    const INDEX: &str = "_idx_entry_status_by_status_created";
    const STAGES: &str = "_idx_entry_by_queue";
    let select = "SELECT entry_id, status, created_at FROM sys_vqueue_entry_status";
    let newest = "ORDER BY created_at DESC NULLS LAST";
    let indexed = [
        format!(
            "{select} WHERE stage IN ('inbox', 'running', 'suspended', 'paused') {newest} LIMIT 7"
        ),
        // The status is checked on each row, and rejected ones are refilled.
        format!(
            "{select} WHERE stage IN ('inbox', 'paused') AND status = 'backing-off' \
             {newest} LIMIT 5 OFFSET 2"
        ),
        format!("{select} WHERE stage = 'running' {newest} LIMIT 3"),
        // Millisecond bounds keep or leave both logical values of the
        // millisecond at the bound.
        format!(
            "{select} WHERE stage IN ('inbox', 'running') \
             AND created_at < to_timestamp_millis({}) {newest} LIMIT 4",
            BASE + 30
        ),
        format!(
            "{select} WHERE stage IN ('inbox', 'running', 'suspended', 'paused') \
             AND created_at <= to_timestamp_millis({}) {newest} LIMIT 3",
            BASE + 30
        ),
        // Alternatives, each of active stages.
        format!("{select} WHERE stage = 'inbox' OR stage = 'suspended' {newest} LIMIT 6"),
        // Any other conditions are checked on each candidate: another column,
        // arithmetic, and alternatives of several conditions.
        format!("{select} WHERE stage = 'inbox' AND num_attempts = 0 {newest} LIMIT 5"),
        format!("{select} WHERE stage = 'inbox' AND num_attempts + 1 = 1 {newest} LIMIT 5"),
        format!(
            "{select} WHERE (stage = 'inbox' AND status = 'new') OR stage = 'suspended' \
             {newest} LIMIT 6"
        ),
        // The index holds Finished entries too.
        format!("{select} WHERE stage IN ('inbox', 'finished') {newest} LIMIT 5"),
        // Lanes that aren't every combination of their stages and statuses:
        // (inbox, new) and every paused pair. The probe also finds keys of
        // (inbox, backing-off), which no lane reads.
        format!(
            "{select} WHERE stage IN ('inbox', 'paused') AND status IN ('new', 'paused') \
             {newest} LIMIT 5"
        ),
        // A stage and a status that rule each other out leave no lane: the
        // read returns nothing, as the scan does.
        format!("{select} WHERE stage = 'inbox' AND status = 'paused' {newest} LIMIT 5"),
        // A status alone: the only entries the index doesn't hold, those in a
        // stage this binary doesn't know, show no status and match none.
        format!("{select} WHERE status = 'new' {newest} LIMIT 5"),
    ];
    let scans = [
        // Alternatives that together restrict neither column.
        format!("{select} WHERE stage = 'inbox' OR status = 'new' {newest} LIMIT 5"),
        // `unknown` names no stage: the query selects nothing before any read.
        format!("{select} WHERE stage = 'unknown' {newest} LIMIT 5"),
        // The transition index's order.
        format!("{select} WHERE stage = 'inbox' ORDER BY transitioned_at DESC NULLS LAST LIMIT 5"),
    ];
    // Tie-aware: the creation times in order, and every row a qualifying one.
    let same =
        |actual: &[Vec<String>], expected: &[Vec<String>], all: &[Vec<String>], sql: &str| {
            let times =
                |rows: &[Vec<String>]| rows.iter().map(|row| row[2].clone()).collect::<Vec<_>>();
            assert_eq!(times(actual), times(expected), "{sql}");
            assert!(actual.iter().all(|row| all.contains(row)), "{sql}");
            assert_eq!(
                actual.iter().collect::<HashSet<_>>().len(),
                actual.len(),
                "{sql}"
            );
        };
    // The ordered column is never NULL: wherever the query puts NULLs, by
    // default first for DESC, it reads the index in the same plan, but for the
    // spelling, and gets the same rows. Ascending, it reads the index
    // backwards, in one plan for every spelling too.
    let unspelled = |plan: String| plan.replace(" NULLS FIRST", "").replace(" NULLS LAST", "");
    for sql in &indexed {
        let all = scanned(&sql.replace(" LIMIT", " LIMIT 1000000 --")).await;
        for spellings in [SPELLINGS, REVERSED] {
            let first = sql.replace("DESC NULLS LAST", spellings[0]);
            let explained = plan(&first, false).await;
            assert!(explained.contains(INDEX), "{first}: {explained}");
            for spelling in spellings {
                let sql = sql.replace("DESC NULLS LAST", spelling);
                assert_eq!(
                    unspelled(plan(&sql, false).await),
                    unspelled(explained.clone()),
                    "{sql}"
                );
                same(&rows(&sql).await, &scanned(&sql).await, &all, &sql);
            }
        }
    }
    for sql in &scans {
        let explained = plan(sql, false).await;
        assert!(!explained.contains(INDEX), "{sql}: {explained}");
    }
    // A few queues read the stage tables first; without the order, a few
    // active stages read the status index's lanes.
    let sql = format!("{select} WHERE vqueue_id = '{queue}' AND stage = 'inbox' {newest} LIMIT 5");
    let explained = plan(&sql, false).await;
    assert!(
        explained.contains(STAGES) && !explained.contains(INDEX),
        "{sql}: {explained}"
    );
    let sql = format!("{select} WHERE stage IN ('inbox', 'paused') LIMIT 5");
    let explained = plan(&sql, false).await;
    assert!(
        explained.contains("index=_idx_entry_status_by_status_created"),
        "{sql}: {explained}"
    );
    // The read went through the index and skipped nothing.
    let analyzed = plan(&indexed[0], true).await;
    assert!(
        analyzed.contains("entry_index_reads=1") && !analyzed.contains("entry_index_scans=1"),
        "{analyzed}"
    );
    assert!(analyzed.contains("entry_index_skipped=0"), "{analyzed}");
    // Backwards too.
    let analyzed = plan(
        &indexed[0].replace("DESC NULLS LAST", "ASC NULLS LAST"),
        true,
    )
    .await;
    assert!(
        analyzed.contains("entry_index_reads=1") && !analyzed.contains("entry_index_scans=1"),
        "{analyzed}"
    );

    // More runs than one read merges in its first hour, 260 services in one
    // lane: the partition scans and sorts instead, before returning anything,
    // and says why and what discovery examined.
    let mut tx = store.transaction();
    for service in 0..260u64 {
        let created = UniqueTimestamp::from_parts_unchecked(BASE + 1_000 + service, 0);
        let mut remainder = [0; 16];
        remainder[..3].copy_from_slice(&[200, service as u8, (service >> 8) as u8]);
        let key = EntryKey::new(
            false,
            created.to_unix_millis(),
            1_000 + service,
            EntryId::new(EntryKind::Invocation, remainder),
        );
        let name = format!("s{service}");
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &EntryTargetRef::Service {
                    scope: None,
                    service: &name,
                    handler: "h",
                },
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created, key.run_at()),
            },
        );
    }
    tx.commit().await.unwrap();
    drop(tx);
    for sql in &indexed[..2] {
        let all = scanned(&sql.replace(" LIMIT", " LIMIT 1000000 --")).await;
        for spelling in SPELLINGS {
            let sql = sql.replace("DESC NULLS LAST", spelling);
            same(&rows(&sql).await, &scanned(&sql).await, &all, &sql);
        }
    }
    let analyzed = plan(&indexed[0], true).await;
    assert!(
        analyzed.contains("entry_index_scans_too_many_runs=1"),
        "{analyzed}"
    );
    assert!(
        !analyzed.contains("entry_index_keys_examined=0"),
        "{analyzed}"
    );
}

/// `_idx_entry_by_queue`, the scheduler's stage records, holds every column of
/// an entry status but the metadata exactly as the status does, so a query of
/// `sys_vqueue_entry_status` or `sys_vqueues` that restricts stages or queues
/// and needs no metadata column reads the records alone: the same rows as the
/// scan, no status fetched, no value decoded for key columns. With the
/// deployment, the read fetches each record's status and shows its current
/// deployment, as before, even for an entry pinned since its last stage write.
/// Unrestricted queries and entry-ID lookups keep their accesses.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stage_index_covers_both_tables() {
    use restate_limiter::LimitKey;
    use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
    use restate_storage_api::vqueue_table::{ReadVQueueTable, StoredEntryRef};
    use restate_types::LockName;
    use restate_types::vqueues::HandlerRef;

    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    const BASE: u64 = 1_744_040_000_000;
    let at = |seconds: u64| {
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(BASE + seconds * 1_000))
            .unwrap()
    };
    let (a, b) = (VQueueId::custom(7, "a"), VQueueId::custom(7, "b"));
    let target = |kind: EntryKind| EntryTargetRef::VirtualObject {
        scope: None,
        service: "Counter",
        key: "k",
        handler: match kind {
            EntryKind::StateMutation => HandlerRef::StateMutation,
            _ => HandlerRef::UserHandler("add"),
        },
    };
    // Queue, stage, status, lock, run time, kind and deployment of each entry,
    // written as the scheduler writes them: the status and its stage record,
    // with the same tuple, status, metadata and statistics.
    let entries = [
        (
            &a,
            Stage::Inbox,
            Status::New,
            false,
            5,
            EntryKind::Invocation,
            None,
        ),
        (
            &a,
            Stage::Inbox,
            Status::BackingOff,
            false,
            1,
            EntryKind::Invocation,
            Some("dp_a"),
        ),
        (
            &a,
            Stage::Inbox,
            Status::New,
            false,
            3,
            EntryKind::StateMutation,
            None,
        ),
        (
            &a,
            Stage::Running,
            Status::Started,
            true,
            0,
            EntryKind::Invocation,
            None,
        ),
        (
            &a,
            Stage::Suspended,
            Status::Started,
            true,
            0,
            EntryKind::Invocation,
            Some("dp_a"),
        ),
        (
            &b,
            Stage::Paused,
            Status::BackingOff,
            false,
            0,
            EntryKind::Invocation,
            Some("dp_b"),
        ),
        (
            &b,
            Stage::Finished,
            Status::Succeeded,
            false,
            0,
            EntryKind::Invocation,
            Some("dp_a"),
        ),
        (
            &b,
            Stage::Finished,
            Status::Failed,
            false,
            0,
            EntryKind::Invocation,
            Some("dp_b"),
        ),
        (
            &b,
            Stage::Running,
            Status::Started,
            true,
            0,
            EntryKind::Invocation,
            Some("dp_b"),
        ),
    ];
    let mut tx = store.transaction();
    for (qid, lock) in [(&a, "Counter/k"), (&b, "Counter/j")] {
        tx.create_vqueue(
            qid,
            &VQueueMeta::new(
                at(0),
                None,
                LimitKey::None,
                VQueueLink::Lock(LockName::parse(lock).unwrap()),
            ),
        );
    }
    let mut ids = Vec::new();
    for (n, &(qid, stage, status, has_lock, run_at, kind, deployment)) in entries.iter().enumerate()
    {
        let n = n as u64;
        let key = EntryKey::new(
            has_lock,
            at(run_at).to_unix_millis(),
            n + 1,
            EntryId::new(kind, [n as u8 + 1; 16]),
        );
        let mut stats = EntryStatistics::new(at(n), key.run_at());
        stats.transitioned_at = at(20 + n);
        stats.num_attempts = n as u32;
        stats.num_errors = (n % 2) as u32;
        let metadata = EntryMetadata {
            deployment: deployment.map(|deployment| deployment.to_restring()),
            ..Default::default()
        };
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid,
                target: &target(kind),
            },
            EntryStateRef {
                stage,
                status,
                entry_key: &key,
                metadata: &metadata,
                stats: &stats,
            },
        );
        tx.put_vqueue_inbox(
            qid,
            stage,
            &key,
            &EntryValue {
                status,
                metadata,
                stats,
            },
        );
        ids.push((
            key.entry_id().to_base_id(7),
            key.entry_id().display(7).to_string(),
        ));
    }
    tx.commit().await.unwrap();
    drop(tx);
    // The running entry of `a` is pinned to a deployment, as the processor
    // pins one: its status only.
    let (pinned_id, pinned) = ids[3].clone();
    let mut tx = store.transaction();
    let header = tx
        .get_vqueue_entry_status(&pinned_id)
        .await
        .unwrap()
        .unwrap();
    tx.update_vqueue_entry_status(
        &EntryContext {
            qid: &a,
            target: &target(EntryKind::Invocation),
        },
        StoredEntryRef::from_header(&header),
        EntryStateRef {
            metadata: &EntryMetadata {
                deployment: Some("dp_new".to_restring()),
                ..Default::default()
            },
            ..EntryStateRef::from_header(&header)
        },
    );
    tx.commit().await.unwrap();
    drop(tx);

    let session = crate::mocks::v2_session(
        &manager,
        vec![(
            PartitionId::MIN,
            Partition::new(PartitionId::MIN, KeyRange::FULL),
        )],
        2,
    )
    .await;
    let rows = async |sql: &str| crate::mocks::rows(session.as_ref(), sql).await;
    let sorted = async |sql: &str| {
        let mut rows = rows(sql).await;
        rows.sort();
        rows
    };
    // The same query over a scan of every status: the view and the table over a
    // subquery that no index serves.
    let view = include_str!("../../catalog/vqueues.sql")
        .split_once(" AS SELECT")
        .unwrap()
        .1
        .replace(
            "restate.public.sys_vqueue_entry_status AS es",
            "(SELECT * FROM restate.public.sys_vqueue_entry_status LIMIT 1000000) AS es",
        );
    let scanned = |sql: &str| {
        format!(
            "WITH sys_vqueues AS (SELECT{view}), \
             statuses AS (SELECT * FROM sys_vqueue_entry_status LIMIT 1000000) {}",
            sql.replace("FROM sys_vqueue_entry_status", "FROM statuses")
        )
    };
    let analyze = async |sql: &str| {
        rows(&format!("EXPLAIN ANALYZE {sql}"))
            .await
            .concat()
            .join("\n")
    };

    // Covered: the stage records alone.
    for (sql, decoded) in [
        (
            "SELECT entry_id, status FROM sys_vqueue_entry_status WHERE stage = 'finished'".to_owned(),
            2,
        ),
        (
            "SELECT entry_id, num_attempts, num_errors FROM sys_vqueue_entry_status \
             WHERE stage IN ('running', 'suspended')"
                .to_owned(),
            3,
        ),
        (
            format!("SELECT stage, count(*) FROM sys_vqueue_entry_status WHERE vqueue_id = '{a}' GROUP BY stage"),
            0,
        ),
        (
            "SELECT v.entry_id AS id FROM sys_vqueues v WHERE v.stage = 'finished' \
             AND (v.stage = 'finished' AND v.status IN ('succeeded', 'failed', 'cancelled', 'killed')) \
             ORDER BY v.created_at DESC NULLS LAST LIMIT 250"
                .to_owned(),
            2,
        ),
        (
            "SELECT id, COUNT(*) AS workload FROM sys_vqueues \
             WHERE stage IN ('inbox', 'running', 'suspended', 'paused') \
             GROUP BY id ORDER BY workload DESC LIMIT 251"
                .to_owned(),
            0,
        ),
        (
            "SELECT MIN(v.transitioned_at) AS oldest_inboxed_at FROM sys_vqueues v \
             WHERE v.id IN (SELECT vm.id FROM sys_vqueue_meta vm \
             WHERE vm.service_name = 'Counter' AND vm.lock_name = 'Counter/k' AND vm.scope IS NULL) \
             AND v.stage = 'inbox'"
                .to_owned(),
            3,
        ),
        (
            format!(
                "SELECT id AS vqueue_id, entry_id AS id, entry_kind AS kind, stage, status, has_lock, \
                 run_at, sequence_number, created_at, transitioned_at \
                 FROM sys_vqueues WHERE id = '{a}' AND stage = 'inbox' LIMIT 26"
            ),
            3,
        ),
    ] {
        let expected = sorted(&scanned(&sql)).await;
        assert!(!expected.is_empty(), "{sql}");
        assert_eq!(sorted(&sql).await, expected, "{sql}");
        let plan = analyze(&sql).await;
        assert!(
            plan.contains("index=_idx_entry_by_queue")
                && plan.contains(&format!("stage_values_decoded={decoded}"))
                && !plan.contains("primary_keys_fetched"),
            "covered: {sql}: {plan}"
        );
    }

    // With the deployment: each record's status, its current deployment.
    for (sql, expected) in [
        (
            format!(
                "SELECT entry_id, deployment FROM sys_vqueues WHERE id = '{a}' AND entry_id = '{pinned}'"
            ),
            "dp_new",
        ),
        (
            format!(
                "SELECT entry_id, deployment FROM sys_vqueue_entry_status \
                 WHERE vqueue_id = '{a}' AND stage = 'running'"
            ),
            "dp_new",
        ),
    ] {
        assert_eq!(
            rows(&sql).await,
            [[pinned.clone(), expected.to_owned()]],
            "{sql}"
        );
    }
    let sql = format!("SELECT entry_id, deployment FROM sys_vqueues WHERE id = '{a}'");
    assert_eq!(sorted(&sql).await, sorted(&scanned(&sql)).await);
    let plan = analyze(&sql).await;
    assert!(
        plan.contains("index=_idx_entry_by_queue")
            && plan.contains("primary_keys_fetched=5")
            && !plan.contains("stage_keys_visited"),
        "fetched: {plan}"
    );

    // Unrestricted, and entry IDs: the scan and the lookup, as before.
    for (sql, access) in [
        (
            "SELECT entry_id, status FROM sys_vqueue_entry_status".to_owned(),
            "TableScanExec",
        ),
        (
            format!("SELECT status FROM sys_vqueues WHERE entry_id = '{pinned}'"),
            "MultiGetExec",
        ),
    ] {
        assert_eq!(sorted(&sql).await, sorted(&scanned(&sql)).await, "{sql}");
        let plan = analyze(&sql).await;
        assert!(
            plan.contains(access) && !plan.contains("IndexScanExec"),
            "{sql}: {plan}"
        );
    }
    assert_eq!(
        sorted("SELECT * FROM sys_vqueues").await,
        sorted(&scanned("SELECT * FROM sys_vqueues")).await
    );

    // A read that fetches statuses first rejects the records that fail the
    // conditions their values answer; a condition on the deployment is checked
    // on the fetched status only, and qualifies the rows after. Conditions on
    // the key alone install no check: navigation applies them already.
    for (sql, work) in [
        (
            format!(
                "SELECT entry_id, deployment FROM sys_vqueues WHERE id = '{a}' AND status = 'new'"
            ),
            &[
                "index_keys_visited=5",
                "index_keys_rejected=3",
                "primary_keys_fetched=2",
            ][..],
        ),
        (
            "SELECT entry_id FROM sys_vqueue_entry_status \
             WHERE deployment = 'dp_new' AND stage = 'running'"
                .to_owned(),
            &["index_keys_visited=2", "primary_keys_fetched=2"],
        ),
        (
            "SELECT entry_id FROM sys_vqueue_entry_status \
             WHERE stage IN ('inbox', 'running') AND (status = 'new' OR deployment = 'dp_new')"
                .to_owned(),
            &["index_keys_visited=5", "primary_keys_fetched=5"],
        ),
    ] {
        let expected = sorted(&scanned(&sql)).await;
        assert!(!expected.is_empty(), "{sql}");
        assert_eq!(sorted(&sql).await, expected, "{sql}");
        let plan = analyze(&sql).await;
        let rejects = work
            .iter()
            .any(|metric| metric.starts_with("index_keys_rejected"));
        assert!(
            work.iter().all(|metric| plan.contains(metric))
                && plan.contains("index_keys_rejected=") == rejects,
            "{sql}: {plan}"
        );
    }
    assert_eq!(
        rows("SELECT entry_id FROM sys_vqueue_entry_status WHERE deployment = 'dp_new' AND stage = 'running'")
            .await,
        [[pinned.clone()]]
    );

    // A condition that can fail installs no check, whatever the others say: a
    // record a status condition would reject must still reach the division, as
    // it does when every status is fetched. The physical order is fixed here:
    // the division first.
    let schema = VqueueEntryStatus::arrow_schema();
    let df_schema = DFSchema::try_from(schema.as_ref().clone()).unwrap();
    let physical = |expr| {
        SessionContext::new()
            .create_physical_expr(expr, &df_schema)
            .unwrap()
    };
    let failed = || col("status").eq(lit(ScalarValue::LargeUtf8(Some("failed".to_owned()))));
    let dividing = physical(
        (col("num_attempts") / col("num_errors"))
            .gt(lit(0u32))
            .and(failed()),
    );
    let comparing = physical(col("num_attempts").gt(lit(0u32)).and(failed()));
    // Conditions on the key alone: navigation applies them.
    let keyed = physical(
        col("stage")
            .eq(lit(ScalarValue::LargeUtf8(Some("running".to_owned()))))
            .and(col("has_lock").eq(lit(true))),
    );
    let prefilters = |predicate| {
        super::read::covered_prefilter(Some(predicate), &schema)
            .unwrap()
            .is_some()
    };
    assert_eq!(
        [
            prefilters(&dividing),
            prefilters(&comparing),
            prefilters(&keyed)
        ],
        [false, true, false],
        "only_safe_value_conditions_prefilter"
    );
    let outcome = async |sql: &str| {
        let result = session
            .execute(sql, Default::default())
            .await
            .map_err(|error| error.to_string())?;
        let batches: Vec<RecordBatch> = result
            .stream
            .try_collect()
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(batches.iter().map(RecordBatch::num_rows).sum::<usize>())
    };
    let sql = format!(
        "SELECT entry_id, deployment FROM sys_vqueues \
         WHERE id = '{a}' AND num_attempts / num_errors > 0 AND status = 'backing-off'"
    );
    let (read, scan) = (outcome(&sql).await, outcome(&scanned(&sql)).await);
    assert!(
        [&read, &scan].iter().all(|outcome| outcome
            .as_ref()
            .is_err_and(|error| error.contains("Divide by zero"))),
        "{read:?} {scan:?}"
    );
}
