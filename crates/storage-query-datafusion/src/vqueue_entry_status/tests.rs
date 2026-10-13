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

use datafusion::arrow::array::{
    BooleanArray, DurationMillisecondArray, LargeStringArray, TimestampMillisecondArray,
    UInt32Array, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::all;
use googletest::prelude::{assert_that, eq};

use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::{
    EntryId, EntryKey, EntryKind, EntryMetadata, Stage, Status, WriteVQueueTable,
    stats::{EntryStatistics, WaitStats},
};
use restate_types::clock::UniqueTimestamp;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::{Seq, VQueueId};
use restate_util_string::ToReString;

use crate::mocks::*;
use crate::row;

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
    let mut canonical_ids = Vec::new();
    let mut tx = engine.partition_store().transaction();
    for i in 1..=6u8 {
        let entry_id = EntryId::new(
            if i % 2 == 0 {
                EntryKind::Invocation
            } else {
                EntryKind::StateMutation
            },
            [i; 16],
        );
        let key = EntryKey::new(
            false,
            MillisSinceEpoch::new(1_744_010_000_000),
            i as u64,
            entry_id,
        );
        let stats = EntryStatistics::new(created_at, key.run_at());
        tx.put_vqueue_entry_status(
            &qid,
            Stage::Running,
            &key,
            &metadata,
            stats,
            Status::Started,
        );
        ids.push(key.entry_id().display(qid.partition_key()).to_string());
        canonical_ids.push(key.to_canonical_entry_id(qid.partition_key()));
    }
    tx.commit().await.unwrap();
    drop(tx);

    let canonical_strings = canonical_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    for (column, filter_ids) in [("entry_id", &ids), ("canonical_id", &canonical_strings)] {
        let listed_ids = filter_ids[0..4]
            .iter()
            .map(|id| format!("'{id}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let wrong_sequence = canonical_ids[0].with_seq(Seq::MAX);
        for (predicate, expected_ids) in [
            (format!("{column} NOT IN ({listed_ids})"), &ids[4..]),
            (format!("{column} IN ({listed_ids})"), &ids[..4]),
            (format!("{column} = '{}'", filter_ids[0]), &ids[..1]),
            (
                format!(
                    "{column} = '{}' AND canonical_id = '{wrong_sequence}'",
                    filter_ids[0]
                ),
                &ids[..0],
            ),
        ] {
            let batches = engine
                .execute(format!(
                    "SELECT entry_id FROM sys_vqueue_entry_status WHERE {predicate}"
                ))
                .await
                .unwrap()
                .stream
                .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
                .await;
            let mut got = Vec::new();
            for records in batches {
                let records = records.unwrap();
                got.extend(
                    records
                        .column_by_name("entry_id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap()
                        .iter()
                        .flatten()
                        .map(str::to_string),
                );
            }
            got.sort();
            let mut expected = expected_ids.to_vec();
            expected.sort();
            assert_eq!(got, expected, "{predicate}");
        }
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
    tx.put_vqueue_entry_status(
        &qid,
        Stage::Running,
        &key,
        &metadata,
        stats,
        Status::Started,
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
