// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mocks::*;
use crate::row;

use datafusion::arrow::array::{LargeStringArray, TimestampMillisecondArray, UInt32Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::all;
use googletest::prelude::{assert_that, eq};

use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::{
    EntryId, EntryKey, EntryKind, EntryMetadata, EntryValue, Stage, Status, WriteVQueueTable,
    stats::EntryStatistics,
};
use restate_types::clock::UniqueTimestamp;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_util_string::ToReString;

async fn select_entry_ids(engine: &mut MockQueryEngine, query: &str) -> Vec<String> {
    let records = engine
        .execute(query.to_owned())
        .await
        .unwrap()
        .stream
        .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
        .await
        .remove(0)
        .unwrap();

    let mut ids = records
        .column_by_name("entry_id")
        .unwrap()
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap()
        .iter()
        .flatten()
        .map(str::to_string)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_vqueue_entry_value_fields() {
    let mut engine = MockQueryEngine::create().await;

    let qid = VQueueId::custom(1337, "df-vqueue");
    let key = EntryKey::new(
        true,
        MillisSinceEpoch::new(1_744_000_001_000),
        7,
        EntryId::new(EntryKind::Invocation, [7; 16]),
    );

    let created_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_000_000_100)).unwrap();
    let transitioned_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_000_000_300)).unwrap();
    let first_attempt_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_000_000_400)).unwrap();
    let latest_attempt_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_000_000_500)).unwrap();

    let mut stats = EntryStatistics::new(created_at, key.run_at().to_owned());
    stats.transitioned_at = transitioned_at;
    stats.num_attempts = 3;
    stats.num_paused = 2;
    stats.num_suspensions = 4;
    stats.num_yields = 5;
    stats.first_attempt_at = Some(first_attempt_at);
    stats.latest_attempt_at = Some(latest_attempt_at);

    let value = EntryValue {
        status: Status::BackingOff,
        metadata: EntryMetadata {
            deployment: Some("dp_123".to_restring()),
            ..Default::default()
        },
        stats,
    };

    // This row should be returned.
    let mut tx = engine.partition_store().transaction();
    tx.put_vqueue_inbox(&qid, Stage::Inbox, &key, &value);

    // This row should only be returned when stage filtering selects it.
    tx.put_vqueue_inbox(&qid, Stage::Running, &key, &value);
    tx.commit().await.unwrap();
    drop(tx);

    let records = engine
        .execute(
            "SELECT stage, status, num_attempts, num_pauses, num_suspensions, num_yields, \
            created_at, transitioned_at, first_attempt_at, latest_attempt_at, first_runnable_at, \
            run_at, deployment FROM sys_vqueues WHERE stage = 'inbox' ORDER BY sequence_number",
        )
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
                "stage" => LargeStringArray: eq("inbox"),
                "status" => LargeStringArray: eq("backing-off"),
                "num_attempts" => UInt32Array: eq(3),
                "num_pauses" => UInt32Array: eq(2),
                "num_suspensions" => UInt32Array: eq(4),
                "num_yields" => UInt32Array: eq(5),
                "created_at" => TimestampMillisecondArray: eq(created_at.to_unix_millis().as_u64() as i64),
                "transitioned_at" => TimestampMillisecondArray: eq(transitioned_at.to_unix_millis().as_u64() as i64),
                "first_attempt_at" => TimestampMillisecondArray: eq(first_attempt_at.to_unix_millis().as_u64() as i64),
                "latest_attempt_at" => TimestampMillisecondArray: eq(latest_attempt_at.to_unix_millis().as_u64() as i64),
                "first_runnable_at" => TimestampMillisecondArray: eq(value.stats.first_runnable_at.as_u64() as i64),
                "run_at" => TimestampMillisecondArray: eq(key.run_at().as_unix_millis().as_u64() as i64),
                "deployment" => LargeStringArray: eq("dp_123"),
            }
        ))
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn vqueue_stage_filter_and_unfiltered_scan() {
    let mut engine = MockQueryEngine::create().await;

    let qid = VQueueId::custom(2337, "df-vqueue-stages");

    let mut tx = engine.partition_store().transaction();
    let stages = [
        Stage::Inbox,
        Stage::Running,
        Stage::Suspended,
        Stage::Paused,
        Stage::Finished,
    ];
    for (index, stage) in stages.into_iter().enumerate() {
        let key = EntryKey::new(
            false,
            MillisSinceEpoch::new(1_744_001_000_000 + index as u64),
            (index + 1) as u64,
            EntryId::new(EntryKind::Invocation, [index as u8 + 1; 16]),
        );
        let value = EntryValue {
            status: Status::Started,
            metadata: EntryMetadata::default(),
            stats: EntryStatistics::new(
                UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(
                    1_744_001_000_100 + index as u64,
                ))
                .unwrap(),
                key.run_at().to_owned(),
            ),
        };
        tx.put_vqueue_inbox(&qid, stage, &key, &value);
    }
    tx.commit().await.unwrap();
    drop(tx);

    let all_stages = engine
        .execute("SELECT stage FROM sys_vqueues ORDER BY stage")
        .await
        .unwrap()
        .stream
        .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
        .await
        .remove(0)
        .unwrap();

    assert_eq!(all_stages.num_rows(), 5);
    assert_that!(
        all_stages,
        all!(
            row!(0, { "stage" => LargeStringArray: eq("finished") }),
            row!(1, { "stage" => LargeStringArray: eq("inbox") }),
            row!(2, { "stage" => LargeStringArray: eq("paused") }),
            row!(3, { "stage" => LargeStringArray: eq("running") }),
            row!(4, { "stage" => LargeStringArray: eq("suspended") })
        )
    );

    let filtered_stages = engine
        .execute(
            "SELECT stage FROM sys_vqueues WHERE stage IN ('running', 'paused') ORDER BY stage",
        )
        .await
        .unwrap()
        .stream
        .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
        .await
        .remove(0)
        .unwrap();

    assert_eq!(filtered_stages.num_rows(), 2);
    assert_that!(
        filtered_stages,
        all!(
            row!(0, { "stage" => LargeStringArray: eq("paused") }),
            row!(1, { "stage" => LargeStringArray: eq("running") })
        )
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn vqueue_entry_id_point_query_and_not_in_fallback() {
    let mut engine = MockQueryEngine::create().await;
    let qid = VQueueId::custom(3337, "df-vqueue-point-lookup");
    let mut keys = Vec::new();

    let mut tx = engine.partition_store().transaction();
    for index in 0..6u8 {
        let key = EntryKey::new(
            false,
            MillisSinceEpoch::new(1_744_002_000_000 + u64::from(index)),
            u64::from(index),
            EntryId::new(EntryKind::Invocation, [index + 1; 16]),
        );
        let stage = if index == 2 {
            Stage::Paused
        } else {
            Stage::Inbox
        };
        let stats = EntryStatistics::new(
            UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(
                1_744_002_000_100 + u64::from(index),
            ))
            .unwrap(),
            key.run_at(),
        );
        let value = EntryValue {
            status: Status::Started,
            metadata: EntryMetadata::default(),
            stats,
        };

        if index < 4 {
            // Point lookups must use the status index; these rows are absent
            // from the stage table so a stage scan cannot satisfy the query.
            tx.put_vqueue_entry_status(
                &qid,
                stage,
                &key,
                &value.metadata,
                value.stats,
                value.status,
            );
        } else {
            // NOT IN must retain the stage-scan path; these rows are absent
            // from the status index so an exact lookup cannot return them.
            tx.put_vqueue_inbox(&qid, stage, &key, &value);
        }
        keys.push(key);
    }
    tx.commit().await.unwrap();
    drop(tx);

    let entry_ids = keys
        .iter()
        .map(|key| key.entry_id().display(qid.partition_key()).to_string())
        .collect::<Vec<_>>();
    let got = select_entry_ids(
        &mut engine,
        &format!(
            "SELECT entry_id FROM sys_vqueues WHERE entry_id IN ('{}', '{}', '{}') AND stage = 'inbox'",
            entry_ids[0], entry_ids[1], entry_ids[2]
        ),
    )
    .await;

    let mut expected = vec![entry_ids[0].clone(), entry_ids[1].clone()];
    expected.sort();
    assert_eq!(got, expected);

    let excluded = entry_ids[0..4]
        .iter()
        .map(|entry_id| format!("'{entry_id}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let got = select_entry_ids(
        &mut engine,
        &format!("SELECT entry_id FROM sys_vqueues WHERE entry_id NOT IN ({excluded})"),
    )
    .await;

    let mut expected = vec![entry_ids[4].clone(), entry_ids[5].clone()];
    expected.sort();
    assert_eq!(got, expected);
}
