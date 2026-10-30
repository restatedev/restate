// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;

use bytes::Bytes;
use datafusion::arrow::array::{
    Array, LargeBinaryArray, LargeStringArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::all;
use googletest::prelude::{assert_that, eq};

use restate_storage_api::Transaction;
use restate_storage_api::blob_table::{BlobBuilder, WriteBlobTable};
use restate_types::blob::{BlobId, BlobKind};
use restate_types::clock::UniqueTimestamp;
use restate_types::errors::{InvocationError, codes};
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::ResponseResult;
use restate_types::time::MillisSinceEpoch;

use crate::mocks::*;
use crate::row;

async fn query(engine: &MockQueryEngine, sql: &str) -> RecordBatch {
    engine
        .execute(sql)
        .await
        .unwrap()
        .stream
        .collect::<Vec<datafusion::common::Result<RecordBatch>>>()
        .await
        .remove(0)
        .unwrap()
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_blobs() {
    let mut engine = MockQueryEngine::create().await;

    let invocation_id = InvocationId::mock_random();
    // Each blob is created one second apart, which the created_at range query relies on.
    let now = MillisSinceEpoch::now().as_u64();
    let ts = |offset_ms: u64| {
        UniqueTimestamp::from_unix_millis_unchecked(MillisSinceEpoch::new(now + offset_ms))
    };
    let created_at = |id: BlobId| id.timestamp().to_unix_millis().as_u64() as i64;
    let success_id = BlobId::generate_output_blob_id(ts(0), 1, &invocation_id);
    let failure_id = BlobId::generate_output_blob_id(ts(1000), 2, &invocation_id);
    let state_id = BlobId::new(ts(2000), 3, invocation_id.partition_key(), [7; 16]);
    let failure = InvocationError::new(codes::INTERNAL, "my-error");
    let output = b"{\"greeting\":\"hi\"}";

    {
        let mut tx = engine.partition_store().transaction();
        let output_blob = || BlobBuilder::default().with_kind(BlobKind::Output);
        tx.put_blob(
            success_id,
            output_blob()
                .with_ref_count(NonZeroU16::new(2).unwrap())
                .build(&ResponseResult::Success(Bytes::from_static(output))),
        )
        .unwrap();
        tx.put_blob(
            failure_id,
            output_blob().build(&ResponseResult::Failure(failure.clone())),
        )
        .unwrap();
        tx.put_blob(
            state_id,
            BlobBuilder::default()
                .with_kind(BlobKind::State)
                .build(&b"state"[..]),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    // 'output' sorts before 'state' and 'failure' before 'success', so the row order is deterministic.
    let records = query(
        &engine,
        "SELECT id, created_at, kind, ref_count, payload_length, result, output, output_utf8, failure_code, failure_json \
         FROM sys_blob ORDER BY kind, result",
    )
    .await;

    assert_eq!(3, records.num_rows());
    assert_that!(
        records,
        all!(
            row!(
                0,
                {
                    "id" => LargeStringArray: eq(failure_id.to_string()),
                    "created_at" => TimestampMillisecondArray: eq(created_at(failure_id)),
                    "kind" => LargeStringArray: eq("output"),
                    "ref_count" => UInt32Array: eq(1),
                    "result" => LargeStringArray: eq("failure"),
                    "failure_code" => UInt32Array: eq(u16::from(codes::INTERNAL) as u32),
                    "failure_json" => LargeStringArray: eq(serde_json::to_string(&failure).unwrap()),
                }
            ),
            row!(
                1,
                {
                    "id" => LargeStringArray: eq(success_id.to_string()),
                    "created_at" => TimestampMillisecondArray: eq(created_at(success_id)),
                    "kind" => LargeStringArray: eq("output"),
                    "ref_count" => UInt32Array: eq(2),
                    "result" => LargeStringArray: eq("success"),
                    "output" => LargeBinaryArray: eq(output.to_vec()),
                    "output_utf8" => LargeStringArray: eq("{\"greeting\":\"hi\"}"),
                }
            ),
            row!(
                2,
                {
                    "id" => LargeStringArray: eq(state_id.to_string()),
                    "created_at" => TimestampMillisecondArray: eq(created_at(state_id)),
                    "kind" => LargeStringArray: eq("state"),
                    "ref_count" => UInt32Array: eq(1),
                    "payload_length" => UInt64Array: eq(5),
                }
            ),
        )
    );
    // output columns are only decoded for output blobs
    for column in [
        "result",
        "output",
        "output_utf8",
        "failure_code",
        "failure_json",
    ] {
        assert!(records.column_by_name(column).unwrap().is_null(2));
    }

    // Exercises the blob id pushdown, which multi-gets the requested ids instead of
    // scanning the whole partition. Ids without a stored blob are skipped.
    let missing_id = BlobId::generate_output_blob_id(ts(0), 4, &invocation_id);
    let records = query(
        &engine,
        &format!("SELECT id, result FROM sys_blob WHERE id IN ('{success_id}', '{missing_id}')"),
    )
    .await;

    assert_eq!(1, records.num_rows());
    assert_that!(
        records,
        row!(
            0,
            {
                "id" => LargeStringArray: eq(success_id.to_string()),
                "result" => LargeStringArray: eq("success"),
            }
        )
    );

    // created_at bounds the scan to the blobs created within the range.
    let records = query(
        &engine,
        &format!(
            "SELECT id FROM sys_blob WHERE created_at >= to_timestamp_millis({}) AND created_at < to_timestamp_millis({})",
            created_at(failure_id),
            created_at(state_id),
        ),
    )
    .await;

    assert_eq!(1, records.num_rows());
    assert_that!(
        records,
        row!(0, { "id" => LargeStringArray: eq(failure_id.to_string()) })
    );

    // Partition keys of the same partition are grouped into a single scan, so no row is
    // returned twice.
    let partition_key = invocation_id.partition_key();
    let records = query(
        &engine,
        &format!(
            "SELECT id FROM sys_blob WHERE partition_key IN ({partition_key}, {})",
            partition_key.wrapping_add(1)
        ),
    )
    .await;
    assert_eq!(3, records.num_rows());
}
