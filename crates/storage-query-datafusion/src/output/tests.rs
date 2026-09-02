// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use datafusion::arrow::array::{LargeBinaryArray, LargeStringArray, UInt32Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::all;
use googletest::prelude::{assert_that, eq};

use restate_storage_api::Transaction;
use restate_storage_api::output_table::WriteOutputTable;
use restate_types::errors::{InvocationError, codes};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::ResponseResult;

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
async fn get_outputs() {
    let mut engine = MockQueryEngine::create().await;

    let success_id = InvocationId::mock_random();
    let failure_id = InvocationId::mock_random();
    let failure = InvocationError::new(codes::INTERNAL, "my-error");

    {
        let mut tx = engine.partition_store().transaction();
        tx.put_output(
            &success_id,
            &ResponseResult::Success(Bytes::from_static(b"{\"greeting\":\"hi\"}")),
        )
        .unwrap();
        tx.put_output(&failure_id, &ResponseResult::Failure(failure.clone()))
            .unwrap();
        tx.commit().await.unwrap();
    }

    // 'failure' sorts before 'success', so the row order is deterministic.
    let records = query(
        &engine,
        "SELECT id, result, output, output_utf8, failure_code, failure_json          FROM sys_invocation_output ORDER BY result",
    )
    .await;

    assert_that!(
        records,
        all!(
            row!(
                0,
                {
                    "id" => LargeStringArray: eq(failure_id.to_string()),
                    "result" => LargeStringArray: eq("failure"),
                    "failure_code" => UInt32Array: eq(u16::from(codes::INTERNAL) as u32),
                    "failure_json" => LargeStringArray: eq(serde_json::to_string(&failure).unwrap()),
                }
            ),
            row!(
                1,
                {
                    "id" => LargeStringArray: eq(success_id.to_string()),
                    "result" => LargeStringArray: eq("success"),
                    "output" => LargeBinaryArray: eq(b"{\"greeting\":\"hi\"}".to_vec()),
                    "output_utf8" => LargeStringArray: eq("{\"greeting\":\"hi\"}"),
                }
            ),
        )
    );

    // Exercises the invocation id pushdown, which scans a narrow key range instead of
    // the whole partition.
    let records = query(
        &engine,
        &format!("SELECT id, result FROM sys_invocation_output WHERE id = '{success_id}'"),
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
}
