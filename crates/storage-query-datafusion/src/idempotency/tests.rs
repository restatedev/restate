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
use bytestring::ByteString;
use datafusion::arrow::array::LargeStringArray;
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::all;
use googletest::prelude::{assert_that, eq};
use restate_storage_api::Transaction;
use restate_storage_api::idempotency_table::{IdempotencyMetadata, IdempotencyTable};
use restate_types::identifiers::{IdempotencyId, InvocationId};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_idempotency_key() {
    let mut engine = MockQueryEngine::create().await;

    let mut tx = engine.partition_store().transaction();
    let invocation_id_1 = InvocationId::mock_random();
    tx.put_idempotency_metadata(
        &IdempotencyId::new(
            "my-service".into(),
            Some(ByteString::from_static("my-key")),
            "my-handler".into(),
            "my-idempotency-key".into(),
        ),
        &IdempotencyMetadata {
            invocation_id: invocation_id_1,
        },
    )
    .await
    .unwrap();
    let invocation_id_2 = InvocationId::mock_random();
    tx.put_idempotency_metadata(
        &IdempotencyId::new(
            "my-service".into(),
            Some(ByteString::from_static("my-key")),
            "my-handler-2".into(),
            "my-idempotency-key".into(),
        ),
        &IdempotencyMetadata {
            invocation_id: invocation_id_2,
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let records = engine
        .execute(
            "SELECT * FROM sys_idempotency ORDER BY service_name, service_key, service_handler",
        )
        .await
        .unwrap()
        .collect::<Vec<Result<RecordBatch, _>>>()
        .await
        .remove(0)
        .unwrap();

    assert_that!(
        records,
        all!(
            row!(
                0,
                {
                    "service_name" => LargeStringArray: eq("my-service"),
                    "service_key" => LargeStringArray: eq("my-key"),
                    "service_handler" => LargeStringArray: eq("my-handler"),
                    "idempotency_key" => LargeStringArray: eq("my-idempotency-key"),
                    "invocation_id" => LargeStringArray: eq(invocation_id_1.to_string()),
                }
            ),
            row!(
                1,
                {
                    "service_name" => LargeStringArray: eq("my-service"),
                    "service_key" => LargeStringArray: eq("my-key"),
                    "service_handler" => LargeStringArray: eq("my-handler-2"),
                    "idempotency_key" => LargeStringArray: eq("my-idempotency-key"),
                    "invocation_id" => LargeStringArray: eq(invocation_id_2.to_string()),
                }
            )
        )
    );
}
