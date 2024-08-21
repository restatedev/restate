// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
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
use datafusion::arrow::array::{LargeStringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::all;
use googletest::prelude::{assert_that, eq};
use restate_core::TaskCenterBuilder;
use restate_storage_api::inbox_table::{InboxEntry, InboxTable};
use restate_storage_api::Transaction;
use restate_types::identifiers::{InvocationId, InvocationUuid, ServiceId, WithPartitionKey};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_inbox() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut engine = tc
        .run_in_scope("mock-query-engine", None, MockQueryEngine::create())
        .await;

    let mut tx = engine.partition_store().transaction();
    let service_id = ServiceId::mock_random();
    let invocation_id_1 =
        InvocationId::from_parts(service_id.partition_key(), InvocationUuid::new());
    tx.put_inbox_entry(
        0,
        &InboxEntry::Invocation(service_id.clone(), invocation_id_1),
    )
    .await;
    let invocation_id_2 =
        InvocationId::from_parts(service_id.partition_key(), InvocationUuid::new());
    tx.put_inbox_entry(
        1,
        &InboxEntry::Invocation(service_id.clone(), invocation_id_2),
    )
    .await;
    tx.commit().await.unwrap();

    let records = engine
        .execute("SELECT * FROM sys_inbox ORDER BY sequence_number")
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
                    "id" => LargeStringArray: eq(invocation_id_1.to_string()),
                    "sequence_number" => UInt64Array: eq(0),
                    "service_name" => LargeStringArray: eq(service_id.service_name.to_string()),
                    "service_key" => LargeStringArray: eq(service_id.key.to_string()),
                }
            ),
            row!(
                1,
                {
                    "id" => LargeStringArray: eq(invocation_id_2.to_string()),
                    "sequence_number" => UInt64Array: eq(1),
                    "service_name" => LargeStringArray: eq(service_id.service_name.to_string()),
                    "service_key" => LargeStringArray: eq(service_id.key.to_string()),
                }
            )
        )
    );
}
