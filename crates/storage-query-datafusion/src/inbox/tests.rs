// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
use restate_storage_api::Transaction;
use restate_storage_api::inbox_table::{InboxEntry, WriteInboxTable};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationTarget;

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_inbox() {
    let mut engine = MockQueryEngine::create().await;

    let mut tx = engine.partition_store().transaction();
    let invocation_target = InvocationTarget::mock_virtual_object();
    let service_id = invocation_target.as_keyed_service_id().unwrap();
    let invocation_id_1 = InvocationId::mock_generate(&invocation_target);
    tx.put_inbox_entry(
        0,
        &InboxEntry::Invocation(service_id.clone(), invocation_id_1),
    )
    .unwrap();
    let invocation_id_2 = InvocationId::mock_generate(&invocation_target);
    tx.put_inbox_entry(
        1,
        &InboxEntry::Invocation(service_id.clone(), invocation_id_2),
    )
    .unwrap();
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
