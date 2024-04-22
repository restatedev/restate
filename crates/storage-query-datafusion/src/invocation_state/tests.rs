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
use restate_invoker_api::status_handle::mocks::MockStatusHandle;
use restate_invoker_api::status_handle::InvocationStatusReportInner;
use restate_invoker_api::{InvocationErrorReport, InvocationStatusReport};
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
};
use restate_storage_api::Transaction;
use restate_types::errors::InvocationError;
use restate_types::identifiers::{DeploymentId, InvocationId, PartitionLeaderEpoch};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::EntryType;
use std::time::{Duration, SystemTime};

#[tokio::test]
async fn join_invocation_tables_with_error() {
    let invocation_id = InvocationId::mock_random();
    let invocation_target = InvocationTarget::service("MySvc", "MyMethod");
    let invocation_error = InvocationError::internal("my error");

    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let (mut engine, shutdown) = tc
        .run_in_scope(
            "mock-query-engine",
            None,
            MockQueryEngine::create_with(
                MockStatusHandle::default().with(InvocationStatusReport::new(
                    invocation_id,
                    PartitionLeaderEpoch::default(),
                    InvocationStatusReportInner {
                        in_flight: false,
                        start_count: 1,
                        last_start_at: SystemTime::now() - Duration::from_secs(10),
                        last_retry_attempt_failure: Some(InvocationErrorReport {
                            err: invocation_error.clone(),
                            doc_error_code: None,
                            related_entry_index: Some(1),
                            related_entry_name: Some("my-side-effect".to_string()),
                            related_entry_type: Some(EntryType::SideEffect),
                        }),
                        next_retry_at: Some(SystemTime::now() + Duration::from_secs(10)),
                        last_attempt_deployment_id: Some(DeploymentId::new()),
                        last_attempt_server: Some("restate-sdk-java/0.8.0".to_owned()),
                    },
                )),
                MockSchemas::default(),
            ),
        )
        .await;

    let mut tx = engine.rocksdb_mut().transaction();
    tx.put_invocation_status(
        &invocation_id,
        InvocationStatus::Invoked(InFlightInvocationMetadata {
            invocation_target: invocation_target.clone(),
            response_sinks: Default::default(),
            ..InFlightInvocationMetadata::mock()
        }),
    )
    .await;
    tx.commit().await.unwrap();

    let records = engine
        .execute(
            "SELECT
                pps.id,
                pps.target_service_name,
                pps.target_handler_name,
                is.last_failure,
                is.last_failure_related_entry_index,
                is.last_failure_related_entry_name,
                is.last_failure_related_entry_type,
                is.last_attempt_server
            FROM sys_invocation_status pps
            JOIN sys_invocation_state is ON pps.id = is.id
            LIMIT 1",
        )
        .await
        .unwrap()
        .collect::<Vec<Result<RecordBatch, _>>>()
        .await
        .remove(0)
        .unwrap();

    assert_that!(
        records,
        all!(row!(
            0,
            {
                "id" => LargeStringArray: eq(invocation_id.to_string()),
                "target_service_name" => LargeStringArray: eq(invocation_target.service_name().to_string()),
                "target_handler_name" => LargeStringArray: eq(invocation_target.handler_name().to_string()),
                "last_failure" => LargeStringArray: eq(invocation_error.to_string()),
                "last_failure_related_entry_index" => UInt64Array: eq(1),
                "last_failure_related_entry_name" => LargeStringArray: eq("my-side-effect"),
                "last_failure_related_entry_type" => LargeStringArray: eq(EntryType::SideEffect.to_string()),
                "last_attempt_server" => LargeStringArray: eq("restate-sdk-java/0.8.0"),
            }
        ))
    );

    shutdown.await;
}
