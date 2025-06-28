// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{Duration, SystemTime};

use datafusion::arrow::array::{LargeStringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::all;
use googletest::prelude::{assert_that, eq};

use crate::mocks::*;
use crate::row;
use restate_invoker_api::status_handle::InvocationStatusReportInner;
use restate_invoker_api::status_handle::test_util::MockStatusHandle;
use restate_invoker_api::{InvocationErrorReport, InvocationStatusReport};
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
};
use restate_types::errors::InvocationError;
use restate_types::identifiers::LeaderEpoch;
use restate_types::identifiers::PartitionId;
use restate_types::identifiers::{DeploymentId, InvocationId};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::EntryType;
use restate_types::service_protocol::ServiceProtocolVersion;

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_sys_invocation() {
    let invocation_id = InvocationId::mock_random();
    let invocation_target = InvocationTarget::service("MySvc", "MyMethod");
    let invocation_error = InvocationError::internal("my error");

    let mut engine = MockQueryEngine::create_with(
        MockStatusHandle::default().with(InvocationStatusReport::new(
            invocation_id,
            (PartitionId::MIN, LeaderEpoch::INITIAL),
            InvocationStatusReportInner {
                in_flight: false,
                start_count: 1,
                last_start_at: SystemTime::now() - Duration::from_secs(10),
                last_retry_attempt_failure: Some(InvocationErrorReport {
                    err: invocation_error.clone(),
                    doc_error_code: None,
                    related_entry_index: Some(1),
                    related_entry_name: Some("my-side-effect".to_string()),
                    related_entry_type: Some(EntryType::Run),
                }),
                next_retry_at: Some(SystemTime::now() + Duration::from_secs(10)),
                last_attempt_deployment_id: Some(DeploymentId::new()),
                last_attempt_protocol_version: Some(ServiceProtocolVersion::V3),
                last_attempt_server: Some("restate-sdk-java/0.8.0".to_owned()),
            },
        )),
        MockSchemas::default(),
    )
    .await;

    let mut tx = engine.partition_store().transaction();
    tx.put_invocation_status(
        &invocation_id,
        &InvocationStatus::Invoked(InFlightInvocationMetadata {
            invocation_target: invocation_target.clone(),
            response_sinks: Default::default(),
            ..InFlightInvocationMetadata::mock()
        }),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let assert_rows = |records: RecordBatch| {
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
                    "last_failure_related_entry_type" => LargeStringArray: eq(EntryType::Run.to_string()),
                    "last_attempt_server" => LargeStringArray: eq("restate-sdk-java/0.8.0"),
                }
            ))
        );
    };

    let records = engine
        .execute(
            "SELECT
                id,
                target_service_name,
                target_handler_name,
                last_failure,
                last_failure_related_entry_index,
                last_failure_related_entry_name,
                last_failure_related_entry_type,
                last_attempt_server
            FROM sys_invocation
            LIMIT 1",
        )
        .await
        .unwrap()
        .collect::<Vec<Result<RecordBatch, _>>>()
        .await
        .remove(0)
        .unwrap();
    assert_rows(records);

    let records = engine
        .execute(
            "SELECT
                id,
                target_service_name,
                target_handler_name,
                last_failure,
                last_failure_related_entry_index,
                last_failure_related_entry_name,
                last_failure_related_entry_type,
                last_attempt_server
            FROM sys_invocation
            ORDER BY last_failure_related_entry_index
            LIMIT 1",
        )
        .await
        .unwrap()
        .collect::<Vec<Result<RecordBatch, _>>>()
        .await
        .remove(0)
        .unwrap();
    assert_rows(records);

    let records = engine
        .execute(format!(
            "SELECT
                id,
                target_service_name,
                target_handler_name,
                last_failure,
                last_failure_related_entry_index,
                last_failure_related_entry_name,
                last_failure_related_entry_type,
                last_attempt_server
            FROM sys_invocation
            WHERE id = '{invocation_id}'
            LIMIT 1"
        ))
        .await
        .unwrap()
        .collect::<Vec<Result<RecordBatch, _>>>()
        .await
        .remove(0)
        .unwrap();
    assert_rows(records);
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_sys_invocation_with_protocol_v4() {
    let invocation_id = InvocationId::mock_random();
    let invocation_target = InvocationTarget::service("MySvc", "MyMethod");
    let invocation_error = InvocationError::internal("my error");

    let mut engine = MockQueryEngine::create_with(
        MockStatusHandle::default().with(InvocationStatusReport::new(
            invocation_id,
            (PartitionId::MIN, LeaderEpoch::INITIAL),
            InvocationStatusReportInner {
                in_flight: false,
                start_count: 1,
                last_start_at: SystemTime::now() - Duration::from_secs(10),
                last_retry_attempt_failure: Some(InvocationErrorReport {
                    err: invocation_error.clone(),
                    doc_error_code: None,
                    related_entry_index: Some(1),
                    related_entry_name: Some("my-side-effect".to_string()),
                    related_entry_type: Some(EntryType::Run),
                }),
                next_retry_at: Some(SystemTime::now() + Duration::from_secs(10)),
                last_attempt_deployment_id: Some(DeploymentId::new()),
                last_attempt_protocol_version: Some(ServiceProtocolVersion::V4),
                last_attempt_server: Some("restate-sdk-java/1.3.0".to_owned()),
            },
        )),
        MockSchemas::default(),
    )
    .await;

    let mut tx = engine.partition_store().transaction();
    tx.put_invocation_status(
        &invocation_id,
        &InvocationStatus::Invoked(InFlightInvocationMetadata {
            invocation_target: invocation_target.clone(),
            response_sinks: Default::default(),
            ..InFlightInvocationMetadata::mock()
        }),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let records = engine
        .execute(
            "SELECT
                id,
                last_failure_related_command_index,
                last_failure_related_command_name,
                last_failure_related_command_type
            FROM sys_invocation
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
                "last_failure_related_command_index" => UInt64Array: eq(1),
                "last_failure_related_command_name" => LargeStringArray: eq("my-side-effect"),
                "last_failure_related_command_type" => LargeStringArray: eq(EntryType::Run.to_string()),
            }
        ))
    );
}
