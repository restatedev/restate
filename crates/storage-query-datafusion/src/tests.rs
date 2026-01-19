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

use crate::mocks::*;
use crate::row;
use datafusion::arrow::array::{
    ArrayRef, LargeStringArray, ListArray, TimestampMillisecondArray, UInt32Array, UInt64Array,
};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use googletest::prelude::{all, assert_that, eq};
use googletest::unordered_elements_are;
use restate_invoker_api::status_handle::InvocationStatusReportInner;
use restate_invoker_api::status_handle::test_util::MockStatusHandle;
use restate_invoker_api::{InvocationErrorReport, InvocationStatusReport};
use restate_storage_api::Transaction;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
};
use restate_types::errors::InvocationError;
use restate_types::identifiers::PartitionId;
use restate_types::identifiers::{DeploymentId, InvocationId};
use restate_types::identifiers::{InvocationUuid, LeaderEpoch};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::EntryType;
use restate_types::journal_v2::NotificationId;
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

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_sys_invocation_status_completed() {
    let invocation_target = InvocationTarget::mock_service();

    // To have deterministic ordering
    let invocation_id_1 = InvocationId::from_parts(0, InvocationUuid::from_u128(1));
    let invocation_id_2 = InvocationId::from_parts(0, InvocationUuid::from_u128(2));
    let invocation_id_3 = InvocationId::from_parts(0, InvocationUuid::from_u128(3));

    let completion_retention_duration_1 = Duration::from_secs(100);
    let journal_retention_duration_1 = Duration::from_secs(50);
    let completed_invocation_1 = CompletedInvocation {
        invocation_target: invocation_target.clone(),
        completion_retention_duration: completion_retention_duration_1,
        journal_retention_duration: journal_retention_duration_1,
        ..CompletedInvocation::mock_neo()
    };
    let completed_on = completed_invocation_1
        .timestamps
        .completed_transition_time()
        .unwrap();

    let mut engine = MockQueryEngine::create().await;

    let mut tx = engine.partition_store().transaction();
    tx.put_invocation_status(
        &invocation_id_1,
        &InvocationStatus::Completed(completed_invocation_1.clone()),
    )
    .unwrap();
    tx.put_invocation_status(
        &invocation_id_2,
        &InvocationStatus::Completed(CompletedInvocation {
            completion_retention_duration: Duration::ZERO,
            journal_retention_duration: Duration::ZERO,
            ..completed_invocation_1.clone()
        }),
    )
    .unwrap();
    tx.put_invocation_status(
        &invocation_id_3,
        &InvocationStatus::Completed(CompletedInvocation {
            completion_retention_duration: Duration::from_secs(10),
            journal_retention_duration: Duration::from_secs(10),
            ..completed_invocation_1
        }),
    )
    .unwrap();
    tx.commit().await.unwrap();

    let records = engine
        .execute(
            "SELECT
                id,
                completed_at + completion_retention AS completion_expiration,
                completed_at + journal_retention AS journal_expiration
            FROM sys_invocation_status
            WHERE journal_retention >= INTERVAL 5 SECOND
            ORDER BY id ASC",
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
                    "id" => LargeStringArray: eq(invocation_id_1.to_string()),
                    "completion_expiration" => TimestampMillisecondArray: eq((completed_on + completion_retention_duration_1).as_u64() as i64),
                    "journal_expiration" => TimestampMillisecondArray: eq((completed_on + journal_retention_duration_1).as_u64() as i64),
                }
            ),
            row!(
                1,
                {
                    "id" => LargeStringArray: eq(invocation_id_3.to_string()),
                    "completion_expiration" => TimestampMillisecondArray: eq((completed_on + Duration::from_secs(10)).as_u64() as i64),
                    "journal_expiration" => TimestampMillisecondArray: eq((completed_on + Duration::from_secs(10)).as_u64() as i64),
                }
            ),
        )
    );
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_sys_invocation_suspended_waiting() {
    let invocation_id = InvocationId::mock_random();

    let mut engine = MockQueryEngine::create().await;

    let mut tx = engine.partition_store().transaction();
    tx.put_invocation_status(
        &invocation_id,
        &InvocationStatus::Suspended {
            metadata: InFlightInvocationMetadata {
                invocation_target: InvocationTarget::mock_service(),
                ..InFlightInvocationMetadata::mock()
            },
            waiting_for_notifications: [
                NotificationId::for_completion(1),
                NotificationId::for_completion(2),
                NotificationId::for_completion(3),
                NotificationId::SignalIndex(10),
                NotificationId::SignalIndex(20),
            ]
            .into(),
        },
    )
    .unwrap();
    tx.commit().await.unwrap();

    let records = engine
        .execute(
            "SELECT
                id,
                status,
                suspended_waiting_for_completions,
                suspended_waiting_for_signals
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
        all!(
            row!(0, {
                    "id" => LargeStringArray: eq(invocation_id.to_string()),
                    "status" => LargeStringArray: eq("suspended")
            }),
            row_column_matcher(
                0,
                "suspended_waiting_for_completions",
                extract_uint32_list,
                unordered_elements_are![eq(1), eq(2), eq(3)]
            ),
            row_column_matcher(
                0,
                "suspended_waiting_for_signals",
                extract_uint32_list,
                unordered_elements_are![eq(10), eq(20)]
            )
        )
    );
}

fn extract_uint32_list(column: &ArrayRef, row: usize) -> Option<Vec<u32>> {
    use datafusion::arrow::array::Array;

    let column = column
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("Downcast ref to ListArray");
    if column.len() <= row {
        return None;
    }

    let row_value_ref = column.value(row);
    let row_value_downcast = row_value_ref
        .as_any()
        .downcast_ref::<UInt32Array>()
        .expect("Downcast ref to UInt32Array");

    Some(row_value_downcast.values().to_owned().into())
}
