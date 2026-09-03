// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use restate_types::time::MillisSinceEpoch;

use super::data::{FixtureFactory, InvocationFixture, InvocationOptions};
use super::fixture::{ExpectedRow, QueryExpectation, QueryFixture, format_expected_table};

fn timestamp(value: MillisSinceEpoch) -> String {
    value.into_timestamp().to_string()
}

fn duration(value: Duration) -> String {
    let millis = value.as_millis();
    let seconds = millis / 1_000;
    let minutes = seconds / 60;
    let hours = minutes / 60;
    let days = hours / 24;

    format!(
        "{} days {} hours {} mins {}.{:03} secs",
        days,
        hours % 24,
        minutes % 60,
        seconds % 60,
        millis % 1_000,
    )
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_sys_invocation_by_id_list() {
    let mut factory = FixtureFactory::default();
    let vqueue = factory.create_vqueue();
    let invocations: [InvocationFixture; 3] = std::array::from_fn(|_| {
        factory.create_invocation(InvocationOptions {
            vqueue: Some(&vqueue),
            service_name: "TestService",
            ..InvocationOptions::default()
        })
    });
    let [invocation_1, _, invocation_3] = &invocations;

    let mut fixture = QueryFixture::create().await;
    fixture
        .populate(|tables| {
            for invocation in &invocations {
                tables.sys_invocation_status().populate(invocation)?;
                tables.sys_invocation_state().populate(invocation);
            }
            Ok(())
        })
        .await;

    let sql = format!(
        r#"SELECT
               id,
               target,
               target_service_name,
               target_service_key,
               target_handler_name,
               target_service_ty,
               idempotency_key,
               invoked_by,
               invoked_by_id,
               invoked_by_subscription_id,
               invoked_by_target,
               restarted_from,
               pinned_deployment_id,
               pinned_service_protocol_version,
               journal_size,
               journal_commands_size,
               created_at,
               modified_at,
               inboxed_at,
               scheduled_at,
               scheduled_start_at,
               running_at,
               completed_at,
               completion_retention,
               journal_retention,
               retry_count,
               last_start_at,
               next_retry_at,
               last_attempt_deployment_id,
               last_attempt_server,
               last_failure,
               last_failure_error_code,
               status,
               completion_result,
               completion_failure,
               last_awaiting_on_future_json,
               suspended_waiting_for_completions,
               suspended_waiting_for_signals,
               suspended_waiting_future_json,
               scope,
               vqueue_id,
               limit_key
             FROM sys_invocation i
             WHERE i.id IN ('{}', '{}')"#,
        invocation_1.id, invocation_3.id,
    );

    let expected_row = |invocation: &InvocationFixture| -> ExpectedRow {
        let source = invocation.source();
        let suspended_waiting = invocation.suspended_waiting();

        expected_row! {
            "id": invocation.id.to_string(),
            "target": invocation.target.to_string(),
            "target_service_name": invocation.target.service_name().to_string(),
            "target_service_key": invocation.target.key().map(ToString::to_string),
            "target_handler_name": invocation.target.handler_name().to_string(),
            "target_service_ty": invocation.target_service_ty(),
            "idempotency_key": invocation.idempotency_key.as_ref().map(ToString::to_string),
            "invoked_by": source.invoked_by,
            "invoked_by_id": source.id,
            "invoked_by_subscription_id": source.subscription_id,
            "invoked_by_target": source.target,
            "restarted_from": source.restarted_from,
            "pinned_deployment_id": invocation.pinned_deployment.as_ref().map(|pinned| pinned.deployment_id.to_string()),
            "pinned_service_protocol_version": invocation.pinned_deployment.as_ref().map(|pinned| pinned.service_protocol_version.as_repr()),
            "journal_size": invocation.journal.length,
            "journal_commands_size": invocation.journal.commands,
            "created_at": timestamp(invocation.timestamps.creation_time()),
            "modified_at": timestamp(invocation.timestamps.modification_time()),
            "inboxed_at": invocation.timestamps.inboxed_transition_time().map(timestamp),
            "scheduled_at": invocation.timestamps.scheduled_transition_time().map(timestamp),
            "scheduled_start_at": invocation.execution_time.map(timestamp),
            "running_at": invocation.timestamps.running_transition_time().map(timestamp),
            "completed_at": invocation.timestamps.completed_transition_time().map(timestamp),
            "completion_retention": duration(invocation.completion_retention),
            "journal_retention": duration(invocation.journal_retention),
            "retry_count": invocation.state.start_count,
            "last_start_at": timestamp(invocation.state.last_start_at.into()),
            "next_retry_at": invocation.state.next_retry_at.map(|value| timestamp(value.into())),
            "last_attempt_deployment_id": invocation.state.last_attempt_deployment_id.map(|id| id.to_string()),
            "last_attempt_server": invocation.state.last_attempt_server.as_deref(),
            "last_failure": invocation.last_failure(),
            "last_failure_error_code": invocation.last_failure_error_code(),
            "status": invocation.status_name(),
            "completion_result": invocation.completion_result(),
            "completion_failure": invocation.completion_failure(),
            "last_awaiting_on_future_json": invocation.last_awaiting_on_future_json(),
            "suspended_waiting_for_completions": suspended_waiting.as_ref().map(|waiting| &waiting.completions),
            "suspended_waiting_for_signals": suspended_waiting.as_ref().map(|waiting| &waiting.signals),
            "suspended_waiting_future_json": suspended_waiting.as_ref().map(|waiting| &waiting.future_json),
            "scope": invocation.target.scope().map(ToString::to_string),
            "vqueue_id": invocation.vqueue_id.as_ref().map(ToString::to_string),
            "limit_key": invocation.limit_key.to_string(),
        }
    };

    let expected = format_expected_table(&[expected_row(invocation_1), expected_row(invocation_3)]);

    fixture
        .assert_queries(&[QueryExpectation {
            name: "sys_invocation rows selected by invocation id",
            sql: &sql,
            expected: &expected,
        }])
        .await;
}
