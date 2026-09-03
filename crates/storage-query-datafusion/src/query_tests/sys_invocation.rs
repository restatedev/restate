// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::data::{FixtureFactory, InvocationFixture, InvocationOptions};
use super::fixture::{QueryExpectation, QueryFixture};

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

    fixture
        .assert_queries(&[QueryExpectation {
            name: "sys_invocation rows selected by invocation id",
            sql: &format!(
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
            ),
            expected: &[
                "+----------------------------------------+-----------------------+---------------------+--------------------+---------------------+-------------------+-----------------+------------+----------------------------------------+----------------------------+--------------------+----------------+----------------------------+---------------------------------+--------------+-----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+--------------+-----------------------------------+-----------------------------------+-------------+----------------------+---------------+----------------------------+------------------------+--------------+-------------------------+---------+-------------------+--------------------+------------------------------+-----------------------------------+-------------------------------+-------------------------------+---------+---------------------------------------+-----------+",
                "| id                                     | target                | target_service_name | target_service_key | target_handler_name | target_service_ty | idempotency_key | invoked_by | invoked_by_id                          | invoked_by_subscription_id | invoked_by_target  | restarted_from | pinned_deployment_id       | pinned_service_protocol_version | journal_size | journal_commands_size | created_at           | modified_at          | inboxed_at           | scheduled_at         | scheduled_start_at   | running_at           | completed_at | completion_retention              | journal_retention                 | retry_count | last_start_at        | next_retry_at | last_attempt_deployment_id | last_attempt_server    | last_failure | last_failure_error_code | status  | completion_result | completion_failure | last_awaiting_on_future_json | suspended_waiting_for_completions | suspended_waiting_for_signals | suspended_waiting_future_json | scope   | vqueue_id                             | limit_key |",
                "+----------------------------------------+-----------------------+---------------------+--------------------+---------------------+-------------------+-----------------+------------+----------------------------------------+----------------------------+--------------------+----------------+----------------------------+---------------------------------+--------------+-----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+--------------+-----------------------------------+-----------------------------------+-------------+----------------------+---------------+----------------------------+------------------------+--------------+-------------------------+---------+-------------------+--------------------+------------------------------+-----------------------------------+-------------------------------+-------------------------------+---------+---------------------------------------+-----------+",
                "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | TestService/key-1/run | TestService         | key-1              | run                 | virtual_object    | request-1       | service    | inv_1000000000000wmGWo1cYRpBzxaa9vzlLi |                            | CallerService/call |                | dp_101SZwviYzes2mkYBx6TUys | 5                               | 2            | 2                     | 1970-01-01T00:00:01Z | 1970-01-01T00:00:06Z | 1970-01-01T00:00:02Z | 1970-01-01T00:00:03Z | 1970-01-01T00:00:04Z | 1970-01-01T00:00:05Z |              | 0 days 0 hours 0 mins 30.000 secs | 0 days 0 hours 0 mins 10.000 secs | 1           | 1970-01-01T00:00:05Z |               | dp_101SZwviYzes2mkYBx6TUys | restate-sdk-rust/0.1.0 |              |                         | running |                   |                    |                              |                                   |                               |                               | scope-a | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | tenant/eu |",
                "| inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | TestService/key-1/run | TestService         | key-1              | run                 | virtual_object    | request-1       | service    | inv_1000000000000A8FZoDa7kjGiaMuSv8PEk |                            | CallerService/call |                | dp_101SZwviYzes2mkYBx6TUys | 5                               | 2            | 2                     | 1970-01-01T00:00:01Z | 1970-01-01T00:00:06Z | 1970-01-01T00:00:02Z | 1970-01-01T00:00:03Z | 1970-01-01T00:00:04Z | 1970-01-01T00:00:05Z |              | 0 days 0 hours 0 mins 30.000 secs | 0 days 0 hours 0 mins 10.000 secs | 1           | 1970-01-01T00:00:05Z |               | dp_101SZwviYzes2mkYBx6TUys | restate-sdk-rust/0.1.0 |              |                         | running |                   |                    |                              |                                   |                               |                               | scope-a | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | tenant/eu |",
                "+----------------------------------------+-----------------------+---------------------+--------------------+---------------------+-------------------+-----------------+------------+----------------------------------------+----------------------------+--------------------+----------------+----------------------------+---------------------------------+--------------+-----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+----------------------+--------------+-----------------------------------+-----------------------------------+-------------+----------------------+---------------+----------------------------+------------------------+--------------+-------------------------+---------+-------------------+--------------------+------------------------------+-----------------------------------+-------------------------------+-------------------------------+---------+---------------------------------------+-----------+",
            ],
        }])
        .await;
}
