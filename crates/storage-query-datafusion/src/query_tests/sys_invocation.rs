// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::harness::{QueryExpectation, QueryTest};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_sys_invocation_by_id_list() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_invocation_status().populate_table(&[
            "+-----------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| column                            | invocation 1                             | invocation 2                             | invocation 3                             |",
            "+-----------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| partition_id                      | 0                                        | 1                                        | 2                                        |",
            "| partition_key                     | 3169317165037139997                      | 6564637988134260717                      | 16740507687615160162                     |",
            "| id                                | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   | inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4   | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy   |",
            "| vqueue_id                         | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR    |                                          | vq_18rEacLHS3jy26a53iN5aIaYj240f03U9L     |",
            "| status                            | invoked                                  | invoked                                  | invoked                                  |",
            "| completion_result                 |                                          |                                          |                                          |",
            "| completion_failure                |                                          |                                          |                                          |",
            "| target                            | TestService/key-1/run                    | TestService/key-1/run                    | TestService/key-1/run                    |",
            "| target_service_name               | TestService                              | TestService                              | TestService                              |",
            "| target_service_key                | key-1                                    | key-1                                    | key-1                                    |",
            "| target_handler_name               | run                                      | run                                      | run                                      |",
            "| target_service_ty                 | virtual_object                           | virtual_object                           | virtual_object                           |",
            "| scope                             | scope-a                                  | scope-j                                  | scope-b                                  |",
            "| limit_key                         | tenant/eu                                | tenant/eu                                | tenant/eu                                |",
            "| idempotency_key                   | request-1                                | request-1                                | request-1                                |",
            "| invoked_by                        | service                                  | ingress                                  | restart_as_new                           |",
            "| invoked_by_id                     | inv_1000000000000wmGWo1cYRpBzxaa9vzlLi   |                                          |                                          |",
            "| invoked_by_subscription_id        |                                          |                                          |                                          |",
            "| invoked_by_target                 | CallerService/call                       |                                          |                                          |",
            "| restarted_from                    |                                          |                                          | inv_18rEacLHS3jy03LZ30BX8sU4IDCkIZztT2   |",
            "| pinned_deployment_id              | dp_101SZwviYzes2mkYBx6TUys               | dp_101SZwviYzes2mkYBx6TUys               | dp_101SZwviYzes2mkYBx6TUys               |",
            "| pinned_service_protocol_version   | 5                                        | 5                                        | 5                                        |",
            "| journal_size                      | 2                                        | 2                                        | 2                                        |",
            "| journal_commands_size             | 2                                        | 2                                        | 2                                        |",
            "| created_at                        | 1000                                     | 1000                                     | 1000                                     |",
            "| modified_at                       | 6000                                     | 6000                                     | 6000                                     |",
            "| inboxed_at                        | 2000                                     | 2000                                     | 2000                                     |",
            "| scheduled_at                      | 3000                                     | 3000                                     | 3000                                     |",
            "| scheduled_start_at                | 4000                                     | 4000                                     | 4000                                     |",
            "| running_at                        | 5000                                     | 5000                                     | 5000                                     |",
            "| completed_at                      |                                          |                                          |                                          |",
            "| completion_retention              | 30000                                    | 30000                                    | 30000                                    |",
            "| journal_retention                 | 10000                                    | 10000                                    | 10000                                    |",
            "| suspended_waiting_for_completions |                                          |                                          |                                          |",
            "| suspended_waiting_for_signals     |                                          |                                          |                                          |",
            "| suspended_waiting_future_json     |                                          |                                          |                                          |",
            "+-----------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
        ])?;
        tables.sys_invocation_state().populate_table(&[
            "+--------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| column                         | invocation 1                             | invocation 2                             | invocation 3                             |",
            "+--------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| partition_id                   | 0                                        | 1                                        | 2                                        |",
            "| partition_key                  | 3169317165037139997                      | 6564637988134260717                      | 16740507687615160162                     |",
            "| id                             | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   | inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4   | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy   |",
            "| in_flight                      | true                                     | true                                     | true                                     |",
            "| retry_count                    | 1                                        | 1                                        | 1                                        |",
            "| last_start_at                  | 5000                                     | 5000                                     | 5000                                     |",
            "| next_retry_at                  |                                          |                                          |                                          |",
            "| last_attempt_deployment_id     | dp_101SZwviYzes2mkYBx6TUys               | dp_101SZwviYzes2mkYBx6TUys               | dp_101SZwviYzes2mkYBx6TUys               |",
            "| last_attempt_server            | restate-sdk-rust/0.1.0                   | restate-sdk-rust/0.1.0                   | restate-sdk-rust/0.1.0                   |",
            "| last_failure                   |                                          |                                          |                                          |",
            "| last_failure_error_code        |                                          |                                          |                                          |",
            "| last_awaiting_on_future_json   |                                          |                                          |                                          |",
            "+--------------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
        ])?;
        Ok(())
    })
    .await;

    test
        .assert_query(QueryExpectation {
            name: "sys_invocation rows selected by invocation id",
            sql: r#"SELECT
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
                     WHERE i.id IN (
                         'inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw',
                         'inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy'
                     )"#,
            expected: &[
                "+-----------------------------------+------------------------------------------+------------------------------------------+",
                "| column                            | row 1                                    | row 2                                    |",
                "+-----------------------------------+------------------------------------------+------------------------------------------+",
                "| id                                | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy   |",
                "| target                            | TestService/key-1/run                    | TestService/key-1/run                    |",
                "| target_service_name               | TestService                              | TestService                              |",
                "| target_service_key                | key-1                                    | key-1                                    |",
                "| target_handler_name               | run                                      | run                                      |",
                "| target_service_ty                 | virtual_object                           | virtual_object                           |",
                "| idempotency_key                   | request-1                                | request-1                                |",
                "| invoked_by                        | service                                  | restart_as_new                           |",
                "| invoked_by_id                     | inv_1000000000000wmGWo1cYRpBzxaa9vzlLi   |                                          |",
                "| invoked_by_subscription_id        |                                          |                                          |",
                "| invoked_by_target                 | CallerService/call                       |                                          |",
                "| restarted_from                    |                                          | inv_18rEacLHS3jy03LZ30BX8sU4IDCkIZztT2   |",
                "| pinned_deployment_id              | dp_101SZwviYzes2mkYBx6TUys               | dp_101SZwviYzes2mkYBx6TUys               |",
                "| pinned_service_protocol_version   | 5                                        | 5                                        |",
                "| journal_size                      | 2                                        | 2                                        |",
                "| journal_commands_size             | 2                                        | 2                                        |",
                "| created_at                        | 1970-01-01T00:00:01Z                     | 1970-01-01T00:00:01Z                     |",
                "| modified_at                       | 1970-01-01T00:00:06Z                     | 1970-01-01T00:00:06Z                     |",
                "| inboxed_at                        | 1970-01-01T00:00:02Z                     | 1970-01-01T00:00:02Z                     |",
                "| scheduled_at                      | 1970-01-01T00:00:03Z                     | 1970-01-01T00:00:03Z                     |",
                "| scheduled_start_at                | 1970-01-01T00:00:04Z                     | 1970-01-01T00:00:04Z                     |",
                "| running_at                        | 1970-01-01T00:00:05Z                     | 1970-01-01T00:00:05Z                     |",
                "| completed_at                      |                                          |                                          |",
                "| completion_retention              | 0 days 0 hours 0 mins 30.000 secs         | 0 days 0 hours 0 mins 30.000 secs         |",
                "| journal_retention                 | 0 days 0 hours 0 mins 10.000 secs         | 0 days 0 hours 0 mins 10.000 secs         |",
                "| retry_count                       | 1                                        | 1                                        |",
                "| last_start_at                     | 1970-01-01T00:00:05Z                     | 1970-01-01T00:00:05Z                     |",
                "| next_retry_at                     |                                          |                                          |",
                "| last_attempt_deployment_id        | dp_101SZwviYzes2mkYBx6TUys               | dp_101SZwviYzes2mkYBx6TUys               |",
                "| last_attempt_server               | restate-sdk-rust/0.1.0                   | restate-sdk-rust/0.1.0                   |",
                "| last_failure                      |                                          |                                          |",
                "| last_failure_error_code           |                                          |                                          |",
                "| status                            | running                                  | running                                  |",
                "| completion_result                 |                                          |                                          |",
                "| completion_failure                |                                          |                                          |",
                "| last_awaiting_on_future_json      |                                          |                                          |",
                "| suspended_waiting_for_completions |                                          |                                          |",
                "| suspended_waiting_for_signals     |                                          |                                          |",
                "| suspended_waiting_future_json     |                                          |                                          |",
                "| scope                             | scope-a                                  | scope-b                                  |",
                "| vqueue_id                         | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR    | vq_18rEacLHS3jy26a53iN5aIaYj240f03U9L     |",
                "| limit_key                         | tenant/eu                                | tenant/eu                                |",
                "+-----------------------------------+------------------------------------------+------------------------------------------+",
            ],
        })
        .await;

    test.assert_query(QueryExpectation {
        name: "sys_invocation renders service, ingress, and restart sources",
        sql: r#"SELECT
                       id,
                       invoked_by,
                       invoked_by_id,
                       invoked_by_subscription_id,
                       invoked_by_target,
                       restarted_from
                   FROM sys_invocation
                   WHERE id IN (
                       'inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw',
                       'inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4',
                       'inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy'
                   )"#,
        expected: &[
            "+----------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| column                     | row 1                                    | row 2                                    | row 3                                    |",
            "+----------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| id                         | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy   | inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4   |",
            "| invoked_by                 | service                                  | restart_as_new                           | ingress                                  |",
            "| invoked_by_id              | inv_1000000000000wmGWo1cYRpBzxaa9vzlLi   |                                          |                                          |",
            "| invoked_by_subscription_id |                                          |                                          |                                          |",
            "| invoked_by_target          | CallerService/call                       |                                          |                                          |",
            "| restarted_from             |                                          | inv_18rEacLHS3jy03LZ30BX8sU4IDCkIZztT2   |                                          |",
            "+----------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
        ],
    })
    .await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_sys_invocation_status_projection_edges() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_invocation_status().populate_table(&[
            "+--------------+----------------------+----------------------------------------+-----------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
            "| partition_id | partition_key        | id                                     | status    | completion_result | created_at | target_service_name | target_service_key | target_handler_name | target_service_ty | scope   |",
            "+--------------+----------------------+----------------------------------------+-----------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | inboxed   |                   | 1000       | TestService         | key-1              | run                 | virtual_object    | scope-a |",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 | scheduled |                   | 2000       | TestService         | key-2              | run                 | virtual_object    | scope-a |",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | invoked   |                   | 3000       | TestService         | key-3              | run                 | virtual_object    | scope-a |",
            "| 1            | 6564637988134260717  | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw | invoked   |                   | 4000       | TestService         | key-4              | run                 | virtual_object    | scope-j |",
            "| 1            | 6564637988134260717  | inv_1klS9KSVEL8v03LZ30BX8sU4IDCkIZztT2 | invoked   |                   | 5000       | TestService         | key-5              | run                 | virtual_object    | scope-j |",
            "| 1            | 6564637988134260717  | inv_1klS9KSVEL8v05EYzvUVHHm74Xqv5umdPy | suspended |                   | 6000       | TestService         | key-6              | run                 | virtual_object    | scope-j |",
            "| 2            | 16740507687615160162 | inv_18rEacLHS3jy01SZwviYzes2mjOamuMJWw | paused    |                   | 7000       | TestService         | key-7              | run                 | virtual_object    | scope-b |",
            "| 2            | 16740507687615160162 | inv_18rEacLHS3jy03LZ30BX8sU4IDCkIZztT2 | completed | success           | 8000       | TestService         | key-8              | run                 | virtual_object    | scope-b |",
            "| 2            | 16740507687615160162 | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy | completed | failure           | 9000       | TestService         | key-9              | run                 | virtual_object    | scope-b |",
            "+--------------+----------------------+----------------------------------------+-----------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
        ])?;
        tables.sys_invocation_state().populate_table(&[
            "+----------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| column                     | backing off                              | running                                  | state only                               |",
            "+----------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| partition_id               | 1                                        | 1                                        | 2                                        |",
            "| partition_key              | 6564637988134260717                      | 6564637988134260717                      | 16740507687615160162                     |",
            "| id                         | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw   | inv_1klS9KSVEL8v03LZ30BX8sU4IDCkIZztT2   | inv_18rEacLHS3jy07xY61dUgVO9rheFrZ8XM4   |",
            "| in_flight                  | false                                    | true                                     | true                                     |",
            "| retry_count                | 2                                        | 0                                        | 4                                        |",
            "| last_start_at              | 4500                                     | 5500                                     | 9500                                     |",
            "| next_retry_at              | 10000                                    |                                          |                                          |",
            "| last_attempt_deployment_id |                                          |                                          |                                          |",
            "| last_attempt_server        |                                          |                                          |                                          |",
            "| last_failure               | [503] retry failure                      |                                          |                                          |",
            "| last_failure_error_code    |                                          |                                          |                                          |",
            "| last_awaiting_on_future_json |                                        | {\"Unknown\":[]}                           |                                          |",
            "+----------------------------+------------------------------------------+------------------------------------------+------------------------------------------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query_ordered(QueryExpectation {
        name: "sys_invocation derives every UI status and keeps nullable live state",
        sql: r#"SELECT id, status, retry_count, completion_result, completion_failure
                   FROM sys_invocation
                   WHERE target_service_name = 'TestService'
                   ORDER BY created_at
                   LIMIT 9"#,
        expected: &[
            "+----------------------------------------+-------------+-------------+-------------------+-----------------------+",
            "| id                                     | status      | retry_count | completion_result | completion_failure    |",
            "+----------------------------------------+-------------+-------------+-------------------+-----------------------+",
            "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | pending     |             |                   |                       |",
            "| inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 | scheduled   |             |                   |                       |",
            "| inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | ready       |             |                   |                       |",
            "| inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw | backing-off | 2           |                   |                       |",
            "| inv_1klS9KSVEL8v03LZ30BX8sU4IDCkIZztT2 | running     | 0           |                   |                       |",
            "| inv_1klS9KSVEL8v05EYzvUVHHm74Xqv5umdPy | suspended   |             |                   |                       |",
            "| inv_18rEacLHS3jy01SZwviYzes2mjOamuMJWw | paused      |             |                   |                       |",
            "| inv_18rEacLHS3jy03LZ30BX8sU4IDCkIZztT2 | completed   |             | success           |                       |",
            "| inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy | completed   |             | failure           | [500] fixture failure |",
            "+----------------------------------------+-------------+-------------+-------------------+-----------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "sys_invocation preserves live retry failures and unresolved futures",
        sql: r#"SELECT id, last_failure, last_failure_error_code, last_awaiting_on_future_json
                   FROM sys_invocation
                   WHERE id IN (
                       'inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw',
                       'inv_1klS9KSVEL8v03LZ30BX8sU4IDCkIZztT2'
                   )"#,
        expected: &[
            "+------------------------------+------------------------------------------+------------------------------------------+",
            "| column                       | row 1                                    | row 2                                    |",
            "+------------------------------+------------------------------------------+------------------------------------------+",
            "| id                           | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw   | inv_1klS9KSVEL8v03LZ30BX8sU4IDCkIZztT2   |",
            "| last_failure                 | [503] retry failure                      |                                          |",
            "| last_failure_error_code      |                                          |                                          |",
            "| last_awaiting_on_future_json |                                          | {\"Unknown\":[]}                           |",
            "+------------------------------+------------------------------------------+------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "sys_invocation exposes a suspended invocation future",
        sql: r#"SELECT
                       id,
                       suspended_waiting_for_completions,
                       suspended_waiting_for_signals,
                       suspended_waiting_future_json
                   FROM sys_invocation
                   WHERE id = 'inv_1klS9KSVEL8v05EYzvUVHHm74Xqv5umdPy'"#,
        expected: &[
            "+------------------------------------------+------------------------------------------+",
            "| column                                   | row 1                                    |",
            "+------------------------------------------+------------------------------------------+",
            "| id                                       | inv_1klS9KSVEL8v05EYzvUVHHm74Xqv5umdPy   |",
            "| suspended_waiting_for_completions       | [1]                                      |",
            "| suspended_waiting_for_signals           | []                                       |",
            "| suspended_waiting_future_json           | {\"Unknown\":[{\"Single\":{\"CompletionId\":1}}]} |",
            "+------------------------------------------+------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "sys_invocation right join excludes an orphan live-state report",
        sql: r#"SELECT COUNT(*) AS count
                   FROM sys_invocation
                   WHERE id = 'inv_18rEacLHS3jy07xY61dUgVO9rheFrZ8XM4'"#,
        expected: &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 0     |",
            "+-------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "sys_invocation id list ignores a missing id",
        sql: r#"SELECT id, status
                   FROM sys_invocation
                   WHERE id IN (
                       'inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw',
                       'inv_18rEacLHS3jy07xY61dUgVO9rheFrZ8XM4'
                   )"#,
        expected: &[
            "+----------------------------------------+---------+",
            "| id                                     | status  |",
            "+----------------------------------------+---------+",
            "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | pending |",
            "+----------------------------------------+---------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "sys_invocation id list excludes completed rows after view status derivation",
        sql: r#"SELECT id, status
                   FROM sys_invocation
                   WHERE id IN (
                       'inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy',
                       'inv_18rEacLHS3jy03LZ30BX8sU4IDCkIZztT2'
                   )
                     AND status <> 'completed'"#,
        expected: &[
            "+----------------------------------------+--------+",
            "| id                                     | status |",
            "+----------------------------------------+--------+",
            "| inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | ready  |",
            "+----------------------------------------+--------+",
        ],
    })
    .await;
}
