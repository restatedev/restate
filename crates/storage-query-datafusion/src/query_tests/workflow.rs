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
async fn query_workflow_ui_shapes() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_invocation_status().populate_table(&[
            "+--------------+----------------------+----------------------------------------+---------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
            "| partition_id | partition_key        | id                                     | status  | completion_result | created_at | target_service_name | target_service_key | target_handler_name | target_service_ty | scope   |",
            "+--------------+----------------------+----------------------------------------+---------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | invoked |                   | 1000       | TestWorkflow        | workflow-1         | run                 | workflow          | scope-a |",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 | invoked |                   | 2000       | TestWorkflow        | workflow-1         | signal              | workflow          | scope-a |",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | completed | success           | 500        | TestWorkflow        | workflow-1         | cancel              | workflow          | scope-a |",
            "| 1            | 6564637988134260717  | inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4 | invoked |                   | 3000       | TestWorkflow        | workflow-2         | run                 | workflow          | scope-j |",
            "| 2            | 16740507687615160162 | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy | invoked |                   | 4000       | TestWorkflow        | workflow-3         | run                 | workflow          | scope-b |",
            "+--------------+----------------------+----------------------------------------+---------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query_ordered(QueryExpectation {
        name: "workflow runs page",
        sql: r#"SELECT id
                   FROM sys_invocation_status
                   WHERE target_service_name = 'TestWorkflow'
                     AND target_service_ty = 'workflow'
                     AND target_handler_name = 'run'
                     AND target_service_key IS NOT NULL
                   ORDER BY created_at DESC NULLS LAST
                   LIMIT 3"#,
        expected: &[
            "+----------------------------------------+",
            "| id                                     |",
            "+----------------------------------------+",
            "| inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy |",
            "| inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4 |",
            "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw |",
            "+----------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "workflow interactions",
        sql: r#"SELECT id
                   FROM sys_invocation_status
                   WHERE target_service_name = 'TestWorkflow'
                     AND target_service_ty = 'workflow'
                     AND target_service_key = 'workflow-1'
                     AND target_handler_name <> 'run'
                     AND scope = 'scope-a'
                   ORDER BY created_at DESC NULLS LAST
                   LIMIT 1"#,
        expected: &[
            "+----------------------------------------+",
            "| id                                     |",
            "+----------------------------------------+",
            "| inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 |",
            "+----------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "workflow run lookup excludes interactions",
        sql: r#"SELECT id
                   FROM sys_invocation_status
                   WHERE target_service_name = 'TestWorkflow'
                     AND target_service_ty = 'workflow'
                     AND target_service_key = 'workflow-1'
                     AND target_handler_name = 'run'
                     AND scope = 'scope-a'
                   LIMIT 1"#,
        expected: &[
            "+----------------------------------------+",
            "| id                                     |",
            "+----------------------------------------+",
            "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw |",
            "+----------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "workflow last interaction uses the newest non-run handler",
        sql: r#"SELECT MAX(created_at) AS last_interaction_at
                   FROM sys_invocation_status
                   WHERE target_service_name = 'TestWorkflow'
                     AND target_service_ty = 'workflow'
                     AND target_service_key = 'workflow-1'
                     AND target_handler_name <> 'run'
                     AND scope = 'scope-a'"#,
        expected: &[
            "+----------------------+",
            "| last_interaction_at  |",
            "+----------------------+",
            "| 1970-01-01T00:00:02Z |",
            "+----------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "workflow run lookup is empty for an unknown workflow key",
        sql: r#"SELECT id
                   FROM sys_invocation_status
                   WHERE target_service_name = 'TestWorkflow'
                     AND target_service_ty = 'workflow'
                     AND target_service_key = 'missing-workflow'
                     AND target_handler_name = 'run'
                     AND scope = 'scope-a'
                   LIMIT 1"#,
        expected: &["+----+", "| id |", "+----+", "+----+"],
    })
    .await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_workflow_pending_promises_ui_shape() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_promise().populate_table(&[
            "+--------------+----------------------+---------+------------------+-------------+------------------+-----------+--------------------------+--------------------+",
            "| partition_id | partition_key        | scope   | service_name     | service_key | key              | completed | completion_success_value | completion_failure |",
            "+--------------+----------------------+---------+------------------+-------------+------------------+-----------+--------------------------+--------------------+",
            "| 0            | 3169317165037139997  | scope-a | TestWorkflow     | workflow-1  | pending-1        | false     |                          |                    |",
            "| 0            | 3169317165037139997  | scope-a | TestWorkflow     | workflow-1  | pending-2        | false     |                          |                    |",
            "| 0            | 3169317165037139997  | scope-a | TestWorkflow     | workflow-1  | completed-ok     | true      | done                     |                    |",
            "| 0            | 3169317165037139997  | scope-a | TestWorkflow     | workflow-1  | completed-failed | true      |                          | [503] promise failed |",
            "| 1            | 6564637988134260717  | scope-j | TestWorkflow     | workflow-2  | pending-other    | false     |                          |                    |",
            "| 2            | 16740507687615160162 | scope-b | TestWorkflow     | workflow-1  | pending-1        | false     |                          |                    |",
            "| 2            | 16740507687615160162 | scope-b | OtherWorkflow    | workflow-1  | pending-1        | false     |                          |                    |",
            "+--------------+----------------------+---------+------------------+-------------+------------------+-----------+--------------------------+--------------------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "workflow pending promise count excludes completed and other scopes",
        sql: r#"SELECT COUNT(*) AS count
                   FROM sys_promise
                   WHERE service_name = 'TestWorkflow'
                     AND service_key = 'workflow-1'
                     AND scope = 'scope-a'
                     AND completed = false"#,
        expected: &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 2     |",
            "+-------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "promise count aggregates rows from every remote scanner",
        sql: "SELECT COUNT(*) AS count FROM sys_promise WHERE completed = false",
        expected: &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 5     |",
            "+-------+",
        ],
    })
    .await;

    test.assert_query_ordered(QueryExpectation {
        name: "workflow promises preserve pending success and failure projections",
        sql: r#"SELECT key, completed, completion_success_value_utf8, completion_failure
                   FROM sys_promise
                   WHERE service_name = 'TestWorkflow'
                     AND service_key = 'workflow-1'
                     AND scope = 'scope-a'
                   ORDER BY key"#,
        expected: &[
            "+------------------+-----------+-------------------------------+----------------------+",
            "| key              | completed | completion_success_value_utf8 | completion_failure   |",
            "+------------------+-----------+-------------------------------+----------------------+",
            "| completed-failed | true      |                               | [503] promise failed |",
            "| completed-ok     | true      | done                          |                      |",
            "| pending-1        | false     |                               |                      |",
            "| pending-2        | false     |                               |                      |",
            "+------------------+-----------+-------------------------------+----------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "workflow pending promise count is empty for an unknown workflow",
        sql: r#"SELECT COUNT(*) AS count
                   FROM sys_promise
                   WHERE service_name = 'TestWorkflow'
                     AND service_key = 'missing-workflow'
                     AND scope = 'scope-a'
                     AND completed = false"#,
        expected: &[
            "+-------+",
            "| count |",
            "+-------+",
            "| 0     |",
            "+-------+",
        ],
    })
    .await;
}
