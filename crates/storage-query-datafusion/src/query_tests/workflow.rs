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
}
