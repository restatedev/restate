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
async fn query_invocation_status_ui_shapes() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_invocation_status().populate_table(&[
            "+--------------+----------------------+----------------------------------------+-----------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
            "| partition_id | partition_key        | id                                     | status    | completion_result | created_at | target_service_name | target_service_key | target_handler_name | target_service_ty | scope   |",
            "+--------------+----------------------+----------------------------------------+-----------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | invoked   |                   | 1000       | TestService         | key-1              | run                 | virtual_object    | scope-a |",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 | invoked   |                   | 2000       | TestService         | key-2              | run                 | virtual_object    | scope-a |",
            "| 1            | 6564637988134260717  | inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4 | completed | success           | 3000       | TestService         | key-3              | run                 | virtual_object    | scope-j |",
            "| 1            | 6564637988134260717  | inv_1klS9KSVEL8v0bjX91PRpoIe9UR0aYIrF6 | invoked   |                   | 4000       | OtherService        | ignored-key        | run                 | virtual_object    | scope-j |",
            "| 2            | 16740507687615160162 | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy | invoked   |                   | 5000       | TestService         | key-5              | run                 | virtual_object    | scope-b |",
            "| 2            | 16740507687615160162 | inv_18rEacLHS3jy09qXCwwSQagbNB2POtVHIA | completed | failure           | 6000       | TestService         | key-4              | run                 | virtual_object    | scope-b |",
            "+--------------+----------------------+----------------------------------------+-----------+-------------------+------------+---------------------+--------------------+---------------------+-------------------+---------+",
        ])?;
        tables.sys_invocation_state().populate_table(&[
            "+--------------+----------------------+----------------------------------------+-----------+",
            "| partition_id | partition_key        | id                                     | in_flight |",
            "+--------------+----------------------+----------------------------------------+-----------+",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | true      |",
            "| 0            | 3169317165037139997  | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 | true      |",
            "| 1            | 6564637988134260717  | inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4 | false     |",
            "| 1            | 6564637988134260717  | inv_1klS9KSVEL8v0bjX91PRpoIe9UR0aYIrF6 | true      |",
            "| 2            | 16740507687615160162 | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy | false     |",
            "| 2            | 16740507687615160162 | inv_18rEacLHS3jy09qXCwwSQagbNB2POtVHIA | false     |",
            "+--------------+----------------------+----------------------------------------+-----------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "active virtual-object identities from invocation status",
        sql: r#"SELECT DISTINCT
                           CAST(partition_key AS VARCHAR) AS partition_key,
                           target_service_key AS object_key,
                           scope
                       FROM sys_invocation_status
                       WHERE target_service_name = 'TestService'
                         AND target_service_ty = 'virtual_object'
                         AND target_service_key IS NOT NULL
                         AND status <> 'completed'
                         AND scope = 'scope-a'
                       LIMIT 2"#,
        expected: &[
            "+---------------------+------------+---------+",
            "| partition_key       | object_key | scope   |",
            "+---------------------+------------+---------+",
            "| 3169317165037139997 | key-1      | scope-a |",
            "| 3169317165037139997 | key-2      | scope-a |",
            "+---------------------+------------+---------+",
        ],
    })
    .await;

    test
        .assert_query(QueryExpectation {
            name: "invocation summary from status and live state",
            sql: r#"SELECT
                           ss.target_service_name AS service_name,
                           CASE
                             WHEN ss.status = 'inboxed' THEN 'pending'
                             WHEN ss.status = 'invoked' AND sis.in_flight IS TRUE THEN 'running'
                             WHEN ss.status = 'invoked' THEN 'ready-yielded-backing-off'
                             WHEN ss.status = 'completed' AND ss.completion_result = 'success' THEN 'succeeded'
                             WHEN ss.status = 'completed' THEN 'failed'
                             ELSE ss.status
                           END AS bucket,
                           COUNT(1) AS count
                       FROM sys_invocation_status ss
                       LEFT JOIN sys_invocation_state sis ON sis.id = ss.id
                       WHERE ss.target_service_name IN ('TestService')
                       GROUP BY
                           ss.target_service_name,
                           CASE
                             WHEN ss.status = 'inboxed' THEN 'pending'
                             WHEN ss.status = 'invoked' AND sis.in_flight IS TRUE THEN 'running'
                             WHEN ss.status = 'invoked' THEN 'ready-yielded-backing-off'
                             WHEN ss.status = 'completed' AND ss.completion_result = 'success' THEN 'succeeded'
                             WHEN ss.status = 'completed' THEN 'failed'
                             ELSE ss.status
                           END"#,
            expected: &[
                "+--------------+---------------------------+-------+",
                "| service_name | bucket                    | count |",
                "+--------------+---------------------------+-------+",
                "| TestService  | failed                    | 1     |",
                "| TestService  | ready-yielded-backing-off | 1     |",
                "| TestService  | running                   | 2     |",
                "| TestService  | succeeded                 | 1     |",
                "+--------------+---------------------------+-------+",
            ],
        })
        .await;

    test.assert_query(QueryExpectation {
        name: "active invocation candidates from status",
        sql: r#"SELECT ss.id AS id
                       FROM sys_invocation_status ss
                       WHERE ss.status != 'completed'
                         AND ss.target_service_name IN ('TestService')
                       LIMIT 3"#,
        expected: &[
            "+----------------------------------------+",
            "| id                                     |",
            "+----------------------------------------+",
            "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw |",
            "| inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 |",
            "| inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy |",
            "+----------------------------------------+",
        ],
    })
    .await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_invocation_status_lifecycle_edges() {
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
            "+--------------+---------------------+----------------------------------------+-----------+",
            "| partition_id | partition_key       | id                                     | in_flight |",
            "+--------------+---------------------+----------------------------------------+-----------+",
            "| 0            | 3169317165037139997 | inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | true      |",
            "| 1            | 6564637988134260717 | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw | false     |",
            "+--------------+---------------------+----------------------------------------+-----------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "invocation summary covers every lifecycle bucket and a missing live-state row",
        sql: r#"SELECT
                       ss.target_service_name AS service_name,
                       CASE
                         WHEN ss.status = 'inboxed' THEN 'pending'
                         WHEN ss.status = 'invoked' AND sis.in_flight IS TRUE THEN 'running'
                         WHEN ss.status = 'invoked' THEN 'ready-yielded-backing-off'
                         WHEN ss.status = 'completed' AND ss.completion_result = 'success' THEN 'succeeded'
                         WHEN ss.status = 'completed' THEN 'failed'
                         ELSE ss.status
                       END AS bucket,
                       COUNT(1) AS count
                   FROM sys_invocation_status ss
                   LEFT JOIN sys_invocation_state sis ON sis.id = ss.id
                   WHERE ss.target_service_name IN ('TestService')
                   GROUP BY
                       ss.target_service_name,
                       CASE
                         WHEN ss.status = 'inboxed' THEN 'pending'
                         WHEN ss.status = 'invoked' AND sis.in_flight IS TRUE THEN 'running'
                         WHEN ss.status = 'invoked' THEN 'ready-yielded-backing-off'
                         WHEN ss.status = 'completed' AND ss.completion_result = 'success' THEN 'succeeded'
                         WHEN ss.status = 'completed' THEN 'failed'
                         ELSE ss.status
                       END"#,
        expected: &[
            "+--------------+---------------------------+-------+",
            "| service_name | bucket                    | count |",
            "+--------------+---------------------------+-------+",
            "| TestService  | failed                    | 1     |",
            "| TestService  | paused                    | 1     |",
            "| TestService  | pending                   | 1     |",
            "| TestService  | ready-yielded-backing-off | 2     |",
            "| TestService  | running                   | 1     |",
            "| TestService  | scheduled                 | 1     |",
            "| TestService  | succeeded                 | 1     |",
            "| TestService  | suspended                 | 1     |",
            "+--------------+---------------------------+-------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "completed invocations are excluded from active identity discovery",
        sql: r#"SELECT DISTINCT target_service_key, scope
                   FROM sys_invocation_status
                   WHERE target_service_name = 'TestService'
                     AND target_service_ty = 'virtual_object'
                     AND target_service_key IS NOT NULL
                     AND status <> 'completed'
                     AND scope = 'scope-b'
                   LIMIT 2"#,
        expected: &[
            "+--------------------+---------+",
            "| target_service_key | scope   |",
            "+--------------------+---------+",
            "| key-7              | scope-b |",
            "+--------------------+---------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "identity discovery returns an empty page for an unknown scope",
        sql: r#"SELECT DISTINCT target_service_key, scope
                   FROM sys_invocation_status
                   WHERE target_service_name = 'TestService'
                     AND target_service_ty = 'virtual_object'
                     AND target_service_key IS NOT NULL
                     AND status <> 'completed'
                     AND scope = 'missing-scope'
                   LIMIT 2"#,
        expected: &[
            "+--------------------+-------+",
            "| target_service_key | scope |",
            "+--------------------+-------+",
            "+--------------------+-------+",
        ],
    })
    .await;

    test.assert_query_ordered(QueryExpectation {
        name: "active invocation candidates apply a global top-k across partitions",
        sql: r#"SELECT ss.id AS id
                   FROM sys_invocation_status ss
                   WHERE ss.status != 'completed'
                     AND ss.target_service_name IN ('TestService')
                   ORDER BY ss.created_at DESC NULLS LAST
                   LIMIT 3"#,
        expected: &[
            "+----------------------------------------+",
            "| id                                     |",
            "+----------------------------------------+",
            "| inv_18rEacLHS3jy01SZwviYzes2mjOamuMJWw |",
            "| inv_1klS9KSVEL8v05EYzvUVHHm74Xqv5umdPy |",
            "| inv_1klS9KSVEL8v03LZ30BX8sU4IDCkIZztT2 |",
            "+----------------------------------------+",
        ],
    })
    .await;
}
