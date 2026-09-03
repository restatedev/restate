// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::data::{FixtureFactory, InvocationFixtureStatus, InvocationOptions};
use super::fixture::{QueryExpectation, QueryFixture};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_invocation_status_ui_shapes() {
    let mut factory = FixtureFactory::default();
    let invocations = [
        factory.create_invocation(InvocationOptions {
            service_key: "key-1",
            ..InvocationOptions::default()
        }),
        factory.create_invocation(InvocationOptions {
            service_key: "key-1",
            ..InvocationOptions::default()
        }),
        factory.create_invocation(InvocationOptions {
            service_key: "key-2",
            status: InvocationFixtureStatus::BackingOff,
            ..InvocationOptions::default()
        }),
        factory.create_invocation(InvocationOptions {
            service_key: "key-3",
            status: InvocationFixtureStatus::CompletedSuccess,
            ..InvocationOptions::default()
        }),
        factory.create_invocation(InvocationOptions {
            service_key: "key-4",
            status: InvocationFixtureStatus::CompletedFailure,
            ..InvocationOptions::default()
        }),
        factory.create_invocation(InvocationOptions {
            service_name: "OtherService",
            service_key: "ignored-key",
            ..InvocationOptions::default()
        }),
    ];

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
        .assert_queries(&[
            QueryExpectation {
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
            },
            QueryExpectation {
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
            },
            QueryExpectation {
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
                    "| inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy |",
                    "+----------------------------------------+",
                ],
            },
        ])
        .await;
}
