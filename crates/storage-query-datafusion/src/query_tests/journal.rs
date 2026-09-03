// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::data::{FixtureFactory, InvocationOptions};
use super::fixture::{QueryExpectation, QueryFixture};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_journal_ui_shapes() {
    let mut factory = FixtureFactory::default();
    let invocation = factory.create_invocation(InvocationOptions::default());

    let mut fixture = QueryFixture::create().await;
    fixture
        .populate(|tables| {
            tables.sys_journal().populate(&invocation)?;
            tables.sys_journal_events().populate(&invocation)?;
            Ok(())
        })
        .await;

    fixture
        .assert_queries(&[
            QueryExpectation {
                name: "journal entries for an invocation",
                sql: &format!(
                    r#"SELECT
                           id,
                           index,
                           appended_at,
                           entry_type,
                           name,
                           raw_length,
                           entry_lite_json,
                           version,
                           completed,
                           sleep_wakeup_at,
                           invoked_id,
                           invoked_target,
                           promise_name
                       FROM sys_journal
                       WHERE id = '{}'
                       ORDER BY index"#,
                    invocation.id
                ),
                expected: &[
                    "+----------------------------------------+-------+-------------+------------+----------------+------------+-----------------+---------+-----------+-----------------+------------+----------------+--------------+",
                    "| id                                     | index | appended_at | entry_type | name           | raw_length | entry_lite_json | version | completed | sleep_wakeup_at | invoked_id | invoked_target | promise_name |",
                    "+----------------------------------------+-------+-------------+------------+----------------+------------+-----------------+---------+-----------+-----------------+------------+----------------+--------------+",
                    "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | 0     |             | Input      |                |            |                 | 1       |           |                 |            |                |              |",
                    "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | 1     |             | Run        | fixture-step-1 |            |                 | 1       |           |                 |            |                |              |",
                    "+----------------------------------------+-------+-------------+------------+----------------+------------+-----------------+---------+-----------+-----------------+------------+----------------+--------------+",
                ],
            },
            QueryExpectation {
                name: "journal events for an invocation",
                sql: &format!(
                    r#"SELECT
                           after_journal_entry_index,
                           appended_at,
                           event_type,
                           event_json
                       FROM sys_journal_events
                       WHERE id = '{}'
                       ORDER BY appended_at"#,
                    invocation.id
                ),
                expected: &[
                    "+---------------------------+--------------------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------+",
                    "| after_journal_entry_index | appended_at              | event_type     | event_json                                                                                                                                     |",
                    "+---------------------------+--------------------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------+",
                    "| 0                         | 1970-01-01T00:00:30.001Z | TransientError | {\"ty\":\"TransientError\",\"error_code\":500,\"error_message\":\"fixture failure 1\",\"related_command_index\":1,\"related_command_name\":\"fixture-step-1\"} |",
                    "| 1                         | 1970-01-01T00:00:31.001Z | Paused         | {\"ty\":\"Paused\"}                                                                                                                                |",
                    "+---------------------------+--------------------------+----------------+------------------------------------------------------------------------------------------------------------------------------------------------+",
                ],
            },
        ])
        .await;
}
