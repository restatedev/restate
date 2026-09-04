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
use super::harness::{QueryExpectation, QueryTest};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_journal_ui_shapes() {
    let mut factory = FixtureFactory::default();
    let invocation = factory.create_invocation(InvocationOptions::default());

    let mut test = QueryTest::create().await;
    test.populate(|tables| {
        tables.sys_journal().populate(&invocation)?;
        tables.sys_journal_events().populate(&invocation)?;
        Ok(())
    })
    .await;

    test
        .assert_query(QueryExpectation {
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
                "+-----------------+------------------------------------------+------------------------------------------+",
                "| column          | row 1                                    | row 2                                    |",
                "+-----------------+------------------------------------------+------------------------------------------+",
                "| id              | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   |",
                "| index           | 0                                        | 1                                        |",
                "| appended_at     |                                          |                                          |",
                "| entry_type      | Input                                    | Run                                      |",
                "| name            |                                          | fixture-step-1                           |",
                "| raw_length      |                                          |                                          |",
                "| entry_lite_json |                                          |                                          |",
                "| version         | 1                                        | 1                                        |",
                "| completed       |                                          |                                          |",
                "| sleep_wakeup_at |                                          |                                          |",
                "| invoked_id      |                                          |                                          |",
                "| invoked_target  |                                          |                                          |",
                "| promise_name    |                                          |                                          |",
                "+-----------------+------------------------------------------+------------------------------------------+",
            ],
        })
        .await;

    test
        .assert_query(QueryExpectation {
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
                "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+",
                "| column                    | row 1                                                                                                                                            | row 2                    |",
                "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+",
                "| after_journal_entry_index | 0                                                                                                                                                | 1                        |",
                "| appended_at               | 1970-01-01T00:00:30.001Z                                                                                                                         | 1970-01-01T00:00:31.001Z |",
                "| event_type                | TransientError                                                                                                                                   | Paused                   |",
                "| event_json                | {\"ty\":\"TransientError\",\"error_code\":500,\"error_message\":\"fixture failure 1\",\"related_command_index\":1,\"related_command_name\":\"fixture-step-1\"} | {\"ty\":\"Paused\"}          |",
                "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+",
            ],
        })
        .await;
}
