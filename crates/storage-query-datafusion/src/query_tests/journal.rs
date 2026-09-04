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
async fn query_journal_ui_shapes() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_journal().populate_table(&[
            "+---------------+------------------------------------------+------------------------------------------+",
            "| column        | row 1                                    | row 2                                    |",
            "+---------------+------------------------------------------+------------------------------------------+",
            "| partition_id  | 1                                        | 1                                        |",
            "| partition_key | 6564637988134260717                      | 6564637988134260717                      |",
            "| id            | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw   | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw   |",
            "| index         | 0                                        | 1                                        |",
            "| version       | 2                                        | 2                                        |",
            "| appended_at   | 20000                                    | 21000                                    |",
            "| entry_type    | Input                                    | Run                                      |",
            "| name          | input                                    | fixture-step-1                           |",
            "| value         | fixture-input                            |                                          |",
            "| completion_id |                                          | 1                                        |",
            "+---------------+------------------------------------------+------------------------------------------+",
        ])?;
        tables.sys_journal_events().populate_table(&[
            "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| column                    | row 1                                                                                                                                            | row 2                    | row 3                                                                                                                                            |",
            "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| partition_id              | 1                                                                                                                                                | 1                        | 1                                                                                                                                                |",
            "| partition_key             | 6564637988134260717                                                                                                                              | 6564637988134260717      | 6564637988134260717                                                                                                                              |",
            "| id                        | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw                                                                                                             | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw                                                                                                             |",
            "| after_journal_entry_index | 0                                                                                                                                                | 1                        | 1                                                                                                                                                |",
            "| appended_at               | 30001                                                                                                                                            | 31001                    | 32001                                                                                                                                            |",
            "| event_type                | TransientError                                                                                                                                   | Paused                   | TransientError                                                                                                                                   |",
            "| event_json                | {\"ty\":\"TransientError\",\"error_code\":500,\"error_message\":\"fixture failure 1\",\"related_command_index\":1,\"related_command_name\":\"fixture-step-1\"} | {\"ty\":\"Paused\"}          | {\"ty\":\"TransientError\",\"error_code\":503,\"error_message\":\"fixture failure 2\",\"related_command_index\":1,\"related_command_name\":\"fixture-step-1\"} |",
            "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query_ordered(QueryExpectation {
        name: "journal entries for an invocation",
        sql: r#"SELECT
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
                   WHERE id = 'inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw'
                   ORDER BY index"#,
        expected: &[
            "+-----------------+----------------------------------------+-----------------------------------------------------------------+",
            "| column          | row 1                                  | row 2                                                           |",
            "+-----------------+----------------------------------------+-----------------------------------------------------------------+",
            "| id              | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw                          |",
            "| index           | 0                                      | 1                                                               |",
            "| appended_at     | 1970-01-01T00:00:20Z                   | 1970-01-01T00:00:21Z                                            |",
            "| entry_type      | Command: Input                         | Command: Run                                                    |",
            "| name            | input                                  | fixture-step-1                                                  |",
            "| raw_length      | 24                                     | 18                                                              |",
            "| entry_lite_json | {\"Command\":{\"Input\":{}}}               | {\"Command\":{\"Run\":{\"completion_id\":1,\"name\":\"fixture-step-1\"}}} |",
            "| version         | 2                                      | 2                                                               |",
            "| completed       |                                        |                                                                 |",
            "| sleep_wakeup_at |                                        |                                                                 |",
            "| invoked_id      |                                        |                                                                 |",
            "| invoked_target  |                                        |                                                                 |",
            "| promise_name    |                                        |                                                                 |",
            "+-----------------+----------------------------------------+-----------------------------------------------------------------+",
        ],
    })
    .await;

    test.assert_query_ordered(QueryExpectation {
        name: "journal events for an invocation",
        sql: r#"SELECT
                       after_journal_entry_index,
                       appended_at,
                       event_type,
                       event_json
                   FROM sys_journal_events
                   WHERE id = 'inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw'
                   ORDER BY appended_at"#,
        expected: &[
            "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| column                    | row 1                                                                                                                                            | row 2                    | row 3                                                                                                                                            |",
            "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| after_journal_entry_index | 0                                                                                                                                                | 1                        | 1                                                                                                                                                |",
            "| appended_at               | 1970-01-01T00:00:30.001Z                                                                                                                         | 1970-01-01T00:00:31.001Z | 1970-01-01T00:00:32.001Z                                                                                                                         |",
            "| event_type                | TransientError                                                                                                                                   | Paused                   | TransientError                                                                                                                                   |",
            "| event_json                | {\"ty\":\"TransientError\",\"error_code\":500,\"error_message\":\"fixture failure 1\",\"related_command_index\":1,\"related_command_name\":\"fixture-step-1\"} | {\"ty\":\"Paused\"}          | {\"ty\":\"TransientError\",\"error_code\":503,\"error_message\":\"fixture failure 2\",\"related_command_index\":1,\"related_command_name\":\"fixture-step-1\"} |",
            "+---------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "latest transient journal error",
        sql: r#"SELECT id, appended_at, event_type, event_json
                   FROM sys_journal_events
                   WHERE id = 'inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw'
                     AND event_type = 'TransientError'
                   ORDER BY appended_at DESC
                   LIMIT 1"#,
        expected: &[
            "+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| column      | row 1                                                                                                                                            |",
            "+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| id          | inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw                                                                                                             |",
            "| appended_at | 1970-01-01T00:00:32.001Z                                                                                                                         |",
            "| event_type  | TransientError                                                                                                                                   |",
            "| event_json  | {\"ty\":\"TransientError\",\"error_code\":503,\"error_message\":\"fixture failure 2\",\"related_command_index\":1,\"related_command_name\":\"fixture-step-1\"} |",
            "+-------------+--------------------------------------------------------------------------------------------------------------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "journal entry payload",
        sql: r#"SELECT entry_type, entry_json, raw
                   FROM sys_journal
                   WHERE id = 'inv_1klS9KSVEL8v01SZwviYzes2mjOamuMJWw'
                     AND index = 1"#,
        expected: &[
            "+--------------+-----------------------------------------------------------------+--------------------------------------+",
            "| entry_type   | entry_json                                                      | raw                                  |",
            "+--------------+-----------------------------------------------------------------+--------------------------------------+",
            "| Command: Run | {\"Command\":{\"Run\":{\"completion_id\":1,\"name\":\"fixture-step-1\"}}} | 5801620e666978747572652d737465702d31 |",
            "+--------------+-----------------------------------------------------------------+--------------------------------------+",
        ],
    })
    .await;
}
