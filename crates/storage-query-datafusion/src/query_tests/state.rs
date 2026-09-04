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
async fn query_state_ui_shapes() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.state().populate_table(&[
            "+--------------+----------------------+---------+--------------+-------------+---------+---------+",
            "| partition_id | partition_key        | scope   | service_name | service_key | key     | value   |",
            "+--------------+----------------------+---------+--------------+-------------+---------+---------+",
            "| 0            | 3169317165037139997  | scope-a | TestService  | key-1       | state-1 | value-1 |",
            "| 0            | 3169317165037139997  | scope-a | TestService  | key-1       | state-2 | value-2 |",
            "| 1            | 6564637988134260717  | scope-j | TestService  | key-2       | state-1 | value-3 |",
            "| 2            | 16740507687615160162 | scope-b | OtherService | ignored-key | state-1 | value-4 |",
            "+--------------+----------------------+---------+--------------+-------------+---------+---------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "distinct scoped service instances",
        sql: r#"SELECT DISTINCT service_key, scope
                    FROM state
                    WHERE "service_name" = 'TestService'
                    LIMIT 2"#,
        expected: &[
            "+-------------+---------+",
            "| service_key | scope   |",
            "+-------------+---------+",
            "| key-1       | scope-a |",
            "| key-2       | scope-j |",
            "+-------------+---------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "state entries page",
        sql: r#"SELECT
                       key,
                       value_length,
                       CASE WHEN value_length <= 65536 THEN value END AS value
                   FROM state
                   WHERE service_name = 'TestService'
                     AND service_key = 'key-1'
                     AND scope = 'scope-a'
                   ORDER BY key
                   LIMIT 2"#,
        expected: &[
            "+---------+--------------+----------------+",
            "| key     | value_length | value          |",
            "+---------+--------------+----------------+",
            "| state-1 | 7            | 76616c75652d31 |",
            "| state-2 | 7            | 76616c75652d32 |",
            "+---------+--------------+----------------+",
        ],
    })
    .await;
}
