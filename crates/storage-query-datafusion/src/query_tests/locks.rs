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
async fn query_locks_ui_shapes() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_locks().populate_table(&[
            "+--------------+----------------------+---------+-------------------+---------------+----------------------------------------+",
            "| partition_id | partition_key        | scope   | lock_name         | acquired_at   | acquired_by                            |",
            "+--------------+----------------------+---------+-------------------+---------------+----------------------------------------+",
            "| 0            | 3169317165037139997  | scope-a | TestService/key-1 | 1744000010001 | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw |",
            "| 1            | 6564637988134260717  | scope-j | TestService/key-2 | 1744000010002 | inv_1klS9KSVEL8v07xY61dUgVO9rheFrZ8XM4 |",
            "| 2            | 16740507687615160162 | scope-b | TestService/key-3 | 1744000010003 | inv_18rEacLHS3jy05EYzvUVHHm74Xqv5umdPy |",
            "+--------------+----------------------+---------+-------------------+---------------+----------------------------------------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "virtual-object lock lookup",
        sql: r#"SELECT acquired_by, acquired_at
                   FROM sys_locks
                   WHERE lock_name = 'TestService/key-1'
                     AND scope = 'scope-a'
                     AND acquired_by IS NOT NULL
                   LIMIT 1"#,
        expected: &[
            "+----------------------------------------+--------------------------+",
            "| acquired_by                            | acquired_at              |",
            "+----------------------------------------+--------------------------+",
            "| inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | 2025-04-07T04:26:50.001Z |",
            "+----------------------------------------+--------------------------+",
        ],
    })
    .await;
}
