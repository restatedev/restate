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
async fn query_vqueue_ui_shapes() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_vqueue_meta().populate_table(&[
            "+--------------+----------------------+---------------------------------------+-----------------+--------------+---------+-----------+----------------------------+---------------+-----------+-------------+---------------+------------+--------------+",
            "| partition_id | partition_key        | id                                    | queue_is_paused | service_name | scope   | limit_key | lock_name                  | created_at    | num_inbox | num_running | num_suspended | num_paused | num_finished |",
            "+--------------+----------------------+---------------------------------------+-----------------+--------------+---------+-----------+----------------------------+---------------+-----------+-------------+---------------+------------+--------------+",
            "| 0            | 3169317165037139997  | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | false           | TestService  | scope-a | tenant/eu | TestService/key-1          | 1744000010001 | 2         | 0           | 0             | 0          | 1            |",
            "| 0            | 3169317165037139997  | vq_12vxF4s3wljd1MRZAIZSLapQUxsh7uydHm | false           | TestService  | scope-a | tenant/eu | TestService/key-2          | 1744000010002 | 0         | 1           | 0             | 0          | 0            |",
            "| 1            | 6564637988134260717  | vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L | false           | OtherService | scope-j | tenant/eu | OtherService/ignored-key   | 1744000010003 | 1         | 0           | 0             | 0          | 0            |",
            "| 2            | 16740507687615160162 | vq_18rEacLHS3jy6eeuqDeGnv2Yi2Xdb7ZwxW | false           | TestService  | scope-b | tenant/eu | TestService/inactive-key   | 1744000010004 | 0         | 0           | 0             | 0          | 0            |",
            "+--------------+----------------------+---------------------------------------+-----------------+--------------+---------+-----------+----------------------------+---------------+-----------+-------------+---------------+------------+--------------+",
        ])?;
        tables.sys_vqueues().populate_table(&[
            "+--------------+---------------------+---------------------------------------+----------+-------------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+----------------------------+",
            "| partition_id | partition_key       | id                                    | stage    | status      | has_lock | run_at        | sequence_number | entry_id                               | entry_kind | created_at    | transitioned_at | num_attempts | num_errors | num_pauses | num_suspensions | num_yields | first_attempt_at | latest_attempt_at | first_runnable_at | deployment                 |",
            "+--------------+---------------------+---------------------------------------+----------+-------------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+----------------------------+",
            "| 0            | 3169317165037139997 | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | inbox    | new         | true     | 1744000020000 | 1               | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | invocation | 1744000015001 | 1744000016001   | 0            | 0          | 0          | 0               | 0          |                  |                   | 1744000020000     | dp_101SZwviYzes2mkYBx6TUys |",
            "| 0            | 3169317165037139997 | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | inbox    | backing-off | true     | 1744000020000 | 2               | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 | invocation | 1744000015002 | 1744000016002   | 1            | 1          | 0          | 0               | 0          | 1744000015502    | 1744000015502     | 1744000020000     | dp_101SZwviYzes2mkYBx6TUys |",
            "| 0            | 3169317165037139997 | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | finished | succeeded   | true     | 1744000020000 | 3               | inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | invocation | 1744000015003 | 1744000016003   | 1            | 0          | 0          | 0               | 0          | 1744000015503    | 1744000015503     | 1744000020000     | dp_101SZwviYzes2mkYBx6TUys |",
            "| 0            | 3169317165037139997 | vq_12vxF4s3wljd1MRZAIZSLapQUxsh7uydHm | running  | started     | true     | 1744000020000 | 4               | inv_12vxF4s3wljd07xY61dUgVO9rheFrZ8XM4 | invocation | 1744000015004 | 1744000016004   | 1            | 0          | 0          | 0               | 0          | 1744000015504    | 1744000015504     | 1744000020000     | dp_101SZwviYzes2mkYBx6TUys |",
            "| 1            | 6564637988134260717 | vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L | inbox    | scheduled   | true     | 1744000020000 | 5               | inv_1klS9KSVEL8v09qXCwwSQagbNB2POtVHIA | invocation | 1744000015005 | 1744000016005   | 0            | 0          | 0          | 0               | 0          |                  |                   | 1744000020000     | dp_101SZwviYzes2mkYBx6TUys |",
            "+--------------+---------------------+---------------------------------------+----------+-------------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+----------------------------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "inbox statuses for selected services",
        sql: r#"SELECT
                           v.status,
                           COUNT(1) AS count
                       FROM sys_vqueues v
                       WHERE v.id IN (
                           SELECT vm.id
                           FROM sys_vqueue_meta vm
                           WHERE vm.service_name IN ('TestService')
                             AND vm.num_inbox > 0
                           LIMIT 1
                       )
                         AND v.stage = 'inbox'
                         AND v.entry_kind = 'invocation'
                       GROUP BY v.status"#,
        expected: &[
            "+-------------+-------+",
            "| status      | count |",
            "+-------------+-------+",
            "| backing-off | 1     |",
            "| new         | 1     |",
            "+-------------+-------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "active virtual-object identities from vqueue metadata",
        sql: r#"SELECT DISTINCT
                           CAST(partition_key AS VARCHAR) AS partition_key,
                           lock_name,
                           scope
                       FROM sys_vqueue_meta
                       WHERE service_name = 'TestService'
                         AND lock_name IS NOT NULL
                         AND (
                           num_inbox > 0
                           OR num_running > 0
                           OR num_suspended > 0
                           OR num_paused > 0
                         )
                         AND scope = 'scope-a'
                       LIMIT 2"#,
        expected: &[
            "+---------------------+-------------------+---------+",
            "| partition_key       | lock_name         | scope   |",
            "+---------------------+-------------------+---------+",
            "| 3169317165037139997 | TestService/key-1 | scope-a |",
            "| 3169317165037139997 | TestService/key-2 | scope-a |",
            "+---------------------+-------------------+---------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "vqueue metadata summary",
        sql: r#"SELECT
                           vm.service_name,
                           SUM(vm.num_inbox) AS inbox,
                           SUM(vm.num_running) AS running,
                           SUM(vm.num_suspended) AS suspended,
                           SUM(vm.num_paused) AS paused
                       FROM sys_vqueue_meta vm
                       WHERE vm.num_inbox > 0
                          OR vm.num_running > 0
                          OR vm.num_suspended > 0
                          OR vm.num_paused > 0
                       GROUP BY vm.service_name"#,
        expected: &[
            "+--------------+-------+---------+-----------+--------+",
            "| service_name | inbox | running | suspended | paused |",
            "+--------------+-------+---------+-----------+--------+",
            "| OtherService | 1     | 0       | 0         | 0      |",
            "| TestService  | 2     | 1       | 0         | 0      |",
            "+--------------+-------+---------+-----------+--------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "finished entries for a vqueue stage",
        sql: r#"SELECT
                           id AS vqueue_id,
                           entry_id AS id,
                           entry_kind AS kind,
                           stage,
                           status,
                           has_lock,
                           sequence_number,
                           created_at,
                           transitioned_at,
                           first_runnable_at,
                           first_attempt_at,
                           latest_attempt_at,
                           num_attempts,
                           num_errors,
                           num_pauses,
                           num_suspensions,
                           num_yields,
                           deployment
                       FROM sys_vqueues
                       WHERE id = 'vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR'
                         AND stage = 'finished'
                       LIMIT 1"#,
        expected: &[
            "+---------------------------------------+----------------------------------------+------------+----------+-----------+----------+-----------------+--------------------------+--------------------------+----------------------+--------------------------+--------------------------+--------------+------------+------------+-----------------+------------+----------------------------+",
            "| vqueue_id                             | id                                     | kind       | stage    | status    | has_lock | sequence_number | created_at               | transitioned_at          | first_runnable_at    | first_attempt_at         | latest_attempt_at        | num_attempts | num_errors | num_pauses | num_suspensions | num_yields | deployment                 |",
            "+---------------------------------------+----------------------------------------+------------+----------+-----------+----------+-----------------+--------------------------+--------------------------+----------------------+--------------------------+--------------------------+--------------+------------+------------+-----------------+------------+----------------------------+",
            "| vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | invocation | finished | succeeded | true     | 3               | 2025-04-07T04:26:55.003Z | 2025-04-07T04:26:56.003Z | 2025-04-07T04:27:00Z | 2025-04-07T04:26:55.503Z | 2025-04-07T04:26:55.503Z | 1            | 0          | 0          | 0               | 0          | dp_101SZwviYzes2mkYBx6TUys |",
            "+---------------------------------------+----------------------------------------+------------+----------+-----------+----------+-----------------+--------------------------+--------------------------+----------------------+--------------------------+--------------------------+--------------+------------+------------+-----------------+------------+----------------------------+",
        ],
    })
    .await;
}
