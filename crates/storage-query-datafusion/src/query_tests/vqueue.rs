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
            "| 0            | 3169317165037139997  | vq_12vxF4s3wljd1MRZAIZSLapQUxsh7uydHm | true            | TestService  | scope-a | tenant/eu | TestService/key-2          | 1744000010002 | 0         | 1           | 1             | 1          | 0            |",
            "| 1            | 6564637988134260717  | vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L | false           | OtherService | scope-j | tenant/eu | OtherService/ignored-key   | 1744000010003 | 1         | 0           | 0             | 0          | 1            |",
            "| 2            | 16740507687615160162 | vq_18rEacLHS3jy6eeuqDeGnv2Yi2Xdb7ZwxW | false           | TestService  | scope-b | tenant/eu | TestService/inactive-key   | 1744000010004 | 0         | 0           | 0             | 0          | 2            |",
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
            "| 1            | 6564637988134260717 | vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L | finished | failed      | true     | 1744000020000 | 6               | inv_1klS9KSVEL8v0bjX91PRpoIe9UR0aYIrF6 | invocation | 1744000015006 | 1744000016006   | 1            | 1          | 0          | 0               | 0          | 1744000015506    | 1744000015506     | 1744000020000     | dp_101SZwviYzes2mkYBx6TUys |",
            "| 2            | 16740507687615160162 | vq_18rEacLHS3jy6eeuqDeGnv2Yi2Xdb7ZwxW | finished | cancelled   | true     | 1744000020000 | 7               | inv_18rEacLHS3jy09qXCwwSQagbNB2POtVHIA | invocation | 1744000015007 | 1744000016007   | 1            | 0          | 0          | 0               | 0          | 1744000015507    | 1744000015507     | 1744000020000     | dp_101SZwviYzes2mkYBx6TUys |",
            "| 2            | 16740507687615160162 | vq_18rEacLHS3jy6eeuqDeGnv2Yi2Xdb7ZwxW | finished | killed      | true     | 1744000020000 | 8               | inv_18rEacLHS3jy03LZ30BX8sU4IDCkIZztT2 | invocation | 1744000015008 | 1744000016008   | 1            | 1          | 0          | 0               | 0          | 1744000015508    | 1744000015508     | 1744000020000     | dp_101SZwviYzes2mkYBx6TUys |",
            "+--------------+---------------------+---------------------------------------+----------+-------------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+----------------------------+",
        ])?;
        tables.sys_scheduler().populate_table(&[
            "+---------------------------------------+-----------------------------------------------+-----------------------------------------------+",
            "| column                                | blocked queue                                 | scheduled queue                               |",
            "+---------------------------------------+-----------------------------------------------+-----------------------------------------------+",
            "| partition_id                          | 0                                             | 1                                             |",
            "| partition_key                         | 3169317165037139997                           | 6564637988134260717                           |",
            "| id                                    | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR       | vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L       |",
            "| num_inbox                             | 2                                             | 1                                             |",
            "| status                                | blocked                                       | scheduled                                     |",
            "| head_entry_id                         | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw        | inv_1klS9KSVEL8v09qXCwwSQagbNB2POtVHIA        |",
            "| scheduled_at                          |                                               | 1744000020000                                 |",
            "| blocked_on                            | invoker-concurrency                           |                                               |",
            "| blocked_on_json                       | {\"resource\":\"invoker-concurrency\"}             |                                               |",
            "| invoker_concurrency_block_duration    | 10                                            | 0                                             |",
            "| throttling_rules_block_duration       | 20                                            | 0                                             |",
            "| invoker_throttling_block_duration     | 30                                            | 0                                             |",
            "| invoker_memory_block_duration         | 40                                            | 0                                             |",
            "| concurrency_rules_block_duration      | 50                                            | 0                                             |",
            "| lock_block_duration                   | 60                                            | 0                                             |",
            "| deployment_concurrency_block_duration | 70                                            | 0                                             |",
            "+---------------------------------------+-----------------------------------------------+-----------------------------------------------+",
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
            "| TestService  | 2     | 1       | 1         | 1      |",
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

    test.assert_query(QueryExpectation {
        name: "finished invocation summary",
        sql: r#"SELECT
                       v.status,
                       COUNT(1) AS count
                   FROM sys_vqueues v
                   WHERE v.stage = 'finished'
                     AND v.entry_kind = 'invocation'
                   GROUP BY v.status"#,
        expected: &[
            "+-----------+-------+",
            "| status    | count |",
            "+-----------+-------+",
            "| cancelled | 1     |",
            "| failed    | 1     |",
            "| killed    | 1     |",
            "| succeeded | 1     |",
            "+-----------+-------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "finished invocation candidates from vqueues",
        sql: r#"SELECT v.entry_id AS id
                   FROM sys_vqueues v
                   WHERE v.entry_kind = 'invocation'
                     AND v.stage = 'finished'
                     AND (
                       v.stage = 'finished'
                       AND v.status IN ('succeeded', 'failed', 'cancelled', 'killed')
                     )
                   LIMIT 4"#,
        expected: &[
            "+----------------------------------------+",
            "| id                                     |",
            "+----------------------------------------+",
            "| inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy |",
            "| inv_18rEacLHS3jy03LZ30BX8sU4IDCkIZztT2 |",
            "| inv_18rEacLHS3jy09qXCwwSQagbNB2POtVHIA |",
            "| inv_1klS9KSVEL8v0bjX91PRpoIe9UR0aYIrF6 |",
            "+----------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "inbox summary merges statuses from different remote partitions",
        sql: r#"SELECT status, COUNT(1) AS count
                   FROM sys_vqueues
                   WHERE stage = 'inbox'
                     AND entry_kind = 'invocation'
                   GROUP BY status"#,
        expected: &[
            "+-------------+-------+",
            "| status      | count |",
            "+-------------+-------+",
            "| backing-off | 1     |",
            "| new         | 1     |",
            "| scheduled   | 1     |",
            "+-------------+-------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "vqueue inbox entry position",
        sql: r#"SELECT position
                   FROM (
                     SELECT
                       entry_id,
                       ROW_NUMBER() OVER (
                         ORDER BY has_lock DESC, run_at ASC, sequence_number ASC, entry_id ASC
                       ) AS position
                     FROM sys_vqueues
                     WHERE id = 'vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR'
                       AND stage = 'inbox'
                   ) ranked
                   WHERE entry_id = 'inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2'"#,
        expected: &[
            "+----------+",
            "| position |",
            "+----------+",
            "| 2        |",
            "+----------+",
        ],
    })
    .await;

    test.assert_query_ordered(QueryExpectation {
        name: "scoped virtual-object inbox entries",
        sql: r#"SELECT
                       v.id AS vqueue_id,
                       v.entry_id AS id,
                       v.entry_kind AS kind,
                       v.stage,
                       v.status,
                       v.has_lock,
                       v.run_at,
                       v.sequence_number,
                       v.created_at,
                       v.transitioned_at
                   FROM sys_vqueues v
                   WHERE v.id IN (
                     SELECT vm.id
                     FROM sys_vqueue_meta vm
                     WHERE vm.service_name = 'TestService'
                       AND vm.lock_name = 'TestService/key-1'
                       AND vm.scope = 'scope-a'
                       AND vm.num_inbox > 0
                     LIMIT 1
                   )
                     AND v.stage = 'inbox'
                   ORDER BY
                     v.run_at ASC NULLS LAST,
                     v.sequence_number ASC,
                     v.entry_id ASC
                   LIMIT 2"#,
        expected: &[
            "+-----------------+------------------------------------------+------------------------------------------+",
            "| column          | row 1                                    | row 2                                    |",
            "+-----------------+------------------------------------------+------------------------------------------+",
            "| vqueue_id       | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR  | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR  |",
            "| id              | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2   |",
            "| kind            | invocation                               | invocation                               |",
            "| stage           | inbox                                    | inbox                                    |",
            "| status          | new                                      | backing-off                              |",
            "| has_lock        | true                                     | true                                     |",
            "| run_at          | 2025-04-07T04:27:00Z                     | 2025-04-07T04:27:00Z                     |",
            "| sequence_number | 1                                        | 2                                        |",
            "| created_at      | 2025-04-07T04:26:55.001Z                 | 2025-04-07T04:26:55.002Z                 |",
            "| transitioned_at | 2025-04-07T04:26:56.001Z                 | 2025-04-07T04:26:56.002Z                 |",
            "+-----------------+------------------------------------------+------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "virtual-object backlog from vqueue metadata",
        sql: r#"SELECT
                       lock_name,
                       scope,
                       SUM(num_inbox) AS backlog
                   FROM sys_vqueue_meta
                   WHERE partition_key IN (3169317165037139997)
                     AND service_name = 'TestService'
                     AND (
                       (lock_name = 'TestService/key-1' AND scope = 'scope-a')
                     )
                   GROUP BY lock_name, scope"#,
        expected: &[
            "+-------------------+---------+---------+",
            "| lock_name         | scope   | backlog |",
            "+-------------------+---------+---------+",
            "| TestService/key-1 | scope-a | 2       |",
            "+-------------------+---------+---------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "oldest inboxed virtual-object entry",
        sql: r#"SELECT MIN(v.transitioned_at) AS oldest_inboxed_at
                   FROM sys_vqueues v
                   WHERE v.id IN (
                     SELECT vm.id
                     FROM sys_vqueue_meta vm
                     WHERE vm.service_name = 'TestService'
                       AND vm.lock_name = 'TestService/key-1'
                       AND vm.scope = 'scope-a'
                       AND vm.num_inbox > 0
                   )
                     AND v.stage = 'inbox'"#,
        expected: &[
            "+--------------------------+",
            "| oldest_inboxed_at        |",
            "+--------------------------+",
            "| 2025-04-07T04:26:56.001Z |",
            "+--------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "vqueue entry statuses are joined back to invocation candidates by id",
        sql: r#"SELECT
                       v.entry_id,
                       v.vqueue_id,
                       v.stage,
                       v.status,
                       v.next_at,
                       v.created_at,
                       v.transitioned_at,
                       v.first_attempt_at,
                       v.latest_attempt_at,
                       v.first_runnable_at,
                       v.retry_attempts,
                       v.retry_count_since_last_stored_command,
                       v.num_attempts,
                       v.num_errors,
                       v.deployment
                   FROM sys_vqueue_entry_status v
                   WHERE v.entry_id IN (
                       'inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw',
                       'inv_1klS9KSVEL8v0bjX91PRpoIe9UR0aYIrF6',
                       'inv_18rEacLHS3jy01SZwviYzes2mjOamuMJWw'
                   )
                     AND v.entry_kind = 'invocation'"#,
        expected: &[
            "+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| column                                   | row 1                                    | row 2                                    |",
            "+------------------------------------------+------------------------------------------+------------------------------------------+",
            "| entry_id                                 | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   | inv_1klS9KSVEL8v0bjX91PRpoIe9UR0aYIrF6   |",
            "| vqueue_id                                | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR    | vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L    |",
            "| stage                                    | inbox                                    | finished                                 |",
            "| status                                   | new                                      | failed                                   |",
            "| next_at                                  | 2025-04-07T04:27:00Z                     | 2025-04-07T04:27:00Z                     |",
            "| created_at                               | 2025-04-07T04:26:55.001Z                 | 2025-04-07T04:26:55.006Z                 |",
            "| transitioned_at                          | 2025-04-07T04:26:56.001Z                 | 2025-04-07T04:26:56.006Z                 |",
            "| first_attempt_at                         |                                          | 2025-04-07T04:26:55.506Z                 |",
            "| latest_attempt_at                        |                                          | 2025-04-07T04:26:55.506Z                 |",
            "| first_runnable_at                        | 2025-04-07T04:27:00Z                     | 2025-04-07T04:27:00Z                     |",
            "| retry_attempts                           | 0                                        | 0                                        |",
            "| retry_count_since_last_stored_command    | 0                                        | 0                                        |",
            "| num_attempts                             | 0                                        | 1                                        |",
            "| num_errors                               | 0                                        | 1                                        |",
            "| deployment                               | dp_101SZwviYzes2mkYBx6TUys               | dp_101SZwviYzes2mkYBx6TUys               |",
            "+------------------------------------------+------------------------------------------+------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "active vqueue entry details exclude a finished candidate",
        sql: r#"SELECT
                       entry_id AS id,
                       entry_kind AS kind,
                       vqueue_id,
                       stage,
                       status,
                       has_lock,
                       next_at AS run_at,
                       sequence_number,
                       created_at,
                       transitioned_at,
                       first_attempt_at,
                       latest_attempt_at,
                       first_runnable_at,
                       retry_attempts,
                       retry_count_since_last_stored_command,
                       num_attempts,
                       num_errors,
                       deployment
                   FROM sys_vqueue_entry_status
                   WHERE entry_id IN (
                       'inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2',
                       'inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy'
                   )
                     AND stage <> 'finished'"#,
        expected: &[
            "+------------------------------------------+------------------------------------------+",
            "| column                                   | row 1                                    |",
            "+------------------------------------------+------------------------------------------+",
            "| id                                       | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2   |",
            "| kind                                     | invocation                               |",
            "| vqueue_id                                | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR    |",
            "| stage                                    | inbox                                    |",
            "| status                                   | backing-off                              |",
            "| has_lock                                 | true                                     |",
            "| run_at                                   | 2025-04-07T04:27:00Z                     |",
            "| sequence_number                          | 2                                        |",
            "| created_at                               | 2025-04-07T04:26:55.002Z                 |",
            "| transitioned_at                          | 2025-04-07T04:26:56.002Z                 |",
            "| first_attempt_at                         | 2025-04-07T04:26:55.502Z                 |",
            "| latest_attempt_at                        | 2025-04-07T04:26:55.502Z                 |",
            "| first_runnable_at                        | 2025-04-07T04:27:00Z                     |",
            "| retry_attempts                           | 0                                        |",
            "| retry_count_since_last_stored_command    | 0                                        |",
            "| num_attempts                             | 1                                        |",
            "| num_errors                               | 1                                        |",
            "| deployment                               | dp_101SZwviYzes2mkYBx6TUys               |",
            "+------------------------------------------+------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "focused vqueue entry exposes retry counters and every block duration",
        sql: r#"SELECT
                       e.entry_id,
                       e.vqueue_id,
                       e.stage,
                       e.status,
                       e.sequence_number,
                       e.created_at,
                       e.first_runnable_at,
                       e.first_attempt_at,
                       e.latest_attempt_at,
                       e.transitioned_at,
                       e.next_at,
                       e.retry_attempts,
                       e.retry_count_since_last_stored_command,
                       e.num_attempts,
                       e.num_errors,
                       e.num_suspensions,
                       e.num_pauses,
                       e.num_yields,
                       e.deployment,
                       e.total_blocked_on_invoker_concurrency,
                       e.total_blocked_on_throttling_rules,
                       e.total_blocked_on_invoker_throttling,
                       e.total_blocked_on_invoker_memory,
                       e.total_blocked_on_concurrency_rules,
                       e.total_blocked_on_lock,
                       e.total_blocked_on_deployment_concurrency,
                       e.latest_attempt_blocked_on_invoker_concurrency,
                       e.latest_attempt_blocked_on_throttling_rules,
                       e.latest_attempt_blocked_on_invoker_throttling,
                       e.latest_attempt_blocked_on_invoker_memory,
                       e.latest_attempt_blocked_on_concurrency_rules,
                       e.latest_attempt_blocked_on_lock,
                       e.latest_attempt_blocked_on_deployment_concurrency
                   FROM sys_vqueue_entry_status e
                   WHERE e.entry_id = 'inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2'
                     AND e.entry_kind = 'invocation'"#,
        expected: &[
            "+--------------------------------------------------+------------------------------------------+",
            "| column                                           | row 1                                    |",
            "+--------------------------------------------------+------------------------------------------+",
            "| entry_id                                         | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2   |",
            "| vqueue_id                                        | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR    |",
            "| stage                                            | inbox                                    |",
            "| status                                           | backing-off                              |",
            "| sequence_number                                  | 2                                        |",
            "| created_at                                       | 2025-04-07T04:26:55.002Z                 |",
            "| first_runnable_at                                | 2025-04-07T04:27:00Z                     |",
            "| first_attempt_at                                 | 2025-04-07T04:26:55.502Z                 |",
            "| latest_attempt_at                                | 2025-04-07T04:26:55.502Z                 |",
            "| transitioned_at                                  | 2025-04-07T04:26:56.002Z                 |",
            "| next_at                                          | 2025-04-07T04:27:00Z                     |",
            "| retry_attempts                                   | 0                                        |",
            "| retry_count_since_last_stored_command            | 0                                        |",
            "| num_attempts                                     | 1                                        |",
            "| num_errors                                       | 1                                        |",
            "| num_suspensions                                  | 0                                        |",
            "| num_pauses                                       | 0                                        |",
            "| num_yields                                       | 0                                        |",
            "| deployment                                       | dp_101SZwviYzes2mkYBx6TUys               |",
            "| total_blocked_on_invoker_concurrency             | 0 days 0 hours 0 mins 0.000 secs         |",
            "| total_blocked_on_throttling_rules                | 0 days 0 hours 0 mins 0.000 secs         |",
            "| total_blocked_on_invoker_throttling              | 0 days 0 hours 0 mins 0.000 secs         |",
            "| total_blocked_on_invoker_memory                  | 0 days 0 hours 0 mins 0.000 secs         |",
            "| total_blocked_on_concurrency_rules               | 0 days 0 hours 0 mins 0.000 secs         |",
            "| total_blocked_on_lock                            | 0 days 0 hours 0 mins 0.000 secs         |",
            "| total_blocked_on_deployment_concurrency          | 0 days 0 hours 0 mins 0.000 secs         |",
            "| latest_attempt_blocked_on_invoker_concurrency    | 0 days 0 hours 0 mins 0.000 secs         |",
            "| latest_attempt_blocked_on_throttling_rules       | 0 days 0 hours 0 mins 0.000 secs         |",
            "| latest_attempt_blocked_on_invoker_throttling     | 0 days 0 hours 0 mins 0.000 secs         |",
            "| latest_attempt_blocked_on_invoker_memory         | 0 days 0 hours 0 mins 0.000 secs         |",
            "| latest_attempt_blocked_on_concurrency_rules      | 0 days 0 hours 0 mins 0.000 secs         |",
            "| latest_attempt_blocked_on_lock                   | 0 days 0 hours 0 mins 0.000 secs         |",
            "| latest_attempt_blocked_on_deployment_concurrency | 0 days 0 hours 0 mins 0.000 secs         |",
            "+--------------------------------------------------+------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "workflow run stage reads its vqueue entry status",
        sql: r#"SELECT stage, transitioned_at, first_attempt_at, first_runnable_at
                   FROM sys_vqueue_entry_status
                   WHERE entry_id = 'inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2'
                     AND entry_kind = 'invocation'
                   LIMIT 1"#,
        expected: &[
            "+-------+--------------------------+--------------------------+----------------------+",
            "| stage | transitioned_at          | first_attempt_at         | first_runnable_at    |",
            "+-------+--------------------------+--------------------------+----------------------+",
            "| inbox | 2025-04-07T04:26:56.002Z | 2025-04-07T04:26:55.502Z | 2025-04-07T04:27:00Z |",
            "+-------+--------------------------+--------------------------+----------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "vqueue snapshot metadata preserves zero durations and nullable timestamps",
        sql: r#"SELECT
                       service_name,
                       scope,
                       lock_name,
                       limit_key,
                       created_at,
                       queue_is_paused,
                       num_inbox,
                       num_running,
                       num_suspended,
                       num_paused,
                       num_finished,
                       avg_inbox_duration,
                       avg_run_duration,
                       avg_suspension_duration,
                       avg_queue_duration,
                       avg_end_to_end_duration,
                       avg_blocked_on_concurrency_rules,
                       avg_blocked_on_invoker_concurrency,
                       avg_blocked_on_invoker_throttling,
                       avg_blocked_on_lock,
                       last_enqueued_at,
                       last_start_at,
                       last_attempt_at,
                       last_finish_at
                   FROM sys_vqueue_meta
                   WHERE id = 'vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR'"#,
        expected: &[
            "+------------------------------------+-------------------------------------+",
            "| column                             | row 1                               |",
            "+------------------------------------+-------------------------------------+",
            "| service_name                       | TestService                         |",
            "| scope                              | scope-a                             |",
            "| lock_name                          | TestService/key-1                   |",
            "| limit_key                          | tenant/eu                           |",
            "| created_at                         | 2025-04-07T04:26:50.001Z            |",
            "| queue_is_paused                    | false                               |",
            "| num_inbox                          | 2                                   |",
            "| num_running                        | 0                                   |",
            "| num_suspended                      | 0                                   |",
            "| num_paused                         | 0                                   |",
            "| num_finished                       | 1                                   |",
            "| avg_inbox_duration                 | 0 days 0 hours 0 mins 0.000 secs    |",
            "| avg_run_duration                   | 0 days 0 hours 0 mins 0.000 secs    |",
            "| avg_suspension_duration            | 0 days 0 hours 0 mins 0.000 secs    |",
            "| avg_queue_duration                 | 0 days 0 hours 0 mins 0.000 secs    |",
            "| avg_end_to_end_duration            | 0 days 0 hours 0 mins 0.000 secs    |",
            "| avg_blocked_on_concurrency_rules    | 0 days 0 hours 0 mins 0.000 secs    |",
            "| avg_blocked_on_invoker_concurrency  | 0 days 0 hours 0 mins 0.000 secs    |",
            "| avg_blocked_on_invoker_throttling   | 0 days 0 hours 0 mins 0.000 secs    |",
            "| avg_blocked_on_lock                 | 0 days 0 hours 0 mins 0.000 secs    |",
            "| last_enqueued_at                   | 2025-04-07T04:26:50.001Z            |",
            "| last_start_at                      |                                     |",
            "| last_attempt_at                    |                                     |",
            "| last_finish_at                     | 2025-04-07T04:26:50.001Z            |",
            "+------------------------------------+-------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "vqueue scheduler snapshot joins the live head to its stored entry status",
        sql: r#"SELECT
                       s.status,
                       s.blocked_on,
                       s.blocked_on_json,
                       s.head_entry_id,
                       s.scheduled_at,
                       s.invoker_concurrency_block_duration,
                       s.throttling_rules_block_duration,
                       s.invoker_throttling_block_duration,
                       s.invoker_memory_block_duration,
                       s.concurrency_rules_block_duration,
                       s.lock_block_duration,
                       s.deployment_concurrency_block_duration,
                       h.entry_id AS head_status_entry_id,
                       h.stage AS head_stage,
                       h.status AS head_status,
                       h.entry_kind AS head_kind,
                       h.transitioned_at AS head_transitioned_at,
                       h.next_at AS head_next_at,
                       h.created_at AS head_created_at,
                       h.sequence_number AS head_sequence_number,
                       h.retry_attempts AS head_retry_attempts,
                       h.num_attempts AS head_num_attempts,
                       h.num_errors AS head_num_errors,
                       h.num_suspensions AS head_num_suspensions,
                       h.num_pauses AS head_num_pauses,
                       h.num_yields AS head_num_yields,
                       h.deployment AS head_deployment,
                       h.has_lock AS head_has_lock,
                       h.total_blocked_on_invoker_concurrency AS head_total_blocked_on_invoker_concurrency,
                       h.total_blocked_on_throttling_rules AS head_total_blocked_on_throttling_rules,
                       h.total_blocked_on_invoker_throttling AS head_total_blocked_on_invoker_throttling,
                       h.total_blocked_on_invoker_memory AS head_total_blocked_on_invoker_memory,
                       h.total_blocked_on_concurrency_rules AS head_total_blocked_on_concurrency_rules,
                       h.total_blocked_on_lock AS head_total_blocked_on_lock,
                       h.total_blocked_on_deployment_concurrency AS head_total_blocked_on_deployment_concurrency
                   FROM sys_scheduler s
                   LEFT JOIN sys_vqueue_entry_status h
                     ON h.vqueue_id = s.id AND h.entry_id = s.head_entry_id
                   WHERE s.id = 'vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR'"#,
        expected: &[
            "+------------------------------------------------------+--------------------------------------------------+",
            "| column                                               | row 1                                            |",
            "+------------------------------------------------------+--------------------------------------------------+",
            "| status                                               | blocked                                          |",
            "| blocked_on                                           | invoker-concurrency                              |",
            "| blocked_on_json                                      | {\"resource\":\"invoker-concurrency\"}                |",
            "| head_entry_id                                        | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw           |",
            "| scheduled_at                                         |                                                  |",
            "| invoker_concurrency_block_duration                   | 0 days 0 hours 0 mins 0.010 secs                 |",
            "| throttling_rules_block_duration                      | 0 days 0 hours 0 mins 0.020 secs                 |",
            "| invoker_throttling_block_duration                    | 0 days 0 hours 0 mins 0.030 secs                 |",
            "| invoker_memory_block_duration                        | 0 days 0 hours 0 mins 0.040 secs                 |",
            "| concurrency_rules_block_duration                     | 0 days 0 hours 0 mins 0.050 secs                 |",
            "| lock_block_duration                                  | 0 days 0 hours 0 mins 0.060 secs                 |",
            "| deployment_concurrency_block_duration                | 0 days 0 hours 0 mins 0.070 secs                 |",
            "| head_status_entry_id                                 | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw           |",
            "| head_stage                                           | inbox                                            |",
            "| head_status                                          | new                                              |",
            "| head_kind                                            | invocation                                       |",
            "| head_transitioned_at                                 | 2025-04-07T04:26:56.001Z                         |",
            "| head_next_at                                         | 2025-04-07T04:27:00Z                             |",
            "| head_created_at                                      | 2025-04-07T04:26:55.001Z                         |",
            "| head_sequence_number                                 | 1                                                |",
            "| head_retry_attempts                                  | 0                                                |",
            "| head_num_attempts                                    | 0                                                |",
            "| head_num_errors                                      | 0                                                |",
            "| head_num_suspensions                                 | 0                                                |",
            "| head_num_pauses                                      | 0                                                |",
            "| head_num_yields                                      | 0                                                |",
            "| head_deployment                                      | dp_101SZwviYzes2mkYBx6TUys                       |",
            "| head_has_lock                                        | true                                             |",
            "| head_total_blocked_on_invoker_concurrency            | 0 days 0 hours 0 mins 0.000 secs                 |",
            "| head_total_blocked_on_throttling_rules               | 0 days 0 hours 0 mins 0.000 secs                 |",
            "| head_total_blocked_on_invoker_throttling             | 0 days 0 hours 0 mins 0.000 secs                 |",
            "| head_total_blocked_on_invoker_memory                 | 0 days 0 hours 0 mins 0.000 secs                 |",
            "| head_total_blocked_on_concurrency_rules              | 0 days 0 hours 0 mins 0.000 secs                 |",
            "| head_total_blocked_on_lock                           | 0 days 0 hours 0 mins 0.000 secs                 |",
            "| head_total_blocked_on_deployment_concurrency         | 0 days 0 hours 0 mins 0.000 secs                 |",
            "+------------------------------------------------------+--------------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "scheduler states preserve blocked and scheduled nullability",
        sql: r#"SELECT
                       id,
                       status,
                       blocked_on,
                       blocked_on_json,
                       head_entry_id,
                       scheduled_at,
                       invoker_concurrency_block_duration,
                       throttling_rules_block_duration,
                       invoker_throttling_block_duration,
                       invoker_memory_block_duration,
                       concurrency_rules_block_duration,
                       lock_block_duration,
                       deployment_concurrency_block_duration
                   FROM sys_scheduler
                   WHERE id IN (
                       'vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR',
                       'vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L'
                   )"#,
        expected: &[
            "+---------------------------------------+------------------------------------------+------------------------------------------+",
            "| column                                | blocked                                  | scheduled                                |",
            "+---------------------------------------+------------------------------------------+------------------------------------------+",
            "| id                                    | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR   | vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L   |",
            "| status                                | blocked                                  | scheduled                                |",
            "| blocked_on                            | invoker-concurrency                      |                                          |",
            "| blocked_on_json                       | {\"resource\":\"invoker-concurrency\"}    |                                          |",
            "| head_entry_id                         | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw   | inv_1klS9KSVEL8v09qXCwwSQagbNB2POtVHIA   |",
            "| scheduled_at                          |                                          | 2025-04-07T04:27:00Z                     |",
            "| invoker_concurrency_block_duration    | 0 days 0 hours 0 mins 0.010 secs         | 0 days 0 hours 0 mins 0.000 secs         |",
            "| throttling_rules_block_duration       | 0 days 0 hours 0 mins 0.020 secs         | 0 days 0 hours 0 mins 0.000 secs         |",
            "| invoker_throttling_block_duration     | 0 days 0 hours 0 mins 0.030 secs         | 0 days 0 hours 0 mins 0.000 secs         |",
            "| invoker_memory_block_duration         | 0 days 0 hours 0 mins 0.040 secs         | 0 days 0 hours 0 mins 0.000 secs         |",
            "| concurrency_rules_block_duration      | 0 days 0 hours 0 mins 0.050 secs         | 0 days 0 hours 0 mins 0.000 secs         |",
            "| lock_block_duration                   | 0 days 0 hours 0 mins 0.060 secs         | 0 days 0 hours 0 mins 0.000 secs         |",
            "| deployment_concurrency_block_duration | 0 days 0 hours 0 mins 0.070 secs         | 0 days 0 hours 0 mins 0.000 secs         |",
            "+---------------------------------------+------------------------------------------+------------------------------------------+",
        ],
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "vqueue entry status lookup ignores unknown ids",
        sql: r#"SELECT entry_id, vqueue_id, stage, status
                   FROM sys_vqueue_entry_status
                   WHERE entry_id = 'inv_18rEacLHS3jy01SZwviYzes2mjOamuMJWw'
                     AND entry_kind = 'invocation'"#,
        expected: &[
            "+----------+-----------+-------+--------+",
            "| entry_id | vqueue_id | stage | status |",
            "+----------+-----------+-------+--------+",
            "+----------+-----------+-------+--------+",
        ],
    })
    .await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_vqueue_position_honors_lock_priority_and_tie_breakers() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_vqueues().populate_table(&[
            "+--------------+---------------------+---------------------------------------+-------+--------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+------------+",
            "| partition_id | partition_key       | id                                    | stage | status | has_lock | run_at        | sequence_number | entry_id                               | entry_kind | created_at    | transitioned_at | num_attempts | num_errors | num_pauses | num_suspensions | num_yields | first_attempt_at | latest_attempt_at | first_runnable_at | deployment |",
            "+--------------+---------------------+---------------------------------------+-------+--------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+------------+",
            "| 0            | 3169317165037139997 | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | inbox | new    | false    | 1744000010000 | 1               | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | invocation | 1744000000001 | 1744000001001   | 0            | 0          | 0          | 0               | 0          |                  |                   | 1744000010000     |            |",
            "| 0            | 3169317165037139997 | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | inbox | new    | true     | 1744000020000 | 1               | inv_12vxF4s3wljd03LZ30BX8sU4IDCkIZztT2 | invocation | 1744000000002 | 1744000001002   | 0            | 0          | 0          | 0               | 0          |                  |                   | 1744000020000     |            |",
            "| 0            | 3169317165037139997 | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | inbox | new    | true     | 1744000020000 | 1               | inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | invocation | 1744000000003 | 1744000001003   | 0            | 0          | 0          | 0               | 0          |                  |                   | 1744000020000     |            |",
            "+--------------+---------------------+---------------------------------------+-------+--------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+------------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "vqueue position ranks locked entries before an earlier unlocked entry",
        sql: r#"SELECT position
                   FROM (
                     SELECT
                       entry_id,
                       ROW_NUMBER() OVER (
                         ORDER BY has_lock DESC, run_at ASC, sequence_number ASC, entry_id ASC
                       ) AS position
                     FROM sys_vqueues
                     WHERE id = 'vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR'
                       AND stage = 'inbox'
                   ) ranked
                   WHERE entry_id = 'inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw'"#,
        expected: &[
            "+----------+",
            "| position |",
            "+----------+",
            "| 3        |",
            "+----------+",
        ],
    })
    .await;
}
