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
async fn query_drained_deployments_ui_shape() {
    let mut test = QueryTest::create_remote().await;
    test.populate(|tables| {
        tables.sys_deployment().populate_table(&[
            "+----------------------------+-------------------------+------------+",
            "| id                         | endpoint                | created_at |",
            "+----------------------------+-------------------------+------------+",
            "| dp_101SZwviYzes2mkYBx6TUys | http://active-service/  | 1000       |",
            "| dp_11nGQpCRmau6ypL82KH2TnP | http://active-vqueue/   | 2000       |",
            "| dp_15VqmTOnXH3Vv2pl5HOG7UB | http://drained/         | 3000       |",
            "+----------------------------+-------------------------+------------+",
        ])?;
        tables.sys_service().populate_table(&[
            "+---------------+---------+----------------------------+",
            "| name          | ty      | deployment_id              |",
            "+---------------+---------+----------------------------+",
            "| ActiveService | service | dp_101SZwviYzes2mkYBx6TUys |",
            "+---------------+---------+----------------------------+",
        ])?;
        tables.sys_vqueues().populate_table(&[
            "+--------------+---------------------+---------------------------------------+----------+-----------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+----------------------------+",
            "| partition_id | partition_key       | id                                    | stage    | status    | has_lock | run_at        | sequence_number | entry_id                               | entry_kind | created_at    | transitioned_at | num_attempts | num_errors | num_pauses | num_suspensions | num_yields | first_attempt_at | latest_attempt_at | first_runnable_at | deployment                 |",
            "+--------------+---------------------+---------------------------------------+----------+-----------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+----------------------------+",
            "| 0            | 3169317165037139997 | vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | running  | started   | true     | 1744000020000 | 1               | inv_12vxF4s3wljd01SZwviYzes2mjOamuMJWw | invocation | 1744000015001 | 1744000016001   | 1            | 0          | 0          | 0               | 0          | 1744000015501    | 1744000015501     | 1744000020000     | dp_11nGQpCRmau6ypL82KH2TnP |",
            "| 1            | 6564637988134260717 | vq_1klS9KSVEL8v26a53iN5aIaYj240f03U9L | finished | succeeded | true     | 1744000020000 | 2               | inv_1klS9KSVEL8v03LZ30BX8sU4IDCkIZztT2 | invocation | 1744000015002 | 1744000016002   | 1            | 0          | 0          | 0               | 0          | 1744000015502    | 1744000015502     | 1744000020000     | dp_15VqmTOnXH3Vv2pl5HOG7UB |",
            "+--------------+---------------------+---------------------------------------+----------+-----------+----------+---------------+-----------------+----------------------------------------+------------+---------------+-----------------+--------------+------------+------------+-----------------+------------+------------------+-------------------+-------------------+----------------------------+",
        ])?;
        Ok(())
    })
    .await;

    test.assert_query(QueryExpectation {
        name: "drained deployments exclude services and unfinished vqueue entries",
        sql: r#"WITH active AS (
                     SELECT deployment_id AS id
                     FROM sys_service
                     WHERE deployment_id IS NOT NULL
                     UNION
                     SELECT deployment AS id
                     FROM sys_vqueues
                     WHERE entry_kind = 'invocation'
                       AND stage != 'finished'
                       AND deployment IS NOT NULL
                   )
                   SELECT id FROM sys_deployment
                   EXCEPT
                   SELECT id FROM active"#,
        expected: &[
            "+----------------------------+",
            "| id                         |",
            "+----------------------------+",
            "| dp_15VqmTOnXH3Vv2pl5HOG7UB |",
            "+----------------------------+",
        ],
    })
    .await;
}
