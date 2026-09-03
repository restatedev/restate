// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::vqueue_table::{Stage, Status};

use super::data::{FixtureFactory, InvocationOptions, VQueueOptions};
use super::fixture::{QueryExpectation, QueryFixture};

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_vqueue_ui_shapes() {
    let mut factory = FixtureFactory::default();
    let vqueue_1 = factory.create_vqueue_with(VQueueOptions {
        service_key: Some("key-1"),
        ..VQueueOptions::default()
    });
    let vqueue_2 = factory.create_vqueue_with(VQueueOptions {
        service_key: Some("key-2"),
        ..VQueueOptions::default()
    });
    let other_vqueue = factory.create_vqueue_with(VQueueOptions {
        service_name: "OtherService",
        service_key: Some("ignored-key"),
        ..VQueueOptions::default()
    });
    let inactive_vqueue = factory.create_vqueue_with(VQueueOptions {
        service_key: Some("inactive-key"),
        ..VQueueOptions::default()
    });

    let invocation_1 = factory.create_invocation(InvocationOptions {
        vqueue: Some(&vqueue_1),
        service_key: "key-1",
        entry_stage: Stage::Inbox,
        entry_status: Status::New,
        ..InvocationOptions::default()
    });
    let invocation_2 = factory.create_invocation(InvocationOptions {
        vqueue: Some(&vqueue_1),
        service_key: "key-1",
        entry_stage: Stage::Inbox,
        entry_status: Status::BackingOff,
        ..InvocationOptions::default()
    });
    let finished_invocation = factory.create_invocation(InvocationOptions {
        vqueue: Some(&vqueue_1),
        service_key: "key-1",
        entry_stage: Stage::Finished,
        entry_status: Status::Succeeded,
        ..InvocationOptions::default()
    });
    let running_invocation = factory.create_invocation(InvocationOptions {
        vqueue: Some(&vqueue_2),
        service_key: "key-2",
        entry_stage: Stage::Running,
        entry_status: Status::Started,
        ..InvocationOptions::default()
    });
    let other_invocation = factory.create_invocation(InvocationOptions {
        vqueue: Some(&other_vqueue),
        service_name: "OtherService",
        service_key: "ignored-key",
        entry_stage: Stage::Inbox,
        entry_status: Status::Scheduled,
        ..InvocationOptions::default()
    });
    let invocations = [
        &invocation_1,
        &invocation_2,
        &finished_invocation,
        &running_invocation,
        &other_invocation,
    ];
    let vqueues = [&vqueue_1, &vqueue_2, &other_vqueue, &inactive_vqueue];

    let mut fixture = QueryFixture::create().await;
    fixture
        .populate(|tables| {
            for vqueue in vqueues {
                tables.sys_vqueue_meta().populate(vqueue);
            }
            for invocation in invocations {
                tables.sys_vqueues().populate(invocation)?;
            }
            Ok(())
        })
        .await;

    fixture
        .assert_queries(&[
            QueryExpectation {
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
            },
            QueryExpectation {
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
            },
            QueryExpectation {
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
            },
            QueryExpectation {
                name: "finished entries for a vqueue stage",
                sql: &format!(
                    r#"SELECT
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
                       WHERE id = '{}'
                         AND stage = 'finished'
                       LIMIT 1"#,
                    vqueue_1.id
                ),
                expected: &[
                    "+---------------------------------------+----------------------------------------+------------+----------+-----------+----------+-----------------+--------------------------+--------------------------+----------------------+--------------------------+--------------------------+--------------+------------+------------+-----------------+------------+----------------------------+",
                    "| vqueue_id                             | id                                     | kind       | stage    | status    | has_lock | sequence_number | created_at               | transitioned_at          | first_runnable_at    | first_attempt_at         | latest_attempt_at        | num_attempts | num_errors | num_pauses | num_suspensions | num_yields | deployment                 |",
                    "+---------------------------------------+----------------------------------------+------------+----------+-----------+----------+-----------------+--------------------------+--------------------------+----------------------+--------------------------+--------------------------+--------------+------------+------------+-----------------+------------+----------------------------+",
                    "| vq_12vxF4s3wljd5JP76hKfp3btXyT36rz5xR | inv_12vxF4s3wljd05EYzvUVHHm74Xqv5umdPy | invocation | finished | succeeded | true     | 3               | 2025-04-07T04:26:55.003Z | 2025-04-07T04:26:56.003Z | 2025-04-07T04:27:00Z | 2025-04-07T04:26:55.503Z | 2025-04-07T04:26:55.503Z | 1            | 0          | 0          | 0               | 0          | dp_101SZwviYzes2mkYBx6TUys |",
                    "+---------------------------------------+----------------------------------------+------------+----------+-----------+----------+-----------------+--------------------------+--------------------------+----------------------+--------------------------+--------------------------+--------------+------------+------------+-----------------+------------+----------------------------+",
                ],
            },
        ])
        .await;
}
