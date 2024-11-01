// Copyright (c) 2024 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use restate_storage_api::inbox_table::ReadOnlyInboxTable;
use restate_types::invocation::SubmitNotificationSink;
use restate_types::time::MillisSinceEpoch;
use std::time::{Duration, SystemTime};
use test_log::test;

#[test(tokio::test)]
async fn send_with_delay() {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_service();
    let invocation_id = InvocationId::mock_random();

    let node_id = GenerationalNodeId::new(1, 1);
    let request_id = IngressRequestId::default();

    let wake_up_time = MillisSinceEpoch::from(SystemTime::now() + Duration::from_secs(60));
    let actions = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: None,
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                node_id,
                request_id,
            }),
            // Doesn't matter the execution time here, just needs to be filled
            execution_time: Some(wake_up_time),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(matchers::actions::invoke_for_id(invocation_id))),
            contains(pat!(Action::RegisterTimer { .. })),
            contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id,
                        is_new_invocation: true,
                    },
                }
            ))))
        )
    );

    // Now fire the timer
    let actions = test_env
        .apply(Command::Timer(TimerKeyValue::neo_invoke(
            wake_up_time,
            invocation_id,
        )))
        .await;

    assert_that!(
        actions,
        all!(
            contains(matchers::actions::invoke_for_id(invocation_id)),
            not(contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id,
                        is_new_invocation: true,
                    },
                }
            )))))
        )
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        ok(pat!(InvocationStatus::Invoked { .. }))
    );
    test_env.shutdown().await;
}

#[test(tokio::test)]
async fn send_with_delay_to_locked_virtual_object() {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::mock_generate(&invocation_target);

    let node_id = GenerationalNodeId::new(1, 1);
    let request_id = IngressRequestId::default();

    let wake_up_time = MillisSinceEpoch::from(SystemTime::now() + Duration::from_secs(60));
    let actions = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: None,
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                node_id,
                request_id,
            }),
            // Doesn't matter the execution time here, just needs to be filled
            execution_time: Some(wake_up_time),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(matchers::actions::invoke_for_id(invocation_id))),
            contains(pat!(Action::RegisterTimer { .. })),
            contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id,
                        is_new_invocation: true,
                    },
                }
            ))))
        )
    );

    // Now lock the service_id
    let mut tx = test_env.storage.transaction();
    tx.put_virtual_object_status(
        &invocation_target.as_keyed_service_id().unwrap(),
        &VirtualObjectStatus::Locked(InvocationId::mock_generate(&invocation_target)),
    )
    .await;
    tx.commit().await.unwrap();

    // Now fire the timer
    let actions = test_env
        .apply(Command::Timer(TimerKeyValue::neo_invoke(
            wake_up_time,
            invocation_id,
        )))
        .await;

    assert_that!(
        actions,
        all!(
            not(contains(matchers::actions::invoke_for_id(invocation_id))),
            not(contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id,
                        is_new_invocation: true,
                    },
                }
            )))))
        )
    );
    assert_that!(
        test_env.storage.get_invocation_status(&invocation_id).await,
        ok(pat!(InvocationStatus::Inboxed { .. }))
    );
    assert_that!(
        test_env
            .storage
            .inbox(&invocation_target.as_keyed_service_id().unwrap())
            .try_collect::<Vec<_>>()
            .await,
        ok(contains(matchers::storage::invocation_inbox_entry(
            invocation_id,
            &invocation_target
        )))
    );
    test_env.shutdown().await;
}

#[test(tokio::test)]
async fn send_with_delay_and_idempotency_key() {
    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));

    let node_id = GenerationalNodeId::new(1, 1);
    let request_id_1 = IngressRequestId::default();

    let actions = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            idempotency_key: Some(idempotency_key.clone()),
            response_sink: None,
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                node_id,
                request_id: request_id_1,
            }),
            completion_retention_duration: Some(retention),
            // Doesn't matter the execution time here, just needs to be filled
            execution_time: Some(MillisSinceEpoch::from(
                SystemTime::now() + Duration::from_secs(60),
            )),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(matchers::actions::invoke_for_id(invocation_id))),
            contains(pat!(Action::RegisterTimer { .. })),
            contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id: request_id_1,
                        is_new_invocation: true,
                    },
                }
            ))))
        )
    );

    // Send another invocation which reattaches to the original one
    let request_id_2 = IngressRequestId::default();
    let actions = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            idempotency_key: Some(idempotency_key),
            response_sink: None,
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                node_id,
                request_id: request_id_2,
            }),
            completion_retention_duration: Some(retention),
            // Doesn't matter the execution time here, just needs to be filled
            execution_time: Some(MillisSinceEpoch::from(
                SystemTime::now() + Duration::from_secs(60),
            )),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(matchers::actions::invoke_for_id(invocation_id))),
            contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id: request_id_2,
                        is_new_invocation: false,
                    },
                }
            ))))
        )
    );
    test_env.shutdown().await;
}
