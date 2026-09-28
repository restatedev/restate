// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

use crate::partition::state_machine::tests::matchers::actions::purge_invocation_reply;
use restate_storage_api::invocation_status_table::CompletedInvocation;
use restate_storage_api::service_status_table::ReadVirtualObjectStatusTable;
use restate_storage_api::timer_table::ReadTimerTable;
use restate_types::errors::WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR;
use restate_types::invocation::{
    AttachInvocationRequest, IngressInvocationResponseSink, InvocationQuery, InvocationTarget,
    PurgeInvocationRequest,
};
use std::time::Duration;

#[restate_core::test]
async fn start_workflow_method() {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_workflow();
    let invocation_id = InvocationId::mock_generate(&invocation_target);
    let request_id_1 = PartitionProcessorRpcRequestId::default();
    let request_id_2 = PartitionProcessorRpcRequestId::default();

    // Send fresh invocation
    test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            completion_retention_duration: Duration::from_secs(60),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Invoked(_))
    );

    // Assert we don't write virtual object status anymore for locking.
    assert_that!(
        test_env
            .storage()
            .get_virtual_object_status(&invocation_target.as_keyed_service_id().unwrap())
            .await
            .unwrap(),
        eq(VirtualObjectStatus::Unlocked)
    );

    // Sending another invocation won't re-execute
    let actions = test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_2,
            }),
            ..ServiceInvocation::mock()
        }))
        .await;
    // We get back this error due to the fact that we disabled the attach semantics
    assert_that!(
        actions,
        contains(pat!(Action::ReplyRpc {
            reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                request_id: eq(request_id_2),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Failure(
                    WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR
                ))
            })))
        }))
    );

    // Send output, then end
    let response_bytes = Bytes::from_static(b"123");
    let actions = test_env
        .apply_multiple([
            commands::InvokerEffectCommand::test_envelope(Effect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            }),
            commands::InvokerEffectCommand::test_envelope(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
            }),
        ])
        .await;

    // Assert response and cleanup timer
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::ReplyRpc {
                reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                    request_id: eq(request_id_1),
                    invocation_id: some(eq(invocation_id)),
                    response: eq(InvocationOutputResponse::Success(
                        invocation_target.clone(),
                        response_bytes.clone()
                    ))
                })))
            })),
            // This is a not() because we currently disabled the attach semantics on request/response
            not(contains(pat!(Action::ReplyRpc {
                reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                    request_id: eq(request_id_2),
                    invocation_id: some(eq(invocation_id)),
                    response: eq(InvocationOutputResponse::Success(
                        invocation_target.clone(),
                        response_bytes.clone()
                    ))
                })))
            })))
        )
    );

    // InvocationStatus contains completed
    let invocation_status = test_env
        .storage()
        .get_invocation_status(&invocation_id)
        .await
        .unwrap();
    assert_that!(
        invocation_status,
        pat!(InvocationStatus::Completed(pat!(CompletedInvocation {
            response_result: eq(ResponseResult::Success(response_bytes.clone()))
        })))
    );

    // Sending a new request will not be completed because we don't support attach semantics
    let request_id_3 = PartitionProcessorRpcRequestId::default();
    let actions = test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_3,
            }),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::ReplyRpc {
            reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                request_id: eq(request_id_3),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Failure(
                    WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR
                ))
            })))
        }))
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn attach_by_workflow_key() {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_workflow();
    let invocation_id = InvocationId::mock_generate(&invocation_target);
    let request_id_1 = PartitionProcessorRpcRequestId::default();
    let request_id_2 = PartitionProcessorRpcRequestId::default();
    let request_id_3 = PartitionProcessorRpcRequestId::default();

    // Send fresh invocation
    test_env
        .apply(commands::InvokeCommand::test_envelope(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            completion_retention_duration: Duration::from_secs(60),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Invoked(_))
    );

    // Sending another invocation won't re-execute
    let actions = test_env
        .apply(commands::AttachInvocationCommand::test_envelope(
            AttachInvocationRequest {
                invocation_query: InvocationQuery::Workflow(
                    invocation_target.as_keyed_service_id().unwrap(),
                ),
                block_on_inflight: true,
                response_sink: ServiceInvocationResponseSink::Ingress {
                    request_id: request_id_2,
                },
            },
        ))
        .await;
    assert_that!(
        actions,
        not(contains(pat!(Action::ReplyRpc {
            reply: pat!(RpcReply::Output(pat!(InvocationOutput { .. })))
        })))
    );

    // Send output, then end
    let response_bytes = Bytes::from_static(b"123");
    let actions = test_env
        .apply_multiple([
            commands::InvokerEffectCommand::test_envelope(Effect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            }),
            commands::InvokerEffectCommand::test_envelope(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
            }),
        ])
        .await;

    // Assert response and cleanup timer
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::ReplyRpc {
                reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                    request_id: eq(request_id_1),
                    invocation_id: some(eq(invocation_id)),
                    response: eq(InvocationOutputResponse::Success(
                        invocation_target.clone(),
                        response_bytes.clone()
                    ))
                })))
            })),
            contains(pat!(Action::ReplyRpc {
                reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                    request_id: eq(request_id_2),
                    invocation_id: some(eq(invocation_id)),
                    response: eq(InvocationOutputResponse::Success(
                        invocation_target.clone(),
                        response_bytes.clone()
                    ))
                })))
            }))
        )
    );

    // InvocationStatus contains completed
    let invocation_status = test_env
        .storage()
        .get_invocation_status(&invocation_id)
        .await
        .unwrap();
    assert_that!(
        invocation_status,
        pat!(InvocationStatus::Completed(pat!(CompletedInvocation {
            response_result: eq(ResponseResult::Success(response_bytes.clone()))
        })))
    );

    // Sending another attach will be completed immediately
    let actions = test_env
        .apply(commands::AttachInvocationCommand::test_envelope(
            AttachInvocationRequest {
                invocation_query: InvocationQuery::Workflow(
                    invocation_target.as_keyed_service_id().unwrap(),
                ),
                block_on_inflight: true,
                response_sink: ServiceInvocationResponseSink::Ingress {
                    request_id: request_id_3,
                },
            },
        ))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::ReplyRpc {
            reply: pat!(RpcReply::Output(pat!(InvocationOutput {
                request_id: eq(request_id_3),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            })))
        }))
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn purge_completed_workflow() {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_workflow();
    let invocation_id = InvocationId::mock_random();

    // Prepare a completed workflow invocation
    let mut txn = test_env.storage().transaction();
    txn.put_invocation_status(
        &invocation_id,
        &InvocationStatus::Completed(CompletedInvocation {
            invocation_target: invocation_target.clone(),
            idempotency_key: None,
            ..CompletedInvocation::mock_neo()
        }),
    )
    .unwrap();
    txn.commit().await.unwrap();
    drop(txn);

    let request_id = PartitionProcessorRpcRequestId::new();
    let actions = test_env
        .apply(commands::PurgeInvocationCommand::test_envelope(
            PurgeInvocationRequest {
                invocation_id,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink { request_id },
                )),
            },
        ))
        .await;
    assert_that!(
        actions,
        contains(purge_invocation_reply(
            request_id,
            PurgeInvocationResponse::Ok
        ))
    );
    assert_that!(
        test_env
            .storage()
            .get_invocation_status(&invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Free)
    );
    test_env.shutdown().await;
}

fn sleep_command(completion_id: CompletionId) -> SleepCommand {
    SleepCommand {
        // Matches the timestamp expected by the delete_sleep_timer matcher
        wake_up_time: MillisSinceEpoch::new(1337),
        completion_id,
        name: Default::default(),
    }
}

#[restate_core::test]
async fn kill_deletes_pending_sleep_timers_only_for_reusable_ids() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;

    // Workflow run with two sleeps, the first one already fired
    let workflow_id = fixtures::mock_start_invocation_with_invocation_target(
        &mut test_env,
        InvocationTarget::mock_workflow(),
    )
    .await;
    fixtures::mock_pinned_deployment_v5(&mut test_env, workflow_id).await;
    test_env
        .apply_multiple([
            fixtures::invoker_entry_effect(workflow_id, sleep_command(1)),
            fixtures::invoker_entry_effect(workflow_id, sleep_command(2)),
            commands::TimerCommand::test_envelope(TimerKeyValue::complete_journal_entry(
                MillisSinceEpoch::new(1337),
                workflow_id,
                1,
            )),
        ])
        .await;

    // Invocation with a random id and a pending sleep
    let random_id = fixtures::mock_start_invocation(&mut test_env).await;
    fixtures::mock_pinned_deployment_v5(&mut test_env, random_id).await;
    test_env
        .apply(fixtures::invoker_entry_effect(random_id, sleep_command(1)))
        .await;

    let kill = |invocation_id| {
        commands::TerminateInvocationCommand::test_envelope(InvocationTermination {
            invocation_id,
            flavor: TerminationFlavor::Kill,
            response_sink: None,
        })
    };

    // Killing the workflow deletes only the timer of the pending sleep
    let actions = test_env.apply(kill(workflow_id)).await;
    assert_that!(
        actions,
        all!(
            contains(matchers::actions::delete_sleep_timer(2)),
            not(contains(matchers::actions::delete_sleep_timer(1)))
        )
    );

    // Killing the invocation with a random id leaves its timer, it can never match another invocation
    let actions = test_env.apply(kill(random_id)).await;
    assert_that!(actions, not(contains(pat!(Action::DeleteTimer { .. }))));

    let timers = test_env
        .storage
        .next_timers_greater_than(None, usize::MAX)?
        .map_ok(|(_, timer)| timer)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(timers, vec![Timer::CompleteJournalEntry(random_id, 1)]);

    test_env.shutdown().await;
    Ok(())
}

#[restate_core::test]
async fn purge_workflow_deletes_pending_sleep_timers() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_workflow();
    let invocation_id = InvocationId::mock_generate(&invocation_target);

    // Complete the workflow while one of its sleeps is still pending, retaining the journal
    let actions = test_env
        .apply_multiple([
            commands::InvokeCommand::test_envelope(ServiceInvocation {
                invocation_id,
                invocation_target,
                completion_retention_duration: Duration::from_secs(60),
                journal_retention_duration: Duration::from_secs(60),
                ..ServiceInvocation::mock()
            }),
            fixtures::pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
            fixtures::invoker_entry_effect(invocation_id, sleep_command(1)),
            fixtures::invoker_entry_effect(
                invocation_id,
                OutputCommand {
                    result: OutputResult::Success(Bytes::from_static(b"done")),
                    name: Default::default(),
                },
            ),
            fixtures::invoker_end_effect(invocation_id),
        ])
        .await;
    assert_that!(actions, not(contains(pat!(Action::DeleteTimer { .. }))));
    assert_that!(
        test_env
            .storage
            .get_invocation_status(&invocation_id)
            .await?,
        pat!(InvocationStatus::Completed(_))
    );

    // Purging drops the journal together with the pending sleep timer
    let actions = test_env
        .apply(commands::PurgeInvocationCommand::test_envelope(
            PurgeInvocationRequest {
                invocation_id,
                response_sink: None,
            },
        ))
        .await;
    assert_that!(actions, contains(matchers::actions::delete_sleep_timer(1)));
    assert_that!(
        test_env
            .storage
            .next_timers_greater_than(None, usize::MAX)?
            .try_collect::<Vec<_>>()
            .await?,
        empty()
    );

    test_env.shutdown().await;
    Ok(())
}
