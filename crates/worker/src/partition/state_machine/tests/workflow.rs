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

use crate::partition::state_machine::tests::matchers::actions::forward_purge_invocation_response;
use restate_storage_api::invocation_status_table::CompletedInvocation;
use restate_storage_api::service_status_table::ReadVirtualObjectStatusTable;
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
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            completion_retention_duration: Duration::from_secs(60),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
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
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_2,
            }),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(pat!(Action::Invoke {
                invocation_id: eq(invocation_id),
                invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
            }))),
            // We get back this error due to the fact that we disabled the attach semantics
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_2),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Failure(
                    WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR
                ))
            }))
        )
    );

    // Send output, then end
    let response_bytes = Bytes::from_static(b"123");
    let actions = test_env
        .apply_multiple([
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            })),
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
            })),
        ])
        .await;

    // Assert response and cleanup timer
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_1),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            })),
            // This is a not() because we currently disabled the attach semantics on request/response
            not(contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_2),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
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
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_3,
            }),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::IngressResponse {
            request_id: eq(request_id_3),
            invocation_id: some(eq(invocation_id)),
            response: eq(InvocationOutputResponse::Failure(
                WORKFLOW_ALREADY_INVOKED_INVOCATION_ERROR
            ))
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
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            completion_retention_duration: Duration::from_secs(60),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    // Sending another invocation won't re-execute
    let actions = test_env
        .apply(Command::AttachInvocation(AttachInvocationRequest {
            invocation_query: InvocationQuery::Workflow(
                invocation_target.as_keyed_service_id().unwrap(),
            ),
            block_on_inflight: true,
            response_sink: ServiceInvocationResponseSink::Ingress {
                request_id: request_id_2,
            },
        }))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(pat!(Action::Invoke {
                invocation_id: eq(invocation_id),
                invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
            }))),
            not(contains(pat!(Action::IngressResponse { .. })))
        )
    );

    // Send output, then end
    let response_bytes = Bytes::from_static(b"123");
    let actions = test_env
        .apply_multiple([
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            })),
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
            })),
        ])
        .await;

    // Assert response and cleanup timer
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_1),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            })),
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_2),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
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
        .apply(Command::AttachInvocation(AttachInvocationRequest {
            invocation_query: InvocationQuery::Workflow(
                invocation_target.as_keyed_service_id().unwrap(),
            ),
            block_on_inflight: true,
            response_sink: ServiceInvocationResponseSink::Ingress {
                request_id: request_id_3,
            },
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::IngressResponse {
            request_id: eq(request_id_3),
            invocation_id: some(eq(invocation_id)),
            response: eq(InvocationOutputResponse::Success(
                invocation_target.clone(),
                response_bytes.clone()
            ))
        }))
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn purge_completed_workflow() {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_workflow();
    let invocation_id = InvocationId::mock_random();

    // Prepare idempotency metadata and completed status
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
    txn.put_virtual_object_status(
        &invocation_target.as_keyed_service_id().unwrap(),
        &VirtualObjectStatus::Locked(invocation_id),
    )
    .unwrap();
    txn.commit().await.unwrap();

    // Send timer fired command
    let request_id = PartitionProcessorRpcRequestId::new();
    let actions = test_env
        .apply(Command::PurgeInvocation(PurgeInvocationRequest {
            invocation_id,
            response_sink: Some(InvocationMutationResponseSink::Ingress(
                IngressInvocationResponseSink { request_id },
            )),
        }))
        .await;
    assert_that!(
        actions,
        contains(forward_purge_invocation_response(
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
    assert_that!(
        test_env
            .storage()
            .get_virtual_object_status(&invocation_target.as_keyed_service_id().unwrap())
            .await
            .unwrap(),
        pat!(VirtualObjectStatus::Unlocked)
    );
    test_env.shutdown().await;
}
