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

use crate::partition::state_machine::tests::matchers::actions::invocation_response_to_partition_processor;
use restate_invoker_api::Effect;
use restate_storage_api::idempotency_table::{
    IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable,
};
use restate_storage_api::inbox_table::{InboxEntry, ReadInboxTable, SequenceNumberInboxEntry};
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, JournalMetadata, StatusTimestamps,
};
use restate_types::identifiers::{IdempotencyId, PartitionProcessorRpcRequestId};
use restate_types::invocation::{
    AttachInvocationRequest, InvocationQuery, InvocationTarget, PurgeInvocationRequest,
    SubmitNotificationSink,
};
use rstest::*;
use std::time::Duration;

#[restate_core::test]
async fn start_and_complete_idempotent_invocation() {
    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());
    let request_id = PartitionProcessorRpcRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
            idempotency_key: Some(idempotency_key),
            completion_retention_duration: retention,
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
        }))
    );

    // Assert we don't write idempotency metadata, the table is deprecated
    assert_that!(
        test_env
            .storage()
            .get_idempotency_metadata(&idempotency_id)
            .await
            .unwrap(),
        none()
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
                memory_lease: MemoryLease::unlinked(),
            })),
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
                memory_lease: MemoryLease::unlinked(),
            })),
        ])
        .await;

    // Assert response and timeout
    assert_that!(
        actions,
        contains(pat!(Action::IngressResponse {
            request_id: eq(request_id),
            invocation_id: some(eq(invocation_id)),
            response: eq(InvocationOutputResponse::Success(
                invocation_target.clone(),
                response_bytes.clone()
            ))
        }))
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
            response_result: eq(ResponseResult::Success(response_bytes))
        })))
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn start_and_complete_idempotent_invocation_neo_table() {
    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());
    let request_id = PartitionProcessorRpcRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
            idempotency_key: Some(idempotency_key),
            completion_retention_duration: retention,
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
        }))
    );

    // Assert we don't write idempotency metadata, the table is deprecated
    assert_that!(
        test_env
            .storage()
            .get_idempotency_metadata(&idempotency_id)
            .await
            .unwrap(),
        none()
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
                memory_lease: MemoryLease::unlinked(),
            })),
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
                memory_lease: MemoryLease::unlinked(),
            })),
        ])
        .await;

    // Assert response and timeout
    assert_that!(
        actions,
        contains(pat!(Action::IngressResponse {
            request_id: eq(request_id),
            invocation_id: some(eq(invocation_id)),
            response: eq(InvocationOutputResponse::Success(
                invocation_target.clone(),
                response_bytes.clone()
            ))
        }))
    );

    // InvocationStatus contains completed
    let invocation_status = test_env
        .storage()
        .get_invocation_status(&invocation_id)
        .await
        .unwrap();
    let_assert!(InvocationStatus::Completed(completed_invocation) = invocation_status);
    assert_eq!(
        completed_invocation.response_result,
        ResponseResult::Success(response_bytes)
    );
    assert!(
        completed_invocation
            .timestamps
            .completed_transition_time()
            .is_some()
    );
    assert_eq!(
        completed_invocation.completion_retention_duration,
        retention
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn complete_already_completed_invocation() {
    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());

    let response_bytes = Bytes::from_static(b"123");

    // Prepare idempotency metadata and completed status
    let mut txn = test_env.storage().transaction();
    txn.put_idempotency_metadata(&idempotency_id, &IdempotencyMetadata { invocation_id })
        .await
        .unwrap();
    txn.put_invocation_status(
        &invocation_id,
        &InvocationStatus::Completed(CompletedInvocation {
            invocation_target: invocation_target.clone(),
            created_using_restate_version: RestateVersion::current(),
            source: Source::Ingress(PartitionProcessorRpcRequestId::new()),
            execution_time: None,
            idempotency_key: Some(idempotency_key.clone()),
            timestamps: StatusTimestamps::mock(),
            response_result: ResponseResult::Success(response_bytes.clone()),
            completion_retention_duration: Default::default(),
            journal_retention_duration: Default::default(),
            journal_metadata: JournalMetadata::empty(),
            pinned_deployment: None,
            random_seed: None,
        }),
    )
    .unwrap();
    txn.commit().await.unwrap();

    // Send a request, should be completed immediately with result
    let request_id = PartitionProcessorRpcRequestId::default();
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
            idempotency_key: Some(idempotency_key),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::IngressResponse {
            request_id: eq(request_id),
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
async fn attach_with_service_invocation_command_while_executing() {
    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));

    let request_id_1 = PartitionProcessorRpcRequestId::default();
    let request_id_2 = PartitionProcessorRpcRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: retention,
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
        }))
    );

    // Latch to existing invocation
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_2,
            }),
            idempotency_key: Some(idempotency_key),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(actions, not(contains(pat!(Action::IngressResponse { .. }))));

    // Send output
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
                memory_lease: MemoryLease::unlinked(),
            })),
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
                memory_lease: MemoryLease::unlinked(),
            })),
        ])
        .await;

    // Assert responses
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
                request_id: eq(request_id_1),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            }))
        )
    );
    test_env.shutdown().await;
}

#[rstest]
#[case(true)]
#[case(false)]
#[restate_core::test]
async fn attach_with_send_service_invocation(#[case] use_same_request_id: bool) {
    use restate_invoker_api::Effect;

    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));

    let request_id_1 = PartitionProcessorRpcRequestId::default();
    let request_id_2 = if use_same_request_id {
        request_id_1
    } else {
        PartitionProcessorRpcRequestId::default()
    };

    // Send fresh invocation with idempotency key
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: retention,
            source: Source::Ingress(request_id_1),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
        }))
    );

    // Latch to existing invocation, but with a send call
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: retention,
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                request_id: request_id_2,
            }),
            source: Source::Ingress(request_id_2),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(pat!(Action::IngressResponse { .. }))),
            contains(eq(Action::IngressSubmitNotification {
                request_id: request_id_2,
                execution_time: None,
                is_new_invocation: use_same_request_id,
            }))
        )
    );

    // Send output
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
                memory_lease: MemoryLease::unlinked(),
            })),
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
                memory_lease: MemoryLease::unlinked(),
            })),
        ])
        .await;

    // Assert responses
    if use_same_request_id {
        assert_that!(
            actions,
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_1),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            }))
        );
    } else {
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
                not(contains(pat!(Action::IngressResponse {
                    request_id: eq(request_id_2)
                }))),
            )
        );
    }
    test_env.shutdown().await;
}

#[restate_core::test]
async fn attach_inboxed_with_send_service_invocation() {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_virtual_object();
    let request_id_1 = PartitionProcessorRpcRequestId::default();
    let request_id_2 = PartitionProcessorRpcRequestId::default();

    // Initialize locked virtual object state
    async {
        let mut tx = test_env.storage.transaction();
        tx.put_virtual_object_status(
            &invocation_target.as_keyed_service_id().unwrap(),
            &VirtualObjectStatus::Locked(InvocationId::mock_generate(&invocation_target)),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }
    .await;

    // Send first invocation, this should end up in the inbox
    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: Duration::from_secs(60) * 60 * 24,
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                request_id: request_id_1,
            }),
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(pat!(Action::Invoke {
                invocation_id: eq(invocation_id),
            }))),
            contains(eq(Action::IngressSubmitNotification {
                request_id: request_id_1,
                execution_time: None,
                is_new_invocation: true,
            }))
        )
    );
    // Invocation is inboxed
    assert_that!(
        test_env
            .storage
            .transaction()
            .peek_inbox(&invocation_target.as_keyed_service_id().unwrap())
            .await
            .unwrap(),
        some(pat!(SequenceNumberInboxEntry {
            inbox_entry: eq(InboxEntry::Invocation(
                invocation_target.as_keyed_service_id().unwrap(),
                invocation_id
            ))
        }))
    );

    // Now send the request that should get the submit notification
    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: Duration::from_secs(60) * 60 * 24,
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
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
            }))),
            not(contains(pat!(Action::IngressResponse { .. }))),
            contains(eq(Action::IngressSubmitNotification {
                request_id: request_id_2,
                execution_time: None,
                is_new_invocation: false,
            }))
        )
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn attach_command() {
    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let completion_retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));

    let request_id_1 = PartitionProcessorRpcRequestId::default();
    let request_id_2 = PartitionProcessorRpcRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: completion_retention,
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
        }))
    );

    // Latch to existing invocation, but with a send call
    let actions = test_env
        .apply(Command::AttachInvocation(AttachInvocationRequest {
            invocation_query: InvocationQuery::Invocation(invocation_id),
            block_on_inflight: true,
            response_sink: ServiceInvocationResponseSink::Ingress {
                request_id: request_id_2,
            },
        }))
        .await;
    assert_that!(
        actions,
        all!(not(contains(pat!(Action::IngressResponse { .. }))))
    );

    // Send output
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
                memory_lease: MemoryLease::unlinked(),
            })),
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                kind: InvokerEffectKind::End,
                memory_lease: MemoryLease::unlinked(),
            })),
        ])
        .await;

    // Assert responses
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse {
                invocation_id: some(eq(invocation_id)),
                request_id: eq(request_id_1),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            })),
            contains(pat!(Action::IngressResponse {
                invocation_id: some(eq(invocation_id)),
                request_id: eq(request_id_2),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            }))
        )
    );
    test_env.shutdown().await;
}

#[restate_core::test]
async fn attach_command_without_blocking_inflight() {
    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let completion_retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));

    // Send fresh invocation with idempotency key
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: PartitionProcessorRpcRequestId::default(),
            }),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: completion_retention,
            ..ServiceInvocation::mock()
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
        }))
    );

    // Latch to existing invocation without blocking on inflight invocation
    let caller_invocation_id = InvocationId::mock_random();
    let actions = test_env
        .apply(Command::AttachInvocation(AttachInvocationRequest {
            invocation_query: InvocationQuery::Invocation(invocation_id),
            block_on_inflight: false,
            response_sink: ServiceInvocationResponseSink::PartitionProcessor(
                JournalCompletionTarget::from_parts(caller_invocation_id, 1),
            ),
        }))
        .await;
    assert_that!(
        actions,
        all!(
            contains(invocation_response_to_partition_processor(
                caller_invocation_id,
                1,
                eq(ResponseResult::from(NOT_READY_INVOCATION_ERROR))
            )),
            not(contains(pat!(Action::IngressResponse { .. })))
        )
    );

    test_env.shutdown().await;
}

#[restate_core::test]
async fn purge_completed_idempotent_invocation() {
    let mut test_env = TestEnv::create().await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());

    // Prepare idempotency metadata and completed status
    let mut txn = test_env.storage().transaction();
    txn.put_idempotency_metadata(&idempotency_id, &IdempotencyMetadata { invocation_id })
        .await
        .unwrap();
    txn.put_invocation_status(
        &invocation_id,
        &InvocationStatus::Completed(CompletedInvocation {
            invocation_target,
            idempotency_key: Some(idempotency_key.clone()),
            ..CompletedInvocation::mock_neo()
        }),
    )
    .unwrap();
    txn.commit().await.unwrap();

    // Send purge command
    let _ = test_env
        .apply(Command::PurgeInvocation(PurgeInvocationRequest {
            invocation_id,
            response_sink: None,
        }))
        .await;
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
            .get_idempotency_metadata(&idempotency_id)
            .await
            .unwrap(),
        none()
    );
    test_env.shutdown().await;
}
