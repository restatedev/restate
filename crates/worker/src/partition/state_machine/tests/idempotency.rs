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

use restate_storage_api::idempotency_table::{
    IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable,
};
use restate_storage_api::inbox_table::{InboxEntry, ReadOnlyInboxTable, SequenceNumberInboxEntry};
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, SourceTable, StatusTimestamps,
};
use restate_storage_api::timer_table::{Timer, TimerKey, TimerKeyKind};
use restate_types::errors::GONE_INVOCATION_ERROR;
use restate_types::identifiers::{IdempotencyId, IngressRequestId};
use restate_types::invocation::{
    AttachInvocationRequest, InvocationQuery, InvocationTarget, PurgeInvocationRequest,
    SubmitNotificationSink,
};
use restate_wal_protocol::timer::TimerKeyValue;
use std::time::Duration;
use test_log::test;

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn start_and_complete_idempotent_invocation() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope("mock-state-machine", None, MockStateMachine::create())
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());
    let node_id = GenerationalNodeId::new(1, 1);
    let request_id = IngressRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id,
                request_id,
            }),
            idempotency_key: Some(idempotency_key),
            completion_retention_duration: Some(retention),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    // Assert idempotency key mapping exists
    let mut txn = state_machine.storage().transaction();
    assert_that!(
        txn.get_idempotency_metadata(&idempotency_id)
            .await
            .unwrap()
            .unwrap(),
        pat!(IdempotencyMetadata {
            invocation_id: eq(invocation_id),
        })
    );
    txn.commit().await.unwrap();

    // Send output, then end
    let response_bytes = Bytes::from_static(b"123");
    let actions = state_machine
        .apply_multiple([
            Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            }),
            Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::End,
            }),
        ])
        .await;

    // Assert response and timeout
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id),
                    inner: pat!(ingress::InvocationResponse {
                        request_id: eq(request_id),
                        invocation_id: some(eq(invocation_id)),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            )))),
            contains(pat!(Action::ScheduleInvocationStatusCleanup {
                invocation_id: eq(invocation_id)
            }))
        )
    );

    // InvocationStatus contains completed
    let invocation_status = state_machine
        .storage()
        .transaction()
        .get_invocation_status(&invocation_id)
        .await
        .unwrap();
    assert_that!(
        invocation_status,
        pat!(InvocationStatus::Completed(pat!(CompletedInvocation {
            response_result: eq(ResponseResult::Success(response_bytes))
        })))
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn start_and_complete_idempotent_invocation_neo_table() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope(
            "mock-state-machine",
            None,
            MockStateMachine::create_with_neo_invocation_status_table(),
        )
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());
    let node_id = GenerationalNodeId::new(1, 1);
    let request_id = IngressRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id,
                request_id,
            }),
            idempotency_key: Some(idempotency_key),
            completion_retention_duration: Some(retention),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    // Assert idempotency key mapping exists
    assert_that!(
        state_machine
            .storage()
            .get_idempotency_metadata(&idempotency_id)
            .await
            .unwrap()
            .unwrap(),
        pat!(IdempotencyMetadata {
            invocation_id: eq(invocation_id),
        })
    );

    // Send output, then end
    let response_bytes = Bytes::from_static(b"123");
    let actions = state_machine
        .apply_multiple([
            Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            }),
            Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::End,
            }),
        ])
        .await;

    // Assert response and timeout
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id),
                    inner: pat!(ingress::InvocationResponse {
                        request_id: eq(request_id),
                        invocation_id: some(eq(invocation_id)),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            )))),
            not(contains(pat!(Action::ScheduleInvocationStatusCleanup {
                invocation_id: eq(invocation_id)
            })))
        )
    );

    // InvocationStatus contains completed
    let invocation_status = state_machine
        .storage()
        .get_invocation_status(&invocation_id)
        .await
        .unwrap();
    let_assert!(InvocationStatus::Completed(completed_invocation) = invocation_status);
    assert_eq!(
        completed_invocation.response_result,
        ResponseResult::Success(response_bytes)
    );
    assert!(unsafe { completed_invocation.timestamps.completed_transition_time() }.is_some());
    assert_eq!(
        completed_invocation.completion_retention_duration,
        retention
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn complete_already_completed_invocation() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope("mock-state-machine", None, MockStateMachine::create())
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());

    let response_bytes = Bytes::from_static(b"123");
    let ingress_id = GenerationalNodeId::new(1, 1);

    // Prepare idempotency metadata and completed status
    let mut txn = state_machine.storage().transaction();
    txn.put_idempotency_metadata(&idempotency_id, IdempotencyMetadata { invocation_id })
        .await;
    txn.put_invocation_status(
        &invocation_id,
        InvocationStatus::Completed(CompletedInvocation {
            invocation_target: invocation_target.clone(),
            span_context: ServiceInvocationSpanContext::default(),
            source: Source::Ingress,
            idempotency_key: Some(idempotency_key.clone()),
            timestamps: StatusTimestamps::now(),
            response_result: ResponseResult::Success(response_bytes.clone()),
            completion_retention_duration: Default::default(),
            source_table: SourceTable::New,
        }),
    )
    .await;
    txn.commit().await.unwrap();

    // Send a request, should be completed immediately with result
    let second_invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let request_id = IngressRequestId::default();
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: second_invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id: ingress_id,
                request_id,
            }),
            idempotency_key: Some(idempotency_key),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::IngressResponse(pat!(
            IngressResponseEnvelope {
                target_node: eq(GenerationalNodeId::new(1, 1)),
                inner: pat!(ingress::InvocationResponse {
                    request_id: eq(request_id),
                    invocation_id: some(eq(invocation_id)),
                    response: eq(IngressResponseResult::Success(
                        invocation_target.clone(),
                        response_bytes.clone()
                    ))
                })
            }
        ))))
    );
    assert_that!(
        state_machine
            .storage()
            .get_invocation_status(&second_invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Free)
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn known_invocation_id_but_missing_completion() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope("mock-state-machine", None, MockStateMachine::create())
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());

    let ingress_id = GenerationalNodeId::new(1, 1);

    // Prepare idempotency metadata
    let mut txn = state_machine.rocksdb_storage.transaction();
    txn.put_idempotency_metadata(&idempotency_id, IdempotencyMetadata { invocation_id })
        .await;
    txn.commit().await.unwrap();

    // Send a request, should be completed immediately with result
    let second_invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let request_id = IngressRequestId::default();
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: second_invocation_id,
            invocation_target,
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id: ingress_id,
                request_id,
            }),
            idempotency_key: Some(idempotency_key),
            completion_retention_duration: Some(retention),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::IngressResponse(pat!(
            IngressResponseEnvelope {
                target_node: eq(ingress_id),
                inner: pat!(ingress::InvocationResponse {
                    request_id: eq(request_id),
                    invocation_id: some(eq(second_invocation_id)),
                    response: eq(IngressResponseResult::Failure(GONE_INVOCATION_ERROR))
                })
            }
        ))))
    );
    assert_that!(
        state_machine
            .storage()
            .transaction()
            .get_invocation_status(&second_invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Free)
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn attach_with_service_invocation_command_while_executing() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope("mock-state-machine", None, MockStateMachine::create())
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let first_invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );

    let node_id = GenerationalNodeId::new(1, 1);
    let request_id_1 = IngressRequestId::default();
    let request_id_2 = IngressRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: first_invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id,
                request_id: request_id_1,
            }),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: Some(retention),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(first_invocation_id),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    // Latch to existing invocation
    let second_invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: second_invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id,
                request_id: request_id_2,
            }),
            idempotency_key: Some(idempotency_key),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(actions, not(contains(pat!(Action::IngressResponse(_)))));

    // Send output
    let response_bytes = Bytes::from_static(b"123");
    let actions = state_machine
        .apply_multiple([
            Command::InvokerEffect(InvokerEffect {
                invocation_id: first_invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            }),
            Command::InvokerEffect(InvokerEffect {
                invocation_id: first_invocation_id,
                kind: InvokerEffectKind::End,
            }),
        ])
        .await;

    // Assert responses
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id),
                    inner: pat!(ingress::InvocationResponse {
                        request_id: eq(request_id_1),
                        invocation_id: some(eq(first_invocation_id)),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            )))),
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id),
                    inner: pat!(ingress::InvocationResponse {
                        request_id: eq(request_id_1),
                        invocation_id: some(eq(first_invocation_id)),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            ))))
        )
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn attach_with_send_service_invocation() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope("mock-state-machine", None, MockStateMachine::create())
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let first_invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );

    let node_id = GenerationalNodeId::new(1, 1);
    let request_id_1 = IngressRequestId::default();
    let request_id_2 = IngressRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: first_invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id,
                request_id: request_id_1,
            }),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: Some(retention),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(first_invocation_id),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    // Latch to existing invocation, but with a send call
    let second_invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: second_invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: None,
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: Some(retention),
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                node_id,
                request_id: request_id_2,
            }),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(pat!(Action::IngressResponse(_)))),
            contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id: request_id_2,
                        original_invocation_id: second_invocation_id,
                        attached_invocation_id: first_invocation_id
                    },
                }
            ))))
        )
    );

    // Send output
    let response_bytes = Bytes::from_static(b"123");
    let actions = state_machine
        .apply_multiple([
            Command::InvokerEffect(InvokerEffect {
                invocation_id: first_invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            }),
            Command::InvokerEffect(InvokerEffect {
                invocation_id: first_invocation_id,
                kind: InvokerEffectKind::End,
            }),
        ])
        .await;

    // Assert responses
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id),
                    inner: pat!(ingress::InvocationResponse {
                        request_id: eq(request_id_1),
                        invocation_id: some(eq(first_invocation_id)),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            )))),
            not(contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id),
                    inner: pat!(ingress::InvocationResponse {
                        request_id: eq(request_id_2)
                    })
                }
            ))))),
        )
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn attach_inboxed_with_send_service_invocation() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope("mock-state-machine", None, MockStateMachine::create())
        .await;

    let invocation_target = InvocationTarget::mock_virtual_object();
    let node_id = GenerationalNodeId::new(1, 1);
    let request_id_1 = IngressRequestId::default();
    let request_id_2 = IngressRequestId::default();

    // Initialize locked virtual object state
    async {
        let mut tx = state_machine.rocksdb_storage.transaction();
        tx.put_virtual_object_status(
            &invocation_target.as_keyed_service_id().unwrap(),
            VirtualObjectStatus::Locked(InvocationId::generate(&invocation_target)),
        )
        .await;
        tx.commit().await.unwrap();
    }
    .await;

    // Send first invocation, this should end up in the inbox
    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let attached_invocation_id =
        InvocationId::generate_with_idempotency_key(&invocation_target, &idempotency_key);
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: attached_invocation_id,
            invocation_target: invocation_target.clone(),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: Some(Duration::from_secs(60) * 60 * 24),
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                node_id,
                request_id: request_id_1,
            }),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(pat!(Action::Invoke {
                invocation_id: eq(attached_invocation_id),
            }))),
            contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id: request_id_1,
                        original_invocation_id: attached_invocation_id,
                        attached_invocation_id
                    },
                }
            ))))
        )
    );
    // Invocation is inboxed
    assert_that!(
        state_machine
            .rocksdb_storage
            .transaction()
            .peek_inbox(&invocation_target.as_keyed_service_id().unwrap())
            .await
            .unwrap(),
        some(pat!(SequenceNumberInboxEntry {
            inbox_entry: eq(InboxEntry::Invocation(
                invocation_target.as_keyed_service_id().unwrap(),
                attached_invocation_id
            ))
        }))
    );

    // Now send the request that should get the submit notification
    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let original_invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: original_invocation_id,
            invocation_target: invocation_target.clone(),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: Some(Duration::from_secs(60) * 60 * 24),
            submit_notification_sink: Some(SubmitNotificationSink::Ingress {
                node_id,
                request_id: request_id_2,
            }),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        all!(
            not(contains(pat!(Action::Invoke {
                invocation_id: eq(original_invocation_id),
            }))),
            not(contains(pat!(Action::IngressResponse(_)))),
            contains(pat!(Action::IngressSubmitNotification(eq(
                IngressResponseEnvelope {
                    target_node: node_id,
                    inner: ingress::SubmittedInvocationNotification {
                        request_id: request_id_2,
                        original_invocation_id,
                        attached_invocation_id,
                    },
                }
            ))))
        )
    );
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn attach_command() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope("mock-state-machine", None, MockStateMachine::create())
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let completion_retention = Duration::from_secs(60) * 60 * 24;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );

    let node_id = GenerationalNodeId::new(1, 1);
    let request_id_1 = IngressRequestId::default();
    let request_id_2 = IngressRequestId::default();

    // Send fresh invocation with idempotency key
    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id,
                request_id: request_id_1,
            }),
            idempotency_key: Some(idempotency_key.clone()),
            completion_retention_duration: Some(completion_retention),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    // Latch to existing invocation, but with a send call
    let actions = state_machine
        .apply(Command::AttachInvocation(AttachInvocationRequest {
            invocation_query: InvocationQuery::Invocation(invocation_id),
            response_sink: ServiceInvocationResponseSink::Ingress {
                node_id,
                request_id: request_id_2,
            },
        }))
        .await;
    assert_that!(
        actions,
        all!(not(contains(pat!(Action::IngressResponse(_)))))
    );

    // Send output
    let response_bytes = Bytes::from_static(b"123");
    let actions = state_machine
        .apply_multiple([
            Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                        EntryResult::Success(response_bytes.clone()),
                    )),
                },
            }),
            Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::End,
            }),
        ])
        .await;

    // Assert responses
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id),
                    inner: pat!(ingress::InvocationResponse {
                        invocation_id: some(eq(invocation_id)),
                        request_id: eq(request_id_1),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            )))),
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id),
                    inner: pat!(ingress::InvocationResponse {
                        invocation_id: some(eq(invocation_id)),
                        request_id: eq(request_id_2),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            ))))
        )
    );
}

// TODO remove this once we remove the old invocation status table
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn timer_cleanup() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope("mock-state-machine", None, MockStateMachine::create())
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());

    // Prepare idempotency metadata and completed status
    let mut txn = state_machine.storage().transaction();
    txn.put_idempotency_metadata(&idempotency_id, IdempotencyMetadata { invocation_id })
        .await;
    txn.put_invocation_status(
        &invocation_id,
        InvocationStatus::Completed(CompletedInvocation {
            invocation_target,
            source: Source::Ingress,
            span_context: Default::default(),
            idempotency_key: Some(idempotency_key.clone()),
            timestamps: StatusTimestamps::now(),
            response_result: ResponseResult::Success(Bytes::from_static(b"123")),
            completion_retention_duration: Duration::MAX,
            source_table: SourceTable::Old,
        }),
    )
    .await;
    txn.commit().await.unwrap();

    // Send timer fired command
    let _ = state_machine
        .apply(Command::Timer(TimerKeyValue::new(
            TimerKey {
                kind: TimerKeyKind::Invoke {
                    invocation_uuid: invocation_id.invocation_uuid(),
                },
                timestamp: 0,
            },
            Timer::CleanInvocationStatus(invocation_id),
        )))
        .await;
    assert_that!(
        state_machine
            .storage()
            .transaction()
            .get_invocation_status(&invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Free)
    );
    assert_that!(
        state_machine
            .storage()
            .transaction()
            .get_idempotency_metadata(&idempotency_id)
            .await
            .unwrap(),
        none()
    );
}

#[test(tokio::test)]
async fn purge_completed_idempotent_invocation() {
    let tc = TaskCenterBuilder::default()
        .default_runtime_handle(tokio::runtime::Handle::current())
        .build()
        .expect("task_center builds");
    let mut state_machine = tc
        .run_in_scope(
            "mock-state-machine",
            None,
            MockStateMachine::create_with_neo_invocation_status_table(),
        )
        .await;

    let idempotency_key = ByteString::from_static("my-idempotency-key");
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::generate_with_idempotency_key(
        &invocation_target,
        Some(idempotency_key.clone()),
    );
    let idempotency_id =
        IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());

    // Prepare idempotency metadata and completed status
    let mut txn = state_machine.storage().transaction();
    txn.put_idempotency_metadata(&idempotency_id, IdempotencyMetadata { invocation_id })
        .await;
    txn.put_invocation_status(
        &invocation_id,
        InvocationStatus::Completed(CompletedInvocation {
            invocation_target,
            idempotency_key: Some(idempotency_key.clone()),
            ..CompletedInvocation::mock_neo()
        }),
    )
    .await;
    txn.commit().await.unwrap();

    // Send purge command
    let _ = state_machine
        .apply(Command::PurgeInvocation(PurgeInvocationRequest {
            invocation_id,
        }))
        .await;
    assert_that!(
        state_machine
            .storage()
            .get_invocation_status(&invocation_id)
            .await
            .unwrap(),
        pat!(InvocationStatus::Free)
    );
    assert_that!(
        state_machine
            .storage()
            .get_idempotency_metadata(&idempotency_id)
            .await
            .unwrap(),
        none()
    );
}
