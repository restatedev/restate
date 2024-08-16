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

mod delayed_send;
mod idempotency;
mod kill_cancel;
mod matchers;
mod workflow;

use crate::partition::types::{InvokerEffect, InvokerEffectKind};
use ::tracing::info;
use bytes::Bytes;
use bytestring::ByteString;
use futures::{StreamExt, TryStreamExt};
use googletest::matcher::Matcher;
use googletest::{all, assert_that, pat, property};
use restate_core::{task_center, TaskCenter, TaskCenterBuilder};
use restate_invoker_api::{EffectKind, InvokeInputJournal};
use restate_partition_store::{OpenMode, PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::inbox_table::ReadOnlyInboxTable;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
    ReadOnlyInvocationStatusTable, SourceTable,
};
use restate_storage_api::journal_table::{JournalEntry, ReadOnlyJournalTable};
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, VirtualObjectStatus, VirtualObjectStatusTable,
};
use restate_storage_api::state_table::{ReadOnlyStateTable, StateTable};
use restate_storage_api::Transaction;
use restate_test_util::matchers::*;
use restate_types::config::{CommonOptions, WorkerOptions};
use restate_types::errors::{codes, InvocationError, KILLED_INVOCATION_ERROR};
use restate_types::identifiers::{
    IngressRequestId, InvocationId, PartitionId, PartitionKey, ServiceId,
};
use restate_types::ingress::{IngressResponseEnvelope, IngressResponseResult};
use restate_types::invocation::{
    Header, InvocationResponse, InvocationTarget, InvocationTermination, ResponseResult,
    ServiceInvocation, ServiceInvocationResponseSink, Source, VirtualObjectHandlerType,
};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{
    CompleteAwakeableEntry, Completion, CompletionResult, EntryResult, InvokeRequest,
};
use restate_types::journal::{Entry, EntryType};
use restate_types::live::{Constant, Live};
use restate_types::state_mut::ExternalStateMutation;
use restate_types::{ingress, GenerationalNodeId};
use std::collections::{HashMap, HashSet};
use test_log::test;

pub struct TestEnv {
    #[allow(dead_code)]
    task_center: TaskCenter,
    state_machine: StateMachine<ProtobufRawEntryCodec>,
    // TODO for the time being we use rocksdb storage because we have no mocks for storage interfaces.
    //  Perhaps we could make these tests faster by having those.
    storage: PartitionStore,
}

impl TestEnv {
    pub fn partition_id(&self) -> PartitionId {
        PartitionId::MIN
    }

    pub async fn create() -> Self {
        Self::create_with_state_machine(StateMachine::new(
            0,    /* inbox_seq_number */
            0,    /* outbox_seq_number */
            None, /* outbox_head_seq_number */
            PartitionKey::MIN..=PartitionKey::MAX,
            SourceTable::Old,
        ))
        .await
    }

    pub async fn create_with_neo_invocation_status_table() -> Self {
        Self::create_with_state_machine(StateMachine::new(
            0,    /* inbox_seq_number */
            0,    /* outbox_seq_number */
            None, /* outbox_head_seq_number */
            PartitionKey::MIN..=PartitionKey::MAX,
            SourceTable::New,
        ))
        .await
    }

    pub async fn create_with_state_machine(
        state_machine: StateMachine<ProtobufRawEntryCodec>,
    ) -> Self {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");

        tc.run_in_scope("init", None, async {
            RocksDbManager::init(Constant::new(CommonOptions::default()));
            let worker_options = Live::from_value(WorkerOptions::default());
            info!(
                "Using RocksDB temp directory {}",
                worker_options.pinned().storage.data_dir().display()
            );
            let manager = PartitionStoreManager::create(
                worker_options.clone().map(|c| &c.storage),
                worker_options.clone().map(|c| &c.storage.rocksdb).boxed(),
                &[],
            )
            .await
            .unwrap();
            let rocksdb_storage = manager
                .open_partition_store(
                    PartitionId::MIN,
                    RangeInclusive::new(PartitionKey::MIN, PartitionKey::MAX),
                    OpenMode::CreateIfMissing,
                    &worker_options.pinned().storage.rocksdb,
                )
                .await
                .unwrap();

            Self {
                task_center: task_center(),
                state_machine,
                storage: rocksdb_storage,
            }
        })
        .await
    }

    pub async fn apply(&mut self, command: Command) -> Vec<Action> {
        let partition_id = self.partition_id();
        let mut transaction = crate::partition::storage::Transaction::new(
            partition_id,
            0..=PartitionKey::MAX,
            self.storage.transaction(),
        );
        let mut action_collector = ActionCollector::default();
        self.state_machine
            .apply(command, &mut transaction, &mut action_collector, true)
            .await
            .unwrap();

        transaction.commit().await.unwrap();

        action_collector
    }

    pub async fn apply_multiple(
        &mut self,
        commands: impl IntoIterator<Item = Command>,
    ) -> Vec<Action> {
        let mut actions = vec![];
        for command in commands {
            actions.append(&mut self.apply(command).await)
        }
        actions
    }

    pub fn storage(&mut self) -> &mut PartitionStore {
        &mut self.storage
    }
}

type TestResult = Result<(), anyhow::Error>;

#[test(tokio::test)]
async fn start_invocation() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let id = mock_start_invocation(&mut test_env).await;

    let invocation_status = test_env.storage().get_invocation_status(&id).await.unwrap();
    assert_that!(invocation_status, pat!(InvocationStatus::Invoked(_)));
    Ok(())
}

#[test(tokio::test)]
async fn shared_invocation_skips_inbox() -> TestResult {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::virtual_object(
        "MySvc",
        "MyKey",
        "MyHandler",
        VirtualObjectHandlerType::Shared,
    );

    // Let's lock the virtual object
    let mut tx = test_env.storage.transaction();
    tx.put_virtual_object_status(
        &invocation_target.as_keyed_service_id().unwrap(),
        VirtualObjectStatus::Locked(InvocationId::mock_random()),
    )
    .await;
    tx.commit().await.unwrap();

    // Start the invocation
    let invocation_id =
        mock_start_invocation_with_invocation_target(&mut test_env, invocation_target.clone())
            .await;

    // Should be in invoked status
    let invocation_status = test_env
        .storage()
        .get_invocation_status(&invocation_id)
        .await
        .unwrap();
    assert_that!(
        invocation_status,
        pat!(InvocationStatus::Invoked(pat!(
            InFlightInvocationMetadata {
                invocation_target: eq(invocation_target.clone())
            }
        )))
    );
    Ok(())
}

#[test(tokio::test)]
async fn awakeable_completion_received_before_entry() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let invocation_id = mock_start_invocation(&mut test_env).await;

    // Send completion first
    let _ = test_env
        .apply(Command::InvocationResponse(InvocationResponse {
            id: invocation_id,
            entry_index: 1,
            result: ResponseResult::Success(Bytes::default()),
        }))
        .await;

    // A couple of notes here:
    // * There can't be a deadlock wrt suspensions.
    //   Proof by contradiction: Assume InvocationStatus == Suspended with entry index X deadlocks due to having only the completion for X, but not the entry.
    //   To end up in the Suspended state, the SDK sent a SuspensionMessage containing suspension index X, and X is not resumable (see StorageReader::is_entry_resumable).
    //   Because in the invoker we check that suspension indexes are within the range of known journal entries,
    //   it means that all the X + 1 entries have been already received and processed by the PP beforehand, due to the ordering requirement of the protocol.
    //   In order to receive a completion for an awakeable, the SDK must have generated in these X + 1 entries the corresponding awakeable entry, and sent it to the runtime.
    //   But this means that once the awakeable entry X is received, it will be merged with completion X and thus X is resumable,
    //   contradicting the condition to end up in the Suspended state.

    // * In case of a crash of the deployment:
    //   * If the awakeable entry has been received, everything goes through the regular flow of the journal reader.
    //   * If the awakeable entry has not been received yet, when receiving it the completion will be sent through.

    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::awakeable(None)),
            },
        }))
        .await;

    // At this point we expect the completion to be forwarded to the invoker
    assert_that!(
        actions,
        contains(pat!(Action::ForwardCompletion {
            invocation_id: eq(invocation_id),
            completion: eq(Completion::new(
                1,
                CompletionResult::Success(Bytes::default())
            ))
        }))
    );

    // The entry should be in storage
    let entry = test_env
        .storage
        .get_journal_entry(&invocation_id, 1)
        .await
        .unwrap()
        .unwrap();
    assert_that!(
        entry,
        pat!(JournalEntry::Entry(all!(
            property!(EnrichedRawEntry.ty(), eq(EntryType::Awakeable)),
            predicate(|e: &EnrichedRawEntry| e.header().is_completed() == Some(true))
        )))
    );

    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: InvokerEffectKind::Suspended {
                waiting_for_completed_entries: HashSet::from([1]),
            },
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id)
        }))
    );
    Ok(())
}

#[test(tokio::test)]
async fn complete_awakeable_with_success() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = mock_start_invocation(&mut test_env).await;

    let callee_invocation_id = InvocationId::mock_random();
    let callee_entry_index = 10;
    let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
        CompleteAwakeableEntry {
            id: AwakeableIdentifier::new(callee_invocation_id, callee_entry_index)
                .to_string()
                .into(),
            result: EntryResult::Success(Bytes::default()),
        },
    ));

    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry,
            },
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        id: eq(callee_invocation_id),
                        entry_index: eq(callee_entry_index),
                        result: pat!(ResponseResult::Success { .. })
                    }
                ))
            )
        }))
    );
}

#[test(tokio::test)]
async fn complete_awakeable_with_failure() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = mock_start_invocation(&mut test_env).await;

    let callee_invocation_id = InvocationId::mock_random();
    let callee_entry_index = 10;
    let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
        CompleteAwakeableEntry {
            id: AwakeableIdentifier::new(callee_invocation_id, callee_entry_index)
                .to_string()
                .into(),
            result: EntryResult::Failure(codes::BAD_REQUEST, "Some failure".into()),
        },
    ));

    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry,
            },
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        id: eq(callee_invocation_id),
                        entry_index: eq(callee_entry_index),
                        result: eq(ResponseResult::Failure(InvocationError::new(
                            codes::BAD_REQUEST,
                            "Some failure"
                        )))
                    }
                ))
            )
        }))
    );
}

#[test(tokio::test)]
async fn invoke_with_headers() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let service_id = ServiceId::mock_random();
    let invocation_id =
        mock_start_invocation_with_service_id(&mut test_env, service_id.clone()).await;

    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::invoke(
                    InvokeRequest {
                        service_name: service_id.service_name,
                        handler_name: "MyMethod".into(),
                        parameter: Bytes::default(),
                        headers: vec![Header::new("foo", "bar")],
                        key: service_id.key,
                    },
                    None,
                )),
            },
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(pat!(
                    restate_types::invocation::ServiceInvocation {
                        headers: eq(vec![Header::new("foo", "bar")])
                    }
                ))
            )
        }))
    );
    Ok(())
}

#[test(tokio::test)]
async fn mutate_state() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let keyed_service_id = invocation_target.as_keyed_service_id().unwrap();
    let invocation_id =
        mock_start_invocation_with_invocation_target(&mut test_env, invocation_target.clone())
            .await;

    let first_state_mutation: HashMap<Bytes, Bytes> = [
        (Bytes::from_static(b"foobar"), Bytes::from_static(b"foobar")),
        (Bytes::from_static(b"bar"), Bytes::from_static(b"bar")),
    ]
    .into_iter()
    .collect();

    let second_state_mutation: HashMap<Bytes, Bytes> =
        [(Bytes::from_static(b"bar"), Bytes::from_static(b"foo"))]
            .into_iter()
            .collect();

    // state should be empty
    assert_eq!(
        test_env
            .storage
            .get_all_user_states_for_service(&keyed_service_id)
            .count()
            .await,
        0
    );

    test_env
        .apply(Command::PatchState(ExternalStateMutation {
            service_id: keyed_service_id.clone(),
            version: None,
            state: first_state_mutation,
        }))
        .await;
    test_env
        .apply(Command::PatchState(ExternalStateMutation {
            service_id: keyed_service_id.clone(),
            version: None,
            state: second_state_mutation.clone(),
        }))
        .await;

    // terminating the ongoing invocation should trigger popping from the inbox until the
    // next invocation is found
    test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: InvokerEffectKind::End,
        }))
        .await;

    let all_states: HashMap<_, _> = test_env
        .storage
        .get_all_user_states_for_service(&keyed_service_id)
        .try_collect()
        .await?;

    assert_eq!(all_states, second_state_mutation);

    Ok(())
}

#[test(tokio::test)]
async fn clear_all_user_states() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;
    let service_id = ServiceId::new("MySvc", "my-key");

    // Fill with some state the service K/V store
    let mut txn = test_env.storage.transaction();
    txn.put_user_state(&service_id, b"my-key-1", b"my-val-1")
        .await;
    txn.put_user_state(&service_id, b"my-key-2", b"my-val-2")
        .await;
    txn.commit().await.unwrap();

    let invocation_id =
        mock_start_invocation_with_service_id(&mut test_env, service_id.clone()).await;

    test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::clear_all_state()),
            },
        }))
        .await;

    let states: Vec<restate_storage_api::Result<(Bytes, Bytes)>> = test_env
        .storage
        .get_all_user_states_for_service(&service_id)
        .collect()
        .await;
    assert_that!(states, empty());

    Ok(())
}

#[test(tokio::test)]
async fn get_state_keys() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let service_id = ServiceId::mock_random();
    let invocation_id =
        mock_start_invocation_with_service_id(&mut test_env, service_id.clone()).await;

    // Mock some state
    let mut txn = test_env.storage.transaction();
    txn.put_user_state(&service_id, b"key1", b"value1").await;
    txn.put_user_state(&service_id, b"key2", b"value2").await;
    txn.commit().await.unwrap();

    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::get_state_keys(None)),
            },
        }))
        .await;

    // At this point we expect the completion to be forwarded to the invoker
    assert_that!(
        actions,
        contains(pat!(Action::ForwardCompletion {
            invocation_id: eq(invocation_id),
            completion: eq(Completion::new(
                1,
                ProtobufRawEntryCodec::serialize_get_state_keys_completion(vec![
                    Bytes::copy_from_slice(b"key1"),
                    Bytes::copy_from_slice(b"key2"),
                ])
            ))
        }))
    );
    Ok(())
}

#[test(tokio::test)]
async fn send_ingress_response_to_multiple_targets() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let (invocation_id, invocation_target) =
        InvocationId::mock_with(InvocationTarget::mock_virtual_object());

    let node_id_1 = GenerationalNodeId::new(1, 1);
    let node_id_2 = GenerationalNodeId::new(2, 1);
    let request_id_1 = IngressRequestId::default();
    let request_id_2 = IngressRequestId::default();
    let request_id_3 = IngressRequestId::default();

    let actions = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            argument: Default::default(),
            source: Source::Ingress,
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                node_id: node_id_1,
                request_id: request_id_1,
            }),
            span_context: Default::default(),
            headers: vec![],
            execution_time: None,
            completion_retention_duration: None,
            idempotency_key: None,
            submit_notification_sink: None,
        }))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    // Let's add another ingress
    let mut txn = test_env.storage.transaction();
    let mut invocation_status = txn.get_invocation_status(&invocation_id).await.unwrap();
    invocation_status.get_response_sinks_mut().unwrap().insert(
        ServiceInvocationResponseSink::Ingress {
            node_id: node_id_2,
            request_id: request_id_2,
        },
    );
    invocation_status.get_response_sinks_mut().unwrap().insert(
        ServiceInvocationResponseSink::Ingress {
            node_id: node_id_2,
            request_id: request_id_3,
        },
    );
    txn.put_invocation_status(&invocation_id, invocation_status)
        .await;
    txn.commit().await.unwrap();

    // Now let's send the output entry
    let response_bytes = Bytes::from_static(b"123");
    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                    EntryResult::Success(response_bytes.clone()),
                )),
            },
        }))
        .await;
    // No ingress response is expected at this point because the invocation did not end yet
    assert_that!(actions, not(contains(pat!(Action::IngressResponse(_)))));

    // Send the End Effect
    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id,
            kind: InvokerEffectKind::End,
        }))
        .await;
    // At this point we expect the completion to be forwarded to the invoker
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id_1),
                    inner: pat!(ingress::InvocationResponse {
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
                    target_node: eq(node_id_2),
                    inner: pat!(ingress::InvocationResponse {
                        request_id: eq(request_id_2),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            )))),
            contains(pat!(Action::IngressResponse(pat!(
                IngressResponseEnvelope {
                    target_node: eq(node_id_2),
                    inner: pat!(ingress::InvocationResponse {
                        request_id: eq(request_id_3),
                        response: eq(IngressResponseResult::Success(
                            invocation_target.clone(),
                            response_bytes.clone()
                        ))
                    })
                }
            )))),
        )
    );

    Ok(())
}

#[test(tokio::test)]
async fn truncate_outbox_from_empty() -> Result<(), Error> {
    // An outbox message with index 0 has been successfully processed, and must now be truncated
    let outbox_index = 0;

    let mut test_env = TestEnv::create().await;

    let _ = test_env.apply(Command::TruncateOutbox(outbox_index)).await;

    assert_that!(
        test_env
            .storage
            .get_outbox_message(test_env.partition_id(), 0)
            .await?,
        none()
    );

    // The head catches up to the next available sequence number on truncation. Since we don't know
    // in advance whether we will get asked to truncate a range of more than one outbox message, we
    // explicitly track the head sequence number as the next position beyond the last known
    // truncation point. It's only safe to leave the head as None when the outbox is known to be
    // empty.
    assert_eq!(test_env.state_machine.outbox_head_seq_number, Some(1));

    Ok(())
}

#[test(tokio::test)]
async fn truncate_outbox_with_gap() -> Result<(), Error> {
    // The outbox contains items [3..=5], and the range must be truncated after message 5 is processed
    let outbox_head_index = 3;
    let outbox_tail_index = 5;

    let mut test_env =
        TestEnv::create_with_state_machine(StateMachine::<ProtobufRawEntryCodec>::new(
            0,
            outbox_tail_index,
            Some(outbox_head_index),
            PartitionKey::MIN..=PartitionKey::MAX,
            SourceTable::New,
        ))
        .await;

    test_env
        .apply(Command::TruncateOutbox(outbox_tail_index))
        .await;

    assert_that!(
        test_env
            .storage
            .get_outbox_message(test_env.partition_id(), 3)
            .await?,
        none()
    );
    assert_that!(
        test_env
            .storage
            .get_outbox_message(test_env.partition_id(), 4)
            .await?,
        none()
    );
    assert_that!(
        test_env
            .storage
            .get_outbox_message(test_env.partition_id(), 5)
            .await?,
        none()
    );

    assert_eq!(
        test_env.state_machine.outbox_head_seq_number,
        Some(outbox_tail_index + 1)
    );

    Ok(())
}

#[test(tokio::test)]
async fn consecutive_exclusive_handler_invocations_will_use_inbox() -> TestResult {
    let mut test_env = TestEnv::create().await;

    let (first_invocation_id, invocation_target) =
        InvocationId::mock_with(InvocationTarget::mock_virtual_object());
    let keyed_service_id = invocation_target.as_keyed_service_id().unwrap();
    let second_invocation_id = InvocationId::generate(&invocation_target);

    // Let's start the first invocation
    let actions = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: first_invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;
    assert_that!(
        actions,
        contains(matchers::actions::invoke_for_id(first_invocation_id))
    );
    assert_that!(
        test_env
            .storage
            .get_virtual_object_status(&keyed_service_id)
            .await,
        ok(eq(VirtualObjectStatus::Locked(first_invocation_id)))
    );

    // Let's start the second invocation
    let actions = test_env
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id: second_invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        }))
        .await;

    // This should have not been invoked, but it should rather be in the inbox
    assert_that!(
        actions,
        not(contains(matchers::actions::invoke_for_id(
            second_invocation_id
        )))
    );
    assert_that!(
        test_env
            .storage
            .inbox(&keyed_service_id)
            .try_collect::<Vec<_>>()
            .await,
        ok(contains(matchers::storage::invocation_inbox_entry(
            second_invocation_id,
            &invocation_target
        )))
    );
    assert_that!(
        test_env
            .storage
            .get_virtual_object_status(&keyed_service_id)
            .await,
        ok(eq(VirtualObjectStatus::Locked(first_invocation_id)))
    );

    // Send the End Effect to terminate the first invocation
    let actions = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id: first_invocation_id,
            kind: InvokerEffectKind::End,
        }))
        .await;
    // At this point we expect the invoke for the second, and also the lock updated
    assert_that!(
        actions,
        contains(matchers::actions::invoke_for_id(second_invocation_id))
    );
    assert_that!(
        test_env
            .storage
            .get_virtual_object_status(&keyed_service_id)
            .await,
        ok(eq(VirtualObjectStatus::Locked(second_invocation_id)))
    );

    let _ = test_env
        .apply(Command::InvokerEffect(InvokerEffect {
            invocation_id: second_invocation_id,
            kind: InvokerEffectKind::End,
        }))
        .await;

    // After the second was completed too, the inbox is empty and the service is unlocked
    assert_that!(
        test_env
            .storage
            .inbox(&keyed_service_id)
            .try_collect::<Vec<_>>()
            .await,
        ok(empty())
    );
    assert_that!(
        test_env
            .storage
            .get_virtual_object_status(&keyed_service_id)
            .await,
        ok(eq(VirtualObjectStatus::Unlocked))
    );

    Ok(())
}

async fn mock_start_invocation_with_service_id(
    state_machine: &mut TestEnv,
    service_id: ServiceId,
) -> InvocationId {
    mock_start_invocation_with_invocation_target(
        state_machine,
        InvocationTarget::mock_from_service_id(service_id),
    )
    .await
}

async fn mock_start_invocation_with_invocation_target(
    state_machine: &mut TestEnv,
    invocation_target: InvocationTarget,
) -> InvocationId {
    let invocation_id = InvocationId::generate(&invocation_target);

    let actions = state_machine
        .apply(Command::Invoke(ServiceInvocation {
            invocation_id,
            invocation_target: invocation_target.clone(),
            argument: Default::default(),
            source: Source::Ingress,
            response_sink: None,
            span_context: Default::default(),
            headers: vec![],
            execution_time: None,
            completion_retention_duration: None,
            idempotency_key: None,
            submit_notification_sink: None,
        }))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id),
            invocation_target: eq(invocation_target),
            invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
        }))
    );

    invocation_id
}

async fn mock_start_invocation(state_machine: &mut TestEnv) -> InvocationId {
    mock_start_invocation_with_invocation_target(
        state_machine,
        InvocationTarget::mock_virtual_object(),
    )
    .await
}
