// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
pub mod fixtures;
mod idempotency;
mod invocation_epoch_awareness;
mod kill_cancel;
pub mod matchers;
mod workflow;

use crate::partition::state_machine::tests::fixtures::{
    background_invoke_entry, incomplete_invoke_entry,
};
use crate::partition::state_machine::tests::matchers::storage::is_entry;
use crate::partition::state_machine::tests::matchers::success_completion;
use crate::partition::types::InvokerEffectKind;
use ::tracing::info;
use bytes::Bytes;
use bytestring::ByteString;
use futures::{StreamExt, TryStreamExt};
use googletest::{all, assert_that, pat, property};
use restate_core::TaskCenter;
use restate_invoker_api::{Effect, EffectKind, InvokeInputJournal};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::Transaction;
use restate_storage_api::inbox_table::ReadOnlyInboxTable;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, ReadInvocationStatusTable,
    WriteInvocationStatusTable,
};
use restate_storage_api::journal_table::{JournalEntry, ReadJournalTable};
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, VirtualObjectStatus, VirtualObjectStatusTable,
};
use restate_storage_api::state_table::{ReadOnlyStateTable, StateTable};
use restate_test_util::matchers::*;
use restate_types::config::StorageOptions;
use restate_types::errors::{InvocationError, KILLED_INVOCATION_ERROR, codes};
use restate_types::identifiers::{
    AwakeableIdentifier, InvocationId, PartitionId, PartitionKey, PartitionProcessorRpcRequestId,
    ServiceId,
};
use restate_types::invocation::client::InvocationOutputResponse;
use restate_types::invocation::{
    Header, InvocationResponse, InvocationTarget, InvocationTermination, ResponseResult,
    ServiceInvocation, ServiceInvocationResponseSink, Source, VirtualObjectHandlerType,
};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{
    CompleteAwakeableEntry, Completion, CompletionResult, EntryResult, InvokeRequest,
};
use restate_types::journal::{Entry, EntryType};
use restate_types::journal_events::Event;
use restate_types::journal_v2::raw::TryFromEntry;
use restate_types::logs::SequenceNumber;
use restate_types::partitions::Partition;
use restate_types::state_mut::ExternalStateMutation;
use std::collections::{HashMap, HashSet};
use test_log::test;
use tracing_subscriber::fmt::format::FmtSpan;

pub struct TestEnv {
    pub state_machine: StateMachine,
    // TODO for the time being we use rocksdb storage because we have no mocks for storage interfaces.
    //  Perhaps we could make these tests faster by having those.
    pub storage: PartitionStore,
}

impl TestEnv {
    pub async fn shutdown(self) {
        TaskCenter::shutdown_node("test complete", 0).await;
        RocksDbManager::get().shutdown().await;
    }

    pub async fn create() -> Self {
        Self::create_with_experimental_features(Default::default()).await
    }

    pub async fn create_with_experimental_features(
        experimental_features: EnumSet<ExperimentalFeature>,
    ) -> Self {
        Self::create_with_state_machine(StateMachine::new(
            0,    /* inbox_seq_number */
            0,    /* outbox_seq_number */
            None, /* outbox_head_seq_number */
            PartitionKey::MIN..=PartitionKey::MAX,
            SemanticRestateVersion::unknown().clone(),
            experimental_features,
        ))
        .await
    }

    pub async fn create_with_state_machine(state_machine: StateMachine) -> Self {
        // Try init logging, if not already initialized. This removes the need for the test_log macro
        let _ = tracing_subscriber::FmtSubscriber::builder().with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(  match std::env::var_os("RUST_LOG_SPAN_EVENTS") {
                Some(mut value) => {
                    value.make_ascii_lowercase();
                    let value = value.to_str().expect("test-log: RUST_LOG_SPAN_EVENTS must be valid UTF-8");
                    value.split(",").map(|filter| match filter.trim() {
                        "new" => FmtSpan::NEW,
                        "enter" => FmtSpan::ENTER,
                        "exit" => FmtSpan::EXIT,
                        "close" => FmtSpan::CLOSE,
                        "active" => FmtSpan::ACTIVE,
                        "full" => FmtSpan::FULL,
                        _ => panic!("test-log: RUST_LOG_SPAN_EVENTS must contain filters separated by `,`.\n\t\
                  For example: `active` or `new,close`\n\t\
                  Supported filters: new, enter, exit, close, active, full\n\t\
                  Got: {value}"),
                    }).fold(FmtSpan::NONE, |acc, filter| filter | acc)
                }
                None => FmtSpan::NONE,
            }).with_test_writer().try_init();

        RocksDbManager::init();
        let storage_options = StorageOptions::default();
        info!(
            "Using RocksDB temp directory {}",
            storage_options.data_dir("db").display()
        );
        let manager = PartitionStoreManager::create().await.unwrap();
        let rocksdb_storage = manager
            .open(
                &Partition::new(
                    PartitionId::MIN,
                    RangeInclusive::new(PartitionKey::MIN, PartitionKey::MAX),
                ),
                None,
            )
            .await
            .unwrap();

        Self {
            state_machine,
            storage: rocksdb_storage,
        }
    }

    pub async fn apply(&mut self, command: Command) -> Vec<Action> {
        let mut transaction = self.storage.transaction();
        let mut action_collector = ActionCollector::default();
        self.state_machine
            .apply(
                command,
                MillisSinceEpoch::now(),
                Lsn::OLDEST,
                &mut transaction,
                &mut action_collector,
                true,
            )
            .await
            .unwrap();

        transaction.commit().await.unwrap();

        action_collector
    }

    pub async fn apply_fallible(&mut self, command: Command) -> Result<Vec<Action>, Error> {
        let mut transaction = self.storage.transaction();
        let mut action_collector = ActionCollector::default();
        self.state_machine
            .apply(
                command,
                MillisSinceEpoch::now(),
                Lsn::OLDEST,
                &mut transaction,
                &mut action_collector,
                true,
            )
            .await?;

        transaction.commit().await?;

        Ok(action_collector)
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

    pub async fn read_journal_to_vec(
        &mut self,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Vec<journal_v2::Entry> {
        restate_storage_api::journal_table_v2::ReadOnlyJournalTable::get_journal(
            self.storage(),
            invocation_id,
            journal_length,
        )
        .expect("storage to be working")
        .try_collect::<Vec<_>>()
        .await
        .unwrap_or_else(|_| panic!("can read journal {invocation_id} of length {journal_length}"))
        .into_iter()
        .map(|(i, e)| {
            e.decode::<ServiceProtocolV4Codec, _>()
                .unwrap_or_else(|_| panic!("entry index {i} can be decoded"))
        })
        .collect()
    }

    #[allow(unused)]
    pub async fn read_journal_entry<E: TryFromEntry>(
        &mut self,
        invocation_id: InvocationId,
        idx: EntryIndex,
    ) -> E {
        restate_storage_api::journal_table_v2::ReadOnlyJournalTable::get_journal_entry(
            self.storage(),
            invocation_id,
            idx,
        )
        .await
        .expect("storage to be working")
        .expect("Entry to be present")
        .decode::<ServiceProtocolV4Codec, E>()
        .expect("Entry to be of the specified type")
    }

    /// Returns journal events ordered by timestamps
    pub async fn read_journal_events(&mut self, invocation_id: InvocationId) -> Vec<Event> {
        let mut events =
            restate_storage_api::journal_events::ReadOnlyJournalEventsTable::get_journal_events(
                self.storage(),
                invocation_id,
            )
            .expect("storage to be working")
            .try_collect::<Vec<_>>()
            .await
            .expect("to be decodable");

        events.sort_by(|x, y| x.append_time.cmp(&y.append_time));

        events
            .into_iter()
            .map(|e| e.event.into_event_or_unknown())
            .collect()
    }

    pub async fn modify_invocation_status(
        &mut self,
        invocation_id: InvocationId,
        f: impl FnOnce(&mut InvocationStatus),
    ) {
        let mut tx = self.storage().transaction();
        let mut status = tx.get_invocation_status(&invocation_id).await.unwrap();
        f(&mut status);
        tx.put_invocation_status(&invocation_id, &status).unwrap();
        tx.commit().await.unwrap();
    }

    pub async fn verify_journal_components(
        &mut self,
        invocation_id: InvocationId,
        entry_types: impl IntoIterator<Item = journal_v2::EntryType>,
    ) {
        let expected_entry_types = entry_types.into_iter().collect::<Vec<_>>();
        let expected_commands = expected_entry_types
            .iter()
            .filter(|e| e.is_command())
            .count();
        assert_that!(
            self.storage.get_invocation_status(&invocation_id).await,
            ok(all!(
                matchers::storage::has_journal_length(expected_entry_types.len() as u32),
                matchers::storage::has_commands(expected_commands as u32)
            ))
        );
        let actual_entry_types = self
            .read_journal_to_vec(invocation_id, expected_entry_types.len() as EntryIndex)
            .await
            .into_iter()
            .map(|e| e.ty())
            .collect::<Vec<_>>();
        assert_eq!(actual_entry_types, expected_entry_types);

        // Verify we don't go out of bounds
        assert_that!(
            self.storage
                .get_journal_entry(&invocation_id, expected_entry_types.len() as u32)
                .await,
            ok(none())
        );
    }
}

type TestResult = Result<(), anyhow::Error>;

#[test(restate_core::test)]
async fn start_invocation() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let id = fixtures::mock_start_invocation(&mut test_env).await;

    let invocation_status = test_env.storage().get_invocation_status(&id).await.unwrap();
    assert_that!(invocation_status, pat!(InvocationStatus::Invoked(_)));
    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
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
        &VirtualObjectStatus::Locked(InvocationId::mock_random()),
    )
    .await?;
    tx.commit().await.unwrap();

    // Start the invocation
    let invocation_id = fixtures::mock_start_invocation_with_invocation_target(
        &mut test_env,
        invocation_target.clone(),
    )
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
    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn awakeable_completion_received_before_entry() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

    // Send completion first
    let _ = test_env
        .apply(Command::InvocationResponse(InvocationResponse {
            target: JournalCompletionTarget::from_parts(invocation_id, 1, 0),
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
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::awakeable(None)),
            },
        })))
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

    // If we try to send the completion again, it should not be forwarded!

    let actions = test_env
        .apply(Command::InvocationResponse(InvocationResponse {
            target: JournalCompletionTarget::from_parts(invocation_id, 1, 0),
            result: ResponseResult::Success(Bytes::default()),
        }))
        .await;
    assert_that!(
        actions,
        not(contains(pat!(Action::ForwardCompletion {
            invocation_id: eq(invocation_id),
            completion: eq(Completion::new(
                1,
                CompletionResult::Success(Bytes::default())
            ))
        })))
    );

    let actions = test_env
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::Suspended {
                waiting_for_completed_entries: HashSet::from([1]),
            },
        })))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::Invoke {
            invocation_id: eq(invocation_id)
        }))
    );
    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn complete_awakeable_with_success() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

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
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry,
            },
        })))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        target: pat!(JournalCompletionTarget {
                            caller_id: eq(callee_invocation_id),
                            caller_completion_id: eq(callee_entry_index),
                        }),
                        result: pat!(ResponseResult::Success { .. })
                    }
                ))
            )
        }))
    );
    test_env.shutdown().await;
}

#[test(restate_core::test)]
async fn complete_awakeable_with_failure() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

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
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry,
            },
        })))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        target: pat!(JournalCompletionTarget {
                            caller_id: eq(callee_invocation_id),
                            caller_completion_id: eq(callee_entry_index),
                        }),
                        result: eq(ResponseResult::Failure(InvocationError::new(
                            codes::BAD_REQUEST,
                            "Some failure"
                        )))
                    }
                ))
            )
        }))
    );
    test_env.shutdown().await;
}

#[test(restate_core::test)]
async fn invoke_with_headers() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let service_id = ServiceId::mock_random();
    let invocation_id =
        fixtures::mock_start_invocation_with_service_id(&mut test_env, service_id.clone()).await;

    let actions = test_env
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::invoke(
                    InvokeRequest {
                        service_name: service_id.service_name,
                        handler_name: "MyMethod".into(),
                        parameter: Bytes::default(),
                        headers: vec![Header::new("foo", "bar")],
                        key: service_id.key,
                        idempotency_key: None,
                    },
                    None,
                )),
            },
        })))
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
    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn mutate_state() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let keyed_service_id = invocation_target.as_keyed_service_id().unwrap();
    let invocation_id = fixtures::mock_start_invocation_with_invocation_target(
        &mut test_env,
        invocation_target.clone(),
    )
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
            .unwrap()
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
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::End,
        })))
        .await;

    let all_states: HashMap<_, _> = test_env
        .storage
        .get_all_user_states_for_service(&keyed_service_id)
        .unwrap()
        .try_collect()
        .await?;

    assert_eq!(all_states, second_state_mutation);

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn clear_all_user_states() -> anyhow::Result<()> {
    let mut test_env = TestEnv::create().await;
    let service_id = ServiceId::new("MySvc", "my-key");

    // Fill with some state the service K/V store
    let mut txn = test_env.storage.transaction();
    txn.put_user_state(&service_id, b"my-key-1", b"my-val-1")
        .await?;
    txn.put_user_state(&service_id, b"my-key-2", b"my-val-2")
        .await?;
    txn.commit().await.unwrap();

    let invocation_id =
        fixtures::mock_start_invocation_with_service_id(&mut test_env, service_id.clone()).await;

    test_env
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::clear_all_state()),
            },
        })))
        .await;

    let states: Vec<restate_storage_api::Result<(Bytes, Bytes)>> = test_env
        .storage
        .get_all_user_states_for_service(&service_id)
        .unwrap()
        .collect()
        .await;
    assert_that!(states, empty());

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn get_state_keys() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let service_id = ServiceId::mock_random();
    let invocation_id =
        fixtures::mock_start_invocation_with_service_id(&mut test_env, service_id.clone()).await;

    // Mock some state
    let mut txn = test_env.storage.transaction();
    txn.put_user_state(&service_id, b"key1", b"value1").await?;
    txn.put_user_state(&service_id, b"key2", b"value2").await?;
    txn.commit().await.unwrap();

    let actions = test_env
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::get_state_keys(None)),
            },
        })))
        .await;

    // At this point we expect the completion to be forwarded to the invoker
    assert_that!(
        actions,
        contains(matchers::actions::forward_completion(
            invocation_id,
            matchers::completion(
                1,
                ProtobufRawEntryCodec::serialize_get_state_keys_completion(vec![
                    Bytes::copy_from_slice(b"key1"),
                    Bytes::copy_from_slice(b"key2"),
                ])
            )
        ))
    );
    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn get_invocation_id_entry() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

    let callee_1 = InvocationId::mock_random();
    let callee_2 = InvocationId::mock_random();

    // Mock some state
    // Add call and one way call journal entry
    let mut tx = test_env.storage.transaction();
    tx.put_journal_entry(&invocation_id, 1, &background_invoke_entry(callee_1))
        .unwrap();
    tx.put_journal_entry(&invocation_id, 2, &incomplete_invoke_entry(callee_2))
        .unwrap();
    let mut invocation_status = tx.get_invocation_status(&invocation_id).await.unwrap();
    invocation_status.get_journal_metadata_mut().unwrap().length = 3;
    tx.put_invocation_status(&invocation_id, &invocation_status)
        .unwrap();
    tx.commit().await.unwrap();

    let actions = test_env
        .apply_multiple(vec![
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                invocation_epoch: 0,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 3,
                    entry: ProtobufRawEntryCodec::serialize_enriched(
                        Entry::get_call_invocation_id(1, None),
                    ),
                },
            })),
            Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                invocation_epoch: 0,
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 4,
                    entry: ProtobufRawEntryCodec::serialize_enriched(
                        Entry::get_call_invocation_id(2, None),
                    ),
                },
            })),
        ])
        .await;

    // Assert completion is forwarded and stored
    assert_that!(
        actions,
        all!(
            contains(matchers::actions::forward_completion(
                invocation_id,
                success_completion(3, callee_1.to_string())
            )),
            contains(matchers::actions::forward_completion(
                invocation_id,
                success_completion(4, callee_2.to_string())
            ))
        )
    );

    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, 3)
            .await
            .unwrap(),
        some(is_entry(Entry::get_call_invocation_id(
            1,
            Some(GetCallInvocationIdResult::InvocationId(
                callee_1.to_string()
            ))
        )))
    );
    assert_that!(
        test_env
            .storage
            .get_journal_entry(&invocation_id, 4)
            .await
            .unwrap(),
        some(is_entry(Entry::get_call_invocation_id(
            2,
            Some(GetCallInvocationIdResult::InvocationId(
                callee_2.to_string()
            ))
        )))
    );

    test_env.shutdown().await;
}

#[restate_core::test]
async fn attach_invocation_entry() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

    let callee_invocation_id = InvocationId::mock_random();

    let actions = test_env
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::AttachInvocation(
                    AttachInvocationEntry {
                        target: AttachInvocationTarget::InvocationId(
                            callee_invocation_id.to_string().into(),
                        ),
                        result: None,
                    },
                )),
            },
        })))
        .await;
    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(pat!(
                    restate_types::invocation::AttachInvocationRequest {
                        invocation_query: eq(InvocationQuery::Invocation(callee_invocation_id)),
                        block_on_inflight: eq(true),
                        response_sink: eq(ServiceInvocationResponseSink::partition_processor(
                            invocation_id,
                            1,
                            0
                        )),
                    }
                ))
            )
        }))
    );

    test_env.shutdown().await;
}

#[restate_core::test]
async fn get_invocation_output_entry() {
    let mut test_env = TestEnv::create().await;
    let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

    let callee_invocation_id = InvocationId::mock_random();

    let actions = test_env
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::GetInvocationOutput(
                    GetInvocationOutputEntry {
                        target: AttachInvocationTarget::InvocationId(
                            callee_invocation_id.to_string().into(),
                        ),
                        result: None,
                    },
                )),
            },
        })))
        .await;

    assert_that!(
        actions,
        contains(pat!(Action::NewOutboxMessage {
            message: pat!(
                restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(pat!(
                    restate_types::invocation::AttachInvocationRequest {
                        invocation_query: eq(InvocationQuery::Invocation(callee_invocation_id)),
                        block_on_inflight: eq(false),
                        response_sink: eq(ServiceInvocationResponseSink::partition_processor(
                            invocation_id,
                            1,
                            0
                        )),
                    }
                ))
            )
        }))
    );

    // Let's try to complete it with not ready, this should forward empty
    let actions = test_env
        .apply(Command::InvocationResponse(InvocationResponse {
            target: JournalCompletionTarget::from_parts(invocation_id, 1, 0),
            result: NOT_READY_INVOCATION_ERROR.into(),
        }))
        .await;
    assert_that!(
        actions,
        contains(matchers::actions::forward_completion(
            invocation_id,
            eq(Completion::new(1, CompletionResult::Empty))
        ))
    );

    test_env.shutdown().await;
}

#[test(restate_core::test)]
async fn send_ingress_response_to_multiple_targets() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = InvocationId::mock_generate(&invocation_target);

    let request_id_1 = PartitionProcessorRpcRequestId::default();
    let request_id_2 = PartitionProcessorRpcRequestId::default();
    let request_id_3 = PartitionProcessorRpcRequestId::default();

    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            response_sink: Some(ServiceInvocationResponseSink::Ingress {
                request_id: request_id_1,
            }),
            ..ServiceInvocation::initialize(
                invocation_id,
                invocation_target.clone(),
                Source::Ingress(request_id_1),
            )
        })))
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
            request_id: request_id_2,
        },
    );
    invocation_status.get_response_sinks_mut().unwrap().insert(
        ServiceInvocationResponseSink::Ingress {
            request_id: request_id_3,
        },
    );
    txn.put_invocation_status(&invocation_id, &invocation_status)?;
    txn.commit().await.unwrap();

    // Now let's send the output entry
    let response_bytes = Bytes::from_static(b"123");
    let actions = test_env
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::JournalEntry {
                entry_index: 1,
                entry: ProtobufRawEntryCodec::serialize_enriched(Entry::output(
                    EntryResult::Success(response_bytes.clone()),
                )),
            },
        })))
        .await;
    // No ingress response is expected at this point because the invocation did not end yet
    assert_that!(actions, not(contains(pat!(Action::IngressResponse { .. }))));

    // Send the End Effect
    let actions = test_env
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::End,
        })))
        .await;
    // At this point we expect the completion to be forwarded to the invoker
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_1),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            })),
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_2),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            })),
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id_3),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            })),
        )
    );

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn truncate_outbox_from_empty() -> Result<(), Error> {
    // An outbox message with index 0 has been successfully processed, and must now be truncated
    let outbox_index = 0;

    let mut test_env = TestEnv::create().await;

    let _ = test_env.apply(Command::TruncateOutbox(outbox_index)).await;

    assert_that!(test_env.storage.get_outbox_message(0).await?, none());

    // The head catches up to the next available sequence number on truncation. Since we don't know
    // in advance whether we will get asked to truncate a range of more than one outbox message, we
    // explicitly track the head sequence number as the next position beyond the last known
    // truncation point. It's only safe to leave the head as None when the outbox is known to be
    // empty.
    assert_eq!(test_env.state_machine.outbox_head_seq_number, Some(1));

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn truncate_outbox_with_gap() -> Result<(), Error> {
    // The outbox contains items [3..=5], and the range must be truncated after message 5 is processed
    let outbox_head_index = 3;
    let outbox_tail_index = 5;

    let mut test_env = TestEnv::create_with_state_machine(StateMachine::new(
        0,
        outbox_tail_index,
        Some(outbox_head_index),
        PartitionKey::MIN..=PartitionKey::MAX,
        SemanticRestateVersion::unknown().clone(),
        EnumSet::empty(),
    ))
    .await;

    test_env
        .apply(Command::TruncateOutbox(outbox_tail_index))
        .await;

    assert_that!(test_env.storage.get_outbox_message(3).await?, none());
    assert_that!(test_env.storage.get_outbox_message(4).await?, none());
    assert_that!(test_env.storage.get_outbox_message(5).await?, none());

    assert_eq!(
        test_env.state_machine.outbox_head_seq_number,
        Some(outbox_tail_index + 1)
    );

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn consecutive_exclusive_handler_invocations_will_use_inbox() -> TestResult {
    let mut test_env = TestEnv::create().await;

    let invocation_target = InvocationTarget::mock_virtual_object();
    let first_invocation_id = InvocationId::mock_generate(&invocation_target);
    let keyed_service_id = invocation_target.as_keyed_service_id().unwrap();
    let second_invocation_id = InvocationId::mock_generate(&invocation_target);

    // Let's start the first invocation
    let actions = test_env
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id: first_invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        })))
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
        .apply(Command::Invoke(Box::new(ServiceInvocation {
            invocation_id: second_invocation_id,
            invocation_target: invocation_target.clone(),
            ..ServiceInvocation::mock()
        })))
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
            .unwrap()
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
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id: first_invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::End,
        })))
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
        .apply(Command::InvokerEffect(Box::new(Effect {
            invocation_id: second_invocation_id,
            invocation_epoch: 0,
            kind: InvokerEffectKind::End,
        })))
        .await;

    // After the second was completed too, the inbox is empty and the service is unlocked
    assert_that!(
        test_env
            .storage
            .inbox(&keyed_service_id)
            .unwrap()
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

    test_env.shutdown().await;
    Ok(())
}

#[test(restate_core::test)]
async fn deduplicate_requests_with_same_pp_rpc_request_id() -> TestResult {
    let mut test_env = TestEnv::create().await;
    let invocation_id = InvocationId::mock_random();

    let request_id = PartitionProcessorRpcRequestId::default();
    let service_invocation = {
        let mut si = ServiceInvocation::mock();
        si.invocation_id = invocation_id;
        si.response_sink = Some(ServiceInvocationResponseSink::Ingress { request_id });
        si.submit_notification_sink = Some(SubmitNotificationSink::Ingress { request_id });
        si.source = Source::Ingress(request_id);
        Box::new(si)
    };
    let actions = test_env
        .apply(Command::Invoke(service_invocation.clone()))
        .await;
    assert_that!(
        actions,
        all!(
            contains(pat!(Action::Invoke {
                invocation_id: eq(invocation_id),
            })),
            contains(pat!(Action::IngressSubmitNotification {
                request_id: eq(request_id),
                is_new_invocation: eq(true)
            }))
        )
    );

    // Applying this again won't generate Invoke action,
    // but will return same submit notification.
    let actions = test_env.apply(Command::Invoke(service_invocation)).await;
    assert_that!(
        actions,
        all!(
            not(contains(pat!(Action::Invoke {
                invocation_id: eq(invocation_id),
            }))),
            contains(pat!(Action::IngressSubmitNotification {
                request_id: eq(request_id),
                is_new_invocation: eq(true)
            }))
        )
    );

    test_env.shutdown().await;
    Ok(())
}
