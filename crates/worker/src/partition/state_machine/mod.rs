// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::metric_definitions::PARTITION_APPLY_COMMAND;
use crate::partition::storage::Transaction;
use command_interpreter::CommandInterpreter;
use metrics::counter;
use restate_types::message::MessageIndex;
use std::ops::RangeInclusive;

mod actions;
mod command_interpreter;
mod effect_interpreter;
mod effects;

pub use actions::Action;
pub use command_interpreter::StateReader;
pub use effect_interpreter::ActionCollector;
pub use effect_interpreter::StateStorage;
pub use effects::Effects;
use restate_types::identifiers::PartitionKey;
use restate_types::journal::raw::{RawEntryCodec, RawEntryCodecError};
use restate_wal_protocol::Command;

#[derive(Debug)]
pub struct StateMachine<Codec>(CommandInterpreter<Codec>);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to deserialize entry: {0}")]
    Codec(#[from] RawEntryCodecError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

impl<Codec> StateMachine<Codec> {
    pub fn new(
        inbox_seq_number: MessageIndex,
        outbox_seq_number: MessageIndex,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> Self {
        Self(CommandInterpreter::new(
            inbox_seq_number,
            outbox_seq_number,
            partition_key_range,
        ))
    }
}

impl<Codec: RawEntryCodec> StateMachine<Codec> {
    pub async fn apply<TransactionType: restate_storage_api::Transaction + Send>(
        &mut self,
        command: Command,
        effects: &mut Effects,
        transaction: &mut Transaction<TransactionType>,
        action_collector: &mut ActionCollector,
        is_leader: bool,
    ) -> Result<(), Error> {
        // Handle the command, returns the span_relation to use to log effects
        let command_type = command.name();
        let (fid, span_relation) = self.0.on_apply(command, effects, transaction).await?;
        counter!(PARTITION_APPLY_COMMAND, "command" => command_type).increment(1);

        // Log the effects
        effects.log(is_leader, fid, span_relation);

        // Interpret effects
        effect_interpreter::EffectInterpreter::<Codec>::interpret_effects(
            effects,
            transaction,
            action_collector,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::types::{InvokerEffect, InvokerEffectKind};
    use bytes::Bytes;
    use bytestring::ByteString;
    use futures::{StreamExt, TryStreamExt};
    use googletest::matcher::Matcher;
    use googletest::{all, assert_that, pat, property};
    use restate_invoker_api::InvokeInputJournal;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_storage_api::inbox_table::InboxTable;
    use restate_storage_api::invocation_status_table::{
        InvocationMetadata, InvocationStatus, InvocationStatusTable, ReadOnlyInvocationStatusTable,
    };
    use restate_storage_api::journal_table::{JournalEntry, ReadOnlyJournalTable};
    use restate_storage_api::outbox_table::OutboxTable;
    use restate_storage_api::state_table::{ReadOnlyStateTable, StateTable};
    use restate_storage_api::Transaction;
    use restate_storage_rocksdb::RocksDBStorage;
    use restate_test_util::matchers::*;
    use restate_types::errors::codes;
    use restate_types::identifiers::{
        FullInvocationId, InvocationId, PartitionId, PartitionKey, ServiceId,
    };
    use restate_types::ingress::IngressResponse;
    use restate_types::invocation::{
        InvocationResponse, InvocationTermination, MaybeFullInvocationId, ResponseResult,
        ServiceInvocation, ServiceInvocationResponseSink, Source,
    };
    use restate_types::journal::enriched::EnrichedRawEntry;
    use restate_types::journal::{Completion, CompletionResult, EntryResult};
    use restate_types::journal::{Entry, EntryType};
    use restate_types::state_mut::ExternalStateMutation;
    use restate_types::GenerationalNodeId;
    use std::collections::{HashMap, HashSet};
    use tempfile::tempdir;
    use test_log::test;
    use tracing::info;

    // Test utility to test the StateMachine
    pub struct MockStateMachine {
        state_machine: StateMachine<ProtobufRawEntryCodec>,
        // TODO for the time being we use rocksdb storage because we have no mocks for storage interfaces.
        //  Perhaps we could make these tests faster by having those.
        rocksdb_storage: RocksDBStorage,
        effects_buffer: Effects,
        signal: drain::Signal,
        writer_join_handle: restate_storage_rocksdb::RocksDBWriterJoinHandle,
    }

    impl Default for MockStateMachine {
        fn default() -> Self {
            MockStateMachine::new(0, 0)
        }
    }

    impl MockStateMachine {
        pub fn partition_id(&self) -> PartitionId {
            0
        }

        pub fn new(inbox_seq_number: MessageIndex, outbox_seq_number: MessageIndex) -> Self {
            let temp_dir = tempdir().unwrap();
            info!("Using RocksDB temp directory {}", temp_dir.path().display());
            let (rocksdb_storage, writer) = restate_storage_rocksdb::OptionsBuilder::default()
                .path(temp_dir.into_path())
                .build()
                .unwrap()
                .build()
                .unwrap();

            let (signal, watch) = drain::channel();
            let writer_join_handle = writer.run(watch);

            Self {
                state_machine: StateMachine::new(
                    inbox_seq_number,
                    outbox_seq_number,
                    PartitionKey::MIN..=PartitionKey::MAX,
                ),
                rocksdb_storage,
                effects_buffer: Default::default(),
                signal,
                writer_join_handle,
            }
        }

        pub async fn apply(&mut self, command: Command) -> Vec<Action> {
            let partition_id = self.partition_id();
            let mut transaction = crate::partition::storage::Transaction::new(
                partition_id,
                0..=PartitionKey::MAX,
                self.rocksdb_storage.transaction(),
            );
            let mut action_collector = ActionCollector::default();
            self.state_machine
                .apply(
                    command,
                    &mut self.effects_buffer,
                    &mut transaction,
                    &mut action_collector,
                    true,
                )
                .await
                .unwrap();

            transaction.commit().await.unwrap();

            action_collector
        }

        pub fn storage(&mut self) -> &mut RocksDBStorage {
            &mut self.rocksdb_storage
        }

        async fn shutdown(self) -> Result<(), anyhow::Error> {
            self.signal.drain().await;
            self.writer_join_handle.await??;

            Ok(())
        }
    }

    type TestResult = Result<(), anyhow::Error>;

    #[test(tokio::test)]
    async fn start_invocation() -> TestResult {
        let mut state_machine = MockStateMachine::default();
        let fid = mock_start_invocation(&mut state_machine).await;

        let invocation_status = state_machine
            .storage()
            .transaction()
            .get_invocation_status(&InvocationId::from(&fid))
            .await
            .unwrap();
        assert_that!(
            invocation_status,
            pat!(InvocationStatus::Invoked(pat!(InvocationMetadata {
                service_id: eq(fid.service_id)
            })))
        );

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn awakeable_completion_received_before_entry() -> TestResult {
        let mut state_machine = MockStateMachine::default();
        let fid = mock_start_invocation(&mut state_machine).await;

        // Send completion first
        let _ = state_machine
            .apply(Command::InvocationResponse(InvocationResponse {
                id: MaybeFullInvocationId::Full(fid.clone()),
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

        let actions = state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                full_invocation_id: fid.clone(),
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
                full_invocation_id: eq(fid.clone()),
                completion: eq(Completion::new(
                    1,
                    CompletionResult::Success(Bytes::default())
                ))
            }))
        );

        // The entry should be in storage
        let entry = state_machine
            .rocksdb_storage
            .transaction()
            .get_journal_entry(&InvocationId::from(&fid), 1)
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

        let actions = state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                full_invocation_id: fid.clone(),
                kind: InvokerEffectKind::Suspended {
                    waiting_for_completed_entries: HashSet::from([1]),
                },
            }))
            .await;

        assert_that!(
            actions,
            contains(pat!(Action::Invoke {
                full_invocation_id: eq(fid.clone()),
            }))
        );

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn kill_inboxed_invocation() -> anyhow::Result<()> {
        let mut state_machine = MockStateMachine::default();

        let fid = FullInvocationId::generate(ServiceId::new("svc", "key"));
        let inboxed_fid = FullInvocationId::generate(ServiceId::new("svc", "key"));
        let caller_fid = FullInvocationId::mock_random();

        let _ = state_machine
            .apply(Command::Invoke(ServiceInvocation {
                fid,
                ..ServiceInvocation::mock()
            }))
            .await;

        let _ = state_machine
            .apply(Command::Invoke(ServiceInvocation {
                fid: inboxed_fid.clone(),
                response_sink: Some(ServiceInvocationResponseSink::PartitionProcessor {
                    caller: caller_fid.clone(),
                    entry_index: 0,
                }),
                ..ServiceInvocation::mock()
            }))
            .await;

        let result = state_machine
            .storage()
            .transaction()
            .get_invocation(inboxed_fid.clone())
            .await?;

        // assert that inboxed invocation is in inbox
        assert!(result.is_some());

        let actions = state_machine
            .apply(Command::TerminateInvocation(InvocationTermination::kill(
                MaybeFullInvocationId::from(inboxed_fid.clone()),
            )))
            .await;

        let result = state_machine
            .storage()
            .transaction()
            .get_invocation(inboxed_fid.clone())
            .await?;

        // assert that inboxed invocation has been removed
        assert!(result.is_none());

        fn outbox_message_matcher(
            caller_fid: FullInvocationId,
        ) -> impl Matcher<ActualT = restate_storage_api::outbox_table::OutboxMessage> {
            pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        id: eq(MaybeFullInvocationId::Full(caller_fid)),
                        entry_index: eq(0),
                        result: pat!(ResponseResult::Failure(
                            eq(codes::ABORTED),
                            eq(ByteString::from_static("killed"))
                        ))
                    }
                ))
            )
        }

        assert_that!(
            actions,
            contains(pat!(Action::NewOutboxMessage {
                message: outbox_message_matcher(caller_fid.clone())
            }))
        );

        let partition_id = state_machine.partition_id();
        let outbox_message = state_machine
            .storage()
            .transaction()
            .get_next_outbox_message(partition_id, 0)
            .await?;

        assert_that!(
            outbox_message,
            some((ge(0), outbox_message_matcher(caller_fid.clone())))
        );

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn mutate_state() -> anyhow::Result<()> {
        let mut state_machine = MockStateMachine::default();
        let fid = mock_start_invocation(&mut state_machine).await;

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
            state_machine
                .rocksdb_storage
                .get_all_user_states(&fid.service_id)
                .count()
                .await,
            0
        );

        state_machine
            .apply(Command::PatchState(ExternalStateMutation {
                component_id: fid.service_id.clone(),
                version: None,
                state: first_state_mutation,
            }))
            .await;
        state_machine
            .apply(Command::PatchState(ExternalStateMutation {
                component_id: fid.service_id.clone(),
                version: None,
                state: second_state_mutation.clone(),
            }))
            .await;

        // terminating the ongoing invocation should trigger popping from the inbox until the
        // next invocation is found
        state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                full_invocation_id: fid.clone(),
                kind: InvokerEffectKind::End,
            }))
            .await;

        let all_states: HashMap<_, _> = state_machine
            .rocksdb_storage
            .get_all_user_states(&fid.service_id)
            .try_collect()
            .await?;

        assert_eq!(all_states, second_state_mutation);

        Ok(())
    }

    #[test(tokio::test)]
    async fn clear_all_user_states() -> anyhow::Result<()> {
        let service_id = ServiceId::new("MySvc", "my-key");

        let mut state_machine = MockStateMachine::default();

        // Fill with some state the service K/V store
        let mut txn = state_machine.rocksdb_storage.transaction();
        txn.put_user_state(&service_id, b"my-key-1", b"my-val-1")
            .await;
        txn.put_user_state(&service_id, b"my-key-2", b"my-val-2")
            .await;
        txn.commit().await.unwrap();

        let fid =
            mock_start_invocation_with_service_id(&mut state_machine, service_id.clone()).await;

        state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                full_invocation_id: fid.clone(),
                kind: InvokerEffectKind::JournalEntry {
                    entry_index: 1,
                    entry: ProtobufRawEntryCodec::serialize_enriched(Entry::clear_all_state()),
                },
            }))
            .await;

        let states: Vec<restate_storage_api::Result<(Bytes, Bytes)>> = state_machine
            .rocksdb_storage
            .get_all_user_states(&service_id)
            .collect()
            .await;
        assert_that!(states, empty());

        Ok(())
    }

    #[test(tokio::test)]
    async fn get_state_keys() -> TestResult {
        let mut state_machine = MockStateMachine::default();
        let fid = mock_start_invocation(&mut state_machine).await;

        // Mock some state
        let mut txn = state_machine.rocksdb_storage.transaction();
        txn.put_user_state(&fid.service_id, b"key1", b"value1")
            .await;
        txn.put_user_state(&fid.service_id, b"key2", b"value2")
            .await;
        txn.commit().await.unwrap();

        let actions = state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                full_invocation_id: fid.clone(),
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
                full_invocation_id: eq(fid.clone()),
                completion: eq(Completion::new(
                    1,
                    ProtobufRawEntryCodec::serialize_get_state_keys_completion(vec![
                        Bytes::copy_from_slice(b"key1"),
                        Bytes::copy_from_slice(b"key2"),
                    ])
                ))
            }))
        );

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn send_ingress_response_to_multiple_targets() -> TestResult {
        let mut state_machine = MockStateMachine::default();
        let fid = FullInvocationId::generate(ServiceId::new("MyObj", "MyKey"));
        let invocation_id = InvocationId::from(&fid);

        let actions = state_machine
            .apply(Command::Invoke(ServiceInvocation {
                fid: fid.clone(),
                method_name: ByteString::from("MyHandler"),
                argument: Default::default(),
                source: Source::Ingress,
                response_sink: Some(ServiceInvocationResponseSink::Ingress(
                    GenerationalNodeId::new(1, 1),
                )),
                span_context: Default::default(),
                headers: vec![],
                execution_time: None,
            }))
            .await;
        assert_that!(
            actions,
            contains(pat!(Action::Invoke {
                full_invocation_id: eq(fid.clone()),
                invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
            }))
        );

        // Let's add another ingress
        let mut txn = state_machine.rocksdb_storage.transaction();
        let mut invocation_status = txn.get_invocation_status(&invocation_id).await.unwrap();
        invocation_status
            .get_invocation_metadata_mut()
            .unwrap()
            .append_response_sink(ServiceInvocationResponseSink::Ingress(
                GenerationalNodeId::new(2, 2),
            ));
        txn.put_invocation_status(&invocation_id, invocation_status)
            .await;
        txn.commit().await.unwrap();

        // Now let's send the output entry
        let response_bytes = Bytes::from_static(b"123");
        let actions = state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                full_invocation_id: fid.clone(),
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
        let actions = state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                full_invocation_id: fid.clone(),
                kind: InvokerEffectKind::End,
            }))
            .await;
        // At this point we expect the completion to be forwarded to the invoker
        assert_that!(
            actions,
            all!(
                contains(pat!(Action::IngressResponse(pat!(IngressResponse {
                    target_node: eq(GenerationalNodeId::new(1, 1)),
                    response: eq(ResponseResult::Success(response_bytes.clone()))
                })))),
                contains(pat!(Action::IngressResponse(pat!(IngressResponse {
                    target_node: eq(GenerationalNodeId::new(2, 2)),
                    response: eq(ResponseResult::Success(response_bytes.clone()))
                })))),
            )
        );

        state_machine.shutdown().await
    }

    async fn mock_start_invocation_with_service_id(
        state_machine: &mut MockStateMachine,
        service_id: ServiceId,
    ) -> FullInvocationId {
        let fid = FullInvocationId::generate(service_id);

        let actions = state_machine
            .apply(Command::Invoke(ServiceInvocation {
                fid: fid.clone(),
                method_name: ByteString::from("MyMethod"),
                argument: Default::default(),
                source: Source::Ingress,
                response_sink: None,
                span_context: Default::default(),
                headers: vec![],
                execution_time: None,
            }))
            .await;

        assert_that!(
            actions,
            contains(pat!(Action::Invoke {
                full_invocation_id: eq(fid.clone()),
                invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
            }))
        );

        fid
    }

    async fn mock_start_invocation(state_machine: &mut MockStateMachine) -> FullInvocationId {
        mock_start_invocation_with_service_id(
            state_machine,
            ServiceId::new("MySvc", Bytes::default()),
        )
        .await
    }
}
