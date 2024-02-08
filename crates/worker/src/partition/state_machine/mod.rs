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

mod actions;
mod command_interpreter;
mod commands;
mod dedup;
mod effect_interpreter;
mod effects;

pub use actions::Action;
pub use command_interpreter::StateReader;
pub use commands::{
    AckCommand, AckMode, AckResponse, AckTarget, Command, DeduplicationSource, IngressAckResponse,
    ShuffleDeduplicationResponse,
};
pub use dedup::DeduplicatingStateMachine;
pub use effect_interpreter::StateStorage;
pub use effect_interpreter::{ActionCollector, InterpretationResult};
pub use effects::Effects;
use restate_types::journal::raw::{RawEntryCodec, RawEntryCodecError};

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
    pub fn new(inbox_seq_number: MessageIndex, outbox_seq_number: MessageIndex) -> Self {
        Self(CommandInterpreter::new(inbox_seq_number, outbox_seq_number))
    }
}

impl<Codec: RawEntryCodec> StateMachine<Codec> {
    pub async fn apply<
        TransactionType: restate_storage_api::Transaction + Send,
        Collector: ActionCollector,
    >(
        &mut self,
        command: Command,
        effects: &mut Effects,
        mut transaction: Transaction<TransactionType>,
        message_collector: Collector,
        is_leader: bool,
    ) -> Result<InterpretationResult<Transaction<TransactionType>, Collector>, Error> {
        // Handle the command, returns the span_relation to use to log effects
        let command_type = command.type_human();
        let (fid, span_relation) = self.0.on_apply(command, effects, &mut transaction).await?;
        counter!(PARTITION_APPLY_COMMAND, "command" => command_type).increment(1);

        // Log the effects
        effects.log(is_leader, fid, span_relation);

        // Interpret effects
        effect_interpreter::EffectInterpreter::<Codec>::interpret_effects(
            effects,
            transaction,
            message_collector,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;
    use bytestring::ByteString;
    use futures::{StreamExt, TryStreamExt};
    use googletest::matcher::Matcher;
    use googletest::{all, assert_that, pat, property};
    use tempfile::tempdir;
    use test_log::test;
    use tracing::info;

    use crate::partition::services::non_deterministic::{Effect, Effects as NBISEffects};
    use crate::partition::types::{InvokerEffect, InvokerEffectKind};

    use restate_invoker_api::InvokeInputJournal;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_storage_api::inbox_table::InboxTable;
    use restate_storage_api::journal_table::{JournalEntry, ReadOnlyJournalTable};
    use restate_storage_api::outbox_table::OutboxTable;
    use restate_storage_api::state_table::{ReadOnlyStateTable, StateTable};
    use restate_storage_api::status_table::{
        InvocationMetadata, JournalMetadata, NotificationTarget, ReadOnlyStatusTable,
    };
    use restate_storage_api::status_table::{InvocationStatus, StatusTable};
    use restate_storage_api::Transaction;
    use restate_storage_rocksdb::RocksDBStorage;
    use restate_test_util::matchers::*;
    use restate_types::errors::UserErrorCode;
    use restate_types::identifiers::{
        FullInvocationId, InvocationUuid, PartitionId, PartitionKey, ServiceId, WithPartitionKey,
    };
    use restate_types::invocation::{
        InvocationResponse, InvocationTermination, MaybeFullInvocationId, ResponseResult,
        ServiceInvocation, ServiceInvocationResponseSink, Source,
    };
    use restate_types::journal::enriched::EnrichedRawEntry;
    use restate_types::journal::{Completion, CompletionResult};
    use restate_types::journal::{Entry, EntryType};
    use restate_types::state_mut::ExternalStateMutation;

    type VecActionCollector = Vec<Action>;

    impl ActionCollector for VecActionCollector {
        fn collect(&mut self, message: Action) {
            self.push(message)
        }
    }

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
                .path(temp_dir.into_path().to_str().unwrap().to_string())
                .build()
                .unwrap()
                .build()
                .unwrap();

            let (signal, watch) = drain::channel();
            let writer_join_handle = writer.run(watch);

            Self {
                state_machine: StateMachine::new(inbox_seq_number, outbox_seq_number),
                rocksdb_storage,
                effects_buffer: Default::default(),
                signal,
                writer_join_handle,
            }
        }

        pub async fn apply(&mut self, command: Command) -> Vec<Action> {
            let partition_id = self.partition_id();
            let transaction = self.rocksdb_storage.transaction();
            self.state_machine
                .apply(
                    command,
                    &mut self.effects_buffer,
                    crate::partition::storage::Transaction::new(
                        partition_id,
                        0..=PartitionKey::MAX,
                        transaction,
                    ),
                    VecActionCollector::default(),
                    true,
                )
                .await
                .unwrap()
                .commit()
                .await
                .unwrap()
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
            .get_invocation_status(&fid.service_id)
            .await
            .unwrap()
            .unwrap();
        assert_that!(
            invocation_status,
            pat!(InvocationStatus::Invoked(pat!(InvocationMetadata {
                invocation_uuid: eq(fid.invocation_uuid)
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
            .apply(Command::Response(InvocationResponse {
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
            .apply(Command::Invoker(InvokerEffect {
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
            .get_journal_entry(&fid.service_id, 1)
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
            .apply(Command::Invoker(InvokerEffect {
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

        let fid = FullInvocationId::generate("svc", "key");
        let inboxed_fid = FullInvocationId::generate("svc", "key");
        let caller_fid = FullInvocationId::mock_random();

        let _ = state_machine
            .apply(Command::Invocation(ServiceInvocation {
                fid,
                ..ServiceInvocation::mock()
            }))
            .await;

        let _ = state_machine
            .apply(Command::Invocation(ServiceInvocation {
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
                            eq(UserErrorCode::Aborted),
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
            .apply(Command::ExternalStateMutation(ExternalStateMutation {
                service_id: fid.service_id.clone(),
                version: None,
                state: first_state_mutation,
            }))
            .await;
        state_machine
            .apply(Command::ExternalStateMutation(ExternalStateMutation {
                service_id: fid.service_id.clone(),
                version: None,
                state: second_state_mutation.clone(),
            }))
            .await;

        // terminating the ongoing invocation should trigger popping from the inbox until the
        // next invocation is found
        state_machine
            .apply(Command::Invoker(InvokerEffect {
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
            .apply(Command::Invoker(InvokerEffect {
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

    mod virtual_invocation {
        use super::*;

        use googletest::{all, assert_that, elements_are, pat};
        use test_log::test;

        use restate_storage_api::status_table::{ReadOnlyStatusTable, StatusTimestamps};
        use restate_types::identifiers::InvocationId;
        use restate_types::invocation::{
            InvocationResponse, MaybeFullInvocationId, ResponseResult,
        };
        use restate_types::journal::enriched::EnrichedRawEntry;
        use restate_types::journal::EntryType;
        use restate_types::journal::{AwakeableEntry, Completion, CompletionResult, Entry};

        #[test(tokio::test)]
        async fn create() -> TestResult {
            let mut state_machine = MockStateMachine::default();

            // Notification receiving service
            let notification_service_service_id =
                ServiceId::new("NotificationReceiver", Bytes::copy_from_slice(b"456"));
            let notification_service_target = NotificationTarget {
                service: notification_service_service_id.clone(),
                method: "InternalMethod".to_string(),
            };

            // Fill the InvocationStatus for notification_service_service_id
            let fid_virtual_invocation_creator = FullInvocationId::with_service_id(
                notification_service_service_id.clone(),
                InvocationUuid::new(),
            );
            let mut t = state_machine.storage().transaction();
            t.put_invocation_status(
                &notification_service_service_id,
                InvocationStatus::Invoked(InvocationMetadata {
                    invocation_uuid: fid_virtual_invocation_creator.invocation_uuid,
                    ..InvocationMetadata::mock()
                }),
            )
            .await;
            t.commit().await.unwrap();

            // Virtual invocation identifiers
            let virtual_invocation_service_id =
                ServiceId::new("Virtual", Bytes::copy_from_slice(b"123"));
            let virtual_invocation_invocation_uuid = InvocationUuid::new();

            let actions = state_machine
                .apply(Command::BuiltInInvoker(NBISEffects::new(
                    fid_virtual_invocation_creator,
                    vec![Effect::CreateJournal {
                        service_id: virtual_invocation_service_id.clone(),
                        invocation_uuid: virtual_invocation_invocation_uuid,
                        span_context: Default::default(),
                        completion_notification_target: notification_service_target.clone(),
                        kill_notification_target: notification_service_target.clone(),
                    }],
                )))
                .await;

            assert!(actions.is_empty());

            let invocation_status = state_machine
                .storage()
                .transaction()
                .get_invocation_status(&virtual_invocation_service_id)
                .await
                .unwrap()
                .unwrap();
            assert_that!(
                invocation_status,
                pat!(InvocationStatus::Virtual {
                    invocation_uuid: eq(virtual_invocation_invocation_uuid),
                    journal_metadata: pat!(JournalMetadata { length: eq(0) }),
                    completion_notification_target: eq(notification_service_target),
                })
            );

            let (resolved_service_id, resolved_invocation_status) = state_machine
                .storage()
                .transaction()
                .get_invocation_status_from(
                    virtual_invocation_service_id.partition_key(),
                    virtual_invocation_invocation_uuid,
                )
                .await
                .unwrap()
                .unwrap();
            assert_that!(resolved_service_id, eq(virtual_invocation_service_id));
            assert_that!(resolved_invocation_status, eq(invocation_status));

            state_machine.shutdown().await
        }

        #[test(tokio::test)]
        async fn write_entry_and_notify_completion() -> TestResult {
            let mut state_machine = MockStateMachine::default();

            // Virtual invocation identifiers
            let virtual_invocation_service_id =
                ServiceId::new("Virtual", Bytes::copy_from_slice(b"123"));
            let virtual_invocation_invocation_uuid = InvocationUuid::new();

            // Notification receiving service
            let notification_service_service_id =
                ServiceId::new("NotificationReceiver", Bytes::copy_from_slice(b"456"));
            let notification_service_target = NotificationTarget {
                service: notification_service_service_id.clone(),
                method: "Handle".to_string(),
            };
            let fid_virtual_invocation_creator = FullInvocationId::with_service_id(
                notification_service_service_id.clone(),
                InvocationUuid::new(),
            );

            // Setup a valid journal for the virtual invocation and the virtual invocation creator
            let mut t = state_machine.storage().transaction();
            t.put_invocation_status(
                &virtual_invocation_service_id,
                InvocationStatus::Virtual {
                    invocation_uuid: virtual_invocation_invocation_uuid,
                    journal_metadata: JournalMetadata {
                        length: 0,
                        span_context: Default::default(),
                    },
                    timestamps: StatusTimestamps::now(),
                    completion_notification_target: notification_service_target.clone(),
                    kill_notification_target: notification_service_target.clone(),
                },
            )
            .await;
            t.put_invocation_status(
                &notification_service_service_id,
                InvocationStatus::Invoked(InvocationMetadata {
                    invocation_uuid: fid_virtual_invocation_creator.invocation_uuid,
                    ..InvocationMetadata::mock()
                }),
            )
            .await;
            t.commit().await.unwrap();

            // Create the entry
            let actions = state_machine
                .apply(Command::BuiltInInvoker(NBISEffects::new(
                    fid_virtual_invocation_creator,
                    vec![
                        Effect::StoreEntry {
                            service_id: virtual_invocation_service_id.clone(),
                            entry_index: 0,
                            journal_entry: ProtobufRawEntryCodec::serialize_enriched(
                                Entry::Awakeable(AwakeableEntry { result: None }),
                            ),
                        },
                        Effect::End(None),
                    ],
                )))
                .await;

            assert!(actions.is_empty());

            // Assert entry is stored
            assert_that!(
                state_machine
                    .storage()
                    .transaction()
                    .get_invocation_status(&virtual_invocation_service_id)
                    .await
                    .unwrap()
                    .unwrap(),
                pat!(InvocationStatus::Virtual {
                    journal_metadata: pat!(JournalMetadata { length: eq(1) }),
                })
            );
            assert_that!(
                state_machine
                    .storage()
                    .transaction()
                    .get_journal_entry(&virtual_invocation_service_id, 0)
                    .await
                    .unwrap()
                    .unwrap(),
                pat!(JournalEntry::Entry(property!(
                    EnrichedRawEntry.ty(),
                    eq(EntryType::Awakeable)
                )))
            );

            // Now send completion
            let actions = state_machine
                .apply(Command::Response(InvocationResponse {
                    id: MaybeFullInvocationId::Partial(InvocationId::new(
                        virtual_invocation_service_id.partition_key(),
                        virtual_invocation_invocation_uuid,
                    )),
                    entry_index: 0,
                    result: ResponseResult::Success(Bytes::from_static(b"123")),
                }))
                .await;

            // Assert entry has been updated
            assert_that!(
                state_machine
                    .storage()
                    .transaction()
                    .get_journal_entry(&virtual_invocation_service_id, 0)
                    .await
                    .unwrap()
                    .unwrap(),
                pat!(JournalEntry::Entry(all!(
                    property!(EnrichedRawEntry.ty(), eq(EntryType::Awakeable)),
                    predicate(|e: &EnrichedRawEntry| e.header().is_completed() == Some(true))
                )))
            );
            // Assert notify action is created
            assert_that!(
                actions,
                elements_are![pat!(Action::NotifyVirtualJournalCompletion {
                    target_service: eq(notification_service_service_id),
                    method_name: eq("Handle".to_string()),
                    invocation_uuid: eq(virtual_invocation_invocation_uuid),
                    completion: eq(Completion::new(
                        0,
                        CompletionResult::Success(Bytes::from_static(b"123"))
                    )),
                })]
            );

            state_machine.shutdown().await
        }
    }

    async fn mock_start_invocation_with_service_id(
        state_machine: &mut MockStateMachine,
        service_id: ServiceId,
    ) -> FullInvocationId {
        let fid = FullInvocationId::with_service_id(service_id, InvocationUuid::new());

        let actions = state_machine
            .apply(Command::Invocation(ServiceInvocation {
                fid: fid.clone(),
                method_name: ByteString::from("MyMethod"),
                argument: Default::default(),
                source: Source::Ingress,
                response_sink: None,
                span_context: Default::default(),
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
