// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::storage::Transaction;
use command_interpreter::CommandInterpreter;
use dedup::DeduplicatingCommandInterpreter;
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
pub use effect_interpreter::StateStorage;
pub use effect_interpreter::{ActionCollector, InterpretationResult};
pub use effects::Effects;
use restate_types::journal::raw::{RawEntryCodec, RawEntryCodecError};

pub struct StateMachine<Codec>(DeduplicatingCommandInterpreter<Codec>);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to deserialize entry: {0}")]
    Codec(#[from] RawEntryCodecError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

impl<Codec> StateMachine<Codec> {
    pub fn new(inbox_seq_number: MessageIndex, outbox_seq_number: MessageIndex) -> Self {
        Self(DeduplicatingCommandInterpreter::new(
            CommandInterpreter::new(inbox_seq_number, outbox_seq_number),
        ))
    }
}

impl<Codec: RawEntryCodec> StateMachine<Codec> {
    pub async fn apply<
        TransactionType: restate_storage_api::Transaction + Send,
        Collector: ActionCollector,
    >(
        &mut self,
        command: AckCommand,
        effects: &mut Effects,
        mut transaction: Transaction<TransactionType>,
        message_collector: Collector,
        is_leader: bool,
    ) -> Result<InterpretationResult<Transaction<TransactionType>, Collector>, Error> {
        // Handle the command, returns the span_relation to use to log effects
        let (fid, span_relation) = self.0.on_apply(command, effects, &mut transaction).await?;

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

    use crate::partition::services::non_deterministic::{Effect, Effects as NBISEffects};
    use bytes::Bytes;
    use bytestring::ByteString;
    use googletest::{all, assert_that, pat};
    use restate_invoker_api::InvokeInputJournal;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_storage_api::journal_table::{JournalEntry, JournalTable};
    use restate_storage_api::status_table::{
        CompletionNotificationTarget, InvocationMetadata, JournalMetadata,
    };
    use restate_storage_api::status_table::{InvocationStatus, StatusTable};
    use restate_storage_api::Transaction;
    use restate_storage_rocksdb::RocksDBStorage;
    use restate_test_util::matchers::*;
    use restate_test_util::test;
    use restate_types::identifiers::{
        FullInvocationId, InvocationUuid, PartitionKey, ServiceId, WithPartitionKey,
    };
    use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext};
    use tempfile::tempdir;
    use tracing::info;

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
    }

    impl Default for MockStateMachine {
        fn default() -> Self {
            MockStateMachine::new(0, 0)
        }
    }

    impl MockStateMachine {
        pub fn new(inbox_seq_number: MessageIndex, outbox_seq_number: MessageIndex) -> Self {
            let temp_dir = tempdir().unwrap();
            info!("Using RocksDB temp directory {}", temp_dir.path().display());
            Self {
                state_machine: StateMachine::new(inbox_seq_number, outbox_seq_number),
                rocksdb_storage: restate_storage_rocksdb::OptionsBuilder::default()
                    .path(temp_dir.into_path().to_str().unwrap().to_string())
                    .build()
                    .unwrap()
                    .build()
                    .unwrap(),
                effects_buffer: Default::default(),
            }
        }

        pub async fn apply(&mut self, command: AckCommand) -> Vec<Action> {
            let transaction = self.rocksdb_storage.transaction();
            self.state_machine
                .apply(
                    command,
                    &mut self.effects_buffer,
                    crate::partition::storage::Transaction::new(
                        0,
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

        pub async fn apply_cmd(&mut self, command: Command) -> Vec<Action> {
            let transaction = self.rocksdb_storage.transaction();
            self.state_machine
                .apply(
                    AckCommand::no_ack(command),
                    &mut self.effects_buffer,
                    crate::partition::storage::Transaction::new(
                        0,
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

        pub fn storage(&self) -> &RocksDBStorage {
            &self.rocksdb_storage
        }
    }

    #[test(tokio::test)]
    async fn start_invocation() {
        let mut state_machine = MockStateMachine::default();

        let fid = FullInvocationId::generate("MySvc", Bytes::default());

        let actions = state_machine
            .apply(AckCommand::no_ack(Command::Invocation(ServiceInvocation {
                fid: fid.clone(),
                method_name: ByteString::from("MyMethod"),
                argument: Default::default(),
                response_sink: None,
                span_context: Default::default(),
            })))
            .await;

        assert_that!(
            actions,
            contains(pat!(Action::Invoke {
                full_invocation_id: eq(fid.clone()),
                invoke_input_journal: pat!(InvokeInputJournal::CachedJournal(_, _))
            }))
        );

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
        )
    }

    mod virtual_invocation {
        use super::*;

        use googletest::{all, assert_that, elements_are, pat};
        use restate_storage_api::status_table::StatusStatistics;
        use restate_test_util::test;
        use restate_types::identifiers::InvocationId;
        use restate_types::invocation::{
            InvocationResponse, MaybeFullInvocationId, ResponseResult,
        };
        use restate_types::journal::enriched::EnrichedRawEntry;
        use restate_types::journal::raw::EntryHeader;
        use restate_types::journal::EntryType;
        use restate_types::journal::{AwakeableEntry, Completion, CompletionResult, Entry};

        #[test(tokio::test)]
        async fn create() {
            let mut state_machine = MockStateMachine::default();

            // Notification receiving service
            let notification_service_service_id =
                ServiceId::new("NotificationReceiver", Bytes::copy_from_slice(b"456"));
            let notification_service_target = CompletionNotificationTarget {
                service: notification_service_service_id.clone(),
                method: "InternalMethod".to_string(),
            };

            // Fill the InvocationStatus for notification_service_service_id
            let fid_virtual_invocation_creator = FullInvocationId::with_service_id(
                notification_service_service_id.clone(),
                InvocationUuid::now_v7(),
            );
            let mut t = state_machine.storage().transaction();
            t.put_invocation_status(
                &notification_service_service_id,
                InvocationStatus::Invoked(InvocationMetadata::new(
                    fid_virtual_invocation_creator.invocation_uuid,
                    JournalMetadata::new(0, ServiceInvocationSpanContext::empty()),
                    None,
                    ByteString::from_static("OtherMethod"),
                    None,
                    StatusStatistics::default(),
                )),
            )
            .await;
            t.commit().await.unwrap();

            // Virtual invocation identifiers
            let virtual_invocation_service_id =
                ServiceId::new("Virtual", Bytes::copy_from_slice(b"123"));
            let virtual_invocation_invocation_uuid = InvocationUuid::now_v7();

            let actions = state_machine
                .apply_cmd(Command::BuiltInInvoker(NBISEffects::new(
                    fid_virtual_invocation_creator,
                    vec![Effect::CreateJournal {
                        service_id: virtual_invocation_service_id.clone(),
                        invocation_uuid: virtual_invocation_invocation_uuid,
                        span_context: Default::default(),
                        notification_target: notification_service_target.clone(),
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
        }

        #[test(tokio::test)]
        async fn write_entry_and_notify_completion() {
            let mut state_machine = MockStateMachine::default();

            // Virtual invocation identifiers
            let virtual_invocation_service_id =
                ServiceId::new("Virtual", Bytes::copy_from_slice(b"123"));
            let virtual_invocation_invocation_uuid = InvocationUuid::now_v7();

            // Notification receiving service
            let notification_service_service_id =
                ServiceId::new("NotificationReceiver", Bytes::copy_from_slice(b"456"));
            let notification_service_target = CompletionNotificationTarget {
                service: notification_service_service_id.clone(),
                method: "Handle".to_string(),
            };
            let fid_virtual_invocation_creator = FullInvocationId::with_service_id(
                notification_service_service_id.clone(),
                InvocationUuid::now_v7(),
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
                    stats: Default::default(),
                    completion_notification_target: notification_service_target.clone(),
                },
            )
            .await;
            t.put_invocation_status(
                &notification_service_service_id,
                InvocationStatus::Invoked(InvocationMetadata::new(
                    fid_virtual_invocation_creator.invocation_uuid,
                    JournalMetadata::new(0, ServiceInvocationSpanContext::empty()),
                    None,
                    ByteString::from_static("OtherMethod"),
                    None,
                    StatusStatistics::default(),
                )),
            )
            .await;
            t.commit().await.unwrap();

            // Create the entry
            let actions = state_machine
                .apply_cmd(Command::BuiltInInvoker(NBISEffects::new(
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
                .apply_cmd(Command::Response(InvocationResponse {
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
                    predicate(|e: &EnrichedRawEntry| e.header.is_completed() == Some(true))
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
            )
        }
    }
}
