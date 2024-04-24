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
        self.0.on_apply(command, effects, transaction).await?;
        counter!(PARTITION_APPLY_COMMAND, "command" => command_type).increment(1);

        // Log the effects
        effects.log(is_leader);

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
    use assert2::assert;
    use bytes::Bytes;
    use bytestring::ByteString;
    use futures::{StreamExt, TryStreamExt};
    use googletest::matcher::Matcher;
    use googletest::{all, assert_that, pat, property};
    use restate_core::{task_center, TaskCenterBuilder};
    use restate_invoker_api::InvokeInputJournal;
    use restate_rocksdb::RocksDbManager;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
        ReadOnlyInvocationStatusTable,
    };
    use restate_storage_api::journal_table::{JournalEntry, ReadOnlyJournalTable};
    use restate_storage_api::outbox_table::OutboxTable;
    use restate_storage_api::service_status_table::{
        VirtualObjectStatus, VirtualObjectStatusTable,
    };
    use restate_storage_api::state_table::{ReadOnlyStateTable, StateTable};
    use restate_storage_api::Transaction;
    use restate_storage_rocksdb::RocksDBStorage;
    use restate_test_util::matchers::*;
    use restate_types::arc_util::Constant;
    use restate_types::config::{CommonOptions, WorkerOptions};
    use restate_types::errors::KILLED_INVOCATION_ERROR;
    use restate_types::identifiers::{InvocationId, PartitionId, PartitionKey, ServiceId};
    use restate_types::ingress::IngressResponse;
    use restate_types::invocation::{
        HandlerType, InvocationResponse, InvocationTarget, InvocationTermination, ResponseResult,
        ServiceInvocation, ServiceInvocationResponseSink, Source,
    };
    use restate_types::journal::enriched::EnrichedRawEntry;
    use restate_types::journal::{Completion, CompletionResult, EntryResult};
    use restate_types::journal::{Entry, EntryType};
    use restate_types::state_mut::ExternalStateMutation;
    use restate_types::GenerationalNodeId;
    use std::collections::{HashMap, HashSet};
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

    impl MockStateMachine {
        pub fn partition_id(&self) -> PartitionId {
            0
        }

        pub async fn create() -> Self {
            task_center().run_in_scope_sync("db-manager-init", None, || {
                RocksDbManager::init(Constant::new(CommonOptions::default()))
            });
            let worker_options = WorkerOptions::default();
            info!(
                "Using RocksDB temp directory {}",
                worker_options.storage.data_dir().display()
            );
            let (rocksdb_storage, writer) = restate_storage_rocksdb::RocksDBStorage::open(
                Constant::new(worker_options.storage.clone()),
                Constant::new(worker_options.storage.rocksdb),
            )
            .await
            .unwrap();

            let (signal, watch) = drain::channel();
            let writer_join_handle = writer.run(watch);

            Self {
                state_machine: StateMachine::new(
                    0, /* inbox_seq_number */
                    0, /* outbox_seq_number */
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
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let mut state_machine = tc
            .run_in_scope("mock-state-machine", None, MockStateMachine::create())
            .await;
        let id = mock_start_invocation(&mut state_machine).await;

        let invocation_status = state_machine
            .storage()
            .transaction()
            .get_invocation_status(&id)
            .await
            .unwrap();
        assert_that!(invocation_status, pat!(InvocationStatus::Invoked(_)));

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn shared_invocation_skips_inbox() -> TestResult {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let mut state_machine = tc
            .run_in_scope("mock-state-machine", None, MockStateMachine::create())
            .await;

        let invocation_target =
            InvocationTarget::virtual_object("MySvc", "MyKey", "MyHandler", HandlerType::Shared);

        // Let's lock the virtual object
        let mut tx = state_machine.rocksdb_storage.transaction();
        tx.put_virtual_object_status(
            &invocation_target.as_keyed_service_id().unwrap(),
            VirtualObjectStatus::Locked(InvocationId::mock_random()),
        )
        .await;
        tx.commit().await.unwrap();

        // Start the invocation
        let invocation_id = mock_start_invocation_with_invocation_target(
            &mut state_machine,
            invocation_target.clone(),
        )
        .await;

        // Should be in invoked status
        let invocation_status = state_machine
            .storage()
            .transaction()
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

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn awakeable_completion_received_before_entry() -> TestResult {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let mut state_machine = tc
            .run_in_scope("mock-state-machine", None, MockStateMachine::create())
            .await;
        let invocation_id = mock_start_invocation(&mut state_machine).await;

        // Send completion first
        let _ = state_machine
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

        let actions = state_machine
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
        let entry = state_machine
            .rocksdb_storage
            .transaction()
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

        let actions = state_machine
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

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn kill_inboxed_invocation() -> anyhow::Result<()> {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let mut state_machine = tc
            .run_in_scope("mock-state-machine", None, MockStateMachine::create())
            .await;

        let (invocation_id, invocation_target) =
            InvocationId::mock_with(InvocationTarget::mock_virtual_object());
        let (inboxed_id, inboxed_target) = InvocationId::mock_with(invocation_target.clone());
        let caller_id = InvocationId::mock_random();

        let _ = state_machine
            .apply(Command::Invoke(ServiceInvocation {
                invocation_id,
                invocation_target: invocation_target.clone(),
                ..ServiceInvocation::mock()
            }))
            .await;

        let _ = state_machine
            .apply(Command::Invoke(ServiceInvocation {
                invocation_id: inboxed_id,
                invocation_target: inboxed_target,
                response_sink: Some(ServiceInvocationResponseSink::PartitionProcessor {
                    caller: caller_id,
                    entry_index: 0,
                }),
                ..ServiceInvocation::mock()
            }))
            .await;

        let current_invocation_status = state_machine
            .storage()
            .transaction()
            .get_invocation_status(&inboxed_id)
            .await?;

        // assert that inboxed invocation is in invocation_status
        assert!(let InvocationStatus::Inboxed(_) = current_invocation_status);

        let actions = state_machine
            .apply(Command::TerminateInvocation(InvocationTermination::kill(
                inboxed_id,
            )))
            .await;

        let current_invocation_status = state_machine
            .storage()
            .transaction()
            .get_invocation_status(&inboxed_id)
            .await?;

        // assert that invocation status was removed
        assert!(let InvocationStatus::Free = current_invocation_status);

        fn outbox_message_matcher(
            caller_id: InvocationId,
        ) -> impl Matcher<ActualT = restate_storage_api::outbox_table::OutboxMessage> {
            pat!(
                restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                    restate_types::invocation::InvocationResponse {
                        id: eq(caller_id),
                        entry_index: eq(0),
                        result: eq(ResponseResult::Failure(KILLED_INVOCATION_ERROR))
                    }
                ))
            )
        }

        assert_that!(
            actions,
            contains(pat!(Action::NewOutboxMessage {
                message: outbox_message_matcher(caller_id)
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
            some((ge(0), outbox_message_matcher(caller_id)))
        );

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn mutate_state() -> anyhow::Result<()> {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let mut state_machine = tc
            .run_in_scope("mock-state-machine", None, MockStateMachine::create())
            .await;
        let invocation_target = InvocationTarget::mock_virtual_object();
        let keyed_service_id = invocation_target.as_keyed_service_id().unwrap();
        let invocation_id = mock_start_invocation_with_invocation_target(
            &mut state_machine,
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
            state_machine
                .rocksdb_storage
                .get_all_user_states(&keyed_service_id)
                .count()
                .await,
            0
        );

        state_machine
            .apply(Command::PatchState(ExternalStateMutation {
                service_id: keyed_service_id.clone(),
                version: None,
                state: first_state_mutation,
            }))
            .await;
        state_machine
            .apply(Command::PatchState(ExternalStateMutation {
                service_id: keyed_service_id.clone(),
                version: None,
                state: second_state_mutation.clone(),
            }))
            .await;

        // terminating the ongoing invocation should trigger popping from the inbox until the
        // next invocation is found
        state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::End,
            }))
            .await;

        let all_states: HashMap<_, _> = state_machine
            .rocksdb_storage
            .get_all_user_states(&keyed_service_id)
            .try_collect()
            .await?;

        assert_eq!(all_states, second_state_mutation);

        Ok(())
    }

    #[test(tokio::test)]
    async fn clear_all_user_states() -> anyhow::Result<()> {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let mut state_machine = tc
            .run_in_scope("mock-state-machine", None, MockStateMachine::create())
            .await;
        let service_id = ServiceId::new("MySvc", "my-key");

        // Fill with some state the service K/V store
        let mut txn = state_machine.rocksdb_storage.transaction();
        txn.put_user_state(&service_id, b"my-key-1", b"my-val-1")
            .await;
        txn.put_user_state(&service_id, b"my-key-2", b"my-val-2")
            .await;
        txn.commit().await.unwrap();

        let invocation_id =
            mock_start_invocation_with_service_id(&mut state_machine, service_id.clone()).await;

        state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                invocation_id,
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
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let mut state_machine = tc
            .run_in_scope("mock-state-machine", None, MockStateMachine::create())
            .await;
        let service_id = ServiceId::mock_random();
        let invocation_id =
            mock_start_invocation_with_service_id(&mut state_machine, service_id.clone()).await;

        // Mock some state
        let mut txn = state_machine.rocksdb_storage.transaction();
        txn.put_user_state(&service_id, b"key1", b"value1").await;
        txn.put_user_state(&service_id, b"key2", b"value2").await;
        txn.commit().await.unwrap();

        let actions = state_machine
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

        state_machine.shutdown().await
    }

    #[test(tokio::test)]
    async fn send_ingress_response_to_multiple_targets() -> TestResult {
        let tc = TaskCenterBuilder::default()
            .default_runtime_handle(tokio::runtime::Handle::current())
            .build()
            .expect("task_center builds");
        let mut state_machine = tc
            .run_in_scope("mock-state-machine", None, MockStateMachine::create())
            .await;
        let (invocation_id, invocation_target) =
            InvocationId::mock_with(InvocationTarget::mock_virtual_object());

        let actions = state_machine
            .apply(Command::Invoke(ServiceInvocation {
                invocation_id,
                invocation_target,
                argument: Default::default(),
                source: Source::Ingress,
                response_sink: Some(ServiceInvocationResponseSink::Ingress(
                    GenerationalNodeId::new(1, 1),
                )),
                span_context: Default::default(),
                headers: vec![],
                execution_time: None,
                idempotency: None,
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
        let mut txn = state_machine.rocksdb_storage.transaction();
        let mut invocation_status = txn.get_invocation_status(&invocation_id).await.unwrap();
        invocation_status.get_response_sinks_mut().unwrap().insert(
            ServiceInvocationResponseSink::Ingress(GenerationalNodeId::new(2, 2)),
        );
        txn.put_invocation_status(&invocation_id, invocation_status)
            .await;
        txn.commit().await.unwrap();

        // Now let's send the output entry
        let response_bytes = Bytes::from_static(b"123");
        let actions = state_machine
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
        let actions = state_machine
            .apply(Command::InvokerEffect(InvokerEffect {
                invocation_id,
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

    mod idempotency {
        use super::*;
        use std::time::Duration;

        use restate_storage_api::idempotency_table::{
            IdempotencyMetadata, IdempotencyTable, ReadOnlyIdempotencyTable,
        };
        use restate_storage_api::invocation_status_table::{CompletedInvocation, StatusTimestamps};
        use restate_storage_api::timer_table::{Timer, TimerKey};
        use restate_types::errors::GONE_INVOCATION_ERROR;
        use restate_types::identifiers::IdempotencyId;
        use restate_types::invocation::{Idempotency, InvocationTarget};
        use restate_wal_protocol::timer::TimerValue;
        use test_log::test;

        #[test(tokio::test)]
        async fn start_idempotent_invocation() {
            let tc = TaskCenterBuilder::default()
                .default_runtime_handle(tokio::runtime::Handle::current())
                .build()
                .expect("task_center builds");
            let mut state_machine = tc
                .run_in_scope("mock-state-machine", None, MockStateMachine::create())
                .await;

            let idempotency = Idempotency {
                key: ByteString::from_static("my-idempotency-key"),
                retention: Duration::from_secs(60) * 60 * 24,
            };
            let invocation_target = InvocationTarget::mock_virtual_object();
            let invocation_id = InvocationId::generate_with_idempotency_key(
                &invocation_target,
                Some(idempotency.key.clone()),
            );
            let idempotency_id =
                IdempotencyId::combine(invocation_id, &invocation_target, idempotency.key.clone());

            // Send fresh invocation with idempotency key
            let actions = state_machine
                .apply(Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress(
                        GenerationalNodeId::new(1, 1),
                    )),
                    idempotency: Some(idempotency),
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
                    contains(pat!(Action::IngressResponse(pat!(IngressResponse {
                        target_node: eq(GenerationalNodeId::new(1, 1)),
                        idempotency_id: some(eq(idempotency_id.clone())),
                        response: eq(ResponseResult::Success(response_bytes.clone()))
                    })))),
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

            state_machine.shutdown().await.unwrap();
        }

        #[test(tokio::test)]
        async fn complete_already_completed_invocation() {
            let tc = TaskCenterBuilder::default()
                .default_runtime_handle(tokio::runtime::Handle::current())
                .build()
                .expect("task_center builds");
            let mut state_machine = tc
                .run_in_scope("mock-state-machine", None, MockStateMachine::create())
                .await;

            let idempotency = Idempotency {
                key: ByteString::from_static("my-idempotency-key"),
                retention: Duration::from_secs(60) * 60 * 24,
            };
            let invocation_target = InvocationTarget::mock_virtual_object();
            let invocation_id = InvocationId::generate_with_idempotency_key(
                &invocation_target,
                Some(idempotency.key.clone()),
            );
            let idempotency_id =
                IdempotencyId::combine(invocation_id, &invocation_target, idempotency.key.clone());

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
                    source: Source::Ingress,
                    idempotency_key: Some(idempotency.key.clone()),
                    timestamps: StatusTimestamps::now(),
                    response_result: ResponseResult::Success(response_bytes.clone()),
                }),
            )
            .await;
            txn.commit().await.unwrap();

            // Send a request, should be completed immediately with result
            let second_invocation_id = InvocationId::generate_with_idempotency_key(
                &invocation_target,
                Some(idempotency.key.clone()),
            );
            let actions = state_machine
                .apply(Command::Invoke(ServiceInvocation {
                    invocation_id: second_invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress(ingress_id)),
                    idempotency: Some(idempotency),
                    ..ServiceInvocation::mock()
                }))
                .await;
            assert_that!(
                actions,
                contains(pat!(Action::IngressResponse(pat!(IngressResponse {
                    target_node: eq(ingress_id),
                    idempotency_id: some(eq(idempotency_id.clone())),
                    response: eq(ResponseResult::Success(response_bytes.clone()))
                }))))
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

            state_machine.shutdown().await.unwrap();
        }

        #[test(tokio::test)]
        async fn known_invocation_id_but_missing_completion() {
            let tc = TaskCenterBuilder::default()
                .default_runtime_handle(tokio::runtime::Handle::current())
                .build()
                .expect("task_center builds");
            let mut state_machine = tc
                .run_in_scope("mock-state-machine", None, MockStateMachine::create())
                .await;

            let idempotency = Idempotency {
                key: ByteString::from_static("my-idempotency-key"),
                retention: Duration::from_secs(60) * 60 * 24,
            };
            let invocation_target = InvocationTarget::mock_virtual_object();
            let invocation_id = InvocationId::generate_with_idempotency_key(
                &invocation_target,
                Some(idempotency.key.clone()),
            );
            let idempotency_id =
                IdempotencyId::combine(invocation_id, &invocation_target, idempotency.key.clone());

            let ingress_id = GenerationalNodeId::new(1, 1);

            // Prepare idempotency metadata
            let mut txn = state_machine.rocksdb_storage.transaction();
            txn.put_idempotency_metadata(&idempotency_id, IdempotencyMetadata { invocation_id })
                .await;
            txn.commit().await.unwrap();

            // Send a request, should be completed immediately with result
            let second_invocation_id = InvocationId::generate_with_idempotency_key(
                &invocation_target,
                Some(idempotency.key.clone()),
            );
            let actions = state_machine
                .apply(Command::Invoke(ServiceInvocation {
                    invocation_id: second_invocation_id,
                    invocation_target,
                    response_sink: Some(ServiceInvocationResponseSink::Ingress(ingress_id)),
                    idempotency: Some(idempotency),
                    ..ServiceInvocation::mock()
                }))
                .await;
            assert_that!(
                actions,
                contains(pat!(Action::IngressResponse(pat!(IngressResponse {
                    target_node: eq(ingress_id),
                    idempotency_id: some(eq(idempotency_id.clone())),
                    response: eq(ResponseResult::from(GONE_INVOCATION_ERROR))
                }))))
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

            state_machine.shutdown().await.unwrap();
        }

        #[test(tokio::test)]
        async fn latch_invocation_while_executing() {
            let tc = TaskCenterBuilder::default()
                .default_runtime_handle(tokio::runtime::Handle::current())
                .build()
                .expect("task_center builds");
            let mut state_machine = tc
                .run_in_scope("mock-state-machine", None, MockStateMachine::create())
                .await;

            let idempotency = Idempotency {
                key: ByteString::from_static("my-idempotency-key"),
                retention: Duration::from_secs(60) * 60 * 24,
            };
            let invocation_target = InvocationTarget::mock_virtual_object();
            let first_invocation_id = InvocationId::generate_with_idempotency_key(
                &invocation_target,
                Some(idempotency.key.clone()),
            );
            let idempotency_id = IdempotencyId::combine(
                first_invocation_id,
                &invocation_target,
                idempotency.key.clone(),
            );

            let ingress_id_1 = GenerationalNodeId::new(1, 1);
            let ingress_id_2 = GenerationalNodeId::new(2, 1);

            // Send fresh invocation with idempotency key
            let actions = state_machine
                .apply(Command::Invoke(ServiceInvocation {
                    invocation_id: first_invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress(ingress_id_1)),
                    idempotency: Some(idempotency.clone()),
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
                Some(idempotency.key.clone()),
            );
            let actions = state_machine
                .apply(Command::Invoke(ServiceInvocation {
                    invocation_id: second_invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress(ingress_id_2)),
                    idempotency: Some(idempotency),
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
                    contains(pat!(Action::IngressResponse(pat!(IngressResponse {
                        target_node: eq(ingress_id_1),
                        idempotency_id: some(eq(idempotency_id.clone())),
                        response: eq(ResponseResult::Success(response_bytes.clone()))
                    })))),
                    contains(pat!(Action::IngressResponse(pat!(IngressResponse {
                        target_node: eq(ingress_id_2),
                        idempotency_id: some(eq(idempotency_id.clone())),
                        response: eq(ResponseResult::Success(response_bytes.clone()))
                    })))),
                )
            );

            state_machine.shutdown().await.unwrap();
        }

        #[test(tokio::test)]
        async fn timer_cleanup() {
            let tc = TaskCenterBuilder::default()
                .default_runtime_handle(tokio::runtime::Handle::current())
                .build()
                .expect("task_center builds");
            let mut state_machine = tc
                .run_in_scope("mock-state-machine", None, MockStateMachine::create())
                .await;

            let idempotency = Idempotency {
                key: ByteString::from_static("my-idempotency-key"),
                retention: Duration::from_secs(60) * 60 * 24,
            };
            let invocation_target = InvocationTarget::mock_virtual_object();
            let invocation_id = InvocationId::generate_with_idempotency_key(
                &invocation_target,
                Some(idempotency.key.clone()),
            );
            let idempotency_id =
                IdempotencyId::combine(invocation_id, &invocation_target, idempotency.key.clone());

            // Prepare idempotency metadata and completed status
            let mut txn = state_machine.storage().transaction();
            txn.put_idempotency_metadata(&idempotency_id, IdempotencyMetadata { invocation_id })
                .await;
            txn.put_invocation_status(
                &invocation_id,
                InvocationStatus::Completed(CompletedInvocation {
                    invocation_target,
                    source: Source::Ingress,
                    idempotency_key: Some(idempotency.key.clone()),
                    timestamps: StatusTimestamps::now(),
                    response_result: ResponseResult::Success(Bytes::from_static(b"123")),
                }),
            )
            .await;
            txn.commit().await.unwrap();

            // Send timer fired command
            let _ = state_machine
                .apply(Command::Timer(TimerValue::new(
                    TimerKey {
                        timestamp: 0,
                        invocation_uuid: invocation_id.invocation_uuid(),
                        journal_index: 0,
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

            state_machine.shutdown().await.unwrap();
        }
    }

    async fn mock_start_invocation_with_service_id(
        state_machine: &mut MockStateMachine,
        service_id: ServiceId,
    ) -> InvocationId {
        mock_start_invocation_with_invocation_target(
            state_machine,
            InvocationTarget::mock_from_service_id(service_id),
        )
        .await
    }

    async fn mock_start_invocation_with_invocation_target(
        state_machine: &mut MockStateMachine,
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
                idempotency: None,
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

    async fn mock_start_invocation(state_machine: &mut MockStateMachine) -> InvocationId {
        mock_start_invocation_with_invocation_target(
            state_machine,
            InvocationTarget::mock_virtual_object(),
        )
        .await
    }
}
