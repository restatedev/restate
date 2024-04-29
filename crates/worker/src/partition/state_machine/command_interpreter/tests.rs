use super::*;

use bytestring::ByteString;
use futures::stream;
use googletest::matcher::Matcher;
use googletest::{all, any, assert_that, pat, unordered_elements_are};
use prost::Message;
use restate_invoker_api::EffectKind;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol::pb::protocol::SleepEntryMessage;
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_storage_api::inbox_table::SequenceNumberInboxEntry;
use restate_storage_api::invocation_status_table::{JournalMetadata, StatusTimestamps};
use restate_storage_api::timer_table::TimerKind;
use restate_storage_api::{Result as StorageResult, StorageError};
use restate_test_util::matchers::*;
use restate_test_util::{assert_eq, let_assert};
use restate_types::errors::codes;
use restate_types::identifiers::{InvocationUuid, WithPartitionKey};
use restate_types::invocation::InvocationTarget;
use restate_types::journal::EntryResult;
use restate_types::journal::{CompleteAwakeableEntry, Entry};
use std::collections::HashMap;
use test_log::test;

use crate::partition::state_machine::command_interpreter::StateReader;
use crate::partition::state_machine::effects::Effect;

#[derive(Default)]
struct StateReaderMock {
    services: HashMap<ServiceId, VirtualObjectStatus>,
    inboxes: HashMap<ServiceId, Vec<SequenceNumberInboxEntry>>,
    invocations: HashMap<InvocationId, InvocationStatus>,
    journals: HashMap<InvocationId, Vec<JournalEntry>>,
}

impl StateReaderMock {
    pub fn mock_invocation_metadata(
        journal_length: u32,
        invocation_target: InvocationTarget,
    ) -> InFlightInvocationMetadata {
        InFlightInvocationMetadata {
            invocation_target,
            journal_metadata: JournalMetadata::new(
                journal_length,
                ServiceInvocationSpanContext::empty(),
            ),
            ..InFlightInvocationMetadata::mock()
        }
    }

    fn lock_service(&mut self, service_id: ServiceId) {
        self.services.insert(
            service_id.clone(),
            VirtualObjectStatus::Locked(InvocationId::from_parts(
                service_id.partition_key(),
                InvocationUuid::new(),
            )),
        );
    }

    fn register_invoked_status_and_locked(
        &mut self,
        invocation_target: InvocationTarget,
        journal: Vec<JournalEntry>,
    ) -> InvocationId {
        let invocation_id = InvocationId::generate(&invocation_target);

        self.services.insert(
            invocation_target.as_keyed_service_id().unwrap(),
            VirtualObjectStatus::Locked(invocation_id),
        );
        self.register_invocation_status(
            invocation_id,
            InvocationStatus::Invoked(Self::mock_invocation_metadata(
                u32::try_from(journal.len()).unwrap(),
                invocation_target,
            )),
            journal,
        );

        invocation_id
    }

    fn register_suspended_status_and_locked(
        &mut self,
        invocation_target: InvocationTarget,
        waiting_for_completed_entries: impl IntoIterator<Item = EntryIndex>,
        journal: Vec<JournalEntry>,
    ) -> InvocationId {
        let invocation_id = InvocationId::generate(&invocation_target);

        self.services.insert(
            invocation_target.as_keyed_service_id().unwrap(),
            VirtualObjectStatus::Locked(invocation_id),
        );
        self.register_invocation_status(
            invocation_id,
            InvocationStatus::Suspended {
                metadata: Self::mock_invocation_metadata(
                    u32::try_from(journal.len()).unwrap(),
                    invocation_target,
                ),
                waiting_for_completed_entries: HashSet::from_iter(waiting_for_completed_entries),
            },
            journal,
        );

        invocation_id
    }

    fn register_invocation_status(
        &mut self,
        invocation_id: InvocationId,
        invocation_status: InvocationStatus,
        journal: Vec<JournalEntry>,
    ) {
        self.invocations.insert(invocation_id, invocation_status);
        self.journals.insert(invocation_id, journal);
    }

    fn enqueue_into_inbox(&mut self, service_id: ServiceId, inbox_entry: SequenceNumberInboxEntry) {
        assert_eq!(
            service_id,
            *inbox_entry.service_id(),
            "Service invocation must have the same service_id as the inbox entry"
        );

        self.inboxes
            .entry(service_id)
            .or_default()
            .push(inbox_entry);
    }
}

impl StateReader for StateReaderMock {
    async fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> StorageResult<VirtualObjectStatus> {
        Ok(self
            .services
            .get(service_id)
            .cloned()
            .unwrap_or(VirtualObjectStatus::Unlocked))
    }

    async fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> StorageResult<InvocationStatus> {
        Ok(self
            .invocations
            .get(invocation_id)
            .cloned()
            .unwrap_or(InvocationStatus::Free))
    }

    async fn is_entry_resumable(
        &mut self,
        _invocation_id: &InvocationId,
        _entry_index: EntryIndex,
    ) -> StorageResult<bool> {
        todo!()
    }

    async fn load_state(
        &mut self,
        _service_id: &ServiceId,
        _key: &Bytes,
    ) -> StorageResult<Option<Bytes>> {
        todo!()
    }

    async fn load_state_keys(&mut self, _: &ServiceId) -> StorageResult<Vec<Bytes>> {
        todo!()
    }

    async fn load_completion_result(
        &mut self,
        _invocation_id: &InvocationId,
        _entry_index: EntryIndex,
    ) -> StorageResult<Option<CompletionResult>> {
        todo!()
    }

    fn get_journal(
        &mut self,
        invocation_id: &InvocationId,
        length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, JournalEntry), StorageError>> + Send {
        ReadOnlyJournalTable::get_journal(self, invocation_id, length)
    }
}

impl ReadOnlyJournalTable for StateReaderMock {
    fn get_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        journal_index: u32,
    ) -> impl Future<Output = StorageResult<Option<JournalEntry>>> + Send {
        futures::future::ready(Ok(self
            .journals
            .get(invocation_id)
            .and_then(|journal| journal.get(journal_index as usize).cloned())))
    }

    fn get_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> impl Stream<Item = StorageResult<(EntryIndex, JournalEntry)>> + Send {
        let journal = self.journals.get(invocation_id);

        let cloned_journal: Vec<JournalEntry> = journal
            .map(|journal| {
                journal
                    .iter()
                    .take(
                        usize::try_from(journal_length)
                            .expect("Converting from u32 to usize should be possible"),
                    )
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        stream::iter(
            cloned_journal
                .into_iter()
                .enumerate()
                .map(|(index, entry)| {
                    Ok((
                        u32::try_from(index).expect("Journal must not be larger than 2^32 - 1"),
                        entry,
                    ))
                }),
        )
    }
}

impl ReadOnlyIdempotencyTable for StateReaderMock {
    async fn get_idempotency_metadata(
        &mut self,
        _idempotency_id: &IdempotencyId,
    ) -> StorageResult<Option<IdempotencyMetadata>> {
        unimplemented!();
    }

    fn all_idempotency_metadata(
        &mut self,
        _range: RangeInclusive<PartitionKey>,
    ) -> impl Stream<Item = StorageResult<(IdempotencyId, IdempotencyMetadata)>> + Send {
        unimplemented!();

        // I need this for type inference to work
        #[allow(unreachable_code)]
        futures::stream::iter(vec![])
    }
}

#[test(tokio::test)]
async fn awakeable_with_success() {
    let mut state_machine: CommandInterpreter<ProtobufRawEntryCodec> =
        CommandInterpreter::new(0, 0, PartitionKey::MIN..=PartitionKey::MAX);
    let mut effects = Effects::default();
    let mut state_reader = StateReaderMock::default();

    let callee_invocation_id = InvocationId::mock_random();
    let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
        CompleteAwakeableEntry {
            id: AwakeableIdentifier::new(callee_invocation_id, 1)
                .to_string()
                .into(),
            result: EntryResult::Success(Bytes::default()),
        },
    ));

    let caller_invocation_id = state_reader.register_invoked_status_and_locked(
        InvocationTarget::mock_virtual_object(),
        vec![JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Awakeable {
                is_completed: false,
            },
            Bytes::default(),
        ))],
    );

    state_machine
        .on_apply(
            Command::InvokerEffect(InvokerEffect {
                invocation_id: caller_invocation_id,
                kind: EffectKind::JournalEntry {
                    entry_index: 1,
                    entry,
                },
            }),
            &mut effects,
            &mut state_reader,
        )
        .await
        .unwrap();
    let_assert!(Effect::EnqueueIntoOutbox { message, .. } = effects.drain().next().unwrap());
    let_assert!(
        OutboxMessage::ServiceResponse(InvocationResponse {
            id,
            entry_index,
            result: ResponseResult::Success(_),
        }) = message
    );
    assert_eq!(id, callee_invocation_id);
    assert_eq!(entry_index, 1);
}

#[test(tokio::test)]
async fn awakeable_with_failure() {
    let mut state_machine: CommandInterpreter<ProtobufRawEntryCodec> =
        CommandInterpreter::new(0, 0, PartitionKey::MIN..=PartitionKey::MAX);
    let mut effects = Effects::default();
    let mut state_reader = StateReaderMock::default();

    let callee_invocation_id = InvocationId::mock_random();
    let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
        CompleteAwakeableEntry {
            id: AwakeableIdentifier::new(callee_invocation_id, 1)
                .to_string()
                .into(),
            result: EntryResult::Failure(codes::BAD_REQUEST, "Some failure".into()),
        },
    ));

    let caller_invocation_id = state_reader.register_invoked_status_and_locked(
        InvocationTarget::mock_virtual_object(),
        vec![JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Awakeable {
                is_completed: false,
            },
            Bytes::default(),
        ))],
    );

    state_machine
        .on_apply(
            Command::InvokerEffect(InvokerEffect {
                invocation_id: caller_invocation_id,
                kind: EffectKind::JournalEntry {
                    entry_index: 1,
                    entry,
                },
            }),
            &mut effects,
            &mut state_reader,
        )
        .await
        .unwrap();
    let_assert!(Effect::EnqueueIntoOutbox { message, .. } = effects.drain().next().unwrap());
    let_assert!(
        OutboxMessage::ServiceResponse(InvocationResponse {
            id,
            entry_index,
            result: ResponseResult::Failure(failure),
        }) = message
    );
    assert_eq!(id, callee_invocation_id);
    assert_eq!(entry_index, 1);
    assert_eq!(failure.message(), "Some failure");
}

#[test(tokio::test)]
async fn send_response_using_invocation_id() {
    let mut state_machine: CommandInterpreter<ProtobufRawEntryCodec> =
        CommandInterpreter::new(0, 0, PartitionKey::MIN..=PartitionKey::MAX);
    let mut effects = Effects::default();
    let mut state_reader = StateReaderMock::default();

    let invocation_id = state_reader
        .register_invoked_status_and_locked(InvocationTarget::mock_virtual_object(), vec![]);

    state_machine
        .on_apply(
            Command::InvocationResponse(InvocationResponse {
                id: invocation_id,
                entry_index: 1,
                result: ResponseResult::Success(Bytes::from_static(b"hello")),
            }),
            &mut effects,
            &mut state_reader,
        )
        .await
        .unwrap();
    assert_that!(
        effects.into_inner(),
        all!(
            contains(pat!(Effect::StoreCompletion {
                invocation_id: eq(invocation_id),
                completion: pat!(Completion { entry_index: eq(1) })
            })),
            contains(pat!(Effect::ForwardCompletion {
                invocation_id: eq(invocation_id),
                completion: pat!(Completion { entry_index: eq(1) })
            }))
        )
    );
}

#[test(tokio::test)]
async fn kill_inboxed_invocation() -> Result<(), Error> {
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(
        0,
        0,
        PartitionKey::MIN..=PartitionKey::MAX,
    );

    let mut effects = Effects::default();
    let mut state_mock = StateReaderMock::default();

    let (inboxed_invocation_id, inboxed_invocation_target) =
        InvocationId::mock_with(InvocationTarget::mock_virtual_object());
    let caller_invocation_id = InvocationId::mock_random();

    state_mock.lock_service(ServiceId::new("svc", "key"));
    state_mock.enqueue_into_inbox(
        inboxed_invocation_target.as_keyed_service_id().unwrap(),
        SequenceNumberInboxEntry {
            inbox_sequence_number: 0,
            inbox_entry: InboxEntry::Invocation(
                inboxed_invocation_target.as_keyed_service_id().unwrap(),
                inboxed_invocation_id,
            ),
        },
    );
    state_mock.invocations.insert(
        inboxed_invocation_id,
        InvocationStatus::Inboxed(InboxedInvocation {
            inbox_sequence_number: 0,
            response_sinks: HashSet::from([ServiceInvocationResponseSink::PartitionProcessor {
                caller: caller_invocation_id,
                entry_index: 0,
            }]),
            timestamps: StatusTimestamps::now(),
            invocation_target: inboxed_invocation_target.clone(),
            argument: Default::default(),
            source: Source::Ingress,
            span_context: Default::default(),
            headers: vec![],
            execution_time: None,
            idempotency: None,
        }),
    );

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::kill(inboxed_invocation_id)),
            &mut effects,
            &mut state_mock,
        )
        .await?;

    assert_that!(
        effects.into_inner(),
        all!(
            contains(pat!(Effect::DeleteInboxEntry {
                service_id: eq(inboxed_invocation_target.as_keyed_service_id().unwrap(),),
                sequence_number: eq(0)
            })),
            contains(pat!(Effect::EnqueueIntoOutbox {
                message: pat!(
                    restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                        InvocationResponse {
                            id: eq(caller_invocation_id),
                            entry_index: eq(0),
                            result: eq(ResponseResult::Failure(KILLED_INVOCATION_ERROR))
                        }
                    ))
                )
            }))
        )
    );

    Ok(())
}

#[test(tokio::test)]
async fn kill_call_tree() -> Result<(), Error> {
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(
        0,
        0,
        PartitionKey::MIN..=PartitionKey::MAX,
    );
    let mut state_reader = StateReaderMock::default();
    let mut effects = Effects::default();

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let invocation_target = InvocationTarget::mock_virtual_object();
    let invocation_id = state_reader.register_invoked_status_and_locked(
        invocation_target.clone(),
        vec![
            uncompleted_invoke_entry(call_invocation_id),
            background_invoke_entry(background_call_invocation_id),
            completed_invoke_entry(finished_call_invocation_id),
        ],
    );

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::kill(invocation_id)),
            &mut effects,
            &mut state_reader,
        )
        .await?;

    let effects = effects.into_inner();

    assert_that!(
        effects,
        all!(
            contains(pat!(Effect::SendAbortInvocationToInvoker(eq(
                invocation_id
            )))),
            contains(pat!(Effect::FreeInvocation(eq(invocation_id)))),
            contains(pat!(Effect::DropJournal {
                invocation_id: eq(invocation_id),
            })),
            contains(pat!(Effect::PopInbox(eq(invocation_target
                .as_keyed_service_id()
                .unwrap())))),
            contains(terminate_invocation_outbox_message_matcher(
                call_invocation_id,
                TerminationFlavor::Kill
            )),
            not(contains(pat!(Effect::EnqueueIntoOutbox {
                message: pat!(
                    restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(pat!(
                        InvocationTermination {
                            invocation_id: any!(
                                eq(background_call_invocation_id),
                                eq(finished_call_invocation_id)
                            )
                        }
                    ))
                )
            })))
        )
    );

    Ok(())
}

fn completed_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::Call {
            is_completed: true,
            enrichment_result: Some(CallEnrichmentResult {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                span_context: ServiceInvocationSpanContext::empty(),
            }),
        },
        Bytes::default(),
    ))
}

fn background_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::OneWayCall {
            enrichment_result: CallEnrichmentResult {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                span_context: ServiceInvocationSpanContext::empty(),
            },
        },
        Bytes::default(),
    ))
}

fn uncompleted_invoke_entry(invocation_id: InvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::Call {
            is_completed: false,
            enrichment_result: Some(CallEnrichmentResult {
                invocation_id,
                invocation_target: InvocationTarget::mock_service(),
                span_context: ServiceInvocationSpanContext::empty(),
            }),
        },
        Bytes::default(),
    ))
}

#[test(tokio::test)]
async fn cancel_invoked_invocation() -> Result<(), Error> {
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(
        0,
        0,
        PartitionKey::MIN..=PartitionKey::MAX,
    );
    let mut state_reader = StateReaderMock::default();
    let mut effects = Effects::default();

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let invocation_id = state_reader.register_invoked_status_and_locked(
        InvocationTarget::mock_virtual_object(),
        create_termination_journal(
            call_invocation_id,
            background_call_invocation_id,
            finished_call_invocation_id,
        ),
    );

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::cancel(invocation_id)),
            &mut effects,
            &mut state_reader,
        )
        .await?;

    let effects = effects.into_inner();

    assert_that!(
        effects,
        unordered_elements_are![
            terminate_invocation_outbox_message_matcher(
                call_invocation_id,
                TerminationFlavor::Cancel
            ),
            store_canceled_completion_matcher(4),
            store_canceled_completion_matcher(5),
            store_canceled_completion_matcher(6),
            forward_canceled_completion_matcher(4),
            forward_canceled_completion_matcher(5),
            forward_canceled_completion_matcher(6),
            delete_timer(5),
        ]
    );

    Ok(())
}

#[test(tokio::test)]
async fn cancel_suspended_invocation() -> Result<(), Error> {
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(
        0,
        0,
        PartitionKey::MIN..=PartitionKey::MAX,
    );
    let mut state_reader = StateReaderMock::default();
    let mut effects = Effects::default();

    let call_invocation_id = InvocationId::mock_random();
    let background_call_invocation_id = InvocationId::mock_random();
    let finished_call_invocation_id = InvocationId::mock_random();

    let journal = create_termination_journal(
        call_invocation_id,
        background_call_invocation_id,
        finished_call_invocation_id,
    );
    let invocation_id = state_reader.register_suspended_status_and_locked(
        InvocationTarget::mock_virtual_object(),
        vec![3, 4, 5, 6],
        journal,
    );

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::cancel(invocation_id)),
            &mut effects,
            &mut state_reader,
        )
        .await?;

    let effects = effects.into_inner();

    assert_that!(
        effects,
        unordered_elements_are![
            terminate_invocation_outbox_message_matcher(
                call_invocation_id,
                TerminationFlavor::Cancel
            ),
            store_canceled_completion_matcher(4),
            store_canceled_completion_matcher(5),
            store_canceled_completion_matcher(6),
            delete_timer(5),
            pat!(Effect::ResumeService {
                invocation_id: eq(invocation_id),
            }),
        ]
    );

    Ok(())
}

fn create_termination_journal(
    call_invocation_id: InvocationId,
    background_invocation_id: InvocationId,
    finished_call_invocation_id: InvocationId,
) -> Vec<JournalEntry> {
    vec![
        uncompleted_invoke_entry(call_invocation_id),
        completed_invoke_entry(finished_call_invocation_id),
        background_invoke_entry(background_invocation_id),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Input {},
            Bytes::default(),
        )),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::GetState {
                is_completed: false,
            },
            Bytes::default(),
        )),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Sleep {
                is_completed: false,
            },
            SleepEntryMessage {
                wake_up_time: 1337,
                result: None,
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        )),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Awakeable {
                is_completed: false,
            },
            Bytes::default(),
        )),
    ]
}

fn canceled_completion_matcher(entry_index: EntryIndex) -> impl Matcher<ActualT = Completion> {
    pat!(Completion {
        entry_index: eq(entry_index),
        result: pat!(CompletionResult::Failure(
            eq(codes::ABORTED),
            eq(ByteString::from_static("canceled"))
        ))
    })
}

fn store_canceled_completion_matcher(entry_index: EntryIndex) -> impl Matcher<ActualT = Effect> {
    pat!(Effect::StoreCompletion {
        completion: canceled_completion_matcher(entry_index),
    })
}

fn forward_canceled_completion_matcher(entry_index: EntryIndex) -> impl Matcher<ActualT = Effect> {
    pat!(Effect::ForwardCompletion {
        completion: canceled_completion_matcher(entry_index),
    })
}

fn delete_timer(entry_index: EntryIndex) -> impl Matcher<ActualT = Effect> {
    pat!(Effect::DeleteTimer(pat!(TimerKey {
        kind: pat!(TimerKind::Journal {
            journal_index: eq(entry_index),
        }),
        timestamp: eq(1337),
    })))
}

fn terminate_invocation_outbox_message_matcher(
    target_invocation_id: InvocationId,
    termination_flavor: TerminationFlavor,
) -> impl Matcher<ActualT = Effect> {
    pat!(Effect::EnqueueIntoOutbox {
        message: pat!(
            restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(pat!(
                InvocationTermination {
                    invocation_id: eq(target_invocation_id),
                    flavor: eq(termination_flavor)
                }
            ))
        )
    })
}
