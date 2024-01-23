use super::*;

use std::collections::HashMap;

use bytestring::ByteString;
use futures::stream;
use googletest::matcher::Matcher;
use googletest::{all, any, assert_that, pat, unordered_elements_are};
use prost::Message;
use test_log::test;

use restate_invoker_api::EffectKind;
use restate_service_protocol::awakeable_id::AwakeableIdentifier;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol::pb::protocol::SleepEntryMessage;
use restate_storage_api::status_table::{JournalMetadata, StatusTimestamps};
use restate_storage_api::{Result as StorageResult, StorageError};
use restate_test_util::matchers::*;
use restate_test_util::{assert_eq, let_assert};
use restate_types::errors::UserErrorCode;
use restate_types::identifiers::WithPartitionKey;
use restate_types::journal::EntryResult;
use restate_types::journal::{CompleteAwakeableEntry, Entry};

use crate::partition::state_machine::command_interpreter::StateReader;
use crate::partition::state_machine::effects::Effect;

#[derive(Default)]
struct StateReaderMock {
    invocations: HashMap<ServiceId, InvocationStatus>,
    inboxes: HashMap<ServiceId, Vec<InboxEntry>>,
    journals: HashMap<ServiceId, Vec<JournalEntry>>,
}

impl StateReaderMock {
    pub fn mock_invocation_metadata(
        journal_length: u32,
        invocation_uuid: InvocationUuid,
    ) -> InvocationMetadata {
        InvocationMetadata {
            invocation_uuid,
            journal_metadata: JournalMetadata {
                length: journal_length,
                span_context: ServiceInvocationSpanContext::empty(),
            },
            ..InvocationMetadata::mock()
        }
    }

    fn register_invoked_status(&mut self, fid: FullInvocationId, journal: Vec<JournalEntry>) {
        let invocation_uuid = fid.invocation_uuid;

        self.register_invocation_status(
            fid,
            InvocationStatus::Invoked(Self::mock_invocation_metadata(
                u32::try_from(journal.len()).unwrap(),
                invocation_uuid,
            )),
            journal,
        );
    }

    fn register_suspended_status(
        &mut self,
        fid: FullInvocationId,
        waiting_for_completed_entries: impl IntoIterator<Item = EntryIndex>,
        journal: Vec<JournalEntry>,
    ) {
        let invocation_uuid = fid.invocation_uuid;

        self.register_invocation_status(
            fid,
            InvocationStatus::Suspended {
                metadata: Self::mock_invocation_metadata(
                    u32::try_from(journal.len()).unwrap(),
                    invocation_uuid,
                ),
                waiting_for_completed_entries: HashSet::from_iter(waiting_for_completed_entries),
            },
            journal,
        );
    }

    fn register_virtual_status(
        &mut self,
        fid: FullInvocationId,
        completion_notification_target: NotificationTarget,
        kill_notification_target: NotificationTarget,
        journal: Vec<JournalEntry>,
    ) {
        let invocation_uuid = fid.invocation_uuid;
        self.register_invocation_status(
            fid,
            InvocationStatus::Virtual {
                invocation_uuid,
                journal_metadata: JournalMetadata {
                    length: u32::try_from(journal.len()).unwrap(),
                    span_context: ServiceInvocationSpanContext::empty(),
                },
                completion_notification_target,
                kill_notification_target,
                timestamps: StatusTimestamps::now(),
            },
            journal,
        );
    }

    fn register_invocation_status(
        &mut self,
        fid: FullInvocationId,
        invocation_status: InvocationStatus,
        journal: Vec<JournalEntry>,
    ) {
        let service_id = fid.service_id.clone();
        self.invocations.insert(fid.service_id, invocation_status);

        self.journals.insert(service_id, journal);
    }

    fn enqueue_into_inbox(&mut self, service_id: ServiceId, inbox_entry: InboxEntry) {
        assert_eq!(
            service_id, inbox_entry.service_invocation.fid.service_id,
            "Service invocation must have the same service_id as the inbox entry"
        );

        self.inboxes
            .entry(service_id)
            .or_default()
            .push(inbox_entry);
    }
}

impl StateReader for StateReaderMock {
    async fn get_invocation_status(
        &mut self,
        service_id: &ServiceId,
    ) -> StorageResult<InvocationStatus> {
        Ok(self.invocations.get(service_id).cloned().unwrap())
    }

    async fn resolve_invocation_status_from_invocation_id(
        &mut self,
        invocation_id: &InvocationId,
    ) -> StorageResult<(FullInvocationId, InvocationStatus)> {
        let (service_id, status) = self
            .invocations
            .iter()
            .find(|(service_id, status)| {
                service_id.partition_key() == invocation_id.partition_key()
                    && status.invocation_uuid().unwrap() == invocation_id.invocation_uuid()
            })
            .map(|(service_id, status)| (service_id.clone(), status.clone()))
            .unwrap_or((ServiceId::new("", ""), InvocationStatus::default()));

        Ok((
            FullInvocationId::with_service_id(service_id, invocation_id.invocation_uuid()),
            status,
        ))
    }

    fn get_inbox_entry(
        &mut self,
        maybe_fid: impl Into<MaybeFullInvocationId>,
    ) -> impl Future<Output = StorageResult<Option<InboxEntry>>> + Send {
        let invocation_id = InvocationId::from(maybe_fid.into());

        let result = self
            .inboxes
            .values()
            .flat_map(|v| v.iter())
            .find(|inbox_entry| {
                inbox_entry.service_invocation.fid.invocation_uuid
                    == invocation_id.invocation_uuid()
            });

        futures::future::ready(Ok(result.cloned()))
    }

    async fn is_entry_resumable(
        &mut self,
        _service_id: &ServiceId,
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

    async fn load_completion_result(
        &mut self,
        _service_id: &ServiceId,
        _entry_index: EntryIndex,
    ) -> StorageResult<Option<CompletionResult>> {
        todo!()
    }

    fn get_journal(
        &mut self,
        service_id: &ServiceId,
        length: EntryIndex,
    ) -> impl Stream<Item = Result<(EntryIndex, JournalEntry), StorageError>> + Send {
        let journal = self.journals.get(service_id);

        let cloned_journal: Vec<JournalEntry> = journal
            .map(|journal| {
                journal
                    .iter()
                    .take(
                        usize::try_from(length)
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

#[test(tokio::test)]
async fn awakeable_with_success() {
    let mut state_machine: CommandInterpreter<ProtobufRawEntryCodec> =
        CommandInterpreter::new(0, 0);
    let mut effects = Effects::default();
    let mut state_reader = StateReaderMock::default();

    let sid_caller = FullInvocationId::mock_random();
    let sid_callee = FullInvocationId::mock_random();

    let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
        CompleteAwakeableEntry {
            id: AwakeableIdentifier::new(sid_callee.clone().into(), 1)
                .encode()
                .into(),
            result: EntryResult::Success(Bytes::default()),
        },
    ));
    let cmd = Command::Invoker(InvokerEffect {
        full_invocation_id: sid_caller.clone(),
        kind: EffectKind::JournalEntry {
            entry_index: 1,
            entry,
        },
    });

    state_reader.register_invoked_status(
        sid_caller.clone(),
        vec![JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Awakeable {
                is_completed: false,
            },
            Bytes::default(),
        ))],
    );

    state_machine
        .on_apply(cmd, &mut effects, &mut state_reader)
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
    assert_eq!(
        id,
        MaybeFullInvocationId::Partial(InvocationId::from(sid_callee))
    );
    assert_eq!(entry_index, 1);
}

#[test(tokio::test)]
async fn awakeable_with_failure() {
    let mut state_machine: CommandInterpreter<ProtobufRawEntryCodec> =
        CommandInterpreter::new(0, 0);
    let mut effects = Effects::default();
    let mut state_reader = StateReaderMock::default();

    let sid_caller = FullInvocationId::mock_random();
    let sid_callee = FullInvocationId::mock_random();

    let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
        CompleteAwakeableEntry {
            id: AwakeableIdentifier::new(sid_callee.clone().into(), 1)
                .encode()
                .into(),
            result: EntryResult::Failure(UserErrorCode::FailedPrecondition, "Some failure".into()),
        },
    ));
    let cmd = Command::Invoker(InvokerEffect {
        full_invocation_id: sid_caller.clone(),
        kind: EffectKind::JournalEntry {
            entry_index: 1,
            entry,
        },
    });

    state_reader.register_invoked_status(
        sid_caller.clone(),
        vec![JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::Awakeable {
                is_completed: false,
            },
            Bytes::default(),
        ))],
    );

    state_machine
        .on_apply(cmd, &mut effects, &mut state_reader)
        .await
        .unwrap();
    let_assert!(Effect::EnqueueIntoOutbox { message, .. } = effects.drain().next().unwrap());
    let_assert!(
        OutboxMessage::ServiceResponse(InvocationResponse {
            id,
            entry_index,
            result: ResponseResult::Failure(UserErrorCode::FailedPrecondition, failure_reason),
        }) = message
    );
    assert_eq!(
        id,
        MaybeFullInvocationId::Partial(InvocationId::from(sid_callee))
    );
    assert_eq!(entry_index, 1);
    assert_eq!(failure_reason, "Some failure");
}

#[test(tokio::test)]
async fn send_response_using_invocation_id() {
    let mut state_machine: CommandInterpreter<ProtobufRawEntryCodec> =
        CommandInterpreter::new(0, 0);
    let mut effects = Effects::default();
    let mut state_reader = StateReaderMock::default();

    let fid = FullInvocationId::mock_random();

    let cmd = Command::Response(InvocationResponse {
        id: MaybeFullInvocationId::Partial(InvocationId::from(fid.clone())),
        entry_index: 1,
        result: ResponseResult::Success(Bytes::from_static(b"hello")),
    });

    state_reader.register_invoked_status(fid.clone(), vec![]);

    state_machine
        .on_apply(cmd, &mut effects, &mut state_reader)
        .await
        .unwrap();
    assert_that!(
        effects.into_inner(),
        all!(
            contains(pat!(Effect::StoreCompletion {
                full_invocation_id: eq(fid.clone()),
                completion: pat!(Completion { entry_index: eq(1) })
            })),
            contains(pat!(Effect::ForwardCompletion {
                full_invocation_id: eq(fid),
                completion: pat!(Completion { entry_index: eq(1) })
            }))
        )
    );
}

#[test(tokio::test)]
async fn kill_inboxed_invocation() -> Result<(), Error> {
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(0, 0);

    let mut effects = Effects::default();
    let mut state_reader = StateReaderMock::default();
    let fid = FullInvocationId::generate("svc", "key");
    let inboxed_fid = FullInvocationId::generate("svc", "key");
    let caller_fid = FullInvocationId::mock_random();

    state_reader.register_invoked_status(fid, vec![]);
    state_reader.enqueue_into_inbox(
        inboxed_fid.service_id.clone(),
        InboxEntry {
            inbox_sequence_number: 0,
            service_invocation: ServiceInvocation {
                fid: inboxed_fid.clone(),
                response_sink: Some(ServiceInvocationResponseSink::PartitionProcessor {
                    caller: caller_fid.clone(),
                    entry_index: 0,
                }),
                ..ServiceInvocation::mock()
            },
        },
    );

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::kill(MaybeFullInvocationId::from(
                inboxed_fid.clone(),
            ))),
            &mut effects,
            &mut state_reader,
        )
        .await?;

    assert_that!(
        effects.into_inner(),
        all!(
            contains(pat!(Effect::DeleteInboxEntry {
                service_id: eq(inboxed_fid.service_id),
                sequence_number: eq(0)
            })),
            contains(pat!(Effect::EnqueueIntoOutbox {
                message: pat!(
                    restate_storage_api::outbox_table::OutboxMessage::ServiceResponse(pat!(
                        InvocationResponse {
                            id: eq(MaybeFullInvocationId::from(caller_fid)),
                            entry_index: eq(0),
                            result: pat!(ResponseResult::Failure(
                                eq(UserErrorCode::Aborted),
                                eq(ByteString::from_static("killed"))
                            ))
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
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(0, 0);
    let mut state_reader = StateReaderMock::default();
    let mut effects = Effects::default();

    let fid = FullInvocationId::mock_random();
    let call_fid = FullInvocationId::mock_random();
    let background_fid = FullInvocationId::mock_random();
    let finished_call_fid = FullInvocationId::mock_random();

    state_reader.register_invoked_status(
        fid.clone(),
        vec![
            uncompleted_invoke_entry(call_fid.clone()),
            background_invoke_entry(background_fid.clone()),
            completed_invoke_entry(finished_call_fid.clone()),
        ],
    );

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::kill(MaybeFullInvocationId::from(
                fid.clone(),
            ))),
            &mut effects,
            &mut state_reader,
        )
        .await?;

    let effects = effects.into_inner();

    assert_that!(
        effects,
        all!(
            contains(pat!(Effect::AbortInvocation(eq(fid.clone())))),
            contains(pat!(Effect::DropJournalAndPopInbox {
                service_id: eq(fid.service_id.clone()),
            })),
            contains(terminate_invocation_outbox_message_matcher(
                call_fid,
                TerminationFlavor::Kill
            )),
            not(contains(pat!(Effect::EnqueueIntoOutbox {
                message: pat!(
                    restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(pat!(
                        InvocationTermination {
                            maybe_fid: any!(
                                eq(MaybeFullInvocationId::from(background_fid)),
                                eq(MaybeFullInvocationId::from(finished_call_fid))
                            )
                        }
                    ))
                )
            })))
        )
    );

    Ok(())
}

fn completed_invoke_entry(target_fid: FullInvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::Invoke {
            is_completed: true,
            enrichment_result: Some(InvokeEnrichmentResult {
                invocation_uuid: target_fid.invocation_uuid,
                service_key: target_fid.service_id.key,
                service_name: target_fid.service_id.service_name,
                span_context: ServiceInvocationSpanContext::empty(),
            }),
        },
        Bytes::default(),
    ))
}

fn background_invoke_entry(target_fid: FullInvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::BackgroundInvoke {
            enrichment_result: InvokeEnrichmentResult {
                invocation_uuid: target_fid.invocation_uuid,
                service_key: target_fid.service_id.key,
                service_name: target_fid.service_id.service_name,
                span_context: ServiceInvocationSpanContext::empty(),
            },
        },
        Bytes::default(),
    ))
}

fn uncompleted_invoke_entry(target_fid: FullInvocationId) -> JournalEntry {
    JournalEntry::Entry(EnrichedRawEntry::new(
        EnrichedEntryHeader::Invoke {
            is_completed: false,
            enrichment_result: Some(InvokeEnrichmentResult {
                invocation_uuid: target_fid.invocation_uuid,
                service_key: target_fid.service_id.key,
                service_name: target_fid.service_id.service_name,
                span_context: ServiceInvocationSpanContext::empty(),
            }),
        },
        Bytes::default(),
    ))
}

#[test(tokio::test)]
async fn cancel_invoked_invocation() -> Result<(), Error> {
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(0, 0);
    let mut state_reader = StateReaderMock::default();
    let mut effects = Effects::default();

    let fid = FullInvocationId::mock_random();
    let call_fid = FullInvocationId::mock_random();
    let background_fid = FullInvocationId::mock_random();
    let finished_call_fid = FullInvocationId::mock_random();

    state_reader.register_invoked_status(
        fid.clone(),
        create_termination_journal(
            call_fid.clone(),
            background_fid.clone(),
            finished_call_fid.clone(),
        ),
    );

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::cancel(
                MaybeFullInvocationId::from(fid.clone()),
            )),
            &mut effects,
            &mut state_reader,
        )
        .await?;

    let effects = effects.into_inner();

    assert_that!(
        effects,
        unordered_elements_are![
            terminate_invocation_outbox_message_matcher(call_fid, TerminationFlavor::Cancel),
            store_canceled_completion_matcher(3),
            store_canceled_completion_matcher(4),
            store_canceled_completion_matcher(5),
            store_canceled_completion_matcher(6),
            forward_canceled_completion_matcher(3),
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
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(0, 0);
    let mut state_reader = StateReaderMock::default();
    let mut effects = Effects::default();

    let fid = FullInvocationId::mock_random();
    let call_fid = FullInvocationId::mock_random();
    let background_fid = FullInvocationId::mock_random();
    let finished_call_fid = FullInvocationId::mock_random();

    let journal = create_termination_journal(
        call_fid.clone(),
        background_fid.clone(),
        finished_call_fid.clone(),
    );
    state_reader.register_suspended_status(fid.clone(), vec![3, 4, 5, 6], journal);

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::cancel(
                MaybeFullInvocationId::from(fid.clone()),
            )),
            &mut effects,
            &mut state_reader,
        )
        .await?;

    let effects = effects.into_inner();

    assert_that!(
        effects,
        unordered_elements_are![
            terminate_invocation_outbox_message_matcher(call_fid, TerminationFlavor::Cancel),
            store_canceled_completion_matcher(3),
            store_canceled_completion_matcher(4),
            store_canceled_completion_matcher(5),
            store_canceled_completion_matcher(6),
            delete_timer(5),
            pat!(Effect::ResumeService {
                service_id: eq(fid.service_id),
            }),
        ]
    );

    Ok(())
}

#[test(tokio::test)]
async fn cancel_virtual_invocation() -> Result<(), Error> {
    let mut command_interpreter = CommandInterpreter::<ProtobufRawEntryCodec>::new(0, 0);
    let mut state_reader = StateReaderMock::default();
    let mut effects = Effects::default();

    let fid = FullInvocationId::mock_random();
    let call_fid = FullInvocationId::mock_random();
    let background_fid = FullInvocationId::mock_random();
    let finished_call_fid = FullInvocationId::mock_random();

    let notification_service_id = ServiceId::new("notification", "key");

    let completion_notification_target = NotificationTarget {
        service: notification_service_id.clone(),
        method: "completion".to_owned(),
    };
    let kill_notification_target = NotificationTarget {
        service: notification_service_id,
        method: "kill".to_owned(),
    };

    state_reader.register_virtual_status(
        fid.clone(),
        completion_notification_target,
        kill_notification_target,
        create_termination_journal(
            call_fid.clone(),
            background_fid.clone(),
            finished_call_fid.clone(),
        ),
    );

    command_interpreter
        .on_apply(
            Command::TerminateInvocation(InvocationTermination::cancel(
                MaybeFullInvocationId::from(fid.clone()),
            )),
            &mut effects,
            &mut state_reader,
        )
        .await?;

    let effects = effects.into_inner();

    assert_that!(
        effects,
        unordered_elements_are![
            terminate_invocation_outbox_message_matcher(call_fid, TerminationFlavor::Cancel),
            store_canceled_completion_matcher(3),
            store_canceled_completion_matcher(4),
            store_canceled_completion_matcher(5),
            store_canceled_completion_matcher(6),
            notify_virtual_journal_canceled_completion_matcher(3),
            notify_virtual_journal_canceled_completion_matcher(4),
            notify_virtual_journal_canceled_completion_matcher(5),
            notify_virtual_journal_canceled_completion_matcher(6),
            delete_timer(5),
        ]
    );

    Ok(())
}

fn create_termination_journal(
    call_fid: FullInvocationId,
    background_fid: FullInvocationId,
    finished_call_fid: FullInvocationId,
) -> Vec<JournalEntry> {
    vec![
        uncompleted_invoke_entry(call_fid),
        completed_invoke_entry(finished_call_fid),
        background_invoke_entry(background_fid),
        JournalEntry::Entry(EnrichedRawEntry::new(
            EnrichedEntryHeader::PollInputStream {
                is_completed: false,
            },
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
            eq(UserErrorCode::Cancelled),
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
        journal_index: eq(entry_index),
        timestamp: eq(1337),
    })))
}

fn notify_virtual_journal_canceled_completion_matcher(
    entry_index: EntryIndex,
) -> impl Matcher<ActualT = Effect> {
    pat!(Effect::NotifyVirtualJournalCompletion {
        completion: canceled_completion_matcher(entry_index),
    })
}

fn terminate_invocation_outbox_message_matcher(
    target_fid: impl Into<MaybeFullInvocationId>,
    termination_flavor: TerminationFlavor,
) -> impl Matcher<ActualT = Effect> {
    pat!(Effect::EnqueueIntoOutbox {
        message: pat!(
            restate_storage_api::outbox_table::OutboxMessage::InvocationTermination(pat!(
                InvocationTermination {
                    maybe_fid: eq(target_fid.into()),
                    flavor: eq(termination_flavor)
                }
            ))
        )
    })
}
