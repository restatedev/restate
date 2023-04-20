use crate::partition::effects::{Effect, Effects, JournalInformation};
use crate::partition::TimerValue;
use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
use futures::future::BoxFuture;
use restate_common::types::{
    CompletionResult, EnrichedEntryHeader, EnrichedRawEntry, EntryIndex, InvocationId,
    InvocationStatus, InvokedStatus, JournalMetadata, MessageIndex, MillisSinceEpoch,
    OutboxMessage, ServiceId, ServiceInvocation, ServiceInvocationId, ServiceInvocationSpanContext,
    SuspendedStatus, Timer,
};
use restate_common::utils::GenericError;
use restate_invoker::InvokeInputJournal;
use restate_journal::raw::{PlainRawEntry, RawEntryCodec, RawEntryCodecError, RawEntryHeader};
use restate_journal::Completion;
use std::marker::PhantomData;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("failed to read state while interpreting effects: {0}")]
    State(#[from] StateStorageError),
    #[error("failed to decode entry while interpreting effects: {0}")]
    Codec(#[from] RawEntryCodecError),
}

#[derive(Debug)]
pub(crate) enum ActuatorMessage {
    Invoke {
        service_invocation_id: ServiceInvocationId,
        invoke_input_journal: InvokeInputJournal,
    },
    NewOutboxMessage {
        seq_number: MessageIndex,
        message: OutboxMessage,
    },
    RegisterTimer(TimerValue),
    AckStoredEntry {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    },
    ForwardCompletion {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    },
    CommitEndSpan {
        service_name: ByteString,
        service_method: String,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (i32, String)>,
    },
}

pub(crate) trait MessageCollector {
    fn collect(&mut self, message: ActuatorMessage);
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StateStorageError {
    #[error("write failed: {source:?}")]
    #[allow(dead_code)]
    WriteFailed { source: Option<GenericError> },
    #[error("read failed: {source:?}")]
    #[allow(dead_code)]
    ReadFailed { source: Option<GenericError> },
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

pub(crate) trait StateStorage {
    // Invocation status
    fn store_invocation_status<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        status: InvocationStatus,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn drop_journal<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        journal_length: EntryIndex,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn store_journal_entry<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn store_completion_result<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    // TODO: Replace with async trait or proper future
    fn load_completion_result<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, StateStorageError>>;

    // TODO: Replace with async trait or proper future
    fn load_journal_entry<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<EnrichedRawEntry>, StateStorageError>>;

    // In-/outbox
    fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        service_invocation: ServiceInvocation,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn enqueue_into_outbox(
        &mut self,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn store_inbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn store_outbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn truncate_outbox(
        &mut self,
        outbox_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn truncate_inbox<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        inbox_sequence_number: MessageIndex,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    // State
    fn store_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: Bytes,
        value: Bytes,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    // TODO: Replace with async trait or proper future
    fn load_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: &'a Bytes,
    ) -> BoxFuture<Result<Option<Bytes>, StateStorageError>>;

    fn clear_state<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        key: &'a Bytes,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    // Timer
    fn store_timer(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        timer: Timer,
    ) -> BoxFuture<Result<(), StateStorageError>>;

    fn delete_timer(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<(), StateStorageError>>;
}

#[derive(Debug, thiserror::Error)]
#[error("failed committing results: {source:?}")]
pub(crate) struct CommitError {
    source: Option<GenericError>,
}

impl CommitError {
    pub(crate) fn with_source(source: impl Into<GenericError>) -> Self {
        CommitError {
            source: Some(source.into()),
        }
    }
}

pub(crate) trait Committable {
    // TODO: Replace with async trait or proper future
    fn commit(self) -> BoxFuture<'static, Result<(), CommitError>>;
}

#[must_use = "Don't forget to commit the interpretation result"]
pub(crate) struct InterpretationResult<Txn, Collector> {
    txn: Txn,
    collector: Collector,
}

impl<Txn, Collector> InterpretationResult<Txn, Collector>
where
    Txn: Committable,
{
    pub(crate) fn new(txn: Txn, collector: Collector) -> Self {
        Self { txn, collector }
    }

    pub(crate) async fn commit(self) -> Result<Collector, CommitError> {
        let Self { txn, collector } = self;

        txn.commit().await?;
        Ok(collector)
    }
}

pub(crate) struct Interpreter<Codec> {
    _codec: PhantomData<Codec>,
}

impl<Codec: RawEntryCodec> Interpreter<Codec> {
    pub(crate) async fn interpret_effects<S: StateStorage + Committable, C: MessageCollector>(
        effects: &mut Effects,
        mut state_storage: S,
        mut message_collector: C,
    ) -> Result<InterpretationResult<S, C>, Error> {
        for effect in effects.drain() {
            Self::interpret_effect(effect, &mut state_storage, &mut message_collector).await?;
        }

        Ok(InterpretationResult::new(state_storage, message_collector))
    }

    async fn interpret_effect<S: StateStorage, C: MessageCollector>(
        effect: Effect,
        state_storage: &mut S,
        collector: &mut C,
    ) -> Result<(), Error> {
        match effect {
            Effect::InvokeService(service_invocation) => {
                state_storage
                    .store_invocation_status(
                        &service_invocation.id.service_id,
                        InvocationStatus::Invoked(InvokedStatus::new(
                            service_invocation.id.invocation_id,
                            JournalMetadata::new(
                                service_invocation.method_name.clone(),
                                service_invocation.span_context.clone(),
                                1, // initial length is 1, because we store the poll input stream entry
                            ),
                            service_invocation.response_sink,
                        )),
                    )
                    .await?;

                let_assert!(
                    restate_common::types::RawEntry {
                        header: RawEntryHeader::PollInputStream { is_completed },
                        entry
                    } = Codec::serialize_as_unary_input_entry(service_invocation.argument)
                );

                let input_entry = EnrichedRawEntry::new(
                    EnrichedEntryHeader::PollInputStream { is_completed },
                    entry,
                );

                let raw_bytes = input_entry.entry.clone();

                state_storage
                    .store_journal_entry(&service_invocation.id.service_id, 0, input_entry)
                    .await?;

                collector.collect(ActuatorMessage::Invoke {
                    service_invocation_id: service_invocation.id,
                    invoke_input_journal: InvokeInputJournal::CachedJournal(
                        JournalMetadata {
                            method: service_invocation.method_name.to_string(),
                            length: 1,
                            span_context: service_invocation.span_context,
                        },
                        vec![PlainRawEntry::new(
                            RawEntryHeader::PollInputStream { is_completed },
                            raw_bytes,
                        )],
                    ),
                });
            }
            Effect::ResumeService {
                journal_information:
                    JournalInformation {
                        journal_metadata,
                        response_sink,
                        service_invocation_id:
                            ServiceInvocationId {
                                service_id,
                                invocation_id,
                            },
                    },
            } => {
                state_storage
                    .store_invocation_status(
                        &service_id,
                        InvocationStatus::Invoked(InvokedStatus::new(
                            invocation_id,
                            journal_metadata,
                            response_sink,
                        )),
                    )
                    .await?;

                collector.collect(ActuatorMessage::Invoke {
                    service_invocation_id: ServiceInvocationId {
                        service_id,
                        invocation_id,
                    },
                    invoke_input_journal: InvokeInputJournal::NoCachedJournal,
                });
            }
            Effect::SuspendService {
                journal_information:
                    JournalInformation {
                        journal_metadata,
                        response_sink,
                        service_invocation_id:
                            ServiceInvocationId {
                                invocation_id,
                                service_id,
                            },
                    },
                waiting_for_completed_entries,
            } => {
                state_storage
                    .store_invocation_status(
                        &service_id,
                        InvocationStatus::Suspended(SuspendedStatus::new(
                            invocation_id,
                            journal_metadata,
                            response_sink,
                            waiting_for_completed_entries,
                        )),
                    )
                    .await?;
            }
            Effect::DropJournalAndFreeService {
                service_id,
                journal_length,
            } => {
                state_storage
                    .drop_journal(&service_id, journal_length)
                    .await?;
                state_storage
                    .store_invocation_status(&service_id, InvocationStatus::Free)
                    .await?;
            }
            Effect::EnqueueIntoInbox {
                seq_number,
                service_invocation,
            } => {
                state_storage
                    .enqueue_into_inbox(seq_number, service_invocation)
                    .await?;
                state_storage.store_inbox_seq_number(seq_number).await?;
            }
            Effect::EnqueueIntoOutbox {
                seq_number,
                message,
            } => {
                state_storage
                    .enqueue_into_outbox(seq_number, message.clone())
                    .await?;
                state_storage.store_outbox_seq_number(seq_number).await?;

                collector.collect(ActuatorMessage::NewOutboxMessage {
                    seq_number,
                    message,
                });
            }
            Effect::SetState {
                journal_information,
                key,
                value,
                journal_entry,
                entry_index,
            } => {
                state_storage
                    .store_state(
                        &journal_information.service_invocation_id.service_id,
                        key,
                        value,
                    )
                    .await?;

                Self::append_journal_entry(
                    state_storage,
                    collector,
                    journal_information,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::ClearState {
                journal_information,
                key,
                journal_entry,
                entry_index,
            } => {
                state_storage
                    .clear_state(&journal_information.service_invocation_id.service_id, &key)
                    .await?;

                Self::append_journal_entry(
                    state_storage,
                    collector,
                    journal_information,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::GetStateAndAppendCompletedEntry {
                key,
                journal_information,
                mut journal_entry,
                entry_index,
            } => {
                let value = state_storage
                    .load_state(&journal_information.service_invocation_id.service_id, &key)
                    .await?;

                let completion_result = value
                    .map(CompletionResult::Success)
                    .unwrap_or(CompletionResult::Empty);

                Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                let service_invocation_id = journal_information.service_invocation_id.clone();

                Self::unchecked_append_journal_entry(
                    state_storage,
                    collector,
                    journal_information,
                    entry_index,
                    journal_entry,
                )
                .await?;

                collector.collect(ActuatorMessage::ForwardCompletion {
                    service_invocation_id,
                    completion: Completion {
                        entry_index,
                        result: completion_result,
                    },
                })
            }
            Effect::RegisterTimer { timer_value } => {
                state_storage
                    .store_timer(
                        timer_value.service_invocation_id.clone(),
                        timer_value.wake_up_time,
                        timer_value.entry_index,
                        timer_value.value.clone(),
                    )
                    .await?;

                collector.collect(ActuatorMessage::RegisterTimer(timer_value));
            }
            Effect::DeleteTimer {
                service_invocation_id,
                wake_up_time,
                entry_index,
            } => {
                state_storage
                    .delete_timer(service_invocation_id, wake_up_time, entry_index)
                    .await?;
            }
            Effect::AppendJournalEntry {
                journal_information,
                entry_index,
                journal_entry,
            } => {
                Self::append_journal_entry(
                    state_storage,
                    collector,
                    journal_information,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::AppendAwakeableEntry {
                journal_information,
                entry_index,
                mut journal_entry,
            } => {
                // check whether the completion has arrived first
                if let Some(completion_result) = state_storage
                    .load_completion_result(
                        &journal_information.service_invocation_id.service_id,
                        entry_index,
                    )
                    .await?
                {
                    Codec::write_completion(&mut journal_entry, completion_result)?;
                }

                Self::unchecked_append_journal_entry(
                    state_storage,
                    collector,
                    journal_information,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::AppendJournalEntryAndAck {
                journal_information,
                journal_entry,
                entry_index,
            } => {
                let service_invocation_id = journal_information.service_invocation_id.clone();

                Self::append_journal_entry(
                    state_storage,
                    collector,
                    journal_information,
                    entry_index,
                    journal_entry,
                )
                .await?;

                // storage is acked by sending an empty completion
                collector.collect(ActuatorMessage::ForwardCompletion {
                    service_invocation_id,
                    completion: Completion {
                        entry_index,
                        result: CompletionResult::Ack,
                    },
                })
            }
            Effect::TruncateOutbox(outbox_sequence_number) => {
                state_storage
                    .truncate_outbox(outbox_sequence_number)
                    .await?;
            }
            Effect::StoreCompletion {
                service_invocation_id,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
            } => {
                Self::store_completion(state_storage, &service_invocation_id, entry_index, result)
                    .await?;
            }
            Effect::ForwardCompletion {
                service_invocation_id,
                completion,
            } => {
                collector.collect(ActuatorMessage::ForwardCompletion {
                    service_invocation_id,
                    completion,
                });
            }
            Effect::StoreCompletionAndForward {
                service_invocation_id,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
            } => {
                if Self::store_completion(
                    state_storage,
                    &service_invocation_id,
                    entry_index,
                    // We need to give ownership because storing the completion requires creating
                    // a protobuf message. However, cloning should be "cheap" because
                    // CompletionResult uses Bytes.
                    result.clone(),
                )
                .await?
                {
                    collector.collect(ActuatorMessage::ForwardCompletion {
                        service_invocation_id,
                        completion: Completion {
                            entry_index,
                            result,
                        },
                    });
                }
            }
            Effect::StoreCompletionAndResume {
                service_invocation_id,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
            } => {
                if Self::store_completion(
                    state_storage,
                    &service_invocation_id,
                    entry_index,
                    // We need to give ownership because storing the completion requires creating
                    // a protobuf message. However, cloning should be "cheap" because
                    // CompletionResult uses Bytes.
                    result.clone(),
                )
                .await?
                {
                    collector.collect(ActuatorMessage::Invoke {
                        service_invocation_id,
                        invoke_input_journal: InvokeInputJournal::NoCachedJournal,
                    });
                }
            }
            Effect::DropJournalAndPopInbox {
                service_id,
                inbox_sequence_number,
                journal_length,
            } => {
                // TODO: Only drop journals if the inbox is empty; this requires that keep track of the max journal length: https://github.com/restatedev/restate/issues/272
                state_storage
                    .drop_journal(&service_id, journal_length)
                    .await?;
                state_storage
                    .truncate_inbox(&service_id, inbox_sequence_number)
                    .await?;
            }
            Effect::NotifyInvocationResult {
                service_name,
                service_method,
                invocation_id,
                span_context,
                result,
            } => collector.collect(ActuatorMessage::CommitEndSpan {
                service_name,
                service_method,
                invocation_id,
                span_context,
                result,
            }),
        }

        Ok(())
    }

    /// Stores the given completion. Returns `true` if an [`RawEntry`] was completed.
    async fn store_completion<S: StateStorage>(
        state_storage: &mut S,
        service_invocation_id: &ServiceInvocationId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> Result<bool, Error> {
        if let Some(mut journal_entry) = state_storage
            .load_journal_entry(&service_invocation_id.service_id, entry_index)
            .await?
        {
            Codec::write_completion(&mut journal_entry, completion_result)?;
            state_storage
                .store_journal_entry(
                    &service_invocation_id.service_id,
                    entry_index,
                    journal_entry,
                )
                .await?;
            Ok(true)
        } else {
            state_storage
                .store_completion_result(
                    &service_invocation_id.service_id,
                    entry_index,
                    completion_result,
                )
                .await?;
            Ok(false)
        }
    }

    async fn append_journal_entry<S: StateStorage, C: MessageCollector>(
        state_storage: &mut S,
        collector: &mut C,
        journal_information: JournalInformation,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> Result<(), Error> {
        debug_assert!(
            state_storage
                .load_completion_result(
                    &journal_information.service_invocation_id.service_id,
                    entry_index
                )
                .await?
                .is_none(),
            "Only awakeable journal entries can have a completion result already stored"
        );

        Self::unchecked_append_journal_entry(
            state_storage,
            collector,
            journal_information,
            entry_index,
            journal_entry,
        )
        .await
    }

    async fn unchecked_append_journal_entry<S: StateStorage, C: MessageCollector>(
        state_storage: &mut S,
        collector: &mut C,
        journal_information: JournalInformation,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> Result<(), Error> {
        let JournalInformation {
            service_invocation_id,
            mut journal_metadata,
            response_sink,
        } = journal_information;

        state_storage
            .store_journal_entry(
                &service_invocation_id.service_id,
                entry_index,
                journal_entry,
            )
            .await?;

        // update the journal metadata
        debug_assert_eq!(
            journal_metadata.length, entry_index,
            "journal should not have gaps"
        );
        journal_metadata.length = entry_index + 1;

        state_storage
            .store_invocation_status(
                &service_invocation_id.service_id,
                InvocationStatus::Invoked(InvokedStatus::new(
                    service_invocation_id.invocation_id,
                    journal_metadata,
                    response_sink,
                )),
            )
            .await?;

        collector.collect(ActuatorMessage::AckStoredEntry {
            service_invocation_id,
            entry_index,
        });

        Ok(())
    }
}
