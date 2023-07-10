use crate::partition::effects::{Effect, Effects};
use crate::partition::{AckResponse, TimerValue};
use assert2::let_assert;
use bytes::Bytes;
use futures::future::BoxFuture;
use restate_invoker::InvokeInputJournal;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::status_table::{InvocationMetadata, InvocationStatus};
use restate_storage_api::timer_table::Timer;
use restate_types::errors::InvocationErrorCode;
use restate_types::identifiers::{EntryIndex, ServiceId, ServiceInvocationId};
use restate_types::invocation::{ServiceInvocation, ServiceInvocationSpanContext};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use restate_types::journal::raw::{
    PlainRawEntry, RawEntryCodec, RawEntryCodecError, RawEntryHeader,
};
use restate_types::journal::{Completion, CompletionResult, JournalMetadata};
use restate_types::message::MessageIndex;
use restate_types::time::MillisSinceEpoch;
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
    RegisterTimer {
        timer_value: TimerValue,
    },
    AckStoredEntry {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    },
    ForwardCompletion {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    },
    CommitEndSpan {
        service_invocation_id: ServiceInvocationId,
        service_method: String,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (InvocationErrorCode, String)>,
    },
    SendAckResponse(AckResponse),
    AbortInvocation(ServiceInvocationId),
}

pub(crate) trait MessageCollector {
    fn collect(&mut self, message: ActuatorMessage);
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StateStorageError {
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
    source: Option<anyhow::Error>,
}

impl CommitError {
    pub(crate) fn with_source(source: impl Into<anyhow::Error>) -> Self {
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
                Self::invoke_service(state_storage, collector, service_invocation).await?;
            }
            Effect::ResumeService {
                service_id,
                mut metadata,
            } => {
                metadata.modification_time = MillisSinceEpoch::now();
                let invocation_id = metadata.invocation_id;
                state_storage
                    .store_invocation_status(&service_id, InvocationStatus::Invoked(metadata))
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
                service_id,
                mut metadata,
                waiting_for_completed_entries,
            } => {
                metadata.modification_time = MillisSinceEpoch::now();
                state_storage
                    .store_invocation_status(
                        &service_id,
                        InvocationStatus::Suspended {
                            metadata,
                            waiting_for_completed_entries,
                        },
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
                // need to store the next inbox sequence number
                state_storage.store_inbox_seq_number(seq_number + 1).await?;
            }
            Effect::EnqueueIntoOutbox {
                seq_number,
                message,
            } => {
                state_storage
                    .enqueue_into_outbox(seq_number, message.clone())
                    .await?;
                // need to store the next outbox sequence number
                state_storage
                    .store_outbox_seq_number(seq_number + 1)
                    .await?;

                collector.collect(ActuatorMessage::NewOutboxMessage {
                    seq_number,
                    message,
                });
            }
            Effect::SetState {
                service_id,
                metadata,
                key,
                value,
                journal_entry,
                entry_index,
            } => {
                state_storage.store_state(&service_id, key, value).await?;

                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_id,
                    metadata,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::ClearState {
                service_id,
                metadata,
                key,
                journal_entry,
                entry_index,
            } => {
                state_storage.clear_state(&service_id, &key).await?;

                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_id,
                    metadata,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::GetStateAndAppendCompletedEntry {
                key,
                service_id,
                metadata,
                mut journal_entry,
                entry_index,
            } => {
                let value = state_storage.load_state(&service_id, &key).await?;

                let completion_result = value
                    .map(CompletionResult::Success)
                    .unwrap_or(CompletionResult::Empty);

                Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                let service_invocation_id = ServiceInvocationId::with_service_id(
                    service_id.clone(),
                    metadata.invocation_id,
                );

                Self::unchecked_append_journal_entry(
                    state_storage,
                    collector,
                    service_id,
                    metadata,
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

                collector.collect(ActuatorMessage::RegisterTimer { timer_value });
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
            Effect::StoreEndpointId {
                service_id,
                endpoint_id,
                mut metadata,
            } => {
                metadata.journal_metadata.endpoint_id = Some(endpoint_id);
                // We recreate the InvocationStatus in Invoked state as the invoker can notify the
                // chosen endpoint_id only when the invocation is in-flight.
                state_storage
                    .store_invocation_status(&service_id, InvocationStatus::Invoked(metadata))
                    .await?;
            }
            Effect::AppendJournalEntry {
                service_id,
                metadata,
                entry_index,
                journal_entry,
            } => {
                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_id,
                    metadata,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::AppendAwakeableEntry {
                service_id,
                metadata,
                entry_index,
                mut journal_entry,
            } => {
                // check whether the completion has arrived first
                if let Some(completion_result) = state_storage
                    .load_completion_result(&service_id, entry_index)
                    .await?
                {
                    Codec::write_completion(&mut journal_entry, completion_result)?;
                }

                Self::unchecked_append_journal_entry(
                    state_storage,
                    collector,
                    service_id,
                    metadata,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::AppendJournalEntryAndAck {
                service_id,
                metadata,
                journal_entry,
                entry_index,
            } => {
                let service_invocation_id = ServiceInvocationId::with_service_id(
                    service_id.clone(),
                    metadata.invocation_id,
                );

                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_id,
                    metadata,
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
                service_id,
                mut metadata,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
            } => {
                let service_invocation_id =
                    ServiceInvocationId::with_service_id(service_id, metadata.invocation_id);
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
                    metadata.modification_time = MillisSinceEpoch::now();
                    state_storage
                        .store_invocation_status(
                            &service_invocation_id.service_id,
                            InvocationStatus::Invoked(metadata),
                        )
                        .await?;

                    collector.collect(ActuatorMessage::Invoke {
                        service_invocation_id,
                        invoke_input_journal: InvokeInputJournal::NoCachedJournal,
                    });
                } else {
                    unreachable!("There must be an entry that is completed if we want to resume");
                }
            }
            Effect::DropJournalAndPopInbox {
                service_id,
                inbox_sequence_number,
                journal_length,
                service_invocation,
            } => {
                // TODO: Only drop journals if the inbox is empty; this requires that keep track of the max journal length: https://github.com/restatedev/restate/issues/272
                state_storage
                    .drop_journal(&service_id, journal_length)
                    .await?;
                state_storage
                    .truncate_inbox(&service_id, inbox_sequence_number)
                    .await?;
                Self::invoke_service(state_storage, collector, service_invocation).await?;
            }
            Effect::NotifyInvocationResult {
                service_invocation_id,
                service_method,
                span_context,
                result,
            } => collector.collect(ActuatorMessage::CommitEndSpan {
                service_invocation_id,
                service_method,
                span_context,
                result,
            }),
            Effect::SendAckResponse(ack_response) => {
                collector.collect(ActuatorMessage::SendAckResponse(ack_response))
            }
            Effect::AbortInvocation(service_invocation_id) => {
                collector.collect(ActuatorMessage::AbortInvocation(service_invocation_id))
            }
        }

        Ok(())
    }

    async fn invoke_service<S: StateStorage, C: MessageCollector>(
        state_storage: &mut S,
        collector: &mut C,
        service_invocation: ServiceInvocation,
    ) -> Result<(), Error> {
        let creation_time = MillisSinceEpoch::now();
        let journal_metadata = JournalMetadata::new(
            service_invocation.method_name.clone(),
            service_invocation.span_context.clone(),
            1, // initial length is 1, because we store the poll input stream entry
        );

        state_storage
            .store_invocation_status(
                &service_invocation.id.service_id,
                InvocationStatus::Invoked(InvocationMetadata::new(
                    service_invocation.id.invocation_id,
                    journal_metadata.clone(),
                    service_invocation.response_sink,
                    creation_time,
                    creation_time,
                )),
            )
            .await?;

        let_assert!(
            restate_types::journal::raw::RawEntry {
                header: RawEntryHeader::PollInputStream { is_completed },
                entry
            } = Codec::serialize_as_unary_input_entry(service_invocation.argument)
        );

        let input_entry =
            EnrichedRawEntry::new(EnrichedEntryHeader::PollInputStream { is_completed }, entry);

        let raw_bytes = input_entry.entry.clone();

        state_storage
            .store_journal_entry(&service_invocation.id.service_id, 0, input_entry)
            .await?;

        collector.collect(ActuatorMessage::Invoke {
            service_invocation_id: service_invocation.id,
            invoke_input_journal: InvokeInputJournal::CachedJournal(
                journal_metadata,
                vec![PlainRawEntry::new(
                    RawEntryHeader::PollInputStream { is_completed },
                    raw_bytes,
                )],
            ),
        });
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
        service_id: ServiceId,
        metadata: InvocationMetadata,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> Result<(), Error> {
        debug_assert!(
            state_storage
                .load_completion_result(&service_id, entry_index)
                .await?
                .is_none(),
            "Only awakeable journal entries can have a completion result already stored"
        );

        Self::unchecked_append_journal_entry(
            state_storage,
            collector,
            service_id,
            metadata,
            entry_index,
            journal_entry,
        )
        .await
    }

    async fn unchecked_append_journal_entry<S: StateStorage, C: MessageCollector>(
        state_storage: &mut S,
        collector: &mut C,
        service_id: ServiceId,
        mut metadata: InvocationMetadata,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> Result<(), Error> {
        state_storage
            .store_journal_entry(&service_id, entry_index, journal_entry)
            .await?;

        let service_invocation_id =
            ServiceInvocationId::with_service_id(service_id, metadata.invocation_id);

        // update the journal metadata
        debug_assert_eq!(
            metadata.journal_metadata.length, entry_index,
            "journal should not have gaps"
        );
        metadata.journal_metadata.length = entry_index + 1;

        state_storage
            .store_invocation_status(
                &service_invocation_id.service_id,
                InvocationStatus::Invoked(metadata),
            )
            .await?;

        collector.collect(ActuatorMessage::AckStoredEntry {
            service_invocation_id,
            entry_index,
        });

        Ok(())
    }
}
