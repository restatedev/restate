use crate::partition::effects::{Effect, Effects, OutboxMessage};
use crate::partition::InvocationStatus;
use bytes::Bytes;
use common::types::{
    EntryIndex, ServiceId, ServiceInvocation, ServiceInvocationId, ServiceInvocationResponseSink,
};
use common::utils::GenericError;
use futures::future::BoxFuture;
use invoker::InvokeInputJournal;
use journal::raw::{RawEntry, RawEntryCodec, RawEntryCodecError};
use journal::{Completion, CompletionResult};
use std::marker::PhantomData;
use tracing::trace;

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
        seq_number: u64,
        message: OutboxMessage,
    },
    #[allow(dead_code)]
    RegisterTimer {
        service_invocation_id: ServiceInvocationId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    },
    AckStoredEntry {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    },
    ForwardCompletion {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
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
}

pub(crate) trait StateStorage {
    // Invocation status
    fn store_invocation_status(
        &self,
        service_id: &ServiceId,
        status: &InvocationStatus,
    ) -> Result<(), StateStorageError>;

    // Journal operations
    fn create_journal(
        &self,
        service_invocation_id: &ServiceInvocationId,
        method_name: impl AsRef<str>,
        response_sink: &ServiceInvocationResponseSink,
    ) -> Result<(), StateStorageError>;

    fn drop_journal(&self, service_id: &ServiceId) -> Result<(), StateStorageError>;

    fn store_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        raw_entry: &RawEntry,
    ) -> Result<(), StateStorageError>;

    fn store_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: &CompletionResult,
    ) -> Result<(), StateStorageError>;

    // TODO: Replace with async trait or proper future
    fn load_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, StateStorageError>>;

    // TODO: Replace with async trait or proper future
    fn load_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<RawEntry>, StateStorageError>>;

    // In-/outbox
    fn enqueue_into_inbox(
        &self,
        seq_number: u64,
        service_invocation: &ServiceInvocation,
    ) -> Result<(), StateStorageError>;

    fn enqueue_into_outbox(
        &self,
        seq_number: u64,
        message: &OutboxMessage,
    ) -> Result<(), StateStorageError>;

    fn store_inbox_seq_number(&self, seq_number: u64) -> Result<(), StateStorageError>;

    fn store_outbox_seq_number(&self, seq_number: u64) -> Result<(), StateStorageError>;

    fn truncate_outbox(&self, outbox_sequence_number: u64) -> Result<(), StateStorageError>;

    fn truncate_inbox(
        &self,
        service_id: &ServiceId,
        inbox_sequence_number: u64,
    ) -> Result<(), StateStorageError>;

    // State
    fn store_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), StateStorageError>;

    // TODO: Replace with async trait or proper future
    fn load_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
    ) -> BoxFuture<Result<Option<Bytes>, StateStorageError>>;

    fn clear_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
    ) -> Result<(), StateStorageError>;

    // Timer
    fn store_timer(
        &self,
        service_invocation_id: &ServiceInvocationId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    ) -> Result<(), StateStorageError>;

    fn delete_timer(
        &self,
        service_id: &ServiceId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    ) -> Result<(), StateStorageError>;
}

#[derive(Debug, thiserror::Error)]
#[error("failed committing results: {source:?}")]
pub(crate) struct CommitError {
    source: Option<GenericError>,
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
        state_storage: S,
        mut message_collector: C,
    ) -> Result<InterpretationResult<S, C>, Error> {
        for effect in effects.drain() {
            Self::interpret_effect(effect, &state_storage, &mut message_collector).await?;
        }

        Ok(InterpretationResult::new(state_storage, message_collector))
    }

    async fn interpret_effect<S: StateStorage, C: MessageCollector>(
        effect: Effect,
        state_storage: &S,
        collector: &mut C,
    ) -> Result<(), Error> {
        trace!(?effect, "Interpreting effect");

        match effect {
            Effect::InvokeService(service_invocation) => {
                state_storage.store_invocation_status(
                    &service_invocation.id.service_id,
                    &InvocationStatus::Invoked(service_invocation.id.invocation_id),
                )?;

                state_storage.create_journal(
                    &service_invocation.id,
                    &service_invocation.method_name,
                    &service_invocation.response_sink,
                )?;

                let input_entry =
                    Codec::serialize_as_unary_input_entry(service_invocation.argument);

                state_storage.store_journal_entry(
                    &service_invocation.id.service_id,
                    0,
                    &input_entry,
                )?;

                // TODO: Send raw PollInputStreamEntry together with Invoke message
                collector.collect(ActuatorMessage::Invoke {
                    service_invocation_id: service_invocation.id,
                    invoke_input_journal: InvokeInputJournal::NoCachedJournal,
                });
            }
            Effect::ResumeService(ServiceInvocationId {
                service_id,
                invocation_id,
            }) => {
                state_storage.store_invocation_status(
                    &service_id,
                    &InvocationStatus::Invoked(invocation_id),
                )?;

                collector.collect(ActuatorMessage::Invoke {
                    service_invocation_id: ServiceInvocationId {
                        service_id,
                        invocation_id,
                    },
                    invoke_input_journal: InvokeInputJournal::NoCachedJournal,
                });
            }
            Effect::SuspendService {
                service_invocation_id:
                    ServiceInvocationId {
                        service_id,
                        invocation_id,
                    },
                waiting_for_completed_entries,
            } => {
                state_storage.store_invocation_status(
                    &service_id,
                    &InvocationStatus::Suspended {
                        invocation_id,
                        waiting_for_completed_entries,
                    },
                )?;
            }
            Effect::DropJournalAndFreeService(service_id) => {
                state_storage.drop_journal(&service_id)?;
                state_storage.store_invocation_status(&service_id, &InvocationStatus::Free)?;
            }
            Effect::EnqueueIntoInbox {
                seq_number,
                service_invocation,
            } => {
                state_storage.enqueue_into_inbox(seq_number, &service_invocation)?;
                state_storage.store_inbox_seq_number(seq_number)?;
            }
            Effect::EnqueueIntoOutbox {
                seq_number,
                message,
            } => {
                state_storage.enqueue_into_outbox(seq_number, &message)?;
                state_storage.store_outbox_seq_number(seq_number)?;

                collector.collect(ActuatorMessage::NewOutboxMessage {
                    seq_number,
                    message,
                });
            }
            Effect::SetState {
                service_invocation_id,
                key,
                value,
                raw_entry,
                entry_index,
            } => {
                state_storage.store_state(&service_invocation_id.service_id, key, value)?;
                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_invocation_id,
                    entry_index,
                    raw_entry,
                )
                .await?;
            }
            Effect::ClearState {
                service_invocation_id,
                key,
                raw_entry,
                entry_index,
            } => {
                state_storage.clear_state(&service_invocation_id.service_id, key)?;
                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_invocation_id,
                    entry_index,
                    raw_entry,
                )
                .await?;
            }
            Effect::GetStateAndAppendCompletedEntry {
                key,
                service_invocation_id,
                mut raw_entry,
                entry_index,
            } => {
                let value = state_storage
                    .load_state(&service_invocation_id.service_id, &key)
                    .await?;

                let completion_result = value
                    .map(CompletionResult::Success)
                    .unwrap_or(CompletionResult::Empty);

                Codec::write_completion(&mut raw_entry, completion_result.clone())?;

                Self::unchecked_append_journal_entry(
                    state_storage,
                    collector,
                    service_invocation_id.clone(),
                    entry_index,
                    raw_entry,
                )?;

                collector.collect(ActuatorMessage::ForwardCompletion {
                    service_invocation_id,
                    completion: Completion {
                        entry_index,
                        result: completion_result,
                    },
                })
            }
            Effect::RegisterTimer {
                service_invocation_id,
                wake_up_time,
                entry_index,
            } => {
                state_storage.store_timer(&service_invocation_id, wake_up_time, entry_index)?;

                collector.collect(ActuatorMessage::RegisterTimer {
                    service_invocation_id,
                    wake_up_time,
                    entry_index,
                });
            }
            Effect::DeleteTimer {
                service_id,
                wake_up_time,
                entry_index,
            } => {
                state_storage.delete_timer(&service_id, wake_up_time, entry_index)?;
            }
            Effect::AppendJournalEntry {
                service_invocation_id,
                entry_index,
                raw_entry,
            } => {
                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_invocation_id,
                    entry_index,
                    raw_entry,
                )
                .await?;
            }
            Effect::AppendAwakeableEntry {
                service_invocation_id,
                entry_index,
                mut raw_entry,
            } => {
                // check whether the completion has arrived first
                if let Some(completion_result) = state_storage
                    .load_completion_result(&service_invocation_id.service_id, entry_index)
                    .await?
                {
                    Codec::write_completion(&mut raw_entry, completion_result)?;
                }

                Self::unchecked_append_journal_entry(
                    state_storage,
                    collector,
                    service_invocation_id,
                    entry_index,
                    raw_entry,
                )?;
            }
            Effect::AppendJournalEntryAndAck {
                service_invocation_id,
                raw_entry,
                entry_index,
            } => {
                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_invocation_id.clone(),
                    entry_index,
                    raw_entry,
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
                state_storage.truncate_outbox(outbox_sequence_number)?;
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
            } => {
                state_storage.drop_journal(&service_id)?;
                state_storage.truncate_inbox(&service_id, inbox_sequence_number)?;
            }
        }

        Ok(())
    }

    /// Stores the given completion. Returns `true` if an [`RawEntry`] was completed.
    async fn store_completion<S: StateStorage>(
        state_storage: &S,
        service_invocation_id: &ServiceInvocationId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> Result<bool, Error> {
        if let Some(mut raw_entry) = state_storage
            .load_journal_entry(&service_invocation_id.service_id, entry_index)
            .await?
        {
            Codec::write_completion(&mut raw_entry, completion_result)?;
            state_storage.store_journal_entry(
                &service_invocation_id.service_id,
                entry_index,
                &raw_entry,
            )?;
            Ok(true)
        } else {
            state_storage.store_completion_result(
                &service_invocation_id.service_id,
                entry_index,
                &completion_result,
            )?;
            Ok(false)
        }
    }

    async fn append_journal_entry<S: StateStorage, C: MessageCollector>(
        state_storage: &S,
        collector: &mut C,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) -> Result<(), Error> {
        debug_assert!(
            state_storage
                .load_completion_result(&service_invocation_id.service_id, entry_index)
                .await?
                .is_none(),
            "Only awakeable journal entries can have a completion result already stored"
        );

        Self::unchecked_append_journal_entry(
            state_storage,
            collector,
            service_invocation_id,
            entry_index,
            raw_entry,
        )
    }

    fn unchecked_append_journal_entry<S: StateStorage, C: MessageCollector>(
        state_storage: &S,
        collector: &mut C,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) -> Result<(), Error> {
        state_storage.store_journal_entry(
            &service_invocation_id.service_id,
            entry_index,
            &raw_entry,
        )?;

        collector.collect(ActuatorMessage::AckStoredEntry {
            service_invocation_id,
            entry_index,
        });

        Ok(())
    }
}
