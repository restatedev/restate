use crate::partition::effects::{Effect, Effects, OutboxMessage};
use crate::partition::InvocationStatus;
use bytes::Bytes;
use common::types::{EntryIndex, ServiceId, ServiceInvocation, ServiceInvocationId};
use common::utils::GenericError;
use futures::future::BoxFuture;
use invoker::InvokeInputJournal;
use journal::raw::{RawEntry, RawEntryCodec};
use journal::{Completion, CompletionResult, PollInputStreamEntry};
use std::marker::PhantomData;
use tracing::trace;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("failed to read state while interpreting effects: {0}")]
    State(GenericError),
    #[error("failed to decode entry while interpreting effects: {0}")]
    Codec(GenericError),
}

#[derive(Debug)]
pub(crate) enum ActuatorMessage {
    Invoke {
        service_invocation_id: ServiceInvocationId,
        invoke_input_journal: InvokeInputJournal,
    },
    NewOutboxMessage(u64),
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

pub(crate) trait StateStorage {
    type Error: std::error::Error + Send + Sync + 'static;

    // Invocation status
    fn store_invocation_status(
        &self,
        service_id: &ServiceId,
        status: &InvocationStatus,
    ) -> Result<(), Self::Error>;

    // Journal operations
    fn create_journal(
        &self,
        service_id: &ServiceId,
        method_name: impl AsRef<str>,
    ) -> Result<(), Self::Error>;

    fn drop_journal(&self, service_id: &ServiceId) -> Result<(), Self::Error>;

    fn store_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        raw_entry: &RawEntry,
    ) -> Result<(), Self::Error>;

    fn store_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: &CompletionResult,
    ) -> Result<(), Self::Error>;

    // TODO: Replace with async trait or proper future
    fn load_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<CompletionResult>, Self::Error>>;

    // TODO: Replace with async trait or proper future
    fn load_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<Option<RawEntry>, Self::Error>>;

    // In-/outbox
    fn enqueue_into_inbox(
        &self,
        seq_number: u64,
        service_invocation: &ServiceInvocation,
    ) -> Result<(), Self::Error>;

    fn enqueue_into_outbox(
        &self,
        seq_number: u64,
        message: &OutboxMessage,
    ) -> Result<(), Self::Error>;

    fn store_inbox_seq_number(&self, seq_number: u64) -> Result<(), Self::Error>;

    fn store_outbox_seq_number(&self, seq_number: u64) -> Result<(), Self::Error>;

    fn truncate_outbox(&self, outbox_sequence_number: u64) -> Result<(), Self::Error>;

    fn truncate_inbox(
        &self,
        service_id: &ServiceId,
        inbox_sequence_number: u64,
    ) -> Result<(), Self::Error>;

    // State
    fn store_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<(), Self::Error>;

    // TODO: Replace with async trait or proper future
    fn load_state(
        &self,
        service_id: &ServiceId,
        key: impl AsRef<[u8]>,
    ) -> BoxFuture<Result<Bytes, Self::Error>>;

    fn clear_state(&self, service_id: &ServiceId, key: impl AsRef<[u8]>)
        -> Result<(), Self::Error>;

    // Timer
    fn store_timer(
        &self,
        service_invocation_id: &ServiceInvocationId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    ) -> Result<(), Self::Error>;

    fn delete_timer(
        &self,
        service_id: &ServiceId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    ) -> Result<(), Self::Error>;
}

pub(crate) trait Committable {
    type Error: std::error::Error + Send + Sync + 'static;

    // TODO: Replace with async trait or proper future
    fn commit(self) -> BoxFuture<'static, Result<(), Self::Error>>;
}

#[must_use = "Don't forget to commit the interpretation result"]
pub(crate) struct InterpretationResult<Txn, Collector> {
    txn: Txn,
    collector: Collector,
}

#[derive(Debug, thiserror::Error)]
#[error("failed committing results: {0}")]
pub(crate) struct CommitError(#[from] GenericError);

impl<Txn, Collector> InterpretationResult<Txn, Collector>
where
    Txn: Committable,
{
    pub(crate) fn new(txn: Txn, collector: Collector) -> Self {
        Self { txn, collector }
    }

    pub(crate) async fn commit(self) -> Result<Collector, CommitError> {
        let Self { txn, collector } = self;

        txn.commit().await.map_err(|err| CommitError(err.into()))?;
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
                state_storage
                    .store_invocation_status(
                        &service_invocation.id.service_id,
                        &InvocationStatus::Invoked(service_invocation.id.invocation_id),
                    )
                    .map_err(|err| Error::State(err.into()))?;
                state_storage
                    .create_journal(
                        &service_invocation.id.service_id,
                        &service_invocation.method_name,
                    )
                    .map_err(|err| Error::State(err.into()))?;

                let input_stream_entry = PollInputStreamEntry {
                    result: service_invocation.argument,
                };

                state_storage
                    .store_journal_entry(
                        &service_invocation.id.service_id,
                        1,
                        &Self::into_raw_entry(&input_stream_entry),
                    )
                    .map_err(|err| Error::State(err.into()))?;

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
                state_storage
                    .store_invocation_status(&service_id, &InvocationStatus::Invoked(invocation_id))
                    .map_err(|err| Error::State(err.into()))?;

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
                state_storage
                    .store_invocation_status(
                        &service_id,
                        &InvocationStatus::Suspended {
                            invocation_id,
                            waiting_for_completed_entries,
                        },
                    )
                    .map_err(|err| Error::State(err.into()))?;
            }
            Effect::DropJournalAndFreeService(service_id) => {
                state_storage
                    .drop_journal(&service_id)
                    .map_err(|err| Error::State(err.into()))?;
                state_storage
                    .store_invocation_status(&service_id, &InvocationStatus::Free)
                    .map_err(|err| Error::State(err.into()))?;
            }
            Effect::EnqueueIntoInbox {
                seq_number,
                service_invocation,
            } => {
                state_storage
                    .enqueue_into_inbox(seq_number, &service_invocation)
                    .map_err(|err| Error::State(err.into()))?;
                state_storage
                    .store_inbox_seq_number(seq_number)
                    .map_err(|err| Error::State(err.into()))?;
            }
            Effect::EnqueueIntoOutbox {
                seq_number,
                message,
            } => {
                state_storage
                    .enqueue_into_outbox(seq_number, &message)
                    .map_err(|err| Error::State(err.into()))?;
                state_storage
                    .store_outbox_seq_number(seq_number)
                    .map_err(|err| Error::State(err.into()))?;

                collector.collect(ActuatorMessage::NewOutboxMessage(seq_number));
            }
            Effect::SetState {
                service_invocation_id,
                key,
                value,
                raw_entry,
                entry_index,
            } => {
                state_storage
                    .store_state(&service_invocation_id.service_id, key, value)
                    .map_err(|err| Error::State(err.into()))?;
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
                state_storage
                    .clear_state(&service_invocation_id.service_id, key)
                    .map_err(|err| Error::State(err.into()))?;
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
                    .await
                    .map_err(|err| Error::State(err.into()))?;

                Codec::write_completion(&mut raw_entry, CompletionResult::Success(value.clone()))
                    .map_err(|err| Error::Codec(err.into()))?;

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
                        result: CompletionResult::Success(value),
                    },
                })
            }
            Effect::RegisterTimer {
                service_invocation_id,
                wake_up_time,
                entry_index,
            } => {
                state_storage
                    .store_timer(&service_invocation_id, wake_up_time, entry_index)
                    .map_err(|err| Error::State(err.into()))?;

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
                state_storage
                    .delete_timer(&service_id, wake_up_time, entry_index)
                    .map_err(|err| Error::State(err.into()))?;
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
                    .await
                    .map_err(|err| Error::State(err.into()))?
                {
                    Codec::write_completion(&mut raw_entry, completion_result)
                        .map_err(|err| Error::Codec(err.into()))?;
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
                state_storage
                    .truncate_outbox(outbox_sequence_number)
                    .map_err(|err| Error::State(err.into()))?;
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
                state_storage
                    .drop_journal(&service_id)
                    .map_err(|err| Error::State(err.into()))?;
                state_storage
                    .truncate_inbox(&service_id, inbox_sequence_number)
                    .map_err(|err| Error::State(err.into()))?;
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
            .await
            .map_err(|err| Error::State(err.into()))?
        {
            Codec::write_completion(&mut raw_entry, completion_result)
                .map_err(|err| Error::Codec(err.into()))?;
            state_storage
                .store_journal_entry(&service_invocation_id.service_id, entry_index, &raw_entry)
                .map_err(|err| Error::State(err.into()))?;
            Ok(true)
        } else {
            state_storage
                .store_completion_result(
                    &service_invocation_id.service_id,
                    entry_index,
                    &completion_result,
                )
                .map_err(|err| Error::State(err.into()))?;
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
                .await
                .map_err(|err| Error::State(err.into()))?
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
        state_storage
            .store_journal_entry(&service_invocation_id.service_id, entry_index, &raw_entry)
            .map_err(|err| Error::State(err.into()))?;

        collector.collect(ActuatorMessage::AckStoredEntry {
            service_invocation_id,
            entry_index,
        });

        Ok(())
    }

    fn into_raw_entry(_input_stream_entry: &PollInputStreamEntry) -> RawEntry {
        todo!()
    }
}
