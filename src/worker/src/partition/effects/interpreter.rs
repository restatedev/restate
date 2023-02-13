use crate::partition::effects::{Effect, Effects, OutboxMessage};
use crate::partition::InvocationStatus;
use common::types::{EntryIndex, ServiceId, ServiceInvocation, ServiceInvocationId};
use journal::raw::{RawEntry, RawEntryCodec};
use journal::{Completion, CompletionResult, JournalRevision, PollInputStreamEntry};
use std::marker::PhantomData;

#[derive(Debug, thiserror::Error)]
pub(crate) struct Error<T>(#[from] T);

#[derive(Debug)]
pub(crate) enum ActuatorMessage {
    Invoke(ServiceInvocationId),
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
        journal_revision: JournalRevision,
    },
    ForwardCompletion {
        service_invocation_id: ServiceInvocationId,
        journal_revision: JournalRevision,
        completion: Completion,
    },
}

pub(crate) trait MessageCollector {
    fn collect(&mut self, message: ActuatorMessage);
}

pub(crate) trait StateStorage {
    fn write_invocation_status(&self, service_id: &ServiceId, status: &InvocationStatus);

    fn create_journal(&self, service_id: &ServiceId, method_name: impl AsRef<str>);

    fn store_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) -> JournalRevision;

    fn store_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    );

    fn enqueue_into_inbox(&self, seq_number: u64, service_invocation: ServiceInvocation);

    fn enqueue_into_outbox(&self, seq_number: u64, message: OutboxMessage);

    fn store_inbox_seq_number(&self, seq_number: u64);

    fn store_outbox_seq_number(&self, seq_number: u64);

    fn write_state(&self, service_id: &ServiceId, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>);

    fn clear_state(&self, service_id: &ServiceId, key: impl AsRef<[u8]>);

    fn store_timer(
        &self,
        service_invocation_id: &ServiceInvocationId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    );

    fn delete_timer(&self, service_id: &ServiceId, wake_up_time: u64, entry_index: EntryIndex);

    fn read_completion_result(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Option<CompletionResult>;

    fn read_journal_entry(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> Option<RawEntry>;

    fn truncate_outbox(&self, outbox_sequence_number: u64);

    fn truncate_inbox(&self, service_id: &ServiceId, inbox_sequence_number: u64);

    fn drop_journal(&self, service_id: &ServiceId);
}

pub(crate) trait Committable {
    fn commit(self);
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

    pub(crate) fn commit(self) -> Collector {
        let Self { txn, collector } = self;

        txn.commit();
        collector
    }
}

pub(crate) struct Interpreter<Codec> {
    _codec: PhantomData<Codec>,
}

impl<Codec: RawEntryCodec> Interpreter<Codec> {
    pub(crate) fn interpret_effects<S: StateStorage + Committable, C: MessageCollector>(
        effects: &mut Effects,
        state_storage: S,
        mut message_collector: C,
    ) -> Result<InterpretationResult<S, C>, Error<Codec::Error>> {
        for effect in effects.drain() {
            Self::interpret_effect(effect, &state_storage, &mut message_collector)?;
        }

        Ok(InterpretationResult::new(state_storage, message_collector))
    }

    fn interpret_effect<S: StateStorage, C: MessageCollector>(
        effect: Effect,
        state_storage: &S,
        collector: &mut C,
    ) -> Result<(), Error<Codec::Error>> {
        match effect {
            Effect::InvokeService(service_invocation) => {
                state_storage.write_invocation_status(
                    &service_invocation.id.service_id,
                    &InvocationStatus::Invoked(service_invocation.id.invocation_id),
                );
                state_storage.create_journal(
                    &service_invocation.id.service_id,
                    &service_invocation.method_name,
                );

                let input_stream_entry = PollInputStreamEntry {
                    result: service_invocation.argument,
                };

                state_storage.store_journal_entry(
                    &service_invocation.id.service_id,
                    1,
                    Self::into_raw_entry(&input_stream_entry),
                );

                // TODO: Send PollInputStreamEntry together with Invoke message
                collector.collect(ActuatorMessage::Invoke(service_invocation.id));
            }
            Effect::ResumeService(ServiceInvocationId {
                service_id,
                invocation_id,
            }) => {
                state_storage.write_invocation_status(
                    &service_id,
                    &InvocationStatus::Invoked(invocation_id),
                );

                collector.collect(ActuatorMessage::Invoke(ServiceInvocationId {
                    service_id,
                    invocation_id,
                }));
            }
            Effect::SuspendService(ServiceInvocationId {
                service_id,
                invocation_id,
            }) => {
                state_storage.write_invocation_status(
                    &service_id,
                    &InvocationStatus::Suspended(invocation_id),
                );
            }
            Effect::FreeService(service_id) => {
                state_storage.write_invocation_status(&service_id, &InvocationStatus::Free)
            }
            Effect::EnqueueIntoInbox {
                seq_number,
                service_invocation,
            } => {
                state_storage.enqueue_into_inbox(seq_number, service_invocation);
                state_storage.store_inbox_seq_number(seq_number);
            }
            Effect::EnqueueIntoOutbox {
                seq_number,
                message,
            } => {
                state_storage.enqueue_into_outbox(seq_number, message);
                state_storage.store_outbox_seq_number(seq_number);

                collector.collect(ActuatorMessage::NewOutboxMessage(seq_number));
            }
            Effect::SetState {
                service_id,
                key,
                value,
            } => state_storage.write_state(&service_id, key, value),
            Effect::ClearState { service_id, key } => {
                state_storage.clear_state(&service_id, key);
            }
            Effect::RegisterTimer {
                service_invocation_id,
                wake_up_time,
                entry_index,
            } => {
                state_storage.store_timer(&service_invocation_id, wake_up_time, entry_index);

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
                state_storage.delete_timer(&service_id, wake_up_time, entry_index);
            }
            Effect::AppendJournalEntry {
                service_invocation_id,
                entry_index,
                raw_entry,
            } => {
                debug_assert!(
                    state_storage
                        .read_completion_result(&service_invocation_id.service_id, entry_index)
                        .is_none(),
                    "Only awakeable journal entries can have a completion result already stored"
                );
                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_invocation_id,
                    entry_index,
                    raw_entry,
                );
            }
            Effect::AppendAwakeableEntry {
                service_invocation_id,
                entry_index,
                mut raw_entry,
            } => {
                // check whether the completion has arrived first
                if let Some(completion_result) = state_storage
                    .read_completion_result(&service_invocation_id.service_id, entry_index)
                {
                    Codec::write_completion(&mut raw_entry, completion_result)?;
                }

                Self::append_journal_entry(
                    state_storage,
                    collector,
                    service_invocation_id,
                    entry_index,
                    raw_entry,
                );
            }
            Effect::TruncateOutbox(outbox_sequence_number) => {
                state_storage.truncate_outbox(outbox_sequence_number);
            }
            Effect::StoreCompletion {
                service_invocation_id,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
            } => {
                Self::store_completion(state_storage, &service_invocation_id, entry_index, result)?;
            }
            Effect::StoreCompletionAndForward {
                service_invocation_id,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
            } => {
                if let Some(journal_revision) = Self::store_completion(
                    state_storage,
                    &service_invocation_id,
                    entry_index,
                    result.clone(),
                )? {
                    collector.collect(ActuatorMessage::ForwardCompletion {
                        service_invocation_id,
                        journal_revision,
                        completion: Completion {
                            entry_index,
                            result,
                        },
                    });
                }
            }
            Effect::DropJournal(service_id) => {
                state_storage.drop_journal(&service_id);
            }
            Effect::PopInbox {
                service_id,
                inbox_sequence_number,
            } => {
                state_storage.truncate_inbox(&service_id, inbox_sequence_number);
            }
        }

        Ok(())
    }

    fn store_completion<S: StateStorage>(
        state_storage: &S,
        service_invocation_id: &ServiceInvocationId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> Result<Option<JournalRevision>, Codec::Error> {
        let result = if let Some(mut raw_entry) =
            state_storage.read_journal_entry(&service_invocation_id.service_id, entry_index)
        {
            Codec::write_completion(&mut raw_entry, completion_result)?;
            Some(state_storage.store_journal_entry(
                &service_invocation_id.service_id,
                entry_index,
                raw_entry,
            ))
        } else {
            state_storage.store_completion_result(
                &service_invocation_id.service_id,
                entry_index,
                completion_result,
            );
            None
        };

        Ok(result)
    }

    fn append_journal_entry<S: StateStorage, C: MessageCollector>(
        state_storage: &S,
        collector: &mut C,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) {
        let journal_revision = state_storage.store_journal_entry(
            &service_invocation_id.service_id,
            entry_index,
            raw_entry,
        );

        collector.collect(ActuatorMessage::AckStoredEntry {
            service_invocation_id,
            entry_index,
            journal_revision,
        });
    }

    fn into_raw_entry(_input_stream_entry: &PollInputStreamEntry) -> RawEntry {
        todo!()
    }
}
