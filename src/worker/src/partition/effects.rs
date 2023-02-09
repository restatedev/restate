use crate::partition::{InvocationStatus, StorageWriterHelper};
use bytes::Bytes;
use common::types::{EntryIndex, Response, ServiceId, ServiceInvocation, ServiceInvocationId};
use journal::raw::{RawEntry, RawEntryCodec};
use journal::{Completion, CompletionResult, JournalRevision, PollInputStreamEntry};
use std::vec::Drain;
use storage_api::WriteTransaction;

#[derive(Debug)]
pub(crate) enum OutboxMessage {
    Invocation(ServiceInvocation),
    Response(Response),
}

#[derive(Debug)]
pub(crate) enum Effect {
    // service status changes
    InvokeService(ServiceInvocation),
    ResumeService(ServiceInvocationId),
    SuspendService(ServiceInvocationId),
    FreeService(ServiceId),

    // In-/outbox
    EnqueueIntoInbox {
        seq_number: u64,
        service_invocation: ServiceInvocation,
    },
    EnqueueIntoOutbox {
        seq_number: u64,
        message: OutboxMessage,
    },
    TruncateOutbox(u64),
    PopInbox {
        service_id: ServiceId,
        inbox_sequence_number: u64,
    },

    // State
    SetState {
        service_id: ServiceId,
        key: Bytes,
        value: Bytes,
    },
    ClearState {
        service_id: ServiceId,
        key: Bytes,
    },

    // Timers
    RegisterTimer {
        service_invocation_id: ServiceInvocationId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    },
    DeleteTimer {
        service_id: ServiceId,
        wake_up_time: u64,
        entry_index: EntryIndex,
    },

    // Journal operations
    AppendJournalEntry {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    },
    AppendAwakeableEntry {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    },
    StoreCompletion {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    },
    StoreCompletionAndForward {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    },
    DropJournal(ServiceId),
}

#[derive(Debug)]
pub(super) enum ActuatorMessage {
    Invoke(ServiceInvocationId),
    NewOutboxMessage(u64),
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

#[derive(Debug, Default)]
pub(crate) struct Effects {
    effects: Vec<Effect>,
}

impl Effects {
    pub(crate) fn clear(&mut self) {
        self.effects.clear()
    }

    pub(crate) fn drain(&mut self) -> Drain<'_, Effect> {
        self.effects.drain(..)
    }

    pub(crate) fn invoke_service(&mut self, service_invocation: ServiceInvocation) {
        self.effects.push(Effect::InvokeService(service_invocation));
    }

    pub(crate) fn resume_service(&mut self, service_invocation_id: ServiceInvocationId) {
        self.effects
            .push(Effect::ResumeService(service_invocation_id));
    }

    pub(crate) fn suspend_service(&mut self, service_invocation_id: ServiceInvocationId) {
        self.effects
            .push(Effect::SuspendService(service_invocation_id))
    }

    pub(crate) fn free_service(&mut self, service_id: ServiceId) {
        self.effects.push(Effect::FreeService(service_id));
    }

    pub(crate) fn enqueue_into_inbox(
        &mut self,
        seq_number: u64,
        service_invocation: ServiceInvocation,
    ) {
        self.effects.push(Effect::EnqueueIntoInbox {
            seq_number,
            service_invocation,
        })
    }

    pub(crate) fn enqueue_into_outbox(&mut self, seq_number: u64, message: OutboxMessage) {
        self.effects.push(Effect::EnqueueIntoOutbox {
            seq_number,
            message,
        })
    }

    pub(crate) fn set_state(&mut self, service_id: ServiceId, key: Bytes, value: Bytes) {
        self.effects.push(Effect::SetState {
            service_id,
            key,
            value,
        })
    }

    pub(crate) fn clear_state(&mut self, service_id: ServiceId, key: Bytes) {
        self.effects.push(Effect::ClearState { service_id, key })
    }

    pub(crate) fn register_timer(
        &mut self,
        wake_up_time: u64,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::RegisterTimer {
            service_invocation_id,
            wake_up_time,
            entry_index,
        })
    }

    pub(crate) fn delete_timer(
        &mut self,
        wake_up_time: u64,
        service_id: ServiceId,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::DeleteTimer {
            service_id,
            wake_up_time,
            entry_index,
        });
    }

    pub(crate) fn append_journal_entry(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) {
        self.effects.push(Effect::AppendJournalEntry {
            service_invocation_id,
            entry_index,
            raw_entry,
        })
    }

    pub(crate) fn append_awakeable_entry(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) {
        self.effects.push(Effect::AppendAwakeableEntry {
            service_invocation_id,
            entry_index,
            raw_entry,
        })
    }

    pub(crate) fn truncate_outbox(&mut self, outbox_sequence_number: u64) {
        self.effects
            .push(Effect::TruncateOutbox(outbox_sequence_number));
    }

    pub(crate) fn store_completion(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) {
        self.effects.push(Effect::StoreCompletion {
            service_invocation_id,
            completion,
        })
    }

    pub(crate) fn store_and_forward_completion(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) {
        self.effects.push(Effect::StoreCompletionAndForward {
            service_invocation_id,
            completion,
        })
    }

    pub(crate) fn drop_journal(&mut self, service_id: ServiceId) {
        self.effects.push(Effect::DropJournal(service_id));
    }

    pub(crate) fn pop_inbox(&mut self, service_id: ServiceId, inbox_sequence_number: u64) {
        self.effects.push(Effect::PopInbox {
            service_id,
            inbox_sequence_number,
        });
    }
}

pub(super) trait Collector {
    fn collect(&mut self, message: ActuatorMessage);
}

#[derive(Debug, thiserror::Error)]
pub struct Error<T>(#[from] T);

pub(super) fn interpret_effects<Txn, C: Collector, Codec: RawEntryCodec>(
    effects: &mut Effects,
    storage: &StorageWriterHelper<Txn>,
    collector: &mut C,
) -> Result<(), Error<Codec::Error>> {
    for effect in effects.drain() {
        interpret_effect::<Txn, C, Codec>(effect, &storage, collector)?;
    }

    Ok(())
}

fn interpret_effect<Txn, C: Collector, Codec: RawEntryCodec>(
    effect: Effect,
    storage: &StorageWriterHelper<Txn>,
    collector: &mut C,
) -> Result<(), Error<Codec::Error>> {
    match effect {
        Effect::InvokeService(service_invocation) => {
            storage.write_invocation_status(
                &service_invocation.id.service_id,
                &InvocationStatus::Invoked(service_invocation.id.invocation_id),
            );
            storage.create_journal(
                &service_invocation.id.service_id,
                &service_invocation.method_name,
            );

            let input_stream_entry = PollInputStreamEntry {
                result: service_invocation.argument,
            };

            storage.store_journal_entry(
                &service_invocation.id.service_id,
                1,
                into_raw_entry(&input_stream_entry),
            );

            // TODO: Send PollInputStreamEntry together with Invoke message
            collector.collect(ActuatorMessage::Invoke(service_invocation.id));
        }
        Effect::ResumeService(ServiceInvocationId {
            service_id,
            invocation_id,
        }) => {
            storage.write_invocation_status(&service_id, &InvocationStatus::Invoked(invocation_id));

            collector.collect(ActuatorMessage::Invoke(ServiceInvocationId {
                service_id,
                invocation_id,
            }));
        }
        Effect::SuspendService(ServiceInvocationId {
            service_id,
            invocation_id,
        }) => {
            storage
                .write_invocation_status(&service_id, &InvocationStatus::Suspended(invocation_id));
        }
        Effect::FreeService(service_id) => {
            storage.write_invocation_status(&service_id, &InvocationStatus::Free)
        }
        Effect::EnqueueIntoInbox {
            seq_number,
            service_invocation,
        } => {
            storage.enqueue_into_inbox(seq_number, service_invocation);
            storage.store_inbox_seq_number(seq_number);
        }
        Effect::EnqueueIntoOutbox {
            seq_number,
            message,
        } => {
            storage.enqueue_into_outbox(seq_number, message);
            storage.store_outbox_seq_number(seq_number);

            collector.collect(ActuatorMessage::NewOutboxMessage(seq_number));
        }
        Effect::SetState {
            service_id,
            key,
            value,
        } => storage.write_state(&service_id, key, value),
        Effect::ClearState { service_id, key } => {
            storage.clear_state(&service_id, key);
        }
        Effect::RegisterTimer {
            service_invocation_id,
            wake_up_time,
            entry_index,
        } => {
            storage.store_timer(&service_invocation_id, wake_up_time, entry_index);

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
            storage.delete_timer(&service_id, wake_up_time, entry_index);
        }
        Effect::AppendJournalEntry {
            service_invocation_id,
            entry_index,
            raw_entry,
        } => {
            debug_assert!(
                storage
                    .read_completion_result(&service_invocation_id.service_id, entry_index)
                    .is_none(),
                "Only awakeable journal entries can have a completion result already stored"
            );
            append_journal_entry(
                storage,
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
            if let Some(completion_result) =
                storage.read_completion_result(&service_invocation_id.service_id, entry_index)
            {
                Codec::write_completion(&mut raw_entry, completion_result)?;
            }

            append_journal_entry(
                storage,
                collector,
                service_invocation_id,
                entry_index,
                raw_entry,
            );
        }
        Effect::TruncateOutbox(outbox_sequence_number) => {
            storage.truncate_outbox(outbox_sequence_number);
        }
        Effect::StoreCompletion {
            service_invocation_id,
            completion:
                Completion {
                    entry_index,
                    result,
                },
        } => {
            store_completion::<Txn, Codec>(storage, &service_invocation_id, entry_index, result)?;
        }
        Effect::StoreCompletionAndForward {
            service_invocation_id,
            completion:
                Completion {
                    entry_index,
                    result,
                },
        } => {
            if let Some(journal_revision) = store_completion::<Txn, Codec>(
                storage,
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
            storage.drop_journal(&service_id);
        }
        Effect::PopInbox {
            service_id,
            inbox_sequence_number,
        } => {
            storage.truncate_inbox(&service_id, inbox_sequence_number);
        }
    }

    Ok(())
}

fn store_completion<Txn, Codec: RawEntryCodec>(
    storage: &StorageWriterHelper<Txn>,
    service_invocation_id: &ServiceInvocationId,
    entry_index: EntryIndex,
    completion_result: CompletionResult,
) -> Result<Option<JournalRevision>, Codec::Error> {
    let result = if let Some(mut raw_entry) =
        storage.read_journal_entry(&service_invocation_id.service_id, entry_index)
    {
        Codec::write_completion(&mut raw_entry, completion_result)?;
        Some(storage.store_journal_entry(&service_invocation_id.service_id, entry_index, raw_entry))
    } else {
        storage.store_completion_result(
            &service_invocation_id.service_id,
            entry_index,
            completion_result,
        );
        None
    };

    Ok(result)
}

fn append_journal_entry<Txn, C: Collector>(
    storage: &StorageWriterHelper<Txn>,
    collector: &mut C,
    service_invocation_id: ServiceInvocationId,
    entry_index: EntryIndex,
    mut raw_entry: RawEntry,
) {
    let journal_revision =
        storage.store_journal_entry(&service_invocation_id.service_id, entry_index, raw_entry);

    collector.collect(ActuatorMessage::AckStoredEntry {
        service_invocation_id,
        entry_index,
        journal_revision,
    });
}

fn into_raw_entry(input_stream_entry: &PollInputStreamEntry) -> RawEntry {
    unimplemented!()
}
