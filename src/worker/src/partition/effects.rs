use bytes::Bytes;
use common::types::{
    EntryIndex, InvocationResponse, ServiceId, ServiceInvocation, ServiceInvocationId,
};
use journal::raw::RawEntry;
use journal::Completion;
use std::collections::HashSet;
use std::vec::Drain;

mod interpreter;

pub(crate) use interpreter::{ActuatorMessage, Interpreter, MessageCollector};

#[derive(Debug, Clone)]
pub(crate) enum OutboxMessage {
    Invocation(ServiceInvocation),
    Response(InvocationResponse),
}

#[derive(Debug)]
pub(crate) enum Effect {
    // service status changes
    InvokeService(ServiceInvocation),
    ResumeService(ServiceInvocationId),
    SuspendService {
        service_invocation_id: ServiceInvocationId,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    DropJournalAndFreeService(ServiceId),

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
    DropJournalAndPopInbox {
        service_id: ServiceId,
        inbox_sequence_number: u64,
    },

    // State
    SetState {
        service_invocation_id: ServiceInvocationId,
        key: Bytes,
        value: Bytes,
        raw_entry: RawEntry,
        entry_index: EntryIndex,
    },
    ClearState {
        service_invocation_id: ServiceInvocationId,
        key: Bytes,
        raw_entry: RawEntry,
        entry_index: EntryIndex,
    },
    GetStateAndAppendCompletedEntry {
        key: Bytes,
        service_invocation_id: ServiceInvocationId,
        raw_entry: RawEntry,
        entry_index: EntryIndex,
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
    AppendJournalEntryAndAck {
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
    StoreCompletionAndResume {
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    },
}

#[derive(Debug, Default)]
pub(crate) struct Effects {
    effects: Vec<Effect>,
}

impl Effects {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            effects: Vec::with_capacity(capacity),
        }
    }

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

    pub(crate) fn suspend_service(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    ) {
        self.effects.push(Effect::SuspendService {
            service_invocation_id,
            waiting_for_completed_entries,
        })
    }

    pub(crate) fn drop_journal_and_free_service(&mut self, service_id: ServiceId) {
        self.effects
            .push(Effect::DropJournalAndFreeService(service_id));
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

    pub(crate) fn set_state(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        key: Bytes,
        value: Bytes,
        raw_entry: RawEntry,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::SetState {
            service_invocation_id,
            key,
            value,
            raw_entry,
            entry_index,
        })
    }

    pub(crate) fn clear_state(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        key: Bytes,
        raw_entry: RawEntry,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::ClearState {
            service_invocation_id,
            key,
            raw_entry,
            entry_index,
        })
    }

    pub(crate) fn get_state_and_append_completed_entry(
        &mut self,
        key: Bytes,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) {
        self.effects.push(Effect::GetStateAndAppendCompletedEntry {
            key,
            service_invocation_id,
            entry_index,
            raw_entry,
        })
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

    pub(crate) fn append_journal_entry_and_ack_storage(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        raw_entry: RawEntry,
    ) {
        self.effects.push(Effect::AppendJournalEntryAndAck {
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
        });
    }

    pub(crate) fn store_and_forward_completion(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) {
        self.effects.push(Effect::StoreCompletionAndForward {
            service_invocation_id,
            completion,
        });
    }

    pub(crate) fn store_completion_and_resume(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) {
        self.effects.push(Effect::StoreCompletionAndResume {
            service_invocation_id,
            completion,
        });
    }

    pub(crate) fn drop_journal_and_pop_inbox(
        &mut self,
        service_id: ServiceId,
        inbox_sequence_number: u64,
    ) {
        self.effects.push(Effect::DropJournalAndPopInbox {
            service_id,
            inbox_sequence_number,
        });
    }
}
