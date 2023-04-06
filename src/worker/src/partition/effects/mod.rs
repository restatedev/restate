use bytes::Bytes;
use common::types::{
    EnrichedRawEntry, EntryIndex, InvocationId, JournalMetadata, MessageIndex, MillisSinceEpoch,
    OutboxMessage, ServiceId, ServiceInvocation, ServiceInvocationId,
    ServiceInvocationResponseSink, ServiceInvocationSpanContext,
};
use journal::Completion;
use std::collections::HashSet;
use std::vec::Drain;

mod interpreter;

pub(crate) use interpreter::{
    ActuatorMessage, CommitError, Committable, Interpreter, MessageCollector, StateStorage,
    StateStorageError,
};

#[derive(Debug)]
pub(crate) struct JournalInformation {
    pub service_invocation_id: ServiceInvocationId,
    pub journal_metadata: JournalMetadata,
    pub response_sink: Option<ServiceInvocationResponseSink>,
}

impl JournalInformation {
    pub(crate) fn new(
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
    ) -> Self {
        Self {
            service_invocation_id,
            journal_metadata,
            response_sink,
        }
    }
}

#[derive(Debug)]
pub(crate) enum Effect {
    // service status changes
    InvokeService(ServiceInvocation),
    ResumeService {
        journal_information: JournalInformation,
    },
    SuspendService {
        journal_information: JournalInformation,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    DropJournalAndFreeService {
        service_id: ServiceId,
        journal_length: EntryIndex,
    },

    // In-/outbox
    EnqueueIntoInbox {
        seq_number: MessageIndex,
        service_invocation: ServiceInvocation,
    },
    EnqueueIntoOutbox {
        seq_number: MessageIndex,
        message: OutboxMessage,
    },
    TruncateOutbox(MessageIndex),
    DropJournalAndPopInbox {
        service_id: ServiceId,
        inbox_sequence_number: MessageIndex,
        journal_length: EntryIndex,
    },

    // State
    SetState {
        journal_information: JournalInformation,
        key: Bytes,
        value: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    },
    ClearState {
        journal_information: JournalInformation,
        key: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    },
    GetStateAndAppendCompletedEntry {
        journal_information: JournalInformation,
        key: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    },

    // Timers
    RegisterTimer {
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    },
    DeleteTimer {
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    },

    // Journal operations
    AppendJournalEntry {
        journal_information: JournalInformation,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    },
    AppendAwakeableEntry {
        journal_information: JournalInformation,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    },
    AppendJournalEntryAndAck {
        journal_information: JournalInformation,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
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
    ForwardCompletion {
        completion: Completion,
        service_invocation_id: ServiceInvocationId,
    },

    // Tracing
    NotifyInvocationResult {
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (i32, String)>,
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

    pub(crate) fn resume_service(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
    ) {
        self.effects.push(Effect::ResumeService {
            journal_information: JournalInformation::new(
                service_invocation_id,
                journal_metadata,
                response_sink,
            ),
        });
    }

    pub(crate) fn suspend_service(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    ) {
        self.effects.push(Effect::SuspendService {
            journal_information: JournalInformation::new(
                service_invocation_id,
                journal_metadata,
                response_sink,
            ),
            waiting_for_completed_entries,
        })
    }

    pub(crate) fn drop_journal_and_free_service(
        &mut self,
        service_id: ServiceId,
        journal_length: EntryIndex,
    ) {
        self.effects.push(Effect::DropJournalAndFreeService {
            service_id,
            journal_length,
        });
    }

    pub(crate) fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        service_invocation: ServiceInvocation,
    ) {
        self.effects.push(Effect::EnqueueIntoInbox {
            seq_number,
            service_invocation,
        })
    }

    pub(crate) fn enqueue_into_outbox(&mut self, seq_number: MessageIndex, message: OutboxMessage) {
        self.effects.push(Effect::EnqueueIntoOutbox {
            seq_number,
            message,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn set_state(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
        key: Bytes,
        value: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::SetState {
            journal_information: JournalInformation::new(
                service_invocation_id,
                journal_metadata,
                response_sink,
            ),
            key,
            value,
            journal_entry,
            entry_index,
        })
    }

    pub(crate) fn clear_state(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
        key: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::ClearState {
            journal_information: JournalInformation::new(
                service_invocation_id,
                journal_metadata,
                response_sink,
            ),
            key,
            journal_entry,
            entry_index,
        })
    }

    pub(crate) fn get_state_and_append_completed_entry(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
        key: Bytes,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::GetStateAndAppendCompletedEntry {
            key,
            journal_information: JournalInformation::new(
                service_invocation_id,
                journal_metadata,
                response_sink,
            ),
            entry_index,
            journal_entry,
        })
    }

    pub(crate) fn register_timer(
        &mut self,
        wake_up_time: MillisSinceEpoch,
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
        wake_up_time: MillisSinceEpoch,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::DeleteTimer {
            service_invocation_id,
            wake_up_time,
            entry_index,
        });
    }

    pub(crate) fn append_journal_entry(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::AppendJournalEntry {
            journal_information: JournalInformation::new(
                service_invocation_id,
                journal_metadata,
                response_sink,
            ),
            entry_index,
            journal_entry,
        })
    }

    pub(crate) fn append_awakeable_entry(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::AppendAwakeableEntry {
            journal_information: JournalInformation::new(
                service_invocation_id,
                journal_metadata,
                response_sink,
            ),
            entry_index,
            journal_entry,
        })
    }

    pub(crate) fn append_journal_entry_and_ack_storage(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        journal_metadata: JournalMetadata,
        response_sink: Option<ServiceInvocationResponseSink>,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::AppendJournalEntryAndAck {
            journal_information: JournalInformation::new(
                service_invocation_id,
                journal_metadata,
                response_sink,
            ),
            entry_index,
            journal_entry,
        })
    }

    pub(crate) fn truncate_outbox(&mut self, outbox_sequence_number: MessageIndex) {
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

    pub(crate) fn forward_completion(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
    ) {
        self.effects.push(Effect::ForwardCompletion {
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
        inbox_sequence_number: MessageIndex,
        journal_length: EntryIndex,
    ) {
        self.effects.push(Effect::DropJournalAndPopInbox {
            service_id,
            inbox_sequence_number,
            journal_length,
        });
    }

    pub(crate) fn notify_invocation_result(
        &mut self,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (i32, String)>,
    ) {
        self.effects.push(Effect::NotifyInvocationResult {
            invocation_id,
            span_context,
            result,
        })
    }
}
