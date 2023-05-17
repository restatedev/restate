use bytes::Bytes;
use bytestring::ByteString;
use restate_common::types::{
    CompletionResult, EnrichedRawEntry, EntryIndex, InvocationId, InvocationMetadata,
    InvocationResponse, JournalMetadata, MessageIndex, MillisSinceEpoch, OutboxMessage,
    ResponseResult, ServiceId, ServiceInvocation, ServiceInvocationId,
    ServiceInvocationSpanContext, SpanRelation, Timer, TimerSeqNumber,
};
use restate_journal::raw::Header;
use restate_journal::Completion;
use std::collections::HashSet;
use std::fmt;
use std::vec::Drain;
use tracing::{debug_span, event_enabled, trace, trace_span, Level};
use types::TimerKeyDisplay;

mod interpreter;

use crate::partition::{types, AckResponse, TimerValue};
pub(crate) use interpreter::{
    ActuatorMessage, CommitError, Committable, Interpreter, MessageCollector, StateStorage,
    StateStorageError,
};

#[derive(Debug)]
pub(crate) enum Effect {
    // service status changes
    InvokeService(ServiceInvocation),
    ResumeService {
        service_id: ServiceId,
        metadata: InvocationMetadata,
    },
    SuspendService {
        service_id: ServiceId,
        metadata: InvocationMetadata,
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
        service_id: ServiceId,
        metadata: InvocationMetadata,
        key: Bytes,
        value: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    },
    ClearState {
        service_id: ServiceId,
        metadata: InvocationMetadata,
        key: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    },
    GetStateAndAppendCompletedEntry {
        service_id: ServiceId,
        metadata: InvocationMetadata,
        key: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    },

    // Timers
    RegisterTimer {
        seq_number: TimerSeqNumber,
        timer_value: TimerValue,
    },
    DeleteTimer {
        service_invocation_id: ServiceInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    },

    // Journal operations
    AppendJournalEntry {
        service_id: ServiceId,
        metadata: InvocationMetadata,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    },
    AppendAwakeableEntry {
        service_id: ServiceId,
        metadata: InvocationMetadata,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    },
    AppendJournalEntryAndAck {
        service_id: ServiceId,
        metadata: InvocationMetadata,
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
        service_id: ServiceId,
        metadata: InvocationMetadata,
        completion: Completion,
    },
    ForwardCompletion {
        completion: Completion,
        service_invocation_id: ServiceInvocationId,
    },

    // Tracing
    NotifyInvocationResult {
        service_name: ByteString,
        service_method: String,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (i32, String)>,
    },

    // Acks
    SendAckResponse(AckResponse),

    // Invoker commands
    AbortInvocation(ServiceInvocationId),
}

macro_rules! debug_if_leader {
    ($i_am_leader:expr, $($args:tt)*) => {{
        use ::tracing::Level;

        if $i_am_leader {
            ::tracing::event!(Level::DEBUG, $($args)*)
        } else {
            ::tracing::event!(Level::TRACE, $($args)*)
        }
    }};
}

impl Effect {
    fn log(&self, is_leader: bool) {
        match self {
            Effect::InvokeService(ServiceInvocation { method_name, .. }) => debug_if_leader!(
                is_leader,
                rpc.method = %method_name,
                "Effect: Invoke service"
            ),
            Effect::ResumeService {
                metadata:
                    InvocationMetadata {
                        journal_metadata: JournalMetadata { method, length, .. },
                        ..
                    },
                ..
            } => debug_if_leader!(
                is_leader,
                rpc.method = %method,
                restate.journal.length = length,
                "Effect: Resume service"
            ),
            Effect::SuspendService {
                metadata:
                    InvocationMetadata {
                        journal_metadata: JournalMetadata { method, length, .. },
                        ..
                    },
                waiting_for_completed_entries,
                ..
            } => debug_if_leader!(
                is_leader,
                rpc.method = %method,
                restate.journal.length = length,
                "Effect: Suspend service waiting on entries {:?}",
                waiting_for_completed_entries
            ),
            Effect::DropJournalAndFreeService { journal_length, .. } => debug_if_leader!(
                is_leader,
                restate.journal.length = journal_length,
                "Effect: Release service instance lock"
            ),
            Effect::EnqueueIntoInbox { seq_number, .. } => debug_if_leader!(
                is_leader,
                restate.inbox.seq = seq_number,
                "Effect: Enqueue invocation in inbox"
            ),
            Effect::EnqueueIntoOutbox {
                seq_number,
                message: OutboxMessage::ServiceInvocation(service_invocation),
            } => debug_if_leader!(
                is_leader,
                rpc.service = %service_invocation.id.service_id.service_name,
                rpc.method = %service_invocation.method_name,
                restate.invocation.key = ?service_invocation.id.service_id.key,
                restate.invocation.id = %service_invocation.id.invocation_id,
                restate.outbox.seq = seq_number,
                "Effect: Send service invocation to partition processor"
            ),
            Effect::EnqueueIntoOutbox {
                seq_number,
                message:
                    OutboxMessage::ServiceResponse(InvocationResponse {
                        result: ResponseResult::Success(_),
                        entry_index,
                        id,
                    }),
            } => debug_if_leader!(
                is_leader,
                rpc.service = %id.service_id.service_name,
                restate.invocation.key = ?id.service_id.key,
                restate.invocation.id = %id.invocation_id,
                restate.outbox.seq = seq_number,
                "Effect: Send success response to another invocation, completing entry index {}",
                entry_index
            ),
            Effect::EnqueueIntoOutbox {
                seq_number,
                message:
                    OutboxMessage::ServiceResponse(InvocationResponse {
                        result: ResponseResult::Failure(failure_code, failure_msg),
                        entry_index,
                        id,
                    }),
            } => debug_if_leader!(
                is_leader,
                rpc.service = %id.service_id.service_name,
                restate.invocation.key = ?id.service_id.key,
                restate.invocation.id = %id.invocation_id,
                restate.outbox.seq = seq_number,
                "Effect: Send failure code {} response to another invocation, completing entry index {}. Reason: {}",
                failure_code,
                entry_index,
                failure_msg
            ),
            Effect::EnqueueIntoOutbox {
                seq_number,
                message:
                    OutboxMessage::IngressResponse {
                        response: ResponseResult::Success(_),
                        service_invocation_id,
                        ..
                    },
            } => debug_if_leader!(
                is_leader,
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.outbox.seq = seq_number,
                "Effect: Send success response to ingress"
            ),
            Effect::EnqueueIntoOutbox {
                seq_number,
                message:
                    OutboxMessage::IngressResponse {
                        response: ResponseResult::Failure(failure_code, failure_msg),
                        service_invocation_id,
                        ..
                    },
            } => debug_if_leader!(
                is_leader,
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id,
                restate.outbox.seq = seq_number,
                "Effect: Send failure code {} response to ingress. Reason: {}",
                failure_code,
                failure_msg
            ),
            Effect::TruncateOutbox(seq_number) => {
                trace!(restate.outbox.seq = seq_number, "Effect: Truncate outbox")
            }
            Effect::DropJournalAndPopInbox {
                journal_length,
                inbox_sequence_number,
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.length = journal_length,
                restate.inbox.seq = inbox_sequence_number,
                "Effect: Execute next enqueued invocation"
            ),
            Effect::SetState {
                key, entry_index, ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                restate.state.key = ?key,
                "Effect: Set state"
            ),
            Effect::ClearState {
                key, entry_index, ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                restate.state.key = ?key,
                "Effect: Clear state"
            ),
            Effect::GetStateAndAppendCompletedEntry {
                key, entry_index, ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                restate.state.key = ?key,
                "Effect: Get state"
            ),
            Effect::RegisterTimer { timer_value, .. } => match &timer_value.value {
                Timer::CompleteSleepEntry => debug_if_leader!(
                    is_leader,
                    restate.timer.key = %timer_value.display_key(),
                    restate.timer.wake_up_time = %timer_value.wake_up_time,
                    "Effect: Register Sleep timer"
                ),
                Timer::Invoke(service_invocation) => debug_if_leader!(
                    is_leader,
                    rpc.service = %service_invocation.id.service_id.service_name,
                    restate.invocation.key = ?service_invocation.id.service_id.key,
                    restate.invocation.id = %service_invocation.id.invocation_id,
                    restate.timer.key = %timer_value.display_key(),
                    restate.timer.wake_up_time = %timer_value.wake_up_time,
                    "Effect: Register background invoke timer"
                ),
            },
            Effect::DeleteTimer {
                service_invocation_id,
                entry_index,
                wake_up_time,
            } => debug_if_leader!(
                is_leader,
                restate.timer.key = %TimerKeyDisplay(service_invocation_id, entry_index),
                restate.timer.wake_up_time = %wake_up_time,
                "Effect: Delete timer"
            ),
            Effect::AppendJournalEntry {
                journal_entry,
                entry_index,
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                "Effect: Write journal entry {:?} to storage",
                journal_entry.header.to_entry_type()
            ),
            Effect::AppendAwakeableEntry {
                journal_entry,
                entry_index,
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                "Effect: Write journal entry {:?} to storage",
                journal_entry.header.to_entry_type()
            ),
            Effect::AppendJournalEntryAndAck {
                journal_entry,
                entry_index,
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                "Effect: Write journal entry {:?} to storage and ack back",
                journal_entry.header.to_entry_type()
            ),
            Effect::StoreCompletion {
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                "Effect: Store completion {}",
                CompletionResultFmt(result)
            ),
            Effect::StoreCompletionAndForward {
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                "Effect: Store completion {} and forward to service endpoint",
                CompletionResultFmt(result)
            ),
            Effect::StoreCompletionAndResume {
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                "Effect: Store completion {} and resume invocation",
                CompletionResultFmt(result)
            ),
            Effect::ForwardCompletion {
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                "Effect: Forward completion {} to service endpoint",
                CompletionResultFmt(result)
            ),
            Effect::NotifyInvocationResult { .. } => {
                // No need to log this
            }
            Effect::SendAckResponse(ack_response) => {
                if is_leader {
                    trace!("Effect: Sending ack response: {ack_response:?}");
                }
            }
            Effect::AbortInvocation(_) => {
                debug_if_leader!(is_leader, "Effect: Abort unknown invocation");
            }
        }
    }
}

// To write completions in the effects log
struct CompletionResultFmt<'a>(&'a CompletionResult);

impl<'a> fmt::Display for CompletionResultFmt<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            CompletionResult::Ack => write!(f, "Ack"),
            CompletionResult::Empty => write!(f, "Empty"),
            CompletionResult::Success(_) => write!(f, "Success"),
            CompletionResult::Failure(code, reason) => write!(f, "Failure({}, {})", code, reason),
        }
    }
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

    pub(crate) fn resume_service(&mut self, service_id: ServiceId, metadata: InvocationMetadata) {
        self.effects.push(Effect::ResumeService {
            service_id,
            metadata,
        });
    }

    pub(crate) fn suspend_service(
        &mut self,
        service_id: ServiceId,
        metadata: InvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    ) {
        self.effects.push(Effect::SuspendService {
            service_id,
            metadata,
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
        service_id: ServiceId,
        metadata: InvocationMetadata,
        key: Bytes,
        value: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::SetState {
            service_id,
            metadata,
            key,
            value,
            journal_entry,
            entry_index,
        })
    }

    pub(crate) fn clear_state(
        &mut self,
        service_id: ServiceId,
        metadata: InvocationMetadata,
        key: Bytes,
        journal_entry: EnrichedRawEntry,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::ClearState {
            service_id,
            metadata,
            key,
            journal_entry,
            entry_index,
        })
    }

    pub(crate) fn get_state_and_append_completed_entry(
        &mut self,
        service_id: ServiceId,
        metadata: InvocationMetadata,
        key: Bytes,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::GetStateAndAppendCompletedEntry {
            service_id,
            metadata,
            key,
            entry_index,
            journal_entry,
        })
    }

    pub(crate) fn register_timer(
        &mut self,
        timer_seq_number: TimerSeqNumber,
        timer_value: TimerValue,
    ) {
        self.effects.push(Effect::RegisterTimer {
            seq_number: timer_seq_number,
            timer_value,
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
        service_id: ServiceId,
        metadata: InvocationMetadata,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::AppendJournalEntry {
            service_id,
            metadata,
            entry_index,
            journal_entry,
        })
    }

    pub(crate) fn append_awakeable_entry(
        &mut self,
        service_id: ServiceId,
        metadata: InvocationMetadata,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::AppendAwakeableEntry {
            service_id,
            metadata,
            entry_index,
            journal_entry,
        })
    }

    pub(crate) fn append_journal_entry_and_ack_storage(
        &mut self,
        service_id: ServiceId,
        metadata: InvocationMetadata,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::AppendJournalEntryAndAck {
            service_id,
            metadata,
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
        service_id: ServiceId,
        metadata: InvocationMetadata,
        completion: Completion,
    ) {
        self.effects.push(Effect::StoreCompletionAndResume {
            service_id,
            metadata,
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
        service_name: ByteString,
        service_method: String,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (i32, String)>,
    ) {
        self.effects.push(Effect::NotifyInvocationResult {
            service_name,
            service_method,
            invocation_id,
            span_context,
            result,
        })
    }

    pub(crate) fn send_ack_response(&mut self, ack_response: AckResponse) {
        self.effects.push(Effect::SendAckResponse(ack_response));
    }

    pub(crate) fn abort_invocation(&mut self, service_invocation_id: ServiceInvocationId) {
        self.effects
            .push(Effect::AbortInvocation(service_invocation_id));
    }

    /// We log only if the level is TRACE, or if the level is DEBUG and we're the leader.
    pub(crate) fn log(
        &self,
        is_leader: bool,
        service_invocation_id: Option<ServiceInvocationId>,
        related_span: SpanRelation,
    ) {
        // Skip this method altogether if logging is disabled
        if !((event_enabled!(Level::DEBUG) && is_leader) || event_enabled!(Level::TRACE)) {
            return;
        }

        let span = match (is_leader, service_invocation_id) {
            (true, Some(service_invocation_id)) => debug_span!(
                "state_machine_effects",
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id
            ),
            (false, Some(service_invocation_id)) => trace_span!(
                "state_machine_effects",
                rpc.service = %service_invocation_id.service_id.service_name,
                restate.invocation.key = ?service_invocation_id.service_id.key,
                restate.invocation.id = %service_invocation_id.invocation_id
            ),
            (true, None) => debug_span!("state_machine_effects"),
            (false, None) => trace_span!("state_machine_effects"),
        };

        // Create span and enter it
        related_span.attach_to_span(&span);
        let _enter = span.enter();

        // Log all the effects
        for effect in self.effects.iter() {
            effect.log(is_leader);
        }
    }
}
