// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use bytestring::ByteString;
use opentelemetry_api::trace::SpanId;
use restate_storage_api::inbox_table::InboxEntry;
use restate_storage_api::invocation_status_table::InvocationMetadata;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, JournalMetadata, NotificationTarget,
};
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_types::errors::InvocationErrorCode;
use restate_types::identifiers::{
    DeploymentId, EntryIndex, FullInvocationId, InvocationId, ServiceId,
};
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    InvocationResponse, ResponseResult, ServiceInvocation, ServiceInvocationSpanContext,
    SpanRelation,
};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::{Completion, CompletionResult};
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::timer::TimerKeyDisplay;
use restate_wal_protocol::timer::TimerValue;
use std::collections::HashSet;
use std::fmt;
use std::vec::Drain;
use tracing::{debug_span, event_enabled, span_enabled, trace, trace_span, Level};

#[derive(Debug)]
pub(crate) enum Effect {
    // service status changes
    InvokeService(ServiceInvocation),
    ResumeService {
        invocation_id: InvocationId,
        metadata: InvocationMetadata,
    },
    SuspendService {
        invocation_id: InvocationId,
        metadata: InvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },

    // In-/outbox
    EnqueueIntoInbox {
        seq_number: MessageIndex,
        inbox_entry: InboxEntry,
    },
    EnqueueIntoOutbox {
        seq_number: MessageIndex,
        message: OutboxMessage,
    },
    TruncateOutbox(MessageIndex),
    DropJournal {
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    },
    DropJournalAndPopInbox {
        full_invocation_id: FullInvocationId,
        journal_length: EntryIndex,
    },
    DeleteInboxEntry {
        service_id: ServiceId,
        sequence_number: MessageIndex,
    },

    // State
    SetState {
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        key: Bytes,
        value: Bytes,
    },
    ClearState {
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        key: Bytes,
    },
    ClearAllState {
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
    },

    // Timers
    RegisterTimer {
        timer_value: TimerValue,
        span_context: ServiceInvocationSpanContext,
    },
    DeleteTimer(TimerKey),

    // Journal operations
    StoreDeploymentId {
        invocation_id: InvocationId,
        deployment_id: DeploymentId,
        metadata: InvocationMetadata,
    },
    AppendJournalEntry {
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        previous_invocation_status: InvocationStatus,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    },
    StoreCompletion {
        invocation_id: InvocationId,
        completion: Completion,
    },
    ForwardCompletion {
        // TODO this can be invocation_id once the invoker uses only InvocationId
        full_invocation_id: FullInvocationId,
        completion: Completion,
    },

    // Virtual journal
    CreateVirtualJournal {
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        completion_notification_target: NotificationTarget,
        kill_notification_target: NotificationTarget,
    },
    NotifyVirtualJournalCompletion {
        target_service: ServiceId,
        method_name: String,
        // TODO perhaps we should rename this type JournalId
        invocation_id: InvocationId,
        completion: Completion,
    },
    NotifyVirtualJournalKill {
        target_service: ServiceId,
        method_name: String,
        // TODO perhaps we should rename this type JournalId
        invocation_id: InvocationId,
    },

    // Effects used only for tracing purposes
    TraceBackgroundInvoke {
        full_invocation_id: FullInvocationId,
        service_method: ByteString,
        span_context: ServiceInvocationSpanContext,
        pointer_span_id: Option<SpanId>,
    },
    TraceInvocationResult {
        full_invocation_id: FullInvocationId,
        creation_time: MillisSinceEpoch,
        service_method: ByteString,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (InvocationErrorCode, String)>,
    },

    // Invoker commands
    AbortInvocation(FullInvocationId),
    SendStoredEntryAckToInvoker(FullInvocationId, EntryIndex),

    // State mutations
    MutateState(ExternalStateMutation),

    IngressResponse(IngressResponse),
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

macro_rules! span_if_leader {
    ($level:expr, $i_am_leader:expr, $sampled:expr, $span_relation:expr, $($args:tt)*) => {{
        if $i_am_leader && $sampled {
            let span = ::tracing::span!($level, $($args)*);
            $span_relation
                .attach_to_span(&span);
            let _ = span.enter();
        }
    }};
}

macro_rules! info_span_if_leader {
    ($i_am_leader:expr, $sampled:expr, $span_relation:expr, $($args:tt)*) => {{
        use ::tracing::Level;
        span_if_leader!(Level::INFO, $i_am_leader, $sampled, $span_relation, $($args)*)
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
                        method,
                        journal_metadata: JournalMetadata { length, .. },
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
                invocation_id,
                metadata,
                waiting_for_completed_entries,
                ..
            } => {
                info_span_if_leader!(
                    is_leader,
                    metadata.journal_metadata.span_context.is_sampled(),
                    metadata.journal_metadata.span_context.as_parent(),
                    "suspend",
                    restate.journal.length = metadata.journal_metadata.length,
                    restate.invocation.id = %invocation_id,
                );
                debug_if_leader!(
                    is_leader,
                    rpc.method = %metadata.method,
                    restate.journal.length = metadata.journal_metadata.length,
                    "Effect: Suspend service waiting on entries {:?}",
                    waiting_for_completed_entries
                )
            }
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
                rpc.service = %service_invocation.fid.service_id.service_name,
                rpc.method = %service_invocation.method_name,
                restate.invocation.id = %service_invocation.fid,
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
                restate.invocation.id = %id,
                restate.outbox.seq = seq_number,
                "Effect: Send success response to another invocation, completing entry index {}",
                entry_index
            ),
            Effect::EnqueueIntoOutbox {
                seq_number,
                message: OutboxMessage::InvocationTermination(invocation_termination),
            } => debug_if_leader!(
                is_leader,
                restate.invocation.id = %invocation_termination.maybe_fid,
                restate.outbox.seq = seq_number,
                "Effect: Send invocation termination command '{:?}' to partition processor",
                invocation_termination.flavor
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
                restate.invocation.id = %id,
                restate.outbox.seq = seq_number,
                "Effect: Send failure code {} response to another invocation, completing entry index {}. Reason: {}",
                failure_code,
                entry_index,
                failure_msg
            ),
            Effect::IngressResponse(IngressResponse {
                response: ResponseResult::Success(_),
                full_invocation_id,
                ..
            }) => debug_if_leader!(
                is_leader,
                restate.invocation.id = %full_invocation_id,
                "Effect: Send response to ingress: Success"),
            Effect::IngressResponse(IngressResponse {
                response: ResponseResult::Failure(error_code, error_msg),
                full_invocation_id,
                ..
            }) => debug_if_leader!(
                is_leader,
                restate.invocation.id = %full_invocation_id,
                "Effect: Send response to ingress: Failure(code: {}, msg: {})",
                error_code,
                error_msg,
            ),
            Effect::DeleteInboxEntry {
                service_id,
                sequence_number,
            } => {
                debug_if_leader!(
                    is_leader,
                    rpc.service = %service_id.service_name,
                    restate.inbox.seq = sequence_number,
                    "Effect: Delete inbox entry",
                );
            }
            Effect::TruncateOutbox(seq_number) => {
                trace!(restate.outbox.seq = seq_number, "Effect: Truncate outbox")
            }
            Effect::DropJournal { journal_length, .. } => {
                debug_if_leader!(
                    is_leader,
                    restate.journal.length = journal_length,
                    "Effect: Drop journal"
                );
            }
            Effect::DropJournalAndPopInbox { journal_length, .. } => {
                debug_if_leader!(
                    is_leader,
                    restate.journal.length = journal_length,
                    "Effect: Drop journal and pop from inbox"
                );
            }
            Effect::SetState {
                service_id,
                invocation_id,
                span_context,
                key,
                ..
            } => {
                info_span_if_leader!(
                    is_leader,
                    span_context.is_sampled(),
                    span_context.as_parent(),
                    "set_state",
                    otel.name = format!("set_state {key:?}"),
                    restate.state.key = ?key,
                    rpc.service = %service_id.service_name,
                    restate.invocation.id = %invocation_id,
                );

                debug_if_leader!(
                    is_leader,
                    restate.state.key = ?key,
                    "Effect: Set state"
                )
            }
            Effect::ClearState {
                service_id,
                invocation_id,
                span_context,
                key,
            } => {
                info_span_if_leader!(
                    is_leader,
                    span_context.is_sampled(),
                    span_context.as_parent(),
                    "clear_state",
                    otel.name = format!("clear_state {key:?}"),
                    restate.state.key = ?key,
                    rpc.service = %service_id.service_name,
                    restate.invocation.id = %invocation_id,
                );

                debug_if_leader!(
                    is_leader,
                    restate.state.key = ?key,
                    "Effect: Clear state"
                )
            }
            Effect::ClearAllState {
                service_id,
                invocation_id,
                span_context,
            } => {
                info_span_if_leader!(
                    is_leader,
                    span_context.is_sampled(),
                    span_context.as_parent(),
                    "clear_all_state",
                    rpc.service = %service_id.service_name,
                    restate.invocation.id = %invocation_id,
                );

                debug_if_leader!(is_leader, "Effect: Clear all state")
            }
            Effect::RegisterTimer {
                timer_value,
                span_context,
                ..
            } => match timer_value.value() {
                Timer::CompleteSleepEntry(service_id) => {
                    info_span_if_leader!(
                        is_leader,
                        span_context.is_sampled(),
                        span_context.as_parent(),
                        "sleep",
                        rpc.service = %service_id.service_name,
                        restate.invocation.id = %timer_value.invocation_id(),
                        restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                        restate.timer.wake_up_time = %timer_value.wake_up_time(),
                        // without converting to i64 this field will encode as a string
                        // however, overflowing i64 seems unlikely
                        restate.internal.end_time = i64::try_from(timer_value.wake_up_time().as_u64()).expect("wake up time should fit into i64"),
                    );

                    debug_if_leader!(
                        is_leader,
                        restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                        restate.timer.wake_up_time = %timer_value.wake_up_time(),
                        "Effect: Register Sleep timer"
                    )
                }
                Timer::Invoke(_, service_invocation) => {
                    // no span necessary; there will already be a background_invoke span
                    debug_if_leader!(
                        is_leader,
                        rpc.service = %service_invocation.fid.service_id.service_name,
                        restate.invocation.id = %service_invocation.fid,
                        restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                        restate.timer.wake_up_time = %timer_value.wake_up_time(),
                        "Effect: Register background invoke timer"
                    )
                }
            },
            Effect::DeleteTimer(timer_key) => {
                let timer_key_display = TimerKeyDisplay(timer_key);
                debug_if_leader!(
                    is_leader,
                    restate.timer.key = %timer_key_display,
                    "Effect: Delete timer"
                )
            }
            Effect::StoreDeploymentId { deployment_id, .. } => debug_if_leader!(
                is_leader,
                restate.deployment.id = %deployment_id,
                "Effect: Store deployment id to storage"
            ),
            Effect::AppendJournalEntry {
                journal_entry,
                entry_index,
                invocation_id,
                ..
            } => debug_if_leader!(
                is_leader,
                restate.journal.index = entry_index,
                restate.invocation.id = %invocation_id,
                "Effect: Write journal entry {:?} to storage",
                journal_entry.header().as_entry_type()
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
                "Effect: Forward completion {} to deployment",
                CompletionResultFmt(result)
            ),
            Effect::CreateVirtualJournal { invocation_id, .. } => {
                debug_if_leader!(
                    is_leader,
                    restate.invocation.id = %invocation_id,
                    "Effect: Create virtual journal"
                )
            }
            Effect::NotifyVirtualJournalCompletion {
                target_service,
                method_name,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
                ..
            } => {
                debug_if_leader!(
                    is_leader,
                    restate.journal.index = entry_index,
                    "Effect: Notify virtual journal completion {} to service {:?} method {}",
                    CompletionResultFmt(result),
                    target_service,
                    method_name
                )
            }
            Effect::NotifyVirtualJournalKill {
                target_service,
                method_name,
                ..
            } => {
                debug_if_leader!(
                    is_leader,
                    "Effect: Notify virtual journal kill to service {:?} method {}",
                    target_service,
                    method_name
                )
            }
            Effect::TraceBackgroundInvoke {
                full_invocation_id,
                service_method,
                span_context,
                pointer_span_id,
            } => {
                // create an instantaneous 'pointer span' which lives in the calling trace at the
                // time of background call, and primarily exists to be linked to by the new trace
                // that will be created for the background invocation, but even if that trace wasn't
                // sampled for some reason, it's still worth producing this span

                if let Some(pointer_span_id) = pointer_span_id {
                    info_span_if_leader!(
                        is_leader,
                        span_context.is_sampled(),
                        span_context.as_parent(),
                        "background_invoke",
                        otel.name = format!("background_invoke {service_method}"),
                        rpc.service = %full_invocation_id.service_id.service_name,
                        rpc.method = %service_method,
                        restate.invocation.id = %full_invocation_id,
                        restate.internal.span_id = %pointer_span_id,
                    );
                } else {
                    info_span_if_leader!(
                        is_leader,
                        span_context.is_sampled(),
                        span_context.as_parent(),
                        "background_invoke",
                        otel.name = format!("background_invoke {service_method}"),
                        rpc.service = %full_invocation_id.service_id.service_name,
                        rpc.method = %service_method,
                        restate.invocation.id = %full_invocation_id,
                    );
                }
            }
            Effect::TraceInvocationResult {
                full_invocation_id,
                creation_time,
                service_method,
                span_context,
                result,
            } => {
                let (result, error) = match result {
                    Ok(_) => ("Success", false),
                    Err(_) => ("Failure", true),
                };

                info_span_if_leader!(
                    is_leader,
                    span_context.is_sampled(),
                    span_context.causing_span_relation(),
                    "invoke",
                    otel.name = format!("invoke {service_method}"),
                    rpc.service = %full_invocation_id.service_id.service_name,
                    rpc.method = %service_method,
                    restate.invocation.id = %full_invocation_id,
                    restate.invocation.result = result,
                    error = error, // jaeger uses this tag to show an error icon
                    // without converting to i64 this field will encode as a string
                    // however, overflowing i64 seems unlikely
                    restate.internal.start_time = i64::try_from(creation_time.as_u64()).expect("creation time should fit into i64"),
                    restate.internal.span_id = %span_context.span_context().span_id(),
                    restate.internal.trace_id = %span_context.span_context().trace_id()
                );
                // No need to log this
            }
            Effect::AbortInvocation(_) => {
                debug_if_leader!(is_leader, "Effect: Abort unknown invocation");
            }
            Effect::SendStoredEntryAckToInvoker(_, _) => {
                // We can ignore these
            }
            Effect::MutateState(state_mutation) => {
                debug_if_leader!(
                    is_leader,
                    "Effect: Mutate state for service id '{:?}'",
                    &state_mutation.service_id
                );
            }
        }
    }
}

// To write completions in the effects log
struct CompletionResultFmt<'a>(&'a CompletionResult);

impl<'a> fmt::Display for CompletionResultFmt<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            CompletionResult::Empty => write!(f, "Empty"),
            CompletionResult::Success(_) => write!(f, "Success"),
            CompletionResult::Failure(code, reason) => write!(f, "Failure({}, {})", code, reason),
        }
    }
}

#[derive(Debug, Default)]
pub struct Effects {
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

    pub(crate) fn resume_service(
        &mut self,
        invocation_id: InvocationId,
        metadata: InvocationMetadata,
    ) {
        self.effects.push(Effect::ResumeService {
            invocation_id,
            metadata,
        });
    }

    pub(crate) fn suspend_service(
        &mut self,
        invocation_id: InvocationId,
        metadata: InvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    ) {
        self.effects.push(Effect::SuspendService {
            invocation_id,
            metadata,
            waiting_for_completed_entries,
        })
    }

    pub(crate) fn enqueue_into_inbox(&mut self, seq_number: MessageIndex, inbox_entry: InboxEntry) {
        self.effects.push(Effect::EnqueueIntoInbox {
            seq_number,
            inbox_entry,
        })
    }

    pub(crate) fn delete_inbox_entry(
        &mut self,
        service_id: ServiceId,
        sequence_number: MessageIndex,
    ) {
        self.effects.push(Effect::DeleteInboxEntry {
            service_id,
            sequence_number,
        });
    }

    pub(crate) fn enqueue_into_outbox(&mut self, seq_number: MessageIndex, message: OutboxMessage) {
        self.effects.push(Effect::EnqueueIntoOutbox {
            seq_number,
            message,
        })
    }

    pub(crate) fn send_ingress_response(&mut self, ingress_response: IngressResponse) {
        self.effects.push(Effect::IngressResponse(ingress_response));
    }

    pub(crate) fn set_state(
        &mut self,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        key: Bytes,
        value: Bytes,
    ) {
        self.effects.push(Effect::SetState {
            service_id,
            invocation_id,
            span_context,
            key,
            value,
        })
    }

    pub(crate) fn clear_state(
        &mut self,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        key: Bytes,
    ) {
        self.effects.push(Effect::ClearState {
            service_id,
            invocation_id,
            span_context,
            key,
        })
    }

    pub(crate) fn clear_all_state(
        &mut self,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
    ) {
        self.effects.push(Effect::ClearAllState {
            service_id,
            invocation_id,
            span_context,
        })
    }

    pub(crate) fn register_timer(
        &mut self,
        timer_value: TimerValue,
        span_context: ServiceInvocationSpanContext,
    ) {
        self.effects.push(Effect::RegisterTimer {
            timer_value,
            span_context,
        })
    }

    pub(crate) fn delete_timer(&mut self, timer_key: TimerKey) {
        self.effects.push(Effect::DeleteTimer(timer_key));
    }

    pub(crate) fn store_chosen_deployment(
        &mut self,
        invocation_id: InvocationId,
        deployment_id: DeploymentId,
        metadata: InvocationMetadata,
    ) {
        self.effects.push(Effect::StoreDeploymentId {
            invocation_id,
            deployment_id,
            metadata,
        })
    }

    pub(crate) fn append_journal_entry(
        &mut self,
        invocation_id: InvocationId,
        previous_invocation_status: InvocationStatus,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) {
        self.effects.push(Effect::AppendJournalEntry {
            invocation_id,
            previous_invocation_status,
            entry_index,
            journal_entry,
        })
    }

    pub(crate) fn truncate_outbox(&mut self, outbox_sequence_number: MessageIndex) {
        self.effects
            .push(Effect::TruncateOutbox(outbox_sequence_number));
    }

    pub(crate) fn store_completion(&mut self, invocation_id: InvocationId, completion: Completion) {
        self.effects.push(Effect::StoreCompletion {
            invocation_id,
            completion,
        });
    }

    pub(crate) fn forward_completion(
        &mut self,
        full_invocation_id: FullInvocationId,
        completion: Completion,
    ) {
        self.effects.push(Effect::ForwardCompletion {
            full_invocation_id,
            completion,
        });
    }

    pub(crate) fn create_virtual_journal(
        &mut self,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        completion_notification_target: NotificationTarget,
        kill_notification_target: NotificationTarget,
    ) {
        self.effects.push(Effect::CreateVirtualJournal {
            invocation_id,
            span_context,
            completion_notification_target,
            kill_notification_target,
        })
    }

    pub(crate) fn notify_virtual_journal_completion(
        &mut self,
        target_service: ServiceId,
        method_name: String,
        invocation_id: InvocationId,
        completion: Completion,
    ) {
        self.effects.push(Effect::NotifyVirtualJournalCompletion {
            target_service,
            method_name,
            invocation_id,
            completion,
        });
    }

    pub(crate) fn notify_virtual_journal_kill(
        &mut self,
        target_service: ServiceId,
        method_name: String,
        invocation_id: InvocationId,
    ) {
        self.effects.push(Effect::NotifyVirtualJournalKill {
            target_service,
            method_name,
            invocation_id,
        });
    }

    pub(crate) fn drop_journal(&mut self, invocation_id: InvocationId, journal_length: EntryIndex) {
        self.effects.push(Effect::DropJournal {
            invocation_id,
            journal_length,
        });
    }

    pub(crate) fn drop_journal_and_pop_inbox(
        &mut self,
        full_invocation_id: FullInvocationId,
        journal_length: EntryIndex,
    ) {
        self.effects.push(Effect::DropJournalAndPopInbox {
            full_invocation_id,
            journal_length,
        });
    }

    pub(crate) fn trace_background_invoke(
        &mut self,
        full_invocation_id: FullInvocationId,
        service_method: ByteString,
        span_context: ServiceInvocationSpanContext,
        pointer_span_id: Option<SpanId>,
    ) {
        self.effects.push(Effect::TraceBackgroundInvoke {
            full_invocation_id,
            service_method,
            span_context,
            pointer_span_id,
        })
    }

    pub(crate) fn trace_invocation_result(
        &mut self,
        full_invocation_id: FullInvocationId,
        service_method: ByteString,
        span_context: ServiceInvocationSpanContext,
        creation_time: MillisSinceEpoch,
        result: Result<(), (InvocationErrorCode, String)>,
    ) {
        self.effects.push(Effect::TraceInvocationResult {
            full_invocation_id,
            creation_time,
            service_method,
            span_context,
            result,
        })
    }

    pub(crate) fn abort_invocation(&mut self, full_invocation_id: FullInvocationId) {
        self.effects
            .push(Effect::AbortInvocation(full_invocation_id));
    }

    pub(crate) fn send_stored_ack_to_invoker(
        &mut self,
        full_invocation_id: FullInvocationId,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::SendStoredEntryAckToInvoker(
            full_invocation_id,
            entry_index,
        ));
    }

    pub(crate) fn apply_state_mutation(&mut self, state_mutation: ExternalStateMutation) {
        self.effects.push(Effect::MutateState(state_mutation));
    }

    /// We log only if the log level is TRACE, or if the log level is DEBUG and we're the leader,
    /// or if the span level is INFO and we're the leader.
    pub(crate) fn log(
        &self,
        is_leader: bool,
        full_invocation_id: Option<FullInvocationId>,
        related_span: SpanRelation,
    ) {
        // Skip this method altogether if logging is disabled
        if !(((event_enabled!(Level::DEBUG) || span_enabled!(Level::INFO)) && is_leader)
            || event_enabled!(Level::TRACE))
        {
            return;
        }

        let span = match (is_leader, full_invocation_id) {
            (true, Some(full_invocation_id)) => debug_span!(
                "state_machine_effects",
                rpc.service = %full_invocation_id.service_id.service_name,
                restate.invocation.id = %full_invocation_id,
            ),
            (false, Some(full_invocation_id)) => trace_span!(
                "state_machine_effects",
                rpc.service = %full_invocation_id.service_id.service_name,
                restate.invocation.id = %full_invocation_id,
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

#[cfg(test)]
mod tests {
    use super::Effect;
    use super::Effects;

    impl Effects {
        pub(crate) fn into_inner(self) -> Vec<Effect> {
            self.effects
        }
    }
}
