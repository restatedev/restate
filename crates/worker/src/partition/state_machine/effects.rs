// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::types::InvocationIdAndTarget;
use bytes::Bytes;
use opentelemetry::trace::SpanId;
use restate_storage_api::inbox_table::InboxEntry;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation,
};
use restate_storage_api::invocation_status_table::{InvocationStatus, JournalMetadata};
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_types::errors::InvocationErrorCode;
use restate_types::identifiers::{
    DeploymentId, EntryIndex, IdempotencyId, InvocationId, ServiceId,
};
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    InvocationResponse, InvocationTarget, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, SpanRelation,
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
use std::time::Duration;
use std::vec::Drain;
use tracing::{debug_span, event_enabled, span_enabled, trace, trace_span, Level};

#[derive(Debug)]
pub(crate) enum Effect {
    // InvocationStatus changes
    InvokeService(ServiceInvocation),
    ResumeService {
        invocation_id: InvocationId,
        metadata: InFlightInvocationMetadata,
    },
    SuspendService {
        invocation_id: InvocationId,
        metadata: InFlightInvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    },
    StoreCompletedInvocation {
        invocation_id: InvocationId,
        retention: Duration,
        completed_invocation: CompletedInvocation,
    },
    StoreInboxedInvocation(InvocationId, InboxedInvocation),
    FreeInvocation(InvocationId),

    // In-/outbox
    EnqueueIntoInbox {
        seq_number: MessageIndex,
        inbox_entry: InboxEntry,
    },
    PopInbox(ServiceId),
    EnqueueIntoOutbox {
        seq_number: MessageIndex,
        message: OutboxMessage,
    },
    TruncateOutbox(MessageIndex),
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
        metadata: InFlightInvocationMetadata,
    },
    AppendResponseSink {
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        previous_invocation_status: InvocationStatus,
        additional_response_sink: ServiceInvocationResponseSink,
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
        invocation_id: InvocationId,
        completion: Completion,
    },
    DropJournal {
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    },

    // Effects used only for tracing purposes
    TraceBackgroundInvoke {
        invocation_id_and_target: InvocationIdAndTarget,
        span_context: ServiceInvocationSpanContext,
        pointer_span_id: Option<SpanId>,
    },
    TraceInvocationResult {
        invocation_id_and_target: InvocationIdAndTarget,
        creation_time: MillisSinceEpoch,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (InvocationErrorCode, String)>,
    },

    // Invoker commands
    SendAbortInvocationToInvoker(InvocationId),
    SendStoredEntryAckToInvoker(InvocationId, EntryIndex),

    // State mutations
    MutateState(ExternalStateMutation),

    // Idempotency
    StoreIdempotencyId(IdempotencyId, InvocationId),
    DeleteIdempotencyId(IdempotencyId),

    // Send ingress response
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
            Effect::InvokeService(ServiceInvocation { .. }) => {
                debug_if_leader!(is_leader, "Effect: Invoke service")
            }
            Effect::ResumeService {
                metadata:
                    InFlightInvocationMetadata {
                        journal_metadata: JournalMetadata { length, .. },
                        ..
                    },
                ..
            } => debug_if_leader!(
                is_leader,
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
                    restate.journal.length = metadata.journal_metadata.length,
                    "Effect: Suspend service waiting on entries {:?}",
                    waiting_for_completed_entries
                )
            }
            Effect::StoreInboxedInvocation(id, inboxed_invocation) => {
                debug_if_leader!(
                    is_leader,
                    restate.invocation.id = %id,
                    restate.outbox.seq = inboxed_invocation.inbox_sequence_number,
                    "Effect: Store inboxed invocation"
                )
            }
            Effect::FreeInvocation(id) => {
                debug_if_leader!(
                    is_leader,
                    restate.invocation.id = %id,
                    "Effect: Free invocation"
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
                rpc.service = %service_invocation.invocation_target.service_name(),
                rpc.method = %service_invocation.invocation_target.handler_name(),
                restate.invocation.id = %service_invocation.invocation_id,
                restate.invocation.target = %service_invocation.invocation_target,
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
                restate.invocation.id = %invocation_termination.invocation_id,
                restate.outbox.seq = seq_number,
                "Effect: Send invocation termination command '{:?}' to partition processor",
                invocation_termination.flavor
            ),
            Effect::EnqueueIntoOutbox {
                seq_number,
                message:
                    OutboxMessage::ServiceResponse(InvocationResponse {
                        result: ResponseResult::Failure(e),
                        entry_index,
                        id,
                    }),
            } => debug_if_leader!(
                is_leader,
                restate.invocation.id = %id,
                restate.outbox.seq = seq_number,
                "Effect: Send failure '{}' response to another invocation, completing entry index {}",
                e,
                entry_index
            ),
            Effect::IngressResponse(IngressResponse {
                response: ResponseResult::Success(_),
                invocation_id,
                ..
            }) => debug_if_leader!(
                is_leader,
                restate.invocation.id = %invocation_id,
                "Effect: Send response to ingress: Success"),
            Effect::IngressResponse(IngressResponse {
                response: ResponseResult::Failure(e),
                invocation_id,
                ..
            }) => debug_if_leader!(
                is_leader,
                restate.invocation.id = %invocation_id,
                "Effect: Send response to ingress: Failure({})",
                e
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
                Timer::CompleteSleepEntry(_) => {
                    info_span_if_leader!(
                        is_leader,
                        span_context.is_sampled(),
                        span_context.as_parent(),
                        "sleep",
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
                Timer::Invoke(service_invocation) => {
                    // no span necessary; there will already be a background_invoke span
                    debug_if_leader!(
                        is_leader,
                        rpc.service = %service_invocation.invocation_target.service_name(),
                        rpc.method = %service_invocation.invocation_target.handler_name(),
                        restate.invocation.id = %service_invocation.invocation_id,
                        restate.invocation.target = %service_invocation.invocation_target,
                        restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                        restate.timer.wake_up_time = %timer_value.wake_up_time(),
                        "Effect: Register background invoke timer"
                    )
                }
                Timer::CleanInvocationStatus(invocation_id) => {
                    debug_if_leader!(
                        is_leader,
                        restate.invocation.id = %invocation_id,
                        restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                        restate.timer.wake_up_time = %timer_value.wake_up_time(),
                        "Effect: Register cleanup invocation status timer"
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
            Effect::TraceBackgroundInvoke {
                invocation_id_and_target: (invocation_id, invocation_target),
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
                        otel.name = format!("background_invoke {invocation_target}"),
                        rpc.service = %invocation_target.service_name(),
                        rpc.method = %invocation_target.handler_name(),
                        restate.invocation.id = %invocation_id,
                        restate.invocation.target = %invocation_target,
                        restate.internal.span_id = %pointer_span_id,
                    );
                } else {
                    info_span_if_leader!(
                        is_leader,
                        span_context.is_sampled(),
                        span_context.as_parent(),
                        "background_invoke",
                        otel.name = format!("background_invoke {invocation_target}"),
                        rpc.service = %invocation_target.service_name(),
                        rpc.method = %invocation_target.handler_name(),
                        restate.invocation.id = %invocation_id,
                        restate.invocation.target = %invocation_target,
                    );
                }
            }
            Effect::TraceInvocationResult {
                invocation_id_and_target: (invocation_id, invocation_target),
                creation_time,
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
                    otel.name = format!("invoke {invocation_target}"),
                    rpc.service = %invocation_target.service_name(),
                    rpc.method = %invocation_target.handler_name(),
                    restate.invocation.id = %invocation_id,
                    restate.invocation.target = %invocation_target,
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
            Effect::SendAbortInvocationToInvoker(invocation_id) => {
                debug_if_leader!(is_leader, restate.invocation.id = %invocation_id, "Effect: Send abort command to invoker");
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
            Effect::StoreCompletedInvocation { invocation_id, .. } => {
                debug_if_leader!(
                    is_leader,
                    restate.invocation.id = %invocation_id,
                    "Effect: Store completed invocation"
                );
            }
            Effect::PopInbox(service_id) => {
                debug_if_leader!(
                    is_leader,
                    rpc.service = %service_id.service_name,
                    "Effect: Pop inbox"
                );
            }
            Effect::AppendResponseSink {
                invocation_id,
                additional_response_sink,
                ..
            } => {
                debug_if_leader!(
                    is_leader,
                    restate.invocation.id = %invocation_id,
                    "Effect: Store additional response sink {:?}",
                    additional_response_sink
                );
            }
            Effect::StoreIdempotencyId(idempotency_id, invocation_id) => {
                debug_if_leader!(
                    is_leader,
                    restate.invocation.id = %invocation_id,
                    "Effect: Store idempotency id {:?}",
                    idempotency_id
                );
            }
            Effect::DeleteIdempotencyId(idempotency_id) => {
                debug_if_leader!(
                    is_leader,
                    "Effect: Delete idempotency id {:?}",
                    idempotency_id
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
    related_invocation_id: Option<InvocationId>,
    related_invocation_target: Option<InvocationTarget>,
    related_span: SpanRelation,
    effects: Vec<Effect>,
}

impl Effects {
    pub(crate) fn clear(&mut self) {
        self.related_invocation_id = None;
        self.related_invocation_target = None;
        self.related_span = SpanRelation::None;
        self.effects.clear();
    }

    pub(crate) fn set_related_invocation_id(&mut self, related_invocation_id: &InvocationId) {
        self.related_invocation_id = Some(*related_invocation_id);
    }

    pub(crate) fn set_related_invocation_target(
        &mut self,
        related_invocation_target: &InvocationTarget,
    ) {
        self.related_invocation_target = Some(related_invocation_target.clone());
    }

    pub(crate) fn set_related_span(&mut self, related_span: SpanRelation) {
        self.related_span = related_span;
    }

    pub(crate) fn set_parent_span_context(
        &mut self,
        service_invocation_span_context: &ServiceInvocationSpanContext,
    ) {
        self.set_related_span(service_invocation_span_context.as_parent())
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
        metadata: InFlightInvocationMetadata,
    ) {
        self.effects.push(Effect::ResumeService {
            invocation_id,
            metadata,
        });
    }

    pub(crate) fn suspend_service(
        &mut self,
        invocation_id: InvocationId,
        metadata: InFlightInvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    ) {
        self.effects.push(Effect::SuspendService {
            invocation_id,
            metadata,
            waiting_for_completed_entries,
        })
    }

    pub(crate) fn store_inboxed_invocation(
        &mut self,
        invocation_id: InvocationId,
        inboxed_invocation: InboxedInvocation,
    ) {
        self.effects.push(Effect::StoreInboxedInvocation(
            invocation_id,
            inboxed_invocation,
        ))
    }

    pub(crate) fn store_completed_invocation(
        &mut self,
        invocation_id: InvocationId,
        retention: Duration,
        completed_invocation: CompletedInvocation,
    ) {
        self.effects.push(Effect::StoreCompletedInvocation {
            invocation_id,
            retention,
            completed_invocation,
        })
    }

    pub(crate) fn free_invocation(&mut self, invocation_id: InvocationId) {
        self.effects.push(Effect::FreeInvocation(invocation_id))
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

    pub(crate) fn pop_inbox(&mut self, service_id: ServiceId) {
        self.effects.push(Effect::PopInbox(service_id))
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
        metadata: InFlightInvocationMetadata,
    ) {
        self.effects.push(Effect::StoreDeploymentId {
            invocation_id,
            deployment_id,
            metadata,
        })
    }

    pub(crate) fn append_response_sink(
        &mut self,
        invocation_id: InvocationId,
        previous_invocation_status: InvocationStatus,
        response_sink: ServiceInvocationResponseSink,
    ) {
        self.effects.push(Effect::AppendResponseSink {
            invocation_id,
            previous_invocation_status,
            additional_response_sink: response_sink,
        });
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
        invocation_id: InvocationId,
        completion: Completion,
    ) {
        self.effects.push(Effect::ForwardCompletion {
            invocation_id,
            completion,
        });
    }

    pub(crate) fn drop_journal(&mut self, invocation_id: InvocationId, journal_length: EntryIndex) {
        self.effects.push(Effect::DropJournal {
            invocation_id,
            journal_length,
        })
    }

    pub(crate) fn trace_background_invoke(
        &mut self,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        span_context: ServiceInvocationSpanContext,
        pointer_span_id: Option<SpanId>,
    ) {
        self.effects.push(Effect::TraceBackgroundInvoke {
            invocation_id_and_target: (invocation_id, invocation_target),
            span_context,
            pointer_span_id,
        })
    }

    pub(crate) fn trace_invocation_result(
        &mut self,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        span_context: ServiceInvocationSpanContext,
        creation_time: MillisSinceEpoch,
        result: Result<(), (InvocationErrorCode, String)>,
    ) {
        self.effects.push(Effect::TraceInvocationResult {
            invocation_id_and_target: (invocation_id, invocation_target),
            creation_time,
            span_context,
            result,
        })
    }

    pub(crate) fn abort_invocation(&mut self, invocation_id: InvocationId) {
        self.effects
            .push(Effect::SendAbortInvocationToInvoker(invocation_id));
    }

    pub(crate) fn store_idempotency_id(
        &mut self,
        idempotency_id: IdempotencyId,
        invocation_id: InvocationId,
    ) {
        self.effects
            .push(Effect::StoreIdempotencyId(idempotency_id, invocation_id));
    }

    pub(crate) fn delete_idempotency_id(&mut self, idempotency_id: IdempotencyId) {
        self.effects
            .push(Effect::DeleteIdempotencyId(idempotency_id));
    }

    pub(crate) fn send_stored_ack_to_invoker(
        &mut self,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) {
        self.effects.push(Effect::SendStoredEntryAckToInvoker(
            invocation_id,
            entry_index,
        ));
    }

    pub(crate) fn apply_state_mutation(&mut self, state_mutation: ExternalStateMutation) {
        self.effects.push(Effect::MutateState(state_mutation));
    }

    /// We log only if the log level is TRACE, or if the log level is DEBUG and we're the leader,
    /// or if the span level is INFO and we're the leader.
    pub(crate) fn log(&self, is_leader: bool) {
        // Skip this method altogether if logging is disabled
        if !(((event_enabled!(Level::DEBUG) || span_enabled!(Level::INFO)) && is_leader)
            || event_enabled!(Level::TRACE))
        {
            return;
        }

        let span = if is_leader {
            debug_span!("state_machine_effects")
        } else {
            trace_span!("state_machine_effects")
        };

        if let Some(invocation_id) = &self.related_invocation_id {
            span.record(
                "restate.invocation.id",
                tracing::field::display(invocation_id),
            );
        }

        if let Some(invocation_target) = &self.related_invocation_target {
            span.record(
                "rpc.service",
                tracing::field::display(invocation_target.service_name()),
            );
            span.record(
                "rpc.method",
                tracing::field::display(invocation_target.handler_name()),
            );
            span.record(
                "restate.invocation.target",
                tracing::field::display(invocation_target),
            );
        }

        // Create span and enter it
        self.related_span.attach_to_span(&span);
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
