// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{Error, StateMachine, StateMachineApplyContext};
use std::collections::HashSet;
use std::fmt;

use crate::partition::state_machine::actions::Action;
use crate::partition::types::InvocationIdAndTarget;
use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
use futures::{Stream, TryStreamExt};
use opentelemetry::trace::SpanId;
use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::idempotency_table::{IdempotencyMetadata, IdempotencyTable};
use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, InvocationStatus,
    ReadOnlyInvocationStatusTable,
};
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::promise_table::{Promise, PromiseTable};
use restate_storage_api::service_status_table::VirtualObjectStatus;
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_storage_api::{invocation_status_table, Result as StorageResult};
use restate_types::deployment::PinnedDeployment;
use restate_types::errors::InvocationErrorCode;
use restate_types::identifiers::{EntryIndex, IdempotencyId, InvocationId, ServiceId};
use restate_types::ingress;
use restate_types::ingress::{IngressResponseEnvelope, IngressResponseResult};
use restate_types::invocation::{
    InvocationInput, InvocationResponse, InvocationTargetType, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, VirtualObjectHandlerType,
    WorkflowHandlerType,
};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::raw::{PlainRawEntry, RawEntryCodec};
use restate_types::journal::{Completion, CompletionResult, EntryType};
use restate_types::message::MessageIndex;
use restate_types::state_mut::{ExternalStateMutation, StateMutationVersion};
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::timer::{TimerKeyDisplay, TimerKeyValue};
use std::future::Future;
use std::ops::RangeInclusive;
use std::time::Duration;
use tracing::{debug, trace, warn};

pub type ActionCollector = Vec<Action>;

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

// TODO this trait can be removed, it's unnecessary as it adds only additional indirection
pub trait StateStorage {
    fn store_service_status(
        &mut self,
        service_id: &ServiceId,
        service_status: VirtualObjectStatus,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn store_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
        invocation_status: InvocationStatus,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn drop_journal(
        &mut self,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn store_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn store_completion_result(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn load_completion_result(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> impl Future<Output = StorageResult<Option<CompletionResult>>> + Send;

    fn load_journal_entry(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> impl Future<Output = StorageResult<Option<EnrichedRawEntry>>> + Send;

    // In-/outbox
    fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        inbox_entry: InboxEntry,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn enqueue_into_outbox(
        &mut self,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn store_inbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn store_outbox_seq_number(
        &mut self,
        seq_number: MessageIndex,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn truncate_outbox(
        &mut self,
        outbox_sequence_number: RangeInclusive<MessageIndex>,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn pop_inbox(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = StorageResult<Option<SequenceNumberInboxEntry>>> + Send;

    fn delete_inbox_entry(
        &mut self,
        service_id: &ServiceId,
        sequence_number: MessageIndex,
    ) -> impl Future<Output = ()> + Send;

    fn get_all_user_states(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Stream<Item = restate_storage_api::Result<(Bytes, Bytes)>> + Send;

    // State
    fn store_state(
        &mut self,
        service_id: &ServiceId,
        key: Bytes,
        value: Bytes,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn load_state(
        &mut self,
        service_id: &ServiceId,
        key: &Bytes,
    ) -> impl Future<Output = StorageResult<Option<Bytes>>> + Send;

    fn clear_state(
        &mut self,
        service_id: &ServiceId,
        key: &Bytes,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn clear_all_state(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    // Timer
    fn store_timer(
        &mut self,
        timer_key: TimerKey,
        timer: Timer,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn delete_timer(
        &mut self,
        timer_key: &TimerKey,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

impl<Codec: RawEntryCodec> StateMachine<Codec> {
    pub(super) async fn do_invoke_service<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_invocation: ServiceInvocation,
        source_table: invocation_status_table::SourceTable,
    ) -> Result<(), Error> {
        debug_if_leader!(ctx.is_leader, "Effect: Invoke service");

        let invocation_id = service_invocation.invocation_id;
        let (in_flight_invocation_meta, invocation_input) =
            InFlightInvocationMetadata::from_service_invocation(service_invocation, source_table);
        Self::invoke_service(
            ctx.storage,
            ctx.action_collector,
            invocation_id,
            in_flight_invocation_meta,
            invocation_input,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn do_resume_service<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        mut metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Resume service"
        );

        metadata.timestamps.update();
        let invocation_target = metadata.invocation_target.clone();
        ctx.storage
            .store_invocation_status(&invocation_id, InvocationStatus::Invoked(metadata))
            .await?;

        ctx.action_collector.push(Action::Invoke {
            invocation_id,
            invocation_target,
            invoke_input_journal: InvokeInputJournal::NoCachedJournal,
        });

        Ok(())
    }

    pub(super) async fn do_suspend_service<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        mut metadata: InFlightInvocationMetadata,
        waiting_for_completed_entries: HashSet<EntryIndex>,
    ) -> Result<(), Error> {
        info_span_if_leader!(
            ctx.is_leader,
            metadata.journal_metadata.span_context.is_sampled(),
            metadata.journal_metadata.span_context.as_parent(),
            "suspend",
            restate.journal.length = metadata.journal_metadata.length,
            restate.invocation.id = %invocation_id,
        );
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Suspend service waiting on entries {:?}",
            waiting_for_completed_entries
        );

        metadata.timestamps.update();
        ctx.storage
            .store_invocation_status(
                &invocation_id,
                InvocationStatus::Suspended {
                    metadata,
                    waiting_for_completed_entries,
                },
            )
            .await?;

        Ok(())
    }

    pub(super) async fn do_store_inboxed_invocation<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        inboxed_invocation: InboxedInvocation,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            restate.outbox.seq = inboxed_invocation.inbox_sequence_number,
            "Effect: Store inboxed invocation"
        );

        ctx.storage
            .store_invocation_status(
                &invocation_id,
                InvocationStatus::Inboxed(inboxed_invocation),
            )
            .await?;

        Ok(())
    }

    pub(super) async fn do_store_completed_invocation<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        retention: Duration,
        completed_invocation: CompletedInvocation,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Store completed invocation"
        );

        ctx.storage
            .store_invocation_status(
                &invocation_id,
                InvocationStatus::Completed(completed_invocation),
            )
            .await?;
        ctx.action_collector
            .push(Action::ScheduleInvocationStatusCleanup {
                invocation_id,
                retention,
            });

        Ok(())
    }

    pub(super) async fn do_free_invocation<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Free invocation"
        );

        ctx.storage
            .store_invocation_status(&invocation_id, InvocationStatus::Free)
            .await?;

        Ok(())
    }

    pub(super) async fn do_enqueue_into_inbox<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        seq_number: MessageIndex,
        inbox_entry: InboxEntry,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.inbox.seq = seq_number,
            "Effect: Enqueue invocation in inbox"
        );

        ctx.storage
            .enqueue_into_inbox(seq_number, inbox_entry)
            .await?;
        // need to store the next inbox sequence number
        ctx.storage.store_inbox_seq_number(seq_number + 1).await?;

        Ok(())
    }

    pub(super) async fn do_pop_inbox<S: StateStorage + ReadOnlyInvocationStatusTable>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_id: ServiceId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            rpc.service = %service_id.service_name,
            "Effect: Pop inbox"
        );

        Self::pop_from_inbox(ctx.storage, ctx.action_collector, service_id).await?;

        Ok(())
    }

    pub(super) async fn do_delete_inbox_entry<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_id: ServiceId,
        sequence_number: MessageIndex,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            rpc.service = %service_id.service_name,
            restate.inbox.seq = sequence_number,
            "Effect: Delete inbox entry",
        );

        ctx.storage
            .delete_inbox_entry(&service_id, sequence_number)
            .await;

        Ok(())
    }

    pub(super) async fn do_enqueue_into_outbox<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        seq_number: MessageIndex,
        message: OutboxMessage,
    ) -> Result<(), Error> {
        match &message {
            OutboxMessage::ServiceInvocation(service_invocation) => debug_if_leader!(
                ctx.is_leader,
                rpc.service = %service_invocation.invocation_target.service_name(),
                rpc.method = %service_invocation.invocation_target.handler_name(),
                restate.invocation.id = %service_invocation.invocation_id,
                restate.invocation.target = %service_invocation.invocation_target,
                restate.outbox.seq = seq_number,
                "Effect: Send service invocation to partition processor"
            ),
            OutboxMessage::ServiceResponse(InvocationResponse {
                result: ResponseResult::Success(_),
                entry_index,
                id,
            }) => {
                debug_if_leader!(
                    ctx.is_leader,
                    restate.invocation.id = %id,
                    restate.outbox.seq = seq_number,
                    "Effect: Send success response to another invocation, completing entry index {}",
                    entry_index
                )
            }
            OutboxMessage::InvocationTermination(invocation_termination) => debug_if_leader!(
                ctx.is_leader,
                restate.invocation.id = %invocation_termination.invocation_id,
                restate.outbox.seq = seq_number,
                "Effect: Send invocation termination command '{:?}' to partition processor",
                invocation_termination.flavor
            ),
            OutboxMessage::ServiceResponse(InvocationResponse {
                result: ResponseResult::Failure(e),
                entry_index,
                id,
            }) => debug_if_leader!(
                ctx.is_leader,
                restate.invocation.id = %id,
                restate.outbox.seq = seq_number,
                "Effect: Send failure '{}' response to another invocation, completing entry index {}",
                e,
                entry_index
            ),
        };

        ctx.storage
            .enqueue_into_outbox(seq_number, message.clone())
            .await?;
        // need to store the next outbox sequence number
        ctx.storage.store_outbox_seq_number(seq_number + 1).await?;

        ctx.action_collector.push(Action::NewOutboxMessage {
            seq_number,
            message,
        });

        Ok(())
    }

    pub(super) async fn do_unlock_service<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_id: ServiceId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            rpc.service = %service_id.service_name,
            "Effect: Unlock service id",
        );

        ctx.storage
            .store_service_status(&service_id, VirtualObjectStatus::Unlocked)
            .await?;

        Ok(())
    }

    pub(super) async fn do_set_state<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        key: Bytes,
        value: Bytes,
    ) -> Result<(), Error> {
        info_span_if_leader!(
            ctx.is_leader,
            span_context.is_sampled(),
            span_context.as_parent(),
            "set_state",
            otel.name = format!("set_state {key:?}"),
            restate.state.key = ?key,
            rpc.service = %service_id.service_name,
            restate.invocation.id = %invocation_id,
        );

        debug_if_leader!(
            ctx.is_leader,
            restate.state.key = ?key,
            "Effect: Set state"
        );

        ctx.storage.store_state(&service_id, key, value).await?;

        Ok(())
    }

    pub(super) async fn do_clear_state<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
        key: Bytes,
    ) -> Result<(), Error> {
        info_span_if_leader!(
            ctx.is_leader,
            span_context.is_sampled(),
            span_context.as_parent(),
            "clear_state",
            otel.name = format!("clear_state {key:?}"),
            restate.state.key = ?key,
            rpc.service = %service_id.service_name,
            restate.invocation.id = %invocation_id,
        );

        debug_if_leader!(
            ctx.is_leader,
            restate.state.key = ?key,
            "Effect: Clear state"
        );

        ctx.storage.clear_state(&service_id, &key).await?;

        Ok(())
    }

    pub(super) async fn do_clear_all_state<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_id: ServiceId,
        invocation_id: InvocationId,
        span_context: ServiceInvocationSpanContext,
    ) -> Result<(), Error> {
        info_span_if_leader!(
            ctx.is_leader,
            span_context.is_sampled(),
            span_context.as_parent(),
            "clear_all_state",
            rpc.service = %service_id.service_name,
            restate.invocation.id = %invocation_id,
        );

        debug_if_leader!(ctx.is_leader, "Effect: Clear all state");

        ctx.storage.clear_all_state(&service_id).await?;

        Ok(())
    }

    pub(super) async fn do_register_timer<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        timer_value: TimerKeyValue,
        span_context: ServiceInvocationSpanContext,
    ) -> Result<(), Error> {
        match timer_value.value() {
            Timer::CompleteJournalEntry(_, entry_index) => {
                info_span_if_leader!(
                    ctx.is_leader,
                    span_context.is_sampled(),
                    span_context.as_parent(),
                    "sleep",
                    restate.journal.index = entry_index,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    // without converting to i64 this field will encode as a string
                    // however, overflowing i64 seems unlikely
                    restate.internal.end_time = i64::try_from(timer_value.wake_up_time().as_u64()).expect("wake up time should fit into i64"),
                );

                debug_if_leader!(
                    ctx.is_leader,
                    restate.journal.index = entry_index,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Effect: Register Sleep timer"
                )
            }
            Timer::Invoke(service_invocation) => {
                // no span necessary; there will already be a background_invoke span
                debug_if_leader!(
                    ctx.is_leader,
                    rpc.service = %service_invocation.invocation_target.service_name(),
                    rpc.method = %service_invocation.invocation_target.handler_name(),
                    restate.invocation.target = %service_invocation.invocation_target,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Effect: Register background invoke timer"
                )
            }
            Timer::NeoInvoke(invocation_id) => {
                // no span necessary; there will already be a background_invoke span
                debug_if_leader!(
                    ctx.is_leader,
                    restate.invocation.id = %invocation_id,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Effect: Register background invoke timer"
                )
            }
            Timer::CleanInvocationStatus(_) => {
                debug_if_leader!(
                    ctx.is_leader,
                    restate.timer.wake_up_time = %timer_value.wake_up_time(),
                    restate.timer.key = %TimerKeyDisplay(timer_value.key()),
                    "Effect: Register cleanup invocation status timer"
                )
            }
        };

        ctx.storage
            .store_timer(timer_value.key().clone(), timer_value.value().clone())
            .await?;

        ctx.action_collector
            .push(Action::RegisterTimer { timer_value });

        Ok(())
    }

    pub(super) async fn do_delete_timer<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        timer_key: TimerKey,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.timer.key = %TimerKeyDisplay(&timer_key),
            "Effect: Delete timer"
        );

        ctx.storage.delete_timer(&timer_key).await?;
        ctx.action_collector.push(Action::DeleteTimer { timer_key });

        Ok(())
    }

    pub(super) async fn do_store_pinned_deployment<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        pinned_deployment: PinnedDeployment,
        mut metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.deployment.id = %pinned_deployment.deployment_id,
            restate.deployment.service_protocol_version = %pinned_deployment.service_protocol_version.as_repr(),
            "Effect: Store chosen deployment to storage"
        );

        metadata.set_pinned_deployment(pinned_deployment);

        // We recreate the InvocationStatus in Invoked state as the invoker can notify the
        // chosen deployment_id only when the invocation is in-flight.
        ctx.storage
            .store_invocation_status(&invocation_id, InvocationStatus::Invoked(metadata))
            .await?;

        Ok(())
    }

    pub(super) async fn do_append_journal_entry<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        previous_invocation_status: InvocationStatus,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.index = entry_index,
            restate.invocation.id = %invocation_id,
            "Effect: Write journal entry {:?} to storage",
            journal_entry.header().as_entry_type()
        );

        Self::append_journal_entry(
            ctx.storage,
            invocation_id,
            previous_invocation_status,
            entry_index,
            journal_entry,
        )
        .await?;

        Ok(())
    }

    pub(super) async fn do_drop_journal<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        journal_length: EntryIndex,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = journal_length,
            "Effect: Drop journal"
        );

        // TODO: Only drop journals if the inbox is empty; this requires that keep track of the max journal length: https://github.com/restatedev/restate/issues/272
        ctx.storage
            .drop_journal(&invocation_id, journal_length)
            .await?;

        Ok(())
    }

    pub(super) async fn do_truncate_outbox<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        range: RangeInclusive<MessageIndex>,
    ) -> Result<(), Error> {
        trace!(
            restate.outbox.seq_from = range.start(),
            restate.outbox.seq_to = range.end(),
            "Effect: Truncate outbox"
        );

        ctx.storage.truncate_outbox(range).await?;

        Ok(())
    }

    pub(super) async fn do_store_completion<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        Completion {
            entry_index,
            result,
        }: Completion,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.index = entry_index,
            "Effect: Store completion {}",
            CompletionResultFmt(&result)
        );

        Self::store_completion(ctx.storage, &invocation_id, entry_index, result).await?;

        Ok(())
    }

    pub(super) fn do_forward_completion<S>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        completion: Completion,
    ) {
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.index = completion.entry_index,
            "Effect: Forward completion {} to deployment",
            CompletionResultFmt(&completion.result)
        );

        ctx.action_collector.push(Action::ForwardCompletion {
            invocation_id,
            completion,
        });
    }

    pub(super) async fn do_append_response_sink<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        // We pass around the invocation_status here to avoid an additional read.
        // We could in theory get rid of this here (and in other places, such as StoreDeploymentId),
        // by using a merge operator in rocksdb.
        mut previous_invocation_status: InvocationStatus,
        additional_response_sink: ServiceInvocationResponseSink,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Store additional response sink {:?}",
            additional_response_sink
        );

        previous_invocation_status
            .get_response_sinks_mut()
            .expect("No response sinks available")
            .insert(additional_response_sink);
        previous_invocation_status.update_timestamps();

        ctx.storage
            .store_invocation_status(&invocation_id, previous_invocation_status)
            .await?;

        Ok(())
    }

    pub(super) async fn do_store_idempotency_id<S: IdempotencyTable>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        idempotency_id: IdempotencyId,
        invocation_id: InvocationId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            restate.invocation.id = %invocation_id,
            "Effect: Store idempotency id {:?}",
            idempotency_id
        );

        ctx.storage
            .put_idempotency_metadata(&idempotency_id, IdempotencyMetadata { invocation_id })
            .await;

        Ok(())
    }

    pub(super) async fn do_delete_idempotency_id<S: StateStorage + IdempotencyTable>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        idempotency_id: IdempotencyId,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            "Effect: Delete idempotency id {:?}",
            idempotency_id
        );

        ctx.storage
            .delete_idempotency_metadata(&idempotency_id)
            .await;

        Ok(())
    }

    pub(super) fn do_trace_invocation_result<S>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        (invocation_id, invocation_target): InvocationIdAndTarget,
        creation_time: MillisSinceEpoch,
        span_context: ServiceInvocationSpanContext,
        result: Result<(), (InvocationErrorCode, String)>,
    ) {
        let (result, error) = match result {
            Ok(_) => ("Success", false),
            Err(_) => ("Failure", true),
        };

        info_span_if_leader!(
            ctx.is_leader,
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
    }

    pub(super) fn do_trace_background_invoke<S>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        (invocation_id, invocation_target): InvocationIdAndTarget,
        span_context: ServiceInvocationSpanContext,
        pointer_span_id: Option<SpanId>,
    ) {
        // create an instantaneous 'pointer span' which lives in the calling trace at the
        // time of background call, and primarily exists to be linked to by the new trace
        // that will be created for the background invocation, but even if that trace wasn't
        // sampled for some reason, it's still worth producing this span

        if let Some(pointer_span_id) = pointer_span_id {
            info_span_if_leader!(
                ctx.is_leader,
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
                ctx.is_leader,
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

    pub(super) fn do_send_abort_invocation_to_invoker<S>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
    ) {
        debug_if_leader!(ctx.is_leader, restate.invocation.id = %invocation_id, "Effect: Send abort command to invoker");

        ctx.action_collector
            .push(Action::AbortInvocation(invocation_id));
    }

    pub(super) fn do_send_stored_entry_ack_to_invoker<S>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
    ) {
        ctx.action_collector.push(Action::AckStoredEntry {
            invocation_id,
            entry_index,
        });
    }

    pub(super) async fn do_mutate_state<S: StateStorage>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        state_mutation: ExternalStateMutation,
    ) -> Result<(), Error> {
        debug_if_leader!(
            ctx.is_leader,
            "Effect: Mutate state for service id '{:?}'",
            &state_mutation.service_id
        );

        Self::mutate_state(ctx.storage, state_mutation).await?;

        Ok(())
    }

    pub(super) fn do_ingress_response<S>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        ingress_response: IngressResponseEnvelope<ingress::InvocationResponse>,
    ) {
        match &ingress_response.inner {
            ingress::InvocationResponse {
                response: IngressResponseResult::Success(_, _),
                request_id,
                ..
            } => debug_if_leader!(
                ctx.is_leader,
                "Effect: Send response with ingress request id {:?} to ingress: Success",
                request_id
            ),
            ingress::InvocationResponse {
                response: IngressResponseResult::Failure(e),
                request_id,
                ..
            } => debug_if_leader!(
                ctx.is_leader,
                "Effect: Send response with ingress request id {:?} to ingress: Failure({})",
                request_id,
                e
            ),
        };

        ctx.action_collector
            .push(Action::IngressResponse(ingress_response));
    }

    pub(super) fn do_ingress_submit_notification<S>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        attach_notification: IngressResponseEnvelope<ingress::SubmittedInvocationNotification>,
    ) {
        debug_if_leader!(
            ctx.is_leader,
            "Effect: Ingress attach invocation {} to {}",
            attach_notification.inner.original_invocation_id,
            attach_notification.inner.attached_invocation_id,
        );

        ctx.action_collector
            .push(Action::IngressSubmitNotification(attach_notification));
    }

    pub(super) async fn do_put_promise<S: PromiseTable>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_id: ServiceId,
        key: ByteString,
        metadata: Promise,
    ) -> Result<(), Error> {
        debug_if_leader!(ctx.is_leader, rpc.service = %service_id.service_name, "Effect: Put promise {} in non completed state", key);

        ctx.storage.put_promise(&service_id, &key, metadata).await;

        Ok(())
    }

    pub(super) async fn do_clear_all_promises<S: PromiseTable>(
        ctx: &mut StateMachineApplyContext<'_, S>,
        service_id: ServiceId,
    ) -> Result<(), Error> {
        debug_if_leader!(
                    ctx.is_leader,
                    rpc.service = %service_id.service_name,
                    "Effect: Clear all promises");

        ctx.storage.delete_all_promises(&service_id).await;

        Ok(())
    }

    async fn pop_from_inbox<S>(
        state_storage: &mut S,
        collector: &mut ActionCollector,
        service_id: ServiceId,
    ) -> Result<(), Error>
    where
        S: StateStorage + ReadOnlyInvocationStatusTable,
    {
        // Pop until we find the first inbox entry.
        // Note: the inbox seq numbers can have gaps.
        while let Some(inbox_entry) = state_storage.pop_inbox(&service_id).await? {
            match inbox_entry.inbox_entry {
                InboxEntry::Invocation(_, invocation_id) => {
                    let inboxed_status =
                        state_storage.get_invocation_status(&invocation_id).await?;

                    let_assert!(
                        InvocationStatus::Inboxed(inboxed_invocation) = inboxed_status,
                        "InvocationStatus must contain an Inboxed invocation for the id {}",
                        invocation_id
                    );

                    let (in_flight_invocation_meta, invocation_input) =
                        InFlightInvocationMetadata::from_inboxed_invocation(inboxed_invocation);
                    Self::invoke_service(
                        state_storage,
                        collector,
                        invocation_id,
                        in_flight_invocation_meta,
                        invocation_input,
                    )
                    .await?;
                    return Ok(());
                }
                InboxEntry::StateMutation(state_mutation) => {
                    Self::mutate_state(state_storage, state_mutation).await?;
                }
            }
        }

        state_storage
            .store_service_status(&service_id, VirtualObjectStatus::Unlocked)
            .await?;

        Ok(())
    }

    async fn mutate_state<S: StateStorage>(
        state_storage: &mut S,
        state_mutation: ExternalStateMutation,
    ) -> StorageResult<()> {
        let ExternalStateMutation {
            service_id,
            version,
            state,
        } = state_mutation;

        // overwrite all existing key value pairs with the provided ones; delete all entries that
        // are not contained in state
        let all_user_states: Vec<(Bytes, Bytes)> = state_storage
            .get_all_user_states(&service_id)
            .try_collect()
            .await?;

        if let Some(expected_version) = version {
            let expected = StateMutationVersion::from_raw(expected_version);
            let actual = StateMutationVersion::from_user_state(&all_user_states);

            if actual != expected {
                debug!("Ignore state mutation for service id '{:?}' because the expected version '{}' is not matching the actual version '{}'", &service_id, expected, actual);
                return Ok(());
            }
        }

        for (key, _) in &all_user_states {
            if !state.contains_key(key) {
                state_storage.clear_state(&service_id, key).await?;
            }
        }

        // overwrite existing key value pairs
        for (key, value) in state {
            state_storage.store_state(&service_id, key, value).await?
        }

        Ok(())
    }

    async fn invoke_service<S: StateStorage>(
        state_storage: &mut S,
        collector: &mut ActionCollector,
        invocation_id: InvocationId,
        mut in_flight_invocation_metadata: InFlightInvocationMetadata,
        invocation_input: InvocationInput,
    ) -> Result<(), Error> {
        // In our current data model, ServiceInvocation has always an input, so initial length is 1
        in_flight_invocation_metadata.journal_metadata.length = 1;

        let invocation_target_type = in_flight_invocation_metadata
            .invocation_target
            .invocation_target_ty();
        if invocation_target_type
            == InvocationTargetType::VirtualObject(VirtualObjectHandlerType::Exclusive)
            || invocation_target_type
                == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            state_storage
                .store_service_status(
                    &in_flight_invocation_metadata
                        .invocation_target
                        .as_keyed_service_id()
                        .unwrap(),
                    VirtualObjectStatus::Locked(invocation_id),
                )
                .await?;
        }
        state_storage
            .store_invocation_status(
                &invocation_id,
                InvocationStatus::Invoked(in_flight_invocation_metadata.clone()),
            )
            .await?;

        let input_entry =
            Codec::serialize_as_input_entry(invocation_input.headers, invocation_input.argument);
        let (entry_header, serialized_entry) = input_entry.into_inner();

        collector.push(Action::Invoke {
            invocation_id,
            invocation_target: in_flight_invocation_metadata.invocation_target,
            invoke_input_journal: InvokeInputJournal::CachedJournal(
                restate_invoker_api::JournalMetadata::new(
                    in_flight_invocation_metadata.journal_metadata.length,
                    in_flight_invocation_metadata.journal_metadata.span_context,
                    None,
                ),
                vec![PlainRawEntry::new(
                    entry_header.clone().erase_enrichment(),
                    serialized_entry.clone(),
                )],
            ),
        });

        state_storage
            .store_journal_entry(
                &invocation_id,
                0,
                EnrichedRawEntry::new(entry_header, serialized_entry),
            )
            .await?;

        Ok(())
    }

    /// Stores the given completion. Returns `true` if an [`RawEntry`] was completed.
    async fn store_completion<S: StateStorage>(
        state_storage: &mut S,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> Result<bool, Error> {
        if let Some(mut journal_entry) = state_storage
            .load_journal_entry(invocation_id, entry_index)
            .await?
        {
            if journal_entry.ty() == EntryType::Awakeable
                && journal_entry.header().is_completed() == Some(true)
            {
                // We can ignore when we get an awakeable completion twice as they might be a result of
                // some request being retried from the ingress to complete the awakeable.
                // We'll use only the first completion, because changing the awakeable result
                // after it has been completed for the first time can cause non-deterministic execution.
                warn!(
                    restate.invocation.id = %invocation_id,
                    restate.journal.index = entry_index,
                    "Trying to complete an awakeable already completed. Ignoring this completion");
                debug!("Discarded awakeable completion: {:?}", completion_result);
                return Ok(false);
            }
            Codec::write_completion(&mut journal_entry, completion_result)?;
            state_storage
                .store_journal_entry(invocation_id, entry_index, journal_entry)
                .await?;
            Ok(true)
        } else {
            // In case we don't have the journal entry (only awakeables case),
            // we'll send the completion afterward once we receive the entry.
            state_storage
                .store_completion_result(invocation_id, entry_index, completion_result)
                .await?;
            Ok(false)
        }
    }

    async fn append_journal_entry<S: StateStorage>(
        state_storage: &mut S,
        invocation_id: InvocationId,
        mut previous_invocation_status: InvocationStatus,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> Result<(), Error> {
        // Store journal entry
        state_storage
            .store_journal_entry(&invocation_id, entry_index, journal_entry)
            .await?;

        // update the journal metadata length
        let journal_meta = previous_invocation_status
            .get_journal_metadata_mut()
            .expect("At this point there must be a journal");
        debug_assert_eq!(
            journal_meta.length, entry_index,
            "journal should not have gaps"
        );
        journal_meta.length = entry_index + 1;

        // Update timestamps
        previous_invocation_status.update_timestamps();

        // Store invocation status
        state_storage
            .store_invocation_status(&invocation_id, previous_invocation_status)
            .await?;

        Ok(())
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
