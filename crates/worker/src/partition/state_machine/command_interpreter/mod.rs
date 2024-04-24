// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::Error;

use crate::partition::services::deterministic;
use crate::partition::state_machine::effects::Effects;
use crate::partition::types::{
    create_response_message, InvokerEffect, InvokerEffectKind, OutboxMessageExt, ResponseMessage,
};
use assert2::let_assert;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_storage_api::idempotency_table::ReadOnlyIdempotencyTable;
use restate_storage_api::inbox_table::InboxEntry;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation, InvocationStatus,
};
use restate_storage_api::journal_table::{JournalEntry, ReadOnlyJournalTable};
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::service_status_table::VirtualObjectStatus;
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_storage_api::Result as StorageResult;
use restate_types::errors::{
    InvocationError, InvocationErrorCode, CANCELED_INVOCATION_ERROR, GONE_INVOCATION_ERROR,
    KILLED_INVOCATION_ERROR,
};
use restate_types::identifiers::{
    EntryIndex, IdempotencyId, InvocationId, PartitionKey, ServiceId, WithPartitionKey,
};
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    HandlerType, InvocationResponse, InvocationTarget, InvocationTermination, ResponseResult,
    ServiceInvocation, ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
    SpanRelationCause, TerminationFlavor,
};
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
};
use restate_types::journal::raw::RawEntryCodec;
use restate_types::journal::Completion;
use restate_types::journal::*;
use restate_types::message::MessageIndex;
use restate_types::state_mut::ExternalStateMutation;
use restate_types::time::MillisSinceEpoch;
use restate_wal_protocol::effects::{BuiltinServiceEffect, BuiltinServiceEffects};
use restate_wal_protocol::timer::TimerValue;
use restate_wal_protocol::Command;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use std::pin::pin;
use tracing::{debug, instrument, trace, warn};

pub trait StateReader {
    fn get_virtual_object_status(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = StorageResult<VirtualObjectStatus>> + Send;

    fn get_invocation_status(
        &mut self,
        invocation_id: &InvocationId,
    ) -> impl Future<Output = StorageResult<InvocationStatus>> + Send;

    fn is_entry_resumable(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> impl Future<Output = StorageResult<bool>> + Send;

    fn load_state(
        &mut self,
        service_id: &ServiceId,
        key: &Bytes,
    ) -> impl Future<Output = StorageResult<Option<Bytes>>> + Send;

    fn load_state_keys(
        &mut self,
        service_id: &ServiceId,
    ) -> impl Future<Output = StorageResult<Vec<Bytes>>> + Send;

    fn load_completion_result(
        &mut self,
        invocation_id: &InvocationId,
        entry_index: EntryIndex,
    ) -> impl Future<Output = StorageResult<Option<CompletionResult>>> + Send;

    fn get_journal(
        &mut self,
        invocation_id: &InvocationId,
        length: EntryIndex,
    ) -> impl Stream<Item = StorageResult<(EntryIndex, JournalEntry)>> + Send;
}

pub(crate) struct CommandInterpreter<Codec> {
    // initialized from persistent storage
    inbox_seq_number: MessageIndex,
    outbox_seq_number: MessageIndex,
    partition_key_range: RangeInclusive<PartitionKey>,

    _codec: PhantomData<Codec>,
}

impl<Codec> Debug for CommandInterpreter<Codec> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectCollector")
            .field("inbox_seq_number", &self.inbox_seq_number)
            .field("outbox_seq_number", &self.outbox_seq_number)
            .finish()
    }
}

impl<Codec> CommandInterpreter<Codec> {
    pub(crate) fn new(
        inbox_seq_number: MessageIndex,
        outbox_seq_number: MessageIndex,
        partition_key_range: RangeInclusive<PartitionKey>,
    ) -> Self {
        Self {
            inbox_seq_number,
            outbox_seq_number,
            partition_key_range,
            _codec: PhantomData,
        }
    }
}

impl<Codec> CommandInterpreter<Codec>
where
    Codec: RawEntryCodec,
{
    /// Applies the given command and returns effects via the provided effects struct
    ///
    /// We pass in the effects message as a mutable borrow to be able to reuse it across
    /// invocations of this methods which lies on the hot path.
    ///
    /// We use the returned service invocation id and span relation to log the effects (see [`Effects#log`]).
    #[instrument(level = "trace", skip_all, fields(command = ?command), err)]
    pub(crate) async fn on_apply<
        State: StateReader + ReadOnlyJournalTable + ReadOnlyIdempotencyTable,
    >(
        &mut self,
        command: Command,
        effects: &mut Effects,
        state: &mut State,
    ) -> Result<(), Error> {
        match command {
            Command::Invoke(service_invocation) => {
                self.handle_invoke(effects, state, service_invocation).await
            }
            Command::InvocationResponse(InvocationResponse {
                id,
                entry_index,
                result,
            }) => {
                let completion = Completion {
                    entry_index,
                    result: result.into(),
                };

                Self::handle_completion(id, completion, state, effects).await
            }
            Command::ProxyThrough(service_invocation) => {
                self.handle_outgoing_message(
                    OutboxMessage::ServiceInvocation(service_invocation),
                    effects,
                );
                Ok(())
            }
            Command::InvokerEffect(effect) => self.try_invoker_effect(effects, state, effect).await,
            Command::TruncateOutbox(index) => {
                effects.truncate_outbox(index);
                Ok(())
            }
            Command::Timer(timer) => self.on_timer(timer, state, effects).await,
            Command::TerminateInvocation(invocation_termination) => {
                self.try_terminate_invocation(invocation_termination, state, effects)
                    .await
            }
            Command::BuiltInInvokerEffect(builtin_service_effects) => {
                self.try_built_in_invoker_effect(effects, state, builtin_service_effects)
                    .await
            }
            Command::PatchState(mutation) => {
                self.handle_external_state_mutation(mutation, state, effects)
                    .await
            }
            Command::AnnounceLeader(_) => {
                // no-op :-)
                Ok(())
            }
            Command::ScheduleTimer(timer) => {
                effects.register_timer(timer, Default::default());
                Ok(())
            }
        }
    }

    async fn handle_invoke<State: StateReader + ReadOnlyIdempotencyTable>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        service_invocation: ServiceInvocation,
    ) -> Result<(), Error> {
        debug_assert!(
            self.partition_key_range.contains(&service_invocation.partition_key()),
            "Service invocation with partition key '{}' has been delivered to a partition processor with key range '{:?}'. This indicates a bug.",
            service_invocation.partition_key(),
            self.partition_key_range);

        effects.set_related_invocation_id(&service_invocation.invocation_id);
        effects.set_related_invocation_target(&service_invocation.invocation_target);
        effects.set_parent_span_context(&service_invocation.span_context);

        // If an idempotency key is set, handle idempotency
        if let Some(idempotency) = &service_invocation.idempotency {
            let idempotency_id = IdempotencyId::combine(
                service_invocation.invocation_id,
                &service_invocation.invocation_target,
                idempotency.key.clone(),
            );
            if self
                .try_resolve_idempotent_request(
                    effects,
                    state,
                    &idempotency_id,
                    &service_invocation.invocation_id,
                    service_invocation.response_sink.as_ref(),
                )
                .await?
            {
                // Invocation was either resolved, or the sink was enqueued. Nothing else to do here.
                return Ok(());
            }
            // Idempotent invocation needs to be processed for the first time, let's roll!
            effects.store_idempotency_id(idempotency_id, service_invocation.invocation_id);
        }

        // If an execution_time is set, we schedule the invocation to be processed later
        if let Some(execution_time) = service_invocation.execution_time {
            let span_context = service_invocation.span_context.clone();
            effects.register_timer(
                TimerValue::new_invoke(
                    service_invocation.invocation_id,
                    execution_time,
                    // This entry_index here makes little sense
                    0,
                    service_invocation,
                ),
                span_context,
            );
            // The span will be created later on invocation
            return Ok(());
        }

        // If deterministic built-in service, just go and execute it
        if deterministic::ServiceInvoker::is_supported(
            service_invocation.invocation_target.service_name(),
        ) {
            self.handle_deterministic_built_in_service_invocation(service_invocation, effects)
                .await;
            return Ok(());
        }

        // If it's exclusive, we need to acquire the exclusive lock
        if service_invocation.invocation_target.handler_ty() == Some(HandlerType::Exclusive) {
            let keyed_service_id = service_invocation
                .invocation_target
                .as_keyed_service_id()
                .expect(
                    "When the handler type is Exclusive, the invocation target must have a key",
                );

            let service_status = state.get_virtual_object_status(&keyed_service_id).await?;

            // If locked, enqueue in inbox and be done with it
            if let VirtualObjectStatus::Locked(_) = service_status {
                let inbox_seq_number = self.enqueue_into_inbox(
                    effects,
                    InboxEntry::Invocation(keyed_service_id, service_invocation.invocation_id),
                );
                effects.store_inboxed_invocation(
                    service_invocation.invocation_id,
                    InboxedInvocation::from_service_invocation(
                        service_invocation,
                        inbox_seq_number,
                    ),
                );
                return Ok(());
            }
        }

        // We're ready to invoke the service!
        effects.invoke_service(service_invocation);
        Ok(())
    }

    fn enqueue_into_inbox(
        &mut self,
        effects: &mut Effects,
        inbox_entry: InboxEntry,
    ) -> MessageIndex {
        let inbox_seq_number = self.inbox_seq_number;
        effects.enqueue_into_inbox(self.inbox_seq_number, inbox_entry);
        self.inbox_seq_number += 1;
        inbox_seq_number
    }

    /// If true is returned, the request has been resolved and no further processing is needed
    async fn try_resolve_idempotent_request<State: StateReader + ReadOnlyIdempotencyTable>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        idempotency_id: &IdempotencyId,
        caller_id: &InvocationId,
        response_sink: Option<&ServiceInvocationResponseSink>,
    ) -> Result<bool, Error> {
        if let Some(idempotency_meta) = state.get_idempotency_metadata(idempotency_id).await? {
            let original_invocation_id = idempotency_meta.invocation_id;
            match state.get_invocation_status(&original_invocation_id).await? {
                executing_invocation_status @ InvocationStatus::Invoked(_)
                | executing_invocation_status @ InvocationStatus::Suspended { .. } => {
                    if let Some(response_sink) = response_sink {
                        if !executing_invocation_status
                            .get_invocation_metadata()
                            .expect("InvocationMetadata must be present")
                            .response_sinks
                            .contains(response_sink)
                        {
                            effects.append_response_sink(
                                original_invocation_id,
                                executing_invocation_status,
                                response_sink.clone(),
                            )
                        }
                    }
                }
                InvocationStatus::Inboxed(inboxed) => {
                    if let Some(response_sink) = response_sink {
                        if !inboxed.response_sinks.contains(response_sink) {
                            effects.append_response_sink(
                                original_invocation_id,
                                InvocationStatus::Inboxed(inboxed),
                                response_sink.clone(),
                            )
                        }
                    }
                }
                InvocationStatus::Completed(completed) => {
                    self.send_response_to_sinks(
                        effects,
                        caller_id,
                        Some(idempotency_id.clone()),
                        response_sink.cloned(),
                        completed.response_result,
                    );
                }
                InvocationStatus::Free => self.send_response_to_sinks(
                    effects,
                    caller_id,
                    Some(idempotency_id.clone()),
                    response_sink.cloned(),
                    GONE_INVOCATION_ERROR,
                ),
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn handle_external_state_mutation<State: StateReader>(
        &mut self,
        mutation: ExternalStateMutation,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        let service_status = state
            .get_virtual_object_status(&mutation.service_id)
            .await?;

        match service_status {
            VirtualObjectStatus::Locked(_) => {
                self.enqueue_into_inbox(effects, InboxEntry::StateMutation(mutation));
            }
            VirtualObjectStatus::Unlocked => effects.apply_state_mutation(mutation),
        }

        Ok(())
    }

    async fn try_built_in_invoker_effect<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        nbis_effects: BuiltinServiceEffects,
    ) -> Result<(), Error> {
        let (invocation_id, nbis_effects) = nbis_effects.into_inner();
        let invocation_status =
            Self::get_invocation_status_and_trace(state, &invocation_id, effects).await?;

        match invocation_status {
            InvocationStatus::Invoked(invocation_metadata) => {
                for nbis_effect in nbis_effects {
                    self.on_built_in_invoker_effect(
                        effects,
                        &invocation_id,
                        &invocation_metadata,
                        nbis_effect,
                    )
                    .await?
                }
                Ok(())
            }
            _ => {
                trace!(
                    "Received built in invoker effect for unknown invocation {}. Ignoring it.",
                    invocation_id
                );
                Ok(())
            }
        }
    }

    async fn on_built_in_invoker_effect(
        &mut self,
        effects: &mut Effects,
        invocation_id: &InvocationId,
        invocation_metadata: &InFlightInvocationMetadata,
        nbis_effect: BuiltinServiceEffect,
    ) -> Result<(), Error> {
        match nbis_effect {
            BuiltinServiceEffect::SetState { key, value } => {
                effects.set_state(
                    invocation_metadata
                        .invocation_target
                        .as_keyed_service_id()
                        .expect("Non deterministic built in services clearing state MUST be keyed"),
                    *invocation_id,
                    invocation_metadata.journal_metadata.span_context.clone(),
                    Bytes::from(key.into_owned()),
                    value,
                );
            }
            BuiltinServiceEffect::ClearState(key) => {
                effects.clear_state(
                    invocation_metadata
                        .invocation_target
                        .as_keyed_service_id()
                        .expect("Non deterministic built in services clearing state MUST be keyed"),
                    *invocation_id,
                    invocation_metadata.journal_metadata.span_context.clone(),
                    Bytes::from(key.into_owned()),
                );
            }
            BuiltinServiceEffect::OutboxMessage(msg) => {
                self.handle_outgoing_message(msg, effects);
            }
            BuiltinServiceEffect::End(None) => {
                self.end_built_in_invocation(effects, *invocation_id, invocation_metadata.clone())
                    .await?
            }
            BuiltinServiceEffect::End(Some(e)) => {
                self.fail_invocation(effects, *invocation_id, invocation_metadata.clone(), e)
                    .await?
            }
            BuiltinServiceEffect::IngressResponse(ingress_response) => {
                self.ingress_response(ingress_response, effects);
            }
        }

        Ok(())
    }

    async fn try_terminate_invocation<State: StateReader>(
        &mut self,
        InvocationTermination {
            invocation_id,
            flavor: termination_flavor,
        }: InvocationTermination,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        match termination_flavor {
            TerminationFlavor::Kill => {
                self.try_kill_invocation(invocation_id, state, effects)
                    .await
            }
            TerminationFlavor::Cancel => {
                self.try_cancel_invocation(invocation_id, state, effects)
                    .await
            }
        }
    }

    async fn try_kill_invocation<State: StateReader>(
        &mut self,
        invocation_id: InvocationId,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        let status = Self::get_invocation_status_and_trace(state, &invocation_id, effects).await?;

        match status {
            InvocationStatus::Invoked(metadata) | InvocationStatus::Suspended { metadata, .. } => {
                self.kill_invocation(invocation_id, metadata, state, effects)
                    .await?;
            }
            InvocationStatus::Inboxed(inboxed) => self.terminate_inboxed_invocation(
                TerminationFlavor::Kill,
                invocation_id,
                inboxed,
                effects,
            )?,
            _ => {
                trace!("Received kill command for unknown invocation with id '{invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                effects.abort_invocation(invocation_id);
            }
        };

        Ok(())
    }

    async fn try_cancel_invocation<State: StateReader>(
        &mut self,
        invocation_id: InvocationId,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        let status = Self::get_invocation_status_and_trace(state, &invocation_id, effects).await?;

        match status {
            InvocationStatus::Invoked(metadata) => {
                self.cancel_journal_leaves(
                    invocation_id,
                    InvocationStatusProjection::Invoked,
                    metadata.journal_metadata.length,
                    state,
                    effects,
                )
                .await?;
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_completed_entries,
            } => {
                if self
                    .cancel_journal_leaves(
                        invocation_id,
                        InvocationStatusProjection::Suspended(waiting_for_completed_entries),
                        metadata.journal_metadata.length,
                        state,
                        effects,
                    )
                    .await?
                {
                    effects.resume_service(invocation_id, metadata);
                }
            }
            InvocationStatus::Inboxed(inboxed) => self.terminate_inboxed_invocation(
                TerminationFlavor::Cancel,
                invocation_id,
                inboxed,
                effects,
            )?,
            _ => {
                trace!("Received cancel command for unknown invocation with id '{invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                effects.abort_invocation(invocation_id);
            }
        };

        Ok(())
    }

    fn terminate_inboxed_invocation(
        &mut self,
        termination_flavor: TerminationFlavor,
        invocation_id: InvocationId,
        inboxed_invocation: InboxedInvocation,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        let error = match termination_flavor {
            TerminationFlavor::Kill => KILLED_INVOCATION_ERROR,
            TerminationFlavor::Cancel => CANCELED_INVOCATION_ERROR,
        };

        let InboxedInvocation {
            inbox_sequence_number,
            response_sinks,
            span_context,
            invocation_target,
            ..
        } = inboxed_invocation;

        // Reply back to callers with error, and publish end trace
        let idempotency_id = inboxed_invocation.idempotency.map(|idempotency| {
            IdempotencyId::combine(invocation_id, &invocation_target, idempotency.key)
        });

        self.send_response_to_sinks(
            effects,
            &invocation_id,
            idempotency_id,
            response_sinks,
            &error,
        );

        // Delete inbox entry and invocation status.
        effects.delete_inbox_entry(
            invocation_target
                .as_keyed_service_id()
                .expect("Because the invocation is inboxed, it must have a keyed service id"),
            inbox_sequence_number,
        );
        effects.free_invocation(invocation_id);

        self.notify_invocation_result(
            invocation_id,
            invocation_target,
            span_context,
            MillisSinceEpoch::now(),
            Err((error.code(), error.to_string())),
            effects,
        );

        Ok(())
    }

    async fn kill_invocation<State: StateReader>(
        &mut self,
        invocation_id: InvocationId,
        metadata: InFlightInvocationMetadata,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        self.kill_child_invocations(
            &invocation_id,
            state,
            effects,
            metadata.journal_metadata.length,
        )
        .await?;

        self.fail_invocation(effects, invocation_id, metadata, KILLED_INVOCATION_ERROR)
            .await?;
        effects.abort_invocation(invocation_id);
        Ok(())
    }

    async fn kill_child_invocations<State: StateReader>(
        &mut self,
        invocation_id: &InvocationId,
        state: &mut State,
        effects: &mut Effects,
        journal_length: EntryIndex,
    ) -> Result<(), Error> {
        let mut journal_entries = pin!(state.get_journal(invocation_id, journal_length));
        while let Some(journal_entry) = journal_entries.next().await {
            let (_, journal_entry) = journal_entry?;

            if let JournalEntry::Entry(enriched_entry) = journal_entry {
                let (h, _) = enriched_entry.into_inner();
                match h {
                    // we only need to kill child invocations if they are not completed and the target was resolved
                    EnrichedEntryHeader::Call {
                        is_completed,
                        enrichment_result: Some(enrichment_result),
                    } if !is_completed => {
                        self.handle_outgoing_message(
                            OutboxMessage::InvocationTermination(InvocationTermination::kill(
                                enrichment_result.invocation_id,
                            )),
                            effects,
                        );
                    }
                    // we neither kill background calls nor delayed calls since we are considering them detached from this
                    // call tree. In the future we want to support a mode which also kills these calls (causally related).
                    // See https://github.com/restatedev/restate/issues/979
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn cancel_journal_leaves<State: StateReader>(
        &mut self,
        invocation_id: InvocationId,
        invocation_status: InvocationStatusProjection,
        journal_length: EntryIndex,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<bool, Error> {
        let mut journal = pin!(state.get_journal(&invocation_id, journal_length));

        let canceled_result = CompletionResult::from(&CANCELED_INVOCATION_ERROR);

        let mut resume_invocation = false;

        while let Some(journal_entry) = journal.next().await {
            let (journal_index, journal_entry) = journal_entry?;

            if let JournalEntry::Entry(journal_entry) = journal_entry {
                let (header, entry) = journal_entry.into_inner();
                match header {
                    // cancel uncompleted invocations
                    EnrichedEntryHeader::Call {
                        is_completed,
                        enrichment_result: Some(enrichment_result),
                    } if !is_completed => {
                        self.handle_outgoing_message(
                            OutboxMessage::InvocationTermination(InvocationTermination::cancel(
                                enrichment_result.invocation_id,
                            )),
                            effects,
                        );
                    }
                    EnrichedEntryHeader::Awakeable { is_completed }
                    | EnrichedEntryHeader::GetState { is_completed }
                        if !is_completed =>
                    {
                        resume_invocation |= Self::cancel_journal_entry_with(
                            invocation_id,
                            &invocation_status,
                            effects,
                            journal_index,
                            canceled_result.clone(),
                        );
                    }
                    EnrichedEntryHeader::Sleep { is_completed } if !is_completed => {
                        resume_invocation |= Self::cancel_journal_entry_with(
                            invocation_id,
                            &invocation_status,
                            effects,
                            journal_index,
                            canceled_result.clone(),
                        );

                        let_assert!(
                            Entry::Sleep(SleepEntry { wake_up_time, .. }) =
                                ProtobufRawEntryCodec::deserialize(EntryType::Sleep, entry)?
                        );

                        let timer_key = TimerKey {
                            invocation_uuid: invocation_id.invocation_uuid(),
                            journal_index,
                            timestamp: wake_up_time,
                        };

                        effects.delete_timer(timer_key);
                    }
                    header => {
                        assert!(
                            header.is_completed().unwrap_or(true),
                            "All non canceled journal entries must be completed."
                        );
                    }
                }
            }
        }

        Ok(resume_invocation)
    }

    fn cancel_journal_entry_with(
        invocation_id: InvocationId,
        invocation_status: &InvocationStatusProjection,
        effects: &mut Effects,
        journal_index: EntryIndex,
        canceled_result: CompletionResult,
    ) -> bool {
        match invocation_status {
            InvocationStatusProjection::Invoked => {
                Self::handle_completion_for_invoked(
                    invocation_id,
                    Completion::new(journal_index, canceled_result),
                    effects,
                );
                false
            }
            InvocationStatusProjection::Suspended(waiting_for_completed_entry) => {
                Self::handle_completion_for_suspended(
                    invocation_id,
                    Completion::new(journal_index, canceled_result),
                    waiting_for_completed_entry,
                    effects,
                )
            }
        }
    }

    async fn on_timer<State: StateReader + ReadOnlyIdempotencyTable>(
        &mut self,
        timer_value: TimerValue,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        let (key, value) = timer_value.into_inner();
        let invocation_uuid = key.invocation_uuid;
        let entry_index = key.journal_index;

        effects.delete_timer(key);

        match value {
            Timer::CompleteSleepEntry(partition_key) => {
                Self::handle_completion(
                    InvocationId::from_parts(partition_key, invocation_uuid),
                    Completion {
                        entry_index,
                        result: CompletionResult::Empty,
                    },
                    state,
                    effects,
                )
                .await
            }
            Timer::Invoke(mut service_invocation) => {
                // Remove the execution time from the service invocation request
                service_invocation.execution_time = None;

                // ServiceInvocations scheduled with a timer are always owned by the same partition processor
                // where the invocation should be executed
                self.handle_invoke(effects, state, service_invocation).await
            }
            Timer::CleanInvocationStatus(invocation_id) => {
                match Self::get_invocation_status_and_trace(state, &invocation_id, effects).await? {
                    InvocationStatus::Completed(CompletedInvocation {
                        invocation_target,
                        idempotency_key: Some(idempotency_key),
                        ..
                    }) => {
                        effects.free_invocation(invocation_id);
                        // Also cleanup the associated idempotency key
                        effects.delete_idempotency_id(IdempotencyId::combine(
                            invocation_id,
                            &invocation_target,
                            idempotency_key,
                        ));
                    }
                    InvocationStatus::Completed(_) => {
                        // Just free the invocation
                        effects.free_invocation(invocation_id);
                    }
                    InvocationStatus::Free => {
                        // Nothing to do
                    }
                    _ => {
                        panic!("Unexpected state. Trying to cleanup an invocation that did not complete yet.")
                    }
                };

                Ok(())
            }
        }
    }

    async fn try_invoker_effect<State: StateReader + ReadOnlyJournalTable>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        invoker_effect: InvokerEffect,
    ) -> Result<(), Error> {
        let status =
            Self::get_invocation_status_and_trace(state, &invoker_effect.invocation_id, effects)
                .await?;

        match status {
            InvocationStatus::Invoked(invocation_metadata) => {
                self.on_invoker_effect(effects, state, invoker_effect, invocation_metadata)
                    .await?
            }
            _ => {
                trace!("Received invoker effect for unknown service invocation. Ignoring the effect and aborting.");
                effects.abort_invocation(invoker_effect.invocation_id);
            }
        };

        Ok(())
    }

    async fn on_invoker_effect<State: StateReader + ReadOnlyJournalTable>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        InvokerEffect {
            invocation_id,
            kind,
        }: InvokerEffect,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        match kind {
            InvokerEffectKind::SelectedDeployment(deployment_id) => {
                effects.store_chosen_deployment(invocation_id, deployment_id, invocation_metadata);
            }
            InvokerEffectKind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    effects,
                    state,
                    invocation_id,
                    entry_index,
                    entry,
                    invocation_metadata,
                )
                .await?;
            }
            InvokerEffectKind::Suspended {
                waiting_for_completed_entries,
            } => {
                debug_assert!(
                    !waiting_for_completed_entries.is_empty(),
                    "Expecting at least one entry on which the invocation {invocation_id} is waiting."
                );
                let mut any_completed = false;
                for entry_index in &waiting_for_completed_entries {
                    if state
                        .is_entry_resumable(&invocation_id, *entry_index)
                        .await?
                    {
                        trace!(
                            rpc.service = %invocation_metadata.invocation_target.service_name(),
                            restate.invocation.id = %invocation_id,
                            "Resuming instead of suspending service because an awaited entry is completed/acked.");
                        any_completed = true;
                        break;
                    }
                }
                if any_completed {
                    effects.resume_service(invocation_id, invocation_metadata);
                } else {
                    effects.suspend_service(
                        invocation_id,
                        invocation_metadata,
                        waiting_for_completed_entries,
                    );
                }
            }
            InvokerEffectKind::End => {
                self.end_invocation(state, effects, invocation_id, invocation_metadata)
                    .await?;
            }
            InvokerEffectKind::Failed(e) => {
                self.fail_invocation(effects, invocation_id, invocation_metadata, e)
                    .await?;
            }
        }

        Ok(())
    }

    async fn end_invocation<State: StateReader + ReadOnlyJournalTable>(
        &mut self,
        state: &mut State,
        effects: &mut Effects,
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        let journal_length = invocation_metadata.journal_metadata.length;
        let completion_retention_time = invocation_metadata.completion_retention_time;

        self.notify_invocation_result(
            invocation_id,
            invocation_metadata.invocation_target.clone(),
            invocation_metadata.journal_metadata.span_context.clone(),
            invocation_metadata.timestamps.creation_time(),
            Ok(()),
            effects,
        );

        // Pop from inbox
        Self::try_pop_inbox(effects, &invocation_metadata.invocation_target);

        // If there are any response sinks, or we need to store back the completed status,
        //  we need to find the latest output entry
        if !invocation_metadata.response_sinks.is_empty() || !completion_retention_time.is_zero() {
            let result = if let Some(output_entry) = self
                .read_last_output_entry(state, &invocation_id, journal_length)
                .await?
            {
                ResponseResult::from(output_entry.result)
            } else {
                // We don't panic on this, although it indicates a bug at the moment.
                warn!("Invocation completed without an output entry. This is not supported yet.");
                return Ok(());
            };

            let idempotency_id =
                invocation_metadata
                    .idempotency_key
                    .as_ref()
                    .map(|idempotency_key| {
                        IdempotencyId::combine(
                            invocation_id,
                            &invocation_metadata.invocation_target,
                            idempotency_key.clone(),
                        )
                    });

            // Send responses out
            self.send_response_to_sinks(
                effects,
                &invocation_id,
                idempotency_id,
                invocation_metadata.response_sinks.clone(),
                result.clone(),
            );

            // Store the completed status, if needed
            if !completion_retention_time.is_zero() {
                let (completed_invocation, completion_retention_time) =
                    CompletedInvocation::from_in_flight_invocation_metadata(
                        invocation_metadata,
                        result,
                    );
                effects.store_completed_invocation(
                    invocation_id,
                    completion_retention_time,
                    completed_invocation,
                );
            }
        }

        // If no retention, immediately cleanup the invocation status
        if completion_retention_time.is_zero() {
            effects.free_invocation(invocation_id);
        }
        effects.drop_journal(invocation_id, journal_length);

        Ok(())
    }

    // This needs a different method because for built-in invocations we send back the output as soon as we have it.
    async fn end_built_in_invocation(
        &mut self,
        effects: &mut Effects,
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        self.notify_invocation_result(
            invocation_id,
            invocation_metadata.invocation_target.clone(),
            invocation_metadata.journal_metadata.span_context,
            invocation_metadata.timestamps.creation_time(),
            Ok(()),
            effects,
        );

        effects.free_invocation(invocation_id);
        effects.drop_journal(invocation_id, invocation_metadata.journal_metadata.length);
        Self::try_pop_inbox(effects, &invocation_metadata.invocation_target);

        Ok(())
    }

    async fn fail_invocation(
        &mut self,
        effects: &mut Effects,
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
        error: InvocationError,
    ) -> Result<(), Error> {
        let journal_length = invocation_metadata.journal_metadata.length;

        self.notify_invocation_result(
            invocation_id,
            invocation_metadata.invocation_target.clone(),
            invocation_metadata.journal_metadata.span_context.clone(),
            invocation_metadata.timestamps.creation_time(),
            Err((error.code(), error.to_string())),
            effects,
        );

        let response_result = ResponseResult::from(error);

        let idempotency_id = invocation_metadata
            .idempotency_key
            .as_ref()
            .map(|idempotency_key| {
                IdempotencyId::combine(
                    invocation_id,
                    &invocation_metadata.invocation_target,
                    idempotency_key.clone(),
                )
            });

        // Send responses out
        self.send_response_to_sinks(
            effects,
            &invocation_id,
            idempotency_id,
            invocation_metadata.response_sinks.clone(),
            response_result.clone(),
        );

        // Pop from inbox
        Self::try_pop_inbox(effects, &invocation_metadata.invocation_target);

        // Store the completed status or free it
        if !invocation_metadata.completion_retention_time.is_zero() {
            let (completed_invocation, completion_retention_time) =
                CompletedInvocation::from_in_flight_invocation_metadata(
                    invocation_metadata,
                    response_result,
                );
            effects.store_completed_invocation(
                invocation_id,
                completion_retention_time,
                completed_invocation,
            );
        } else {
            effects.free_invocation(invocation_id);
        }

        effects.drop_journal(invocation_id, journal_length);

        Ok(())
    }

    fn send_response_to_sinks(
        &mut self,
        effects: &mut Effects,
        invocation_id: &InvocationId,
        idempotency_id: Option<IdempotencyId>,
        response_sinks: impl IntoIterator<Item = ServiceInvocationResponseSink>,
        res: impl Into<ResponseResult>,
    ) {
        let result = res.into();
        for response_sink in response_sinks {
            let response_message = create_response_message(
                invocation_id,
                idempotency_id.clone(),
                response_sink,
                result.clone(),
            );
            match response_message {
                ResponseMessage::Outbox(outbox) => self.handle_outgoing_message(outbox, effects),
                ResponseMessage::Ingress(ingress) => self.ingress_response(ingress, effects),
            }
        }
    }

    fn try_pop_inbox(effects: &mut Effects, invocation_target: &InvocationTarget) {
        // Inbox exists only for exclusive handler cases
        if invocation_target.handler_ty() == Some(HandlerType::Exclusive) {
            effects.pop_inbox(invocation_target.as_keyed_service_id().expect(
                "When the handler type is Exclusive, the invocation target must have a key",
            ))
        }
    }

    async fn handle_journal_entry<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        mut journal_entry: EnrichedRawEntry,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        debug_assert_eq!(
            entry_index, invocation_metadata.journal_metadata.length,
            "Expect to receive next journal entry for {invocation_id}"
        );

        match journal_entry.header() {
            // nothing to do
            EnrichedEntryHeader::Input { .. } => {}
            EnrichedEntryHeader::Output { .. } => {
                // Just store it, on End we send back the responses
            }
            EnrichedEntryHeader::GetState { is_completed, .. } => {
                if !is_completed {
                    let_assert!(
                        Entry::GetState(GetStateEntry { key, .. }) =
                            journal_entry.deserialize_entry_ref::<Codec>()?
                    );

                    if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        // Load state and write completion
                        let value = state.load_state(&service_id, &key).await?;
                        let completion_result = value
                            .map(CompletionResult::Success)
                            .unwrap_or(CompletionResult::Empty);
                        Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                        effects.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, completion_result),
                        );
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no state",
                            journal_entry.header().as_entry_type()
                        );
                        effects.forward_completion(
                            invocation_id,
                            Completion::new(entry_index, CompletionResult::Empty),
                        );
                    }
                }
            }
            EnrichedEntryHeader::SetState { .. } => {
                let_assert!(
                    Entry::SetState(SetStateEntry { key, value }) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );

                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    effects.set_state(
                        service_id,
                        invocation_id,
                        invocation_metadata.journal_metadata.span_context.clone(),
                        key,
                        value,
                    );
                } else {
                    warn!(
                        "Trying to process entry {} for a target that has no state",
                        journal_entry.header().as_entry_type()
                    );
                }
            }
            EnrichedEntryHeader::ClearState { .. } => {
                let_assert!(
                    Entry::ClearState(ClearStateEntry { key }) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );

                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    effects.clear_state(
                        service_id,
                        invocation_id,
                        invocation_metadata.journal_metadata.span_context.clone(),
                        key,
                    );
                } else {
                    warn!(
                        "Trying to process entry {} for a target that has no state",
                        journal_entry.header().as_entry_type()
                    );
                }
            }
            EnrichedEntryHeader::ClearAllState { .. } => {
                if let Some(service_id) =
                    invocation_metadata.invocation_target.as_keyed_service_id()
                {
                    effects.clear_all_state(
                        service_id,
                        invocation_id,
                        invocation_metadata.journal_metadata.span_context.clone(),
                    );
                } else {
                    warn!(
                        "Trying to process entry {} for a target that has no state",
                        journal_entry.header().as_entry_type()
                    );
                }
            }
            EnrichedEntryHeader::GetStateKeys { is_completed, .. } => {
                if !is_completed {
                    // Load state and write completion
                    let value = if let Some(service_id) =
                        invocation_metadata.invocation_target.as_keyed_service_id()
                    {
                        state.load_state_keys(&service_id).await?
                    } else {
                        warn!(
                            "Trying to process entry {} for a target that has no state",
                            journal_entry.header().as_entry_type()
                        );
                        vec![]
                    };

                    let completion_result = Codec::serialize_get_state_keys_completion(value);
                    Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                    // We can already forward the completion
                    effects.forward_completion(
                        invocation_id,
                        Completion::new(entry_index, completion_result),
                    );
                }
            }
            EnrichedEntryHeader::Sleep { is_completed, .. } => {
                debug_assert!(!is_completed, "Sleep entry must not be completed.");
                let_assert!(
                    Entry::Sleep(SleepEntry { wake_up_time, .. }) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );
                effects.register_timer(
                    TimerValue::new_sleep(
                        invocation_id,
                        MillisSinceEpoch::new(wake_up_time),
                        entry_index,
                    ),
                    invocation_metadata.journal_metadata.span_context.clone(),
                );
            }
            EnrichedEntryHeader::Call {
                enrichment_result, ..
            } => {
                if let Some(CallEnrichmentResult {
                    span_context,
                    invocation_id: callee_invocation_id,
                    invocation_target: callee_invocation_target,
                    ..
                }) = enrichment_result
                {
                    let_assert!(
                        Entry::Call(InvokeEntry { request, .. }) =
                            journal_entry.deserialize_entry_ref::<Codec>()?
                    );

                    let service_invocation = Self::create_service_invocation(
                        *callee_invocation_id,
                        callee_invocation_target.clone(),
                        request,
                        Source::Service(
                            invocation_id,
                            invocation_metadata.invocation_target.clone(),
                        ),
                        Some((invocation_id, entry_index)),
                        span_context.clone(),
                        None,
                    );
                    self.handle_outgoing_message(
                        OutboxMessage::ServiceInvocation(service_invocation),
                        effects,
                    );
                } else {
                    // no action needed for an invoke entry that has been completed by the deployment
                }
            }
            EnrichedEntryHeader::OneWayCall {
                enrichment_result, ..
            } => {
                let CallEnrichmentResult {
                    invocation_id: callee_invocation_id,
                    invocation_target: callee_invocation_target,
                    span_context,
                    ..
                } = enrichment_result;

                let_assert!(
                    Entry::OneWayCall(OneWayCallEntry {
                        request,
                        invoke_time
                    }) = journal_entry.deserialize_entry_ref::<Codec>()?
                );

                // 0 is equal to not set, meaning execute now
                let delay = if invoke_time == 0 {
                    None
                } else {
                    Some(MillisSinceEpoch::new(invoke_time))
                };

                let service_invocation = Self::create_service_invocation(
                    *callee_invocation_id,
                    callee_invocation_target.clone(),
                    request,
                    Source::Service(invocation_id, invocation_metadata.invocation_target.clone()),
                    None,
                    span_context.clone(),
                    delay,
                );

                let pointer_span_id = match span_context.span_cause() {
                    Some(SpanRelationCause::Linked(_, span_id)) => Some(*span_id),
                    _ => None,
                };

                effects.trace_background_invoke(
                    *callee_invocation_id,
                    callee_invocation_target.clone(),
                    invocation_metadata.journal_metadata.span_context.clone(),
                    pointer_span_id,
                );

                self.handle_outgoing_message(
                    OutboxMessage::ServiceInvocation(service_invocation),
                    effects,
                );
            }
            EnrichedEntryHeader::Awakeable { is_completed, .. } => {
                debug_assert!(!is_completed, "Awakeable entry must not be completed.");
                // Check the awakeable_completion_received_before_entry test in state_machine/server for more details

                // If completion is already here, let's merge it and forward it.
                if let Some(completion_result) = state
                    .load_completion_result(&invocation_id, entry_index)
                    .await?
                {
                    Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                    effects.forward_completion(
                        invocation_id,
                        Completion::new(entry_index, completion_result),
                    );
                }
            }
            EnrichedEntryHeader::CompleteAwakeable {
                enrichment_result:
                    AwakeableEnrichmentResult {
                        invocation_id,
                        entry_index,
                    },
                ..
            } => {
                let_assert!(
                    Entry::CompleteAwakeable(entry) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );

                self.handle_outgoing_message(
                    OutboxMessage::from_awakeable_completion(
                        *invocation_id,
                        *entry_index,
                        entry.result.into(),
                    ),
                    effects,
                );
            }
            EnrichedEntryHeader::Run { .. } | EnrichedEntryHeader::Custom { .. } => {
                // We just store it
            }
        }

        effects.append_journal_entry(
            invocation_id,
            InvocationStatus::Invoked(invocation_metadata),
            entry_index,
            journal_entry,
        );
        effects.send_stored_ack_to_invoker(invocation_id, entry_index);

        Ok(())
    }

    async fn handle_completion<State: StateReader>(
        invocation_id: InvocationId,
        completion: Completion,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        let status = Self::get_invocation_status_and_trace(state, &invocation_id, effects).await?;

        match status {
            InvocationStatus::Invoked(_) => {
                Self::handle_completion_for_invoked(invocation_id, completion, effects);
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_completed_entries,
            } => {
                if Self::handle_completion_for_suspended(
                    invocation_id,
                    completion,
                    &waiting_for_completed_entries,
                    effects,
                ) {
                    effects.resume_service(invocation_id, metadata);
                }
            }
            _ => {
                debug!(
                    restate.invocation.id = %invocation_id,
                    ?completion,
                    "Ignoring completion for invocation that is no longer running."
                )
            }
        }

        Ok(())
    }

    fn handle_completion_for_suspended(
        invocation_id: InvocationId,
        completion: Completion,
        waiting_for_completed_entries: &HashSet<EntryIndex>,
        effects: &mut Effects,
    ) -> bool {
        let resume_invocation = waiting_for_completed_entries.contains(&completion.entry_index);
        effects.store_completion(invocation_id, completion);

        resume_invocation
    }

    fn handle_completion_for_invoked(
        invocation_id: InvocationId,
        completion: Completion,
        effects: &mut Effects,
    ) {
        effects.store_completion(invocation_id, completion.clone());
        effects.forward_completion(invocation_id, completion);
    }

    async fn read_last_output_entry<State: ReadOnlyJournalTable>(
        &mut self,
        state: &mut State,
        invocation_id: &InvocationId,
        journal_length: EntryIndex,
    ) -> Result<Option<OutputEntry>, Error> {
        // Find last output entry
        let mut output_entry = None;
        for i in (0..journal_length).rev() {
            if let JournalEntry::Entry(e) = state
                .get_journal_entry(invocation_id, i)
                .await?
                .unwrap_or_else(|| panic!("There should be a journal entry at index {}", i))
            {
                if e.ty() == EntryType::Output {
                    output_entry = Some(e);
                    break;
                }
            }
        }

        output_entry
            .map(|enriched_entry| {
                let_assert!(Entry::Output(e) = enriched_entry.deserialize_entry_ref::<Codec>()?);
                Ok(e)
            })
            .transpose()
    }

    async fn handle_deterministic_built_in_service_invocation(
        &mut self,
        invocation: ServiceInvocation,
        effects: &mut Effects,
    ) {
        // Invoke built-in service
        for effect in deterministic::ServiceInvoker::invoke(
            &invocation.invocation_id,
            &invocation.invocation_target,
            &invocation.span_context,
            invocation.response_sink.as_ref(),
            invocation.argument.clone(),
        )
        .await
        {
            match effect {
                deterministic::Effect::OutboxMessage(outbox_message) => {
                    self.handle_outgoing_message(outbox_message, effects)
                }
                deterministic::Effect::IngressResponse(ingress_response) => {
                    self.ingress_response(ingress_response, effects);
                }
            }
        }
    }

    fn notify_invocation_result(
        &mut self,
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        span_context: ServiceInvocationSpanContext,
        creation_time: MillisSinceEpoch,
        result: Result<(), (InvocationErrorCode, String)>,
        effects: &mut Effects,
    ) {
        effects.trace_invocation_result(
            invocation_id,
            invocation_target,
            span_context,
            creation_time,
            result,
        );
    }

    fn handle_outgoing_message(&mut self, message: OutboxMessage, effects: &mut Effects) {
        // TODO Here we could add an optimization to immediately execute outbox message command
        //  for partition_key within the range of this PP, but this is problematic due to how we tie
        //  the effects buffer with tracing. Once we solve that, we could implement that by roughly uncommenting this code :)
        //  if self.partition_key_range.contains(&message.partition_key()) {
        //             // We can process this now!
        //             let command = message.to_command();
        //             return self.on_apply(
        //                 command,
        //                 effects,
        //                 state
        //             ).await
        //         }
        effects.enqueue_into_outbox(self.outbox_seq_number, message);
        self.outbox_seq_number += 1;
    }

    fn ingress_response(&mut self, ingress_response: IngressResponse, effects: &mut Effects) {
        effects.send_ingress_response(ingress_response);
    }

    // Gets the invocation status from storage and also adds tracing info to effects
    async fn get_invocation_status_and_trace<State: StateReader>(
        state: &mut State,
        invocation_id: &InvocationId,
        effects: &mut Effects,
    ) -> Result<InvocationStatus, Error> {
        effects.set_related_invocation_id(invocation_id);
        let status = state.get_invocation_status(invocation_id).await?;
        if let Some(invocation_target) = status.invocation_target() {
            effects.set_related_invocation_target(invocation_target);
        }
        if let Some(journal_metadata) = status.get_journal_metadata() {
            effects.set_parent_span_context(&journal_metadata.span_context)
        }
        Ok(status)
    }

    #[allow(clippy::too_many_arguments)]
    fn create_service_invocation(
        invocation_id: InvocationId,
        invocation_target: InvocationTarget,
        invoke_request: InvokeRequest,
        source: Source,
        response_target: Option<(InvocationId, EntryIndex)>,
        span_context: ServiceInvocationSpanContext,
        execution_time: Option<MillisSinceEpoch>,
    ) -> ServiceInvocation {
        let InvokeRequest { parameter, .. } = invoke_request;

        let response_sink = if let Some((caller, entry_index)) = response_target {
            Some(ServiceInvocationResponseSink::PartitionProcessor {
                caller,
                entry_index,
            })
        } else {
            None
        };

        ServiceInvocation {
            invocation_id,
            invocation_target,
            argument: parameter,
            source,
            response_sink,
            span_context,
            headers: vec![],
            execution_time,
            idempotency: None,
        }
    }
}

/// Projected [`InvocationStatus`] for cancellation purposes.
enum InvocationStatusProjection {
    Invoked,
    Suspended(HashSet<EntryIndex>),
}

#[cfg(test)]
mod tests;
