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
use std::collections::HashSet;

use crate::partition::services::deterministic;
use crate::partition::state_machine::effects::Effects;
use crate::partition::types::{
    create_response_message, InvokerEffect, InvokerEffectKind, OutboxMessageExt, ResponseMessage,
};
use assert2::let_assert;
use bytes::Bytes;
use bytestring::ByteString;
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
    EntryIndex, FullInvocationId, IdempotencyId, InvocationId, InvocationUuid, PartitionKey,
    ServiceId, WithPartitionKey,
};
use restate_types::ingress::IngressResponse;
use restate_types::invocation::{
    InvocationResponse, InvocationTermination, MaybeFullInvocationId, ResponseResult,
    ServiceInvocation, ServiceInvocationResponseSink, ServiceInvocationSpanContext, Source,
    SpanRelation, SpanRelationCause, TerminationFlavor,
};
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry, InvokeEnrichmentResult,
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
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Deref, RangeInclusive};
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
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
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
            Command::InvokerEffect(effect) => {
                let (related_sid, span_relation) =
                    self.try_invoker_effect(effects, state, effect).await?;
                Ok((Some(related_sid), span_relation))
            }
            Command::TruncateOutbox(index) => {
                effects.truncate_outbox(index);
                Ok((None, SpanRelation::None))
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
                Ok((None, SpanRelation::None))
            }
            Command::ScheduleTimer(timer) => {
                effects.register_timer(timer, Default::default());
                Ok((None, SpanRelation::None))
            }
        }
    }

    async fn handle_invoke<State: StateReader + ReadOnlyIdempotencyTable>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        service_invocation: ServiceInvocation,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        debug_assert!(
            self.partition_key_range.contains(&service_invocation.partition_key()),
                "Service invocation with partition key '{}' has been delivered to a partition processor with key range '{:?}'. This indicates a bug.",
                service_invocation.partition_key(),
                self.partition_key_range);

        // If an idempotency key is set, handle idempotency
        if let Some(idempotency) = &service_invocation.idempotency {
            let idempotency_id = IdempotencyId::new(
                service_invocation.fid.service_id.service_name.clone(),
                // The service_id.key will always be the idempotency key now,
                // until we get rid of that field for regular services with https://github.com/restatedev/restate/issues/1329
                Some(service_invocation.fid.service_id.key.clone()),
                service_invocation.method_name.clone(),
                idempotency.key.clone(),
            );
            if self
                .try_resolve_idempotent_request(
                    effects,
                    state,
                    &idempotency_id,
                    &InvocationId::from(&service_invocation.fid),
                    service_invocation.response_sink.as_ref(),
                )
                .await?
            {
                // Invocation was either resolved, or the sink was enqueued. Nothing else to do here.
                return Ok((
                    Some(service_invocation.fid),
                    service_invocation.span_context.as_parent(),
                ));
            }
            // Idempotent invocation needs to be processed for the first time, let's roll!
            effects
                .store_idempotency_id(idempotency_id, InvocationId::from(&service_invocation.fid));
        }

        // If an execution_time is set, we schedule the invocation to be processed later
        if let Some(execution_time) = service_invocation.execution_time {
            let span_context = service_invocation.span_context.clone();
            effects.register_timer(
                TimerValue::new_invoke(
                    service_invocation.fid.clone(),
                    execution_time,
                    // This entry_index here makes little sense
                    0,
                    service_invocation,
                ),
                span_context,
            );
            // The span will be created later on invocation
            return Ok((None, SpanRelation::None));
        }

        let service_status = state
            .get_virtual_object_status(&service_invocation.fid.service_id)
            .await?;

        let fid = service_invocation.fid.clone();
        let span_relation = service_invocation.span_context.as_parent();

        if deterministic::ServiceInvoker::is_supported(fid.service_id.service_name.deref()) {
            self.handle_deterministic_built_in_service_invocation(service_invocation, effects)
                .await;
        } else if let VirtualObjectStatus::Unlocked = service_status {
            effects.invoke_service(service_invocation);
        } else {
            let inbox_seq_number = self.enqueue_into_inbox(
                effects,
                InboxEntry::Invocation(service_invocation.fid.clone()),
            );
            effects.store_inboxed_invocation(
                InvocationId::from(&service_invocation.fid),
                InboxedInvocation::from_service_invocation(service_invocation, inbox_seq_number),
            );
        }
        Ok((Some(fid), span_relation))
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
                        response_sink.cloned(),
                        completed.response_result,
                    );
                }
                InvocationStatus::Free => self.send_response_to_sinks(
                    effects,
                    caller_id,
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
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let service_status = state
            .get_virtual_object_status(&mutation.component_id)
            .await?;

        match service_status {
            VirtualObjectStatus::Locked(_) => {
                self.enqueue_into_inbox(effects, InboxEntry::StateMutation(mutation));
            }
            VirtualObjectStatus::Unlocked => effects.apply_state_mutation(mutation),
        }

        Ok((None, SpanRelation::None))
    }

    async fn try_built_in_invoker_effect<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        nbis_effects: BuiltinServiceEffects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let (full_invocation_id, nbis_effects) = nbis_effects.into_inner();
        let invocation_status = state
            .get_invocation_status(&InvocationId::from(&full_invocation_id))
            .await?;

        match invocation_status {
            InvocationStatus::Invoked(invocation_metadata) => {
                let span_relation = invocation_metadata
                    .journal_metadata
                    .span_context
                    .as_parent();

                for nbis_effect in nbis_effects {
                    self.on_built_in_invoker_effect(
                        effects,
                        &full_invocation_id,
                        &invocation_metadata,
                        nbis_effect,
                    )
                    .await?
                }
                Ok((Some(full_invocation_id), span_relation))
            }
            _ => {
                trace!(
                    "Received built in invoker effect for unknown invocation {}. Ignoring it.",
                    full_invocation_id
                );
                Ok((Some(full_invocation_id), SpanRelation::None))
            }
        }
    }

    async fn on_built_in_invoker_effect(
        &mut self,
        effects: &mut Effects,
        full_invocation_id: &FullInvocationId,
        invocation_metadata: &InFlightInvocationMetadata,
        nbis_effect: BuiltinServiceEffect,
    ) -> Result<(), Error> {
        match nbis_effect {
            BuiltinServiceEffect::SetState { key, value } => {
                effects.set_state(
                    full_invocation_id.service_id.clone(),
                    full_invocation_id.clone().into(),
                    invocation_metadata.journal_metadata.span_context.clone(),
                    Bytes::from(key.into_owned()),
                    value,
                );
            }
            BuiltinServiceEffect::ClearState(key) => {
                effects.clear_state(
                    full_invocation_id.service_id.clone(),
                    full_invocation_id.clone().into(),
                    invocation_metadata.journal_metadata.span_context.clone(),
                    Bytes::from(key.into_owned()),
                );
            }
            BuiltinServiceEffect::OutboxMessage(msg) => {
                self.handle_outgoing_message(msg, effects);
            }
            BuiltinServiceEffect::End(None) => {
                self.end_built_in_invocation(
                    effects,
                    full_invocation_id.clone(),
                    invocation_metadata.clone(),
                )
                .await?
            }
            BuiltinServiceEffect::End(Some(e)) => {
                self.fail_invocation(
                    effects,
                    InvocationId::from(full_invocation_id),
                    invocation_metadata.clone(),
                    e,
                )
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
            maybe_fid,
            flavor: termination_flavor,
        }: InvocationTermination,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        match termination_flavor {
            TerminationFlavor::Kill => self.try_kill_invocation(maybe_fid, state, effects).await,
            TerminationFlavor::Cancel => {
                self.try_cancel_invocation(maybe_fid, state, effects).await
            }
        }
    }

    async fn try_kill_invocation<State: StateReader>(
        &mut self,
        maybe_fid: MaybeFullInvocationId,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let invocation_id: InvocationId = maybe_fid.clone().into();
        let status = state.get_invocation_status(&invocation_id).await?;

        match status {
            InvocationStatus::Invoked(metadata) | InvocationStatus::Suspended { metadata, .. } => {
                let related_span = metadata.journal_metadata.span_context.as_parent();
                let fid = FullInvocationId::combine(metadata.service_id.clone(), invocation_id);

                self.kill_invocation(fid.clone(), metadata, state, effects)
                    .await?;

                Ok((Some(fid), related_span))
            }
            InvocationStatus::Inboxed(inboxed) => self.terminate_inboxed_invocation(
                TerminationFlavor::Kill,
                FullInvocationId::combine(inboxed.service_id.clone(), invocation_id),
                inboxed,
                effects,
            ),
            _ => {
                trace!("Received kill command for unknown invocation with id '{maybe_fid}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                if let MaybeFullInvocationId::Full(fid) = maybe_fid {
                    effects.abort_invocation(fid.clone());
                    Ok((Some(fid), SpanRelation::None))
                } else {
                    Ok((None, SpanRelation::None))
                }
            }
        }
    }

    async fn try_cancel_invocation<State: StateReader>(
        &mut self,
        maybe_fid: MaybeFullInvocationId,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let invocation_id: InvocationId = maybe_fid.clone().into();
        let status = state.get_invocation_status(&invocation_id).await?;

        match status {
            InvocationStatus::Invoked(metadata) => {
                let related_span = metadata.journal_metadata.span_context.as_parent();
                let fid = FullInvocationId::combine(metadata.service_id.clone(), invocation_id);

                self.cancel_journal_leaves(
                    fid.clone(),
                    InvocationStatusProjection::Invoked,
                    metadata.journal_metadata.length,
                    state,
                    effects,
                )
                .await?;

                Ok((Some(fid), related_span))
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_completed_entries,
            } => {
                let related_span = metadata.journal_metadata.span_context.as_parent();
                let fid = FullInvocationId::combine(metadata.service_id.clone(), invocation_id);

                if self
                    .cancel_journal_leaves(
                        fid.clone(),
                        InvocationStatusProjection::Suspended(waiting_for_completed_entries),
                        metadata.journal_metadata.length,
                        state,
                        effects,
                    )
                    .await?
                {
                    effects.resume_service(InvocationId::from(&fid), metadata);
                }

                Ok((Some(fid), related_span))
            }
            InvocationStatus::Inboxed(inboxed) => self.terminate_inboxed_invocation(
                TerminationFlavor::Cancel,
                FullInvocationId::combine(inboxed.service_id.clone(), invocation_id),
                inboxed,
                effects,
            ),
            _ => {
                trace!("Received cancel command for unknown invocation with id '{maybe_fid}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                if let MaybeFullInvocationId::Full(fid) = maybe_fid {
                    effects.abort_invocation(fid.clone());
                    Ok((Some(fid), SpanRelation::None))
                } else {
                    Ok((None, SpanRelation::None))
                }
            }
        }
    }

    fn terminate_inboxed_invocation(
        &mut self,
        termination_flavor: TerminationFlavor,
        fid: FullInvocationId,
        inboxed_invocation: InboxedInvocation,
        effects: &mut Effects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let error = match termination_flavor {
            TerminationFlavor::Kill => KILLED_INVOCATION_ERROR,
            TerminationFlavor::Cancel => CANCELED_INVOCATION_ERROR,
        };

        let InboxedInvocation {
            inbox_sequence_number,
            response_sinks,
            handler_name,
            span_context,
            ..
        } = inboxed_invocation;

        let parent_span = span_context.as_parent();

        // Reply back to callers with error, and publish end trace
        self.send_response_to_sinks(effects, &InvocationId::from(&fid), response_sinks, &error);
        self.notify_invocation_result(
            &fid,
            handler_name,
            span_context,
            MillisSinceEpoch::now(),
            Err((error.code(), error.to_string())),
            effects,
        );

        // Delete inbox entry and invocation status.
        effects.delete_inbox_entry(fid.service_id.clone(), inbox_sequence_number);
        effects.free_invocation(InvocationId::from(&fid));

        Ok((Some(fid), parent_span))
    }

    async fn kill_invocation<State: StateReader>(
        &mut self,
        full_invocation_id: FullInvocationId,
        metadata: InFlightInvocationMetadata,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        self.kill_child_invocations(
            &InvocationId::from(full_invocation_id.clone()),
            state,
            effects,
            metadata.journal_metadata.length,
        )
        .await?;

        self.fail_invocation(
            effects,
            InvocationId::from(&full_invocation_id),
            metadata,
            KILLED_INVOCATION_ERROR,
        )
        .await?;
        effects.abort_invocation(full_invocation_id);
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
                    EnrichedEntryHeader::Invoke {
                        is_completed,
                        enrichment_result: Some(enrichment_result),
                    } if !is_completed => {
                        let target_fid = FullInvocationId::new(
                            enrichment_result.service_name,
                            enrichment_result.service_key,
                            enrichment_result.invocation_uuid,
                        );
                        self.handle_outgoing_message(
                            OutboxMessage::InvocationTermination(InvocationTermination::kill(
                                target_fid,
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
        full_invocation_id: FullInvocationId,
        invocation_status: InvocationStatusProjection,
        journal_length: EntryIndex,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<bool, Error> {
        let invocation_id = InvocationId::from(&full_invocation_id);
        let mut journal = pin!(state.get_journal(&invocation_id, journal_length));

        let canceled_result = CompletionResult::from(&CANCELED_INVOCATION_ERROR);

        let mut resume_invocation = false;

        while let Some(journal_entry) = journal.next().await {
            let (journal_index, journal_entry) = journal_entry?;

            if let JournalEntry::Entry(journal_entry) = journal_entry {
                let (header, entry) = journal_entry.into_inner();
                match header {
                    // cancel uncompleted invocations
                    EnrichedEntryHeader::Invoke {
                        is_completed,
                        enrichment_result: Some(enrichment_result),
                    } if !is_completed => {
                        let target_fid = FullInvocationId::new(
                            enrichment_result.service_name,
                            enrichment_result.service_key,
                            enrichment_result.invocation_uuid,
                        );

                        self.handle_outgoing_message(
                            OutboxMessage::InvocationTermination(InvocationTermination::cancel(
                                target_fid,
                            )),
                            effects,
                        );
                    }
                    EnrichedEntryHeader::Awakeable { is_completed }
                    | EnrichedEntryHeader::GetState { is_completed }
                        if !is_completed =>
                    {
                        resume_invocation |= Self::cancel_journal_entry_with(
                            full_invocation_id.clone(),
                            &invocation_status,
                            effects,
                            journal_index,
                            canceled_result.clone(),
                        );
                    }
                    EnrichedEntryHeader::Sleep { is_completed } if !is_completed => {
                        resume_invocation |= Self::cancel_journal_entry_with(
                            full_invocation_id.clone(),
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
                            invocation_uuid: full_invocation_id.invocation_uuid,
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
        full_invocation_id: FullInvocationId,
        invocation_status: &InvocationStatusProjection,
        effects: &mut Effects,
        journal_index: EntryIndex,
        canceled_result: CompletionResult,
    ) -> bool {
        match invocation_status {
            InvocationStatusProjection::Invoked => {
                Self::handle_completion_for_invoked(
                    full_invocation_id,
                    Completion::new(journal_index, canceled_result),
                    effects,
                );
                false
            }
            InvocationStatusProjection::Suspended(waiting_for_completed_entry) => {
                Self::handle_completion_for_suspended(
                    full_invocation_id,
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
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let (key, value) = timer_value.into_inner();
        let invocation_uuid = key.invocation_uuid;
        let entry_index = key.journal_index;

        effects.delete_timer(key);

        match value {
            Timer::CompleteSleepEntry(service_id) => {
                Self::handle_completion(
                    MaybeFullInvocationId::Full(FullInvocationId {
                        service_id,
                        invocation_uuid,
                    }),
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
                match state.get_invocation_status(&invocation_id).await? {
                    InvocationStatus::Completed(CompletedInvocation {
                        service_id,
                        handler,
                        idempotency_key: Some(idempotency_key),
                        ..
                    }) => {
                        effects.free_invocation(invocation_id);
                        // Also cleanup the associated idempotency key
                        effects.delete_idempotency_id(IdempotencyId::new(
                            service_id.service_name,
                            // The service_id.key will always be the idempotency key now,
                            // until we get rid of that field for regular services with https://github.com/restatedev/restate/issues/1329
                            Some(service_id.key),
                            handler,
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

                Ok((None, SpanRelation::None))
            }
        }
    }

    async fn try_invoker_effect<State: StateReader + ReadOnlyJournalTable>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        invoker_effect: InvokerEffect,
    ) -> Result<(FullInvocationId, SpanRelation), Error> {
        let invocation_id = InvocationId::from(&invoker_effect.full_invocation_id);
        let status = state.get_invocation_status(&invocation_id).await?;

        match status {
            InvocationStatus::Invoked(invocation_metadata) => {
                self.on_invoker_effect(effects, state, invoker_effect, invocation_metadata)
                    .await
            }
            _ => {
                trace!("Received invoker effect for unknown service invocation. Ignoring the effect and aborting.");
                effects.abort_invocation(invoker_effect.full_invocation_id.clone());
                Ok((invoker_effect.full_invocation_id, SpanRelation::None))
            }
        }
    }

    async fn on_invoker_effect<State: StateReader + ReadOnlyJournalTable>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        InvokerEffect {
            full_invocation_id,
            kind,
        }: InvokerEffect,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(FullInvocationId, SpanRelation), Error> {
        let related_sid = full_invocation_id.clone();
        let span_relation = invocation_metadata
            .journal_metadata
            .span_context
            .as_parent();

        match kind {
            InvokerEffectKind::SelectedDeployment(deployment_id) => {
                effects.store_chosen_deployment(
                    full_invocation_id.into(),
                    deployment_id,
                    invocation_metadata,
                );
            }
            InvokerEffectKind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    effects,
                    state,
                    full_invocation_id,
                    entry_index,
                    entry,
                    invocation_metadata,
                )
                .await?;
            }
            InvokerEffectKind::Suspended {
                waiting_for_completed_entries,
            } => {
                let invocation_id = InvocationId::from(&full_invocation_id);
                debug_assert!(
                    !waiting_for_completed_entries.is_empty(),
                    "Expecting at least one entry on which the invocation {full_invocation_id} is waiting."
                );
                let mut any_completed = false;
                for entry_index in &waiting_for_completed_entries {
                    if state
                        .is_entry_resumable(&invocation_id, *entry_index)
                        .await?
                    {
                        trace!(
                            rpc.service = %full_invocation_id.service_id.service_name,
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
                self.end_invocation(
                    state,
                    effects,
                    InvocationId::from(&full_invocation_id),
                    invocation_metadata,
                )
                .await?;
            }
            InvokerEffectKind::Failed(e) => {
                self.fail_invocation(
                    effects,
                    InvocationId::from(&full_invocation_id),
                    invocation_metadata,
                    e,
                )
                .await?;
            }
        }

        Ok((related_sid, span_relation))
    }

    async fn end_invocation<State: StateReader + ReadOnlyJournalTable>(
        &mut self,
        state: &mut State,
        effects: &mut Effects,
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        let service_id = invocation_metadata.service_id.clone();
        let journal_length = invocation_metadata.journal_metadata.length;
        let completion_retention_time = invocation_metadata.completion_retention_time;

        self.notify_invocation_result(
            &FullInvocationId::combine(
                invocation_metadata.service_id.clone(),
                invocation_id.clone(),
            ),
            invocation_metadata.method.clone(),
            invocation_metadata.journal_metadata.span_context.clone(),
            invocation_metadata.timestamps.creation_time(),
            Ok(()),
            effects,
        );

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

            // Send responses out
            self.send_response_to_sinks(
                effects,
                &invocation_id,
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
                    invocation_id.clone(),
                    completion_retention_time,
                    completed_invocation,
                );
            }
        }

        // If no retention, immediately cleanup the invocation status
        if completion_retention_time.is_zero() {
            effects.free_invocation(invocation_id.clone());
        }
        effects.drop_journal(invocation_id, journal_length);
        effects.pop_inbox(service_id);

        Ok(())
    }

    // This needs a different method because for built-in invocations we send back the output as soon as we have it.
    async fn end_built_in_invocation(
        &mut self,
        effects: &mut Effects,
        full_invocation_id: FullInvocationId,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        let invocation_id = InvocationId::from(&full_invocation_id);

        self.notify_invocation_result(
            &full_invocation_id,
            invocation_metadata.method,
            invocation_metadata.journal_metadata.span_context,
            invocation_metadata.timestamps.creation_time(),
            Ok(()),
            effects,
        );

        effects.free_invocation(invocation_id.clone());
        effects.drop_journal(invocation_id, invocation_metadata.journal_metadata.length);
        effects.pop_inbox(full_invocation_id.service_id);

        Ok(())
    }

    async fn fail_invocation(
        &mut self,
        effects: &mut Effects,
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
        error: InvocationError,
    ) -> Result<(), Error> {
        let service_id = invocation_metadata.service_id.clone();
        let journal_length = invocation_metadata.journal_metadata.length;

        self.notify_invocation_result(
            &FullInvocationId::combine(
                invocation_metadata.service_id.clone(),
                invocation_id.clone(),
            ),
            invocation_metadata.method.clone(),
            invocation_metadata.journal_metadata.span_context.clone(),
            invocation_metadata.timestamps.creation_time(),
            Err((error.code(), error.to_string())),
            effects,
        );

        let response_result = ResponseResult::from(error);

        // Send responses out
        self.send_response_to_sinks(
            effects,
            &invocation_id,
            invocation_metadata.response_sinks.clone(),
            response_result.clone(),
        );

        // Store the completed status or free it
        if !invocation_metadata.completion_retention_time.is_zero() {
            let (completed_invocation, completion_retention_time) =
                CompletedInvocation::from_in_flight_invocation_metadata(
                    invocation_metadata,
                    response_result,
                );
            effects.store_completed_invocation(
                invocation_id.clone(),
                completion_retention_time,
                completed_invocation,
            );
        } else {
            effects.free_invocation(invocation_id.clone());
        }

        effects.drop_journal(invocation_id, journal_length);
        effects.pop_inbox(service_id);

        Ok(())
    }

    fn send_response_to_sinks(
        &mut self,
        effects: &mut Effects,
        invocation_id: &InvocationId,
        response_sinks: impl IntoIterator<Item = ServiceInvocationResponseSink>,
        res: impl Into<ResponseResult>,
    ) {
        let result = res.into();
        for response_sink in response_sinks {
            let response_message =
                create_response_message(invocation_id, response_sink, result.clone());
            match response_message {
                ResponseMessage::Outbox(outbox) => self.outbox_message(outbox, effects),
                ResponseMessage::Ingress(ingress) => self.ingress_response(ingress, effects),
            }
        }
    }

    async fn handle_journal_entry<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        full_invocation_id: FullInvocationId,
        entry_index: EntryIndex,
        mut journal_entry: EnrichedRawEntry,
        invocation_metadata: InFlightInvocationMetadata,
    ) -> Result<(), Error> {
        debug_assert_eq!(
            entry_index, invocation_metadata.journal_metadata.length,
            "Expect to receive next journal entry for {full_invocation_id}"
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

                    // Load state and write completion
                    let value = state
                        .load_state(&full_invocation_id.service_id, &key)
                        .await?;
                    let completion_result = value
                        .map(CompletionResult::Success)
                        .unwrap_or(CompletionResult::Empty);
                    Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                    // We can already forward the completion
                    effects.forward_completion(
                        full_invocation_id.clone(),
                        Completion::new(entry_index, completion_result),
                    );
                }
            }
            EnrichedEntryHeader::SetState { .. } => {
                let_assert!(
                    Entry::SetState(SetStateEntry { key, value }) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );

                effects.set_state(
                    full_invocation_id.service_id.clone(),
                    InvocationId::from(&full_invocation_id),
                    invocation_metadata.journal_metadata.span_context.clone(),
                    key,
                    value,
                );
            }
            EnrichedEntryHeader::ClearState { .. } => {
                let_assert!(
                    Entry::ClearState(ClearStateEntry { key }) =
                        journal_entry.deserialize_entry_ref::<Codec>()?
                );
                effects.clear_state(
                    full_invocation_id.service_id.clone(),
                    InvocationId::from(&full_invocation_id),
                    invocation_metadata.journal_metadata.span_context.clone(),
                    key,
                );
            }
            EnrichedEntryHeader::ClearAllState { .. } => {
                effects.clear_all_state(
                    full_invocation_id.service_id.clone(),
                    InvocationId::from(&full_invocation_id),
                    invocation_metadata.journal_metadata.span_context.clone(),
                );
            }
            EnrichedEntryHeader::GetStateKeys { is_completed, .. } => {
                if !is_completed {
                    // Load state and write completion
                    let value = state
                        .load_state_keys(&full_invocation_id.service_id)
                        .await?;
                    let completion_result = Codec::serialize_get_state_keys_completion(value);
                    Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                    // We can already forward the completion
                    effects.forward_completion(
                        full_invocation_id.clone(),
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
                        // Registering a timer generates multiple effects: timer registration and
                        // journal append which each generate actuator messages for the timer service
                        // and the invoker --> Cloning required
                        full_invocation_id.clone(),
                        MillisSinceEpoch::new(wake_up_time),
                        entry_index,
                    ),
                    invocation_metadata.journal_metadata.span_context.clone(),
                );
            }
            EnrichedEntryHeader::Invoke {
                enrichment_result, ..
            } => {
                if let Some(InvokeEnrichmentResult {
                    service_key,
                    invocation_uuid: invocation_id,
                    span_context,
                    ..
                }) = enrichment_result
                {
                    let_assert!(
                        Entry::Invoke(InvokeEntry { request, .. }) =
                            journal_entry.deserialize_entry_ref::<Codec>()?
                    );

                    let service_invocation = Self::create_service_invocation(
                        *invocation_id,
                        service_key.clone(),
                        request,
                        Source::Service(full_invocation_id.clone()),
                        Some((full_invocation_id.clone(), entry_index)),
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
            EnrichedEntryHeader::BackgroundInvoke {
                enrichment_result, ..
            } => {
                let InvokeEnrichmentResult {
                    service_key,
                    invocation_uuid: invocation_id,
                    span_context,
                    ..
                } = enrichment_result;

                let_assert!(
                    Entry::BackgroundInvoke(BackgroundInvokeEntry {
                        request,
                        invoke_time
                    }) = journal_entry.deserialize_entry_ref::<Codec>()?
                );

                let service_method = request.method_name.clone();

                // 0 is equal to not set, meaning execute now
                let delay = if invoke_time == 0 {
                    None
                } else {
                    Some(MillisSinceEpoch::new(invoke_time))
                };

                let service_invocation = Self::create_service_invocation(
                    *invocation_id,
                    service_key.clone(),
                    request,
                    Source::Service(full_invocation_id.clone()),
                    None,
                    span_context.clone(),
                    delay,
                );

                let pointer_span_id = match span_context.span_cause() {
                    Some(SpanRelationCause::Linked(_, span_id)) => Some(*span_id),
                    _ => None,
                };

                effects.trace_background_invoke(
                    service_invocation.fid.clone(),
                    service_method,
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
                    .load_completion_result(&InvocationId::from(&full_invocation_id), entry_index)
                    .await?
                {
                    Codec::write_completion(&mut journal_entry, completion_result.clone())?;

                    effects.forward_completion(
                        full_invocation_id.clone(),
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
                        invocation_id.clone(),
                        *entry_index,
                        entry.result.into(),
                    ),
                    effects,
                );
            }
            EnrichedEntryHeader::Custom { .. } => {
                // We just store it
            }
        }

        effects.append_journal_entry(
            InvocationId::from(&full_invocation_id),
            InvocationStatus::Invoked(invocation_metadata),
            entry_index,
            journal_entry,
        );
        effects.send_stored_ack_to_invoker(full_invocation_id, entry_index);

        Ok(())
    }

    async fn handle_completion<State: StateReader>(
        maybe_full_invocation_id: MaybeFullInvocationId,
        completion: Completion,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let status = Self::read_invocation_status(&maybe_full_invocation_id, state).await?;
        let mut related_sid = None;
        let mut span_relation = SpanRelation::None;
        let invocation_id = InvocationId::from(maybe_full_invocation_id);

        match status {
            InvocationStatus::Invoked(metadata) => {
                let full_invocation_id =
                    FullInvocationId::combine(metadata.service_id, invocation_id);
                Self::handle_completion_for_invoked(
                    full_invocation_id.clone(),
                    completion,
                    effects,
                );
                related_sid = Some(full_invocation_id);
                span_relation = metadata.journal_metadata.span_context.as_parent();
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_completed_entries,
            } => {
                let full_invocation_id =
                    FullInvocationId::combine(metadata.service_id.clone(), invocation_id);
                span_relation = metadata.journal_metadata.span_context.as_parent();

                if Self::handle_completion_for_suspended(
                    full_invocation_id.clone(),
                    completion,
                    &waiting_for_completed_entries,
                    effects,
                ) {
                    effects.resume_service(InvocationId::from(&full_invocation_id), metadata);
                }
                related_sid = Some(full_invocation_id);
            }
            _ => {
                debug!(
                    restate.invocation.id = %invocation_id,
                    ?completion,
                    "Ignoring completion for invocation that is no longer running."
                )
            }
        }

        Ok((related_sid, span_relation))
    }

    fn handle_completion_for_suspended(
        full_invocation_id: FullInvocationId,
        completion: Completion,
        waiting_for_completed_entries: &HashSet<EntryIndex>,
        effects: &mut Effects,
    ) -> bool {
        let resume_invocation = waiting_for_completed_entries.contains(&completion.entry_index);
        effects.store_completion(InvocationId::from(full_invocation_id), completion);

        resume_invocation
    }

    fn handle_completion_for_invoked(
        full_invocation_id: FullInvocationId,
        completion: Completion,
        effects: &mut Effects,
    ) {
        effects.store_completion(InvocationId::from(&full_invocation_id), completion.clone());
        effects.forward_completion(full_invocation_id, completion);
    }

    // TODO: Introduce distinction between invocation_status and service_instance_status to
    //  properly handle case when the given invocation is not executing + avoid cloning maybe_fid
    async fn read_invocation_status<State: StateReader>(
        maybe_full_invocation_id: &MaybeFullInvocationId,
        state: &mut State,
    ) -> Result<InvocationStatus, Error> {
        Ok(match maybe_full_invocation_id {
            MaybeFullInvocationId::Partial(iid) => state.get_invocation_status(iid).await?,
            MaybeFullInvocationId::Full(fid) => {
                state
                    .get_invocation_status(&InvocationId::from(fid))
                    .await?
            }
        })
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
            &invocation.fid,
            invocation.method_name.deref(),
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
        full_invocation_id: &FullInvocationId,
        service_method: ByteString,
        span_context: ServiceInvocationSpanContext,
        creation_time: MillisSinceEpoch,
        result: Result<(), (InvocationErrorCode, String)>,
        effects: &mut Effects,
    ) {
        effects.trace_invocation_result(
            full_invocation_id.clone(),
            service_method,
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

    fn outbox_message(&mut self, message: OutboxMessage, effects: &mut Effects) {
        effects.enqueue_into_outbox(self.outbox_seq_number, message);
        self.outbox_seq_number += 1;
    }

    fn ingress_response(&mut self, ingress_response: IngressResponse, effects: &mut Effects) {
        effects.send_ingress_response(ingress_response);
    }

    fn create_service_invocation(
        invocation_id: InvocationUuid,
        invocation_key: Bytes,
        invoke_request: InvokeRequest,
        source: Source,
        response_target: Option<(FullInvocationId, EntryIndex)>,
        span_context: ServiceInvocationSpanContext,
        execution_time: Option<MillisSinceEpoch>,
    ) -> ServiceInvocation {
        let InvokeRequest {
            service_name,
            method_name,
            parameter,
            ..
        } = invoke_request;

        let response_sink = if let Some((caller, entry_index)) = response_target {
            Some(ServiceInvocationResponseSink::PartitionProcessor {
                caller,
                entry_index,
            })
        } else {
            None
        };

        ServiceInvocation {
            fid: FullInvocationId::new(service_name, invocation_key, invocation_id),
            method_name,
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
