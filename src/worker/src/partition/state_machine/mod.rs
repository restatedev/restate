// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert2::let_assert;
use bytes::Bytes;
use futures::future::BoxFuture;
use restate_types::errors::{InvocationError, InvocationErrorCode, RestateErrorCode};
use restate_types::journal::raw::{RawEntryCodec, RawEntryCodecError};
use restate_types::journal::{
    BackgroundInvokeEntry, ClearStateEntry, CompleteAwakeableEntry, Completion, CompletionResult,
    Entry, GetStateEntry, InvokeEntry, InvokeRequest, OutputStreamEntry, SetStateEntry, SleepEntry,
};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use tracing::{debug, instrument, trace};

use crate::partition::effects::Effects;
use crate::partition::types::{InvokerEffect, InvokerEffectKind, TimerValue};

mod dedup;

pub(crate) use dedup::DeduplicatingStateMachine;
use restate_storage_api::inbox_table::InboxEntry;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::status_table::{InvocationMetadata, InvocationStatus};
use restate_storage_api::timer_table::Timer;
use restate_types::identifiers::{
    EntryIndex, FullInvocationId, InvocationId, InvocationUuid, ServiceId,
};
use restate_types::invocation::{
    InvocationResponse, MaybeFullInvocationId, ResponseResult, ServiceInvocation,
    ServiceInvocationResponseSink, ServiceInvocationSpanContext, SpanRelation, SpanRelationCause,
};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry, ResolutionResult};
use restate_types::message::MessageIndex;
use restate_types::time::MillisSinceEpoch;

const KILLED_INVOCATION_ERROR: InvocationError = InvocationError::new_static(
    InvocationErrorCode::Restate(RestateErrorCode::Killed),
    "killed",
);

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("failed to read from state reader: {0}")]
    State(#[from] StateReaderError),
    #[error("failed to deserialize entry: {0}")]
    Codec(#[from] RawEntryCodecError),
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

#[derive(Debug)]
pub(crate) enum Command {
    Kill(InvocationId),
    Invoker(InvokerEffect),
    Timer(TimerValue),
    OutboxTruncation(MessageIndex),
    Invocation(ServiceInvocation),
    Response(InvocationResponse),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StateReaderError {
    #[error(transparent)]
    Storage(#[from] restate_storage_api::StorageError),
}

pub(crate) trait StateReader {
    // TODO: Replace with async trait or proper future
    fn get_invocation_status<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
    ) -> BoxFuture<'a, Result<InvocationStatus, StateReaderError>>;

    // TODO: Replace with async trait or proper future
    fn resolve_invocation_status_from_invocation_id<'a>(
        &'a mut self,
        invocation_id: &'a InvocationId,
    ) -> BoxFuture<'a, Result<(FullInvocationId, InvocationStatus), StateReaderError>>;

    // TODO: Replace with async trait or proper future
    fn peek_inbox<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
    ) -> BoxFuture<'a, Result<Option<InboxEntry>, StateReaderError>>;

    // TODO: Replace with async trait or proper future
    fn is_entry_resumable<'a>(
        &'a mut self,
        service_id: &'a ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<bool, StateReaderError>>;
}

pub(crate) fn create_deduplicating_state_machine<Codec>(
    inbox_seq_number: MessageIndex,
    outbox_seq_number: MessageIndex,
) -> DeduplicatingStateMachine<Codec> {
    DeduplicatingStateMachine::new(StateMachine::new(inbox_seq_number, outbox_seq_number))
}

pub(crate) struct StateMachine<Codec> {
    // initialized from persistent storage
    inbox_seq_number: MessageIndex,
    outbox_seq_number: MessageIndex,

    _codec: PhantomData<Codec>,
}

impl<Codec> Debug for StateMachine<Codec> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachine")
            .field("inbox_seq_number", &self.inbox_seq_number)
            .field("outbox_seq_number", &self.outbox_seq_number)
            .finish()
    }
}

impl<Codec> StateMachine<Codec> {
    pub(crate) fn new(inbox_seq_number: MessageIndex, outbox_seq_number: MessageIndex) -> Self {
        Self {
            inbox_seq_number,
            outbox_seq_number,
            _codec: PhantomData::default(),
        }
    }
}

impl<Codec> StateMachine<Codec>
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
    pub(crate) async fn on_apply<State: StateReader>(
        &mut self,
        command: Command,
        effects: &mut Effects,
        state: &mut State,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        return match command {
            Command::Invocation(service_invocation) => {
                let status = state
                    .get_invocation_status(&service_invocation.fid.service_id)
                    .await?;

                let fid = service_invocation.fid.clone();

                if let InvocationStatus::Free = status {
                    effects.invoke_service(service_invocation);
                } else {
                    effects.enqueue_into_inbox(self.inbox_seq_number, service_invocation);
                    self.inbox_seq_number += 1;
                }
                Ok((Some(fid), extract_span_relation(&status)))
            }
            Command::Response(InvocationResponse {
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
            Command::Invoker(effect) => {
                let (related_sid, span_relation) =
                    self.try_invoker_effect(effects, state, effect).await?;
                Ok((Some(related_sid), span_relation))
            }
            Command::OutboxTruncation(index) => {
                effects.truncate_outbox(index);
                Ok((None, SpanRelation::None))
            }
            Command::Timer(timer) => self.on_timer(timer, state, effects).await,
            Command::Kill(iid) => self.try_kill_invocation(iid, state, effects).await,
        };
    }

    async fn try_kill_invocation<State: StateReader>(
        &mut self,
        invocation_id: InvocationId,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let (full_invocation_id, status) = state
            .resolve_invocation_status_from_invocation_id(&invocation_id)
            .await?;

        match status {
            InvocationStatus::Invoked(metadata) | InvocationStatus::Suspended { metadata, .. }
                if metadata.invocation_uuid == full_invocation_id.invocation_uuid =>
            {
                let (fid, related_span) = self
                    .kill_invocation(full_invocation_id, metadata, state, effects)
                    .await?;
                Ok((Some(fid), related_span))
            }
            _ => {
                trace!(
                    "Received kill command for unknown invocation with fid '{full_invocation_id}'."
                );
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                effects.abort_invocation(full_invocation_id.clone());
                Ok((Some(full_invocation_id), SpanRelation::None))
            }
        }
    }

    async fn kill_invocation<State: StateReader>(
        &mut self,
        full_invocation_id: FullInvocationId,
        metadata: InvocationMetadata,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(FullInvocationId, SpanRelation), Error> {
        let related_span = metadata.journal_metadata.span_context.as_parent();

        self.fail_invocation(
            effects,
            state,
            full_invocation_id.clone(),
            metadata,
            KILLED_INVOCATION_ERROR,
        )
        .await?;
        effects.abort_invocation(full_invocation_id.clone());

        Ok((full_invocation_id, related_span))
    }

    async fn on_timer<State: StateReader>(
        &mut self,
        TimerValue {
            full_invocation_id,
            entry_index,
            wake_up_time,
            value,
        }: TimerValue,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        effects.delete_timer(wake_up_time, full_invocation_id.clone(), entry_index);

        return match value {
            Timer::CompleteSleepEntry => {
                Self::handle_completion(
                    MaybeFullInvocationId::Full(full_invocation_id),
                    Completion {
                        entry_index,
                        result: CompletionResult::Empty,
                    },
                    state,
                    effects,
                )
                .await
            }
            Timer::Invoke(service_invocation) => {
                self.send_message(
                    OutboxMessage::ServiceInvocation(service_invocation),
                    effects,
                );
                Ok((Some(full_invocation_id), SpanRelation::None))
            }
        };
    }

    async fn try_invoker_effect<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        invoker_effect: InvokerEffect,
    ) -> Result<(FullInvocationId, SpanRelation), Error> {
        let status = state
            .get_invocation_status(&invoker_effect.full_invocation_id.service_id)
            .await?;

        match status {
            InvocationStatus::Invoked(invocation_metadata)
                if invocation_metadata.invocation_uuid
                    == invoker_effect.full_invocation_id.invocation_uuid =>
            {
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

    async fn on_invoker_effect<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        InvokerEffect {
            full_invocation_id,
            kind,
        }: InvokerEffect,
        invocation_metadata: InvocationMetadata,
    ) -> Result<(FullInvocationId, SpanRelation), Error> {
        let related_sid = full_invocation_id.clone();
        let span_relation = invocation_metadata
            .journal_metadata
            .span_context
            .as_parent();

        match kind {
            InvokerEffectKind::SelectedEndpoint(endpoint_id) => {
                effects.store_chosen_endpoint(
                    full_invocation_id.service_id,
                    endpoint_id,
                    invocation_metadata,
                );
            }
            InvokerEffectKind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    effects,
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
                debug_assert!(
                    !waiting_for_completed_entries.is_empty(),
                    "Expecting at least one entry on which the invocation {full_invocation_id} is waiting."
                );
                let mut any_completed = false;
                for entry_index in &waiting_for_completed_entries {
                    if state
                        .is_entry_resumable(&full_invocation_id.service_id, *entry_index)
                        .await?
                    {
                        trace!(
                            rpc.service = %full_invocation_id.service_id.service_name,
                            restate.invocation.id = %full_invocation_id,
                            "Resuming instead of suspending service because an awaited entry is completed/acked.");
                        any_completed = true;
                        break;
                    }
                }
                if any_completed {
                    effects.resume_service(full_invocation_id.service_id, invocation_metadata);
                } else {
                    effects.suspend_service(
                        full_invocation_id.service_id,
                        invocation_metadata,
                        waiting_for_completed_entries,
                    );
                }
            }
            InvokerEffectKind::End => {
                self.finish_invocation(effects, state, full_invocation_id, invocation_metadata)
                    .await?;
            }
            InvokerEffectKind::Failed(e) => {
                self.fail_invocation(effects, state, full_invocation_id, invocation_metadata, e)
                    .await?;
            }
        }

        Ok((related_sid, span_relation))
    }

    async fn finish_invocation<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        full_invocation_id: FullInvocationId,
        invocation_metadata: InvocationMetadata,
    ) -> Result<(), Error> {
        let length = invocation_metadata.journal_metadata.length;
        self.notify_invocation_result(&full_invocation_id, invocation_metadata, effects, Ok(()));
        self.complete_invocation(full_invocation_id, state, length, effects)
            .await
    }

    async fn fail_invocation<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        full_invocation_id: FullInvocationId,
        invocation_metadata: InvocationMetadata,
        error: InvocationError,
    ) -> Result<(), Error> {
        let code = error.code();
        if let Some(response_sink) = &invocation_metadata.response_sink {
            let outbox_message = Self::create_response(
                &full_invocation_id,
                response_sink.clone(),
                ResponseResult::Failure(code.into(), error.message().into()),
            );
            // TODO: We probably only need to send the response if we haven't send a response before
            self.send_message(outbox_message, effects);
        }

        let length = invocation_metadata.journal_metadata.length;
        self.notify_invocation_result(
            &full_invocation_id,
            invocation_metadata,
            effects,
            Err((code, error.to_string())),
        );
        self.complete_invocation(full_invocation_id, state, length, effects)
            .await
    }

    async fn handle_journal_entry(
        &mut self,
        effects: &mut Effects,
        full_invocation_id: FullInvocationId,
        entry_index: EntryIndex,
        mut journal_entry: EnrichedRawEntry,
        invocation_metadata: InvocationMetadata,
    ) -> Result<(), Error> {
        debug_assert_eq!(
            entry_index, invocation_metadata.journal_metadata.length,
            "Expect to receive next journal entry for {full_invocation_id}"
        );

        match journal_entry.header {
            // nothing to do
            EnrichedEntryHeader::PollInputStream { is_completed } => {
                debug_assert!(
                    !is_completed,
                    "Poll input stream entry must not be completed."
                );
            }
            EnrichedEntryHeader::OutputStream => {
                if let Some(ref response_sink) = invocation_metadata.response_sink {
                    let_assert!(
                        Entry::OutputStream(OutputStreamEntry { result }) =
                            Codec::deserialize(&journal_entry)?
                    );

                    let outbox_message = Self::create_response(
                        &full_invocation_id,
                        response_sink.clone(),
                        result.into(),
                    );
                    self.send_message(outbox_message, effects);
                }
            }
            EnrichedEntryHeader::GetState { is_completed } => {
                if !is_completed {
                    let_assert!(
                        Entry::GetState(GetStateEntry { key, .. }) =
                            Codec::deserialize(&journal_entry)?
                    );

                    effects.get_state_and_append_completed_entry(
                        full_invocation_id.service_id,
                        invocation_metadata,
                        key,
                        entry_index,
                        journal_entry,
                    );
                    return Ok(());
                }
            }
            EnrichedEntryHeader::SetState => {
                let_assert!(
                    Entry::SetState(SetStateEntry { key, value }) =
                        Codec::deserialize(&journal_entry)?
                );

                effects.set_state(
                    full_invocation_id.service_id,
                    invocation_metadata,
                    key,
                    value,
                    journal_entry,
                    entry_index,
                );
                // set_state includes append journal entry in order to avoid unnecessary clones.
                // That's why we must return here.
                return Ok(());
            }
            EnrichedEntryHeader::ClearState => {
                let_assert!(
                    Entry::ClearState(ClearStateEntry { key }) =
                        Codec::deserialize(&journal_entry)?
                );
                effects.clear_state(
                    full_invocation_id.service_id,
                    invocation_metadata,
                    key,
                    journal_entry,
                    entry_index,
                );
                // clear_state includes append journal entry in order to avoid unnecessary clones.
                // That's why we must return here.
                return Ok(());
            }
            EnrichedEntryHeader::Sleep { is_completed } => {
                debug_assert!(!is_completed, "Sleep entry must not be completed.");
                let_assert!(
                    Entry::Sleep(SleepEntry { wake_up_time, .. }) =
                        Codec::deserialize(&journal_entry)?
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
                ref resolution_result,
                ..
            } => {
                if let Some(ResolutionResult {
                    service_key,
                    invocation_uuid: invocation_id,
                    span_context,
                }) = resolution_result
                {
                    let_assert!(
                        Entry::Invoke(InvokeEntry { request, .. }) =
                            Codec::deserialize(&journal_entry)?
                    );

                    let service_invocation = Self::create_service_invocation(
                        *invocation_id,
                        service_key.clone(),
                        request,
                        Some((full_invocation_id.clone(), entry_index)),
                        span_context.clone(),
                    );
                    self.send_message(
                        OutboxMessage::ServiceInvocation(service_invocation),
                        effects,
                    );
                } else {
                    // no action needed for an invoke entry that has been completed by the service endpoint
                }
            }
            EnrichedEntryHeader::BackgroundInvoke {
                ref resolution_result,
            } => {
                let ResolutionResult {
                    service_key,
                    invocation_uuid: invocation_id,
                    span_context,
                } = resolution_result;

                let_assert!(
                    Entry::BackgroundInvoke(BackgroundInvokeEntry {
                        request,
                        invoke_time
                    }) = Codec::deserialize(&journal_entry)?
                );

                let method = request.method_name.to_string();

                let service_invocation = Self::create_service_invocation(
                    *invocation_id,
                    service_key.clone(),
                    request,
                    None,
                    span_context.clone(),
                );

                let pointer_span_id = match span_context.span_cause() {
                    Some(SpanRelationCause::Linked(_, span_id)) => Some(*span_id),
                    _ => None,
                };

                effects.background_invoke(
                    service_invocation.fid.clone(),
                    method,
                    invocation_metadata.journal_metadata.span_context.clone(),
                    pointer_span_id,
                );

                // 0 is equal to not set, meaning execute now
                if invoke_time == 0 {
                    self.send_message(
                        OutboxMessage::ServiceInvocation(service_invocation),
                        effects,
                    );
                } else {
                    effects.register_timer(
                        TimerValue::new_invoke(
                            full_invocation_id.clone(),
                            MillisSinceEpoch::new(invoke_time),
                            entry_index,
                            service_invocation,
                        ),
                        invocation_metadata.journal_metadata.span_context.clone(),
                    );
                }
            }
            // special handling because we can have a completion present
            EnrichedEntryHeader::Awakeable { is_completed } => {
                debug_assert!(!is_completed, "Awakeable entry must not be completed.");
                effects.append_awakeable_entry(
                    full_invocation_id.service_id,
                    invocation_metadata,
                    entry_index,
                    journal_entry,
                );
                return Ok(());
            }
            EnrichedEntryHeader::CompleteAwakeable {
                ref invocation_id,
                entry_index,
            } => {
                let_assert!(Entry::CompleteAwakeable(entry) = Codec::deserialize(&journal_entry)?);

                let response = Self::create_response_for_awakeable_entry(
                    invocation_id.clone(),
                    entry_index,
                    entry,
                );
                self.send_message(response, effects);
            }
            EnrichedEntryHeader::Custom { requires_ack, code } => {
                if requires_ack {
                    // Reset the requires_ack flag to false
                    journal_entry.header = EnrichedEntryHeader::Custom {
                        code,
                        requires_ack: false,
                    };
                    effects.append_journal_entry_and_ack_storage(
                        full_invocation_id.service_id,
                        invocation_metadata,
                        entry_index,
                        journal_entry,
                    );
                    return Ok(());
                }
            }
        }

        effects.append_journal_entry(
            full_invocation_id.service_id,
            invocation_metadata,
            entry_index,
            journal_entry,
        );

        Ok(())
    }

    async fn handle_completion<State: StateReader>(
        maybe_full_invocation_id: MaybeFullInvocationId,
        completion: Completion,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<FullInvocationId>, SpanRelation), Error> {
        let (full_invocation_id, status) = match maybe_full_invocation_id {
            MaybeFullInvocationId::Partial(iid) => {
                state
                    .resolve_invocation_status_from_invocation_id(&iid)
                    .await?
            }
            MaybeFullInvocationId::Full(fid) => {
                let status = state.get_invocation_status(&fid.service_id).await?;
                (fid, status)
            }
        };
        let mut related_sid = None;
        let mut span_relation = SpanRelation::None;

        match status {
            InvocationStatus::Invoked(metadata) => {
                if metadata.invocation_uuid == full_invocation_id.invocation_uuid {
                    effects.store_and_forward_completion(full_invocation_id.clone(), completion);
                    related_sid = Some(full_invocation_id);
                    span_relation = metadata.journal_metadata.span_context.as_parent();
                } else {
                    debug!(
                        rpc.service = %full_invocation_id.service_id.service_name,
                        restate.invocation.id = %full_invocation_id,
                        ?completion,
                        "Ignoring completion for invocation that is no longer running."
                    );
                }
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_completed_entries,
            } => {
                if metadata.invocation_uuid == full_invocation_id.invocation_uuid {
                    span_relation = metadata.journal_metadata.span_context.as_parent();

                    if waiting_for_completed_entries.contains(&completion.entry_index) {
                        effects.store_completion_and_resume(
                            full_invocation_id.service_id.clone(),
                            metadata,
                            completion,
                        );
                    } else {
                        effects.store_completion(full_invocation_id.clone(), completion);
                    }
                    related_sid = Some(full_invocation_id);
                } else {
                    debug!(
                        rpc.service = %full_invocation_id.service_id.service_name,
                        restate.invocation.id = %full_invocation_id,
                        ?completion,
                        "Ignoring completion for invocation that is no longer running."
                    );
                }
            }
            InvocationStatus::Free => {
                debug!(
                    rpc.service = %full_invocation_id.service_id.service_name,
                    restate.invocation.id = %full_invocation_id,
                    ?completion,
                    "Ignoring completion for invocation that is no longer running."
                )
            }
        }

        Ok((related_sid, span_relation))
    }

    fn notify_invocation_result(
        &mut self,
        full_invocation_id: &FullInvocationId,
        invocation_metadata: InvocationMetadata,
        effects: &mut Effects,
        result: Result<(), (InvocationErrorCode, String)>,
    ) {
        effects.notify_invocation_result(full_invocation_id.clone(), invocation_metadata, result);
    }

    async fn complete_invocation<State: StateReader>(
        &mut self,
        full_invocation_id: FullInvocationId,
        state: &mut State,
        journal_length: EntryIndex,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        if let Some(InboxEntry {
            inbox_sequence_number,
            service_invocation,
        }) = state.peek_inbox(&full_invocation_id.service_id).await?
        {
            effects.drop_journal_and_pop_inbox(
                full_invocation_id.service_id,
                inbox_sequence_number,
                journal_length,
                service_invocation,
            );
        } else {
            effects.drop_journal_and_free_service(full_invocation_id.service_id, journal_length);
        }

        Ok(())
    }

    fn send_message(&mut self, message: OutboxMessage, effects: &mut Effects) {
        effects.enqueue_into_outbox(self.outbox_seq_number, message);
        self.outbox_seq_number += 1;
    }

    fn create_service_invocation(
        invocation_id: InvocationUuid,
        invocation_key: Bytes,
        invoke_request: InvokeRequest,
        response_target: Option<(FullInvocationId, EntryIndex)>,
        span_context: ServiceInvocationSpanContext,
    ) -> ServiceInvocation {
        let InvokeRequest {
            service_name,
            method_name,
            parameter,
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
            response_sink,
            span_context,
        }
    }

    fn create_response_for_awakeable_entry(
        invocation_id: InvocationId,
        entry_index: EntryIndex,
        entry: CompleteAwakeableEntry,
    ) -> OutboxMessage {
        let CompleteAwakeableEntry { result, .. } = entry;
        OutboxMessage::ServiceResponse(InvocationResponse {
            entry_index,
            result: result.into(),
            id: MaybeFullInvocationId::Partial(invocation_id),
        })
    }

    fn create_response(
        callee: &FullInvocationId,
        response_sink: ServiceInvocationResponseSink,
        result: ResponseResult,
    ) -> OutboxMessage {
        match response_sink {
            ServiceInvocationResponseSink::PartitionProcessor {
                entry_index,
                caller,
            } => OutboxMessage::ServiceResponse(InvocationResponse {
                id: MaybeFullInvocationId::Full(caller),
                entry_index,
                result,
            }),
            ServiceInvocationResponseSink::Ingress(ingress_id) => OutboxMessage::IngressResponse {
                ingress_id,
                full_invocation_id: callee.clone(),
                response: result,
            },
        }
    }
}

fn extract_span_relation(status: &InvocationStatus) -> SpanRelation {
    match status {
        InvocationStatus::Invoked(metadata) => metadata.journal_metadata.span_context.as_parent(),
        InvocationStatus::Suspended { metadata, .. } => {
            metadata.journal_metadata.span_context.as_parent()
        }
        InvocationStatus::Free => SpanRelation::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::effects::Effect;
    use futures::future::ok;
    use futures::FutureExt;
    use restate_invoker_api::EffectKind;
    use restate_service_protocol::awakeable_id::AwakeableIdentifier;
    use restate_service_protocol::codec::ProtobufRawEntryCodec;
    use restate_test_util::{assert_eq, let_assert, test};
    use restate_types::errors::UserErrorCode;
    use restate_types::identifiers::WithPartitionKey;
    use restate_types::journal::{EntryResult, JournalMetadata};
    use std::collections::HashMap;

    #[derive(Default)]
    struct StateReaderMock {
        invocations: HashMap<ServiceId, InvocationStatus>,
    }

    impl StateReaderMock {
        fn register_invocation_status(&mut self, fid: &FullInvocationId, length: EntryIndex) {
            self.invocations.insert(
                fid.service_id.clone(),
                InvocationStatus::Invoked(InvocationMetadata {
                    invocation_uuid: fid.invocation_uuid,
                    journal_metadata: JournalMetadata {
                        endpoint_id: None,
                        length,
                        method: "".to_string(),
                        span_context: ServiceInvocationSpanContext::empty(),
                    },
                    response_sink: None,
                    creation_time: MillisSinceEpoch::now(),
                    modification_time: MillisSinceEpoch::now(),
                }),
            );
        }
    }

    impl StateReader for StateReaderMock {
        fn get_invocation_status<'a>(
            &'a mut self,
            service_id: &'a ServiceId,
        ) -> BoxFuture<'a, Result<InvocationStatus, StateReaderError>> {
            ok(self.invocations.get(service_id).cloned().unwrap()).boxed()
        }

        fn resolve_invocation_status_from_invocation_id<'a>(
            &'a mut self,
            invocation_id: &'a InvocationId,
        ) -> BoxFuture<'a, Result<(FullInvocationId, InvocationStatus), StateReaderError>> {
            let (service_id, status) = self
                .invocations
                .iter()
                .find(|(service_id, status)| {
                    service_id.partition_key() == invocation_id.partition_key()
                        && status.invocation_uuid().unwrap() == invocation_id.invocation_uuid()
                })
                .unwrap();

            ok((
                FullInvocationId::with_service_id(
                    service_id.clone(),
                    invocation_id.invocation_uuid(),
                ),
                status.clone(),
            ))
            .boxed()
        }

        fn peek_inbox<'a>(
            &'a mut self,
            _service_id: &'a ServiceId,
        ) -> BoxFuture<'a, Result<Option<InboxEntry>, StateReaderError>> {
            todo!()
        }

        fn is_entry_resumable<'a>(
            &'a mut self,
            _service_id: &'a ServiceId,
            _entry_index: EntryIndex,
        ) -> BoxFuture<Result<bool, StateReaderError>> {
            todo!()
        }
    }

    #[test(tokio::test)]
    async fn awakeable_with_success() {
        let mut state_machine: StateMachine<ProtobufRawEntryCodec> = StateMachine::new(0, 0);
        let mut effects = Effects::default();
        let mut state_reader = StateReaderMock::default();

        let sid_caller = FullInvocationId::mock_random();
        let sid_callee = FullInvocationId::mock_random();

        let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
            CompleteAwakeableEntry {
                id: AwakeableIdentifier::new(sid_callee.clone().into(), 1)
                    .encode()
                    .into(),
                result: EntryResult::Success(Bytes::default()),
            },
        ));
        let cmd = Command::Invoker(InvokerEffect {
            full_invocation_id: sid_caller.clone(),
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry,
            },
        });

        state_reader.register_invocation_status(&sid_caller, 1);

        state_machine
            .on_apply(cmd, &mut effects, &mut state_reader)
            .await
            .unwrap();
        let_assert!(Effect::EnqueueIntoOutbox { message, .. } = effects.drain().next().unwrap());
        let_assert!(
            OutboxMessage::ServiceResponse(InvocationResponse {
                id,
                entry_index,
                result: ResponseResult::Success(_),
            }) = message
        );
        assert_eq!(
            id,
            MaybeFullInvocationId::Partial(InvocationId::from(sid_callee))
        );
        assert_eq!(entry_index, 1);
    }

    #[test(tokio::test)]
    async fn awakeable_with_failure() {
        let mut state_machine: StateMachine<ProtobufRawEntryCodec> = StateMachine::new(0, 0);
        let mut effects = Effects::default();
        let mut state_reader = StateReaderMock::default();

        let sid_caller = FullInvocationId::mock_random();
        let sid_callee = FullInvocationId::mock_random();

        let entry = ProtobufRawEntryCodec::serialize_enriched(Entry::CompleteAwakeable(
            CompleteAwakeableEntry {
                id: AwakeableIdentifier::new(sid_callee.clone().into(), 1)
                    .encode()
                    .into(),
                result: EntryResult::Failure(
                    UserErrorCode::FailedPrecondition,
                    "Some failure".into(),
                ),
            },
        ));
        let cmd = Command::Invoker(InvokerEffect {
            full_invocation_id: sid_caller.clone(),
            kind: EffectKind::JournalEntry {
                entry_index: 1,
                entry,
            },
        });

        state_reader.register_invocation_status(&sid_caller, 1);

        state_machine
            .on_apply(cmd, &mut effects, &mut state_reader)
            .await
            .unwrap();
        let_assert!(Effect::EnqueueIntoOutbox { message, .. } = effects.drain().next().unwrap());
        let_assert!(
            OutboxMessage::ServiceResponse(InvocationResponse {
                id,
                entry_index,
                result: ResponseResult::Failure(UserErrorCode::FailedPrecondition, failure_reason),
            }) = message
        );
        assert_eq!(
            id,
            MaybeFullInvocationId::Partial(InvocationId::from(sid_callee))
        );
        assert_eq!(entry_index, 1);
        assert_eq!(failure_reason, "Some failure");
    }

    #[test(tokio::test)]
    async fn send_response_using_invocation_id() {
        let mut state_machine: StateMachine<ProtobufRawEntryCodec> = StateMachine::new(0, 0);
        let mut effects = Effects::default();
        let mut state_reader = StateReaderMock::default();

        let fid = FullInvocationId::mock_random();

        let cmd = Command::Response(InvocationResponse {
            id: MaybeFullInvocationId::Partial(InvocationId::from(fid.clone())),
            entry_index: 1,
            result: ResponseResult::Success(Bytes::from_static(b"hello")),
        });

        state_reader.register_invocation_status(&fid, 1);

        state_machine
            .on_apply(cmd, &mut effects, &mut state_reader)
            .await
            .unwrap();
        let_assert!(
            Effect::StoreCompletionAndForward {
                full_invocation_id,
                ..
            } = effects.drain().next().unwrap()
        );
        assert_eq!(full_invocation_id, fid);
    }
}
