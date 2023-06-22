use assert2::let_assert;
use bytes::Bytes;
use futures::future::BoxFuture;
use restate_common::errors::{InvocationError, InvocationErrorCode, RestateErrorCode};
use restate_common::types::{
    CompletionResult, EnrichedEntryHeader, EnrichedRawEntry, EntryIndex, InboxEntry, InvocationId,
    InvocationMetadata, InvocationResponse, InvocationStatus, MessageIndex, MillisSinceEpoch,
    OutboxMessage, ResolutionResult, ResponseResult, ServiceId, ServiceInvocation,
    ServiceInvocationId, ServiceInvocationResponseSink, ServiceInvocationSpanContext, SpanRelation,
    Timer,
};
use restate_journal::raw::{RawEntryCodec, RawEntryCodecError};
use restate_journal::{
    BackgroundInvokeEntry, ClearStateEntry, CompleteAwakeableEntry, Completion, Entry,
    GetStateEntry, InvokeEntry, InvokeRequest, OutputStreamEntry, SetStateEntry, SleepEntry,
};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use tracing::{debug, instrument, trace};

use crate::partition::effects::Effects;
use crate::partition::types::{InvokerEffect, InvokerEffectKind, TimerValue};

mod dedup;

pub(crate) use dedup::DeduplicatingStateMachine;

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
    Kill(ServiceInvocationId),
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
    ) -> Result<(Option<ServiceInvocationId>, SpanRelation), Error> {
        return match command {
            Command::Invocation(service_invocation) => {
                let status = state
                    .get_invocation_status(&service_invocation.id.service_id)
                    .await?;

                let sid = service_invocation.id.clone();

                if let InvocationStatus::Free = status {
                    effects.invoke_service(service_invocation);
                } else {
                    effects.enqueue_into_inbox(self.inbox_seq_number, service_invocation);
                    self.inbox_seq_number += 1;
                }
                Ok((Some(sid), extract_span_relation(&status)))
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
            Command::Kill(sid) => self.try_kill_invocation(sid, state, effects).await,
        };
    }

    async fn try_kill_invocation<State: StateReader>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<ServiceInvocationId>, SpanRelation), Error> {
        let status = state
            .get_invocation_status(&service_invocation_id.service_id)
            .await?;

        match status {
            InvocationStatus::Invoked(metadata) | InvocationStatus::Suspended { metadata, .. }
                if metadata.invocation_id == service_invocation_id.invocation_id =>
            {
                let (sid, related_span) = self
                    .kill_invocation(service_invocation_id, metadata, state, effects)
                    .await?;
                Ok((Some(sid), related_span))
            }
            _ => {
                trace!("Received kill command for unknown invocation with sid '{service_invocation_id}'.");
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                effects.abort_invocation(service_invocation_id.clone());
                Ok((Some(service_invocation_id), SpanRelation::None))
            }
        }
    }

    async fn kill_invocation<State: StateReader>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        metadata: InvocationMetadata,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(ServiceInvocationId, SpanRelation), Error> {
        let related_span = metadata.journal_metadata.span_context.as_parent();

        self.fail_invocation(
            effects,
            state,
            service_invocation_id.clone(),
            metadata,
            KILLED_INVOCATION_ERROR,
        )
        .await?;
        effects.abort_invocation(service_invocation_id.clone());

        Ok((service_invocation_id, related_span))
    }

    async fn on_timer<State: StateReader>(
        &mut self,
        TimerValue {
            service_invocation_id,
            entry_index,
            wake_up_time,
            value,
        }: TimerValue,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<ServiceInvocationId>, SpanRelation), Error> {
        effects.delete_timer(wake_up_time, service_invocation_id.clone(), entry_index);

        return match value {
            Timer::CompleteSleepEntry => {
                Self::handle_completion(
                    service_invocation_id,
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
                Ok((Some(service_invocation_id), SpanRelation::None))
            }
        };
    }

    async fn try_invoker_effect<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        invoker_effect: InvokerEffect,
    ) -> Result<(ServiceInvocationId, SpanRelation), Error> {
        let status = state
            .get_invocation_status(&invoker_effect.service_invocation_id.service_id)
            .await?;

        match status {
            InvocationStatus::Invoked(invocation_metadata)
                if invocation_metadata.invocation_id
                    == invoker_effect.service_invocation_id.invocation_id =>
            {
                self.on_invoker_effect(effects, state, invoker_effect, invocation_metadata)
                    .await
            }
            _ => {
                trace!("Received invoker effect for unknown service invocation. Ignoring the effect and aborting.");
                effects.abort_invocation(invoker_effect.service_invocation_id.clone());
                Ok((invoker_effect.service_invocation_id, SpanRelation::None))
            }
        }
    }

    async fn on_invoker_effect<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        InvokerEffect {
            service_invocation_id,
            kind,
        }: InvokerEffect,
        invocation_metadata: InvocationMetadata,
    ) -> Result<(ServiceInvocationId, SpanRelation), Error> {
        let related_sid = service_invocation_id.clone();
        let span_relation = invocation_metadata
            .journal_metadata
            .span_context
            .as_parent();

        match kind {
            InvokerEffectKind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    effects,
                    service_invocation_id,
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
                    "Expecting at least one entry on which the invocation {service_invocation_id} is waiting."
                );
                let mut any_completed = false;
                for entry_index in &waiting_for_completed_entries {
                    if state
                        .is_entry_resumable(&service_invocation_id.service_id, *entry_index)
                        .await?
                    {
                        trace!(
                            rpc.service = %service_invocation_id.service_id.service_name,
                            restate.invocation.sid = %service_invocation_id,
                            "Resuming instead of suspending service because an awaited entry is completed/acked.");
                        any_completed = true;
                        break;
                    }
                }
                if any_completed {
                    effects.resume_service(service_invocation_id.service_id, invocation_metadata);
                } else {
                    effects.suspend_service(
                        service_invocation_id.service_id,
                        invocation_metadata,
                        waiting_for_completed_entries,
                    );
                }
            }
            InvokerEffectKind::End => {
                self.finish_invocation(effects, state, service_invocation_id, invocation_metadata)
                    .await?;
            }
            InvokerEffectKind::Failed(e) => {
                self.fail_invocation(
                    effects,
                    state,
                    service_invocation_id,
                    invocation_metadata,
                    e,
                )
                .await?;
            }
        }

        Ok((related_sid, span_relation))
    }

    async fn finish_invocation<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        service_invocation_id: ServiceInvocationId,
        invocation_metadata: InvocationMetadata,
    ) -> Result<(), Error> {
        self.notify_invocation_result(
            &service_invocation_id,
            invocation_metadata.journal_metadata.method,
            invocation_metadata.journal_metadata.span_context,
            effects,
            Ok(()),
        );
        self.complete_invocation(
            service_invocation_id,
            state,
            invocation_metadata.journal_metadata.length,
            effects,
        )
        .await
    }

    async fn fail_invocation<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &mut State,
        service_invocation_id: ServiceInvocationId,
        invocation_metadata: InvocationMetadata,
        error: InvocationError,
    ) -> Result<(), Error> {
        let code = error.code();
        if let Some(response_sink) = invocation_metadata.response_sink {
            let outbox_message = Self::create_response(
                &service_invocation_id,
                response_sink,
                ResponseResult::Failure(code.into(), error.message().into()),
            );
            // TODO: We probably only need to send the response if we haven't send a response before
            self.send_message(outbox_message, effects);
        }

        self.notify_invocation_result(
            &service_invocation_id,
            invocation_metadata.journal_metadata.method,
            invocation_metadata.journal_metadata.span_context,
            effects,
            Err((code, error.to_string())),
        );
        self.complete_invocation(
            service_invocation_id,
            state,
            invocation_metadata.journal_metadata.length,
            effects,
        )
        .await
    }

    async fn handle_journal_entry(
        &mut self,
        effects: &mut Effects,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
        invocation_metadata: InvocationMetadata,
    ) -> Result<(), Error> {
        debug_assert_eq!(
            entry_index, invocation_metadata.journal_metadata.length,
            "Expect to receive next journal entry for {service_invocation_id}"
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
                        &service_invocation_id,
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
                        service_invocation_id.service_id,
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
                    service_invocation_id.service_id,
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
                    service_invocation_id.service_id,
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
                effects.register_timer(TimerValue::new_sleep(
                    // Registering a timer generates multiple effects: timer registration and
                    // journal append which each generate actuator messages for the timer service
                    // and the invoker --> Cloning required
                    service_invocation_id.clone(),
                    MillisSinceEpoch::new(wake_up_time),
                    entry_index,
                ));
            }
            EnrichedEntryHeader::Invoke {
                ref resolution_result,
                ..
            } => {
                if let Some(ResolutionResult {
                    service_key,
                    invocation_id,
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
                        Some((service_invocation_id.clone(), entry_index)),
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
                    invocation_id,
                    span_context,
                } = resolution_result;

                let_assert!(
                    Entry::BackgroundInvoke(BackgroundInvokeEntry {
                        request,
                        invoke_time
                    }) = Codec::deserialize(&journal_entry)?
                );

                let service_invocation = Self::create_service_invocation(
                    *invocation_id,
                    service_key.clone(),
                    request,
                    None,
                    span_context.clone(),
                );

                // 0 is equal to not set, meaning execute now
                if invoke_time == 0 {
                    self.send_message(
                        OutboxMessage::ServiceInvocation(service_invocation),
                        effects,
                    );
                } else {
                    effects.register_timer(TimerValue::new_invoke(
                        service_invocation_id.clone(),
                        MillisSinceEpoch::new(invoke_time),
                        entry_index,
                        service_invocation,
                    ));
                }
            }
            // special handling because we can have a completion present
            EnrichedEntryHeader::Awakeable { is_completed } => {
                debug_assert!(!is_completed, "Awakeable entry must not be completed.");
                effects.append_awakeable_entry(
                    service_invocation_id.service_id,
                    invocation_metadata,
                    entry_index,
                    journal_entry,
                );
                return Ok(());
            }
            EnrichedEntryHeader::CompleteAwakeable => {
                let_assert!(Entry::CompleteAwakeable(entry) = Codec::deserialize(&journal_entry)?);

                let response = Self::create_response_for_awakeable_entry(entry);
                self.send_message(response, effects);
            }
            EnrichedEntryHeader::Custom {
                requires_ack,
                code: _,
            } => {
                if requires_ack {
                    effects.append_journal_entry_and_ack_storage(
                        service_invocation_id.service_id,
                        invocation_metadata,
                        entry_index,
                        journal_entry,
                    );
                    return Ok(());
                }
            }
        }

        effects.append_journal_entry(
            service_invocation_id.service_id,
            invocation_metadata,
            entry_index,
            journal_entry,
        );

        Ok(())
    }

    async fn handle_completion<State: StateReader>(
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
        state: &mut State,
        effects: &mut Effects,
    ) -> Result<(Option<ServiceInvocationId>, SpanRelation), Error> {
        let status = state
            .get_invocation_status(&service_invocation_id.service_id)
            .await?;
        let mut related_sid = None;
        let mut span_relation = SpanRelation::None;

        match status {
            InvocationStatus::Invoked(metadata) => {
                if metadata.invocation_id == service_invocation_id.invocation_id {
                    effects.store_and_forward_completion(service_invocation_id.clone(), completion);
                    related_sid = Some(service_invocation_id);
                    span_relation = metadata.journal_metadata.span_context.as_parent();
                } else {
                    debug!(
                        rpc.service = %service_invocation_id.service_id.service_name,
                        restate.invocation.sid = %service_invocation_id,
                        ?completion,
                        "Ignoring completion for invocation that is no longer running."
                    );
                }
            }
            InvocationStatus::Suspended {
                metadata,
                waiting_for_completed_entries,
            } => {
                if metadata.invocation_id == service_invocation_id.invocation_id {
                    span_relation = metadata.journal_metadata.span_context.as_parent();

                    if waiting_for_completed_entries.contains(&completion.entry_index) {
                        effects.store_completion_and_resume(
                            service_invocation_id.service_id.clone(),
                            metadata,
                            completion,
                        );
                    } else {
                        effects.store_completion(service_invocation_id.clone(), completion);
                    }
                    related_sid = Some(service_invocation_id);
                } else {
                    debug!(
                        rpc.service = %service_invocation_id.service_id.service_name,
                        restate.invocation.sid = %service_invocation_id,
                        ?completion,
                        "Ignoring completion for invocation that is no longer running."
                    );
                }
            }
            InvocationStatus::Free => {
                debug!(
                    rpc.service = %service_invocation_id.service_id.service_name,
                    restate.invocation.sid = %service_invocation_id,
                    ?completion,
                    "Ignoring completion for invocation that is no longer running."
                )
            }
        }

        Ok((related_sid, span_relation))
    }

    fn notify_invocation_result(
        &mut self,
        service_invocation_id: &ServiceInvocationId,
        service_method: String,
        span_context: ServiceInvocationSpanContext,
        effects: &mut Effects,
        result: Result<(), (InvocationErrorCode, String)>,
    ) {
        effects.notify_invocation_result(
            service_invocation_id.clone(),
            service_method,
            span_context,
            result,
        );
    }

    async fn complete_invocation<State: StateReader>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        state: &mut State,
        journal_length: EntryIndex,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        if let Some(InboxEntry {
            inbox_sequence_number,
            service_invocation,
        }) = state.peek_inbox(&service_invocation_id.service_id).await?
        {
            effects.drop_journal_and_pop_inbox(
                service_invocation_id.service_id,
                inbox_sequence_number,
                journal_length,
            );
            effects.invoke_service(service_invocation);
        } else {
            effects.drop_journal_and_free_service(service_invocation_id.service_id, journal_length);
        }

        Ok(())
    }

    fn send_message(&mut self, message: OutboxMessage, effects: &mut Effects) {
        effects.enqueue_into_outbox(self.outbox_seq_number, message);
        self.outbox_seq_number += 1;
    }

    fn create_service_invocation(
        invocation_id: InvocationId,
        invocation_key: Bytes,
        invoke_request: InvokeRequest,
        response_target: Option<(ServiceInvocationId, EntryIndex)>,
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
            id: ServiceInvocationId::new(service_name, invocation_key, invocation_id),
            method_name,
            argument: parameter,
            response_sink,
            span_context,
        }
    }

    fn create_response_for_awakeable_entry(entry: CompleteAwakeableEntry) -> OutboxMessage {
        let CompleteAwakeableEntry {
            entry_index,
            result,
            service_name,
            instance_key,
            invocation_id,
        } = entry;
        OutboxMessage::ServiceResponse(InvocationResponse {
            entry_index,
            result: result.into(),
            id: ServiceInvocationId::new(
                service_name,
                instance_key,
                InvocationId::from_slice(&invocation_id)
                    .expect("Invocation id must be parse-able. If not, then this is a bug. Please contact the Restate developers."),
            ),
        })
    }

    fn create_response(
        callee: &ServiceInvocationId,
        response_sink: ServiceInvocationResponseSink,
        result: ResponseResult,
    ) -> OutboxMessage {
        match response_sink {
            ServiceInvocationResponseSink::PartitionProcessor {
                entry_index,
                caller,
            } => OutboxMessage::ServiceResponse(InvocationResponse {
                id: caller,
                entry_index,
                result,
            }),
            ServiceInvocationResponseSink::Ingress(ingress_id) => OutboxMessage::IngressResponse {
                ingress_id,
                service_invocation_id: callee.clone(),
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
