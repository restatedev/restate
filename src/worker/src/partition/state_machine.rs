use assert2::let_assert;
use bytes::Bytes;
use common::types::{
    EntryIndex, IngressId, InvocationId, InvocationResponse, MessageIndex, ResponseResult,
    ServiceId, ServiceInvocation, ServiceInvocationId, ServiceInvocationResponseSink, SpanRelation,
};
use common::utils::GenericError;
use futures::future::BoxFuture;
use journal::raw::{RawEntryCodec, RawEntryCodecError};
use journal::{
    BackgroundInvokeEntry, ClearStateEntry, CompleteAwakeableEntry, Completion, CompletionResult,
    Entry, GetStateEntry, InvokeEntry, InvokeRequest, OutputStreamEntry, SetStateEntry, SleepEntry,
};
use std::fmt::Debug;
use std::marker::PhantomData;
use tracing::{debug, trace, warn};

use crate::partition::effects::{Effects, OutboxMessage};
use crate::partition::types::{
    EnrichedEntryHeader, EnrichedRawEntry, InvokerEffect, InvokerEffectKind, ResolutionResult,
    Timer,
};
use crate::partition::InvocationStatus;

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("failed to read from state reader: {0}")]
    State(#[from] StateReaderError),
    #[error("failed to deserialize entry: {0}")]
    Codec(#[from] RawEntryCodecError),
}

#[derive(Debug)]
pub(crate) enum Command {
    Invoker(InvokerEffect),
    Timer(Timer),
    OutboxTruncation(MessageIndex),
    Invocation(ServiceInvocation),
    Response(InvocationResponse),
}

#[derive(Debug, Default)]
pub(super) struct JournalStatus {
    pub(super) length: EntryIndex,
}

#[derive(Debug, Clone)]
pub(super) enum ResponseSink {
    Ingress(IngressId, ServiceInvocationId),
    PartitionProcessor(ServiceInvocationId, EntryIndex),
}

impl ResponseSink {
    #[allow(dead_code)]
    pub(super) fn from_service_invocation_response_sink(
        service_invocation_id: &ServiceInvocationId,
        response_sink: &ServiceInvocationResponseSink,
    ) -> Option<ResponseSink> {
        match response_sink {
            ServiceInvocationResponseSink::Ingress(ingress_id) => Some(ResponseSink::Ingress(
                *ingress_id,
                service_invocation_id.clone(),
            )),
            ServiceInvocationResponseSink::None => None,
            ServiceInvocationResponseSink::PartitionProcessor {
                entry_index,
                caller,
            } => Some(ResponseSink::PartitionProcessor(
                caller.clone(),
                *entry_index,
            )),
        }
    }
}

pub type InboxEntry = (MessageIndex, ServiceInvocation);

#[derive(Debug, thiserror::Error)]
#[error("failed reading state: {source:?}")]
pub(super) struct StateReaderError {
    source: Option<GenericError>,
}

pub(super) trait StateReader {
    // TODO: Replace with async trait or proper future
    fn get_invocation_status(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<InvocationStatus, StateReaderError>>;

    // TODO: Replace with async trait or proper future
    fn peek_inbox(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<Option<InboxEntry>, StateReaderError>>;

    // TODO: Replace with async trait or proper future
    fn get_journal_status(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<JournalStatus, StateReaderError>>;

    // TODO: Replace with async trait or proper future
    fn is_entry_completed(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<bool, StateReaderError>>;

    // TODO: Replace with async trait or proper future
    fn get_response_sink(
        &self,
        service_invocation_id: &ServiceInvocationId,
    ) -> BoxFuture<Result<Option<ResponseSink>, StateReaderError>>;
}

#[derive(Debug, Default)]
pub(super) struct StateMachine<Codec> {
    // initialized from persistent storage
    inbox_seq_number: MessageIndex,
    outbox_seq_number: MessageIndex,

    _codec: PhantomData<Codec>,
}

impl<Codec> StateMachine<Codec>
where
    Codec: RawEntryCodec,
{
    /// Applies the given command and returns effects via the provided effects struct
    ///
    /// We pass in the effects message as a mutable borrow to be able to reuse it across
    /// invocations of this methods which lies on the hot path.
    pub(super) async fn on_apply<State: StateReader>(
        &mut self,
        command: Command,
        effects: &mut Effects,
        state: &State,
    ) -> Result<(), Error> {
        debug!(?command, "Apply");

        match command {
            Command::Invocation(service_invocation) => {
                let status = state
                    .get_invocation_status(&service_invocation.id.service_id)
                    .await?;

                if status == InvocationStatus::Free {
                    effects.invoke_service(service_invocation);
                } else {
                    effects.enqueue_into_inbox(self.inbox_seq_number, service_invocation);
                    self.inbox_seq_number += 1;
                }
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

                Self::handle_completion(id, completion, state, effects).await?;
            }
            Command::Invoker(effect) => {
                self.on_invoker_effect(effects, state, effect).await?;
            }
            Command::OutboxTruncation(index) => {
                effects.truncate_outbox(index);
            }
            Command::Timer(timer) => {
                self.on_timer(timer, state, effects).await?;
            }
        }

        Ok(())
    }

    async fn on_timer<State: StateReader>(
        &mut self,
        Timer {
            service_invocation_id,
            entry_index,
            wake_up_time,
        }: Timer,
        state: &State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        effects.delete_timer(
            wake_up_time,
            service_invocation_id.service_id.clone(),
            entry_index,
        );

        let completion = Completion {
            entry_index,
            result: CompletionResult::Empty,
        };
        Self::handle_completion(service_invocation_id, completion, state, effects).await?;

        Ok(())
    }

    async fn on_invoker_effect<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &State,
        InvokerEffect {
            service_invocation_id,
            kind,
        }: InvokerEffect,
    ) -> Result<(), Error> {
        let status = state
            .get_invocation_status(&service_invocation_id.service_id)
            .await?;

        debug_assert!(
            matches!(
                status,
                InvocationStatus::Invoked(invocation_id) if service_invocation_id.invocation_id == invocation_id
            ),
            "Expect to only receive invoker messages when being invoked"
        );

        match kind {
            InvokerEffectKind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    effects,
                    state,
                    service_invocation_id,
                    entry_index,
                    entry,
                )
                .await?;
            }
            InvokerEffectKind::Suspended {
                waiting_for_completed_entries,
            } => {
                debug_assert!(
                    !waiting_for_completed_entries.is_empty(),
                    "Expecting at least one entry on which the invocation {service_invocation_id:?} is waiting."
                );
                for entry_index in &waiting_for_completed_entries {
                    if state
                        .is_entry_completed(&service_invocation_id.service_id, *entry_index)
                        .await?
                    {
                        trace!(
                        ?service_invocation_id,
                        "Resuming instead of suspending service because an awaited entry is completed.");
                        effects.resume_service(service_invocation_id);
                        return Ok(());
                    }
                }

                effects.suspend_service(service_invocation_id, waiting_for_completed_entries);
            }
            InvokerEffectKind::End => {
                self.complete_invocation(service_invocation_id, state, effects)
                    .await?;
            }
            InvokerEffectKind::Failed { error, error_code } => {
                if let Some(response_sink) = state.get_response_sink(&service_invocation_id).await?
                {
                    let outbox_message = Self::create_response(
                        response_sink,
                        ResponseResult::Failure(error_code, error.to_string().into()),
                    );

                    // TODO: We probably only need to send the response if we haven't send a response before
                    self.send_message(outbox_message, effects);
                }

                self.complete_invocation(service_invocation_id, state, effects)
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_journal_entry<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &State,
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> Result<(), Error> {
        let journal_length = state
            .get_journal_status(&service_invocation_id.service_id)
            .await?
            .length;

        debug_assert_eq!(
            entry_index, journal_length,
            "Expect to receive next journal entry"
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
                if let Some(response_sink) = state.get_response_sink(&service_invocation_id).await?
                {
                    let_assert!(
                        Entry::OutputStream(OutputStreamEntry { result }) =
                            Codec::deserialize(&journal_entry)?
                    );

                    let outbox_message = Self::create_response(response_sink, result.into());

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
                        key,
                        service_invocation_id,
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
                    service_invocation_id,
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
                effects.clear_state(service_invocation_id, key, journal_entry, entry_index);
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
                    wake_up_time as u64,
                    // Registering a timer generates multiple effects: timer registration and
                    // journal append which each generate actuator messages for the timer service
                    // and the invoker --> Cloning required
                    service_invocation_id.clone(),
                    entry_index,
                );
            }
            EnrichedEntryHeader::Invoke {
                ref resolution_result,
                ..
            } => {
                if let Some(resolution_result) = resolution_result {
                    match resolution_result {
                        ResolutionResult::Success {
                            service_key,
                            invocation_id,
                        } => {
                            let_assert!(
                                Entry::Invoke(InvokeEntry { request, .. }) =
                                    Codec::deserialize(&journal_entry)?
                            );

                            let service_invocation = Self::create_service_invocation(
                                *invocation_id,
                                service_key.clone(),
                                request,
                                Some((service_invocation_id.clone(), entry_index)),
                            );
                            self.send_message(
                                OutboxMessage::ServiceInvocation(service_invocation),
                                effects,
                            );
                        }
                        ResolutionResult::Failure { error_code, error } => effects
                            .forward_completion(
                                service_invocation_id.clone(),
                                Completion::new(
                                    entry_index,
                                    CompletionResult::Failure(*error_code, error.clone()),
                                ),
                            ),
                    }
                } else {
                    // no action needed for an invoke entry that has been completed by the service endpoint
                }
            }
            EnrichedEntryHeader::BackgroundInvoke {
                ref resolution_result,
            } => match resolution_result {
                ResolutionResult::Success {
                    service_key,
                    invocation_id,
                } => {
                    let_assert!(
                        Entry::BackgroundInvoke(BackgroundInvokeEntry(request)) =
                            Codec::deserialize(&journal_entry)?
                    );

                    let service_invocation = Self::create_service_invocation(
                        *invocation_id,
                        service_key.clone(),
                        request,
                        None,
                    );
                    self.send_message(
                        OutboxMessage::ServiceInvocation(service_invocation),
                        effects,
                    );
                }
                ResolutionResult::Failure { error_code, error } => {
                    warn!("Failed to send background invocation: Error code: {error_code}, error message: {error}");
                }
            },
            // special handling because we can have a completion present
            EnrichedEntryHeader::Awakeable { is_completed } => {
                debug_assert!(!is_completed, "Awakeable entry must not be completed.");
                effects.append_awakeable_entry(service_invocation_id, entry_index, journal_entry);
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
                        service_invocation_id,
                        entry_index,
                        journal_entry,
                    );
                    return Ok(());
                }
            }
        }

        effects.append_journal_entry(service_invocation_id, entry_index, journal_entry);

        Ok(())
    }

    async fn handle_completion<State: StateReader>(
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
        state: &State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        let status = state
            .get_invocation_status(&service_invocation_id.service_id)
            .await?;

        match status {
            InvocationStatus::Invoked(invocation_id) => {
                if invocation_id == service_invocation_id.invocation_id {
                    effects.store_and_forward_completion(service_invocation_id, completion);
                } else {
                    debug!(
                        ?completion,
                        "Ignoring completion for invocation that is no longer running."
                    );
                }
            }
            InvocationStatus::Suspended {
                invocation_id,
                waiting_for_completed_entries,
            } => {
                if invocation_id == service_invocation_id.invocation_id {
                    for entry_index in &waiting_for_completed_entries {
                        if state
                            .is_entry_completed(&service_invocation_id.service_id, *entry_index)
                            .await?
                        {
                            effects.store_completion_and_resume(service_invocation_id, completion);
                            return Ok(());
                        }
                    }

                    effects.store_completion(service_invocation_id, completion);
                } else {
                    debug!(
                        ?completion,
                        "Ignoring completion for invocation that is no longer running."
                    );
                }
            }
            InvocationStatus::Free => {
                debug!(
                    ?completion,
                    "Ignoring completion for invocation that is no longer running."
                )
            }
        }

        Ok(())
    }

    async fn complete_invocation<State: StateReader>(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        state: &State,
        effects: &mut Effects,
    ) -> Result<(), Error> {
        if let Some((inbox_sequence_number, service_invocation)) =
            state.peek_inbox(&service_invocation_id.service_id).await?
        {
            effects.drop_journal_and_pop_inbox(
                service_invocation_id.service_id,
                inbox_sequence_number,
            );
            effects.invoke_service(service_invocation);
        } else {
            effects.drop_journal_and_free_service(service_invocation_id.service_id);
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
    ) -> ServiceInvocation {
        let InvokeRequest {
            service_name,
            method_name,
            parameter,
        } = invoke_request;

        let response_sink = if let Some((caller, entry_index)) = response_target {
            ServiceInvocationResponseSink::PartitionProcessor {
                caller,
                entry_index,
            }
        } else {
            ServiceInvocationResponseSink::None
        };

        ServiceInvocation {
            id: ServiceInvocationId::new(service_name, invocation_key, invocation_id),
            method_name,
            argument: parameter,
            response_sink,
            // TODO: Propagate span relation. See https://github.com/restatedev/restate/issues/165
            span_relation: SpanRelation::None,
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

    fn create_response(response_sink: ResponseSink, result: ResponseResult) -> OutboxMessage {
        match response_sink {
            ResponseSink::Ingress(ingress_id, service_invocation_id) => {
                OutboxMessage::IngressResponse {
                    ingress_id,
                    service_invocation_id,
                    response: result,
                }
            }
            ResponseSink::PartitionProcessor(service_invocation_id, entry_index) => {
                OutboxMessage::ServiceResponse(InvocationResponse {
                    id: service_invocation_id,
                    entry_index,
                    result,
                })
            }
        }
    }
}
