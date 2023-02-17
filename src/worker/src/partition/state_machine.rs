use bytes::Bytes;
use common::types::{
    EntryIndex, InvocationResponse, ServiceId, ServiceInvocation, ServiceInvocationId,
};
use futures::future::BoxFuture;
use invoker::Kind;
use journal::raw::{RawEntry, RawEntryCodec, RawEntryHeader};
use journal::{
    BackgroundInvokeEntry, ClearStateEntry, CompleteAwakeableEntry, Completion, CompletionResult,
    Entry, EntryResult, GetStateEntry, InvokeEntry, InvokeRequest, OutputStreamEntry,
    SetStateEntry, SleepEntry,
};
use std::fmt::Debug;
use std::marker::PhantomData;
use tracing::{debug, trace};

use crate::partition::effects::{Effects, OutboxMessage};
use crate::partition::InvocationStatus;

#[derive(Debug, thiserror::Error)]
pub enum Error<S, C> {
    #[error("failed to read from state reader")]
    State(S),
    #[error("failed to deserialize state")]
    Codec(C),
}

#[derive(Debug)]
pub(crate) enum Command {
    Invoker(invoker::OutputEffect),
    #[allow(dead_code)]
    Timer {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        timestamp: u64,
    },
    #[allow(dead_code)]
    OutboxTruncation(u64),
    #[allow(dead_code)]
    Invocation(ServiceInvocation),
    #[allow(dead_code)]
    Response(InvocationResponse),
}

pub(super) struct JournalStatus {
    pub(super) length: EntryIndex,
}

pub type InboxEntry = (u64, ServiceInvocation);

pub(super) trait StateReader {
    type Error;

    // TODO: Replace with async trait or proper future
    fn get_invocation_status(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<InvocationStatus, Self::Error>>;

    // TODO: Replace with async trait or proper future
    fn peek_inbox(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<Option<InboxEntry>, Self::Error>>;

    // TODO: Replace with async trait or proper future
    fn get_journal_status(
        &self,
        service_id: &ServiceId,
    ) -> BoxFuture<Result<JournalStatus, Self::Error>>;

    // TODO: Replace with async trait or proper future
    fn is_entry_completed(
        &self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> BoxFuture<Result<bool, Self::Error>>;
}

#[derive(Debug, Default)]
pub(super) struct StateMachine<Codec> {
    // initialized from persistent storage
    inbox_seq_number: u64,
    outbox_seq_number: u64,

    _codec: PhantomData<Codec>,
}

/// Unwraps the inner value of a given enum variant.
///
/// # Panics
/// If the enum variant does not match the given enum variant, it panics.
///
/// # Example
///
/// ```
/// enum Enum {
///     A(u64),
///     B(String),
/// }
///
/// let variant = Enum::A(42);
///
/// let inner = enum_inner!(variant, Enum::A);
/// assert_eq!(inner, 42);
/// ```
///
/// ## Expansion
///
/// The given example will expand to:
///
/// ```
/// enum Enum {
///     A(u64),
///     B(String),
/// }
///
/// let variant = Enum::A(42);
///
/// let inner = match variant {
///     Enum::A(inner) => inner,
///     _ => panic!()
/// };
/// ```
macro_rules! enum_inner {
    ($ty:expr, $variant:path) => {
        match $ty {
            $variant(inner) => inner,
            _ => panic!("Unexpected enum type"),
        }
    };
}

impl<Codec> StateMachine<Codec>
where
    Codec: RawEntryCodec,
    Codec::Error: Debug,
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
    ) -> Result<(), Error<State::Error, Codec::Error>> {
        debug!(?command, "Apply");

        match command {
            Command::Invocation(service_invocation) => {
                let status = state
                    .get_invocation_status(&service_invocation.id.service_id)
                    .await
                    .map_err(Error::State)?;

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
            Command::Timer {
                service_invocation_id,
                entry_index,
                timestamp: wake_up_time,
            } => {
                effects.delete_timer(
                    wake_up_time,
                    service_invocation_id.service_id.clone(),
                    entry_index,
                );

                let completion = Completion {
                    entry_index,
                    result: CompletionResult::Success(Bytes::new()),
                };
                Self::handle_completion(service_invocation_id, completion, state, effects).await?;
            }
        }

        Ok(())
    }

    async fn on_invoker_effect<State: StateReader>(
        &mut self,
        effects: &mut Effects,
        state: &State,
        invoker::OutputEffect {
            service_invocation_id,
            kind,
        }: invoker::OutputEffect,
    ) -> Result<(), Error<State::Error, Codec::Error>> {
        let status = state
            .get_invocation_status(&service_invocation_id.service_id)
            .await
            .map_err(Error::State)?;

        debug_assert!(
            matches!(
                status,
                InvocationStatus::Invoked(invocation_id) if service_invocation_id.invocation_id == invocation_id
            ),
            "Expect to only receive invoker messages when being invoked"
        );

        match kind {
            Kind::JournalEntry { entry_index, entry } => {
                self.handle_journal_entry(
                    effects,
                    state,
                    service_invocation_id,
                    entry_index,
                    entry,
                )
                .await?;
            }
            Kind::Suspended {
                waiting_for_completed_entries,
            } => {
                debug_assert!(
                    !waiting_for_completed_entries.is_empty(),
                    "Expecting at least one entry on which the invocation {service_invocation_id:?} is waiting."
                );
                for entry_index in &waiting_for_completed_entries {
                    if state
                        .is_entry_completed(&service_invocation_id.service_id, *entry_index)
                        .await
                        .map_err(Error::State)?
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
            Kind::End => {
                self.complete_invocation(service_invocation_id, state, effects)
                    .await?;
            }
            Kind::Failed { error, error_code } => {
                let response = Self::create_response(EntryResult::Failure(
                    error_code,
                    error.to_string().into(),
                ));

                // TODO: We probably only need to send the response if we haven't send a response before
                self.send_message(OutboxMessage::Response(response), effects);

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
        entry: RawEntry,
    ) -> Result<(), Error<State::Error, Codec::Error>> {
        let journal_length = state
            .get_journal_status(&service_invocation_id.service_id)
            .await
            .map_err(Error::State)?
            .length;

        debug_assert_eq!(
            entry_index,
            journal_length + 1,
            "Expect to receive next journal entry"
        );

        match entry.header {
            // nothing to do
            RawEntryHeader::PollInputStream { is_completed } => {
                debug_assert!(
                    !is_completed,
                    "Poll input stream entry must not be completed."
                );
            }
            RawEntryHeader::OutputStream => {
                let OutputStreamEntry { result } = enum_inner!(
                    Self::deserialize(&entry).map_err(Error::Codec)?,
                    Entry::OutputStream
                );

                let response = Self::create_response(result);

                self.send_message(OutboxMessage::Response(response), effects);
            }
            RawEntryHeader::GetState { is_completed } => {
                if !is_completed {
                    let GetStateEntry { key, .. } = enum_inner!(
                        Self::deserialize(&entry).map_err(Error::Codec)?,
                        Entry::GetState
                    );

                    effects.get_state_and_append_completed_entry(
                        key,
                        service_invocation_id,
                        entry_index,
                        entry,
                    );
                    return Ok(());
                }
            }
            RawEntryHeader::SetState => {
                let SetStateEntry { key, value } = enum_inner!(
                    Self::deserialize(&entry).map_err(Error::Codec)?,
                    Entry::SetState
                );

                effects.set_state(service_invocation_id, key, value, entry, entry_index);
                // set_state includes append journal entry in order to avoid unnecessary clones.
                // That's why we must return here.
                return Ok(());
            }
            RawEntryHeader::ClearState => {
                let ClearStateEntry { key } = enum_inner!(
                    Self::deserialize(&entry).map_err(Error::Codec)?,
                    Entry::ClearState
                );
                effects.clear_state(service_invocation_id, key, entry, entry_index);
                // clear_state includes append journal entry in order to avoid unnecessary clones.
                // That's why we must return here.
                return Ok(());
            }
            RawEntryHeader::Sleep { is_completed } => {
                debug_assert!(!is_completed, "Sleep entry must not be completed.");
                let SleepEntry { wake_up_time, .. } = enum_inner!(
                    Self::deserialize(&entry).map_err(Error::Codec)?,
                    Entry::Sleep
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
            RawEntryHeader::Invoke { is_completed } => {
                if !is_completed {
                    let InvokeEntry { request, .. } = enum_inner!(
                        Self::deserialize(&entry).map_err(Error::Codec)?,
                        Entry::Invoke
                    );

                    let service_invocation = Self::create_service_invocation(
                        request,
                        Some((service_invocation_id.clone(), entry_index)),
                    );
                    self.send_message(OutboxMessage::Invocation(service_invocation), effects);
                } else {
                    // no action needed for a completed invoke entry
                }
            }
            RawEntryHeader::BackgroundInvoke => {
                let BackgroundInvokeEntry(request) = enum_inner!(
                    Self::deserialize(&entry).map_err(Error::Codec)?,
                    Entry::BackgroundInvoke
                );

                let service_invocation = Self::create_service_invocation(request, None);
                self.send_message(OutboxMessage::Invocation(service_invocation), effects);
            }
            // special handling because we can have a completion present
            RawEntryHeader::Awakeable { is_completed } => {
                debug_assert!(!is_completed, "Awakeable entry must not be completed.");
                effects.append_awakeable_entry(service_invocation_id, entry_index, entry);
                return Ok(());
            }
            RawEntryHeader::CompleteAwakeable => {
                let entry = enum_inner!(
                    Self::deserialize(&entry).map_err(Error::Codec)?,
                    Entry::CompleteAwakeable
                );

                let response = Self::create_response_for_awakeable_entry(entry);
                self.send_message(OutboxMessage::Response(response), effects);
            }
            RawEntryHeader::Custom {
                requires_ack,
                code: _,
            } => {
                if requires_ack {
                    effects.append_journal_entry_and_ack_storage(
                        service_invocation_id,
                        entry_index,
                        entry,
                    );
                    return Ok(());
                }
            }
        }

        effects.append_journal_entry(service_invocation_id, entry_index, entry);

        Ok(())
    }

    async fn handle_completion<State: StateReader>(
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
        state: &State,
        effects: &mut Effects,
    ) -> Result<(), Error<State::Error, Codec::Error>> {
        let status = state
            .get_invocation_status(&service_invocation_id.service_id)
            .await
            .map_err(Error::State)?;

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
                            .await
                            .map_err(Error::State)?
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
    ) -> Result<(), Error<State::Error, Codec::Error>> {
        if let Some((inbox_sequence_number, service_invocation)) = state
            .peek_inbox(&service_invocation_id.service_id)
            .await
            .map_err(Error::State)?
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
        _invoke_request: InvokeRequest,
        _response_target: Option<(ServiceInvocationId, EntryIndex)>,
    ) -> ServiceInvocation {
        // We might want to create the service invocation when receiving the journal entry from
        // service endpoint. That way we can fail it fast if the service cannot be resolved.
        todo!()
    }

    fn create_response_for_awakeable_entry(_entry: CompleteAwakeableEntry) -> InvocationResponse {
        todo!()
    }

    fn create_response(_result: EntryResult) -> InvocationResponse {
        todo!()
    }

    fn deserialize(raw_entry: &RawEntry) -> Result<Entry, Codec::Error> {
        Codec::deserialize(raw_entry)
    }
}
