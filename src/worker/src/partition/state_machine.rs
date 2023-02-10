use bytes::Bytes;
use common::types::{EntryIndex, Response, ServiceInvocation, ServiceInvocationId};
use journal::raw::{RawEntry, RawEntryCodec};
use journal::{
    BackgroundInvokeEntry, ClearStateEntry, CompleteAwakeableEntry, Completion, CompletionResult,
    Entry, EntryType, InvokeEntry, InvokeRequest, SetStateEntry, SleepEntry,
};
use std::fmt::Debug;
use std::marker::PhantomData;
use storage_api::StorageReader;
use tracing::debug;

mod storage;

use storage::StorageReaderHelper;
use crate::partition::effects::{Effects, OutboxMessage};
use crate::partition::state_machine::storage::InvocationStatus;

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error<E>(#[from] E);

#[derive(Debug)]
pub(crate) enum Command {
    Invoker(invoker::OutputEffect),
    Timer {
        service_invocation_id: ServiceInvocationId,
        entry_index: EntryIndex,
        timestamp: i64,
    },
    OutboxTruncation(u64),
    Invocation(ServiceInvocation),
    Response(Response),
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
    pub(super) fn on_apply<Storage: StorageReader>(
        &mut self,
        command: Command,
        effects: &mut Effects,
        storage: &Storage,
    ) -> Result<(), Error<Codec::Error>> {
        debug!(?command, "Apply");

        let storage = StorageReaderHelper::new(storage);

        match command {
            Command::Invocation(service_invocation) => {
                let status = storage.get_invocation_status(&service_invocation.id.service_id);

                if status == InvocationStatus::Free {
                    effects.invoke_service(service_invocation);
                } else {
                    effects.enqueue_into_inbox(self.inbox_seq_number, service_invocation);
                    self.inbox_seq_number += 1;
                }
            }
            Command::Response(Response {
                id,
                entry_index,
                result,
            }) => {
                let completion = Completion {
                    entry_index,
                    result: result.into(),
                };

                Self::handle_completion(id, completion, &storage, effects)
            }
            Command::Invoker(invoker::OutputEffect {
                service_invocation_id,
                kind,
            }) => {
                let status = storage.get_invocation_status(&service_invocation_id.service_id);

                debug_assert!(
                    matches!(
                        status,
                        InvocationStatus::Invoked(invocation_id) if service_invocation_id.invocation_id == invocation_id
                    ),
                    "Expect to only receive invoker messages when being invoked"
                );

                match kind {
                    invoker::Kind::JournalEntry { entry_index, entry } => {
                        let journal_length =
                            storage.get_journal_length(&service_invocation_id.service_id);

                        debug_assert_eq!(
                            entry_index,
                            journal_length + 1,
                            "Expect to receive next journal entry"
                        );

                        match entry.header.ty {
                            EntryType::Invoke => {
                                let InvokeEntry { request, .. } =
                                    enum_inner!(Self::deserialize(&entry)?, Entry::Invoke);

                                let service_invocation = Self::create_service_invocation(
                                    request,
                                    Some((service_invocation_id.clone(), entry_index)),
                                );
                                self.send_message(
                                    OutboxMessage::Invocation(service_invocation),
                                    effects,
                                );
                            }
                            EntryType::BackgroundInvoke => {
                                let BackgroundInvokeEntry(request) = enum_inner!(
                                    Self::deserialize(&entry)?,
                                    Entry::BackgroundInvoke
                                );

                                let service_invocation =
                                    Self::create_service_invocation(request, None);
                                self.send_message(
                                    OutboxMessage::Invocation(service_invocation),
                                    effects,
                                );
                            }
                            EntryType::CompleteAwakeable => {
                                let entry = enum_inner!(
                                    Self::deserialize(&entry)?,
                                    Entry::CompleteAwakeable
                                );

                                let response = Self::create_response_for_awakeable_entry(entry);
                                self.send_message(OutboxMessage::Response(response), effects);
                            }
                            EntryType::SetState => {
                                let SetStateEntry { key, value } =
                                    enum_inner!(Self::deserialize(&entry)?, Entry::SetState);

                                effects.set_state(
                                    service_invocation_id.service_id.clone(),
                                    key,
                                    value,
                                );
                            }
                            EntryType::ClearState => {
                                let ClearStateEntry { key } =
                                    enum_inner!(Self::deserialize(&entry)?, Entry::ClearState);
                                effects.clear_state(service_invocation_id.service_id.clone(), key);
                            }
                            EntryType::Sleep => {
                                let SleepEntry { wake_up_time, .. } =
                                    enum_inner!(Self::deserialize(&entry)?, Entry::Sleep);
                                effects.register_timer(
                                    wake_up_time,
                                    service_invocation_id.clone(),
                                    entry_index,
                                );
                            }

                            // nothing to do
                            EntryType::GetState => {}
                            EntryType::Custom(_) => {}
                            EntryType::PollInputStream => {}
                            EntryType::OutputStream => {}

                            // special handling because we can have a completion present
                            EntryType::Awakeable => {
                                effects.append_awakeable_entry(
                                    service_invocation_id.service_id,
                                    entry_index,
                                    entry,
                                );
                                return Ok(());
                            }
                        }

                        effects.append_journal_entry(
                            service_invocation_id.service_id,
                            entry_index,
                            entry,
                        );
                    }
                    invoker::Kind::Suspended {
                        journal_revision: expected_journal_revision,
                    } => {
                        let actual_journal_revision =
                            storage.get_journal_revision(&service_invocation_id.service_id);

                        if actual_journal_revision > expected_journal_revision {
                            effects.resume_service(service_invocation_id.service_id);
                        } else {
                            effects.suspend_service(service_invocation_id.service_id);
                        }
                    }
                    invoker::Kind::End => {
                        self.complete_invocation(
                            service_invocation_id,
                            CompletionResult::Success(Bytes::new()),
                            &storage,
                            effects,
                        );
                    }
                    invoker::Kind::Failed { error } => {
                        self.complete_invocation(
                            service_invocation_id,
                            CompletionResult::Failure(502, error.to_string().into()),
                            &storage,
                            effects,
                        );
                    }
                }
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
                Self::handle_completion(service_invocation_id, completion, &storage, effects);
            }
        }

        Ok(())
    }

    fn handle_completion(
        service_invocation_id: ServiceInvocationId,
        completion: Completion,
        storage: &StorageReaderHelper,
        effects: &mut Effects,
    ) {
        let status = storage.get_invocation_status(&service_invocation_id.service_id);

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
            InvocationStatus::Suspended(invocation_id) => {
                if invocation_id == service_invocation_id.invocation_id {
                    effects.resume_service(service_invocation_id.service_id.clone());
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
    }

    fn complete_invocation(
        &mut self,
        service_invocation_id: ServiceInvocationId,
        completion_result: CompletionResult,
        storage: &StorageReaderHelper,
        effects: &mut Effects,
    ) {
        effects.drop_journal(service_invocation_id.service_id.clone());

        if let Some(service_invocation) = storage.peek_inbox(&service_invocation_id.service_id) {
            effects.pop_inbox(service_invocation_id.service_id);
            effects.invoke_service(service_invocation);
        } else {
            effects.mark_service_instance_as_free(service_invocation_id.service_id);
        }

        let response = Self::create_response(completion_result);

        self.send_message(OutboxMessage::Response(response), effects);
    }

    fn send_message(&mut self, message: OutboxMessage, effects: &mut Effects) {
        effects.enqueue_into_outbox(self.outbox_seq_number, message);
        self.outbox_seq_number += 1;
    }

    fn create_service_invocation(
        invoke_request: InvokeRequest,
        response_target: Option<(ServiceInvocationId, EntryIndex)>,
    ) -> ServiceInvocation {
        // We might want to create the service invocation when receiving the journal entry from
        // service endpoint. That way we can fail it fast if the service cannot be resolved.
        unimplemented!()
    }

    fn create_response_for_awakeable_entry(entry: CompleteAwakeableEntry) -> Response {
        unimplemented!()
    }

    fn create_response(result: CompletionResult) -> Response {
        unimplemented!()
    }

    fn deserialize(raw_entry: &RawEntry) -> Result<Entry, Error<Codec::Error>> {
        Codec::deserialize(raw_entry).map_err(Into::into)
    }
}
