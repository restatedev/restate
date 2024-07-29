// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{Effects, Error};

use crate::partition::state_machine::actions::Action;
use crate::partition::state_machine::effects::Effect;
use assert2::let_assert;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::idempotency_table::IdempotencyMetadata;
use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
use restate_storage_api::invocation_status_table::{InFlightInvocationMetadata, InvocationStatus};
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::service_status_table::VirtualObjectStatus;
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_storage_api::Result as StorageResult;
use restate_types::identifiers::{EntryIndex, InvocationId, ServiceId};
use restate_types::invocation::{
    InvocationInput, InvocationTargetType, VirtualObjectHandlerType, WorkflowHandlerType,
};
use restate_types::journal::enriched::EnrichedRawEntry;
use restate_types::journal::raw::{PlainRawEntry, RawEntryCodec};
use restate_types::journal::{Completion, CompletionResult, EntryType};
use restate_types::message::MessageIndex;
use restate_types::state_mut::{ExternalStateMutation, StateMutationVersion};
use std::future::Future;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use tracing::{debug, warn};

pub type ActionCollector = Vec<Action>;

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

pub(crate) struct EffectInterpreter<Codec> {
    _codec: PhantomData<Codec>,
}

impl<Codec: RawEntryCodec> EffectInterpreter<Codec> {
    pub(crate) async fn interpret_effects<
        S: StateStorage
            + restate_storage_api::invocation_status_table::ReadOnlyInvocationStatusTable
            + restate_storage_api::idempotency_table::IdempotencyTable
            + restate_storage_api::promise_table::PromiseTable,
    >(
        effects: &mut Effects,
        state_storage: &mut S,
        action_collector: &mut ActionCollector,
    ) -> Result<(), Error> {
        for effect in effects.drain() {
            Self::interpret_effect(effect, state_storage, action_collector).await?;
        }

        Ok(())
    }

    async fn interpret_effect<
        S: StateStorage
            + restate_storage_api::invocation_status_table::ReadOnlyInvocationStatusTable
            + restate_storage_api::idempotency_table::IdempotencyTable
            + restate_storage_api::promise_table::PromiseTable,
    >(
        effect: Effect,
        state_storage: &mut S,
        collector: &mut ActionCollector,
    ) -> Result<(), Error> {
        match effect {
            Effect::InvokeService(service_invocation) => {
                let invocation_id = service_invocation.invocation_id;
                let (in_flight_invocation_meta, invocation_input) =
                    InFlightInvocationMetadata::from_service_invocation(service_invocation);
                Self::invoke_service(
                    state_storage,
                    collector,
                    invocation_id,
                    in_flight_invocation_meta,
                    invocation_input,
                )
                .await?;
            }
            Effect::ResumeService {
                invocation_id,
                mut metadata,
            } => {
                metadata.timestamps.update();
                let invocation_target = metadata.invocation_target.clone();
                state_storage
                    .store_invocation_status(&invocation_id, InvocationStatus::Invoked(metadata))
                    .await?;

                collector.push(Action::Invoke {
                    invocation_id,
                    invocation_target,
                    invoke_input_journal: InvokeInputJournal::NoCachedJournal,
                });
            }
            Effect::SuspendService {
                invocation_id,
                mut metadata,
                waiting_for_completed_entries,
            } => {
                metadata.timestamps.update();
                state_storage
                    .store_invocation_status(
                        &invocation_id,
                        InvocationStatus::Suspended {
                            metadata,
                            waiting_for_completed_entries,
                        },
                    )
                    .await?;
            }
            Effect::StoreInboxedInvocation(invocation_id, inboxed) => {
                state_storage
                    .store_invocation_status(&invocation_id, InvocationStatus::Inboxed(inboxed))
                    .await?;
            }
            Effect::StoreCompletedInvocation {
                invocation_id,
                retention,
                completed_invocation,
            } => {
                state_storage
                    .store_invocation_status(
                        &invocation_id,
                        InvocationStatus::Completed(completed_invocation),
                    )
                    .await?;
                collector.push(Action::ScheduleInvocationStatusCleanup {
                    invocation_id,
                    retention,
                });
            }
            Effect::FreeInvocation(invocation_id) => {
                state_storage
                    .store_invocation_status(&invocation_id, InvocationStatus::Free)
                    .await?;
            }
            Effect::EnqueueIntoInbox {
                seq_number,
                inbox_entry,
            } => {
                state_storage
                    .enqueue_into_inbox(seq_number, inbox_entry)
                    .await?;
                // need to store the next inbox sequence number
                state_storage.store_inbox_seq_number(seq_number + 1).await?;
            }
            Effect::PopInbox(service_id) => {
                Self::pop_from_inbox(state_storage, collector, service_id).await?;
            }
            Effect::DeleteInboxEntry {
                service_id,
                sequence_number,
            } => {
                state_storage
                    .delete_inbox_entry(&service_id, sequence_number)
                    .await;
            }
            Effect::EnqueueIntoOutbox {
                seq_number,
                message,
            } => {
                state_storage
                    .enqueue_into_outbox(seq_number, message.clone())
                    .await?;
                // need to store the next outbox sequence number
                state_storage
                    .store_outbox_seq_number(seq_number + 1)
                    .await?;

                collector.push(Action::NewOutboxMessage {
                    seq_number,
                    message,
                });
            }
            Effect::UnlockService(service_id) => {
                state_storage
                    .store_service_status(&service_id, VirtualObjectStatus::Unlocked)
                    .await?;
            }
            Effect::SetState {
                service_id,
                key,
                value,
                ..
            } => {
                state_storage.store_state(&service_id, key, value).await?;
            }
            Effect::ClearState {
                service_id, key, ..
            } => {
                state_storage.clear_state(&service_id, &key).await?;
            }
            Effect::ClearAllState { service_id, .. } => {
                state_storage.clear_all_state(&service_id).await?;
            }
            Effect::RegisterTimer { timer_value, .. } => {
                state_storage
                    .store_timer(timer_value.key().clone(), timer_value.value().clone())
                    .await?;

                collector.push(Action::RegisterTimer { timer_value });
            }
            Effect::DeleteTimer(timer_key) => {
                state_storage.delete_timer(&timer_key).await?;
                collector.push(Action::DeleteTimer { timer_key });
            }
            Effect::StorePinnedDeployment {
                invocation_id,
                pinned_deployment,
                mut metadata,
            } => {
                metadata.set_pinned_deployment(pinned_deployment);

                // We recreate the InvocationStatus in Invoked state as the invoker can notify the
                // chosen deployment_id only when the invocation is in-flight.
                state_storage
                    .store_invocation_status(&invocation_id, InvocationStatus::Invoked(metadata))
                    .await?;
            }
            Effect::AppendJournalEntry {
                invocation_id,
                previous_invocation_status,
                entry_index,
                journal_entry,
            } => {
                Self::append_journal_entry(
                    state_storage,
                    invocation_id,
                    previous_invocation_status,
                    entry_index,
                    journal_entry,
                )
                .await?;
            }
            Effect::DropJournal {
                invocation_id,
                journal_length,
            } => {
                // TODO: Only drop journals if the inbox is empty; this requires that keep track of the max journal length: https://github.com/restatedev/restate/issues/272
                state_storage
                    .drop_journal(&invocation_id, journal_length)
                    .await?;
            }
            Effect::TruncateOutbox(range) => {
                state_storage.truncate_outbox(range).await?;
            }
            Effect::StoreCompletion {
                invocation_id,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
            } => {
                Self::store_completion(state_storage, &invocation_id, entry_index, result).await?;
            }
            Effect::ForwardCompletion {
                invocation_id,
                completion,
            } => {
                collector.push(Action::ForwardCompletion {
                    invocation_id,
                    completion,
                });
            }
            Effect::AppendResponseSink {
                invocation_id,
                additional_response_sink,
                mut previous_invocation_status,
            } => {
                previous_invocation_status
                    .get_response_sinks_mut()
                    .expect("No response sinks available")
                    .insert(additional_response_sink);
                previous_invocation_status.update_timestamps();

                state_storage
                    .store_invocation_status(&invocation_id, previous_invocation_status)
                    .await?;
            }
            Effect::StoreIdempotencyId(idempotency_id, invocation_id) => {
                state_storage
                    .put_idempotency_metadata(
                        &idempotency_id,
                        IdempotencyMetadata { invocation_id },
                    )
                    .await;
            }
            Effect::DeleteIdempotencyId(idempotency_id) => {
                state_storage
                    .delete_idempotency_metadata(&idempotency_id)
                    .await;
            }
            Effect::TraceInvocationResult { .. } | Effect::TraceBackgroundInvoke { .. } => {
                // these effects are only needed for span creation
            }
            Effect::SendAbortInvocationToInvoker(invocation_id) => {
                collector.push(Action::AbortInvocation(invocation_id))
            }
            Effect::SendStoredEntryAckToInvoker(invocation_id, entry_index) => {
                collector.push(Action::AckStoredEntry {
                    invocation_id,
                    entry_index,
                });
            }
            Effect::MutateState(state_mutation) => {
                Self::mutate_state(state_storage, state_mutation).await?;
            }
            Effect::IngressResponse(ingress_response) => {
                collector.push(Action::IngressResponse(ingress_response));
            }
            Effect::IngressSubmitNotification(attach_notification) => {
                collector.push(Action::IngressSubmitNotification(attach_notification))
            }
            Effect::PutPromise {
                service_id,
                key,
                metadata,
            } => {
                state_storage.put_promise(&service_id, &key, metadata).await;
            }
            Effect::ClearAllPromises { service_id } => {
                state_storage.delete_all_promises(&service_id).await
            }
        }

        Ok(())
    }

    async fn pop_from_inbox<S>(
        state_storage: &mut S,
        collector: &mut ActionCollector,
        service_id: ServiceId,
    ) -> Result<(), Error>
    where
        S: StateStorage
            + restate_storage_api::invocation_status_table::ReadOnlyInvocationStatusTable,
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
