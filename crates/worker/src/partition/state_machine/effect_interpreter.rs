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

use crate::partition::services::non_deterministic;
use crate::partition::state_machine::actions::Action;
use crate::partition::state_machine::effects::Effect;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::inbox_table::{InboxEntry, SequenceNumberInboxEntry};
use restate_storage_api::invocation_status_table::{
    InvocationMetadata, InvocationStatus, JournalMetadata, StatusTimestamps,
};
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::service_status_table::VirtualObjectStatus;
use restate_storage_api::timer_table::{Timer, TimerKey};
use restate_storage_api::Result as StorageResult;
use restate_types::identifiers::{EntryIndex, FullInvocationId, InvocationId, ServiceId};
use restate_types::invocation::ServiceInvocation;
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use restate_types::journal::raw::{PlainRawEntry, RawEntryCodec};
use restate_types::journal::{Completion, CompletionResult, EntryType};
use restate_types::message::MessageIndex;
use restate_types::state_mut::{ExternalStateMutation, StateMutationVersion};
use std::future::Future;
use std::marker::PhantomData;
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
        outbox_sequence_number: MessageIndex,
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
    pub(crate) async fn interpret_effects<S: StateStorage>(
        effects: &mut Effects,
        state_storage: &mut S,
        action_collector: &mut ActionCollector,
    ) -> Result<(), Error> {
        for effect in effects.drain() {
            Self::interpret_effect(effect, state_storage, action_collector).await?;
        }

        Ok(())
    }

    async fn interpret_effect<S: StateStorage>(
        effect: Effect,
        state_storage: &mut S,
        collector: &mut ActionCollector,
    ) -> Result<(), Error> {
        match effect {
            Effect::InvokeService(service_invocation) => {
                Self::invoke_service(state_storage, collector, service_invocation).await?;
            }
            Effect::ResumeService {
                invocation_id,
                mut metadata,
            } => {
                metadata.timestamps.update();
                let service_id = metadata.service_id.clone();
                state_storage
                    .store_invocation_status(&invocation_id, InvocationStatus::Invoked(metadata))
                    .await?;

                collector.push(Action::Invoke {
                    full_invocation_id: FullInvocationId::combine(service_id, invocation_id),
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
            Effect::StoreDeploymentId {
                invocation_id,
                deployment_id,
                mut metadata,
            } => {
                debug_assert_eq!(
                    metadata.deployment_id, None,
                    "No deployment_id should be fixed for the current invocation"
                );
                metadata.deployment_id = Some(deployment_id);
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
            Effect::TruncateOutbox(outbox_sequence_number) => {
                state_storage
                    .truncate_outbox(outbox_sequence_number)
                    .await?;
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
                full_invocation_id,
                completion,
            } => {
                collector.push(Action::ForwardCompletion {
                    full_invocation_id,
                    completion,
                });
            }
            Effect::DropJournalAndPopInbox {
                full_invocation_id,
                journal_length,
            } => {
                // TODO: Only drop journals if the inbox is empty; this requires that keep track of the max journal length: https://github.com/restatedev/restate/issues/272
                state_storage
                    .drop_journal(&InvocationId::from(&full_invocation_id), journal_length)
                    .await?;
                state_storage
                    .store_invocation_status(
                        &InvocationId::from(&full_invocation_id),
                        InvocationStatus::Free,
                    )
                    .await?;

                Self::pop_from_inbox(state_storage, collector, &full_invocation_id.service_id)
                    .await?;
            }
            Effect::TraceInvocationResult { .. } | Effect::TraceBackgroundInvoke { .. } => {
                // these effects are only needed for span creation
            }
            Effect::AbortInvocation(full_invocation_id) => {
                collector.push(Action::AbortInvocation(full_invocation_id))
            }
            Effect::SendStoredEntryAckToInvoker(full_invocation_id, entry_index) => {
                collector.push(Action::AckStoredEntry {
                    full_invocation_id,
                    entry_index,
                });
            }
            Effect::MutateState(state_mutation) => {
                Self::mutate_state(state_storage, state_mutation).await?;
            }
            Effect::IngressResponse(ingress_response) => {
                collector.push(Action::IngressResponse(ingress_response));
            }
        }

        Ok(())
    }

    async fn pop_from_inbox<S>(
        state_storage: &mut S,
        collector: &mut ActionCollector,
        service_id: &ServiceId,
    ) -> Result<(), Error>
    where
        S: StateStorage,
    {
        // pop until we find the first invocation that we can invoke
        while let Some(inbox_entry) = state_storage.pop_inbox(service_id).await? {
            match inbox_entry.inbox_entry {
                InboxEntry::Invocation(service_invocation) => {
                    Self::invoke_service(state_storage, collector, service_invocation).await?;
                    return Ok(());
                }
                InboxEntry::StateMutation(state_mutation) => {
                    Self::mutate_state(state_storage, state_mutation).await?;
                }
            }
        }

        state_storage
            .store_service_status(service_id, VirtualObjectStatus::Unlocked)
            .await?;

        Ok(())
    }

    async fn mutate_state<S: StateStorage>(
        state_storage: &mut S,
        state_mutation: ExternalStateMutation,
    ) -> StorageResult<()> {
        let ExternalStateMutation {
            component_id: service_id,
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
        service_invocation: ServiceInvocation,
    ) -> Result<(), Error> {
        let journal_metadata = JournalMetadata::new(
            1, // initial length is 1, because we store the poll input stream entry
            service_invocation.span_context.clone(),
        );

        let invocation_id = InvocationId::from(&service_invocation.fid);
        state_storage
            .store_service_status(
                &service_invocation.fid.service_id,
                VirtualObjectStatus::Locked(invocation_id.clone()),
            )
            .await?;
        state_storage
            .store_invocation_status(
                &invocation_id,
                InvocationStatus::Invoked(InvocationMetadata::new(
                    service_invocation.fid.service_id.clone(),
                    journal_metadata.clone(),
                    None,
                    service_invocation.method_name.clone(),
                    service_invocation.response_sink.clone(),
                    StatusTimestamps::now(),
                    service_invocation.source,
                )),
            )
            .await?;

        let input_entry = if non_deterministic::ServiceInvoker::is_supported(
            &service_invocation.fid.service_id.service_name,
        ) {
            collector.push(Action::InvokeBuiltInService {
                full_invocation_id: service_invocation.fid.clone(),
                span_context: service_invocation.span_context,
                response_sink: service_invocation.response_sink,
                method: service_invocation.method_name,
                argument: service_invocation.argument.clone(),
            });

            // TODO clean up custom entry hack by allowing to store bytes directly?
            EnrichedRawEntry::new(
                EnrichedEntryHeader::Custom { code: 0 },
                service_invocation.argument,
            )
        } else {
            let poll_input_stream_entry = Codec::serialize_as_input_entry(
                service_invocation.headers,
                service_invocation.argument.clone(),
            );
            let (header, serialized_entry) = poll_input_stream_entry.into_inner();

            collector.push(Action::Invoke {
                full_invocation_id: service_invocation.fid.clone(),
                invoke_input_journal: InvokeInputJournal::CachedJournal(
                    restate_invoker_api::JournalMetadata::new(
                        journal_metadata.length,
                        journal_metadata.span_context,
                        service_invocation.method_name.clone(),
                        None,
                    ),
                    vec![PlainRawEntry::new(
                        header.clone().erase_enrichment(),
                        serialized_entry.clone(),
                    )],
                ),
            });

            EnrichedRawEntry::new(header, serialized_entry)
        };

        state_storage
            .store_journal_entry(&InvocationId::from(&service_invocation.fid), 0, input_entry)
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
