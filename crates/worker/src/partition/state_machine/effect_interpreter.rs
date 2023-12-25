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
use std::future::Future;

use crate::partition::services::non_deterministic;
use crate::partition::state_machine::actions::Action;
use crate::partition::state_machine::effects::Effect;
use crate::partition::{CommitError, Committable};
use bytes::Bytes;
use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::outbox_table::OutboxMessage;
use restate_storage_api::status_table::{
    InvocationMetadata, InvocationStatus, JournalMetadata, StatusTimestamps,
};
use restate_storage_api::timer_table::Timer;
use restate_storage_api::Result as StorageResult;
use restate_types::identifiers::{EntryIndex, FullInvocationId, ServiceId};
use restate_types::invocation::ServiceInvocation;
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use restate_types::journal::raw::{PlainRawEntry, RawEntryCodec};
use restate_types::journal::{Completion, CompletionResult, EntryType};
use restate_types::message::MessageIndex;
use restate_types::time::MillisSinceEpoch;
use std::marker::PhantomData;
use tracing::{debug, warn};

pub trait ActionCollector {
    fn collect(&mut self, message: Action);
}

pub trait StateStorage {
    // Invocation status
    fn store_invocation_status(
        &mut self,
        service_id: &ServiceId,
        status: InvocationStatus,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn drop_journal(
        &mut self,
        service_id: &ServiceId,
        journal_length: EntryIndex,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn store_journal_entry(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn store_completion_result(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn load_completion_result(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> impl Future<Output = StorageResult<Option<CompletionResult>>> + Send;

    fn load_journal_entry(
        &mut self,
        service_id: &ServiceId,
        entry_index: EntryIndex,
    ) -> impl Future<Output = StorageResult<Option<EnrichedRawEntry>>> + Send;

    // In-/outbox
    fn enqueue_into_inbox(
        &mut self,
        seq_number: MessageIndex,
        service_invocation: ServiceInvocation,
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

    fn truncate_inbox(
        &mut self,
        service_id: &ServiceId,
        inbox_sequence_number: MessageIndex,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn delete_inbox_entry(
        &mut self,
        service_id: &ServiceId,
        sequence_number: MessageIndex,
    ) -> impl Future<Output = ()> + Send;

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

    // Timer
    fn store_timer(
        &mut self,
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
        timer: Timer,
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn delete_timer(
        &mut self,
        full_invocation_id: FullInvocationId,
        wake_up_time: MillisSinceEpoch,
        entry_index: EntryIndex,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

#[must_use = "Don't forget to commit the interpretation result"]
pub struct InterpretationResult<Txn, Collector> {
    txn: Txn,
    collector: Collector,
}

impl<Txn, Collector> InterpretationResult<Txn, Collector>
where
    Txn: Committable,
{
    pub(crate) fn new(txn: Txn, collector: Collector) -> Self {
        Self { txn, collector }
    }

    pub async fn commit(self) -> Result<Collector, CommitError> {
        let Self { txn, collector } = self;

        txn.commit().await?;
        Ok(collector)
    }
}

pub(crate) struct EffectInterpreter<Codec> {
    _codec: PhantomData<Codec>,
}

impl<Codec: RawEntryCodec> EffectInterpreter<Codec> {
    pub(crate) async fn interpret_effects<S: StateStorage + Committable, C: ActionCollector>(
        effects: &mut Effects,
        mut state_storage: S,
        mut message_collector: C,
    ) -> Result<InterpretationResult<S, C>, Error> {
        for effect in effects.drain() {
            Self::interpret_effect(effect, &mut state_storage, &mut message_collector).await?;
        }

        Ok(InterpretationResult::new(state_storage, message_collector))
    }

    async fn interpret_effect<S: StateStorage, C: ActionCollector>(
        effect: Effect,
        state_storage: &mut S,
        collector: &mut C,
    ) -> Result<(), Error> {
        match effect {
            Effect::InvokeService(service_invocation) => {
                Self::invoke_service(state_storage, collector, service_invocation).await?;
            }
            Effect::ResumeService {
                service_id,
                mut metadata,
            } => {
                metadata.timestamps.update();
                let invocation_id = metadata.invocation_uuid;
                state_storage
                    .store_invocation_status(&service_id, InvocationStatus::Invoked(metadata))
                    .await?;

                collector.collect(Action::Invoke {
                    full_invocation_id: FullInvocationId {
                        service_id,
                        invocation_uuid: invocation_id,
                    },
                    invoke_input_journal: InvokeInputJournal::NoCachedJournal,
                });
            }
            Effect::SuspendService {
                service_id,
                mut metadata,
                waiting_for_completed_entries,
            } => {
                metadata.timestamps.update();
                state_storage
                    .store_invocation_status(
                        &service_id,
                        InvocationStatus::Suspended {
                            metadata,
                            waiting_for_completed_entries,
                        },
                    )
                    .await?;
            }
            Effect::DropJournalAndFreeService {
                service_id,
                journal_length,
            } => {
                state_storage
                    .drop_journal(&service_id, journal_length)
                    .await?;
                state_storage
                    .store_invocation_status(&service_id, InvocationStatus::Free)
                    .await?;
            }
            Effect::EnqueueIntoInbox {
                seq_number,
                service_invocation,
            } => {
                state_storage
                    .enqueue_into_inbox(seq_number, service_invocation)
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

                collector.collect(Action::NewOutboxMessage {
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
            Effect::RegisterTimer { timer_value, .. } => {
                state_storage
                    .store_timer(
                        timer_value.full_invocation_id.clone(),
                        timer_value.wake_up_time,
                        timer_value.entry_index,
                        timer_value.value.clone(),
                    )
                    .await?;

                collector.collect(Action::RegisterTimer { timer_value });
            }
            Effect::DeleteTimer {
                full_invocation_id,
                wake_up_time,
                entry_index,
            } => {
                state_storage
                    .delete_timer(full_invocation_id, wake_up_time, entry_index)
                    .await?;
            }
            Effect::StoreDeploymentId {
                service_id,
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
                    .store_invocation_status(&service_id, InvocationStatus::Invoked(metadata))
                    .await?;
            }
            Effect::AppendJournalEntry {
                service_id,
                previous_invocation_status,
                entry_index,
                journal_entry,
            } => {
                Self::append_journal_entry(
                    state_storage,
                    service_id,
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
                full_invocation_id,
                completion:
                    Completion {
                        entry_index,
                        result,
                    },
            } => {
                Self::store_completion(state_storage, &full_invocation_id, entry_index, result)
                    .await?;
            }
            Effect::ForwardCompletion {
                full_invocation_id,
                completion,
            } => {
                collector.collect(Action::ForwardCompletion {
                    full_invocation_id,
                    completion,
                });
            }
            Effect::CreateVirtualJournal {
                service_id,
                invocation_uuid,
                span_context,
                completion_notification_target,
                kill_notification_target,
            } => {
                state_storage
                    .store_invocation_status(
                        &service_id,
                        InvocationStatus::Virtual {
                            invocation_uuid,
                            journal_metadata: JournalMetadata::initialize(span_context),
                            completion_notification_target,
                            timestamps: StatusTimestamps::now(),
                            kill_notification_target,
                        },
                    )
                    .await?;
            }
            Effect::NotifyVirtualJournalCompletion {
                target_service,
                method_name,
                invocation_uuid,
                completion,
            } => collector.collect(Action::NotifyVirtualJournalCompletion {
                target_service,
                method_name,
                invocation_uuid,
                completion,
            }),
            Effect::NotifyVirtualJournalKill {
                target_service,
                method_name,
                invocation_uuid,
            } => collector.collect(Action::NotifyVirtualJournalKill {
                target_service,
                method_name,
                invocation_uuid,
            }),
            Effect::DropJournalAndPopInbox {
                service_id,
                inbox_sequence_number,
                journal_length,
                service_invocation,
            } => {
                // TODO: Only drop journals if the inbox is empty; this requires that keep track of the max journal length: https://github.com/restatedev/restate/issues/272
                state_storage
                    .drop_journal(&service_id, journal_length)
                    .await?;
                state_storage
                    .truncate_inbox(&service_id, inbox_sequence_number)
                    .await?;
                Self::invoke_service(state_storage, collector, service_invocation).await?;
            }
            Effect::TraceInvocationResult { .. } | Effect::TraceBackgroundInvoke { .. } => {
                // these effects are only needed for span creation
            }
            Effect::AbortInvocation(full_invocation_id) => {
                collector.collect(Action::AbortInvocation(full_invocation_id))
            }
            Effect::SendStoredEntryAckToInvoker(full_invocation_id, entry_index) => {
                collector.collect(Action::AckStoredEntry {
                    full_invocation_id,
                    entry_index,
                });
            }
        }

        Ok(())
    }

    async fn invoke_service<S: StateStorage, C: ActionCollector>(
        state_storage: &mut S,
        collector: &mut C,
        service_invocation: ServiceInvocation,
    ) -> Result<(), Error> {
        let journal_metadata = JournalMetadata::new(
            1, // initial length is 1, because we store the poll input stream entry
            service_invocation.span_context.clone(),
        );

        state_storage
            .store_invocation_status(
                &service_invocation.fid.service_id,
                InvocationStatus::Invoked(InvocationMetadata::new(
                    service_invocation.fid.invocation_uuid,
                    journal_metadata.clone(),
                    None,
                    service_invocation.method_name.clone(),
                    service_invocation.response_sink.clone(),
                    StatusTimestamps::now(),
                    service_invocation.source,
                )),
            )
            .await?;

        let service_id = service_invocation.fid.service_id.clone();

        let input_entry = if non_deterministic::ServiceInvoker::is_supported(
            &service_invocation.fid.service_id.service_name,
        ) {
            collector.collect(Action::InvokeBuiltInService {
                full_invocation_id: service_invocation.fid,
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
            let poll_input_stream_entry =
                Codec::serialize_as_unary_input_entry(service_invocation.argument.clone());
            let (header, serialized_entry) = poll_input_stream_entry.into_inner();

            collector.collect(Action::Invoke {
                full_invocation_id: service_invocation.fid,
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
            .store_journal_entry(&service_id, 0, input_entry)
            .await?;

        Ok(())
    }

    /// Stores the given completion. Returns `true` if an [`RawEntry`] was completed.
    async fn store_completion<S: StateStorage>(
        state_storage: &mut S,
        full_invocation_id: &FullInvocationId,
        entry_index: EntryIndex,
        completion_result: CompletionResult,
    ) -> Result<bool, Error> {
        if let Some(mut journal_entry) = state_storage
            .load_journal_entry(&full_invocation_id.service_id, entry_index)
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
                    restate.invocation.id = %full_invocation_id,
                    restate.journal.index = entry_index,
                    "Trying to complete an awakeable already completed. Ignoring this completion");
                debug!("Discarded awakeable completion: {:?}", completion_result);
                return Ok(false);
            }
            Codec::write_completion(&mut journal_entry, completion_result)?;
            state_storage
                .store_journal_entry(&full_invocation_id.service_id, entry_index, journal_entry)
                .await?;
            Ok(true)
        } else {
            // In case we don't have the journal entry (only awakeables case),
            // we'll send the completion afterward once we receive the entry.
            state_storage
                .store_completion_result(
                    &full_invocation_id.service_id,
                    entry_index,
                    completion_result,
                )
                .await?;
            Ok(false)
        }
    }

    async fn append_journal_entry<S: StateStorage>(
        state_storage: &mut S,
        service_id: ServiceId,
        mut previous_invocation_status: InvocationStatus,
        entry_index: EntryIndex,
        journal_entry: EnrichedRawEntry,
    ) -> Result<(), Error> {
        // Store journal entry
        state_storage
            .store_journal_entry(&service_id, entry_index, journal_entry)
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
            .store_invocation_status(&service_id, previous_invocation_status)
            .await?;

        Ok(())
    }
}
