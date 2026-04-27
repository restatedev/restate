// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::Infallible;

use bytes::Bytes;

use restate_errors::NotRunningError;
use restate_memory::{IgnorePinnableMemoryStream, LocalMemoryLease, LocalMemoryPool};
use restate_types::errors::InvocationError;
use restate_types::identifiers::{EntryIndex, InvocationId, InvocationUuid, ServiceId};
use restate_types::invocation::{InvocationTarget, ServiceInvocationSpanContext};
use restate_types::journal::enriched::{
    AwakeableEnrichmentResult, CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
};
use restate_types::journal::raw::{PlainEntryHeader, PlainRawEntry, RawEntry};
use restate_types::journal_v2::CommandIndex;
use restate_types::time::MillisSinceEpoch;
use restate_types::vqueues::VQueueId;
use restate_worker_api::invoker::invocation_reader::{
    EagerState, InvocationReader, InvocationReaderTransaction, JournalEntry, JournalKind,
};
use restate_worker_api::invoker::{EntryEnricher, InvokerHandle, JournalMetadata};
use restate_worker_api::resources::ReservedResources;

#[derive(Debug, Clone, Default)]
pub struct EmptyStorageReader;

impl InvocationReader for EmptyStorageReader {
    type Transaction<'a> = EmptyStorageReaderTransaction;
    type Error = Infallible;

    fn transaction(&mut self) -> Self::Transaction<'_> {
        EmptyStorageReaderTransaction
    }

    async fn read_journal_entry(
        &mut self,
        _invocation_id: &InvocationId,
        _entry_index: EntryIndex,
        _journal_kind: JournalKind,
    ) -> Result<Option<JournalEntry>, Infallible> {
        Ok(None)
    }

    async fn read_journal_entry_budgeted(
        &mut self,
        _invocation_id: &InvocationId,
        _entry_index: EntryIndex,
        _journal_kind: JournalKind,
        _budget: &mut LocalMemoryPool,
    ) -> Result<Option<(JournalEntry, LocalMemoryLease)>, Infallible> {
        Ok(None)
    }
}

pub struct EmptyStorageReaderTransaction;

impl InvocationReaderTransaction for EmptyStorageReaderTransaction {
    type JournalStream<'a> = futures::stream::Empty<Result<JournalEntry, Self::Error>>;
    type StateStream<'a> = futures::stream::Empty<Result<(Bytes, Bytes), Self::Error>>;
    type LocalMemoryPooledJournalStream<'a> =
        futures::stream::Empty<Result<(JournalEntry, LocalMemoryLease), Self::Error>>;
    type LocalMemoryPooledStateStream<'a> = IgnorePinnableMemoryStream<
        futures::stream::Empty<Result<(Bytes, Bytes, LocalMemoryLease), Self::Error>>,
    >;
    type Error = Infallible;

    async fn read_journal_metadata(
        &mut self,
        _invocation_id: &InvocationId,
    ) -> Result<Option<JournalMetadata>, Self::Error> {
        Ok(Some(JournalMetadata::new(
            0,
            ServiceInvocationSpanContext::empty(),
            None,
            MillisSinceEpoch::UNIX_EPOCH,
            0,
            JournalKind::V2,
        )))
    }

    fn read_journal(
        &self,
        _invocation_id: &InvocationId,
        _length: EntryIndex,
        _journal_kind: JournalKind,
    ) -> Result<Self::JournalStream<'_>, Self::Error> {
        Ok(futures::stream::empty())
    }

    fn read_state(
        &self,
        _service_id: &ServiceId,
    ) -> Result<EagerState<Self::StateStream<'_>>, Self::Error> {
        Ok(EagerState::new_complete(futures::stream::empty()))
    }

    fn read_journal_budgeted<'a>(
        &'a self,
        _invocation_id: &InvocationId,
        _length: EntryIndex,
        _journal_kind: JournalKind,
        _budget: &'a mut LocalMemoryPool,
    ) -> Result<Self::LocalMemoryPooledJournalStream<'a>, Self::Error> {
        Ok(futures::stream::empty())
    }

    fn read_state_budgeted<'a>(
        &'a self,
        _service_id: &ServiceId,
        _budget: &'a mut LocalMemoryPool,
    ) -> Result<EagerState<Self::LocalMemoryPooledStateStream<'a>>, Self::Error> {
        Ok(EagerState::new_complete(IgnorePinnableMemoryStream::new(
            futures::stream::empty(),
        )))
    }
}

#[derive(Debug, Default)]
pub struct MockInvokerHandle;

impl InvokerHandle for MockInvokerHandle {
    fn invoke(
        &mut self,
        _invocation_id: InvocationId,
        _invocation_target: InvocationTarget,
    ) -> Result<(), NotRunningError> {
        Ok(())
    }

    fn vqueue_invoke(
        &mut self,
        _qid: VQueueId,
        _permit: ReservedResources,
        _invocation_id: InvocationId,
        _invocation_target: InvocationTarget,
    ) -> Result<(), NotRunningError> {
        Ok(())
    }

    fn notify_completion(
        &mut self,
        _invocation_id: InvocationId,
        _entry_index: EntryIndex,
    ) -> Result<(), NotRunningError> {
        Ok(())
    }

    fn notify_notification(
        &mut self,
        _invocation_id: InvocationId,
        _entry_index: EntryIndex,
        _notification_id: restate_types::journal_v2::NotificationId,
    ) -> Result<(), NotRunningError> {
        Ok(())
    }

    fn notify_stored_command_ack(
        &mut self,
        _invocation_id: InvocationId,
        _command_index: CommandIndex,
    ) -> Result<(), NotRunningError> {
        Ok(())
    }

    fn abort_all(&mut self) -> Result<(), NotRunningError> {
        Ok(())
    }

    fn abort_invocation(&mut self, _invocation_id: InvocationId) -> Result<(), NotRunningError> {
        Ok(())
    }

    fn retry_invocation_now(
        &mut self,
        _invocation_id: InvocationId,
    ) -> Result<(), NotRunningError> {
        Ok(())
    }

    fn pause_invocation(&mut self, _invocation_id: InvocationId) -> Result<(), NotRunningError> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct MockEntryEnricher;

impl EntryEnricher for MockEntryEnricher {
    fn enrich_entry(
        &mut self,
        entry: PlainRawEntry,
        _current_invocation_target: &InvocationTarget,
        current_invocation_span_context: &ServiceInvocationSpanContext,
    ) -> Result<EnrichedRawEntry, InvocationError> {
        let (header, entry) = entry.into_inner();
        let enriched_header = match header {
            PlainEntryHeader::Input {} => EnrichedEntryHeader::Input {},
            PlainEntryHeader::Output {} => EnrichedEntryHeader::Output {},
            PlainEntryHeader::GetState { is_completed } => {
                EnrichedEntryHeader::GetState { is_completed }
            }
            PlainEntryHeader::SetState {} => EnrichedEntryHeader::SetState {},
            PlainEntryHeader::ClearState {} => EnrichedEntryHeader::ClearState {},
            PlainEntryHeader::GetStateKeys { is_completed } => {
                EnrichedEntryHeader::GetStateKeys { is_completed }
            }
            PlainEntryHeader::ClearAllState {} => EnrichedEntryHeader::ClearAllState {},
            PlainEntryHeader::GetPromise { is_completed } => {
                EnrichedEntryHeader::GetPromise { is_completed }
            }
            PlainEntryHeader::PeekPromise { is_completed } => {
                EnrichedEntryHeader::PeekPromise { is_completed }
            }
            PlainEntryHeader::CompletePromise { is_completed } => {
                EnrichedEntryHeader::CompletePromise { is_completed }
            }
            PlainEntryHeader::Sleep { is_completed } => EnrichedEntryHeader::Sleep { is_completed },
            PlainEntryHeader::Call { is_completed, .. } => {
                if !is_completed {
                    EnrichedEntryHeader::Call {
                        is_completed,
                        enrichment_result: Some(CallEnrichmentResult {
                            invocation_id: InvocationId::mock_random(),
                            invocation_target: InvocationTarget::service("", ""),
                            completion_retention_time: None,
                            span_context: current_invocation_span_context.clone(),
                        }),
                    }
                } else {
                    // No need to service resolution if the entry was completed by the service
                    EnrichedEntryHeader::Call {
                        is_completed,
                        enrichment_result: None,
                    }
                }
            }
            PlainEntryHeader::OneWayCall { .. } => EnrichedEntryHeader::OneWayCall {
                enrichment_result: CallEnrichmentResult {
                    invocation_id: InvocationId::mock_random(),
                    invocation_target: InvocationTarget::service("", ""),
                    completion_retention_time: None,
                    span_context: current_invocation_span_context.clone(),
                },
            },
            PlainEntryHeader::Awakeable { is_completed } => {
                EnrichedEntryHeader::Awakeable { is_completed }
            }
            PlainEntryHeader::CompleteAwakeable { .. } => EnrichedEntryHeader::CompleteAwakeable {
                enrichment_result: AwakeableEnrichmentResult {
                    invocation_id: InvocationId::from_parts(
                        0,
                        InvocationUuid::mock_generate(&InvocationTarget::mock_service()),
                    ),
                    entry_index: 1,
                },
            },
            PlainEntryHeader::Run {} => EnrichedEntryHeader::Run {},
            PlainEntryHeader::Custom { code } => EnrichedEntryHeader::Custom { code },
            PlainEntryHeader::CancelInvocation => EnrichedEntryHeader::CancelInvocation,
            PlainEntryHeader::GetCallInvocationId { is_completed } => {
                EnrichedEntryHeader::GetCallInvocationId { is_completed }
            }
            PlainEntryHeader::AttachInvocation { is_completed } => {
                EnrichedEntryHeader::AttachInvocation { is_completed }
            }
            PlainEntryHeader::GetInvocationOutput { is_completed } => {
                EnrichedEntryHeader::GetInvocationOutput { is_completed }
            }
        };

        Ok(RawEntry::new(enriched_header, entry))
    }
}
