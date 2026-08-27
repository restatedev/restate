// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert2::let_assert;
use restate_storage_api::output_table::WriteOutputTable;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use tracing::warn;

use restate_clock::UniqueTimestamp;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::inbox_table::WriteInboxTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, CompletionStatus, InFlightInvocationMetadata, JournalMetadata,
    JournalRetentionPolicy, ReadInvocationStatusTable, ResponseResultRef,
    WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::journal_table::{JournalEntry, ReadJournalTable, WriteJournalTable};
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::{ReadPromiseTable, WritePromiseTable};
use restate_storage_api::service_status_table::WriteVirtualObjectStatusTable;
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_storage_api::vqueue_table::{self, ReadVQueueTable, WriteVQueueTable};
use restate_storage_api::{StorageError, journal_table_v2};
use restate_types::errors::{InvocationError, KILLED_INVOCATION_ERROR};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::ResponseResult;
use restate_types::journal::EntryType;
use restate_types::journal_v2::{self, CommandType, EntryMetadata, OutputCommand, OutputResult};
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::sharding::WithPartitionKey;
use restate_types::vqueues::EntryId;
use restate_vqueues::VQueue;
use restate_worker_api::processor::PartitionFeatures;

use crate::partition::processor::{FsmAccess, ProcessorContext};
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

/// Terminal step of the invocation lifecycle: publishes the result, applies the retention
/// policies and releases whatever the invocation was holding (vqueue entry or inbox lock).
pub struct EndInvocationCommand {
    invocation_id: InvocationId,
    invocation_metadata: InFlightInvocationMetadata,
    reason: EndInvocationReason,
}

/// How the invocation ended.
pub enum EndInvocationReason {
    /// The invoker reported the invocation ran to completion. The result is read from the
    /// last Output entry in the journal.
    Completed,
    /// The invoker reported a terminal failure, The entry is appended at the end of the journal.
    Failed(InvocationError),
    /// The invocation was killed.
    Killed,
}

enum Cached<T> {
    None,
    NotFound,
    Found(T),
}

struct ResponseResultCache {
    invocation_id: InvocationId,
    journal_length: u32,
    protocol_version: ServiceProtocolVersion,

    result: Cached<ResponseResult>,
}

impl ResponseResultCache {
    fn new(
        invocation_id: InvocationId,
        journal_length: u32,
        protocol_version: ServiceProtocolVersion,
    ) -> Self {
        Self {
            invocation_id,
            journal_length,
            protocol_version,
            result: Cached::None,
        }
    }

    async fn read_last_output_entry_result<'s, S, P>(
        &self,
        ctx: &mut StateMachineApplyContext<'s, S, P>,
    ) -> Result<Option<ResponseResult>, Error>
    where
        P: ProcessorContext,
        S: ReadJournalTable + journal_table_v2::ReadJournalTable,
    {
        if self.protocol_version >= ServiceProtocolVersion::V4 {
            // Find last output entry
            for i in (0..self.journal_length).rev() {
                let entry = journal_table_v2::ReadJournalTable::get_journal_entry(
                    ctx.storage,
                    self.invocation_id,
                    i,
                )
                .await?
                .unwrap_or_else(|| panic!("There should be a journal entry at index {i}"));
                if entry.ty() == journal_v2::EntryType::Command(CommandType::Output) {
                    let cmd = entry.decode::<ServiceProtocolV4Codec, OutputCommand>()?;
                    return Ok(Some(match cmd.result {
                        OutputResult::Success(s) => ResponseResult::Success(s),
                        OutputResult::Failure(f) => ResponseResult::Failure(f.into()),
                    }));
                }
            }
            Ok(None)
        } else {
            // Find last output entry
            let mut output_entry = None;
            for i in (0..self.journal_length).rev() {
                if let JournalEntry::Entry(e) =
                    ReadJournalTable::get_journal_entry(ctx.storage, &self.invocation_id, i)
                        .await?
                        .unwrap_or_else(|| panic!("There should be a journal entry at index {i}"))
                    && e.ty() == EntryType::Output
                {
                    output_entry = Some(e);
                    break;
                }
            }

            output_entry
                .map(|enriched_entry| {
                    let_assert!(
                        restate_types::journal::Entry::Output(e) =
                            enriched_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );
                    Ok(e.result.into())
                })
                .transpose()
        }
    }

    async fn fetch<'s, S, P>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'s, S, P>,
    ) -> Result<(), Error>
    where
        P: ProcessorContext,
        S: ReadJournalTable + journal_table_v2::ReadJournalTable,
    {
        match &self.result {
            Cached::NotFound | Cached::Found(_) => {}
            Cached::None => match self.read_last_output_entry_result(ctx).await? {
                Some(response) => {
                    self.result = Cached::Found(response);
                }
                None => self.result = Cached::NotFound,
            },
        }
        Ok(())
    }

    async fn response_result<'s, S, P>(
        &mut self,
        ctx: &mut StateMachineApplyContext<'s, S, P>,
    ) -> Result<Option<&ResponseResult>, Error>
    where
        P: ProcessorContext,
        S: ReadJournalTable + journal_table_v2::ReadJournalTable,
    {
        self.fetch(ctx).await?;

        match &self.result {
            Cached::None => {
                unreachable!()
            }
            Cached::NotFound => Ok(None),
            Cached::Found(response) => Ok(Some(response)),
        }
    }

    async fn into_response_result<'s, S, P>(
        mut self,
        ctx: &mut StateMachineApplyContext<'s, S, P>,
    ) -> Result<Option<ResponseResult>, Error>
    where
        P: ProcessorContext,
        S: ReadJournalTable + journal_table_v2::ReadJournalTable,
    {
        self.fetch(ctx).await?;

        match self.result {
            Cached::None => {
                unreachable!()
            }
            Cached::NotFound => Ok(None),
            Cached::Found(response) => Ok(Some(response)),
        }
    }
}

fn append_journal_entry<'s, S, P>(
    ctx: &mut StateMachineApplyContext<'s, S, P>,
    invocation_id: &InvocationId,
    journal_meta: &mut JournalMetadata,
    entry: impl Into<restate_types::journal_v2::Entry>,
) -> Result<(), StorageError>
where
    P: ProcessorContext,
    S: journal_table_v2::WriteJournalTable,
{
    let entry = entry.into().encode::<ServiceProtocolV4Codec>();
    let entry_index = journal_meta.length;

    // Update journal length
    journal_meta.length += 1;
    if matches!(entry.ty(), restate_types::journal_v2::EntryType::Command(_)) {
        journal_meta.commands += 1;
    }

    // Store journal entry
    journal_table_v2::WriteJournalTable::put_journal_entry(
        ctx.storage,
        invocation_id,
        entry_index,
        // Make sure that a deterministic append time is set based on Bifrost's record creation
        // time. This ensures that the append time does not depend on the application time of
        // the record and ensures that subsequent journal entries have monotonically increasing
        // append times.
        &StoredRawEntry::new(StoredRawEntryHeader::new(ctx.record_created_at), entry),
        &[],
    )
}

impl EndInvocationCommand {
    pub fn new(
        invocation_id: InvocationId,
        invocation_metadata: InFlightInvocationMetadata,
        reason: EndInvocationReason,
    ) -> Self {
        Self {
            invocation_id,
            invocation_metadata,
            reason,
        }
    }
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for EndInvocationCommand
where
    S: WriteInboxTable
        + ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteVirtualObjectStatusTable
        + WriteJournalTable
        + ReadJournalTable
        + WriteOutboxTable
        + WriteFsmTable
        + ReadStateTable
        + WriteStateTable
        + journal_table_v2::WriteJournalTable
        + journal_table_v2::ReadJournalTable
        + ReadVQueueTable
        + WriteVQueueTable
        + WriteLockTable
        + WriteJournalEventsTable
        + WriteTimerTable
        + ReadPromiseTable
        + WritePromiseTable
        + WriteOutputTable,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let EndInvocationCommand {
            invocation_id,
            mut invocation_metadata,
            reason,
        } = self;

        let invocation_target = invocation_metadata.invocation_target.clone();
        let completion_retention = invocation_metadata.completion_retention_duration;
        let journal_retention = invocation_metadata.journal_retention_duration;

        let mut journal_length = invocation_metadata.journal_metadata.length;

        let pinned_service_protocol_version = invocation_metadata
            .pinned_deployment
            .as_ref()
            .map(|pd| pd.service_protocol_version);

        let mut response_cache = ResponseResultCache::new(
            invocation_id,
            journal_length,
            pinned_service_protocol_version.unwrap_or_default(),
        );

        // The feature stores a *reference* (journal entry index) to the output entry instead of
        // inlining the result bytes, and synthesizes a missing output entry on Killed/Failed.
        // Both only work on journal table v2, which is used by invocations pinned to protocol
        // version >= V4: `append_journal_entry` writes into journal table v2, and resolving the
        // reference later reads from it as well (see
        // `ResponseResultCache::read_last_output_entry_result`). Invocations pinned to <= V3 keep
        // their journal in table v1, which has no addressable output entry, so we fall back to
        // inlining the result for them.
        //
        // A missing pinned deployment means no user code has run yet, so the journal only holds
        // entries we wrote ourselves in v2 format; it's safe to treat it as V4.
        let is_write_result_reference_enabled = ctx
            .processor
            .fsm()
            .features()
            .is_write_result_reference_enabled()
            && pinned_service_protocol_version.is_none_or(|v| v >= ServiceProtocolVersion::V4);

        let vqueue_id = invocation_metadata.vqueue_id.clone();

        if is_write_result_reference_enabled {
            // auto append (error) output journal entry if one doesn't exist
            let err = match &reason {
                EndInvocationReason::Killed => Some(KILLED_INVOCATION_ERROR),
                EndInvocationReason::Failed(err) => Some(err.clone()),
                EndInvocationReason::Completed => None,
            };

            if let Some(err) = err {
                append_journal_entry(
                    ctx,
                    &invocation_id,
                    &mut invocation_metadata.journal_metadata,
                    OutputCommand {
                        result: OutputResult::Failure(err.into()),
                        name: Default::default(),
                    },
                )?;

                // make sure journal_length and cache are updated with
                // the new length after the append.
                journal_length = invocation_metadata.journal_metadata.length;
                response_cache = ResponseResultCache::new(
                    invocation_id,
                    journal_length,
                    pinned_service_protocol_version.unwrap_or(ServiceProtocolVersion::V4),
                );
            }
        }

        let end_status = match &reason {
            EndInvocationReason::Killed => vqueue_table::Status::Killed,
            EndInvocationReason::Failed(_) => vqueue_table::Status::Failed,
            EndInvocationReason::Completed => {
                let Some(response_result) = response_cache.response_result(ctx).await? else {
                    // We don't panic on this, although it indicates a bug at the moment.
                    warn!(
                        "Invocation completed without an output entry. This is not supported yet."
                    );
                    return Ok(());
                };

                match response_result {
                    ResponseResult::Success(_) => vqueue_table::Status::Succeeded,
                    ResponseResult::Failure(err) => {
                        if err.code == restate_types::errors::codes::ABORTED {
                            vqueue_table::Status::Cancelled
                        } else {
                            vqueue_table::Status::Failed
                        }
                    }
                }
            }
        };

        let mut retain_journal_index = None;
        // If there are any response sinks, or we need to store back the completed status,
        //  we need to find the latest output entry
        if !invocation_metadata.response_sinks.is_empty() || !completion_retention.is_zero() {
            let response_result_ref = match is_write_result_reference_enabled {
                // always inline, we can't reference the output entry
                false => match reason {
                    EndInvocationReason::Killed => {
                        ResponseResultRef::Failure(KILLED_INVOCATION_ERROR)
                    }
                    EndInvocationReason::Failed(err) => ResponseResultRef::Failure(err),
                    EndInvocationReason::Completed => {
                        let Some(response_result) = response_cache.response_result(ctx).await?
                        else {
                            warn!(
                                "Invocation completed without an output entry. This is not supported yet."
                            );
                            return Ok(());
                        };

                        // bytes are cheaply clonable. Errors not so much.
                        match response_result {
                            ResponseResult::Success(bytes) => {
                                ResponseResultRef::Success(bytes.clone())
                            }
                            ResponseResult::Failure(err) => ResponseResultRef::Failure(err.clone()),
                        }
                    }
                },
                true => {
                    // write result to output table
                    // the output here can be synthetic (on kill or failure) as
                    // done above, or organic from the invocation completion. In call cases,
                    // we need to insert the output into the output table.
                    let Some(response_result) = response_cache.response_result(ctx).await? else {
                        warn!(
                            "Invocation completed without an output entry. This is not supported yet."
                        );
                        return Ok(());
                    };

                    ctx.storage.put_output(&invocation_id, response_result)?;

                    match reason {
                        EndInvocationReason::Killed => ResponseResultRef::Killed,
                        EndInvocationReason::Failed(err) => {
                            ResponseResultRef::Completed(CompletionStatus::Failure(err.code))
                        }
                        EndInvocationReason::Completed => match response_result {
                            ResponseResult::Success(_) => {
                                ResponseResultRef::Completed(CompletionStatus::Success)
                            }
                            ResponseResult::Failure(err) => {
                                ResponseResultRef::Completed(CompletionStatus::Failure(err.code))
                            }
                        },
                    }
                }
            };

            // We still need to create a ResponseResult object to send to sinks
            //
            // Note: the cost of copy is only paid when is_write_result_reference_enabled is disabled.
            // Once is_write_result_reference_enabled is on by default, there will be no copy
            // since everything will be referenced via the Completed state
            let response_result = match &response_result_ref {
                ResponseResultRef::Success(bytes) => ResponseResult::Success(bytes.clone()),
                ResponseResultRef::Failure(err) => ResponseResult::Failure(err.clone()),
                ResponseResultRef::Killed | ResponseResultRef::Completed(_) => {
                    // Note: we can only be here iff response_result has been inserted
                    // into the output table, so it's safe to just unwrap()
                    response_cache.into_response_result(ctx).await?.unwrap()
                }
            };

            // Notify invocation result
            ctx.emit_invocation_end_span(
                &invocation_id,
                &invocation_metadata.invocation_target,
                &invocation_metadata.journal_metadata.span_context,
                match &response_result {
                    ResponseResult::Success(_) => Ok(()),
                    ResponseResult::Failure(err) => Err(err),
                },
            );

            // Send responses out
            ctx.send_response_to_sinks(
                invocation_metadata.response_sinks.clone(),
                response_result,
                Some(invocation_id),
                None,
                Some(&invocation_metadata.invocation_target),
            )?;

            // Store the completed status, if needed
            if !completion_retention.is_zero() {
                retain_journal_index = response_result_ref.referenced_journal_index();

                let completed_invocation = CompletedInvocation::from_in_flight_invocation_metadata(
                    invocation_metadata,
                    if journal_retention.is_zero() {
                        JournalRetentionPolicy::Drop
                    } else {
                        JournalRetentionPolicy::Retain
                    },
                    response_result_ref,
                    ctx.record_created_at,
                );
                ctx.do_store_completed_invocation(invocation_id, completed_invocation)?;
            }
        } else {
            // Just notify Ok, no need to read the output entry
            ctx.emit_invocation_end_span(
                &invocation_id,
                &invocation_target,
                &invocation_metadata.journal_metadata.span_context,
                Ok(()),
            );
        }

        // If no retention, immediately cleanup the invocation status
        if completion_retention.is_zero() {
            ctx.do_free_invocation(&invocation_id)?;
        }

        if journal_retention.is_zero() {
            ctx.do_drop_journal(
                &invocation_id,
                journal_length,
                (0..journal_length)
                    .filter(|idx| retain_journal_index.is_none_or(|retain| retain != *idx)),
                pinned_service_protocol_version,
            )
            .await?;
        }

        if let Some(vqueue_id) = vqueue_id {
            let Some(entry_status) = ctx
                .storage
                .get_vqueue_entry_status(
                    invocation_id.partition_key(),
                    &EntryId::from(invocation_id),
                )
                .await?
            else {
                // Invocation has been removed already!
                return Ok(());
            };
            let record_unique_ts =
                UniqueTimestamp::from_unix_millis_unchecked(ctx.record_created_at);

            VQueue::get(
                &vqueue_id,
                ctx.storage,
                ctx.processor.vqueues_mut(),
                ctx.is_leader.then_some(ctx.action_collector),
            )
            .await?
            .expect("terminate expects vqueue to exist")
            .end(
                record_unique_ts,
                &entry_status,
                end_status,
                completion_retention,
            );
        } else {
            // Consume inbox and move on
            ctx.consume_inbox(&invocation_target).await?;
        }

        Ok(())
    }
}
