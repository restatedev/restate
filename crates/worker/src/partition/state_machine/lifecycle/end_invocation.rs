// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert2::assert;
use tracing::warn;

use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::blob_table::WriteBlobTable;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::inbox_table::WriteInboxTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, CompletedResult, CompletionStatus, InFlightInvocationMetadata,
    JournalRetentionPolicy, ReadInvocationStatusTable, ResponseResultRef,
    WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::journal_table::{JournalEntry, ReadJournalTable, WriteJournalTable};
use restate_storage_api::journal_table_v2;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::{ReadPromiseTable, WritePromiseTable};
use restate_storage_api::service_status_table::WriteVirtualObjectStatusTable;
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_storage_api::vqueue_table::{self, ReadVQueueTable, WriteVQueueTable};
use restate_types::blob::BlobId;
use restate_types::errors::{InvocationError, KILLED_INVOCATION_ERROR};
use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid};
use restate_types::invocation::ResponseResult;
use restate_types::journal::EntryType;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{self, CommandType, EntryMetadata, OutputCommand, OutputResult};
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::vqueues::EntryTargetExt;
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
    /// The invoker reported the invocation ran to completion via an `end` message. The result is read from the
    /// last Output entry in the journal.
    End,
    /// The invoker reported a terminal failure, The entry is appended at the end of the journal.
    Failed(InvocationError),
    /// Invocation completed normally by producing an output. `entry` is the Output entry as
    /// received from the deployment, stored as-is.
    Completed {
        output: OutputCommand,
        entry: RawEntry,
    },
    /// The invocation was killed.
    Killed,
}

struct ResponseResultLoader {
    invocation_id: InvocationId,
    journal_length: u32,
    protocol_version: ServiceProtocolVersion,
}

impl ResponseResultLoader {
    fn new(
        invocation_id: InvocationId,
        journal_length: u32,
        protocol_version: ServiceProtocolVersion,
    ) -> Self {
        Self {
            invocation_id,
            journal_length,
            protocol_version,
        }
    }

    async fn read_last_output_entry_result<'s, S, P>(
        self,
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
                    let cmd = journal_table_v2::ReadJournalTable::hydrate_journal_entry(
                        ctx.storage,
                        entry,
                    )?
                    .decode::<ServiceProtocolV4Codec, OutputCommand>()?;
                    return Ok(Some(cmd.result.into()));
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
                    assert!(let
                        restate_types::journal::Entry::Output(e) =
                            enriched_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                    );
                    Ok(e.result.into())
                })
                .transpose()
        }
    }
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
        + WriteBlobTable,
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
        let delete_pending_timers = InvocationUuid::is_deterministic(
            &invocation_target,
            invocation_metadata.idempotency_key.as_deref(),
        );

        let pinned_service_protocol_version = invocation_metadata
            .pinned_deployment
            .as_ref()
            .map(|pd| pd.service_protocol_version);

        // With the feature, the completed status refers to the content blob of the invocation's
        // Output entry instead of inlining the result, if the result is large enough to have one.
        // An End reads the result from an Output journaled before the feature was enabled (with the
        // feature, the Output ends the invocation right away), keep inlining it.
        let is_invocation_status_output_blob_enabled = ctx
            .processor
            .fsm()
            .features()
            .is_invocation_status_output_blob_enabled()
            && !matches!(reason, EndInvocationReason::End);

        // With the blob table, the journal ends with an Output entry (synthesized for killed/failed
        // invocations), whose content blob, if any, is shared with the completed status. Only v2
        // journals can hold it. It's appended even if the journal is dropped right after, if the
        // completed status is kept: the blob outlives the journal.
        //
        // Unpinned invocations are skipped: they never reached a deployment, so their journal is
        // only the Input, and it may still be in the v1 table if the invocation was created before
        // journal v2 became the default (it's only migrated when pinning or when a v2 entry is
        // applied, see `VerifyOrMigrateJournalTableToV2Command`). Appending to v2 here would split
        // the journal across both tables. Their result is inlined instead: a kill's error is
        // short, and so are pre-pinning failures, which are invoker errors.
        let append_output_entry_to_journal = is_invocation_status_output_blob_enabled
            && !(journal_retention.is_zero() && completion_retention.is_zero())
            && pinned_service_protocol_version
                .is_some_and(|version| version >= ServiceProtocolVersion::V4);

        let mut journal_length = invocation_metadata.journal_metadata.length;

        let response_cache = ResponseResultLoader::new(
            invocation_id,
            journal_length,
            pinned_service_protocol_version.unwrap_or_default(),
        );

        let vqueue_id = invocation_metadata.vqueue_id.clone();
        // Before the metadata moves into the completed status.
        let finished_at = ctx.vqueue_finished_at(&invocation_metadata.timestamps);

        // Killed/Failed are known upfront, End/Completed are refined from the output below
        let mut end_status = match &reason {
            EndInvocationReason::Killed => vqueue_table::Status::Killed,
            EndInvocationReason::Failed(err)
                if err.code == restate_types::errors::codes::ABORTED =>
            {
                vqueue_table::Status::Cancelled
            }
            EndInvocationReason::Failed(_) => vqueue_table::Status::Failed,
            EndInvocationReason::End | EndInvocationReason::Completed { .. } => {
                vqueue_table::Status::Succeeded
            }
        };

        // If there are any response sinks, or we need to store back the completed status,
        //  we need to find the latest output entry
        if !invocation_metadata.response_sinks.is_empty() || !completion_retention.is_zero() {
            let output = match &reason {
                EndInvocationReason::Completed { output, .. } => match &output.result {
                    OutputResult::Success(bytes) => ResponseResult::Success(bytes.clone()),
                    OutputResult::Failure(failure) => {
                        ResponseResult::Failure(failure.clone().into())
                    }
                },
                EndInvocationReason::Killed => ResponseResult::Failure(KILLED_INVOCATION_ERROR),
                EndInvocationReason::Failed(error) => ResponseResult::Failure(error.clone()),
                EndInvocationReason::End => {
                    // If we receive and End. It means the output has already
                    // been written to the journal table. We need to load this out
                    //
                    // todo: If we return now because an output was not found this means this invocation
                    // will always remain in "invoked" state. This is specially bad for VOs because the VO
                    // will remain locked forever.
                    // Maybe it's possible to synthesis an empty (Void) output here instead of returning Ok(())
                    let Some(output) = response_cache.read_last_output_entry_result(ctx).await?
                    else {
                        warn!(
                            "Invocation completed without an output entry. This is not supported yet."
                        );
                        return Ok(());
                    };
                    output
                }
            };

            if let (
                EndInvocationReason::End | EndInvocationReason::Completed { .. },
                ResponseResult::Failure(err),
            ) = (&reason, &output)
            {
                end_status = if err.code == restate_types::errors::codes::ABORTED {
                    vqueue_table::Status::Cancelled
                } else {
                    vqueue_table::Status::Failed
                };
            }

            // A large result is stored in the content blob of the Output entry
            let output_blob_id = if append_output_entry_to_journal {
                let entry = match &reason {
                    // Cheap clone, the content is ref-counted
                    EndInvocationReason::Completed { entry, .. } => entry.clone(),
                    // Synthesized for killed/failed invocations
                    EndInvocationReason::Failed(_) | EndInvocationReason::Killed => {
                        journal_v2::Entry::from(OutputCommand {
                            result: output.clone().into(),
                            name: Default::default(),
                        })
                        .encode::<ServiceProtocolV4Codec>()
                    }
                    EndInvocationReason::End => {
                        unreachable!(
                            "is_invocation_status_output_blob_enabled is explicitly disabled on End"
                        )
                    }
                };
                let blob_id =
                    append_output_entry(ctx, invocation_id, &mut invocation_metadata, entry)?;
                journal_length = invocation_metadata.journal_metadata.length;
                blob_id
            } else {
                None
            };

            let response_result_ref = match output_blob_id {
                Some(blob_id) if !completion_retention.is_zero() => {
                    // The completed status shares the blob, released when it's purged
                    ctx.storage.increment_ref_count(blob_id, 1);
                    let status = match &output {
                        ResponseResult::Success(_) => CompletionStatus::Success,
                        ResponseResult::Failure(err) => CompletionStatus::failure(err),
                    };
                    ResponseResultRef::Completed(CompletedResult { blob_id, status })
                }
                _ => match reason {
                    EndInvocationReason::Killed => {
                        ResponseResultRef::Failure(KILLED_INVOCATION_ERROR)
                    }
                    EndInvocationReason::Failed(err) => ResponseResultRef::Failure(err),
                    EndInvocationReason::End | EndInvocationReason::Completed { .. } => {
                        // bytes are cheaply cloneable. Errors not so much.
                        match &output {
                            ResponseResult::Success(bytes) => {
                                ResponseResultRef::Success(bytes.clone())
                            }
                            ResponseResult::Failure(err) => ResponseResultRef::Failure(err.clone()),
                        }
                    }
                },
            };

            // Notify invocation result
            ctx.emit_invocation_end_span(
                &invocation_id,
                &invocation_metadata.invocation_target,
                &invocation_metadata.journal_metadata.span_context,
                match &output {
                    ResponseResult::Success(_) => Ok(()),
                    ResponseResult::Failure(err) => Err(err),
                },
            );

            // Send responses out
            ctx.send_response_to_sinks(
                invocation_metadata.response_sinks.clone(),
                output,
                Some(invocation_id),
                None,
                Some(&invocation_metadata.invocation_target),
            )?;

            // Store the completed status, if needed
            if !completion_retention.is_zero() {
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
            ctx.do_free_invocation(&invocation_id, None)?;
        }

        if journal_retention.is_zero() {
            ctx.do_drop_journal(
                &invocation_id,
                journal_length,
                pinned_service_protocol_version,
                delete_pending_timers,
            )
            .await?;
        }

        if let Some(vqueue_id) = vqueue_id {
            let entry_status = ctx
                .storage
                .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
                .await?;

            let Some(entry_status) = entry_status else {
                // Invocation has been removed already!
                return Ok(());
            };
            VQueue::get(
                &vqueue_id,
                ctx.storage,
                ctx.processor.vqueues_mut(),
                ctx.leader.is_some().then_some(ctx.action_collector),
            )
            .await?
            .expect("terminate expects vqueue to exist")
            .end(
                finished_at,
                &entry_status,
                &invocation_target.entry_target_ref(),
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

/// Appends the Output entry, returning its content blob, if any.
fn append_output_entry<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    invocation_metadata: &mut InFlightInvocationMetadata,
    entry: RawEntry,
) -> Result<Option<BlobId>, Error>
where
    S: journal_table_v2::WriteJournalTable,
    P: ProcessorContext,
{
    // Not counted in the journal usage: the deployment's Output was already counted when received,
    // and the synthesized one (killed/failed) isn't written by the user.

    let journal_metadata = &mut invocation_metadata.journal_metadata;
    let blob_id = journal_table_v2::WriteJournalTable::put_journal_entry(
        ctx.storage,
        &invocation_id,
        journal_metadata.length,
        &StoredRawEntry::new(StoredRawEntryHeader::new(ctx.record_created_at), entry),
        &[],
    )?;
    journal_metadata.length += 1;
    journal_metadata.commands += 1;
    Ok(blob_id)
}
