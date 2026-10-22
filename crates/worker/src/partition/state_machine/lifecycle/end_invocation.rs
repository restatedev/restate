// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU16;

use assert2::assert;
use tracing::warn;

use restate_clock::UniqueTimestamp;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::blob_table::{BlobBuilder, WriteBlobTable};
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
use restate_storage_api::vqueue_table::{
    self, EntryStatusHeader, ReadVQueueTable, WriteVQueueTable,
};
use restate_types::blob::{BaseBlobId, BlobKind};
use restate_types::errors::{InvocationError, KILLED_INVOCATION_ERROR};
use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid};
use restate_types::invocation::ResponseResult;
use restate_types::journal::EntryType;
use restate_types::journal_v2::{self, CommandType, EntryMetadata, OutputCommand, OutputResult};
use restate_types::service_protocol::ServiceProtocolVersion;
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
    /// Invocation completed normally by producing an output
    Completed(OutputCommand),
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
            invocation_metadata,
            reason,
        } = self;

        let invocation_target = invocation_metadata.invocation_target.clone();
        let completion_retention = invocation_metadata.completion_retention_duration;
        let journal_retention = invocation_metadata.journal_retention_duration;
        let delete_pending_timers = InvocationUuid::is_deterministic(
            &invocation_target,
            invocation_metadata.idempotency_key.as_deref(),
        );

        let journal_length = invocation_metadata.journal_metadata.length;

        let pinned_service_protocol_version = invocation_metadata
            .pinned_deployment
            .as_ref()
            .map(|pd| pd.service_protocol_version);

        let response_cache = ResponseResultLoader::new(
            invocation_id,
            journal_length,
            pinned_service_protocol_version.unwrap_or_default(),
        );

        let entry_status = if invocation_metadata.vqueue_id.is_some() {
            ctx.storage
                .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
                .await?
        } else {
            None
        };

        let sequence = entry_status
            .as_ref()
            .map(|r| r.seq())
            .or(invocation_metadata.creation_seq);

        let vqueue_id = invocation_metadata.vqueue_id.clone();

        // The feature stores a *reference* (journal entry index) to the blob table instead of
        // inlining the result bytes, and synthesizes a missing output entry on Killed/Failed.
        // note: write_blob_table is only possible if canonical entry id is known
        let is_write_blob_table_enabled =
            ctx.processor.fsm().features().is_write_blob_table_enabled() && sequence.is_some();

        // Killed/Failed are known upfront, End/Completed are refined from the output below
        let mut end_status = match &reason {
            EndInvocationReason::Killed => vqueue_table::Status::Killed,
            EndInvocationReason::Failed(err)
                if err.code == restate_types::errors::codes::ABORTED =>
            {
                vqueue_table::Status::Cancelled
            }
            EndInvocationReason::Failed(_) => vqueue_table::Status::Failed,
            EndInvocationReason::End | EndInvocationReason::Completed(_) => {
                vqueue_table::Status::Succeeded
            }
        };

        // If there are any response sinks, or we need to store back the completed status,
        //  we need to find the latest output entry
        if !invocation_metadata.response_sinks.is_empty() || !completion_retention.is_zero() {
            let output = match &reason {
                EndInvocationReason::Completed(output) => match &output.result {
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
                EndInvocationReason::End | EndInvocationReason::Completed(_),
                ResponseResult::Failure(err),
            ) = (&reason, &output)
            {
                end_status = if err.code == restate_types::errors::codes::ABORTED {
                    vqueue_table::Status::Cancelled
                } else {
                    vqueue_table::Status::Failed
                };
            }

            let response_result_ref = if completion_retention.is_zero() {
                // Nothing to store back
                None
            } else if is_write_blob_table_enabled {
                let status = match reason {
                    EndInvocationReason::Killed => CompletionStatus::Killed,
                    EndInvocationReason::Failed(err) => CompletionStatus::Failure(err.code),
                    EndInvocationReason::End | EndInvocationReason::Completed(_) => match &output {
                        ResponseResult::Success(_) => CompletionStatus::Success,
                        ResponseResult::Failure(err) => CompletionStatus::Failure(err.code),
                    },
                };

                // todo(azmy): Once we use the blob for outbox we can
                // add the partition processor response sinks to the initial ref count

                // initial_ref_count += invocation_metadata
                //     .response_sinks
                //     .iter()
                //     .filter(|sink| sink.is_partition_processor())
                //     .count();

                // todo: In the future we can use the Lsn/Seq of the Output journal instead of the
                // invocation journal. this change is not breaking because we store the BaseBlobId of the
                // output blob on the invocation status as long as it's coordinated with a partition feature.
                let blob_id =
                    BaseBlobId::generate_output_blob_id(&invocation_id, sequence.unwrap());
                ctx.storage.put_blob(
                    blob_id,
                    BlobBuilder::default()
                        .with_ref_count(NonZeroU16::MIN)
                        .with_kind(BlobKind::Output)
                        .typed(&output),
                )?;
                Some(ResponseResultRef::Completed(CompletedResult {
                    blob_id,
                    status,
                }))
            } else {
                // always inline, we can't reference the output entry
                Some(match reason {
                    EndInvocationReason::Killed => {
                        ResponseResultRef::Failure(KILLED_INVOCATION_ERROR)
                    }
                    EndInvocationReason::Failed(err) => ResponseResultRef::Failure(err),
                    EndInvocationReason::End | EndInvocationReason::Completed(_) => {
                        // bytes are cheaply clonable. Errors not so much.
                        match &output {
                            ResponseResult::Success(bytes) => {
                                ResponseResultRef::Success(bytes.clone())
                            }
                            ResponseResult::Failure(err) => ResponseResultRef::Failure(err.clone()),
                        }
                    }
                })
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
            if let Some(response_result_ref) = response_result_ref {
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
            let Some(entry_status) = entry_status else {
                // Invocation has been removed already!
                return Ok(());
            };
            let record_unique_ts =
                UniqueTimestamp::from_unix_millis_unchecked(ctx.record_created_at);

            VQueue::get(
                &vqueue_id,
                ctx.storage,
                ctx.processor.vqueues_mut(),
                ctx.leader.is_some().then_some(ctx.action_collector),
            )
            .await?
            .expect("terminate expects vqueue to exist")
            .end(
                record_unique_ts,
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
