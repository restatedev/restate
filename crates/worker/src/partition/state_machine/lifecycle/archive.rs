// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::debug_if_leader;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InvocationStatus, InvocationStatusTable, JournalMetadata,
};
use restate_storage_api::journal_table_v2::JournalTable;
use restate_types::identifiers::InvocationId;
use std::time::Duration;

/// This command is used in restart to archive an invocation.
pub struct ArchiveInvocationCommand {
    pub invocation_id: InvocationId,
    pub completed_invocation: CompletedInvocation,
    pub previous_attempt_retention_override: Option<Duration>,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ArchiveInvocationCommand
where
    S: InvocationStatusTable + JournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let ArchiveInvocationCommand {
            invocation_id,
            completed_invocation: mut completed_invocation_status,
            previous_attempt_retention_override,
        } = self;

        if let Some(previous_attempt_retention_override) = previous_attempt_retention_override {
            completed_invocation_status.completion_retention_duration =
                previous_attempt_retention_override;
            completed_invocation_status.journal_retention_duration =
                previous_attempt_retention_override;
        }

        let should_retain_status = !completed_invocation_status
            .completion_retention_duration
            .is_zero();
        let should_retain_journal = should_retain_status
            && !completed_invocation_status
                .journal_retention_duration
                .is_zero();

        if !should_retain_journal {
            completed_invocation_status.journal_metadata = JournalMetadata::empty();
        }

        completed_invocation_status.timestamps.update();

        let journal_length = completed_invocation_status.journal_metadata.length;
        let invocation_epoch = completed_invocation_status.invocation_epoch;

        if should_retain_status {
            debug_if_leader!(ctx.is_leader, "Archiving invocation metadata");
            ctx.storage
                .archive_invocation_status_to_epoch(
                    &invocation_id,
                    completed_invocation_status.invocation_epoch,
                    &InvocationStatus::Completed(completed_invocation_status),
                )
                .await?;
        }
        if should_retain_journal {
            debug_if_leader!(ctx.is_leader, "Archiving invocation journal");
            ctx.storage
                .archive_journal_to_epoch(&invocation_id, invocation_epoch, journal_length)
                .await?;
        }

        Ok(())
    }
}
