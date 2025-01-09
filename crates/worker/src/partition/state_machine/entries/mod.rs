// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod call_commands;
mod event;
mod notification;
mod sleep_command;

use crate::debug_if_leader;
use crate::partition::state_machine::entries::call_commands::{
    ApplyCallCommand, ApplyOneWayCallCommand,
};
use crate::partition::state_machine::entries::event::ApplyEventCommand;
use crate::partition::state_machine::entries::notification::ApplyNotificationCommand;
use crate::partition::state_machine::entries::sleep_command::ApplySleepCommand;
use crate::partition::state_machine::lifecycle::VerifyOrMigrateJournalTableToV2Command;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{CommandType, EntryMetadata, EntryType};
use std::collections::VecDeque;
use tracing::info;

pub(super) struct OnJournalEntryCommand {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: InvocationStatus,
    pub(super) entry: RawEntry,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnJournalEntryCommand
where
    S: JournalTable
        + journal_table_v1::JournalTable
        + InvocationStatusTable
        + TimerTable
        + FsmTable
        + OutboxTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        if !matches!(self.invocation_status, InvocationStatus::Invoked(_))
            || !matches!(self.invocation_status, InvocationStatus::Suspended { .. })
        {
            info!(
                "Received entry for invocation that is not invoked nor suspended. Ignoring the effect."
            );
            return Ok(());
        }

        // In case we get a notification (e.g. awakeable completion),
        // but we haven't pinned the deployment yet, we might need to run a migration to V2.
        if let Some(meta) = self.invocation_status.get_invocation_metadata() {
            if meta.pinned_deployment.is_none() {
                // The pinned deployment wasn't established yet, but we have a V2 journal entry.
                // So we need to try to run the migration
                VerifyOrMigrateJournalTableToV2Command {
                    invocation_id: self.invocation_id,
                    journal_length: meta.journal_metadata.length,
                }
                .apply(ctx)
                .await?;
            }
        }

        let mut entries = VecDeque::from([self.entry]);
        while let Some(mut entry) = entries.pop_front() {
            // --- Process entry effect
            match entry.ty() {
                EntryType::Command(CommandType::Input) => {
                    // Nothing to do, just process it
                }
                EntryType::Command(CommandType::Run) => {
                    // Just store it
                }
                EntryType::Command(CommandType::Sleep) => {
                    ApplySleepCommand {
                        invocation_id: self.invocation_id,
                        invocation_status: &mut self.invocation_status,
                        entry: entry.decode::<ServiceProtocolV4Codec, _>()?,
                    }
                    .apply(ctx)
                    .await?;
                }
                EntryType::Command(CommandType::Call) => {
                    ApplyCallCommand {
                        caller_invocation_id: self.invocation_id,
                        caller_invocation_status: &self.invocation_status,
                        entry: entry.decode::<ServiceProtocolV4Codec, _>()?,
                        additional_entries_to_process: &mut entries,
                    }
                    .apply(ctx)
                    .await?;
                }
                EntryType::Command(CommandType::OneWayCall) => {
                    ApplyOneWayCallCommand {
                        caller_invocation_id: self.invocation_id,
                        caller_invocation_status: &self.invocation_status,
                        entry: entry.decode::<ServiceProtocolV4Codec, _>()?,
                        additional_entries_to_process: &mut entries,
                    }
                    .apply(ctx)
                    .await?;
                }
                EntryType::Command(CommandType::Output) => {
                    // Just store it, on End we send back the responses
                }
                et @ EntryType::Notification(_) => {
                    ApplyNotificationCommand {
                        invocation_id: self.invocation_id,
                        invocation_status: &mut self.invocation_status,
                        entry: entry
                            .inner
                            .try_as_notification_mut()
                            .ok_or(Error::BadEntryVariant(et))?,
                    }
                    .apply(ctx)
                    .await?;
                }
                EntryType::Event => {
                    ApplyEventCommand {
                        invocation_id: self.invocation_id,
                        invocation_status: &mut self.invocation_status,
                        entry: entry
                            .inner
                            .try_as_event_mut()
                            .ok_or(Error::BadEntryVariant(EntryType::Event))?,
                    }
                    .apply(ctx)
                    .await?;
                }
                _ => todo!("slinkydeveloper tomorrow i need to finish this!"),
            };

            // -- Append journal entry
            let journal_meta = self
                .invocation_status
                .get_journal_metadata_mut()
                .expect("At this point there must be a journal");

            let entry_index = journal_meta.length;
            debug_if_leader!(
                ctx.is_leader,
                restate.journal.index = entry_index,
                restate.invocation.id = %self.invocation_id,
                "Write journal entry {:?} to storage",
                entry.ty()
            );

            // Store journal entry
            JournalTable::put_journal_entry(ctx.storage, self.invocation_id, entry_index, &entry)
                .await?;

            // Update journal length
            journal_meta.length += 1;
        }

        // Update timestamps
        if let Some(timestamps) = self.invocation_status.get_timestamps_mut() {
            timestamps.update();
        }

        // Store invocation status
        ctx.storage
            .put_invocation_status(&self.invocation_id, &self.invocation_status)
            .await;

        Ok(())
    }
}
