// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod attach_invocation_command;
mod call_commands;
mod clear_all_state_command;
mod clear_state_command;
mod complete_awakeable_command;
mod complete_promise_command;
mod event;
mod get_invocation_output_command;
mod get_lazy_state_command;
mod get_lazy_state_keys_command;
mod get_promise_command;
mod notification;
mod peek_promise_command;
mod send_signal_command;
mod set_state_command;
mod sleep_command;

use crate::debug_if_leader;
use crate::partition::state_machine::entries::attach_invocation_command::ApplyAttachInvocationCommand;
use crate::partition::state_machine::entries::call_commands::{
    ApplyCallCommand, ApplyOneWayCallCommand,
};
use crate::partition::state_machine::entries::clear_all_state_command::ApplyClearAllStateCommand;
use crate::partition::state_machine::entries::clear_state_command::ApplyClearStateCommand;
use crate::partition::state_machine::entries::complete_awakeable_command::ApplyCompleteAwakeableCommand;
use crate::partition::state_machine::entries::complete_promise_command::ApplyCompletePromiseCommand;
use crate::partition::state_machine::entries::event::ApplyEventCommand;
use crate::partition::state_machine::entries::get_invocation_output_command::ApplyGetInvocationOutputCommand;
use crate::partition::state_machine::entries::get_lazy_state_command::ApplyGetLazyStateCommand;
use crate::partition::state_machine::entries::get_lazy_state_keys_command::ApplyGetLazyStateKeysCommand;
use crate::partition::state_machine::entries::get_promise_command::ApplyGetPromiseCommand;
use crate::partition::state_machine::entries::notification::ApplyNotificationCommand;
use crate::partition::state_machine::entries::peek_promise_command::ApplyPeekPromiseCommand;
use crate::partition::state_machine::entries::send_signal_command::ApplySendSignalCommand;
use crate::partition::state_machine::entries::set_state_command::ApplySetStateCommand;
use crate::partition::state_machine::entries::sleep_command::ApplySleepCommand;
use crate::partition::state_machine::lifecycle::VerifyOrMigrateJournalTableToV2Command;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{
    Command, CommandMetadata, Completion, Entry, EntryMetadata, EntryType,
};
use std::collections::VecDeque;
use tracing::info;

pub(super) struct OnJournalEntryCommand {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: InvocationStatus,
    pub(super) entry: RawEntry,
}

impl OnJournalEntryCommand {
    pub(super) fn from_entry(
        invocation_id: InvocationId,
        invocation_status: InvocationStatus,
        entry: Entry,
    ) -> Self {
        Self {
            invocation_id,
            invocation_status,
            entry: entry.encode::<ServiceProtocolV4Codec>(),
        }
    }

    pub(super) fn from_raw_entry(
        invocation_id: InvocationId,
        invocation_status: InvocationStatus,
        entry: RawEntry,
    ) -> Self {
        Self {
            invocation_id,
            invocation_status,
            entry,
        }
    }
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnJournalEntryCommand
where
    S: JournalTable
        + journal_table_v1::JournalTable
        + InvocationStatusTable
        + TimerTable
        + FsmTable
        + OutboxTable
        + PromiseTable
        + StateTable,
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        if !matches!(self.invocation_status, InvocationStatus::Invoked(_))
            && !matches!(self.invocation_status, InvocationStatus::Suspended { .. })
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
        while let Some(entry) = entries.pop_front() {
            // We need this information to store the journal entry!
            let mut related_completion_ids = vec![];

            // --- Process entry effect
            match entry.ty() {
                EntryType::Command(_) => {
                    let cmd = entry.decode::<ServiceProtocolV4Codec, Command>()?;
                    related_completion_ids = cmd.related_completion_ids();
                    match cmd {
                        Command::Input(_)
                        | Command::Output(_)
                        | Command::Run(_)
                        | Command::GetEagerState(_)
                        | Command::GetEagerStateKeys(_) => {
                            // For these entries, we don't need to perform operations, we just need to store them
                        }

                        Command::GetLazyState(entry) => {
                            ApplyGetLazyStateCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::SetState(entry) => {
                            ApplySetStateCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &mut self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::ClearState(entry) => {
                            ApplyClearStateCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &mut self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::ClearAllState(entry) => {
                            ApplyClearAllStateCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &mut self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::GetLazyStateKeys(entry) => {
                            ApplyGetLazyStateKeysCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &mut self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }

                        Command::GetPromise(entry) => {
                            ApplyGetPromiseCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &mut self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::PeekPromise(entry) => {
                            ApplyPeekPromiseCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &mut self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::CompletePromise(entry) => {
                            ApplyCompletePromiseCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &mut self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }

                        Command::Sleep(entry) => {
                            ApplySleepCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }

                        Command::Call(entry) => {
                            ApplyCallCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::OneWayCall(entry) => {
                            ApplyOneWayCallCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::SendSignal(entry) => {
                            ApplySendSignalCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::AttachInvocation(entry) => {
                            ApplyAttachInvocationCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::GetInvocationOutput(entry) => {
                            ApplyGetInvocationOutputCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                        Command::CompleteAwakeable(entry) => {
                            ApplyCompleteAwakeableCommand {
                                invocation_id: self.invocation_id,
                                invocation_status: &self.invocation_status,
                                entry,
                                completions_to_process: &mut entries,
                            }
                            .apply(ctx)
                            .await?;
                        }
                    }
                }

                et @ EntryType::Notification(_) => {
                    ApplyNotificationCommand {
                        invocation_id: self.invocation_id,
                        invocation_status: &mut self.invocation_status,
                        entry: entry
                            .inner
                            .try_as_notification_ref()
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
                            .try_as_event_ref()
                            .ok_or(Error::BadEntryVariant(EntryType::Event))?,
                    }
                    .apply(ctx)
                    .await?;
                }
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
            JournalTable::put_journal_entry(
                ctx.storage,
                self.invocation_id,
                entry_index,
                &entry,
                &related_completion_ids,
            )
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
            .await
            .map_err(Error::Storage)?;

        Ok(())
    }
}

struct ApplyJournalCommandEffect<'e, CMD> {
    invocation_id: InvocationId,
    invocation_status: &'e InvocationStatus,
    entry: CMD,
    completions_to_process: &'e mut VecDeque<RawEntry>,
}

impl<CMD> ApplyJournalCommandEffect<'_, CMD> {
    fn then_apply_completion(&mut self, e: impl Into<Completion>) {
        self.completions_to_process
            .push_back(Entry::from(e.into()).encode::<ServiceProtocolV4Codec>())
    }
}
