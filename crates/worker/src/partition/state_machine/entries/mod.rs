mod event;
mod notification;
mod sleep_command;

use crate::debug_if_leader;
use crate::partition::state_machine::entries::event::ApplyEventCommand;
use crate::partition::state_machine::entries::notification::ApplyNotificationCommand;
use crate::partition::state_machine::entries::sleep_command::ApplySleepCommand;
use crate::partition::state_machine::{ CommandHandler, Error, StateMachineApplyContext};
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table_v2::JournalTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{CommandType, EntryMetadata, EntryType};
use tracing::info;

pub(super) struct OnJournalEntryCommand {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: InvocationStatus,
    pub(super) entry: RawEntry,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnJournalEntryCommand
where
    S: JournalTable + InvocationStatusTable + TimerTable,
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

        match self.entry.ty() {
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
                    entry: self.entry.deserialize_to::<ServiceProtocolV4Codec, _>()?,
                }
                .apply(ctx)
                .await?;
            }
            EntryType::Command(CommandType::Call) => {
                todo!()
            }
            EntryType::Command(CommandType::OneWayCall) => {
                todo!()
            }
            EntryType::Command(CommandType::Output) => {
                // Just store it, on End we send back the responses
            }
            EntryType::Notification => {
                ApplyNotificationCommand {
                    invocation_id: self.invocation_id,
                    invocation_status: &mut self.invocation_status,
                    entry: self
                        .entry
                        .inner
                        .try_as_notification_mut()
                        .ok_or(Error::BadEntryVariant(EntryType::Notification))?,
                }
                .apply(ctx)
                .await?;
            }
            EntryType::Event => {
                ApplyEventCommand {
                    invocation_id: self.invocation_id,
                    invocation_status: &mut self.invocation_status,
                    entry: self
                        .entry
                        .inner
                        .try_as_event_mut()
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
            self.entry.ty()
        );

        // Store journal entry
        ctx.storage
            .put_journal_entry(self.invocation_id, entry_index, &self.entry)
            .await?;

        // Update journal length
        journal_meta.length += 1;

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
