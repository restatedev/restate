use crate::debug_if_leader;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::JournalMetadata;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationEpoch;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{CompletionId, EntryMetadata, EntryType};

pub struct WriteJournalEntryCommand<'e> {
    pub invocation_id: InvocationId,
    pub invocation_epoch: InvocationEpoch,
    pub journal_metadata: &'e mut JournalMetadata,
    pub entry: RawEntry,
    pub related_completion_ids: Vec<CompletionId>,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for WriteJournalEntryCommand<'e>
where
    S: JournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let entry_index = self.journal_metadata.length;
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.index = entry_index,
            restate.invocation.id = %self.invocation_id,
            "Write journal entry {:?} to storage",
            self.entry.ty()
        );

        // Store journal entry
        ctx.storage
            .put_journal_entry(
                self.invocation_id,
                self.invocation_epoch,
                entry_index,
                &self.entry,
                &self.related_completion_ids,
            )
            .await?;

        // Update journal length
        self.journal_metadata.length += 1;
        if matches!(self.entry.ty(), EntryType::Command(_)) {
            self.journal_metadata.commands += 1;
        }

        Ok(())
    }
}
