use crate::debug_if_leader;
use crate::partition::state_machine::{Action, CommandHandler, Error, StateMachineApplyContext};
use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_types::identifiers::InvocationId;

pub struct ResumeInvocationCommand<'e> {
    pub invocation_id: InvocationId,
    pub invocation_status: &'e mut InvocationStatus,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ResumeInvocationCommand<'e>
{
    async fn apply(mut self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let InvocationStatus::Suspended { metadata, .. } = self.invocation_status else {
            // Nothing to do, we're not suspended
            return Ok(());
        };

        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Resume service"
        );
        let invocation_target = metadata.invocation_target.clone();

        metadata.timestamps.update();
        self.invocation_status = &mut InvocationStatus::Invoked(metadata.clone());

        ctx.action_collector.push(Action::Invoke {
            invocation_id: self.invocation_id,
            invocation_target,
            invoke_input_journal: InvokeInputJournal::NoCachedJournal,
        });

        Ok(())
    }
}
