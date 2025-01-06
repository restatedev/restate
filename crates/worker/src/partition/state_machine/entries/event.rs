#![allow(dead_code)]

use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::Event;

pub(super) struct ApplyEventCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) entry: &'e mut Event,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyEventCommand<'e>
{
    async fn apply(self, _ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        // TODO is there anything to do here?!
        Ok(())
    }
}
