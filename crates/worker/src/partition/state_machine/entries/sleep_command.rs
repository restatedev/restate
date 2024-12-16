use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::timer_table::TimerTable;
use restate_tracing_instrumentation as instrumentation;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::command::SleepCommand;
use restate_wal_protocol::timer::TimerKeyValue;

pub(super) struct HandleSleepCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) entry: SleepCommand,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for HandleSleepCommand<'e>
where
    S: TimerTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        // Create the instrumentation span
        let _span = instrumentation::info_invocation_span!(
            relation = invocation_metadata
                .journal_metadata
                .span_context
                .as_parent(),
            id = self.invocation_id,
            name = "sleep",
            tags = (rpc.service = invocation_metadata
                .invocation_target
                .service_name()
                .to_string())
        );

        ctx.register_timer(
            TimerKeyValue::complete_journal_entry(
                self.entry.wake_up_time,
                self.invocation_id,
                self.entry.completion_id,
            ),
            invocation_metadata.journal_metadata.span_context.clone(),
        )
        .await?;

        Ok(())
    }
}
