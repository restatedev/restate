mod pinned_deployment;

use crate::metric_definitions::PARTITION_HANDLE_INVOKER_EFFECT_COMMAND;
use crate::partition::state_machine::actions::ActionCollector;
use crate::partition::state_machine::commands::{
    ApplicableCommand, CommandContext, Error, MaybeApplicableCommand,
};
use crate::partition::state_machine::Action;
use crate::partition::types::{InvokerEffect, InvokerEffectKind};
use metrics::histogram;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
};
use restate_storage_api::Transaction;
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::InvocationId;
use restate_types::journal::raw::RawEntryCodec;
use std::time::Instant;
use tracing::trace;

struct InvokerEffectCommand {
    invocation_id: InvocationId,
    in_flight_invocation_metadata: InFlightInvocationMetadata,
    effect_kind: InvokerEffectKind,
}

impl<Storage, Actions, Codec> MaybeApplicableCommand<Storage, Actions, Codec> for InvokerEffect
where
    Storage: Transaction + Send,
    Actions: ActionCollector,
    Codec: RawEntryCodec,
{
    async fn apply(
        self,
        mut ctx: CommandContext<'_, Storage, Actions, Codec>,
    ) -> Result<Result<(), Error>, Self> {
        let start = Instant::now();
        let status = match ctx.get_invocation_status(&self.invocation_id).await {
            Ok(is) => is,
            Err(e) => {
                histogram!(PARTITION_HANDLE_INVOKER_EFFECT_COMMAND).record(start.elapsed());
                return Ok(Err(e));
            }
        };

        match status {
            InvocationStatus::Invoked(invocation_metadata) => {
                let res = InvokerEffectCommand {
                    invocation_id: self.invocation_id,
                    in_flight_invocation_metadata: invocation_metadata,
                    effect_kind: self.kind,
                }
                .apply(ctx)
                .await;
                match res {
                    Ok(res) => {
                        // Record the histogram only in the success case
                        histogram!(PARTITION_HANDLE_INVOKER_EFFECT_COMMAND).record(start.elapsed());
                        Ok(res)
                    }
                    Err(cmd) => Err(InvokerEffect {
                        invocation_id: cmd.invocation_id,
                        kind: cmd.effect_kind,
                    }),
                }
            }
            _ => {
                trace!("Received invoker effect for unknown service invocation. Ignoring the effect and aborting.");
                ctx.actions
                    .append(Action::AbortInvocation(self.invocation_id));
                histogram!(PARTITION_HANDLE_INVOKER_EFFECT_COMMAND).record(start.elapsed());
                Ok(Ok(()))
            }
        }
    }
}

struct InvokerPinnedDeploymentCommand {
    invocation_id: InvocationId,
    in_flight_invocation_metadata: InFlightInvocationMetadata,
    pinned_deployment: PinnedDeployment,
}

impl<Storage: InvocationStatusTable, Actions, Codec> MaybeApplicableCommand<Storage, Actions, Codec>
    for InvokerEffectCommand
{
    async fn apply(
        self,
        ctx: CommandContext<'_, Storage, Actions, Codec>,
    ) -> Result<Result<(), Error>, Self> {
        match self.effect_kind {
            InvokerEffectKind::PinnedDeployment(pinned_deployment) => {
                Ok(InvokerPinnedDeploymentCommand {
                    invocation_id: self.invocation_id,
                    in_flight_invocation_metadata: self.in_flight_invocation_metadata,
                    pinned_deployment,
                }
                .apply(ctx)
                .await
                .map_err(Error::from))
            }
            effect_kind => Err(InvokerEffectCommand {
                invocation_id: self.invocation_id,
                in_flight_invocation_metadata: self.in_flight_invocation_metadata,
                effect_kind,
            }),
        }
    }
}
