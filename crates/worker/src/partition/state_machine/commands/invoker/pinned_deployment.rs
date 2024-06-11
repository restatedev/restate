use crate::partition::state_machine::commands::invoker::InvokerPinnedDeploymentCommand;
use crate::partition::state_machine::commands::{ApplicableCommand, CommandContext, Error};
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use tracing::trace;

impl<Storage: InvocationStatusTable, Actions, Codec> ApplicableCommand<Storage, Actions, Codec>
    for InvokerPinnedDeploymentCommand
{
    async fn apply(
        mut self,
        ctx: CommandContext<'_, Storage, Actions, Codec>,
    ) -> Result<(), Error> {
        trace!(
            restate.deployment.id = %self.pinned_deployment.deployment_id,
            restate.deployment.service_protocol_version = %self.pinned_deployment.service_protocol_version.as_repr(),
            "Storing chosen deployment to storage"
        );

        self.in_flight_invocation_metadata.pinned_deployment = Some(self.pinned_deployment);
        ctx.transaction
            .put_invocation_status(
                &self.invocation_id,
                InvocationStatus::Invoked(self.in_flight_invocation_metadata),
            )
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::commands::mocks::MockStateMachine;
    use crate::partition::types::{InvokerEffect, InvokerEffectKind};
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, ReadOnlyInvocationStatusTable,
    };
    use restate_storage_api::Transaction;
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::{DeploymentId, InvocationId};
    use restate_types::service_protocol::ServiceProtocolVersion;
    use test_log::test;

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn invoker_pinned_deployment_command() {
        let (_tc, mut state_machine) = MockStateMachine::init().await;

        // Let's put an invocation status
        let mut txn = state_machine.storage().transaction();
        let invocation_id = InvocationId::mock_random();
        txn.put_invocation_status(
            &invocation_id,
            InvocationStatus::Invoked(InFlightInvocationMetadata {
                pinned_deployment: None,
                ..InFlightInvocationMetadata::mock()
            }),
        )
        .await;
        txn.commit().await.unwrap();

        let pinned_deployment =
            PinnedDeployment::new(DeploymentId::new(), ServiceProtocolVersion::V1);
        let actions = state_machine
            .maybe_apply(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::PinnedDeployment(pinned_deployment.clone()),
            })
            .await;

        assert_that!(actions, empty());
        assert_that!(
            state_machine
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(pat!(InvocationStatus::Invoked(pat!(
                InFlightInvocationMetadata {
                    pinned_deployment: some(eq(pinned_deployment))
                }
            ))))
        );
    }
}
