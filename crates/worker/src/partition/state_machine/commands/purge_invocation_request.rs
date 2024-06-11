use crate::partition::state_machine::actions::ActionCollector;
use crate::partition::state_machine::commands::{ApplicableCommand, CommandContext, Error};
use restate_storage_api::idempotency_table::IdempotencyTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InvocationStatus, InvocationStatusTable,
};
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::service_status_table::{VirtualObjectStatus, VirtualObjectStatusTable};
use restate_storage_api::state_table::StateTable;
use restate_types::identifiers::IdempotencyId;
use restate_types::invocation::{
    InvocationTargetType, PurgeInvocationRequest, WorkflowHandlerType,
};
use restate_types::journal::raw::RawEntryCodec;
use tracing::trace;

impl<Storage, Actions, Codec> ApplicableCommand<Storage, Actions, Codec> for PurgeInvocationRequest
where
    Storage: InvocationStatusTable
        + VirtualObjectStatusTable
        + IdempotencyTable
        + StateTable
        + PromiseTable
        + Send,
    Actions: ActionCollector,
    Codec: RawEntryCodec,
{
    async fn apply(
        self,
        mut ctx: CommandContext<'_, Storage, Actions, Codec>,
    ) -> Result<(), Error> {
        match ctx.get_invocation_status(&self.invocation_id).await? {
            InvocationStatus::Completed(CompletedInvocation {
                invocation_target,
                idempotency_key,
                ..
            }) => {
                trace!("Freeing invocation");
                ctx.transaction
                    .put_invocation_status(&self.invocation_id, InvocationStatus::Free)
                    .await;

                // Also cleanup the associated idempotency key if any
                if let Some(idempotency_key) = idempotency_key {
                    trace!(
                        restate.idempotency.key = ?idempotency_key,
                        "Deleting idempotency key"
                    );
                    ctx.transaction
                        .delete_idempotency_metadata(&IdempotencyId::combine(
                            self.invocation_id,
                            &invocation_target,
                            idempotency_key,
                        ))
                        .await;
                }

                // For workflow, we should also clean up the service lock, associated state and promises.
                if invocation_target.invocation_target_ty()
                    == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                {
                    let service_id = invocation_target
                        .as_keyed_service_id()
                        .expect("Workflow methods must have keyed service id");

                    trace!("Clearing service status, state entries and promises");
                    ctx.transaction
                        .put_virtual_object_status(&service_id, VirtualObjectStatus::Unlocked)
                        .await;
                    ctx.transaction.delete_all_user_state(&service_id).await?;
                    ctx.transaction.delete_all_promises(&service_id).await;
                }
            }
            InvocationStatus::Free => {
                trace!(
                    "Received purge command for unknown invocation with id '{}'.",
                    self.invocation_id
                );
                // Nothing to do
            }
            _ => {
                trace!(
                    "Ignoring purge command as the invocation '{}' is still ongoing.",
                    self.invocation_id
                );
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::commands::mocks::MockStateMachine;
    use bytestring::ByteString;
    use googletest::prelude::*;
    use restate_storage_api::idempotency_table::{IdempotencyMetadata, ReadOnlyIdempotencyTable};
    use restate_storage_api::invocation_status_table::ReadOnlyInvocationStatusTable;
    use restate_storage_api::Transaction;
    use restate_types::identifiers::InvocationId;
    use restate_types::invocation::{InvocationTarget, PurgeInvocationRequest};
    use test_log::test;

    #[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn purge_invocation_idempotent_request() {
        let (_tc, mut state_machine) = MockStateMachine::init().await;

        // Let's put an idempotent invocation
        let mut txn = state_machine.storage().transaction();
        let invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_service();
        let idempotency_key = ByteString::from_static("123");
        let idempotency_id =
            IdempotencyId::combine(invocation_id, &invocation_target, idempotency_key.clone());
        txn.put_invocation_status(
            &invocation_id,
            InvocationStatus::Completed(CompletedInvocation {
                invocation_target: invocation_target.clone(),
                idempotency_key: Some(idempotency_key.clone()),
                ..CompletedInvocation::mock()
            }),
        )
        .await;
        txn.put_idempotency_metadata(&idempotency_id, IdempotencyMetadata { invocation_id })
            .await;
        txn.commit().await.unwrap();

        let actions = state_machine
            .apply(PurgeInvocationRequest { invocation_id })
            .await;

        assert_that!(actions, empty());
        assert_that!(
            state_machine
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(pat!(InvocationStatus::Free))
        );
        assert_that!(
            state_machine
                .storage()
                .get_idempotency_metadata(&idempotency_id)
                .await,
            ok(none())
        )
    }
}
