// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_types::identifiers::InvocationId;
use restate_types::net::partition_processor::PauseInvocationRpcResponse;

pub(super) struct Request {
    pub(super) invocation_id: InvocationId,
}

impl<'a, TActuator: Actuator, TSchemas, TStorage> RpcHandler<Request>
    for RpcContext<'a, TActuator, TSchemas, TStorage>
where
    TStorage: ReadInvocationStatusTable,
{
    type Output = PauseInvocationRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request { invocation_id }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        // Reading from a non-leader partition processor can return stale results
        // (e.g. NotFound for an invocation that exists on the leader) because the
        // follower's local store may not have replayed all log entries yet.
        if !self.proposer.is_leader() {
            replier.send_result(Err(PartitionProcessorRpcError::NotLeader(
                self.proposer.partition_id(),
            )));
            return Ok(());
        }

        // -- Figure out the invocation status
        match self.storage.get_invocation_status(&invocation_id).await {
            Ok(InvocationStatus::Invoked(_)) => {
                // Let's poke the invoker to retry now, if possible
                self.proposer.notify_invoker_to_pause(invocation_id);
                replier.send(PauseInvocationRpcResponse::Accepted);
            }
            Ok(
                InvocationStatus::Completed(_)
                | InvocationStatus::Scheduled(_)
                | InvocationStatus::Inboxed(_),
            ) => {
                replier.send(PauseInvocationRpcResponse::NotRunning);
            }
            Ok(InvocationStatus::Paused(_) | InvocationStatus::Suspended { .. }) => {
                replier.send(PauseInvocationRpcResponse::AlreadyPaused);
            }
            Ok(InvocationStatus::Free) => {
                replier.send(PauseInvocationRpcResponse::NotFound);
            }
            Err(storage_error) => {
                replier.send_result(Err(PartitionProcessorRpcError::Internal(
                    storage_error.to_string(),
                )));
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::rpc::MockActuator;
    use restate_core::network::Reciprocal;
    use test_log::test;

    #[test(restate_core::test)]
    async fn reply_not_leader_when_not_leader() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer.expect_is_leader().return_const(false);
        proposer
            .expect_partition_id()
            .return_const(PartitionId::from(0));

        struct NoopStorage;
        impl ReadInvocationStatusTable for NoopStorage {
            #[allow(unreachable_code)]
            fn get_invocation_status(
                &mut self,
                _: &InvocationId,
            ) -> impl Future<Output = restate_storage_api::Result<InvocationStatus>> + Send
            {
                panic!("storage should not be accessed on non-leader");
                std::future::ready(Ok(InvocationStatus::Free))
            }
        }

        let mut storage = NoopStorage;

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            Request { invocation_id },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert!(matches!(
            rx.recv().await,
            Err(PartitionProcessorRpcError::NotLeader(_))
        ));
    }
}
