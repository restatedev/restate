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

impl<'a, TActuator: Actuator, Schemas, TStorage> RpcHandler<Request>
    for RpcContext<'a, TActuator, Schemas, TStorage>
where
    TActuator: Actuator,
    TStorage: ReadInvocationStatusTable,
{
    type Output = PauseInvocationRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request { invocation_id }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
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
