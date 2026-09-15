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
use restate_types::identifiers::WithPartitionKey;
use restate_types::state_mut::ExternalStateMutation;

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) mutation: ExternalStateMutation,
}

impl<'a, TSchemas, TStorage> RpcHandler<Request, PatchStateRpcResponse>
    for RpcContext<'a, TSchemas, TStorage>
where
    TStorage: ReadInvocationStatusTable,
{
    async fn handle(
        self,
        Request {
            request_id,
            mut mutation,
        }: Request,
    ) -> Decision<PatchStateRpcResponse> {
        if !self.is_leader {
            return Decision::Reply(Err(PartitionProcessorRpcError::NotLeader(
                self.partition_id,
            )));
        }

        // Attach the request id so that when it's applied, we can route it back to the caller.
        mutation.request_id = Some(request_id);
        Decision::Propose(RpcProposal {
            partition_key: mutation.service_id.partition_key(),
            cmd: Command::PatchState(mutation),
            reply_on: ReplyOn::Apply { request_id },
        })
    }
}
