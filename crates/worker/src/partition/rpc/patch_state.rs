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
use restate_types::net::partition_processor::{PatchStateRpcRequest, PatchStateRpcResponse};
use restate_wal_protocol::v2::commands;

impl<'a, TSchemas, TStorage> RpcHandler<PatchStateRpcRequest, PatchStateRpcResponse>
    for RpcContext<'a, TSchemas, TStorage>
{
    async fn handle(
        self,
        PatchStateRpcRequest {
            request_id,
            mut mutation,
        }: PatchStateRpcRequest,
    ) -> Decision<PatchStateRpcResponse> {
        if !self.is_leader {
            return Decision::Reply(Err(PartitionProcessorRpcError::NotLeader(
                self.partition_id,
            )));
        }

        // Attach the request id so that when it's applied, we can route it back to the caller.
        mutation.request_id = Some(request_id);
        Decision::Propose(RpcProposal::new(
            commands::PatchStateCommand::from(mutation),
            ReplyOn::Apply { request_id },
        ))
    }
}
