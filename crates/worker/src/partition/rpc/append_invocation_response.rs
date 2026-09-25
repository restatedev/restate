// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::net::partition_processor::{
    AppendInvocationResponseRpcRequest, AppendInvocationResponseRpcResponse,
};
use restate_wal_protocol::v2::commands;

use super::*;

impl<'a, TSchemas, TStorage> RpcHandler<AppendInvocationResponseRpcRequest>
    for RpcContext<'a, TSchemas, TStorage>
{
    async fn handle(
        self,
        AppendInvocationResponseRpcRequest {
            invocation_response,
            ..
        }: AppendInvocationResponseRpcRequest,
    ) -> Decision<AppendInvocationResponseRpcResponse> {
        Decision::Propose(RpcProposal::new(
            commands::InvocationResponseCommand::from(invocation_response),
            ReplyOn::Commit {
                response: AppendInvocationResponseRpcResponse,
            },
        ))
    }
}
