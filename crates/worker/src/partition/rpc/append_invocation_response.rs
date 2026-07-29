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
use restate_types::invocation::InvocationResponse;
use restate_types::net::partition_processor::PartitionProcessorRpcResponse;
use restate_wal_protocol::v2::commands;

pub(super) struct Request {
    pub(super) invocation_response: InvocationResponse,
}

impl<'a, TSchemas, TStorage> RpcHandler<Request> for RpcContext<'a, TSchemas, TStorage> {
    async fn handle(
        self,
        Request {
            invocation_response,
        }: Request,
    ) -> Decision {
        Decision::Propose(RpcProposal::new(
            commands::InvocationResponseCommand::from(invocation_response),
            ReplyOn::Commit {
                response: PartitionProcessorRpcResponse::Appended,
            },
        ))
    }
}
