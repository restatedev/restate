// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationMutationResponseSink, InvocationTermination,
    TerminationFlavor,
};
use restate_types::net::partition_processor::{
    CancelInvocationRpcRequest, CancelInvocationRpcResponse,
};
use restate_wal_protocol::v2::commands;

use super::*;

impl<'a, TSchemas, TStorage> RpcHandler<CancelInvocationRpcRequest>
    for RpcContext<'a, TSchemas, TStorage>
{
    async fn handle(
        self,
        CancelInvocationRpcRequest {
            header,
            invocation_id,
        }: CancelInvocationRpcRequest,
    ) -> Decision<CancelInvocationRpcResponse> {
        let request_id = header.request_id;
        Decision::Propose(RpcProposal::new(
            commands::TerminateInvocationCommand::from(InvocationTermination {
                invocation_id,
                flavor: TerminationFlavor::Cancel,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink { request_id },
                )),
            }),
            ReplyOn::Apply { request_id },
        ))
    }
}
