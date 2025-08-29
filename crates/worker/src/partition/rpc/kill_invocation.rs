// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationMutationResponseSink, InvocationTermination,
    TerminationFlavor,
};
use restate_types::net::partition_processor::KillInvocationRpcResponse;
use restate_wal_protocol::Command;

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
}

impl<'a, TActuator: Actuator, TStorage> RpcHandler<Request>
    for RpcContext<'a, TActuator, TStorage>
{
    type Output = KillInvocationRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request {
            request_id,
            invocation_id,
        }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        self.proposer
            .handle_rpc_proposal_command(
                invocation_id.partition_key(),
                Command::TerminateInvocation(InvocationTermination {
                    invocation_id,
                    flavor: TerminationFlavor::Kill,
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink { request_id },
                    )),
                }),
                request_id,
                replier,
            )
            .await;

        Ok(())
    }
}
