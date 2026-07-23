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
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationMutationResponseSink, InvocationTermination,
    TerminationFlavor,
};
use restate_wal_protocol::Command;

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
}

impl<'a, TSchemas, TStorage> RpcHandler<Request> for RpcContext<'a, TSchemas, TStorage> {
    async fn handle(
        self,
        Request {
            request_id,
            invocation_id,
        }: Request,
    ) -> Decision {
        Decision::Propose(RpcProposal {
            partition_key: invocation_id.partition_key(),
            cmd: Command::TerminateInvocation(InvocationTermination {
                invocation_id,
                flavor: TerminationFlavor::Cancel,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink { request_id },
                )),
            }),
            reply_on: ReplyOn::Apply { request_id },
        })
    }
}
