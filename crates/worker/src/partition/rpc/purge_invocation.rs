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
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationMutationResponseSink, PurgeInvocationRequest,
};
use restate_wal_protocol::v2::commands;

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
        Decision::Propose(RpcProposal::new(
            commands::PurgeInvocationCommand::from(PurgeInvocationRequest {
                invocation_id,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink { request_id },
                )),
            }),
            ReplyOn::Apply { request_id },
        ))
    }
}
