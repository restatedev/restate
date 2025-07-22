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
use restate_types::identifiers::WithPartitionKey;
use restate_types::invocation;
use restate_types::invocation::{
    ServiceInvocation, ServiceInvocationResponseSink, SubmitNotificationSink,
};

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_request: Arc<InvocationRequest>,
    pub(super) append_invocation_reply_on: AppendInvocationReplyOn,
}

impl<'a, Proposer: CommandProposer, Storage> RpcHandler<Request>
    for RpcContext<'a, Proposer, Storage>
{
    type Output = PartitionProcessorRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request {
            request_id,
            invocation_request,
            append_invocation_reply_on,
        }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        let mut service_invocation = ServiceInvocation::from_request(
            Arc::unwrap_or_clone(invocation_request),
            invocation::Source::ingress(request_id),
        );

        match append_invocation_reply_on {
            AppendInvocationReplyOn::Appended => {
                self.proposer
                    .self_propose_and_respond_asynchronously(
                        service_invocation.partition_key(),
                        Command::Invoke(Box::new(service_invocation)),
                        replier,
                    )
                    .await;
                return Ok(());
            }
            AppendInvocationReplyOn::Submitted => {
                service_invocation.submit_notification_sink =
                    Some(SubmitNotificationSink::Ingress { request_id });
            }
            AppendInvocationReplyOn::Output => {
                service_invocation.response_sink =
                    Some(ServiceInvocationResponseSink::Ingress { request_id });
            }
        };

        self.proposer
            .handle_rpc_proposal_command(
                service_invocation.partition_key(),
                Command::Invoke(Box::new(service_invocation)),
                request_id,
                replier,
            )
            .await;

        Ok(())
    }
}
