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
use restate_types::invocation;
use restate_types::invocation::{
    ServiceInvocation, ServiceInvocationResponseSink, SubmitNotificationSink,
};

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_request: Arc<InvocationRequest>,
    pub(super) append_invocation_reply_on: AppendInvocationReplyOn,
}

impl<'a, TSchemas, TStorage> RpcHandler<Request> for RpcContext<'a, TSchemas, TStorage> {
    async fn handle(
        self,
        Request {
            request_id,
            invocation_request,
            append_invocation_reply_on,
        }: Request,
    ) -> Decision {
        let mut service_invocation = ServiceInvocation::from_request(
            Arc::unwrap_or_clone(invocation_request),
            invocation::Source::ingress(request_id),
        );

        match append_invocation_reply_on {
            AppendInvocationReplyOn::Appended => {
                // No sinks needed — respond on Bifrost commit
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

        let partition_key = service_invocation.partition_key();
        let cmd = Command::Invoke(Box::new(service_invocation));

        Decision::Propose(RpcProposal {
            partition_key,
            cmd,
            reply_on: match append_invocation_reply_on {
                AppendInvocationReplyOn::Appended => ReplyOn::Commit {
                    response: PartitionProcessorRpcResponse::Appended,
                },
                AppendInvocationReplyOn::Submitted | AppendInvocationReplyOn::Output => {
                    ReplyOn::Apply { request_id }
                }
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use googletest::prelude::*;
    use restate_test_util::let_assert;
    use test_log::test;

    async fn handle(
        request_id: PartitionProcessorRpcRequestId,
        append_invocation_reply_on: AppendInvocationReplyOn,
    ) -> Decision {
        RpcHandler::handle(
            RpcContext::new(true, PartitionId::MIN, &(), &mut ()),
            Request {
                request_id,
                invocation_request: Arc::new(InvocationRequest::mock()),
                append_invocation_reply_on,
            },
        )
        .await
    }

    #[test(restate_core::test)]
    async fn reply_on_appended() {
        let_assert!(
            Decision::Propose(RpcProposal {
                cmd: Command::Invoke(service_invocation),
                reply_on: ReplyOn::Commit { response },
                ..
            }) = handle(Default::default(), AppendInvocationReplyOn::Appended).await
        );
        assert_eq!(response, PartitionProcessorRpcResponse::Appended);
        assert_that!(
            service_invocation,
            points_to(all!(
                field!(ServiceInvocation.response_sink, none()),
                field!(ServiceInvocation.submit_notification_sink, none()),
            ))
        );
    }

    #[test(restate_core::test)]
    async fn reply_on_submitted() {
        let request_id = PartitionProcessorRpcRequestId::new();
        let_assert!(
            Decision::Propose(RpcProposal {
                cmd: Command::Invoke(service_invocation),
                reply_on: ReplyOn::Apply {
                    request_id: actual_request_id,
                },
                ..
            }) = handle(request_id, AppendInvocationReplyOn::Submitted).await
        );
        assert_eq!(actual_request_id, request_id);
        assert_that!(
            service_invocation,
            points_to(all!(
                field!(ServiceInvocation.response_sink, none()),
                field!(
                    ServiceInvocation.submit_notification_sink,
                    some(eq(SubmitNotificationSink::Ingress { request_id }))
                ),
            ))
        );
    }

    #[test(restate_core::test)]
    async fn reply_on_output() {
        let request_id = PartitionProcessorRpcRequestId::new();
        let_assert!(
            Decision::Propose(RpcProposal {
                cmd: Command::Invoke(service_invocation),
                reply_on: ReplyOn::Apply {
                    request_id: actual_request_id,
                },
                ..
            }) = handle(request_id, AppendInvocationReplyOn::Output).await
        );
        assert_eq!(actual_request_id, request_id);
        assert_that!(
            service_invocation,
            points_to(all!(
                field!(
                    ServiceInvocation.response_sink,
                    some(eq(ServiceInvocationResponseSink::Ingress { request_id }))
                ),
                field!(ServiceInvocation.submit_notification_sink, none()),
            ))
        );
    }
}
