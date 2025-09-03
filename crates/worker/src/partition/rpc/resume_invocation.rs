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
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationMutationResponseSink, ResumeInvocationRequest,
};
use restate_types::net::partition_processor::ResumeInvocationRpcResponse;

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
}

impl<'a, TActuator: Actuator, TStorage> RpcHandler<Request> for RpcContext<'a, TActuator, TStorage>
where
    TActuator: Actuator,
    TStorage: ReadOnlyInvocationStatusTable,
{
    type Output = ResumeInvocationRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request {
            request_id,
            invocation_id,
        }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        // -- Figure out the invocation status
        match self.storage.get_invocation_status(&invocation_id).await {
            Ok(InvocationStatus::Invoked(metadata)) => {
                // Let's poke the invoker to retry now, if possible
                self.proposer
                    .notify_invoker_to_retry_now(invocation_id, metadata.current_invocation_epoch)
                    .await;
                replier.send(ResumeInvocationRpcResponse::Ok);
            }
            Ok(InvocationStatus::Suspended { .. }) | Ok(InvocationStatus::Paused(_)) => {
                // We need to propose the message, PP will deal with invoking this back
                self.proposer
                    .handle_rpc_proposal_command(
                        invocation_id.partition_key(),
                        Command::ResumeInvocation(ResumeInvocationRequest {
                            invocation_id,
                            response_sink: Some(InvocationMutationResponseSink::Ingress(
                                IngressInvocationResponseSink { request_id },
                            )),
                        }),
                        request_id,
                        replier,
                    )
                    .await;
            }
            Ok(InvocationStatus::Scheduled(_)) | Ok(InvocationStatus::Inboxed(_)) => {
                replier.send(ResumeInvocationRpcResponse::NotStarted);
            }
            Ok(InvocationStatus::Completed(_)) => {
                replier.send(ResumeInvocationRpcResponse::Completed);
            }
            Ok(InvocationStatus::Free) => {
                replier.send(ResumeInvocationRpcResponse::NotFound);
            }
            Err(storage_error) => {
                replier.send_result(Err(PartitionProcessorRpcError::Internal(
                    storage_error.to_string(),
                )));
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::rpc::MockActuator;
    use assert2::let_assert;
    use futures::FutureExt;
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation,
        PreFlightInvocationMetadata, ScheduledInvocation,
    };
    use rstest::rstest;
    use std::collections::HashSet;
    use std::future::ready;
    use test_log::test;

    struct MockStorage {
        expected_invocation_id: InvocationId,
        status: InvocationStatus,
    }

    impl ReadOnlyInvocationStatusTable for MockStorage {
        fn get_invocation_status(
            &mut self,
            inv_id: &InvocationId,
        ) -> impl Future<Output = restate_storage_api::Result<InvocationStatus>> + Send {
            assert_eq!(*inv_id, self.expected_invocation_id);
            ready(Ok(self.status.clone()))
        }
    }

    #[test(restate_core::test)]
    async fn reply_ok_when_invoked() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer
            .expect_notify_invoker_to_retry_now()
            .return_once_st(move |got_invocation_id, _| {
                assert_eq!(got_invocation_id, invocation_id);
                ready(()).boxed()
            });

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::ResumeInvocation(ResumeInvocationRpcResponse::Ok)
        );
    }

    #[rstest]
    #[restate_core::test]
    async fn propose_resume_command_on_paused_and_suspended(
        #[values(
            InvocationStatus::Suspended {
                metadata: InFlightInvocationMetadata::mock(),
                waiting_for_notifications: HashSet::new(),
                },
            InvocationStatus::Paused(InFlightInvocationMetadata::mock())
        )]
        status: InvocationStatus,
    ) {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<ResumeInvocationRpcResponse>()
            .return_once_st(move |_, cmd, request_id, replier| {
                let_assert!(Command::ResumeInvocation(resume_invocation_request) = cmd);
                assert_that!(
                    resume_invocation_request,
                    all!(
                        field!(ResumeInvocationRequest.invocation_id, eq(invocation_id)),
                        field!(
                            ResumeInvocationRequest.response_sink,
                            some(eq(InvocationMutationResponseSink::Ingress(
                                IngressInvocationResponseSink { request_id }
                            )))
                        ),
                    )
                );
                replier.send(ResumeInvocationRpcResponse::Ok);
                ready(()).boxed()
            });

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status,
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::ResumeInvocation(ResumeInvocationRpcResponse::Ok)
        );
    }

    #[test(restate_core::test)]
    async fn reply_not_found_for_unknown_invocation() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: Default::default(),
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::ResumeInvocation(ResumeInvocationRpcResponse::NotFound)
        );
    }

    #[test(restate_core::test)]
    async fn reply_completed() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<PartitionProcessorRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: InvocationStatus::Completed(CompletedInvocation::mock_neo()),
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::ResumeInvocation(ResumeInvocationRpcResponse::Completed)
        );
    }

    #[rstest]
    #[restate_core::test]
    async fn reply_not_started(
        #[values(
            InvocationStatus::Inboxed(InboxedInvocation {
                inbox_sequence_number: 0,
                metadata: PreFlightInvocationMetadata::mock(),
            }),
            InvocationStatus::Scheduled(ScheduledInvocation {
                metadata: PreFlightInvocationMetadata::mock(),
            })
        )]
        status: InvocationStatus,
    ) {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<ResumeInvocationRpcResponse>()
            .never();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status,
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::ResumeInvocation(
                ResumeInvocationRpcResponse::NotStarted
            )
        );
    }
}
