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
    InFlightInvocationMetadata, InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::invocation::client::PatchDeploymentId;
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationMutationResponseSink, ResumeInvocationRequest,
};
use restate_types::net::partition_processor::ResumeInvocationRpcResponse;
use restate_types::schema::deployment::DeploymentResolver;

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
    pub(super) update_deployment_id: PatchDeploymentId,
}

impl<'a, TActuator: Actuator, TSchemas, TStorage> RpcHandler<Request>
    for RpcContext<'a, TActuator, TSchemas, TStorage>
where
    TActuator: Actuator,
    TSchemas: DeploymentResolver,
    TStorage: ReadOnlyInvocationStatusTable,
{
    type Output = ResumeInvocationRpcResponse;
    type Error = ();

    async fn handle(
        self,
        Request {
            request_id,
            invocation_id,
            update_deployment_id,
        }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        // -- Figure out the invocation status
        match self.storage.get_invocation_status(&invocation_id).await {
            Ok(InvocationStatus::Invoked(metadata)) => {
                if !matches!(update_deployment_id, PatchDeploymentId::KeepPinned) {
                    replier.send(ResumeInvocationRpcResponse::CannotPatchDeploymentId);
                    return Ok(());
                }

                // Let's poke the invoker to retry now, if possible
                self.proposer
                    .notify_invoker_to_retry_now(invocation_id, metadata.current_invocation_epoch)
                    .await;
                replier.send(ResumeInvocationRpcResponse::Ok);
            }
            Ok(InvocationStatus::Suspended {
                metadata:
                    InFlightInvocationMetadata {
                        invocation_target,
                        pinned_deployment,
                        ..
                    },
                ..
            })
            | Ok(InvocationStatus::Paused(InFlightInvocationMetadata {
                invocation_target,
                pinned_deployment,
                ..
            })) => {
                // Let's look at the deployment id here, and see if we need to do some changes
                let update_pinned_deployment_id = match (update_deployment_id, pinned_deployment) {
                    (PatchDeploymentId::KeepPinned, _) | (PatchDeploymentId::PinToLatest, None) => {
                        // If the request is to keep pinned, no change will be applied.
                        None
                    }
                    (_, None) => {
                        // No deployment is even pinned, return back the error
                        replier.send(ResumeInvocationRpcResponse::CannotPatchDeploymentId);
                        return Ok(());
                    }
                    (resume_invocation_deployment_id, Some(pinned_deployment)) => {
                        let Some(deployment) = (match resume_invocation_deployment_id {
                            PatchDeploymentId::PinToLatest => {
                                self.schemas.resolve_latest_deployment_for_service(
                                    invocation_target.service_name(),
                                )
                            }
                            PatchDeploymentId::PinTo { id } => self.schemas.get_deployment(&id),
                            PatchDeploymentId::KeepPinned => {
                                unreachable!()
                            }
                        }) else {
                            replier.send(ResumeInvocationRpcResponse::DeploymentNotFound);
                            return Ok(());
                        };

                        if !deployment
                            .metadata
                            .supported_protocol_versions
                            .contains(&(pinned_deployment.service_protocol_version as i32))
                        {
                            replier.send(ResumeInvocationRpcResponse::IncompatibleDeploymentId {
                                pinned_protocol_version: pinned_deployment.service_protocol_version
                                    as i32,
                                deployment_id: deployment.id,
                                supported_protocol_versions: deployment
                                    .metadata
                                    .supported_protocol_versions,
                            });
                            return Ok(());
                        }
                        Some(deployment.id)
                    }
                };

                // We need to propose the message, PP will deal with invoking this back
                self.proposer
                    .handle_rpc_proposal_command(
                        invocation_id.partition_key(),
                        Command::ResumeInvocation(ResumeInvocationRequest {
                            invocation_id,
                            update_pinned_deployment_id,
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
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::{DeploymentId, ServiceRevision};
    use restate_types::invocation::InvocationTarget;
    use restate_types::schema::deployment::Deployment;
    use restate_types::schema::deployment::test_util::MockDeploymentMetadataRegistry;
    use restate_types::schema::service::ServiceMetadata;
    use restate_types::service_protocol::ServiceProtocolVersion;
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

    struct NoopDeploymentResolver;

    impl DeploymentResolver for NoopDeploymentResolver {
        fn resolve_latest_deployment_for_service(&self, _: impl AsRef<str>) -> Option<Deployment> {
            todo!()
        }

        fn get_deployment(&self, _: &DeploymentId) -> Option<Deployment> {
            todo!()
        }

        fn get_deployment_and_services(
            &self,
            _: &DeploymentId,
        ) -> Option<(Deployment, Vec<ServiceMetadata>)> {
            todo!()
        }

        fn get_deployments(&self) -> Vec<(Deployment, Vec<(String, ServiceRevision)>)> {
            todo!()
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
            RpcContext::new(&mut proposer, &NoopDeploymentResolver, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                update_deployment_id: Default::default(),
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
            RpcContext::new(&mut proposer, &NoopDeploymentResolver, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                update_deployment_id: Default::default(),
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
            RpcContext::new(&mut proposer, &NoopDeploymentResolver, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                update_deployment_id: Default::default(),
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
            RpcContext::new(&mut proposer, &NoopDeploymentResolver, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                update_deployment_id: Default::default(),
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
            RpcContext::new(&mut proposer, &NoopDeploymentResolver, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                update_deployment_id: Default::default(),
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

    #[rstest]
    #[restate_core::test]
    async fn override_incompatible_pinned_deployment(
        #[values(true, false)] pin_to_specific: bool,
        #[values(true, false)] suspended: bool,
    ) {
        let invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_service();
        let pinned_version = ServiceProtocolVersion::V5;

        // Candidate deployment supports only up to V4 -> incompatible with V5
        let mut dep = Deployment::mock();
        dep.metadata.supported_protocol_versions = 1..=4;
        let expected_deployment_id = dep.id;

        let mut schemas = MockDeploymentMetadataRegistry::default();
        schemas.mock_deployment(dep.clone());
        if !pin_to_specific {
            schemas.mock_latest_service(invocation_target.service_name(), dep.id);
        }

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<ResumeInvocationRpcResponse>()
            .never();

        let metadata = InFlightInvocationMetadata {
            invocation_target,
            pinned_deployment: Some(PinnedDeployment::new(DeploymentId::new(), pinned_version)),
            ..InFlightInvocationMetadata::mock()
        };
        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: if suspended {
                InvocationStatus::Suspended {
                    metadata,
                    waiting_for_notifications: Default::default(),
                }
            } else {
                InvocationStatus::Paused(metadata)
            },
        };

        let (tx, rx) = Reciprocal::mock();
        let update_deployment_id = if pin_to_specific {
            PatchDeploymentId::PinTo {
                id: expected_deployment_id,
            }
        } else {
            PatchDeploymentId::PinToLatest
        };
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &schemas, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                update_deployment_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_that!(
            rx.recv().await.unwrap(),
            eq(PartitionProcessorRpcResponse::ResumeInvocation(
                ResumeInvocationRpcResponse::IncompatibleDeploymentId {
                    pinned_protocol_version: i32::from(pinned_version),
                    deployment_id: expected_deployment_id,
                    supported_protocol_versions: 1..=4,
                }
            ))
        );
    }

    #[rstest]
    #[restate_core::test]
    async fn override_compatible_pinned_deployment_proposes_command(
        #[values(true, false)] pin_to_specific: bool,
        #[values(true, false)] suspended: bool,
    ) {
        let invocation_id = InvocationId::mock_random();
        let invocation_target = InvocationTarget::mock_service();
        let pinned_version = ServiceProtocolVersion::V4;

        // Candidate deployment supports V4 -> Should be compatible
        let mut dep = Deployment::mock();
        dep.metadata.supported_protocol_versions = 1..=4;
        let expected_deployment_id = dep.id;

        let mut schemas = MockDeploymentMetadataRegistry::default();
        schemas.mock_deployment(dep.clone());
        if !pin_to_specific {
            schemas.mock_latest_service(invocation_target.service_name(), dep.id);
        }

        let mut proposer = MockActuator::new();
        proposer
            .expect_handle_rpc_proposal_command::<ResumeInvocationRpcResponse>()
            .return_once_st(move |_, cmd, request_id, replier| {
                assert_that!(
                    cmd,
                    pat!(Command::ResumeInvocation(pat!(ResumeInvocationRequest {
                        invocation_id: eq(invocation_id),
                        update_pinned_deployment_id: some(eq(expected_deployment_id)),
                        response_sink: some(eq(InvocationMutationResponseSink::Ingress(
                            IngressInvocationResponseSink { request_id }
                        )))
                    })))
                );
                replier.send(ResumeInvocationRpcResponse::Ok);
                ready(()).boxed()
            });

        let metadata = InFlightInvocationMetadata {
            invocation_target,
            pinned_deployment: Some(PinnedDeployment::new(DeploymentId::new(), pinned_version)),
            ..InFlightInvocationMetadata::mock()
        };
        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: if suspended {
                InvocationStatus::Suspended {
                    metadata,
                    waiting_for_notifications: Default::default(),
                }
            } else {
                InvocationStatus::Paused(metadata)
            },
        };

        let (tx, rx) = Reciprocal::mock();
        let update_deployment_id = if pin_to_specific {
            PatchDeploymentId::PinTo {
                id: expected_deployment_id,
            }
        } else {
            PatchDeploymentId::PinToLatest
        };
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &schemas, &mut storage),
            Request {
                request_id: Default::default(),
                invocation_id,
                update_deployment_id,
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
}
