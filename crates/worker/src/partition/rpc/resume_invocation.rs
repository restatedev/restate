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
use crate::partition::state_machine::resolve_pinned_deployment;
use restate_storage_api::invocation_status_table::{InvocationStatus, ReadInvocationStatusTable};
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

impl<'a, TSchemas, TStorage> RpcHandler<Request> for RpcContext<'a, TSchemas, TStorage>
where
    // Needed for the non-VQueue path, which resolves the deployment here (see `handle`).
    TSchemas: DeploymentResolver,
    TStorage: ReadInvocationStatusTable,
{
    async fn handle(
        self,
        Request {
            request_id,
            invocation_id,
            update_deployment_id,
        }: Request,
    ) -> Decision {
        // Reading from a non-leader partition processor can return stale results
        // (e.g. NotFound for an invocation that exists on the leader) because the
        // follower's local store may not have replayed all log entries yet.
        if !self.is_leader {
            return Decision::Reply(Err(PartitionProcessorRpcError::NotLeader(
                self.partition_id,
            )));
        }

        // -- Figure out the invocation status
        match self.storage.get_invocation_status(&invocation_id).await {
            Ok(InvocationStatus::Invoked(metadata)) => {
                if metadata.vqueue_id.is_some() {
                    // VQueue-owned: the invoker no longer solely drives the lifecycle. Propose the
                    // persisted command, forwarding the *unresolved* deployment patch -- it is
                    // resolved and validated (and the entry rescheduled) in the apply path
                    // (`OnManualResumeCommand`), which classifies the possibly-changed status. There
                    // is no running attempt to fence, so use the plain proposal path -- unlike
                    // pause's `propose_pause_and_fence`. Writing the new `update_deployment_id` field
                    // is safe here: vqueues being enabled implies a cluster min version >= 1.7.0.
                    Decision::Propose(RpcProposal {
                        partition_key: invocation_id.partition_key(),
                        cmd: Command::ResumeInvocation(ResumeInvocationRequest {
                            invocation_id,
                            update_deployment_id: Some(update_deployment_id),
                            update_pinned_deployment_id: None,
                            run_at: None,
                            response_sink: Some(InvocationMutationResponseSink::Ingress(
                                IngressInvocationResponseSink { request_id },
                            )),
                        }),
                        reply_on: ReplyOn::Apply { request_id },
                    })
                } else {
                    // Legacy invoker-owned path: there is a live attempt, so the pinned deployment
                    // cannot be patched. Poke the invoker to retry now, if possible.
                    if !matches!(update_deployment_id, PatchDeploymentId::KeepPinned) {
                        return Decision::Reply(Ok(
                            ResumeInvocationRpcResponse::CannotPatchDeploymentId.into(),
                        ));
                    }
                    Decision::NotifyInvokerAndReply {
                        notification: InvokerNotification::RetryNow(invocation_id),
                        reply: ResumeInvocationRpcResponse::Ok.into(),
                    }
                }
            }
            Ok(InvocationStatus::Suspended { metadata, .. })
            | Ok(InvocationStatus::Paused(metadata)) => {
                if metadata.vqueue_id.is_some() {
                    // VQueue path: forward the unresolved patch; the apply path resolves it against
                    // the status as of the command's log position. Safe to write the new
                    // `update_deployment_id` field -- vqueues imply a cluster min version >= 1.7.0.
                    return Decision::Propose(RpcProposal {
                        partition_key: invocation_id.partition_key(),
                        cmd: Command::ResumeInvocation(ResumeInvocationRequest {
                            invocation_id,
                            update_deployment_id: Some(update_deployment_id),
                            update_pinned_deployment_id: None,
                            run_at: None,
                            response_sink: Some(InvocationMutationResponseSink::Ingress(
                                IngressInvocationResponseSink { request_id },
                            )),
                        }),
                        reply_on: ReplyOn::Apply { request_id },
                    });
                }

                // Non-VQueue path: a pre-1.7.0 node may still apply this command and does not know
                // `update_deployment_id`, so we resolve here -- reusing the apply path's resolver --
                // and propose the already-resolved `update_pinned_deployment_id` (the field older
                // nodes understand).
                let update_pinned_deployment_id = match resolve_pinned_deployment(
                    Some(&update_deployment_id),
                    None,
                    &metadata,
                    Some(self.schemas),
                ) {
                    Ok(resolved) => resolved,
                    Err(response) => {
                        return Decision::Reply(Ok(
                            ResumeInvocationRpcResponse::from(response).into()
                        ));
                    }
                };

                Decision::Propose(RpcProposal {
                    partition_key: invocation_id.partition_key(),
                    cmd: Command::ResumeInvocation(ResumeInvocationRequest {
                        invocation_id,
                        update_deployment_id: None,
                        update_pinned_deployment_id,
                        run_at: None,
                        response_sink: Some(InvocationMutationResponseSink::Ingress(
                            IngressInvocationResponseSink { request_id },
                        )),
                    }),
                    reply_on: ReplyOn::Apply { request_id },
                })
            }
            Ok(InvocationStatus::Scheduled(_)) | Ok(InvocationStatus::Inboxed(_)) => {
                Decision::Reply(Ok(ResumeInvocationRpcResponse::NotStarted.into()))
            }
            Ok(InvocationStatus::Completed(_)) => {
                Decision::Reply(Ok(ResumeInvocationRpcResponse::Completed.into()))
            }
            Ok(InvocationStatus::Free) => {
                Decision::Reply(Ok(ResumeInvocationRpcResponse::NotFound.into()))
            }
            Err(storage_error) => Decision::Reply(Err(PartitionProcessorRpcError::Internal(
                storage_error.to_string(),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::future::ready;

    use assert2::let_assert;
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InFlightInvocationMetadata, InboxedInvocation,
        PreFlightInvocationMetadata, ScheduledInvocation,
    };
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::DeploymentId;
    use restate_types::invocation::InvocationTarget;
    use restate_types::journal_v2::UnresolvedFuture;
    use restate_types::schema::deployment::Deployment;
    use restate_types::schema::deployment::test_util::MockDeploymentMetadataRegistry;
    use restate_types::service_protocol::ServiceProtocolVersion;
    use rstest::rstest;
    use test_log::test;

    use super::*;

    struct MockStorage {
        expected_invocation_id: InvocationId,
        status: InvocationStatus,
    }

    impl ReadInvocationStatusTable for MockStorage {
        fn get_invocation_status(
            &mut self,
            inv_id: &InvocationId,
        ) -> impl Future<Output = restate_storage_api::Result<InvocationStatus>> + Send {
            assert_eq!(*inv_id, self.expected_invocation_id);
            ready(Ok(self.status.clone()))
        }

        fn any_non_completed_invocation_in_range(
            &mut self,
            _: restate_types::sharding::KeyRange,
        ) -> impl Future<Output = restate_storage_api::Result<bool>> + Send {
            ready(Ok(false))
        }
    }

    async fn handle<R: DeploymentResolver>(
        is_leader: bool,
        schemas: &R,
        storage: &mut MockStorage,
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
        update_deployment_id: PatchDeploymentId,
    ) -> Decision {
        RpcHandler::handle(
            RpcContext::new(is_leader, PartitionId::MIN, schemas, storage),
            Request {
                request_id,
                invocation_id,
                update_deployment_id,
            },
        )
        .await
    }

    #[test(restate_core::test)]
    async fn reply_ok_when_invoked() {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
        };

        let decision = handle(
            true,
            &(),
            &mut storage,
            Default::default(),
            invocation_id,
            Default::default(),
        )
        .await;
        assert_matches!(
            decision,
            Decision::NotifyInvokerAndReply {
                notification: InvokerNotification::RetryNow(actual_invocation_id),
                reply: PartitionProcessorRpcResponse::ResumeInvocation(
                    ResumeInvocationRpcResponse::Ok
                ),
            } if actual_invocation_id == invocation_id
        );
    }

    /// A VQueue Invoked invocation is resumed by proposing the persisted ResumeInvocation command
    /// (so the apply path can pull a backing-off attempt forward), not by poking the invoker.
    #[test(restate_core::test)]
    async fn vqueue_invoked_proposes_resume_command() {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: InvocationStatus::Invoked(InFlightInvocationMetadata::mock_with_vqueue(
                invocation_id.partition_key(),
            )),
        };

        let decision = handle(
            true,
            &(),
            &mut storage,
            Default::default(),
            invocation_id,
            Default::default(),
        )
        .await;
        let_assert!(
            Decision::Propose(RpcProposal {
                cmd: Command::ResumeInvocation(request),
                reply_on: ReplyOn::Apply { .. },
                ..
            }) = decision
        );
        assert_eq!(request.invocation_id, invocation_id);
    }

    #[rstest]
    #[restate_core::test]
    async fn propose_resume_command_on_paused_and_suspended(
        #[values(
            InvocationStatus::Suspended {
                metadata: InFlightInvocationMetadata::mock(),
                awaiting_on: UnresolvedFuture::empty(),
                },
            InvocationStatus::Paused(InFlightInvocationMetadata::mock())
        )]
        status: InvocationStatus,
    ) {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status,
        };

        let request_id = PartitionProcessorRpcRequestId::new();
        let decision = handle(
            true,
            &(),
            &mut storage,
            request_id,
            invocation_id,
            Default::default(),
        )
        .await;
        let_assert!(
            Decision::Propose(RpcProposal {
                cmd: Command::ResumeInvocation(resume_invocation_request),
                reply_on: ReplyOn::Apply {
                    request_id: actual_request_id,
                },
                ..
            }) = decision
        );
        assert_eq!(actual_request_id, request_id);
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
    }

    #[test(restate_core::test)]
    async fn reply_not_found_for_unknown_invocation() {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: Default::default(),
        };

        let decision = handle(
            true,
            &(),
            &mut storage,
            Default::default(),
            invocation_id,
            Default::default(),
        )
        .await;
        assert_matches!(
            decision,
            Decision::Reply(Ok(response))
                if response == PartitionProcessorRpcResponse::from(
                    ResumeInvocationRpcResponse::NotFound
                )
        );
    }

    #[test(restate_core::test)]
    async fn reply_completed() {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: InvocationStatus::Completed(CompletedInvocation::mock_neo()),
        };

        let decision = handle(
            true,
            &(),
            &mut storage,
            Default::default(),
            invocation_id,
            Default::default(),
        )
        .await;
        assert_matches!(
            decision,
            Decision::Reply(Ok(response))
                if response == PartitionProcessorRpcResponse::from(
                    ResumeInvocationRpcResponse::Completed
                )
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

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status,
        };

        let decision = handle(
            true,
            &(),
            &mut storage,
            Default::default(),
            invocation_id,
            Default::default(),
        )
        .await;
        assert_matches!(
            decision,
            Decision::Reply(Ok(response))
                if response == PartitionProcessorRpcResponse::from(
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
        dep.supported_protocol_versions = 1..=4;
        let expected_deployment_id = dep.id;

        let mut schemas = MockDeploymentMetadataRegistry::default();
        schemas.mock_deployment(dep.clone());
        if !pin_to_specific {
            schemas.mock_latest_service(invocation_target.service_name(), dep.id);
        }

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
                    awaiting_on: UnresolvedFuture::empty(),
                }
            } else {
                InvocationStatus::Paused(metadata)
            },
        };

        let update_deployment_id = if pin_to_specific {
            PatchDeploymentId::PinTo {
                id: expected_deployment_id,
            }
        } else {
            PatchDeploymentId::PinToLatest
        };
        let decision = handle(
            true,
            &schemas,
            &mut storage,
            Default::default(),
            invocation_id,
            update_deployment_id,
        )
        .await;

        // Non-VQueue path resolves the deployment in the RPC and rejects the incompatible pin
        // synchronously (no command proposed).
        assert_eq!(
            match decision {
                Decision::Reply(Ok(response)) => response,
                _ => panic!("expected an immediate successful reply"),
            },
            PartitionProcessorRpcResponse::ResumeInvocation(
                ResumeInvocationRpcResponse::IncompatibleDeploymentId {
                    pinned_protocol_version: i32::from(pinned_version),
                    deployment_id: expected_deployment_id,
                    supported_protocol_versions: 1..=4,
                }
            )
        );
    }

    /// A legacy invoker-owned (non-VQueue) Invoked invocation has a live attempt, so its pinned
    /// deployment cannot be patched: the RPC rejects synchronously without poking the invoker.
    #[test(restate_core::test)]
    async fn legacy_invoked_rejects_deployment_patch() {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
        };

        let decision = handle(
            true,
            &(),
            &mut storage,
            Default::default(),
            invocation_id,
            PatchDeploymentId::PinToLatest,
        )
        .await;
        assert_matches!(
            decision,
            Decision::Reply(Ok(response))
                if response == PartitionProcessorRpcResponse::from(
                    ResumeInvocationRpcResponse::CannotPatchDeploymentId
                )
        );
    }

    #[test(restate_core::test)]
    async fn reply_not_leader_when_not_leader() {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            expected_invocation_id: invocation_id,
            status: Default::default(),
        };

        let decision = handle(
            false,
            &(),
            &mut storage,
            Default::default(),
            invocation_id,
            Default::default(),
        )
        .await;

        assert_matches!(
            decision,
            Decision::Reply(Err(PartitionProcessorRpcError::NotLeader(_)))
        );
    }
}
