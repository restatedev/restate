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
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_types::identifiers::{InvocationId, WithPartitionKey};
use restate_types::net::partition_processor::PauseInvocationRpcResponse;
use restate_wal_protocol::invocation::PauseInvocationCommand;

pub(super) struct PauseRequest {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
}

impl<'a, TSchemas, TStorage> RpcHandler<PauseRequest> for RpcContext<'a, TSchemas, TStorage>
where
    TStorage: ReadInvocationStatusTable,
{
    async fn handle(
        self,
        PauseRequest {
            request_id,
            invocation_id,
        }: PauseRequest,
    ) -> Decision {
        // Reading from a non-leader partition processor can return stale results
        // (e.g. NotFound for an invocation that exists on the leader) because the
        // follower's local store may not have replayed all log entries yet.
        if !self.is_leader {
            return Decision::Reply(Err(PartitionProcessorRpcError::NotLeader(
                self.partition_id,
            )));
        }

        let status = match self.storage.get_invocation_status(&invocation_id).await {
            Ok(status) => status,
            Err(storage_error) => {
                return Decision::Reply(Err(PartitionProcessorRpcError::Internal(
                    storage_error.to_string(),
                )));
            }
        };

        // The persisted PauseInvocation WAL command (kind 25) is only emitted for invocations that
        // are on VQueues: a `vqueue_id` implies the partition enabled VQueues (gated by a version
        // barrier), so every replica is new enough to decode the command. It is also exactly where
        // the persisted pause is needed -- VQueues lets the partition processor drive the lifecycle,
        // so the invoker no longer solely owns the invocation. Non-VQueue invocations remain
        // invoker-owned, so the legacy best-effort invoker-poke path below is correct for them.
        let on_vqueues = status
            .get_invocation_metadata()
            .is_some_and(|metadata| metadata.vqueue_id.is_some());

        if on_vqueues {
            // The apply path (OnManualPauseCommand) classifies the (possibly changed) status and
            // replies via Action::ForwardPauseInvocationResponse. propose_pause_and_fence clears
            // the leader's in-memory fencing token (after appending the command) so that any
            // straggler effect from the attempt we are pausing is dropped at write time.
            return Decision::Propose(RpcProposal {
                partition_key: invocation_id.partition_key(),
                cmd: Command::PauseInvocation(
                    PauseInvocationCommand {
                        invocation_id,
                        request_id: Some(request_id),
                    }
                    .bilrost_encode_to_bytes(),
                ),
                reply_on: ReplyOn::ApplyAndFence {
                    request_id,
                    invocation_id,
                },
            });
        }

        // -- Legacy path for invoker-owned (non-VQueue) invocations: best-effort invoker poke.
        match status {
            InvocationStatus::Invoked(_) => Decision::NotifyInvokerAndReply {
                notification: InvokerNotification::Pause(invocation_id),
                reply: PauseInvocationRpcResponse::Accepted.into(),
            },
            InvocationStatus::Completed(_)
            | InvocationStatus::Scheduled(_)
            | InvocationStatus::Inboxed(_) => {
                Decision::Reply(Ok(PauseInvocationRpcResponse::NotRunning.into()))
            }
            InvocationStatus::Paused(_) | InvocationStatus::Suspended { .. } => {
                Decision::Reply(Ok(PauseInvocationRpcResponse::AlreadyPaused.into()))
            }
            InvocationStatus::Free => {
                Decision::Reply(Ok(PauseInvocationRpcResponse::NotFound.into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::future::ready;

    use restate_storage_api::invocation_status_table::InFlightInvocationMetadata;
    use test_log::test;

    use super::*;

    struct MockStorage {
        status: InvocationStatus,
    }

    impl ReadInvocationStatusTable for MockStorage {
        fn get_invocation_status(
            &mut self,
            _: &InvocationId,
        ) -> impl Future<Output = restate_storage_api::Result<InvocationStatus>> + Send {
            ready(Ok(self.status.clone()))
        }

        fn any_non_completed_invocation_in_range(
            &mut self,
            _: restate_types::sharding::KeyRange,
        ) -> impl Future<Output = restate_storage_api::Result<bool>> + Send {
            ready(Ok(false))
        }
    }

    #[test(restate_core::test)]
    async fn reply_not_leader_when_not_leader() {
        let invocation_id = InvocationId::mock_random();

        struct NoopStorage;
        impl ReadInvocationStatusTable for NoopStorage {
            #[allow(unreachable_code)]
            fn get_invocation_status(
                &mut self,
                _: &InvocationId,
            ) -> impl Future<Output = restate_storage_api::Result<InvocationStatus>> + Send
            {
                panic!("storage should not be accessed on non-leader");
                std::future::ready(Ok(InvocationStatus::Free))
            }

            #[allow(unreachable_code)]
            fn any_non_completed_invocation_in_range(
                &mut self,
                _: restate_types::sharding::KeyRange,
            ) -> impl Future<Output = restate_storage_api::Result<bool>> + Send {
                panic!("storage should not be accessed on non-leader");
                std::future::ready(Ok(false))
            }
        }

        let mut storage = NoopStorage;

        let decision = RpcHandler::handle(
            RpcContext::new(false, PartitionId::from(0), &(), &mut storage),
            PauseRequest {
                request_id: Default::default(),
                invocation_id,
            },
        )
        .await;

        assert_matches!(
            decision,
            Decision::Reply(Err(PartitionProcessorRpcError::NotLeader(_)))
        );
    }

    /// A non-VQueue (invoker-owned) invocation uses the legacy invoker-poke path and is not
    /// proposed as a WAL command.
    #[test(restate_core::test)]
    async fn non_vqueue_invocation_uses_legacy_invoker_poke() {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            // mock() has no vqueue_id.
            status: InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
        };

        let decision = RpcHandler::handle(
            RpcContext::new(true, PartitionId::MIN, &(), &mut storage),
            PauseRequest {
                request_id: Default::default(),
                invocation_id,
            },
        )
        .await;

        assert_matches!(
            decision,
            Decision::NotifyInvokerAndReply {
                notification: InvokerNotification::Pause(actual_invocation_id),
                reply: PartitionProcessorRpcResponse::PauseInvocation(
                    PauseInvocationRpcResponse::Accepted
                ),
            } if actual_invocation_id == invocation_id
        );
    }

    /// A VQueue invocation is paused by proposing the persisted PauseInvocation command.
    #[test(restate_core::test)]
    async fn vqueue_invocation_proposes_pause_command() {
        let invocation_id = InvocationId::mock_random();

        let mut storage = MockStorage {
            status: InvocationStatus::Invoked(InFlightInvocationMetadata::mock_with_vqueue(
                invocation_id.partition_key(),
            )),
        };

        let request_id = PartitionProcessorRpcRequestId::new();
        let decision = RpcHandler::handle(
            RpcContext::new(true, PartitionId::MIN, &(), &mut storage),
            PauseRequest {
                request_id,
                invocation_id,
            },
        )
        .await;

        let Decision::Propose(RpcProposal {
            partition_key,
            cmd: Command::PauseInvocation(bytes),
            reply_on:
                ReplyOn::ApplyAndFence {
                    request_id: actual_request_id,
                    invocation_id: actual_invocation_id,
                },
        }) = decision
        else {
            panic!("expected a fenced pause proposal");
        };
        assert_eq!(partition_key, invocation_id.partition_key());
        assert_eq!(actual_request_id, request_id);
        assert_eq!(actual_invocation_id, invocation_id);
        let pause = PauseInvocationCommand::bilrost_decode(bytes).unwrap();
        assert_eq!(pause.invocation_id, invocation_id);
        assert_eq!(pause.request_id, Some(request_id));
    }
}
