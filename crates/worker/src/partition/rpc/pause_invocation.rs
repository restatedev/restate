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
use restate_types::identifiers::InvocationId;
use restate_types::net::partition_processor::PauseInvocationRpcResponse;
use restate_wal_protocol::invocation::PauseInvocationCommand;

pub(super) struct PauseRequest {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
}

impl<'a, TActuator: Actuator, TSchemas, TStorage> RpcHandler<PauseRequest>
    for RpcContext<'a, TActuator, TSchemas, TStorage>
where
    TStorage: ReadInvocationStatusTable,
{
    type Output = PauseInvocationRpcResponse;
    type Error = ();

    async fn handle(
        self,
        PauseRequest {
            request_id,
            invocation_id,
        }: PauseRequest,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        // Reading from a non-leader partition processor can return stale results
        // (e.g. NotFound for an invocation that exists on the leader) because the
        // follower's local store may not have replayed all log entries yet.
        if !self.proposer.is_leader() {
            replier.send_result(Err(PartitionProcessorRpcError::NotLeader(
                self.proposer.partition_id(),
            )));
            return Ok(());
        }

        let status = match self.storage.get_invocation_status(&invocation_id).await {
            Ok(status) => status,
            Err(storage_error) => {
                replier.send_result(Err(PartitionProcessorRpcError::Internal(
                    storage_error.to_string(),
                )));
                return Ok(());
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
            self.proposer
                .propose_pause_and_fence(
                    PauseInvocationCommand {
                        invocation_id,
                        request_id: Some(request_id),
                    },
                    request_id,
                    replier,
                )
                .await;
            return Ok(());
        }

        // -- Legacy path for invoker-owned (non-VQueue) invocations: best-effort invoker poke.
        match status {
            InvocationStatus::Invoked(_) => {
                self.proposer.notify_invoker_to_pause(invocation_id);
                replier.send(PauseInvocationRpcResponse::Accepted);
            }
            InvocationStatus::Completed(_)
            | InvocationStatus::Scheduled(_)
            | InvocationStatus::Inboxed(_) => {
                replier.send(PauseInvocationRpcResponse::NotRunning);
            }
            InvocationStatus::Paused(_) | InvocationStatus::Suspended { .. } => {
                replier.send(PauseInvocationRpcResponse::AlreadyPaused);
            }
            InvocationStatus::Free => {
                replier.send(PauseInvocationRpcResponse::NotFound);
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::rpc::MockActuator;
    use futures::FutureExt;
    use restate_core::network::Reciprocal;
    use restate_storage_api::invocation_status_table::InFlightInvocationMetadata;
    use restate_types::identifiers::WithPartitionKey;
    use std::future::ready;
    use test_log::test;

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

        let mut proposer = MockActuator::new();
        proposer.expect_is_leader().return_const(false);
        proposer
            .expect_partition_id()
            .return_const(PartitionId::from(0));

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

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            PauseRequest {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert!(matches!(
            rx.recv().await,
            Err(PartitionProcessorRpcError::NotLeader(_))
        ));
    }

    /// A non-VQueue (invoker-owned) invocation uses the legacy invoker-poke path and is not
    /// proposed as a WAL command.
    #[test(restate_core::test)]
    async fn non_vqueue_invocation_uses_legacy_invoker_poke() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer.expect_is_leader().return_const(true);
        proposer
            .expect_notify_invoker_to_pause()
            .return_once_st(move |got_invocation_id| {
                assert_eq!(got_invocation_id, invocation_id);
            });

        let mut storage = MockStorage {
            // mock() has no vqueue_id.
            status: InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            PauseRequest {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::PauseInvocation(PauseInvocationRpcResponse::Accepted)
        );
    }

    /// A VQueue invocation is paused by proposing the persisted PauseInvocation command.
    #[test(restate_core::test)]
    async fn vqueue_invocation_proposes_pause_command() {
        let invocation_id = InvocationId::mock_random();

        let mut proposer = MockActuator::new();
        proposer.expect_is_leader().return_const(true);
        proposer
            .expect_propose_pause_and_fence::<PauseInvocationRpcResponse>()
            .return_once_st(move |pause, request_id, replier| {
                assert_eq!(pause.invocation_id, invocation_id);
                assert_eq!(pause.request_id, Some(request_id));
                replier.send(PauseInvocationRpcResponse::Accepted);
                ready(()).boxed()
            });

        let mut storage = MockStorage {
            status: InvocationStatus::Invoked(InFlightInvocationMetadata::mock_with_vqueue(
                invocation_id.partition_key(),
            )),
        };

        let (tx, rx) = Reciprocal::mock();
        RpcHandler::handle(
            RpcContext::new(&mut proposer, &(), &mut storage),
            PauseRequest {
                request_id: Default::default(),
                invocation_id,
            },
            Replier::new(tx),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.recv().await.unwrap(),
            PartitionProcessorRpcResponse::PauseInvocation(PauseInvocationRpcResponse::Accepted)
        );
    }
}
