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
use restate_types::logs::BodyWithKeys;
use restate_wal_protocol::invocation::PauseInvocationCommand;

pub(super) struct PauseRequest {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_id: InvocationId,
}

impl<'a, TSchemas, TStorage> RpcHandler<PauseRequest> for RpcContext<'a, TSchemas, TStorage> {
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

        // The apply path (OnManualPauseCommand) classifies the (possibly changed) status and
        // replies via Action::ForwardPauseInvocationResponse. propose_pause_and_fence clears
        // the leader's in-memory fencing token (after appending the command) so that any
        // straggler effect from the attempt we are pausing is dropped at write time.
        Decision::Propose(RpcProposal::new(
            BodyWithKeys::new(
                PauseInvocationCommand {
                    invocation_id,
                    request_id: Some(request_id),
                },
                Keys::Single(invocation_id.partition_key()),
            ),
            ReplyOn::ApplyAndFence {
                request_id,
                invocation_id,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use assert2::let_assert;
    use restate_wal_protocol::v2::commands;
    use test_log::test;

    use super::*;

    #[test(restate_core::test)]
    async fn reply_not_leader_when_not_leader() {
        let invocation_id = InvocationId::mock_random();

        struct NoopStorage;
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

    /// A VQueue invocation is paused by proposing the persisted PauseInvocation command.
    #[test(restate_core::test)]
    async fn vqueue_invocation_proposes_pause_command() {
        let invocation_id = InvocationId::mock_random();

        struct NoopStorage;
        let mut storage = NoopStorage;

        let request_id = PartitionProcessorRpcRequestId::new();
        let decision = RpcHandler::handle(
            RpcContext::new(true, PartitionId::MIN, &(), &mut storage),
            PauseRequest {
                request_id,
                invocation_id,
            },
        )
        .await;

        let (keys, pause, reply_on) =
            decision.extract_as_rpc_proposal::<commands::PauseInvocationCommand>();

        let_assert!(
            ReplyOn::ApplyAndFence {
                request_id: actual_request_id,
                invocation_id: actual_invocation_id,
            } = reply_on
        );

        assert!(matches!(keys, Keys::Single(pk) if pk == invocation_id.partition_key()));

        assert_eq!(actual_request_id, request_id);
        assert_eq!(actual_invocation_id, invocation_id);
        assert_eq!(pause.invocation_id, invocation_id);
        assert_eq!(pause.request_id, Some(request_id));
    }
}
