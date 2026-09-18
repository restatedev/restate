// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InvocationStatus, ReadInvocationStatusTable,
};
use restate_types::invocation::{ResponseResult, client};
use restate_types::net::partition_processor::{
    GetInvocationStatusRpcRequest, GetInvocationStatusRpcResponse, PartitionProcessorRpcError,
};

use super::*;

impl<'a, TSchemas, Storage> RpcHandler<GetInvocationStatusRpcRequest>
    for RpcContext<'a, TSchemas, Storage>
where
    Storage: ReadInvocationStatusTable,
{
    async fn handle(
        self,
        GetInvocationStatusRpcRequest { invocation_id, .. }: GetInvocationStatusRpcRequest,
    ) -> Decision<GetInvocationStatusRpcResponse> {
        if !self.is_leader {
            return Decision::Reply(Err(PartitionProcessorRpcError::NotLeader(
                self.partition_id,
            )));
        }

        Decision::Reply(
            self.storage
                .get_invocation_status(&invocation_id)
                .await
                .map(|invocation_status| match invocation_status {
                    InvocationStatus::Scheduled(_) => GetInvocationStatusRpcResponse::Status {
                        state: client::InvocationState::Scheduled,
                        error: None,
                    },
                    InvocationStatus::Inboxed(_) => GetInvocationStatusRpcResponse::Status {
                        state: client::InvocationState::Inboxed,
                        error: None,
                    },
                    InvocationStatus::Invoked(_) => GetInvocationStatusRpcResponse::Status {
                        state: client::InvocationState::Invoked,
                        error: None,
                    },
                    InvocationStatus::Suspended { .. } => GetInvocationStatusRpcResponse::Status {
                        state: client::InvocationState::Suspended,
                        error: None,
                    },
                    InvocationStatus::Paused(_) => GetInvocationStatusRpcResponse::Status {
                        state: client::InvocationState::Paused,
                        error: None,
                    },
                    InvocationStatus::Completed(CompletedInvocation {
                        response_result: ResponseResult::Success(_),
                        ..
                    }) => GetInvocationStatusRpcResponse::Status {
                        state: client::InvocationState::Succeeded,
                        error: None,
                    },
                    InvocationStatus::Completed(CompletedInvocation {
                        response_result: ResponseResult::Failure(error),
                        ..
                    }) => GetInvocationStatusRpcResponse::Status {
                        state: client::InvocationState::Failed,
                        error: Some(error.into()),
                    },
                    InvocationStatus::Free => GetInvocationStatusRpcResponse::NotFound,
                })
                .map_err(|err| PartitionProcessorRpcError::Internal(err.to_string())),
        )
    }
}
