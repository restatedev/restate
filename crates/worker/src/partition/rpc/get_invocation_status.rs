// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::warn;

use restate_storage_api::StorageError;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, CompletionStatus, InvocationStatus, ReadInvocationStatusTable,
    ResponseResultRef,
};
use restate_types::invocation::{ResponseResult, client};
use restate_types::net::partition_processor::{
    GetInvocationStatusRpcRequest, GetInvocationStatusRpcResponse, PartitionProcessorRpcError,
};

use super::*;

impl<'a, TSchemas, Storage> RpcHandler<GetInvocationStatusRpcRequest>
    for RpcContext<'a, TSchemas, Storage>
where
    Storage: ReadInvocationStatusTable + ReadInvocationOutputTable,
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
            handle(self.storage, &invocation_id)
                .await
                .map_err(|err| PartitionProcessorRpcError::Internal(err.to_string())),
        )
    }
}

async fn handle<S>(
    storage: &mut S,
    invocation_id: &InvocationId,
) -> Result<GetInvocationStatusRpcResponse, StorageError>
where
    S: ReadInvocationStatusTable + ReadInvocationOutputTable,
{
    let invocation_status = storage.get_invocation_status(invocation_id).await?;

    let response = match invocation_status {
        InvocationStatus::Scheduled(_) => GetInvocationStatusRpcResponse::Status {
            state: client::InvocationState::Scheduled.into(),
            error: None,
        },
        InvocationStatus::Inboxed(_) => GetInvocationStatusRpcResponse::Status {
            state: client::InvocationState::Inboxed.into(),
            error: None,
        },
        InvocationStatus::Invoked(_) => GetInvocationStatusRpcResponse::Status {
            state: client::InvocationState::Invoked.into(),
            error: None,
        },
        InvocationStatus::Suspended { .. } => GetInvocationStatusRpcResponse::Status {
            state: client::InvocationState::Suspended.into(),
            error: None,
        },
        InvocationStatus::Paused(_) => GetInvocationStatusRpcResponse::Status {
            state: client::InvocationState::Paused.into(),
            error: None,
        },
        InvocationStatus::Completed(CompletedInvocation {
            response_result, ..
        }) => match response_result {
            ResponseResultRef::Success(_)
            | ResponseResultRef::Completed(CompletionStatus::Success) => {
                GetInvocationStatusRpcResponse::Status {
                    state: client::InvocationState::Succeeded.into(),
                    error: None,
                }
            }
            ResponseResultRef::Failure(error) => GetInvocationStatusRpcResponse::Status {
                state: client::InvocationState::Failed.into(),
                error: Some(error.into()),
            },
            ResponseResultRef::Killed
            | ResponseResultRef::Completed(CompletionStatus::Failure(_)) => {
                // todo: Do we need the full error, or is the error code is enough.
                // If the error code is enough then this should be way more efficient
                // since we have this already.
                // Currently we have to get the full failure output to extract the full
                // error.
                match storage.get_invocation_output(invocation_id).await? {
                    None => GetInvocationStatusRpcResponse::NotFound,
                    Some(result) => {
                        let ResponseResult::Failure(error) = result else {
                            warn!(invocation_id=%invocation_id, "invocation response result inconsistency");
                            return Err(StorageError::DataIntegrityError);
                        };

                        GetInvocationStatusRpcResponse::Status {
                            state: client::InvocationState::Failed.into(),
                            error: Some(error.into()),
                        }
                    }
                }
            }
        },
        InvocationStatus::Free => GetInvocationStatusRpcResponse::NotFound,
    };

    Ok(response)
}
