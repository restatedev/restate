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
use restate_storage_api::StorageError;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, CompletionStatus, InvocationStatus, ReadInvocationStatusTable,
    ResponseResultRef,
};
use restate_types::invocation::{ResponseResult, client};
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcResponse,
};
use tracing::warn;

pub(super) struct Request {
    pub(super) invocation_id: InvocationId,
}

impl<'a, TSchemas, Storage> RpcHandler<Request> for RpcContext<'a, TSchemas, Storage>
where
    Storage: ReadInvocationStatusTable + ReadOutputTable,
{
    async fn handle(self, Request { invocation_id }: Request) -> Decision {
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
) -> Result<PartitionProcessorRpcResponse, StorageError>
where
    S: ReadInvocationStatusTable + ReadOutputTable,
{
    let invocation_status = storage.get_invocation_status(invocation_id).await?;

    let response = match invocation_status {
        InvocationStatus::Scheduled(_) => {
            PartitionProcessorRpcResponse::Status(client::InvocationStatus {
                state: client::InvocationState::Scheduled,
                error: None,
            })
        }
        InvocationStatus::Inboxed(_) => {
            PartitionProcessorRpcResponse::Status(client::InvocationStatus {
                state: client::InvocationState::Inboxed,
                error: None,
            })
        }
        InvocationStatus::Invoked(_) => {
            PartitionProcessorRpcResponse::Status(client::InvocationStatus {
                state: client::InvocationState::Invoked,
                error: None,
            })
        }
        InvocationStatus::Suspended { .. } => {
            PartitionProcessorRpcResponse::Status(client::InvocationStatus {
                state: client::InvocationState::Suspended,
                error: None,
            })
        }
        InvocationStatus::Paused(_) => {
            PartitionProcessorRpcResponse::Status(client::InvocationStatus {
                state: client::InvocationState::Paused,
                error: None,
            })
        }
        InvocationStatus::Completed(CompletedInvocation {
            response_result, ..
        }) => match response_result {
            ResponseResultRef::Success(_)
            | ResponseResultRef::Completed(CompletionStatus::Success) => {
                PartitionProcessorRpcResponse::Status(client::InvocationStatus {
                    state: client::InvocationState::Succeeded,
                    error: None,
                })
            }
            ResponseResultRef::Failure(error) => {
                PartitionProcessorRpcResponse::Status(client::InvocationStatus {
                    state: client::InvocationState::Failed,
                    error: Some(error),
                })
            }
            ResponseResultRef::Killed
            | ResponseResultRef::Completed(CompletionStatus::Failure(_)) => {
                // todo: Do we need the full error, or is the error code is enough.
                // If the error code is enough then this should be way more efficient
                // since we have this already.
                // Currently we have to get the full failure output to extract the full
                // error.
                match storage.get_output(invocation_id).await? {
                    None => PartitionProcessorRpcResponse::NotFound,
                    Some(result) => {
                        let ResponseResult::Failure(error) = result else {
                            warn!(invocation_id=%invocation_id, "invocation response result inconsistency");
                            return Err(StorageError::DataIntegrityError);
                        };

                        PartitionProcessorRpcResponse::Status(client::InvocationStatus {
                            state: client::InvocationState::Failed,
                            error: Some(error),
                        })
                    }
                }
            }
        },
        InvocationStatus::Free => PartitionProcessorRpcResponse::NotFound,
    };

    Ok(response)
}
