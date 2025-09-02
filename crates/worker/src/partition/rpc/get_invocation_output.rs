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
use restate_storage_api::StorageError;
use restate_storage_api::idempotency_table::ReadOnlyIdempotencyTable;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadOnlyInvocationStatusTable,
};
use restate_storage_api::service_status_table::{
    ReadOnlyVirtualObjectStatusTable, VirtualObjectStatus,
};
use restate_types::identifiers::WithPartitionKey;
use restate_types::invocation;
use restate_types::invocation::client::{InvocationOutput, InvocationOutputResponse};
use restate_types::invocation::{
    AttachInvocationRequest, InvocationQuery, ServiceInvocationResponseSink,
};
use restate_types::net::partition_processor::{
    GetInvocationOutputResponseMode, PartitionProcessorRpcError, PartitionProcessorRpcResponse,
};
use restate_wal_protocol::Command;

pub(super) struct Request {
    pub(super) request_id: PartitionProcessorRpcRequestId,
    pub(super) invocation_query: InvocationQuery,
    pub(super) response_mode: GetInvocationOutputResponseMode,
}

impl<'a, TActuator, TSchemas, TStorage> RpcContext<'a, TActuator, TSchemas, TStorage>
where
    TActuator: Actuator,
    TStorage:
        ReadOnlyInvocationStatusTable + ReadOnlyVirtualObjectStatusTable + ReadOnlyIdempotencyTable,
{
    async fn get_invocation_output(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> Result<PartitionProcessorRpcResponse, StorageError> {
        // We can handle this immediately by querying the partition store, no need to go through proposals
        let invocation_id = match invocation_query {
            InvocationQuery::Invocation(iid) => iid,
            ref q @ InvocationQuery::Workflow(ref sid) => {
                // TODO We need this query for backward compatibility, remove when we remove the idempotency table
                match self.storage.get_virtual_object_status(sid).await? {
                    VirtualObjectStatus::Locked(iid) => iid,
                    VirtualObjectStatus::Unlocked => {
                        // Try the deterministic id
                        q.to_invocation_id()
                    }
                }
            }
            ref q @ InvocationQuery::IdempotencyId(ref iid) => {
                // TODO We need this query for backward compatibility, remove when we remove the idempotency table
                match self.storage.get_idempotency_metadata(iid).await? {
                    Some(idempotency_metadata) => idempotency_metadata.invocation_id,
                    None => {
                        // Try the deterministic id
                        q.to_invocation_id()
                    }
                }
            }
        };

        let invocation_status = self.storage.get_invocation_status(&invocation_id).await?;

        match invocation_status {
            InvocationStatus::Free => Ok(PartitionProcessorRpcResponse::NotFound),
            InvocationStatus::Completed(completed) => {
                let completion_expiry_time = completed.completion_expiry_time();
                Ok(PartitionProcessorRpcResponse::Output(InvocationOutput {
                    request_id,
                    response: match completed.response_result.clone() {
                        invocation::ResponseResult::Success(res) => {
                            InvocationOutputResponse::Success(completed.invocation_target, res)
                        }
                        invocation::ResponseResult::Failure(err) => {
                            InvocationOutputResponse::Failure(err)
                        }
                    },
                    invocation_id: Some(invocation_id),
                    completion_expiry_time,
                }))
            }
            _ => Ok(PartitionProcessorRpcResponse::NotReady),
        }
    }
}

impl<'a, Proposer: Actuator, TSchemas, Storage> RpcHandler<Request>
    for RpcContext<'a, Proposer, TSchemas, Storage>
where
    Storage:
        ReadOnlyInvocationStatusTable + ReadOnlyVirtualObjectStatusTable + ReadOnlyIdempotencyTable,
{
    type Output = PartitionProcessorRpcResponse;
    type Error = ();

    async fn handle(
        mut self,
        Request {
            request_id,
            invocation_query,
            response_mode,
        }: Request,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        match response_mode {
            GetInvocationOutputResponseMode::BlockWhenNotReady => {
                // Try to get invocation output now, if it's ready reply immediately with it
                if let Ok(ready_result @ PartitionProcessorRpcResponse::Output(_)) = self
                    .get_invocation_output(request_id, invocation_query.clone())
                    .await
                {
                    replier.send(ready_result);
                    return Ok(());
                }

                self.proposer
                    .handle_rpc_proposal_command(
                        invocation_query.partition_key(),
                        Command::AttachInvocation(AttachInvocationRequest {
                            invocation_query,
                            block_on_inflight: true,
                            response_sink: ServiceInvocationResponseSink::Ingress { request_id },
                        }),
                        request_id,
                        replier,
                    )
                    .await;
            }
            GetInvocationOutputResponseMode::ReplyIfNotReady => {
                replier.send_result(
                    self.get_invocation_output(request_id, invocation_query)
                        .await
                        .map_err(|err| PartitionProcessorRpcError::Internal(err.to_string())),
                );
            }
        }

        Ok(())
    }
}
