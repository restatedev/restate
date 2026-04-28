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
use restate_storage_api::invocation_status_table::{InvocationStatus, ReadInvocationStatusTable};
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
    TStorage: ReadInvocationStatusTable,
{
    async fn get_invocation_output(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        invocation_query: InvocationQuery,
    ) -> Result<PartitionProcessorRpcResponse, StorageError> {
        // We can handle this immediately by querying the partition store, no need to go through proposals
        let invocation_id = invocation_query.to_invocation_id();
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
    Storage: ReadInvocationStatusTable,
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
                // Reading invocation output from a non-leader partition processor can return
                // stale results (e.g. NotFound for an invocation that exists on the leader)
                // because the follower's local store may not have replayed all log entries yet.
                // To prevent stale reads, we only serve this point-read from the leader whose
                // store is authoritative. Non-leaders return NotLeader, which the ingress retry
                // loop will retry until the request reaches the actual leader.
                //
                // TODO: We could relax the leadership requirement by implementing linearizable
                // reads on followers using the wait_for_tail_then pattern: find the current log
                // tail and wait until this replica has applied up to that point before serving the
                // read. This would allow followers to serve reads and reduce load on the leader.
                // An open question is how to handle partitioned followers that can no longer apply
                // the latest records (e.g. due to network partitions) — they would need to time
                // out and return an error rather than blocking indefinitely.
                if !self.proposer.is_leader() {
                    replier.send_result(Err(PartitionProcessorRpcError::NotLeader(
                        self.proposer.partition_id(),
                    )));
                    return Ok(());
                }
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
