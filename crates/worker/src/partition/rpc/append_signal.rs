// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::invocation::NotifySignalRequest;
use restate_types::journal_v2::Signal;
use restate_types::net::partition_processor::{AppendSignalRpcRequest, AppendSignalRpcResponse};
use restate_wal_protocol::v2::commands;

use super::*;

impl<'a, TSchemas, TStorage> RpcHandler<AppendSignalRpcRequest>
    for RpcContext<'a, TSchemas, TStorage>
{
    async fn handle(
        self,
        AppendSignalRpcRequest {
            invocation_id,
            signal_id,
            result,
            ..
        }: AppendSignalRpcRequest,
    ) -> Decision<AppendSignalRpcResponse> {
        let (Some(signal_id), Some(result)) = (signal_id, result) else {
            return Decision::Reply(Err(PartitionProcessorRpcError::Internal(
                "append signal request is missing the signal id or result".to_owned(),
            )));
        };

        Decision::Propose(RpcProposal::new(
            commands::NotifySignalCommand::from(NotifySignalRequest {
                invocation_id,
                signal: Signal::new(signal_id.into(), result.into()),
            }),
            ReplyOn::Commit {
                response: AppendSignalRpcResponse,
            },
        ))
    }
}
