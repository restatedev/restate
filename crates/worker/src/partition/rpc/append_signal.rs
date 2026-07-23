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
use restate_types::invocation::NotifySignalRequest;
use restate_types::journal_v2::Signal;
use restate_types::net::partition_processor::PartitionProcessorRpcResponse;
use restate_wal_protocol::Command;

pub(super) struct Request {
    pub(super) invocation_id: InvocationId,
    pub(super) signal: Signal,
}

impl<'a, TSchemas, TStorage> RpcHandler<Request> for RpcContext<'a, TSchemas, TStorage> {
    async fn handle(
        self,
        Request {
            invocation_id,
            signal,
        }: Request,
    ) -> Decision {
        Decision::Propose(RpcProposal {
            partition_key: invocation_id.partition_key(),
            cmd: Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal,
            }),
            reply_on: ReplyOn::Commit {
                response: PartitionProcessorRpcResponse::Appended,
            },
        })
    }
}
