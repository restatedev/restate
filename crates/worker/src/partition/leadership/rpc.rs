// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::net::partition_processor::PartitionProcessorRpcResponse;

use crate::partition::state_machine::RpcReply;

impl From<RpcReply> for PartitionProcessorRpcResponse {
    fn from(reply: RpcReply) -> Self {
        match reply {
            RpcReply::Output(response) => Self::Output(response),
            RpcReply::Submitted(response) => Self::Submitted(response),
            RpcReply::KillInvocation(response) => Self::KillInvocation(response.into()),
            RpcReply::CancelInvocation(response) => Self::CancelInvocation(response.into()),
            RpcReply::PurgeInvocation(response) => Self::PurgeInvocation(response.into()),
            RpcReply::PurgeJournal(response) => Self::PurgeJournal(response.into()),
            RpcReply::ResumeInvocation(response) => Self::ResumeInvocation(response.into()),
            RpcReply::PauseInvocation(response) => Self::PauseInvocation(response.into()),
            RpcReply::RestartAsNewInvocation(response) => {
                Self::RestartAsNewInvocation(response.into())
            }
        }
    }
}
