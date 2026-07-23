// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod append_invocation;
mod append_invocation_response;
mod append_signal;
mod cancel_invocation;
mod get_invocation_output;
mod kill_invocation;
mod pause_invocation;
mod purge_invocation;
mod purge_journal;
mod restart_as_new_invocation;
mod resume_invocation;

use std::sync::Arc;

use restate_storage_api::invocation_status_table::ReadInvocationStatusTable;
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2::ReadJournalTable;
use restate_types::identifiers::{
    InvocationId, PartitionId, PartitionKey, PartitionProcessorRpcRequestId,
};
use restate_types::invocation::InvocationRequest;
use restate_types::net::partition_processor::{
    AppendInvocationReplyOn, PartitionProcessorRpcError, PartitionProcessorRpcRequest,
    PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse,
};
use restate_types::schema::deployment::DeploymentResolver;
use restate_wal_protocol::Command;

#[derive(Debug, Clone)]
pub(crate) struct RpcProposal {
    pub(crate) partition_key: PartitionKey,
    pub(crate) cmd: Command,
    pub(crate) reply_on: ReplyOn,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Decision {
    Propose(RpcProposal),
    /// Reply immediately; nothing is proposed.
    Reply(Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>),
    /// Legacy invoker-owned paths only: poke the invoker, then reply immediately.
    /// TODO: remove this once the non-vqueues support is dropped.
    NotifyInvokerAndReply {
        notification: InvokerNotification,
        reply: PartitionProcessorRpcResponse,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ReplyOn {
    /// Responds to the request; the state machine's Action replies later.
    Apply {
        request_id: PartitionProcessorRpcRequestId,
    },
    /// Append WITHOUT dedup ESN; reply `response` on Bifrost commit.
    Commit {
        response: PartitionProcessorRpcResponse,
    },
    /// Like Apply, but clear the invocation's fencing token strictly AFTER the
    /// append succeeds.
    ApplyAndFence {
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    },
}

#[derive(Debug)]
pub(crate) enum InvokerNotification {
    RetryNow(InvocationId), // resume legacy path, resume_invocation.rs:92
    Pause(InvocationId),    // pause legacy path,  pause_invocation.rs:93
}

pub(super) struct RpcContext<'a, Schemas, Storage> {
    is_leader: bool,
    partition_id: PartitionId,
    schemas: &'a Schemas,
    storage: &'a mut Storage,
}

impl<'a, Schemas, Storage> RpcContext<'a, Schemas, Storage> {
    pub(super) fn new(
        is_leader: bool,
        partition_id: PartitionId,
        schemas: &'a Schemas,
        storage: &'a mut Storage,
    ) -> Self {
        Self {
            is_leader,
            partition_id,
            schemas,
            storage,
        }
    }
}

pub(super) trait RpcHandler<Input> {
    fn handle(self, input: Input) -> impl Future<Output = Decision>;
}

impl<'a, TSchemas, TStorage> RpcHandler<PartitionProcessorRpcRequest>
    for RpcContext<'a, TSchemas, TStorage>
where
    TSchemas: DeploymentResolver,
    TStorage: ReadInvocationStatusTable + ReadJournalTable + journal_table_v1::ReadJournalTable,
{
    async fn handle(
        self,
        PartitionProcessorRpcRequest {
            request_id,
            partition_id: _,
            inner,
        }: PartitionProcessorRpcRequest,
    ) -> Decision {
        match inner {
            PartitionProcessorRpcRequestInner::AppendInvocation(
                invocation_request,
                append_invocation_reply_on,
            ) => {
                self.handle(append_invocation::Request {
                    request_id,
                    invocation_request,
                    append_invocation_reply_on,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::GetInvocationOutput(
                invocation_query,
                response_mode,
            ) => {
                self.handle(get_invocation_output::Request {
                    request_id,
                    invocation_query,
                    response_mode,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::AppendInvocationResponse(invocation_response) => {
                self.handle(append_invocation_response::Request {
                    invocation_response,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::AppendSignal(invocation_id, signal) => {
                self.handle(append_signal::Request {
                    invocation_id,
                    signal,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::CancelInvocation { invocation_id } => {
                self.handle(cancel_invocation::Request {
                    request_id,
                    invocation_id,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::KillInvocation { invocation_id } => {
                self.handle(kill_invocation::Request {
                    request_id,
                    invocation_id,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::PurgeInvocation { invocation_id } => {
                self.handle(purge_invocation::Request {
                    request_id,
                    invocation_id,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::PurgeJournal { invocation_id } => {
                self.handle(purge_journal::Request {
                    request_id,
                    invocation_id,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::RestartAsNewInvocation {
                invocation_id,
                copy_prefix_up_to_index_included,
                patch_deployment_id,
            } => {
                self.handle(restart_as_new_invocation::Request {
                    request_id,
                    invocation_id,
                    copy_prefix_up_to_index_included,
                    patch_deployment_id,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::ResumeInvocation {
                invocation_id,
                deployment_id,
            } => {
                self.handle(resume_invocation::Request {
                    request_id,
                    invocation_id,
                    update_deployment_id: deployment_id,
                })
                .await
            }
            PartitionProcessorRpcRequestInner::PauseInvocation { invocation_id } => {
                self.handle(pause_invocation::PauseRequest {
                    request_id,
                    invocation_id,
                })
                .await
            }
        }
    }
}
