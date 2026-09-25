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
mod get_invocation_status;
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
use restate_types::identifiers::{InvocationId, PartitionId, PartitionProcessorRpcRequestId};
use restate_types::invocation::InvocationRequest;
use restate_types::logs::Keys;
use restate_types::net::partition_processor::{
    AppendInvocationReplyOn, AppendInvocationResponseRpcRequest, AppendInvocationRpcRequest,
    AppendSignalRpcRequest, CancelInvocationRpcRequest, GetInvocationOutputRpcRequest,
    GetInvocationStatusRpcRequest, KillInvocationRpcRequest, PartitionProcessorRpcError,
    PartitionProcessorRpcOkOf, PartitionProcessorRpcRequest, PartitionProcessorRpcRequestHeader,
    PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse, PartitionProcessorWireRpc,
    PauseInvocationRpcRequest, PurgeInvocationRpcRequest, PurgeJournalRpcRequest,
    RestartAsNewInvocationRpcRequest, ResumeInvocationRpcRequest,
};
use restate_types::schema::deployment::DeploymentResolver;
use restate_wal_protocol::v2::{Command, CommandWithKeys, ErasedCommand};

#[derive(Clone, derive_more::Debug)]
pub(crate) struct RpcProposal<Response> {
    keys: Keys,
    cmd: ErasedCommand,
    reply_on: ReplyOn<Response>,
}

impl<R> RpcProposal<R> {
    pub(crate) fn new<C: Command>(cmd: impl CommandWithKeys<C>, reply_on: ReplyOn<R>) -> Self {
        let keys = cmd.keys();
        let cmd = cmd.inner();
        Self {
            keys,
            cmd: ErasedCommand::new(cmd),
            reply_on,
        }
    }

    pub(crate) fn into_parts(self) -> (Keys, ErasedCommand, ReplyOn<R>) {
        let Self {
            keys,
            cmd,
            reply_on,
        } = self;

        (keys, cmd, reply_on)
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub(crate) enum Decision<Response = PartitionProcessorRpcResponse> {
    Propose(RpcProposal<Response>),
    /// Reply immediately; nothing is proposed.
    Reply(Result<Response, PartitionProcessorRpcError>),
}

impl<R> Decision<R> {
    fn map_response<T>(self, map: impl FnOnce(R) -> T) -> Decision<T> {
        match self {
            Self::Propose(RpcProposal {
                keys,
                cmd,
                reply_on,
            }) => Decision::Propose(RpcProposal {
                keys,
                cmd,
                reply_on: reply_on.map_response(map),
            }),
            Self::Reply(response) => Decision::Reply(response.map(map)),
        }
    }

    #[cfg(test)]
    fn extract_as_rpc_proposal<C: Command>(self) -> (Keys, C, ReplyOn<R>) {
        let Self::Propose(proposal) = self else {
            panic!("Invalid Decision variant, expecting Decision::Propose");
        };

        let Some(inner) = proposal.cmd.downcast_arc::<C>() else {
            panic!("Command type is not match '{}'", C::KIND);
        };

        (
            proposal.keys,
            Arc::into_inner(inner).expect("only owner"),
            proposal.reply_on,
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ReplyOn<Response> {
    /// Responds to the request; the state machine's Action replies later.
    Apply {
        request_id: PartitionProcessorRpcRequestId,
    },
    /// Append WITHOUT dedup ESN; reply `response` on Bifrost commit.
    Commit { response: Response },
    /// Like Apply, but clear the invocation's fencing token strictly AFTER the
    /// append succeeds.
    ApplyAndFence {
        request_id: PartitionProcessorRpcRequestId,
        invocation_id: InvocationId,
    },
}

impl<R> ReplyOn<R> {
    fn map_response<T>(self, map: impl FnOnce(R) -> T) -> ReplyOn<T> {
        match self {
            Self::Apply { request_id } => ReplyOn::Apply { request_id },
            Self::Commit { response } => ReplyOn::Commit {
                response: map(response),
            },
            Self::ApplyAndFence {
                request_id,
                invocation_id,
            } => ReplyOn::ApplyAndFence {
                request_id,
                invocation_id,
            },
        }
    }
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

pub(super) trait RpcHandler<Request: PartitionProcessorWireRpc> {
    fn handle(
        self,
        request: Request,
    ) -> impl Future<Output = Decision<PartitionProcessorRpcOkOf<Request>>>;
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
            sent_at,
            inner,
        }: PartitionProcessorRpcRequest,
    ) -> Decision {
        let header = PartitionProcessorRpcRequestHeader {
            request_id,
            sent_at,
        };

        match inner {
            PartitionProcessorRpcRequestInner::AppendInvocation(
                invocation_request,
                append_invocation_reply_on,
            ) => self
                .handle(AppendInvocationRpcRequest {
                    header,
                    invocation_request,
                    append_invocation_reply_on,
                })
                .await
                .map_response(|response| {
                    response
                        .try_into()
                        .expect("handler returned an invalid append invocation response")
                }),
            PartitionProcessorRpcRequestInner::GetInvocationOutput(
                invocation_query,
                response_mode,
            ) => self
                .handle(GetInvocationOutputRpcRequest {
                    header,
                    invocation_query: invocation_query.into(),
                    response_mode,
                })
                .await
                .map_response(|response| {
                    response
                        .try_into()
                        .expect("handler returned an invalid get invocation output response")
                }),
            PartitionProcessorRpcRequestInner::GetInvocationStatus { invocation_id } => self
                .handle(GetInvocationStatusRpcRequest {
                    header,
                    invocation_id,
                })
                .await
                .map_response(|response| {
                    response
                        .try_into()
                        .expect("handler returned an invalid get invocation status response")
                }),
            PartitionProcessorRpcRequestInner::AppendInvocationResponse(invocation_response) => {
                self.handle(AppendInvocationResponseRpcRequest {
                    header,
                    invocation_response,
                })
                .await
                .map_response(Into::into)
            }
            PartitionProcessorRpcRequestInner::AppendSignal(invocation_id, signal) => self
                .handle(AppendSignalRpcRequest {
                    header,
                    invocation_id,
                    signal_id: Some(signal.id.into()),
                    result: Some(signal.result.into()),
                })
                .await
                .map_response(Into::into),
            PartitionProcessorRpcRequestInner::CancelInvocation { invocation_id } => self
                .handle(CancelInvocationRpcRequest {
                    header,
                    invocation_id,
                })
                .await
                .map_response(Into::into),
            PartitionProcessorRpcRequestInner::KillInvocation { invocation_id } => self
                .handle(KillInvocationRpcRequest {
                    header,
                    invocation_id,
                })
                .await
                .map_response(Into::into),
            PartitionProcessorRpcRequestInner::PurgeInvocation { invocation_id } => self
                .handle(PurgeInvocationRpcRequest {
                    header,
                    invocation_id,
                })
                .await
                .map_response(Into::into),
            PartitionProcessorRpcRequestInner::PurgeJournal { invocation_id } => self
                .handle(PurgeJournalRpcRequest {
                    header,
                    invocation_id,
                })
                .await
                .map_response(PartitionProcessorRpcResponse::PurgeJournal),
            PartitionProcessorRpcRequestInner::RestartAsNewInvocation {
                invocation_id,
                copy_prefix_up_to_index_included,
                patch_deployment_id,
            } => self
                .handle(RestartAsNewInvocationRpcRequest {
                    header,
                    invocation_id,
                    copy_prefix_up_to_index_included,
                    patch_deployment_id: patch_deployment_id.into(),
                })
                .await
                .map_response(Into::into),
            PartitionProcessorRpcRequestInner::ResumeInvocation {
                invocation_id,
                deployment_id,
            } => self
                .handle(ResumeInvocationRpcRequest {
                    header,
                    invocation_id,
                    deployment_id: deployment_id.into(),
                })
                .await
                .map_response(Into::into),
            PartitionProcessorRpcRequestInner::PauseInvocation { invocation_id } => self
                .handle(PauseInvocationRpcRequest {
                    header,
                    invocation_id,
                })
                .await
                .map_response(Into::into),
        }
    }
}
