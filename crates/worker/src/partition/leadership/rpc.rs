// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::{ErasedReciprocal, Oneshot, Reciprocal};
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use restate_types::net::partition_processor::{
    AppendInvocationResponseRpcResponse, AppendInvocationRpcResponse, AppendSignalRpcResponse,
    CancelInvocationRpcResponse, GetInvocationOutputRpcResponse, GetInvocationStatusRpcResponse,
    KillInvocationRpcResponse, PartitionProcessorRpcError, PartitionProcessorRpcResponse,
    PartitionProcessorWireRpc, PauseInvocationRpcResponse, PurgeInvocationRpcResponse,
    RestartAsNewInvocationRpcResponse, ResumeInvocationRpcResponse,
};

use crate::partition::rpc as partition_rpc;
use crate::partition::state_machine::RpcReply;

/// Builds an rpc's successful response from a state machine reply.
///
/// Returns `None` if the rpc never takes this reply. Rpcs that are answered immediately or on
/// commit never receive a reply and keep the default.
pub(crate) trait FromRpcReply: Sized {
    fn from_rpc_reply(_reply: RpcReply) -> Option<Self> {
        None
    }
}

impl FromRpcReply for PartitionProcessorRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        Some(match reply {
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
        })
    }
}

impl FromRpcReply for AppendInvocationRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        match reply {
            RpcReply::Output(response) => Some(Self::Output(response.into())),
            RpcReply::Submitted(response) => Some(Self::Submitted(response.into())),
            _ => None,
        }
    }
}

impl FromRpcReply for GetInvocationOutputRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        match reply {
            RpcReply::Output(response) => Some(Self::Output(response.into())),
            _ => None,
        }
    }
}

impl FromRpcReply for CancelInvocationRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        match reply {
            RpcReply::CancelInvocation(response) => Some(response.into()),
            _ => None,
        }
    }
}

impl FromRpcReply for KillInvocationRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        match reply {
            RpcReply::KillInvocation(response) => Some(response.into()),
            _ => None,
        }
    }
}

impl FromRpcReply for PurgeInvocationRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        match reply {
            RpcReply::PurgeInvocation(response) | RpcReply::PurgeJournal(response) => {
                Some(response.into())
            }
            _ => None,
        }
    }
}

impl FromRpcReply for RestartAsNewInvocationRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        match reply {
            RpcReply::RestartAsNewInvocation(response) => Some(response.into()),
            _ => None,
        }
    }
}

impl FromRpcReply for ResumeInvocationRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        match reply {
            RpcReply::ResumeInvocation(response) => Some(response.into()),
            _ => None,
        }
    }
}

impl FromRpcReply for PauseInvocationRpcResponse {
    fn from_rpc_reply(reply: RpcReply) -> Option<Self> {
        match reply {
            RpcReply::PauseInvocation(response) => Some(response.into()),
            _ => None,
        }
    }
}

// Answered immediately or on commit, so they never receive a reply.
impl FromRpcReply for GetInvocationStatusRpcResponse {}
impl FromRpcReply for AppendInvocationResponseRpcResponse {}
impl FromRpcReply for AppendSignalRpcResponse {}

/// Reply port of an rpc waiting for the state machine to apply its command.
///
/// The response type is erased so that the reply ports of all rpcs can be kept in one map; `send`
/// is instantiated for the rpc's wire type and restores it.
pub(crate) struct RpcReciprocal {
    reciprocal: ErasedReciprocal,
    send: fn(ErasedReciprocal, Result<RpcReply, PartitionProcessorRpcError>),
}

impl RpcReciprocal {
    pub fn new<W>(reciprocal: Reciprocal<Oneshot<W::Response>>) -> Self
    where
        W: PartitionProcessorWireRpc,
        W::Ok: FromRpcReply,
    {
        Self {
            reciprocal: reciprocal.erase(),
            send: send_reply::<W>,
        }
    }

    pub fn fail(self, error: PartitionProcessorRpcError) {
        (self.send)(self.reciprocal, Err(error));
    }

    pub(super) fn reply(self, reply: RpcReply) {
        (self.send)(self.reciprocal, Ok(reply));
    }
}

/// Converts the state machine reply into the rpc's response and sends it over the wire.
fn send_reply<W>(reciprocal: ErasedReciprocal, reply: Result<RpcReply, PartitionProcessorRpcError>)
where
    W: PartitionProcessorWireRpc,
    W::Ok: FromRpcReply,
{
    let result = reply.and_then(|reply| {
        W::Ok::from_rpc_reply(reply).ok_or_else(|| {
            debug_assert!(false, "rpc reciprocal cannot use this reply");
            PartitionProcessorRpcError::Internal("unexpected response type for this rpc".to_owned())
        })
    });
    reciprocal
        .into_typed::<W::Response>()
        .send(W::wrap_response(result));
}

/// How the leader answers an rpc whose command it is about to propose.
#[derive(derive_more::Debug)]
pub(crate) enum PendingReply {
    /// The state machine's Action replies once the command is applied.
    OnApply {
        request_id: PartitionProcessorRpcRequestId,
        /// If set, clear this invocation's fencing token strictly AFTER the append succeeds.
        fence: Option<InvocationId>,
        #[debug(skip)]
        reciprocal: RpcReciprocal,
    },
    /// Append WITHOUT dedup ESN; reply on Bifrost commit.
    OnCommit(#[debug(skip)] CommitCallback),
}

impl PendingReply {
    /// Binds the handler's [`partition_rpc::ReplyOn`] decision to the reciprocal that will carry the
    /// reply.
    pub(super) fn new<W>(
        reply_on: partition_rpc::ReplyOn<W::Ok>,
        reciprocal: Reciprocal<Oneshot<W::Response>>,
    ) -> Self
    where
        W: PartitionProcessorWireRpc,
        W::Ok: FromRpcReply,
    {
        match reply_on {
            partition_rpc::ReplyOn::Apply { request_id } => Self::OnApply {
                request_id,
                fence: None,
                reciprocal: RpcReciprocal::new::<W>(reciprocal),
            },
            partition_rpc::ReplyOn::ApplyAndFence {
                request_id,
                invocation_id,
            } => Self::OnApply {
                request_id,
                fence: Some(invocation_id),
                reciprocal: RpcReciprocal::new::<W>(reciprocal),
            },
            partition_rpc::ReplyOn::Commit { response } => Self::OnCommit(CommitCallback::from(
                move |result: Result<(), PartitionProcessorRpcError>| {
                    reciprocal.send(W::wrap_response(result.map(|()| response)));
                },
            )),
        }
    }

    pub(super) fn fail(self, error: PartitionProcessorRpcError) {
        match self {
            PendingReply::OnApply { reciprocal, .. } => reciprocal.fail(error),
            PendingReply::OnCommit(callback) => callback.call(Err(error)),
        }
    }
}

trait CommitCallbackInner: Send + Sync + 'static {
    fn call(self: Box<Self>, result: Result<(), PartitionProcessorRpcError>);
}

impl<F> CommitCallbackInner for F
where
    F: FnOnce(Result<(), PartitionProcessorRpcError>) + Send + Sync + 'static,
{
    fn call(self: Box<Self>, result: Result<(), PartitionProcessorRpcError>) {
        self(result)
    }
}

pub(crate) struct CommitCallback {
    inner: Box<dyn CommitCallbackInner>,
}

impl CommitCallback {
    pub(super) fn call(self, result: Result<(), PartitionProcessorRpcError>) {
        self.inner.call(result);
    }
}

impl<I> From<I> for CommitCallback
where
    I: CommitCallbackInner,
{
    fn from(value: I) -> Self {
        Self {
            inner: Box::new(value),
        }
    }
}
