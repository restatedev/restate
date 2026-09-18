// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_core::network::{Oneshot, Reciprocal};
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use restate_types::net::partition_processor::{
    AppendInvocationResponseRpcResponse, AppendInvocationRpcResponse, AppendSignalRpcResponse,
    CancelInvocationRpcResponse, GetInvocationOutputRpcResponse, GetInvocationStatusRpcResponse,
    KillInvocationRpcResponse, PartitionProcessorResponseRpcEnvelope, PartitionProcessorRpcError,
    PartitionProcessorRpcResponse, PartitionProcessorWireEnvelope, PauseInvocationRpcResponse,
    PurgeInvocationRpcResponse, RestartAsNewInvocationRpcResponse, ResumeInvocationRpcResponse,
};

use crate::partition::rpc as partition_rpc;
use crate::partition::state_machine::RpcReply;

/// Defines [`RpcReciprocal`] and how each response is built from a state machine reply.
///
/// Entries without `@from_reply` are answered immediately or on commit and never receive a
/// reply.
/// The envelope defaults to [`PartitionProcessorResponseRpcEnvelope`]; `@envelope` overrides it
/// for the legacy response.
///
/// Example:
/// ```ignore
///   define_rpc_reciprocals! {
///       CancelInvocation {
///           @response = CancelInvocationRpcResponse,
///           @from_reply = {
///               RpcReply::CancelInvocation(response) => Self::from(response),
///           },
///       }
///       AppendSignal {
///           @response = AppendSignalRpcResponse,
///       }
///   }
/// ```
macro_rules! define_rpc_reciprocals {
    (@envelope $response:ty) => {
        PartitionProcessorResponseRpcEnvelope<$response>
    };
    (@envelope $response:ty, $envelope:ty) => {
        $envelope
    };
    ($(
        $tag:ident {
            @response = $response:ty,
            $(@envelope = $envelope:ty,)?
            $(@from_reply = { $($action:pat => $value:expr),* $(,)? },)?
        }
    )*) => {
        $(
            impl TryFrom<RpcReply> for $response {
                type Error = PartitionProcessorRpcError;

                fn try_from(reply: RpcReply) -> Result<Self, Self::Error> {
                    #[allow(unreachable_patterns)]
                    match reply {
                        $($( $action => Ok($value), )*)?
                        _ => Err(PartitionProcessorRpcError::Internal(
                            "unexpected action".into(),
                        )),
                    }
                }
            }
        )*

        pub enum RpcReciprocal {
            $(
                $tag(Reciprocal<Oneshot<
                    define_rpc_reciprocals!(@envelope $response $(, $envelope)?)
                >>),
            )*
        }

        $(
            impl From<Reciprocal<Oneshot<
                define_rpc_reciprocals!(@envelope $response $(, $envelope)?)
            >>> for RpcReciprocal
            {
                fn from(reciprocal: Reciprocal<Oneshot<
                    define_rpc_reciprocals!(@envelope $response $(, $envelope)?)
                >>) -> Self {
                    Self::$tag(reciprocal)
                }
            }
        )*

        impl RpcReciprocal {
            pub fn fail(self, error: PartitionProcessorRpcError) {
                match self {
                    $( Self::$tag(reciprocal) => reciprocal.send(error.into()), )*
                }
            }

            pub(super) fn reply(self, reply: RpcReply) {
                match self {
                    $( Self::$tag(reciprocal) => send_reply(reciprocal, reply), )*
                }
            }
        }
    };
}

/// Converts the state machine reply into the handler's response and sends it over the wire.
fn send_reply<W>(reciprocal: Reciprocal<Oneshot<W>>, reply: RpcReply)
where
    W: PartitionProcessorWireEnvelope,
    W::Ok: TryFrom<RpcReply, Error = PartitionProcessorRpcError>,
{
    let response = <W::Ok as TryFrom<RpcReply>>::try_from(reply).map_err(|_| {
        debug_assert!(false, "rpc reciprocal cannot use this reply");
        PartitionProcessorRpcError::Internal("unexpected response type for this rpc".to_owned())
    });
    reciprocal.send(response.into());
}

define_rpc_reciprocals! {
    Legacy {
        @response = PartitionProcessorRpcResponse,
        @envelope = Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>,
        @from_reply = {
            RpcReply::Output(response) => Self::Output(response),
            RpcReply::Submitted(response) => Self::Submitted(response),
            RpcReply::KillInvocation(response) =>
                Self::KillInvocation(KillInvocationRpcResponse::from(response)),
            RpcReply::CancelInvocation(response) =>
                Self::CancelInvocation(CancelInvocationRpcResponse::from(response)),
            RpcReply::PurgeInvocation(response) =>
                Self::PurgeInvocation(PurgeInvocationRpcResponse::from(response)),
            RpcReply::PurgeJournal(response) =>
                Self::PurgeJournal(PurgeInvocationRpcResponse::from(response)),
            RpcReply::ResumeInvocation(response) =>
                Self::ResumeInvocation(ResumeInvocationRpcResponse::from(response)),
            RpcReply::PauseInvocation(response) =>
                Self::PauseInvocation(PauseInvocationRpcResponse::from(response)),
            RpcReply::RestartAsNewInvocation(response) =>
                Self::RestartAsNewInvocation(RestartAsNewInvocationRpcResponse::from(response)),
        },
    }
    AppendInvocation {
        @response = AppendInvocationRpcResponse,
        @from_reply = {
            RpcReply::Output(response) => Self::Output(response.into()),
            RpcReply::Submitted(response) => Self::Submitted(response.into()),
        },
    }
    GetInvocationOutput {
        @response = GetInvocationOutputRpcResponse,
        @from_reply = {
            RpcReply::Output(response) => Self::Output(response.into()),
        },
    }
    CancelInvocation {
        @response = CancelInvocationRpcResponse,
        @from_reply = {
            RpcReply::CancelInvocation(response) => Self::from(response),
        },
    }
    KillInvocation {
        @response = KillInvocationRpcResponse,
        @from_reply = {
            RpcReply::KillInvocation(response) => Self::from(response),
        },
    }
    PurgeInvocation {
        @response = PurgeInvocationRpcResponse,
        @from_reply = {
            RpcReply::PurgeInvocation(response) | RpcReply::PurgeJournal(response) => {
                Self::from(response)
            }
        },
    }
    RestartAsNewInvocation {
        @response = RestartAsNewInvocationRpcResponse,
        @from_reply = {
            RpcReply::RestartAsNewInvocation(response) => Self::from(response),
        },
    }
    ResumeInvocation {
        @response = ResumeInvocationRpcResponse,
        @from_reply = {
            RpcReply::ResumeInvocation(response) => Self::from(response),
        },
    }
    PauseInvocation {
        @response = PauseInvocationRpcResponse,
        @from_reply = {
            RpcReply::PauseInvocation(response) => Self::from(response),
        },
    }
    // Answered immediately or on commit, so they never receive a reply.
    GetInvocationStatus {
        @response = GetInvocationStatusRpcResponse,
    }
    AppendInvocationResponse {
        @response = AppendInvocationResponseRpcResponse,
    }
    AppendSignal {
        @response = AppendSignalRpcResponse,
    }
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
        reciprocal: Reciprocal<Oneshot<W>>,
    ) -> Self
    where
        W: PartitionProcessorWireEnvelope,
        RpcReciprocal: From<Reciprocal<Oneshot<W>>>,
    {
        match reply_on {
            partition_rpc::ReplyOn::Apply { request_id } => Self::OnApply {
                request_id,
                fence: None,
                reciprocal: reciprocal.into(),
            },
            partition_rpc::ReplyOn::ApplyAndFence {
                request_id,
                invocation_id,
            } => Self::OnApply {
                request_id,
                fence: Some(invocation_id),
                reciprocal: reciprocal.into(),
            },
            partition_rpc::ReplyOn::Commit { response } => Self::OnCommit(CommitCallback::from(
                move |result: Result<(), PartitionProcessorRpcError>| {
                    reciprocal.send(result.map(|()| response).into());
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
