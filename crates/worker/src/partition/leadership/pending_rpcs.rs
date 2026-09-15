// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use tracing::trace;

use restate_core::network::{Oneshot, Reciprocal};
use restate_types::identifiers::{InvocationId, PartitionProcessorRpcRequestId};
use restate_types::net::RpcResponse;
use restate_types::net::partition_processor::{
    PartitionProcessorRpcError, PartitionProcessorRpcResponse,
};

use crate::partition::rpc;

pub(super) type RpcResponseTx<Response> =
    Reciprocal<Oneshot<Result<Response, PartitionProcessorRpcError>>>;

/// The outcome of applying an rpc-originated command, as carried by the state machine's replying
/// [`crate::partition::state_machine::Action`]s.
pub(super) enum ApplyOutcome {
    Legacy(PartitionProcessorRpcResponse),
}

impl From<ApplyOutcome> for PartitionProcessorRpcResponse {
    fn from(value: ApplyOutcome) -> Self {
        match value {
            ApplyOutcome::Legacy(response) => response,
        }
    }
}

/// Defines [`RpcReciprocal`], one variant per wire response type the leader can reply with.
///
/// Every response type must implement `TryFrom<ApplyOutcome>`; the conversion fails for the
/// outcomes that response type cannot carry. A `From<ApplyOutcome>` impl satisfies this too.
macro_rules! define_rpc_reciprocals {
    ($($tag:ident => $response:ty),* $(,)?) => {
        pub enum RpcReciprocal {
            $(
                $tag(RpcResponseTx<$response>),
            )*
        }

        $(
            impl From<RpcResponseTx<$response>> for RpcReciprocal {
                fn from(reciprocal: RpcResponseTx<$response>) -> Self {
                    Self::$tag(reciprocal)
                }
            }
        )*

        impl RpcReciprocal {
            pub fn fail(self, error: PartitionProcessorRpcError) {
                match self {
                    $(
                        Self::$tag(reciprocal) => reciprocal.send(Err(error)),
                    )*
                }
            }

            pub(super) fn reply(self, outcome: ApplyOutcome) {
                match self {
                    $(
                        Self::$tag(reciprocal) => match <$response>::try_from(outcome) {
                            Ok(response) => reciprocal.send(Ok(response)),
                            Err(_) => {
                                debug_assert!(false, "rpc reciprocal cannot reply with this outcome");
                                reciprocal.send(Err(PartitionProcessorRpcError::Internal(
                                    "unexpected response type for this rpc".to_owned(),
                                )));
                            }
                        },
                    )*
                }
            }
        }
    };
}

define_rpc_reciprocals!(Legacy => PartitionProcessorRpcResponse);

/// The rpcs whose command has been proposed and whose reply is produced by a state machine
/// [`crate::partition::state_machine::Action`] once the command is applied.
#[derive(Default)]
pub(super) struct PendingRpcs {
    inner: HashMap<PartitionProcessorRpcRequestId, RpcReciprocal>,
}

impl PendingRpcs {
    /// Registers `reciprocal` as awaiting the apply of `request_id`.
    ///
    /// Returns `false` if this request id was already pending. In that case the request is a
    /// retry of an already proposed command: the newer reciprocal replaces the older one, which
    /// is failed, and the caller must *not* propose the command again.
    pub fn insert(
        &mut self,
        request_id: PartitionProcessorRpcRequestId,
        reciprocal: RpcReciprocal,
    ) -> bool {
        let Some(old_reciprocal) = self.inner.insert(request_id, reciprocal) else {
            return true;
        };

        trace!(%request_id, "Replacing rpc with newer request");
        old_reciprocal.fail(PartitionProcessorRpcError::Internal("retried".to_string()));
        false
    }

    pub fn remove(&mut self, request_id: &PartitionProcessorRpcRequestId) -> Option<RpcReciprocal> {
        self.inner.remove(request_id)
    }

    pub fn fail_all(&mut self, error: PartitionProcessorRpcError) {
        for (_, reciprocal) in self.inner.drain() {
            reciprocal.fail(error.clone());
        }
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
    /// Binds the handler's [`rpc::ReplyOn`] decision to the reciprocal that will carry the reply.
    pub(super) fn new<R>(reply_on: rpc::ReplyOn<R>, reciprocal: RpcResponseTx<R>) -> Self
    where
        RpcReciprocal: From<RpcResponseTx<R>>,
        Result<R, PartitionProcessorRpcError>: RpcResponse,
        R: Send + Sync + 'static,
    {
        match reply_on {
            rpc::ReplyOn::Apply { request_id } => Self::OnApply {
                request_id,
                fence: None,
                reciprocal: reciprocal.into(),
            },
            rpc::ReplyOn::ApplyAndFence {
                request_id,
                invocation_id,
            } => Self::OnApply {
                request_id,
                fence: Some(invocation_id),
                reciprocal: reciprocal.into(),
            },
            rpc::ReplyOn::Commit { response } => Self::OnCommit(CommitCallback::from(
                move |result: Result<(), PartitionProcessorRpcError>| {
                    reciprocal.send(result.map(|()| response));
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
