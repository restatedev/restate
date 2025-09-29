// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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
mod purge_invocation;
mod purge_journal;
mod restart_as_new_invocation;
mod resume_invocation;

use crate::partition;
use crate::partition::leadership::LeadershipState;
use restate_core::network::{Oneshot, Reciprocal};
use restate_invoker_api::InvokerHandle;
use restate_storage_api::idempotency_table::ReadOnlyIdempotencyTable;
use restate_storage_api::invocation_status_table::ReadInvocationStatusTable;
use restate_storage_api::journal_table_v2::ReadJournalTable;
use restate_storage_api::service_status_table::ReadOnlyVirtualObjectStatusTable;
use restate_types::identifiers::{InvocationId, PartitionKey, PartitionProcessorRpcRequestId};
use restate_types::invocation::{InvocationEpoch, InvocationRequest};
use restate_types::net::partition_processor::{
    AppendInvocationReplyOn, PartitionProcessorRpcError, PartitionProcessorRpcRequest,
    PartitionProcessorRpcRequestInner, PartitionProcessorRpcResponse,
};
use restate_types::schema::deployment::DeploymentResolver;
use restate_wal_protocol::Command;
use std::marker::PhantomData;
use std::sync::Arc;

#[cfg_attr(test, mockall::automock)]
pub(super) trait Actuator {
    fn self_propose_and_respond_asynchronously<O: 'static + Into<PartitionProcessorRpcResponse>>(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        replier: Replier<O>,
        on_proposed_response: O,
    ) -> impl Future<Output = ()>;

    fn handle_rpc_proposal_command<O: 'static>(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        request_id: PartitionProcessorRpcRequestId,
        replier: Replier<O>,
    ) -> impl Future<Output = ()>;

    fn notify_invoker_to_retry_now(
        &mut self,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
    ) -> impl Future<Output = ()>;
}

impl<
    I: InvokerHandle<
        partition::invoker_storage_reader::InvokerStorageReader<
            restate_partition_store::PartitionStore,
        >,
    >,
> Actuator for LeadershipState<I>
{
    async fn self_propose_and_respond_asynchronously<O: Into<PartitionProcessorRpcResponse>>(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        replier: Replier<O>,
        on_proposed_response: O,
    ) {
        LeadershipState::self_propose_and_respond_asynchronously(
            self,
            partition_key,
            cmd,
            replier.0,
            on_proposed_response.into(),
        )
        .await
    }

    async fn handle_rpc_proposal_command<O>(
        &mut self,
        partition_key: PartitionKey,
        cmd: Command,
        request_id: PartitionProcessorRpcRequestId,
        replier: Replier<O>,
    ) {
        LeadershipState::handle_rpc_proposal_command(
            self,
            request_id,
            replier.0,
            partition_key,
            cmd,
        )
        .await
    }

    async fn notify_invoker_to_retry_now(
        &mut self,
        invocation_id: InvocationId,
        invocation_epoch: InvocationEpoch,
    ) {
        let Some((partition_leader_epoch, invoker_handle)) = LeadershipState::invoker_handle(self)
        else {
            return;
        };
        let _ = invoker_handle
            .retry_invocation_now(partition_leader_epoch, invocation_id, invocation_epoch)
            .await;
    }
}

pub(super) struct RpcContext<'a, Actuator, Schemas, Storage> {
    proposer: &'a mut Actuator,
    schemas: &'a Schemas,
    storage: &'a mut Storage,
}

impl<'a, Actuator, Schemas, Storage> RpcContext<'a, Actuator, Schemas, Storage> {
    pub(super) fn new(
        proposer: &'a mut Actuator,
        schemas: &'a Schemas,
        storage: &'a mut Storage,
    ) -> Self {
        Self {
            proposer,
            schemas,
            storage,
        }
    }
}

pub(super) struct Replier<O>(
    Reciprocal<Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>>,
    PhantomData<O>,
);

impl<O: Into<PartitionProcessorRpcResponse>> Replier<O> {
    pub fn new(
        reciprocal: Reciprocal<
            Oneshot<Result<PartitionProcessorRpcResponse, PartitionProcessorRpcError>>,
        >,
    ) -> Self {
        Self(reciprocal, Default::default())
    }

    pub fn send(self, msg: O) {
        self.0.send(Ok(msg.into()));
    }

    pub fn send_result(self, res: Result<O, PartitionProcessorRpcError>) {
        self.0.send(res.map(|msg| msg.into()));
    }

    pub fn map<U: Into<PartitionProcessorRpcResponse>>(self) -> Replier<U> {
        Replier::new(self.0)
    }
}

pub(super) trait RpcHandler<Input> {
    type Output: Into<PartitionProcessorRpcResponse>;
    type Error;

    fn handle(
        self,
        input: Input,
        response_tx: Replier<Self::Output>,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

impl<'a, TActuator, TSchemas, TStorage> RpcHandler<PartitionProcessorRpcRequest>
    for RpcContext<'a, TActuator, TSchemas, TStorage>
where
    TActuator: Actuator,
    TSchemas: DeploymentResolver,
    TStorage: ReadInvocationStatusTable
        + ReadOnlyVirtualObjectStatusTable
        + ReadOnlyIdempotencyTable
        + ReadJournalTable,
{
    type Output = PartitionProcessorRpcResponse;
    type Error = ();

    async fn handle(
        self,
        PartitionProcessorRpcRequest {
            request_id,
            partition_id: _,
            inner,
        }: PartitionProcessorRpcRequest,
        replier: Replier<Self::Output>,
    ) -> Result<(), Self::Error> {
        match inner {
            PartitionProcessorRpcRequestInner::AppendInvocation(
                invocation_request,
                append_invocation_reply_on,
            ) => {
                self.handle(
                    append_invocation::Request {
                        request_id,
                        invocation_request,
                        append_invocation_reply_on,
                    },
                    replier,
                )
                .await
            }
            PartitionProcessorRpcRequestInner::GetInvocationOutput(
                invocation_query,
                response_mode,
            ) => {
                self.handle(
                    get_invocation_output::Request {
                        request_id,
                        invocation_query,
                        response_mode,
                    },
                    replier,
                )
                .await
            }
            PartitionProcessorRpcRequestInner::AppendInvocationResponse(invocation_response) => {
                self.handle(
                    append_invocation_response::Request {
                        invocation_response,
                    },
                    replier,
                )
                .await
            }
            PartitionProcessorRpcRequestInner::AppendSignal(invocation_id, signal) => {
                self.handle(
                    append_signal::Request {
                        invocation_id,
                        signal,
                    },
                    replier,
                )
                .await
            }
            PartitionProcessorRpcRequestInner::CancelInvocation { invocation_id } => {
                self.handle(
                    cancel_invocation::Request {
                        request_id,
                        invocation_id,
                    },
                    replier.map(),
                )
                .await
            }
            PartitionProcessorRpcRequestInner::KillInvocation { invocation_id } => {
                self.handle(
                    kill_invocation::Request {
                        request_id,
                        invocation_id,
                    },
                    replier.map(),
                )
                .await
            }
            PartitionProcessorRpcRequestInner::PurgeInvocation { invocation_id } => {
                self.handle(
                    purge_invocation::Request {
                        request_id,
                        invocation_id,
                    },
                    replier.map(),
                )
                .await
            }
            PartitionProcessorRpcRequestInner::PurgeJournal { invocation_id } => {
                self.handle(
                    purge_journal::Request {
                        request_id,
                        invocation_id,
                    },
                    replier.map(),
                )
                .await
            }
            PartitionProcessorRpcRequestInner::RestartAsNewInvocation {
                invocation_id,
                copy_prefix_up_to_index_included,
                patch_deployment_id,
            } => {
                self.handle(
                    restart_as_new_invocation::Request {
                        request_id,
                        invocation_id,
                        copy_prefix_up_to_index_included,
                        patch_deployment_id,
                    },
                    replier.map(),
                )
                .await
            }
            PartitionProcessorRpcRequestInner::ResumeInvocation {
                invocation_id,
                deployment_id,
            } => {
                self.handle(
                    resume_invocation::Request {
                        request_id,
                        invocation_id,
                        update_deployment_id: deployment_id,
                    },
                    replier.map(),
                )
                .await
            }
        }
    }
}
