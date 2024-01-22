// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::subscription_integration;

use crate::partition::{StateMachineAckCommand, StateMachineCommand};
use restate_consensus::ProposalSender;
use restate_network::PartitionTableError;
use restate_types::identifiers::WithPartitionKey;
use restate_types::invocation::InvocationTermination;
use restate_types::message::PeerTarget;
use restate_types::state_mut::ExternalStateMutation;
use tokio::sync::mpsc;
use tracing::debug;

/// Commands that can be sent to a worker.
#[derive(Debug, Clone, Eq, PartialEq)]
enum WorkerCommand {
    TerminateInvocation(InvocationTermination),
    ExternalStateMutation(ExternalStateMutation),
}

#[derive(Debug, Clone)]
pub struct WorkerCommandSender {
    command_tx: mpsc::Sender<WorkerCommand>,
    subscription_controller_handle: subscription_integration::SubscriptionControllerHandle,
}

impl WorkerCommandSender {
    fn new(
        command_tx: mpsc::Sender<WorkerCommand>,
        subscription_controller_handle: subscription_integration::SubscriptionControllerHandle,
    ) -> Self {
        Self {
            command_tx,
            subscription_controller_handle,
        }
    }
}

impl restate_worker_api::Handle for WorkerCommandSender {
    type SubscriptionControllerHandle = subscription_integration::SubscriptionControllerHandle;

    async fn terminate_invocation(
        &self,
        invocation_termination: InvocationTermination,
    ) -> Result<(), restate_worker_api::Error> {
        self.command_tx
            .send(WorkerCommand::TerminateInvocation(invocation_termination))
            .await
            .map_err(|_| restate_worker_api::Error::Unreachable)
    }

    async fn external_state_mutation(
        &self,
        mutation: ExternalStateMutation,
    ) -> Result<(), restate_worker_api::Error> {
        self.command_tx
            .send(WorkerCommand::ExternalStateMutation(mutation))
            .await
            .map_err(|_| restate_worker_api::Error::Unreachable)
    }

    fn subscription_controller_handle(&self) -> Self::SubscriptionControllerHandle {
        self.subscription_controller_handle.clone()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("consensus closed")]
    ConsensusClosed,
    #[error(transparent)]
    PartitionNotFound(#[from] PartitionTableError),
}

pub(crate) struct Services<PartitionTable> {
    command_rx: mpsc::Receiver<WorkerCommand>,

    proposal_tx: ProposalSender<PeerTarget<StateMachineAckCommand>>,
    partition_table: PartitionTable,

    command_tx: WorkerCommandSender,
}

impl<PartitionTable> Services<PartitionTable>
where
    PartitionTable: restate_network::PartitionTable,
{
    pub(crate) fn new(
        proposal_tx: ProposalSender<PeerTarget<StateMachineAckCommand>>,
        subscription_controller_handle: subscription_integration::SubscriptionControllerHandle,
        partition_table: PartitionTable,
        channel_size: usize,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(channel_size);

        Self {
            command_rx,
            command_tx: WorkerCommandSender::new(command_tx, subscription_controller_handle),
            proposal_tx,
            partition_table,
        }
    }

    pub(crate) fn worker_command_tx(&self) -> WorkerCommandSender {
        self.command_tx.clone()
    }

    pub(crate) async fn run(self, shutdown_watch: drain::Watch) -> Result<(), Error> {
        let Self {
            mut command_rx,
            proposal_tx,
            partition_table,
            ..
        } = self;

        let shutdown_signal = shutdown_watch.signaled();
        tokio::pin!(shutdown_signal);

        debug!("Running the worker services");

        loop {
            tokio::select! {
                _ = &mut shutdown_signal => {
                    debug!("Stopping the worker services");
                    break;
                },
                Some(command) = command_rx.recv() => {
                    match command {
                        WorkerCommand::ExternalStateMutation(mutation) => {
                            let target_peer_id = partition_table
                                .partition_key_to_target_peer(mutation.service_id.partition_key())
                                .await?;
                            let msg = StateMachineAckCommand::no_ack(StateMachineCommand::ExternalStateMutation(mutation));
                            proposal_tx.send((target_peer_id, msg)).await.map_err(|_| Error::ConsensusClosed)?
                        },
                        WorkerCommand::TerminateInvocation(invocation_termination) => {
                            let target_peer_id = partition_table
                                .partition_key_to_target_peer(invocation_termination.maybe_fid.partition_key())
                                .await?;
                            let msg = StateMachineAckCommand::no_ack(StateMachineCommand::TerminateInvocation(invocation_termination));
                            proposal_tx.send((target_peer_id, msg)).await.map_err(|_| Error::ConsensusClosed)?
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
