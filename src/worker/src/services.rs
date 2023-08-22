// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::{AckCommand, Command};
use futures::future::BoxFuture;
use futures::FutureExt;
use restate_consensus::ProposalSender;
use restate_network::PartitionTableError;
use restate_types::identifiers::FullInvocationId;
use restate_types::identifiers::WithPartitionKey;
use restate_types::message::PeerTarget;
use tokio::sync::mpsc;
use tracing::debug;

/// Commands that can be sent to a worker.
#[derive(Debug, Clone, Eq, PartialEq)]
enum WorkerCommand {
    KillInvocation(FullInvocationId),
}

#[derive(Debug, Clone)]
pub struct WorkerCommandSender(mpsc::Sender<WorkerCommand>);

impl WorkerCommandSender {
    fn new(command_tx: mpsc::Sender<WorkerCommand>) -> Self {
        Self(command_tx)
    }
}

impl restate_worker_api::Handle for WorkerCommandSender {
    type Future = BoxFuture<'static, Result<(), restate_worker_api::Error>>;

    fn kill_invocation(&self, full_invocation_id: FullInvocationId) -> Self::Future {
        let tx = self.0.clone();
        async move {
            tx.send(WorkerCommand::KillInvocation(full_invocation_id))
                .await
                .map_err(|_| restate_worker_api::Error::Unreachable)
        }
        .boxed()
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

    proposal_tx: ProposalSender<PeerTarget<AckCommand>>,
    partition_table: PartitionTable,

    command_tx: WorkerCommandSender,
}

impl<PartitionTable> Services<PartitionTable>
where
    PartitionTable: restate_network::PartitionTable,
{
    pub(crate) fn new(
        proposal_tx: ProposalSender<PeerTarget<AckCommand>>,
        partition_table: PartitionTable,
        channel_size: usize,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(channel_size);

        Self {
            command_rx,
            command_tx: WorkerCommandSender::new(command_tx),
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
                        WorkerCommand::KillInvocation(full_invocation_id) => {
                            let target_peer_id = partition_table
                                .partition_key_to_target_peer(full_invocation_id.service_id.partition_key())
                                .await?;
                            let msg = AckCommand::no_ack(Command::Kill(full_invocation_id));
                            proposal_tx.send((target_peer_id, msg)).await.map_err(|_| Error::ConsensusClosed)?
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
