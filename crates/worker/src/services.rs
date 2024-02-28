// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! todo: This service can probably be removed once the admin service can directly write into target partitions

use crate::subscription_integration::SubscriptionControllerHandle;
use restate_core::{cancellation_watcher, metadata};
use restate_types::identifiers::{PartitionId, PartitionKey, WithPartitionKey};
use restate_types::invocation::InvocationTermination;
use restate_types::message::PartitionTarget;
use restate_types::partition_table::{FindPartition, PartitionTableError};
use restate_types::state_mut::ExternalStateMutation;
use restate_wal_protocol::{AckMode, Command, Destination, Envelope, Header, Source};
use tokio::sync::mpsc;
use tracing::debug;

// todo: Should become the log writer
type ConsensusWriter = mpsc::Sender<PartitionTarget<Envelope>>;

/// Commands that can be sent to a worker.
#[derive(Debug, Clone, Eq, PartialEq)]
enum WorkerCommand {
    TerminateInvocation(InvocationTermination),
    ExternalStateMutation(ExternalStateMutation),
}

#[derive(Debug, Clone)]
pub struct WorkerCommandSender {
    command_tx: mpsc::Sender<WorkerCommand>,
}

impl WorkerCommandSender {
    fn new(command_tx: mpsc::Sender<WorkerCommand>) -> Self {
        Self { command_tx }
    }
}

impl restate_worker_api::Handle for WorkerCommandSender {
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
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("consensus closed")]
    ConsensusClosed,
    #[error(transparent)]
    PartitionNotFound(#[from] PartitionTableError),
}

pub(crate) struct Services {
    command_rx: mpsc::Receiver<WorkerCommand>,

    consensus_writer: ConsensusWriter,

    command_tx: WorkerCommandSender,
    subscription_controller_handle: SubscriptionControllerHandle,
}

impl Services {
    pub(crate) fn new(
        consensus_writer: ConsensusWriter,
        subscription_controller_handle: SubscriptionControllerHandle,
        channel_size: usize,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(channel_size);

        Self {
            command_rx,
            command_tx: WorkerCommandSender::new(command_tx),
            subscription_controller_handle,
            consensus_writer,
        }
    }

    pub(crate) fn worker_command_tx(&self) -> WorkerCommandSender {
        self.command_tx.clone()
    }

    pub(crate) fn subscription_controller_handler(&self) -> SubscriptionControllerHandle {
        self.subscription_controller_handle.clone()
    }

    pub(crate) async fn run(self) -> anyhow::Result<()> {
        let Self {
            mut command_rx,
            consensus_writer,
            ..
        } = self;

        let shutdown_signal = cancellation_watcher();
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
                            let partition_key = mutation.service_id.partition_key();
                            let partition_id = Self::find_partition_id(partition_key)?;
                            let header = create_header(partition_key);
                            let envelope = Envelope::new(header, Command::PatchState(mutation));
                            consensus_writer.send((partition_id, envelope)).await.map_err(|_| Error::ConsensusClosed)?
                        },
                        WorkerCommand::TerminateInvocation(invocation_termination) => {
                            let partition_key = invocation_termination.maybe_fid.partition_key();
                            let partition_id = Self::find_partition_id(partition_key)?;

                            let header = create_header(partition_key);
                            let envelope = Envelope::new(header, Command::TerminateInvocation(invocation_termination));
                            consensus_writer.send((partition_id, envelope)).await.map_err(|_| Error::ConsensusClosed)?
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn find_partition_id(partition_key: PartitionKey) -> Result<PartitionId, PartitionTableError> {
        metadata()
            .partition_table()
            .find_partition_id(partition_key)
    }
}

fn create_header(partition_key: PartitionKey) -> Header {
    Header {
        source: Source::ControlPlane {},
        dest: Destination::Processor {
            partition_key,
            dedup: None,
        },
        ack_mode: AckMode::None,
    }
}
