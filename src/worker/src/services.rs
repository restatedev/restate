use crate::partition::{AckCommand, Command};
use restate_common::types::PeerTarget;
use restate_common::worker_command::{WorkerCommand, WorkerCommandSender};
use restate_consensus::ProposalSender;
use restate_network::PartitionTableError;
use tokio::sync::mpsc;
use tracing::debug;

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
                        WorkerCommand::KillInvocation(service_invocation_id) => {
                            let target_peer_id = partition_table
                                .partition_key_to_target_peer(service_invocation_id.service_id.partition_key())
                                .await?;
                            let msg = AckCommand::no_ack(Command::Kill(service_invocation_id));
                            proposal_tx.send((target_peer_id, msg)).await.map_err(|_| Error::ConsensusClosed)?
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
