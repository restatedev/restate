use crate::fsm::Command;
use crate::partition::{Id, PartitionProcessor};
use consensus::{Command, Consensus, ProposalSender};
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use network::Network;
use tokio::sync::mpsc;
use tokio::try_join;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;

mod fsm;
mod partition;

#[derive(Debug)]
pub struct Worker {
    consensus: Consensus<
        fsm::Command,
        PollSender<consensus::Command<fsm::Command>>,
        ReceiverStream<fsm::Command>,
        PollSender<fsm::Command>,
    >,
    processors: Vec<
        PartitionProcessor<
            ReceiverStream<consensus::Command<fsm::Command>>,
            PollSender<fsm::Command>,
        >,
    >,
    network: Network<ReceiverStream<fsm::Command>, PollSender<fsm::Command>, fsm::Command>,
}

impl Worker {
    pub fn build() -> Self {
        let num_partition_processors = 10;
        let (raft_in_tx, raft_in_rx) = mpsc::channel(64);
        let (raft_out_tx, raft_out_rx) = mpsc::channel(64);

        let network = Network::build(
            ReceiverStream::new(raft_out_rx),
            PollSender::new(raft_in_tx),
        );

        let mut consensus = Consensus::build(
            ReceiverStream::new(raft_in_rx),
            PollSender::new(raft_out_tx),
        );

        let (command_senders, processors) = (0..num_partition_processors)
            .map(|idx| {
                let proposal_sender = consensus.create_proposal_sender();
                Self::create_partition_processor(idx, proposal_sender)
            })
            .unzip();

        consensus.register_command_senders(command_senders);

        Self {
            consensus,
            processors,
            network,
        }
    }

    fn create_partition_processor(
        idx: Id,
        proposal_sender: ProposalSender<Command>,
    ) -> (
        PollSender<Command<Command>>,
        PartitionProcessor<ReceiverStream<Command<Command>>, ProposalSender<Command>>,
    ) {
        let (command_tx, command_rx) = mpsc::channel(1);
        let processor =
            PartitionProcessor::build(idx, ReceiverStream::new(command_rx), proposal_sender);

        (PollSender::new(command_tx), processor)
    }

    pub async fn run(self, drain: drain::Watch) {
        let network_handle = tokio::spawn(self.network.run(drain.clone()));
        let consensus_handle = tokio::spawn(self.consensus.run());
        let processors_handles: FuturesUnordered<_> = self
            .processors
            .into_iter()
            .map(|partition_processor| tokio::spawn(partition_processor.run()))
            .collect();

        // Only signal shutdown once all handles have been completed
        let _release_shutdown = drain.ignore_signaled();

        try_join!(
            network_handle,
            consensus_handle,
            processors_handles.try_collect::<Vec<_>>()
        )
        .expect("Worker component failed");
    }
}
