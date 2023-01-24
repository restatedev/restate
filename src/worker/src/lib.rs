use crate::partition::PartitionProcessor;
use consensus::Consensus;
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
    processor: PartitionProcessor<
        ReceiverStream<consensus::Command<fsm::Command>>,
        PollSender<fsm::Command>,
    >,
    network: Network<ReceiverStream<fsm::Command>, PollSender<fsm::Command>, fsm::Command>,
}

impl Worker {
    pub fn build() -> Self {
        let (command_tx, command_rx) = mpsc::channel(1);
        let (raft_in_tx, raft_in_rx) = mpsc::channel(64);
        let (raft_out_tx, raft_out_rx) = mpsc::channel(64);

        let network = Network::build(
            ReceiverStream::new(raft_out_rx),
            PollSender::new(raft_in_tx),
        );

        let consensus = Consensus::build(
            PollSender::new(command_tx),
            ReceiverStream::new(raft_in_rx),
            PollSender::new(raft_out_tx),
        );

        let processor = PartitionProcessor::build(
            ReceiverStream::new(command_rx),
            consensus.create_proposal_sender(),
        );

        Self {
            consensus,
            processor,
            network,
        }
    }

    pub async fn run(self, drain: drain::Watch) {
        let consensus_handle = tokio::spawn(self.consensus.run());
        let processor_handle = tokio::spawn(self.processor.run());
        let network_handle = tokio::spawn(self.network.run(drain.clone()));

        // Only signal shutdown once all handles have been completed
        let _release_shutdown = drain.ignore_signaled();

        try_join!(consensus_handle, processor_handle, network_handle)
            .expect("Worker component failed");
    }
}
