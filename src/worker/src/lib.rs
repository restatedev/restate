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
    #[allow(clippy::type_complexity)]
    consensus: Consensus<
        PollSender<consensus::Command<fsm::Command>>,
        fsm::Command,
        ReceiverStream<fsm::Command>,
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
        let (proposal_tx, proposal_rx) = mpsc::channel(64);
        let (raft_in_tx, raft_in_rx) = mpsc::channel(64);
        let (raft_out_tx, raft_out_rx) = mpsc::channel(64);

        let consensus = Consensus::build(
            PollSender::new(command_tx),
            ReceiverStream::new(proposal_rx),
            ReceiverStream::new(raft_in_rx),
            PollSender::new(raft_out_tx),
        );
        let processor = PartitionProcessor::build(
            ReceiverStream::new(command_rx),
            PollSender::new(proposal_tx),
        );
        let network = Network::build(
            ReceiverStream::new(raft_out_rx),
            PollSender::new(raft_in_tx),
        );

        Self {
            consensus,
            processor,
            network,
        }
    }

    pub async fn run(self, drain: drain::Watch) {
        let consensus_handle = tokio::spawn(self.consensus.run(drain.clone()));
        let processor_handle = tokio::spawn(self.processor.run());
        let network_handle = tokio::spawn(self.network.run(drain));

        try_join!(consensus_handle, processor_handle, network_handle)
            .expect("Worker component failed");
    }
}
