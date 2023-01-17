use crate::partition::PartitionProcessor;
use consensus::Consensus;
use tokio::sync::mpsc;
use tokio::try_join;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::PollSender;

mod fsm;
mod partition;

#[derive(Debug)]
pub struct Worker {
    consensus: Consensus<PollSender<fsm::Command>, fsm::Command>,
    processor: PartitionProcessor<ReceiverStream<fsm::Command>>,
}

impl Worker {
    pub fn build() -> Self {
        let (command_tx, command_rx) = mpsc::channel(1);
        let consensus = Consensus::build(PollSender::new(command_tx));
        let processor = PartitionProcessor::build(command_rx.into());

        Self {
            consensus,
            processor,
        }
    }

    pub async fn run(self, drain: drain::Watch) {
        let consensus_handle = tokio::spawn(self.consensus.run(drain));
        let processor_handle = tokio::spawn(self.processor.run());

        try_join!(consensus_handle, processor_handle).expect("Worker component failed");
    }
}
