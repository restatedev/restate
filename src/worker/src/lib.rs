use crate::partition::PartitionProcessor;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

mod fsm;
mod partition;

#[derive(Debug)]
pub struct Worker {
    processor: PartitionProcessor<ReceiverStream<fsm::Command>>,
}

impl Worker {
    pub fn build() -> Self {
        let (_command_tx, command_rx) = mpsc::channel(1);
        let processor = PartitionProcessor::build(command_rx.into());

        Self { processor }
    }

    pub async fn run(self, drain: drain::Watch) {
        let processor_handle = tokio::spawn(self.processor.run(drain));

        tokio::select! {
            processor_result = processor_handle => {
                processor_result.expect("partition processor panicked");
            }
        }
    }
}
