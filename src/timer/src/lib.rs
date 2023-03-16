use common::types::PartitionLeaderEpoch;
use tokio::sync::mpsc;

mod service;

pub use service::Service;

enum Input {
    Register {
        partition_leader_epoch: PartitionLeaderEpoch,
        output_tx: mpsc::Sender<Output>,
    },
    Unregister(PartitionLeaderEpoch),
}

#[derive(Debug)]
pub enum Output {}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("timer service has been closed")]
    Closed,
}

#[derive(Debug, Clone)]
pub struct TimerHandle {
    input_tx: mpsc::Sender<Input>,
}

impl TimerHandle {
    fn new(input_tx: mpsc::Sender<Input>) -> Self {
        Self { input_tx }
    }

    pub async fn register(
        &self,
        partition_leader_epoch: PartitionLeaderEpoch,
        output_tx: mpsc::Sender<Output>,
    ) -> Result<(), Error> {
        self.input_tx
            .send(Input::Register {
                partition_leader_epoch,
                output_tx,
            })
            .await
            .map_err(|_| Error::Closed)
    }

    pub async fn unregister(
        &self,
        partition_leader_epoch: PartitionLeaderEpoch,
    ) -> Result<(), Error> {
        self.input_tx
            .send(Input::Unregister(partition_leader_epoch))
            .await
            .map_err(|_| Error::Closed)
    }
}
