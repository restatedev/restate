extern crate core;

use common::types::PartitionLeaderEpoch;
use std::time::SystemTime;
use tokio::sync::mpsc;

mod service;

pub use service::Service;

enum Input<T> {
    Register {
        partition_leader_epoch: PartitionLeaderEpoch,
        output_tx: mpsc::Sender<Output<T>>,
    },
    Unregister(PartitionLeaderEpoch),
    Timer {
        partition_leader_epoch: PartitionLeaderEpoch,
        wake_up_time: SystemTime,
        payload: T,
    },
}

#[derive(Debug)]
pub enum Output<T> {
    TimerFired(T),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("timer service has been closed")]
    Closed,
}

#[derive(Debug, Clone)]
pub struct TimerHandle<T> {
    input_tx: mpsc::Sender<Input<T>>,
}

impl<T> TimerHandle<T> {
    fn new(input_tx: mpsc::Sender<Input<T>>) -> Self {
        Self { input_tx }
    }

    pub async fn register(
        &self,
        partition_leader_epoch: PartitionLeaderEpoch,
        output_tx: mpsc::Sender<Output<T>>,
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

    pub async fn add_timer(
        &self,
        wake_up_time: SystemTime,
        partition_leader_epoch: PartitionLeaderEpoch,
        payload: T,
    ) -> Result<(), Error> {
        self.input_tx
            .send(Input::Timer {
                wake_up_time,
                partition_leader_epoch,
                payload,
            })
            .await
            .map_err(|_| Error::Closed)
    }
}
