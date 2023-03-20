extern crate core;

use common::types::MillisSinceEpoch;
use tokio::sync::mpsc;

mod service;

pub use service::{Service, ServiceError, TimerReader};

enum Input<T> {
    Timer {
        wake_up_time: MillisSinceEpoch,
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

    pub async fn add_timer(&self, wake_up_time: MillisSinceEpoch, payload: T) -> Result<(), Error> {
        self.input_tx
            .send(Input::Timer {
                wake_up_time,
                payload,
            })
            .await
            .map_err(|_| Error::Closed)
    }
}

pub trait Timer {
    fn wake_up_time(&self) -> MillisSinceEpoch;
}
