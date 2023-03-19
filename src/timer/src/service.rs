use crate::{Input, Output, Timer, TimerHandle};
use std::time::SystemTime;
use timer_queue::TimerQueue;
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tracing::debug;

pub trait TimerReader<T>
where
    T: Timer,
{
    type TimerStream: Stream<Item = T> + Unpin + Send;

    fn scan_timers(&self, earliest_wake_up_time: SystemTime) -> Self::TimerStream;
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("output receiver of timer service is closed")]
    OutputClosed,
}

#[derive(Debug)]
pub struct Service<T, TR> {
    input_rx: mpsc::Receiver<Input<T>>,

    output_tx: mpsc::Sender<Output<T>>,

    timer_reader: TR,

    // used to create the timer handle
    input_tx: mpsc::Sender<Input<T>>,
}

impl<T, TR> Service<T, TR>
where
    T: Timer,
    TR: TimerReader<T>,
{
    pub fn new(output_tx: mpsc::Sender<Output<T>>, timer_reader: TR) -> Self {
        let (input_tx, input_rx) = mpsc::channel(64);

        Self {
            input_rx,
            output_tx,
            timer_reader,
            input_tx,
        }
    }

    pub fn create_timer_handle(&self) -> TimerHandle<T> {
        TimerHandle::new(self.input_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), ServiceError> {
        debug!("Running the timer service.");

        let Self {
            mut input_rx,
            output_tx,
            timer_reader,
            ..
        } = self;

        let shutdown_signal = drain.signaled();
        tokio::pin!(shutdown_signal);

        let mut timer_logic = Self::initialize_timer(output_tx, timer_reader).await;

        loop {
            tokio::select! {
                Some(input) = input_rx.recv() => {
                    match input {
                        Input::Timer { wake_up_time, payload } => {
                            timer_logic.add_timer(wake_up_time, payload);
                        }
                    }
                },
                result = timer_logic.run() => result?,
                _ = &mut shutdown_signal => {
                    break;
                }
            }
        }

        debug!("Shut down the timer service.");

        Ok(())
    }

    async fn initialize_timer(
        output_tx: mpsc::Sender<Output<T>>,
        timer_reader: TR,
    ) -> TimerLogic<T> {
        let mut timer_logic = TimerLogic::new(output_tx);

        let mut timer_stream = timer_reader.scan_timers(SystemTime::UNIX_EPOCH);

        while let Some(timer) = timer_stream.next().await {
            timer_logic.add_timer(timer.wake_up_time(), timer);
        }

        timer_logic
    }
}

struct TimerLogic<T> {
    output_tx: mpsc::Sender<Output<T>>,
    timer_queue: TimerQueue<T>,
}

impl<T> TimerLogic<T> {
    fn new(output_tx: mpsc::Sender<Output<T>>) -> Self {
        Self {
            output_tx,
            timer_queue: TimerQueue::new(),
        }
    }

    fn add_timer(&mut self, wake_up_time: SystemTime, payload: T) {
        self.timer_queue.sleep_until(wake_up_time, payload);
    }

    async fn run(&mut self) -> Result<(), ServiceError> {
        loop {
            let payload = self.timer_queue.await_timer().await.into_inner();
            self.output_tx
                .send(Output::TimerFired(payload))
                .await
                .map_err(|_| ServiceError::OutputClosed)?;
        }
    }
}
