use crate::service::timer_logic::TimerLogic;
use crate::{Input, Output, TimerHandle};
use std::fmt::Debug;
use tokio::sync::mpsc;
use tracing::debug;

pub mod clock;
#[cfg(test)]
mod tests;
mod timer_logic;

#[derive(Debug, thiserror::Error)]
pub enum TimerServiceError {
    #[error("output receiver of timer service is closed")]
    OutputClosed,
}

#[derive(Debug)]
pub struct TimerService<Timer, TimerReader, Clock> {
    input_rx: mpsc::Receiver<Input<Timer>>,

    output_tx: mpsc::Sender<Output<Timer>>,

    timer_reader: TimerReader,

    clock: Clock,

    num_timers_in_memory_limit: Option<usize>,

    // used to create the timer handle
    input_tx: mpsc::Sender<Input<Timer>>,
}

impl<Timer, TimerReader, Clock> TimerService<Timer, TimerReader, Clock>
where
    Timer: crate::Timer + Debug + Clone,
    TimerReader: crate::TimerReader<Timer>,
    Clock: clock::Clock,
{
    pub(crate) fn new(
        num_timers_in_memory_limit: Option<usize>,
        output_tx: mpsc::Sender<Output<Timer>>,
        timer_reader: TimerReader,
        clock: Clock,
        channel_size: usize,
    ) -> Self {
        let (input_tx, input_rx) = mpsc::channel(channel_size);

        Self {
            input_rx,
            output_tx,
            timer_reader,
            clock,
            input_tx,
            num_timers_in_memory_limit,
        }
    }

    pub fn create_timer_handle(&self) -> TimerHandle<Timer> {
        TimerHandle::new(self.input_tx.clone())
    }

    pub async fn run(self, drain: drain::Watch) -> Result<(), TimerServiceError> {
        debug!("Running timer service");

        let Self {
            mut input_rx,
            output_tx,
            timer_reader,
            clock,
            num_timers_in_memory_limit,
            ..
        } = self;

        let shutdown_signal = drain.signaled();

        let timer_logic = TimerLogic::new(
            clock,
            num_timers_in_memory_limit,
            |num_timers, previous_timer| timer_reader.scan_timers(num_timers, previous_timer),
            || output_tx.reserve(),
        );
        tokio::pin!(timer_logic, shutdown_signal);

        loop {
            tokio::select! {
                Some(input) = input_rx.recv() => {
                    match input {
                        Input::Timer { timer } => {
                            timer_logic.as_mut().add_timer(timer);
                        }
                    }
                },
                result = timer_logic.as_mut().run() => result?,
                _ = &mut shutdown_signal => {
                    break;
                }
            }
        }

        debug!("Shut down the timer service.");

        Ok(())
    }
}
