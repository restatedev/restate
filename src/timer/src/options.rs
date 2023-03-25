use crate::{Output, TimerService};
use std::fmt::Debug;
use tokio::sync::mpsc;

#[derive(Debug, Clone, clap::Parser)]
#[group(skip)]
pub struct Options {
    #[arg(
        long = "num-timers-in-memory-limit",
        env = "NUM_TIMERS_IN_MEMORY_LIMIT",
        default_value = "10"
    )]
    pub num_timers_in_memory_limit: Option<usize>,
}

impl Options {
    pub fn build<Timer, TimerReader, Clock>(
        &self,
        output_tx: mpsc::Sender<Output<Timer>>,
        timer_reader: TimerReader,
        clock: Clock,
    ) -> TimerService<Timer, TimerReader, Clock>
    where
        Timer: crate::Timer + Debug + Clone,
        TimerReader: crate::TimerReader<Timer>,
        Clock: crate::Clock,
    {
        TimerService::new(
            self.num_timers_in_memory_limit,
            output_tx,
            timer_reader,
            clock,
        )
    }
}
