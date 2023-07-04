use crate::service::TimerService;
use std::fmt::Debug;

/// # Timer options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, derive_builder::Builder)]
#[cfg_attr(feature = "options_schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "options_schema", schemars(rename = "TimerOptions"))]
#[builder(default)]
pub struct Options {
    /// # Num timers in memory limit
    ///
    /// The number of timers in memory limit is used to bound the amount of timers loaded in memory. If this limit is set, when exceeding it, the timers farther in the future will be spilled to disk.
    #[cfg_attr(
        feature = "options_schema",
        schemars(default = "Options::default_num_timers_in_memory_limit")
    )]
    num_timers_in_memory_limit: Option<usize>,
}

impl Options {
    pub fn build<'a, Timer, TimerReader, Clock>(
        &self,
        timer_reader: &'a TimerReader,
        clock: Clock,
    ) -> TimerService<'a, Timer, Clock, TimerReader>
    where
        Timer: crate::Timer + Debug + Clone,
        TimerReader: crate::TimerReader<Timer>,
        Clock: crate::Clock,
    {
        TimerService::new(clock, self.num_timers_in_memory_limit, timer_reader)
    }

    fn default_num_timers_in_memory_limit() -> usize {
        1024
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            num_timers_in_memory_limit: Some(Options::default_num_timers_in_memory_limit()),
        }
    }
}
