extern crate core;

use restate_common::types::{MillisSinceEpoch, TimerSeqNumber};
use std::fmt::Debug;
use std::hash::Hash;
use tokio::sync::mpsc;
use tokio_stream::Stream;

mod options;
mod service;

pub use options::Options;
pub use service::clock::{Clock, TokioClock};
pub use service::{TimerService, TimerServiceError};

#[derive(Debug)]
enum Input<T> {
    Timer { timer: Sequenced<T> },
}

#[derive(Debug, PartialEq)]
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

impl<T> TimerHandle<T>
where
    T: Timer,
{
    fn new(input_tx: mpsc::Sender<Input<T>>) -> Self {
        Self { input_tx }
    }

    /// Adds a timer to the associated [`TimerService`].
    ///
    /// # Important
    /// Before adding a timer via this method, the timer needs to be made available
    /// to the associated timer service via the provided [`TimerReader`]. This usually
    /// entails storing the timer so that the timer reader can retrieve it.
    ///
    /// Only if **no** memory limit has been configured on the associated timer service, this is
    /// not required.
    pub async fn add_timer(&self, timer: Sequenced<T>) -> Result<(), Error> {
        self.input_tx
            .send(Input::Timer { timer })
            .await
            .map_err(|_| Error::Closed)
    }
}

pub trait Timer: Hash + Eq {
    type TimerKey: TimerKey;

    fn timer_key(&self) -> Self::TimerKey;
}

/// Timer key establishes an absolute order on [`Timer`]. Naturally, this should be key under
/// which the timer value is stored and can be retrieved.
pub trait TimerKey: Ord + Clone + Debug {
    fn wake_up_time(&self) -> MillisSinceEpoch;
}

#[derive(Debug, Clone, Copy)]
pub struct Sequenced<T> {
    seq_number: TimerSeqNumber,
    timer: T,
}

impl<T> Sequenced<T> {
    pub fn new(seq_number: TimerSeqNumber, timer: T) -> Self {
        Self { seq_number, timer }
    }

    pub fn into_inner(self) -> (TimerSeqNumber, T) {
        (self.seq_number, self.timer)
    }

    pub fn into_timer(self) -> T {
        self.timer
    }

    pub fn timer(&self) -> &T {
        &self.timer
    }
}

pub trait TimerReader<T>
where
    T: Timer,
{
    type TimerStream<'a>: Stream<Item = Sequenced<T>> + Send
    where
        Self: 'a;

    /// Scan the next `num_timers` starting with the next timer after `previous_timer_key`.
    ///
    /// # Contract
    /// The returned timers need to follow the order defined by [`TimerKey`]. This entails
    /// scan timers must never return a timer whose key is <= `previous_timer_key`
    fn scan_timers(
        &self,
        num_timers: usize,
        previous_timer_key: Option<T::TimerKey>,
    ) -> Self::TimerStream<'_>;
}
