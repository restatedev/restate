#![allow(clippy::enum_variant_names)]

use crate::{Output, Sequenced, TimerKey, TimerServiceError};
use pin_project::pin_project;
use restate_common::types::TimerSeqNumber;
use std::fmt::Debug;
use std::future;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{ready, Context, Poll, Waker};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tracing::trace;

// Using ahash for faster hashing operations. See: https://github.com/garro95/priority-queue#speeding-up
type DoublePriorityQueue<T> =
    priority_queue::DoublePriorityQueue<T, <T as crate::Timer>::TimerKey, ahash::RandomState>;

#[pin_project(project = StateProj)]
enum State<TimerKey, ReservePermitFuture, TimerStream, SleepFuture> {
    Idle(Waker),
    LoadTimers(#[pin] TimerStream),
    ProcessTimers {
        timer_batch: Option<TimerBatch<TimerKey>>,
        #[pin]
        process_timers_state: ProcessTimersState<ReservePermitFuture, TimerKey, SleepFuture>,
    },
}

impl<TimerKey, ReservePermitFuture, TimerStream, SleepFuture>
    State<TimerKey, ReservePermitFuture, TimerStream, SleepFuture>
{
    fn process_timers(timer_batch: Option<TimerBatch<TimerKey>>) -> Self {
        State::ProcessTimers {
            timer_batch,
            process_timers_state: ProcessTimersState::ReadNextTimer,
        }
    }
}

#[pin_project(project = ProcessTimersStateProj)]
enum ProcessTimersState<ReservePermitFuture, TimerKey, SleepFuture> {
    ReadNextTimer,
    AwaitTimer {
        timer_key: TimerKey,
        #[pin]
        sleep: SleepFuture,
    },
    NotifyDueTimer(#[pin] ReservePermitFuture),
}

/// Current batch of timers that is being processed by the service
#[derive(Debug)]
struct TimerBatch<T> {
    end: T,
}

impl<T> TimerBatch<T>
where
    T: Ord,
{
    fn new(end: T) -> Self {
        Self { end }
    }

    /// Checks whether the given timer is less or equal than the timer batch's end
    fn contains(&self, timer_key: &T) -> bool {
        timer_key <= &self.end
    }
}

#[pin_project]
pub(crate) struct TimerLogic<
    'a,
    Timer,
    Clock,
    ReservePermit,
    ReservePermitFuture,
    LoadTimers,
    TimerStream,
> where
    Timer: crate::Timer,
    Clock: crate::service::clock::Clock,
{
    clock: Clock,

    #[pin]
    state: State<Timer::TimerKey, ReservePermitFuture, TimerStream, Clock::SleepFuture>,

    max_fired_timer: Option<Timer::TimerKey>,

    max_seq_number: Option<TimerSeqNumber>,

    timer_queue: DoublePriorityQueue<Timer>,

    reserve_permit: ReservePermit,
    load_timers: LoadTimers,

    num_timers_in_memory_limit: Option<usize>,

    _lifetime: PhantomData<&'a ReservePermitFuture>,
}

impl<'a, Timer, Clock, ReservePermit, ReservePermitFuture, LoadTimers, TimerStream>
    TimerLogic<'a, Timer, Clock, ReservePermit, ReservePermitFuture, LoadTimers, TimerStream>
where
    Timer: crate::Timer + Debug + 'a,
    Clock: crate::service::clock::Clock,
    ReservePermit: Fn() -> ReservePermitFuture,
    ReservePermitFuture:
        Future<Output = Result<mpsc::Permit<'a, Output<Timer>>, mpsc::error::SendError<()>>>,
    LoadTimers: Fn(usize, Option<Timer::TimerKey>) -> TimerStream,
    TimerStream: Stream<Item = Sequenced<Timer>>,
{
    pub(crate) fn new(
        clock: Clock,
        num_timers_in_memory_limit: Option<usize>,
        load_timers: LoadTimers,
        reserve_permit: ReservePermit,
    ) -> Self {
        debug_assert!(
            num_timers_in_memory_limit.unwrap_or(usize::MAX) >= 1,
            "Timer service needs to keep at least one timer in memory."
        );
        Self {
            clock,
            num_timers_in_memory_limit,
            state: State::LoadTimers(load_timers(
                num_timers_in_memory_limit.unwrap_or(usize::MAX),
                None,
            )),
            max_fired_timer: None,
            max_seq_number: None,
            timer_queue: DoublePriorityQueue::default(),
            reserve_permit,
            load_timers,
            _lifetime: PhantomData::default(),
        }
    }

    pub(crate) fn add_timer(self: Pin<&mut Self>, timer: Sequenced<Timer>) {
        let this = self.project();
        let timer_queue = this.timer_queue;
        let max_fired_timer = this.max_fired_timer;
        let max_seq_number = this.max_seq_number;
        let mut state = this.state;

        let (seq_number, timer) = timer.into_inner();

        if max_seq_number
            .map(|max_seq_number| max_seq_number >= seq_number)
            .unwrap_or(false)
        {
            trace!("Received an already processed timer {timer:?}. Ignoring it.");
            return;
        } else {
            *max_seq_number = Some(seq_number);
        }

        match state.as_mut().project() {
            StateProj::Idle(waker) => {
                debug_assert!(
                    timer_queue.is_empty(),
                    "Timer queue should be empty if timer logic is idling."
                );

                trace!("Start processing timers because new timer {timer:?} was added.");

                let timer_key = timer.timer_key();
                let timer_batch =
                    TimerBatch::new(Self::max_timer_key(&timer_key, max_fired_timer.as_ref()));

                timer_queue.push(timer, timer_key);
                waker.wake_by_ref();

                state.set(State::process_timers(Some(timer_batch)));
            }
            StateProj::LoadTimers(_) => {
                trace!("Add timer {timer:?} to in memory queue while loading timers from storage.");

                let timer_key = timer.timer_key();
                timer_queue.push(timer, timer_key);

                this.num_timers_in_memory_limit
                    .map(|num_timers_in_memory_limit| {
                        Self::trim_timer_queue(
                            timer_queue,
                            num_timers_in_memory_limit,
                            max_fired_timer.as_ref(),
                        )
                    });
            }
            StateProj::ProcessTimers {
                timer_batch,
                mut process_timers_state,
            } => {
                let timer_batch = timer_batch.as_mut().expect("Expect valid timer batch.");
                let timer_key = timer.timer_key();

                // if memory limit is configured, then check whether timer is in batch, otherwise
                // add timer to batch (since all timers are kept in memory)
                if this.num_timers_in_memory_limit.is_none() || timer_batch.contains(&timer_key) {
                    trace!("Add timer {timer:?} to in memory queue.");
                    let new_timer_key = timer_key.clone();
                    timer_queue.push(timer, timer_key);

                    // the new timer is guaranteed to be smaller than the current end
                    let new_batch_end = this
                        .num_timers_in_memory_limit
                        .map(|limit| {
                            Self::trim_timer_queue(timer_queue, limit, max_fired_timer.as_ref())
                        })
                        .unwrap_or(true);

                    if new_batch_end {
                        let (_, timer_key) = timer_queue
                            .peek_max()
                            .expect("Timer queue should contain at least one element.");

                        *timer_batch = TimerBatch::new(Self::max_timer_key(
                            timer_key,
                            max_fired_timer.as_ref(),
                        ));
                        trace!("Updated current timer batch to {timer_batch:?}.");
                    }

                    match process_timers_state.as_mut().project() {
                        ProcessTimersStateProj::ReadNextTimer => {
                            // nothing to do because peek timer will be read next
                        }
                        ProcessTimersStateProj::AwaitTimer { timer_key, .. } => {
                            // we might wait for a later timer if the newly added timer fires earlier
                            if new_timer_key < *timer_key {
                                trace!("Reset process timer state to ReadNextTimer because added timer fires earlier.");
                                process_timers_state.set(ProcessTimersState::ReadNextTimer);
                            }
                        }
                        ProcessTimersStateProj::NotifyDueTimer(_) => {
                            // nothing to do because peek timer will be sent next
                        }
                    }
                } else {
                    trace!("Ignore timer {timer:?} because it is not contained in the current timer batch {timer_batch:?}.");
                }
            }
        }
    }

    pub(crate) fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TimerServiceError>> {
        let this = self.project();
        let timer_queue = this.timer_queue;
        let max_fired_timer = this.max_fired_timer;
        let max_seq_number = this.max_seq_number;
        let mut state = this.state;
        let reserve_permit = this.reserve_permit;
        let load_timers = this.load_timers;

        loop {
            match state.as_mut().project() {
                StateProj::Idle(_) => {
                    return Poll::Pending;
                }
                StateProj::LoadTimers(timer_stream) => {
                    let seq_next_timer = ready!(timer_stream.poll_next(cx));

                    let mut finished_loading_timers = false;

                    if let Some(seq_next_timer) = seq_next_timer {
                        let (seq_number, next_timer) = seq_next_timer.into_inner();

                        if max_seq_number
                            .map(|max_seq_number| max_seq_number < seq_number)
                            .unwrap_or(true)
                        {
                            *max_seq_number = Some(seq_number);
                        }

                        let timer_key = next_timer.timer_key();

                        // We can only stop loading timers if we know that all subsequent timers have
                        // a strictly larger timer key (later wake up time or larger key)
                        if this
                            .num_timers_in_memory_limit
                            .map(|limit| timer_queue.len() >= limit)
                            .unwrap_or(false)
                            && timer_queue
                                .peek_max()
                                .expect("Timer queue expected to contain an element.")
                                .1
                                < &timer_key
                        {
                            trace!("Finished loading timers from storage because the in memory limit has been reached.");
                            finished_loading_timers = true;
                        } else {
                            trace!("Load timer {next_timer:?} into in memory queue.");
                            timer_queue.push(next_timer, timer_key);
                        }

                        // get rid of larger timers that exceed in memory threshold
                        this.num_timers_in_memory_limit.map(|limit| {
                            Self::trim_timer_queue(timer_queue, limit, max_fired_timer.as_ref())
                        });
                    } else {
                        finished_loading_timers = true;
                    }

                    if finished_loading_timers {
                        // get rid of larger timers that exceed in memory threshold
                        this.num_timers_in_memory_limit.map(|limit| {
                            Self::trim_timer_queue(timer_queue, limit, max_fired_timer.as_ref())
                        });

                        if let Some((_, timer_key)) = timer_queue.peek_max() {
                            trace!("Start processing timers.");
                            let timer_batch = TimerBatch::new(Self::max_timer_key(
                                timer_key,
                                max_fired_timer.as_ref(),
                            ));

                            state.set(State::ProcessTimers {
                                process_timers_state: ProcessTimersState::ReadNextTimer,
                                timer_batch: Some(timer_batch),
                            });
                        } else {
                            trace!("Go into idle state because there are no timers to await.");
                            state.set(State::Idle(cx.waker().clone()));
                        }
                    }
                }
                StateProj::ProcessTimers {
                    timer_batch,
                    mut process_timers_state,
                } => match process_timers_state.as_mut().project() {
                    ProcessTimersStateProj::ReadNextTimer => {
                        if let Some((_, timer_key)) = timer_queue.peek_min() {
                            let wake_up_time = timer_key.wake_up_time();
                            if let Some(sleep) = this.clock.sleep_until(wake_up_time) {
                                trace!("Awaiting next timer {timer_key:?} which is due at {wake_up_time}.");
                                process_timers_state.set(ProcessTimersState::AwaitTimer {
                                    timer_key: timer_key.clone(),
                                    sleep,
                                });
                            } else {
                                trace!("Notify due timer {timer_key:?}.");
                                process_timers_state
                                    .set(ProcessTimersState::NotifyDueTimer(reserve_permit()))
                            }
                        } else {
                            let end_of_batch =
                                timer_batch.take().map(|timer_batch| timer_batch.end);

                            assert_eq!(
                                max_fired_timer, &end_of_batch,
                                "Max fired timer should coincide with end of batch."
                            );

                            trace!("Finished processing of current timer batch '{:?}'. Trying loading new timers from storage.", end_of_batch);
                            state.set(State::LoadTimers(load_timers(
                                this.num_timers_in_memory_limit.unwrap_or(usize::MAX),
                                end_of_batch,
                            )))
                        }
                    }
                    ProcessTimersStateProj::AwaitTimer { sleep, .. } => {
                        ready!(sleep.poll(cx));
                        process_timers_state
                            .set(ProcessTimersState::NotifyDueTimer(reserve_permit()));
                    }
                    ProcessTimersStateProj::NotifyDueTimer(permit) => {
                        let permit = ready!(permit.poll(cx));

                        if permit.is_err() {
                            return Poll::Ready(Err(TimerServiceError::OutputClosed));
                        }

                        let permit = permit.unwrap();

                        if let Some((timer, timer_key)) = timer_queue.pop_min() {
                            trace!("Send notification for fired timer {timer:?}.");
                            permit.send(Output::TimerFired(timer));

                            // update max fired timer if the fired timer is larger
                            if max_fired_timer
                                .as_ref()
                                .map(|max_fired_timer| *max_fired_timer < timer_key)
                                .unwrap_or(true)
                            {
                                *max_fired_timer = Some(timer_key);
                            }
                        }

                        trace!("Try to read next timer.");
                        process_timers_state.set(ProcessTimersState::ReadNextTimer);
                    }
                },
            }
        }
    }

    pub(crate) async fn run(mut self: Pin<&mut Self>) -> Result<(), TimerServiceError> {
        future::poll_fn(|cx| self.as_mut().poll(cx)).await
    }

    /// Trim timer queue with respect to target queue size and max fired timer so far.
    /// Only timers that are larger than the max fired timer can be trimmed. The next
    /// read from storage needs to continue at least from the max fired timer because
    /// we cannot guarantee that triggered timers have been deleted.
    fn trim_timer_queue(
        timer_queue: &mut DoublePriorityQueue<Timer>,
        target_queue_size: usize,
        max_fired_timer: Option<&Timer::TimerKey>,
    ) -> bool {
        debug_assert!(
            target_queue_size >= 1,
            "Target queue size must be larger than 0."
        );

        let mut has_trimmed_queue = false;

        while timer_queue.len() > target_queue_size {
            let (_, current_max_key) = timer_queue
                .peek_max()
                .expect("Element must exist since queue is not empty.");

            // only trim timers that are larger than the max fired timer, because that's where the
            // next read will at least continue from
            if max_fired_timer
                .map(|last_fired_timer| last_fired_timer < current_max_key)
                .unwrap_or(true)
            {
                let (popped_timer, _) = timer_queue
                    .pop_max()
                    .expect("Element must exist since queue is not empty.");
                trace!("Removing timer {popped_timer:?} from in memory timer queue.");
                has_trimmed_queue = true;
            } else {
                break;
            }
        }

        has_trimmed_queue
    }

    fn max_timer_key(
        timer_key: &Timer::TimerKey,
        max_fired_timer: Option<&Timer::TimerKey>,
    ) -> Timer::TimerKey {
        if max_fired_timer
            .map(|max_fired_timer| max_fired_timer > timer_key)
            .unwrap_or(false)
        {
            max_fired_timer.unwrap().clone()
        } else {
            timer_key.clone()
        }
    }
}
