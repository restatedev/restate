// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::enum_variant_names)]

use pin_project::pin_project;
use restate_types::timer::TimerKey;
use std::collections::HashSet;
use std::fmt::Debug;
use std::future;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker, ready};
use tokio_util::sync::ReusableBoxFuture;
use tracing::trace;

pub mod clock;
#[cfg(test)]
mod tests;

// Using ahash for faster hashing operations. See: https://github.com/garro95/priority-queue#speeding-up
type DoublePriorityQueue<T> =
    priority_queue::DoublePriorityQueue<T, <T as crate::Timer>::TimerKey, ahash::RandomState>;

#[pin_project(project = StateProj)]
enum State<TimerKey, SleepFuture> {
    Idle(Waker),
    LoadTimers {
        removed_timers: Option<HashSet<TimerKey>>,
    },
    ProcessTimers {
        timer_batch: Option<TimerBatch<TimerKey>>,
        #[pin]
        process_timers_state: ProcessTimersState<TimerKey, SleepFuture>,
    },
}

impl<TimerKey, SleepFuture> State<TimerKey, SleepFuture> {
    fn process_timers(timer_batch: Option<TimerBatch<TimerKey>>) -> Self {
        State::ProcessTimers {
            timer_batch,
            process_timers_state: ProcessTimersState::ReadNextTimer,
        }
    }
}

#[pin_project(project = ProcessTimersStateProj)]
enum ProcessTimersState<TimerKey, SleepFuture> {
    ReadNextTimer,
    AwaitTimer {
        timer_key: TimerKey,
        #[pin]
        sleep: SleepFuture,
    },
    TriggerTimer,
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
pub struct TimerService<Timer, Clock, TimerReader>
where
    Timer: crate::Timer,
    Clock: clock::Clock,
    TimerReader: crate::TimerReader<Timer>,
{
    clock: Clock,

    timer_reader: Option<TimerReader>,

    read_future: ReusableBoxFuture<'static, (TimerReader, Vec<Timer>)>,

    #[pin]
    state: State<Timer::TimerKey, Clock::SleepFuture>,

    removed_timers: Option<HashSet<Timer::TimerKey>>,

    max_fired_timer: Option<Timer::TimerKey>,

    timer_queue: DoublePriorityQueue<Timer>,

    num_timers_in_memory_limit: Option<usize>,
}

async fn get_timers<Timer, TimerReader>(
    mut timer_reader: TimerReader,
    num_timers: usize,
    previous_timer_key: Option<Timer::TimerKey>,
) -> (TimerReader, Vec<Timer>)
where
    Timer: crate::Timer + Debug,
    TimerReader: crate::TimerReader<Timer>,
{
    let result = timer_reader
        .get_timers(num_timers, previous_timer_key)
        .await;
    (timer_reader, result)
}

impl<Timer, Clock, TimerReader> TimerService<Timer, Clock, TimerReader>
where
    Timer: crate::Timer + Debug + 'static,
    Clock: clock::Clock,
    TimerReader: crate::TimerReader<Timer> + Send + 'static,
{
    pub fn new(
        clock: Clock,
        num_timers_in_memory_limit: Option<usize>,
        timer_reader: TimerReader,
    ) -> Self {
        debug_assert!(
            num_timers_in_memory_limit.unwrap_or(usize::MAX) >= 1,
            "Timer service needs to keep at least one timer in memory."
        );
        Self {
            clock,
            timer_reader: None,
            read_future: ReusableBoxFuture::new(get_timers(
                timer_reader,
                num_timers_in_memory_limit.unwrap_or(usize::MAX),
                None,
            )),
            num_timers_in_memory_limit,
            state: State::LoadTimers {
                removed_timers: Some(HashSet::default()),
            },
            removed_timers: None,
            max_fired_timer: None,
            timer_queue: DoublePriorityQueue::default(),
        }
    }

    pub fn add_timer(self: Pin<&mut Self>, timer: Timer) {
        let this = self.project();
        let timer_queue = this.timer_queue;
        let max_fired_timer = this.max_fired_timer;
        let mut state = this.state;

        match state.as_mut().project() {
            StateProj::Idle(waker) => {
                debug_assert!(
                    timer_queue.is_empty(),
                    "Timer queue should be empty if timer logic is idling."
                );

                trace!("Start processing timers because new timer {timer:?} was added.");

                let timer_key = timer.timer_key().clone();
                let timer_batch =
                    TimerBatch::new(Self::max_timer_key(&timer_key, max_fired_timer.as_ref()));

                timer_queue.push(timer, timer_key);
                waker.wake_by_ref();

                state.set(State::process_timers(Some(timer_batch)));
            }
            StateProj::LoadTimers { removed_timers } => {
                trace!("Add timer {timer:?} to in memory queue while loading timers from storage.");

                let timer_key = timer.timer_key().clone();
                // remove removed timer in case we removed it before
                removed_timers
                    .as_mut()
                    .expect("removed_timers hash set must be present")
                    .remove(&timer_key);
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
                let timer_key = timer.timer_key();

                // if memory limit is configured, then check whether timer is in batch, otherwise
                // add timer to batch (since all timers are kept in memory)
                if this.num_timers_in_memory_limit.is_none()
                    || timer_batch
                        .as_ref()
                        .map(|batch| batch.contains(timer_key))
                        .unwrap_or_default()
                {
                    trace!("Add timer {timer:?} to in memory queue.");
                    let new_timer_key = timer_key.clone();
                    let timer_key = timer_key.clone();
                    timer_queue.push(timer, timer_key);

                    // the new timer is guaranteed to be smaller than the current end
                    let new_batch_end = this
                        .num_timers_in_memory_limit
                        .map(|limit| {
                            Self::trim_timer_queue(timer_queue, limit, max_fired_timer.as_ref())
                        })
                        .unwrap_or(true);

                    if new_batch_end {
                        Self::adjust_timer_batch_end(timer_queue, max_fired_timer, timer_batch);
                    }

                    match process_timers_state.as_mut().project() {
                        ProcessTimersStateProj::ReadNextTimer => {
                            // nothing to do because peek timer will be read next
                        }
                        ProcessTimersStateProj::AwaitTimer { timer_key, .. } => {
                            // we might wait for a later timer if the newly added timer fires earlier
                            if new_timer_key < *timer_key {
                                trace!(
                                    "Reset process timer state to ReadNextTimer because added timer fires earlier."
                                );
                                process_timers_state.set(ProcessTimersState::ReadNextTimer);
                            }
                        }
                        ProcessTimersStateProj::TriggerTimer => {
                            // nothing to do because peek timer will be sent next
                        }
                    }
                } else {
                    trace!(
                        "Ignore timer {timer:?} because it is not contained in the current timer batch {timer_batch:?}."
                    );
                }
            }
        }
    }

    pub fn remove_timer(self: Pin<&mut Self>, key: Timer::TimerKey) {
        let this = self.project();
        let timer_queue = this.timer_queue;
        let max_fired_timer = this.max_fired_timer;

        match this.state.project() {
            StateProj::Idle(_) => {
                debug_assert!(
                    timer_queue.is_empty(),
                    "Timer queue should be empty if timer logic is idling."
                );
            }
            StateProj::LoadTimers { removed_timers, .. } => {
                trace!(
                    "Remove timer '{key:?}' from timer queue and remember for filtering out newly loaded timers while loading."
                );
                timer_queue.remove(&key);
                removed_timers
                    .as_mut()
                    .expect("removed_timers hash set must be present")
                    .insert(key);
            }
            StateProj::ProcessTimers {
                mut process_timers_state,
                timer_batch,
            } => {
                let removed_timer = timer_queue.remove(&key);

                if let Some((timer, _)) = removed_timer {
                    trace!("Removed timer '{timer:?}' while processing timer batch.");
                }

                if timer_batch
                    .as_ref()
                    .map(|batch| batch.end == key)
                    .unwrap_or_default()
                {
                    Self::adjust_timer_batch_end(timer_queue, max_fired_timer, timer_batch);
                }

                match process_timers_state.as_mut().project() {
                    ProcessTimersStateProj::ReadNextTimer => {
                        // nothing to do
                    }
                    ProcessTimersStateProj::AwaitTimer { timer_key, .. } => {
                        if key == *timer_key {
                            process_timers_state.set(ProcessTimersState::ReadNextTimer);

                            trace!("Skip awaiting removed timer '{key:?}'. Read next timer.");
                        }
                    }
                    ProcessTimersStateProj::TriggerTimer => {
                        // nothing to do
                    }
                }
            }
        }
    }

    fn adjust_timer_batch_end(
        timer_queue: &mut DoublePriorityQueue<Timer>,
        max_fired_timer: &mut Option<Timer::TimerKey>,
        timer_batch: &mut Option<TimerBatch<Timer::TimerKey>>,
    ) {
        let max_timer_in_queue = timer_queue.peek_max().map(|(_, key)| key);

        if let Some(max_timer_in_queue) = max_timer_in_queue {
            *timer_batch = Some(TimerBatch::new(Self::max_timer_key(
                max_timer_in_queue,
                max_fired_timer.as_ref(),
            )));
        } else {
            *timer_batch = max_fired_timer
                .as_ref()
                .map(|max_fired_timer| TimerBatch::new(max_fired_timer.clone()));
        }

        trace!("Updated current timer batch to {timer_batch:?}.");
    }

    pub fn poll_next_timer(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Timer> {
        let this = self.project();
        let timer_queue = this.timer_queue;
        let max_fired_timer = this.max_fired_timer;
        let mut state = this.state;

        loop {
            match state.as_mut().project() {
                StateProj::Idle(_) => {
                    return Poll::Pending;
                }
                StateProj::LoadTimers { removed_timers } => {
                    let (timer_reader, next_timers) = ready!(this.read_future.poll(cx));
                    *this.timer_reader = Some(timer_reader);

                    {
                        let removed_timers = removed_timers
                            .as_ref()
                            .expect("removed_timers hash set must be present");

                        for next_timer in next_timers
                            .into_iter()
                            .filter(|timer| !removed_timers.contains(timer.timer_key()))
                        {
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
                                    < timer_key
                            {
                                trace!(
                                    "Finished loading timers from storage because the in memory limit has been reached."
                                );
                                break;
                            } else {
                                trace!("Load timer {next_timer:?} into in memory queue.");
                                let timer_key = timer_key.clone();
                                timer_queue.push(next_timer, timer_key);
                            }
                        }
                    }

                    // get rid of larger timers that exceed in memory threshold
                    this.num_timers_in_memory_limit.map(|limit| {
                        Self::trim_timer_queue(timer_queue, limit, max_fired_timer.as_ref())
                    });

                    *this.removed_timers = removed_timers.take();
                    this.removed_timers
                        .as_mut()
                        .expect("removed_timers must be present")
                        .clear();

                    if let Some((_, timer_key)) = timer_queue.peek_max() {
                        trace!("Start processing timers.");
                        let timer_batch = TimerBatch::new(Self::max_timer_key(
                            timer_key,
                            max_fired_timer.as_ref(),
                        ));

                        state.set(State::process_timers(Some(timer_batch)));
                    } else {
                        trace!("Go into idle state because there are no timers to await.");
                        state.set(State::Idle(cx.waker().clone()));
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
                                trace!(
                                    "Awaiting next timer {timer_key:?} which is due at {wake_up_time}."
                                );
                                process_timers_state.set(ProcessTimersState::AwaitTimer {
                                    timer_key: timer_key.clone(),
                                    sleep,
                                });
                            } else {
                                trace!("Trigger due timer {timer_key:?}.");
                                process_timers_state.set(ProcessTimersState::TriggerTimer)
                            }
                        } else {
                            let end_of_batch =
                                timer_batch.take().map(|timer_batch| timer_batch.end);

                            assert_eq!(
                                max_fired_timer, &end_of_batch,
                                "Max fired timer should coincide with end of batch."
                            );

                            trace!(
                                "Finished processing of current timer batch '{:?}'. Trying loading new timers from storage.",
                                end_of_batch
                            );

                            let removed_timers = this.removed_timers.take();

                            this.read_future.set(get_timers(
                                this.timer_reader
                                    .take()
                                    .expect("timer_reader must be present"),
                                this.num_timers_in_memory_limit.unwrap_or(usize::MAX),
                                end_of_batch,
                            ));
                            state.set(State::LoadTimers { removed_timers });
                        }
                    }
                    ProcessTimersStateProj::AwaitTimer { sleep, .. } => {
                        ready!(sleep.poll(cx));
                        process_timers_state.set(ProcessTimersState::TriggerTimer);
                    }
                    ProcessTimersStateProj::TriggerTimer => {
                        process_timers_state.set(ProcessTimersState::ReadNextTimer);

                        if let Some((timer, timer_key)) = timer_queue.pop_min() {
                            trace!("Trigger timer {timer:?}.");

                            // update max fired timer if the fired timer is larger
                            if max_fired_timer
                                .as_ref()
                                .map(|max_fired_timer| *max_fired_timer < timer_key)
                                .unwrap_or(true)
                            {
                                *max_fired_timer = Some(timer_key);
                            }

                            return Poll::Ready(timer);
                        }
                    }
                },
            }
        }
    }

    pub async fn next_timer(mut self: Pin<&mut Self>) -> Timer {
        future::poll_fn(|cx| self.as_mut().poll_next_timer(cx)).await
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
